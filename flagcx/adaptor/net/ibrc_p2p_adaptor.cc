/*************************************************************************
 * Copyright (c) 2026 BAAI. All rights reserved.
 *
 * IBRC P2P Net Adaptor — implements flagcxNetAdaptor for one-sided RDMA
 * (P2P) use cases. Shares IB device discovery and utility code with the
 * existing IBRC adaptor but uses P2P-native handle formats, eager PD
 * allocation, and simplified (no-FIFO) connection setup.
 ************************************************************************/

#include "flagcx_common.h"
#include "flagcx_net_adaptor.h"
#include "ib_common.h"
#include "ibvwrap.h"
#include "socket.h"

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <pthread.h>
#include <string.h>
#include <thread>
#include <unistd.h>
#include <vector>

/* ------------------------------------------------------------------ */
/*  Internal structs                                                   */
/* ------------------------------------------------------------------ */

// Per-device context — created at init, holds eagerly allocated PD.
// Passed as the `comm` parameter to regMr/deregMr when no connection exists.
// ibDevN MUST be the first field so regMr can cast any comm pointer to extract
// it.
struct flagcxP2pDevCtx {
  int ibDevN;
  struct ibv_pd *pd;
};

// P2P MR handle — replaces rank-indexed flagcxOneSideHandleInfo
struct flagcxP2pMrHandle {
  uintptr_t baseVa;
  uint32_t lkey;
  uint32_t rkey;
  ibv_mr *mr;
  int ibDevN; // for cache lookup during deregMr
};

// P2P listen handle — stable wire metadata only, no mutable stage
struct flagcxP2pListenHandle {
  union flagcxSocketAddress connectAddr;
  uint64_t magic;
};
static_assert(sizeof(struct flagcxP2pListenHandle) <= FLAGCX_NET_HANDLE_MAXSIZE,
              "P2P listen handle must fit in FLAGCX_NET_HANDLE_MAXSIZE");

// P2P listen comm
struct flagcxP2pListenComm {
  int dev;
  struct flagcxSocket sock;
};

// Connection metadata exchanged over TCP during connect/accept
struct flagcxP2pConnMeta {
  uint32_t qpn;
  union ibv_gid gid;
  uint8_t ibPort;
  uint8_t linkLayer;
  uint32_t lid;
  enum ibv_mtu mtu;
};

// P2P request — simplified from flagcxIbRequest
#define FLAGCX_P2P_MAX_REQUESTS 256
#define FLAGCX_P2P_REQ_UNUSED 0
#define FLAGCX_P2P_REQ_IPUT 1
#define FLAGCX_P2P_REQ_IGET 2
#define FLAGCX_P2P_BATCH_POLL_SIZE 32
#define FLAGCX_P2P_QPS_PER_CONN 4
#define FLAGCX_P2P_IGET_BATCH_MAX_WR 64
#define FLAGCX_P2P_READ_BATCH_WINDOW 8

// flagcxIbCreateQp configures each P2P QP with 2 * MAX_REQUESTS send WRs.
// The read engine round-robins an 8-batch window across four QPs, so one QP
// can hold at most two fixed-size READ batches.
static_assert(2 * FLAGCX_P2P_IGET_BATCH_MAX_WR <= 2 * MAX_REQUESTS,
              "P2P READ batch window exceeds QP send queue capacity");
static_assert(FLAGCX_P2P_READ_BATCH_WINDOW / FLAGCX_P2P_QPS_PER_CONN == 2,
              "P2P READ batch capacity assumes two batches per QP");

struct flagcxP2pRequest {
  int type;
  int events;                    // outstanding CQEs expected
  struct ibv_cq *cq;             // CQ to poll for this request
  struct flagcxP2pRequest *reqs; // back-pointer to owning reqs[] array
  std::atomic<uint8_t> *reqDone; // pointer to owning comm's reqDone[] array
};

struct flagcxP2pChannel {
  struct ibv_cq *cq;
  struct flagcxIbQp qp;
};

// P2P send comm — fixed QP/CQ channels, blocking connect
struct flagcxP2pSendComm {
  int ibDevN; // MUST be first field
  struct flagcxIbNetCommDevBase base;
  struct flagcxP2pChannel channels[FLAGCX_P2P_QPS_PER_CONN];
  struct flagcxSocket sock;
  struct flagcxP2pRequest reqs[FLAGCX_P2P_MAX_REQUESTS];
  uint64_t putSignalScratchpad;
  struct ibv_mr *putSignalScratchpadMr;
  std::atomic<uint8_t> reqDone[FLAGCX_P2P_MAX_REQUESTS];
  std::atomic<uint32_t> nextChannel{0};
  std::atomic<bool> cqError{false};
};

// P2P recv comm — symmetric with send comm so both sides can initiate transfers
struct flagcxP2pRecvComm {
  int ibDevN; // MUST be first field
  struct flagcxIbNetCommDevBase base;
  struct flagcxP2pChannel channels[FLAGCX_P2P_QPS_PER_CONN];
  struct flagcxSocket sock;
  struct flagcxP2pRequest reqs[FLAGCX_P2P_MAX_REQUESTS];
  uint64_t putSignalScratchpad;
  struct ibv_mr *putSignalScratchpadMr;
  std::atomic<uint8_t> reqDone[FLAGCX_P2P_MAX_REQUESTS];
  std::atomic<uint32_t> nextChannel{0};
  std::atomic<bool> cqError{false};
};

/* ------------------------------------------------------------------ */
/*  Globals                                                            */
/* ------------------------------------------------------------------ */

static struct flagcxP2pDevCtx flagcxP2pDevCtxs[MAX_IB_DEVS];
static int flagcxP2pInitialized = 0;
static pthread_mutex_t flagcxP2pInitLock = PTHREAD_MUTEX_INITIALIZER;

/* ------------------------------------------------------------------ */
/*  Background CQ Poller                                               */
/* ------------------------------------------------------------------ */

struct CqPollEntry {
  struct ibv_cq *cq;
  struct flagcxP2pRequest *reqs;
  std::atomic<uint8_t> *reqDone;
  std::atomic<bool> *cqError;
  bool active;
};

struct CqPoller {
  std::thread thread;
  std::mutex mutex;
  std::vector<CqPollEntry> entries;
  std::atomic<bool> running{false};
};

static CqPoller gCqPoller;

static void cqPollerFunc() {
  while (gCqPoller.running.load(std::memory_order_relaxed)) {
    bool anyWork = false;
    {
      // Keep unregister serialized with polling so CQ teardown cannot race a
      // snapshot that still contains the CQ.
      std::lock_guard<std::mutex> lock(gCqPoller.mutex);
      for (auto &entry : gCqPoller.entries) {
        if (!entry.active)
          continue;

        struct ibv_wc wcs[FLAGCX_P2P_BATCH_POLL_SIZE];
        int nCqe = 0;
        if (flagcxWrapIbvPollCq(entry.cq, FLAGCX_P2P_BATCH_POLL_SIZE, wcs,
                                &nCqe) != flagcxSuccess) {
          entry.cqError->store(true, std::memory_order_release);
          continue;
        }

        for (int i = 0; i < nCqe; i++) {
          if (wcs[i].status != IBV_WC_SUCCESS) {
            WARN("NET/IB_P2P : CQ poller got error status %d for wr_id %lu",
                 wcs[i].status, wcs[i].wr_id);
            entry.cqError->store(true, std::memory_order_release);
            break;
          }
          uint32_t idx = (uint32_t)wcs[i].wr_id;
          if (idx >= FLAGCX_P2P_MAX_REQUESTS)
            continue;

          entry.reqs[idx].events--;
          if (entry.reqs[idx].events == 0) {
            entry.reqs[idx].type = FLAGCX_P2P_REQ_UNUSED;
            entry.reqDone[idx].store(1, std::memory_order_release);
          }
        }
        if (nCqe > 0)
          anyWork = true;
      }
    }

    if (!anyWork) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }
}

static void ensureCqPollerStarted() {
  if (!gCqPoller.running.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(gCqPoller.mutex);
    if (!gCqPoller.running.load(std::memory_order_relaxed)) {
      gCqPoller.running.store(true, std::memory_order_release);
      gCqPoller.thread = std::thread(cqPollerFunc);
    }
  }
}

static void cqPollerRegister(struct ibv_cq *cq, struct flagcxP2pRequest *reqs,
                             std::atomic<uint8_t> *reqDone,
                             std::atomic<bool> *cqError) {
  ensureCqPollerStarted();
  std::lock_guard<std::mutex> lock(gCqPoller.mutex);
  gCqPoller.entries.push_back({cq, reqs, reqDone, cqError, true});
}

static void cqPollerStop() {
  if (gCqPoller.running.load(std::memory_order_acquire)) {
    gCqPoller.running.store(false, std::memory_order_release);
    if (gCqPoller.thread.joinable()) {
      gCqPoller.thread.join();
    }
    std::lock_guard<std::mutex> lock(gCqPoller.mutex);
    gCqPoller.entries.clear();
  }
}

static void cqPollerUnregister(struct ibv_cq *cq) {
  bool anyActive = false;
  {
    std::lock_guard<std::mutex> lock(gCqPoller.mutex);
    for (auto &entry : gCqPoller.entries) {
      if (entry.cq == cq) {
        entry.active = false;
      } else if (entry.active) {
        anyActive = true;
      }
    }
  }
  if (!anyActive) {
    cqPollerStop();
  }
}

/* ------------------------------------------------------------------ */
/*  Request helpers                                                    */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pGetRequest(struct flagcxP2pRequest *reqs,
                                          std::atomic<uint8_t> *reqDone,
                                          struct ibv_cq *cq, int type,
                                          struct flagcxP2pRequest **req) {
  for (int i = 0; i < FLAGCX_P2P_MAX_REQUESTS; i++) {
    if (reqs[i].type == FLAGCX_P2P_REQ_UNUSED) {
      reqs[i].type = type;
      reqs[i].events = 0;
      reqs[i].cq = cq;
      reqs[i].reqs = reqs;
      reqs[i].reqDone = reqDone;
      reqDone[i].store(0, std::memory_order_relaxed);
      *req = &reqs[i];
      return flagcxSuccess;
    }
  }
  WARN("NET/IB_P2P : unable to allocate request");
  *req = NULL;
  return flagcxInternalError;
}

static inline void flagcxP2pFreeRequest(struct flagcxP2pRequest *req) {
  req->type = FLAGCX_P2P_REQ_UNUSED;
}

/* ------------------------------------------------------------------ */
/*  Init / Devices / Properties                                        */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pInit() {
  pthread_mutex_lock(&flagcxP2pInitLock);
  if (flagcxP2pInitialized) {
    pthread_mutex_unlock(&flagcxP2pInitLock);
    return flagcxSuccess;
  }

  // Reuse IBRC device discovery (idempotent)
  FLAGCXCHECK(flagcxIbInit());

  // Eagerly allocate PD for each physical IB device
  for (int i = 0; i < flagcxNIbDevs; i++) {
    flagcxP2pDevCtxs[i].ibDevN = i;
    struct flagcxIbDev *ibDev = flagcxIbDevs + i;
    pthread_mutex_lock(&ibDev->lock);
    if (0 == ibDev->pdRefs++) {
      flagcxResult_t res;
      FLAGCXCHECKGOTO(flagcxWrapIbvAllocPd(&ibDev->pd, ibDev->context), res,
                      pd_fail);
      if (0) {
      pd_fail:
        ibDev->pdRefs--;
        pthread_mutex_unlock(&ibDev->lock);
        pthread_mutex_unlock(&flagcxP2pInitLock);
        return res;
      }
    }
    flagcxP2pDevCtxs[i].pd = ibDev->pd;
    pthread_mutex_unlock(&ibDev->lock);
  }

  flagcxP2pInitialized = 1;
  INFO(FLAGCX_INIT | FLAGCX_NET,
       "NET/IB_P2P : P2P adaptor initialized, %d devices, eager PD allocated",
       flagcxNIbDevs);
  pthread_mutex_unlock(&flagcxP2pInitLock);
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pDevices(int *ndev) {
  *ndev = flagcxNMergedIbDevs;
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pGetProperties(int dev, void *props) {
  return flagcxIbGetProperties(dev, props);
}

/* ------------------------------------------------------------------ */
/*  Memory Registration                                                */
/* ------------------------------------------------------------------ */

// Resolve ibDevN from a comm pointer. The comm may be:
//   - flagcxP2pDevCtx*  (from P2P engine, before any connection)
//   - flagcxP2pSendComm* or flagcxP2pRecvComm* (after connection)
// All have ibDevN as their first field.
static inline int flagcxP2pGetIbDevN(void *comm) { return *(int *)comm; }

static flagcxResult_t flagcxP2pRegMrDmaBuf(void *comm, void *data, size_t size,
                                           int type, uint64_t offset, int fd,
                                           int mrFlags, void **mhandle) {
  assert(size > 0);
  assert(comm != NULL);

  int ibDevN = flagcxP2pGetIbDevN(comm);
  struct flagcxIbDev *ibDev = flagcxIbDevs + ibDevN;

  // Build a temporary flagcxIbNetCommDevBase for the internal registration call
  struct flagcxIbNetCommDevBase devBase;
  memset(&devBase, 0, sizeof(devBase));
  devBase.ibDevN = ibDevN;
  devBase.pd = ibDev->pd;

  struct flagcxP2pMrHandle *handle =
      (struct flagcxP2pMrHandle *)malloc(sizeof(struct flagcxP2pMrHandle));
  if (!handle) {
    WARN("NET/IB_P2P : failed to allocate MR handle");
    return flagcxInternalError;
  }

  ibv_mr *mr = NULL;
  FLAGCXCHECK(flagcxIbRegMrDmaBufInternal(&devBase, data, size, type, offset,
                                          fd, mrFlags, &mr));

  handle->baseVa = (uintptr_t)data;
  handle->lkey = mr->lkey;
  handle->rkey = mr->rkey;
  handle->mr = mr;
  handle->ibDevN = ibDevN;

  *mhandle = (void *)handle;
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pRegMr(void *comm, void *data, size_t size,
                                     int type, int mrFlags, void **mhandle) {
  return flagcxP2pRegMrDmaBuf(comm, data, size, type, 0ULL, -1, mrFlags,
                              mhandle);
}

static flagcxResult_t flagcxP2pDeregMr(void *comm, void *mhandle) {
  struct flagcxP2pMrHandle *handle = (struct flagcxP2pMrHandle *)mhandle;

  // Build a temporary devBase for the internal deregistration call
  struct flagcxIbNetCommDevBase devBase;
  memset(&devBase, 0, sizeof(devBase));
  devBase.ibDevN = handle->ibDevN;
  devBase.pd = flagcxIbDevs[handle->ibDevN].pd;

  FLAGCXCHECK(flagcxIbDeregMrInternal(&devBase, handle->mr));
  free(handle);
  return flagcxSuccess;
}

/* ------------------------------------------------------------------ */
/*  Listen / Connect / Accept                                          */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pListen(int dev, void *opaqueHandle,
                                      void **listenComm) {
  struct flagcxP2pListenComm *comm;
  FLAGCXCHECK(flagcxCalloc(&comm, 1));
  struct flagcxP2pListenHandle *handle =
      (struct flagcxP2pListenHandle *)opaqueHandle;
  memset(handle, 0, sizeof(struct flagcxP2pListenHandle));
  comm->dev = dev;
  handle->magic = FLAGCX_SOCKET_MAGIC;
  FLAGCXCHECK(flagcxSocketInit(&comm->sock, &flagcxIbIfAddr, handle->magic,
                               flagcxSocketTypeNetIb, NULL, 1));
  FLAGCXCHECK(flagcxSocketListen(&comm->sock));
  FLAGCXCHECK(flagcxSocketGetAddr(&comm->sock, &handle->connectAddr));
  *listenComm = comm;
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pReleasePd(int ibDevN);
static void flagcxP2pDrainCq(struct ibv_cq *cq);

// Helper: set up PD (from eager init), CQs, QPs, and GID for a connection
static flagcxResult_t flagcxP2pSetupConn(int dev,
                                         struct flagcxIbNetCommDevBase *base,
                                         struct flagcxP2pChannel *channels,
                                         int *outIbDevN) {
  struct flagcxIbMergedDev *mergedDev = flagcxIbMergedDevs + dev;
  int ibDevN = mergedDev->devs[0]; // v1: single physical NIC
  *outIbDevN = ibDevN;

  struct flagcxIbDev *ibDev = flagcxIbDevs + ibDevN;
  base->ibDevN = ibDevN;

  // Reuse PD from eager init, increment refcount
  pthread_mutex_lock(&ibDev->lock);
  ibDev->pdRefs++;
  base->pd = ibDev->pd;
  pthread_mutex_unlock(&ibDev->lock);

  int accessFlags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_ATOMIC;

  // Get GID info
  flagcxResult_t res;
  FLAGCXCHECKGOTO(flagcxIbGetGidIndex(ibDev->context, ibDev->portNum,
                                      ibDev->portAttr.gid_tbl_len,
                                      &base->gidInfo.localGidIndex),
                  res, setup_fail);
  FLAGCXCHECKGOTO(flagcxWrapIbvQueryGid(ibDev->context, ibDev->portNum,
                                        base->gidInfo.localGidIndex,
                                        &base->gidInfo.localGid),
                  res, setup_fail);
  base->gidInfo.linkLayer = ibDev->link;

  // Create RC QPs with remote write, read, and atomic access.
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++) {
    FLAGCXCHECKGOTO(flagcxWrapIbvCreateCq(&channels[i].cq, ibDev->context,
                                          2 * FLAGCX_P2P_MAX_REQUESTS, NULL,
                                          NULL, 0),
                    res, setup_fail);
    base->cq = channels[i].cq;
    FLAGCXCHECKGOTO(
        flagcxIbCreateQp(ibDev->portNum, base, accessFlags, &channels[i].qp),
        res, setup_fail);
    channels[i].qp.devIndex = 0;
  }

  return flagcxSuccess;

setup_fail:
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++) {
    if (channels[i].qp.qp) {
      flagcxWrapIbvDestroyQp(channels[i].qp.qp);
      channels[i].qp.qp = NULL;
    }
    if (channels[i].cq) {
      flagcxWrapIbvDestroyCq(channels[i].cq);
      channels[i].cq = NULL;
    }
  }
  base->cq = NULL;
  flagcxP2pReleasePd(ibDevN);
  base->pd = NULL;
  return res;
}

// Helper: build local connection metadata
static void flagcxP2pBuildConnMeta(struct flagcxP2pConnMeta *meta,
                                   struct flagcxIbNetCommDevBase *base,
                                   struct flagcxIbQp *qp, int ibDevN) {
  struct flagcxIbDev *ibDev = flagcxIbDevs + ibDevN;
  memset(meta, 0, sizeof(*meta));
  meta->qpn = qp->qp->qp_num;
  meta->gid = base->gidInfo.localGid;
  meta->ibPort = ibDev->portNum;
  meta->linkLayer = ibDev->link;
  meta->lid = ibDev->portAttr.lid;
  meta->mtu = ibDev->portAttr.active_mtu;
}

// Helper: transition QP to RTR+RTS using remote metadata
static flagcxResult_t
flagcxP2pTransitionQp(struct flagcxIbQp *qp,
                      struct flagcxIbNetCommDevBase *base,
                      struct flagcxP2pConnMeta *remoteMeta, int ibDevN) {
  struct flagcxIbDev *ibDev = flagcxIbDevs + ibDevN;

  // Clamp MTU to min(remote, local) — same as IBRC accept path
  enum ibv_mtu mtu = (enum ibv_mtu)std::min((int)remoteMeta->mtu,
                                            (int)ibDev->portAttr.active_mtu);

  struct flagcxIbDevInfo remoteInfo;
  memset(&remoteInfo, 0, sizeof(remoteInfo));
  remoteInfo.lid = remoteMeta->lid;
  remoteInfo.ibPort = remoteMeta->ibPort;
  remoteInfo.linkLayer = remoteMeta->linkLayer;
  remoteInfo.mtu = mtu;
  remoteInfo.spn = remoteMeta->gid.global.subnet_prefix;
  remoteInfo.iid = remoteMeta->gid.global.interface_id;

  FLAGCXCHECK(flagcxIbRtrQp(qp->qp, base->gidInfo.localGidIndex,
                            remoteMeta->qpn, &remoteInfo));
  FLAGCXCHECK(flagcxIbRtsQp(qp->qp));
  return flagcxSuccess;
}

static void flagcxP2pRegisterChannels(struct flagcxP2pChannel *channels,
                                      struct flagcxP2pRequest *reqs,
                                      std::atomic<uint8_t> *reqDone,
                                      std::atomic<bool> *cqError) {
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    cqPollerRegister(channels[i].cq, reqs, reqDone, cqError);
}

static void flagcxP2pUnregisterChannels(struct flagcxP2pChannel *channels) {
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    if (channels[i].cq)
      cqPollerUnregister(channels[i].cq);
}

static flagcxResult_t
flagcxP2pDestroyChannels(struct flagcxP2pChannel *channels) {
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    flagcxP2pDrainCq(channels[i].cq);
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++) {
    if (channels[i].qp.qp) {
      FLAGCXCHECK(flagcxWrapIbvDestroyQp(channels[i].qp.qp));
      channels[i].qp.qp = NULL;
    }
  }
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++) {
    if (channels[i].cq) {
      FLAGCXCHECK(flagcxWrapIbvDestroyCq(channels[i].cq));
      channels[i].cq = NULL;
    }
  }
  return flagcxSuccess;
}

static inline struct flagcxP2pChannel *
flagcxP2pNextChannel(struct flagcxP2pChannel *channels,
                     std::atomic<uint32_t> *nextChannel) {
  uint32_t idx = nextChannel->fetch_add(1, std::memory_order_relaxed);
  return channels + (idx % FLAGCX_P2P_QPS_PER_CONN);
}

static flagcxResult_t flagcxP2pConnect(int dev, void *opaqueHandle,
                                       void **sendComm) {
  struct flagcxP2pListenHandle *handle =
      (struct flagcxP2pListenHandle *)opaqueHandle;
  flagcxResult_t res;
  *sendComm = NULL;

  // Allocate send comm
  struct flagcxP2pSendComm *comm;
  FLAGCXCHECK(flagcxCalloc(&comm, 1));
  int ready = 0;
  auto connectStart = std::chrono::steady_clock::time_point();
  struct flagcxP2pConnMeta localMeta[FLAGCX_P2P_QPS_PER_CONN];
  struct flagcxP2pConnMeta remoteMeta[FLAGCX_P2P_QPS_PER_CONN];
  int localReady = 1, remoteReady = 0;

  // TCP connect (blocking with timeout)
  FLAGCXCHECKGOTO(flagcxSocketInit(&comm->sock, &handle->connectAddr,
                                   handle->magic, flagcxSocketTypeNetIb, NULL,
                                   1),
                  res, connect_fail);
  FLAGCXCHECKGOTO(flagcxSocketConnect(&comm->sock), res, connect_fail);
  connectStart = std::chrono::steady_clock::now();
  while (!ready) {
    FLAGCXCHECKGOTO(flagcxSocketReady(&comm->sock, &ready), res, connect_fail);
    if (!ready) {
      if (std::chrono::steady_clock::now() - connectStart >
          std::chrono::seconds(30)) {
        WARN("NET/IB_P2P : connect socket ready timed out after 30s");
        res = flagcxSystemError;
        goto connect_fail;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // Set up PD, CQs, QPs
  FLAGCXCHECKGOTO(
      flagcxP2pSetupConn(dev, &comm->base, comm->channels, &comm->ibDevN), res,
      connect_fail);

  // Exchange connection metadata
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    flagcxP2pBuildConnMeta(&localMeta[i], &comm->base, &comm->channels[i].qp,
                           comm->ibDevN);
  FLAGCXCHECKGOTO(flagcxSocketSend(&comm->sock, localMeta, sizeof(localMeta)),
                  res, connect_fail);
  FLAGCXCHECKGOTO(flagcxSocketRecv(&comm->sock, remoteMeta, sizeof(remoteMeta)),
                  res, connect_fail);

  // Transition each matched QP to RTR then RTS.
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    FLAGCXCHECKGOTO(flagcxP2pTransitionQp(&comm->channels[i].qp, &comm->base,
                                          &remoteMeta[i], comm->ibDevN),
                    res, connect_fail);

  // Register putSignal scratchpad MR
  comm->putSignalScratchpad = 0;
  FLAGCXCHECKGOTO(
      flagcxWrapIbvRegMr(&comm->putSignalScratchpadMr, comm->base.pd,
                         &comm->putSignalScratchpad,
                         sizeof(comm->putSignalScratchpad),
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
      res, connect_fail);

  // Exchange ready
  FLAGCXCHECKGOTO(
      flagcxSocketSend(&comm->sock, &localReady, sizeof(localReady)), res,
      connect_fail);
  FLAGCXCHECKGOTO(
      flagcxSocketRecv(&comm->sock, &remoteReady, sizeof(remoteReady)), res,
      connect_fail);

  flagcxP2pRegisterChannels(comm->channels, comm->reqs, comm->reqDone,
                            &comm->cqError);

  *sendComm = comm;
  return flagcxSuccess;

connect_fail:
  if (comm->putSignalScratchpadMr)
    flagcxWrapIbvDeregMr(comm->putSignalScratchpadMr);
  flagcxP2pDestroyChannels(comm->channels);
  if (comm->base.pd)
    flagcxP2pReleasePd(comm->ibDevN);
  flagcxSocketClose(&comm->sock);
  free(comm);
  return res;
}

static flagcxResult_t flagcxP2pAccept(void *listenComm, void **recvComm) {
  struct flagcxP2pListenComm *lComm = (struct flagcxP2pListenComm *)listenComm;
  *recvComm = NULL;

  // Allocate recv comm
  struct flagcxP2pRecvComm *comm;
  FLAGCXCHECK(flagcxCalloc(&comm, 1));

  // TCP accept (blocking, no timeout)
  flagcxResult_t res;
  int ready;
  struct flagcxP2pConnMeta localMeta[FLAGCX_P2P_QPS_PER_CONN];
  struct flagcxP2pConnMeta remoteMeta[FLAGCX_P2P_QPS_PER_CONN];
  int localReady = 1, remoteReady = 0;
  FLAGCXCHECKGOTO(flagcxSocketInit(&comm->sock), res, accept_fail);
  FLAGCXCHECKGOTO(flagcxSocketAccept(&comm->sock, &lComm->sock), res,
                  accept_fail);
  ready = 0;
  while (!ready) {
    FLAGCXCHECKGOTO(flagcxSocketReady(&comm->sock, &ready), res, accept_fail);
    if (!ready) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  if (0) {
  accept_fail:
    free(comm);
    return res;
  }

  // Set up PD, CQs, QPs
  FLAGCXCHECKGOTO(flagcxP2pSetupConn(lComm->dev, &comm->base, comm->channels,
                                     &comm->ibDevN),
                  res, accept_cleanup);

  // Exchange connection metadata (accept receives first, then sends)
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    flagcxP2pBuildConnMeta(&localMeta[i], &comm->base, &comm->channels[i].qp,
                           comm->ibDevN);
  FLAGCXCHECKGOTO(flagcxSocketRecv(&comm->sock, remoteMeta, sizeof(remoteMeta)),
                  res, accept_cleanup);
  FLAGCXCHECKGOTO(flagcxSocketSend(&comm->sock, localMeta, sizeof(localMeta)),
                  res, accept_cleanup);

  // Transition each matched QP to RTR then RTS.
  for (int i = 0; i < FLAGCX_P2P_QPS_PER_CONN; i++)
    FLAGCXCHECKGOTO(flagcxP2pTransitionQp(&comm->channels[i].qp, &comm->base,
                                          &remoteMeta[i], comm->ibDevN),
                    res, accept_cleanup);

  // Register putSignal scratchpad MR (symmetric with connect)
  comm->putSignalScratchpad = 0;
  FLAGCXCHECKGOTO(
      flagcxWrapIbvRegMr(&comm->putSignalScratchpadMr, comm->base.pd,
                         &comm->putSignalScratchpad,
                         sizeof(comm->putSignalScratchpad),
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC),
      res, accept_cleanup);

  // Exchange ready
  FLAGCXCHECKGOTO(
      flagcxSocketRecv(&comm->sock, &remoteReady, sizeof(remoteReady)), res,
      accept_cleanup);
  FLAGCXCHECKGOTO(
      flagcxSocketSend(&comm->sock, &localReady, sizeof(localReady)), res,
      accept_cleanup);

  flagcxP2pRegisterChannels(comm->channels, comm->reqs, comm->reqDone,
                            &comm->cqError);

  *recvComm = comm;
  return flagcxSuccess;

accept_cleanup:
  if (comm->putSignalScratchpadMr)
    flagcxWrapIbvDeregMr(comm->putSignalScratchpadMr);
  flagcxP2pDestroyChannels(comm->channels);
  if (comm->base.pd)
    flagcxP2pReleasePd(comm->ibDevN);
  flagcxSocketClose(&comm->sock);
  free(comm);
  return res;
}

/* ------------------------------------------------------------------ */
/*  One-sided transfers: iput / iget / iputSignal                      */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pIput(void *sendComm, uint64_t srcOff,
                                    uint64_t dstOff, size_t size, int srcRank,
                                    int dstRank, void **srcHandles,
                                    void **dstHandles, void **request) {
  struct flagcxP2pSendComm *comm = (struct flagcxP2pSendComm *)sendComm;
  struct flagcxP2pMrHandle *src = (struct flagcxP2pMrHandle *)srcHandles;
  struct flagcxP2pMrHandle *dst = (struct flagcxP2pMrHandle *)dstHandles;

  struct flagcxP2pRequest *req;
  FLAGCXCHECK(flagcxP2pGetRequest(comm->reqs, comm->reqDone, NULL,
                                  FLAGCX_P2P_REQ_IPUT, &req));
  struct flagcxP2pChannel *channel =
      flagcxP2pNextChannel(comm->channels, &comm->nextChannel);
  req->cq = channel->cq;

  struct ibv_sge sge;
  memset(&sge, 0, sizeof(sge));
  sge.addr = src->baseVa + srcOff;
  sge.length = (uint32_t)size;
  if ((size_t)sge.length != size) {
    WARN("NET/IB_P2P : iput size %zu exceeds 32-bit limit", size);
    flagcxP2pFreeRequest(req);
    return flagcxInternalError;
  }
  sge.lkey = src->lkey;

  struct ibv_send_wr wr;
  memset(&wr, 0, sizeof(wr));
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = req - comm->reqs;
  wr.wr.rdma.remote_addr = dst->baseVa + dstOff;
  wr.wr.rdma.rkey = dst->rkey;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  req->events = 1;
  struct ibv_send_wr *bad_wr;
  flagcxResult_t res = flagcxWrapIbvPostSend(channel->qp.qp, &wr, &bad_wr);
  if (res != flagcxSuccess) {
    flagcxP2pFreeRequest(req);
    return res;
  }

  *request = req;
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pIget(void *sendComm, uint64_t srcOff,
                                    uint64_t dstOff, size_t size, int srcRank,
                                    int dstRank, void **srcHandles,
                                    void **dstHandles, void **request) {
  struct flagcxP2pSendComm *comm = (struct flagcxP2pSendComm *)sendComm;
  struct flagcxP2pMrHandle *src = (struct flagcxP2pMrHandle *)srcHandles;
  struct flagcxP2pMrHandle *dst = (struct flagcxP2pMrHandle *)dstHandles;

  struct flagcxP2pRequest *req;
  FLAGCXCHECK(flagcxP2pGetRequest(comm->reqs, comm->reqDone, NULL,
                                  FLAGCX_P2P_REQ_IGET, &req));
  struct flagcxP2pChannel *channel =
      flagcxP2pNextChannel(comm->channels, &comm->nextChannel);
  req->cq = channel->cq;

  struct ibv_sge sge;
  memset(&sge, 0, sizeof(sge));
  sge.addr = dst->baseVa + dstOff;
  sge.length = (uint32_t)size;
  if ((size_t)sge.length != size) {
    WARN("NET/IB_P2P : iget size %zu exceeds 32-bit limit", size);
    flagcxP2pFreeRequest(req);
    return flagcxInternalError;
  }
  sge.lkey = dst->lkey;

  struct ibv_send_wr wr;
  memset(&wr, 0, sizeof(wr));
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr_id = req - comm->reqs;
  wr.wr.rdma.remote_addr = src->baseVa + srcOff;
  wr.wr.rdma.rkey = src->rkey;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  req->events = 1;
  struct ibv_send_wr *bad_wr;
  flagcxResult_t res = flagcxWrapIbvPostSend(channel->qp.qp, &wr, &bad_wr);
  if (res != flagcxSuccess) {
    flagcxP2pFreeRequest(req);
    return res;
  }

  *request = req;
  return flagcxSuccess;
}

static flagcxResult_t
flagcxP2pIgetBatch(void *sendComm, int count, const uint64_t *srcOffs,
                   const uint64_t *dstOffs, const size_t *sizes, int srcRank,
                   int dstRank, void *const *srcHandles,
                   void *const *dstHandles, void **request) {
  struct flagcxP2pSendComm *comm = (struct flagcxP2pSendComm *)sendComm;
  if (count <= 0 || count > FLAGCX_P2P_IGET_BATCH_MAX_WR || srcOffs == NULL ||
      dstOffs == NULL || sizes == NULL || srcHandles == NULL ||
      dstHandles == NULL || request == NULL) {
    WARN("NET/IB_P2P : invalid igetBatch arguments, count %d", count);
    return flagcxInternalError;
  }

  struct flagcxP2pRequest *req;
  FLAGCXCHECK(flagcxP2pGetRequest(comm->reqs, comm->reqDone, NULL,
                                  FLAGCX_P2P_REQ_IGET, &req));
  struct flagcxP2pChannel *channel =
      flagcxP2pNextChannel(comm->channels, &comm->nextChannel);
  req->cq = channel->cq;

  struct ibv_send_wr wrs[FLAGCX_P2P_IGET_BATCH_MAX_WR];
  struct ibv_sge sges[FLAGCX_P2P_IGET_BATCH_MAX_WR];
  memset(wrs, 0, sizeof(wrs));
  memset(sges, 0, sizeof(sges));

  for (int i = 0; i < count; i++) {
    if (srcHandles[i] == NULL || dstHandles[i] == NULL) {
      WARN("NET/IB_P2P : igetBatch handle %d is NULL", i);
      flagcxP2pFreeRequest(req);
      return flagcxInternalError;
    }

    struct flagcxP2pMrHandle *src = (struct flagcxP2pMrHandle *)srcHandles[i];
    struct flagcxP2pMrHandle *dst = (struct flagcxP2pMrHandle *)dstHandles[i];

    sges[i].addr = dst->baseVa + dstOffs[i];
    sges[i].length = (uint32_t)sizes[i];
    if ((size_t)sges[i].length != sizes[i]) {
      WARN("NET/IB_P2P : igetBatch size %zu exceeds 32-bit limit", sizes[i]);
      flagcxP2pFreeRequest(req);
      return flagcxInternalError;
    }
    sges[i].lkey = dst->lkey;

    wrs[i].opcode = IBV_WR_RDMA_READ;
    wrs[i].send_flags =
        i == count - 1 ? IBV_SEND_SIGNALED : 0; // final CQE tracks batch
    wrs[i].wr_id = i == count - 1 ? req - comm->reqs : 0;
    wrs[i].wr.rdma.remote_addr = src->baseVa + srcOffs[i];
    wrs[i].wr.rdma.rkey = src->rkey;
    wrs[i].sg_list = &sges[i];
    wrs[i].num_sge = 1;
    wrs[i].next = i + 1 < count ? &wrs[i + 1] : NULL;
  }

  req->events = 1;
  struct ibv_send_wr *bad_wr;
  flagcxResult_t res = flagcxWrapIbvPostSend(channel->qp.qp, &wrs[0], &bad_wr);
  if (res != flagcxSuccess) {
    flagcxP2pFreeRequest(req);
    return res;
  }

  *request = req;
  return flagcxSuccess;
}

static flagcxResult_t
flagcxP2pIputSignal(void *sendComm, uint64_t srcOff, uint64_t dstOff,
                    size_t size, int srcRank, int dstRank, void **srcHandles,
                    void **dstHandles, uint64_t signalOff, void **signalHandles,
                    uint64_t signalValue, void **request) {
  struct flagcxP2pSendComm *comm = (struct flagcxP2pSendComm *)sendComm;
  struct flagcxP2pMrHandle *signalInfo =
      (struct flagcxP2pMrHandle *)signalHandles;

  struct flagcxP2pRequest *req;
  FLAGCXCHECK(flagcxP2pGetRequest(comm->reqs, comm->reqDone, NULL,
                                  FLAGCX_P2P_REQ_IPUT, &req));
  struct flagcxP2pChannel *channel =
      flagcxP2pNextChannel(comm->channels, &comm->nextChannel);
  req->cq = channel->cq;

  bool chainData = (size > 0 && srcHandles != NULL && dstHandles != NULL);

  struct ibv_sge sge[2];
  struct ibv_send_wr wr[2];
  memset(sge, 0, sizeof(sge));
  memset(wr, 0, sizeof(wr));

  // wr[0]: RDMA WRITE for data (unsignaled, chained to wr[1])
  if (chainData) {
    struct flagcxP2pMrHandle *src = (struct flagcxP2pMrHandle *)srcHandles;
    struct flagcxP2pMrHandle *dst = (struct flagcxP2pMrHandle *)dstHandles;

    sge[0].addr = src->baseVa + srcOff;
    sge[0].length = (uint32_t)size;
    if ((size_t)sge[0].length != size) {
      WARN("NET/IB_P2P : iputSignal size %zu exceeds 32-bit limit", size);
      flagcxP2pFreeRequest(req);
      return flagcxInternalError;
    }
    sge[0].lkey = src->lkey;

    wr[0].opcode = IBV_WR_RDMA_WRITE;
    wr[0].send_flags = 0; // unsignaled
    wr[0].wr.rdma.remote_addr = dst->baseVa + dstOff;
    wr[0].wr.rdma.rkey = dst->rkey;
    wr[0].sg_list = &sge[0];
    wr[0].num_sge = 1;
    wr[0].next = &wr[1]; // chain to atomic
  }

  // wr[1]: ATOMIC FETCH_AND_ADD for signal (signaled)
  sge[1].addr = (uintptr_t)&comm->putSignalScratchpad;
  sge[1].length = sizeof(comm->putSignalScratchpad);
  sge[1].lkey = comm->putSignalScratchpadMr->lkey;

  wr[1].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  wr[1].send_flags = IBV_SEND_SIGNALED;
  wr[1].wr_id = req - comm->reqs;
  wr[1].wr.atomic.remote_addr = signalInfo->baseVa + signalOff;
  wr[1].wr.atomic.rkey = signalInfo->rkey;
  wr[1].wr.atomic.compare_add = signalValue;
  wr[1].sg_list = &sge[1];
  wr[1].num_sge = 1;
  wr[1].next = NULL;

  req->events = 1;
  struct ibv_send_wr *bad_wr;
  flagcxResult_t res = flagcxWrapIbvPostSend(
      channel->qp.qp, chainData ? &wr[0] : &wr[1], &bad_wr);
  if (res != flagcxSuccess) {
    flagcxP2pFreeRequest(req);
    return res;
  }

  *request = req;
  return flagcxSuccess;
}

/* ------------------------------------------------------------------ */
/*  Test                                                               */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pTest(void *request, int *done, int *sizes) {
  *done = 0;
  struct flagcxP2pRequest *req = (struct flagcxP2pRequest *)request;
  if (req == NULL || req->type == FLAGCX_P2P_REQ_UNUSED) {
    *done = 1;
    return flagcxSuccess;
  }

  uint32_t idx = (uint32_t)(req - req->reqs);
  if (idx >= FLAGCX_P2P_MAX_REQUESTS) {
    WARN("NET/IB_P2P : invalid request index %u in test()", idx);
    return flagcxInternalError;
  }

  if (req->reqDone[idx].load(std::memory_order_acquire)) {
    req->reqDone[idx].store(0, std::memory_order_relaxed);
    *done = 1;
    if (sizes)
      *sizes = 0;
  }
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pTestBatch(void **requests, int nRequests,
                                         int *doneFlags, int *doneCount) {
  int completed = 0;
  for (int i = 0; i < nRequests; i++) {
    doneFlags[i] = 0;
    struct flagcxP2pRequest *req = (struct flagcxP2pRequest *)requests[i];
    if (req == NULL || req->type == FLAGCX_P2P_REQ_UNUSED) {
      doneFlags[i] = 1;
      completed++;
      continue;
    }

    uint32_t idx = (uint32_t)(req - req->reqs);
    if (idx >= FLAGCX_P2P_MAX_REQUESTS)
      continue;

    if (req->reqDone[idx].load(std::memory_order_acquire)) {
      req->reqDone[idx].store(0, std::memory_order_relaxed);
      doneFlags[i] = 1;
      completed++;
    }
  }
  *doneCount = completed;
  return flagcxSuccess;
}

/* ------------------------------------------------------------------ */
/*  Close                                                              */
/* ------------------------------------------------------------------ */

// Helper: decrement PD refcount, dealloc if last ref
static flagcxResult_t flagcxP2pReleasePd(int ibDevN) {
  struct flagcxIbDev *ibDev = flagcxIbDevs + ibDevN;
  pthread_mutex_lock(&ibDev->lock);
  if (0 == --ibDev->pdRefs) {
    flagcxResult_t res = flagcxWrapIbvDeallocPd(ibDev->pd);
    pthread_mutex_unlock(&ibDev->lock);
    if (res != flagcxSuccess) {
      INFO(FLAGCX_ALL,
           "NET/IB_P2P : Failed to deallocate PD (non-fatal, may have "
           "remaining resources)");
    }
    return flagcxSuccess;
  }
  pthread_mutex_unlock(&ibDev->lock);
  return flagcxSuccess;
}

// Helper: drain CQ before destroying resources
static void flagcxP2pDrainCq(struct ibv_cq *cq) {
  if (!cq)
    return;
  struct ibv_wc wcs[64];
  int nCqe = 0;
  for (int i = 0; i < 16; i++) {
    flagcxWrapIbvPollCq(cq, 64, wcs, &nCqe);
    if (nCqe == 0)
      break;
  }
}

static flagcxResult_t flagcxP2pCloseSend(void *sendComm) {
  struct flagcxP2pSendComm *comm = (struct flagcxP2pSendComm *)sendComm;
  if (comm) {
    flagcxP2pUnregisterChannels(comm->channels);
    FLAGCXCHECK(flagcxP2pDestroyChannels(comm->channels));
    if (comm->putSignalScratchpadMr)
      FLAGCXCHECK(flagcxWrapIbvDeregMr(comm->putSignalScratchpadMr));
    FLAGCXCHECK(flagcxP2pReleasePd(comm->ibDevN));
    FLAGCXCHECK(flagcxSocketClose(&comm->sock));
    free(comm);
  }
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pCloseRecv(void *recvComm) {
  struct flagcxP2pRecvComm *comm = (struct flagcxP2pRecvComm *)recvComm;
  if (comm) {
    flagcxP2pUnregisterChannels(comm->channels);
    FLAGCXCHECK(flagcxP2pDestroyChannels(comm->channels));
    if (comm->putSignalScratchpadMr)
      FLAGCXCHECK(flagcxWrapIbvDeregMr(comm->putSignalScratchpadMr));
    FLAGCXCHECK(flagcxP2pReleasePd(comm->ibDevN));
    FLAGCXCHECK(flagcxSocketClose(&comm->sock));
    free(comm);
  }
  return flagcxSuccess;
}

static flagcxResult_t flagcxP2pCloseListen(void *listenComm) {
  struct flagcxP2pListenComm *comm = (struct flagcxP2pListenComm *)listenComm;
  if (comm) {
    FLAGCXCHECK(flagcxSocketClose(&comm->sock));
    free(comm);
  }
  return flagcxSuccess;
}

/* ------------------------------------------------------------------ */
/*  Two-sided stubs (not supported by P2P adaptor)                     */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pIsend(void *, void *, size_t, int, void *,
                                     void *, void **) {
  WARN("NET/IB_P2P : isend not supported");
  return flagcxInternalError;
}

static flagcxResult_t flagcxP2pIrecv(void *, int, void **, size_t *, int *,
                                     void **, void **, void **) {
  WARN("NET/IB_P2P : irecv not supported");
  return flagcxInternalError;
}

static flagcxResult_t flagcxP2pIflush(void *, int, void **, int *, void **,
                                      void **) {
  WARN("NET/IB_P2P : iflush not supported");
  return flagcxInternalError;
}

/* ------------------------------------------------------------------ */
/*  Device name lookup                                                 */
/* ------------------------------------------------------------------ */

static flagcxResult_t flagcxP2pGetDevFromName(char *name, int *dev) {
  for (int i = 0; i < flagcxNMergedIbDevs; i++) {
    if (strcmp(flagcxIbMergedDevs[i].devName, name) == 0) {
      *dev = i;
      return flagcxSuccess;
    }
  }
  WARN("NET/IB_P2P : device %s not found", name);
  return flagcxInternalError;
}

/* ------------------------------------------------------------------ */
/*  Adaptor struct                                                     */
/* ------------------------------------------------------------------ */

struct flagcxNetAdaptor flagcxNetIbP2p = {
    // Basic functions
    "IB_P2P",
    flagcxP2pInit,
    flagcxP2pDevices,
    flagcxP2pGetProperties,

    // Setup functions
    flagcxP2pListen,
    flagcxP2pConnect,
    flagcxP2pAccept,
    flagcxP2pCloseSend,
    flagcxP2pCloseRecv,
    flagcxP2pCloseListen,

    // Memory region functions
    flagcxP2pRegMr,
    flagcxP2pRegMrDmaBuf,
    flagcxP2pDeregMr,

    // Two-sided functions (stubs)
    flagcxP2pIsend,
    flagcxP2pIrecv,
    flagcxP2pIflush,
    flagcxP2pTest,

    // One-sided functions
    flagcxP2pIput,
    flagcxP2pIget,
    flagcxP2pIputSignal,

    // Device name lookup
    flagcxP2pGetDevFromName,

    // Optional batch operations
    nullptr,            // iputBatch
    flagcxP2pTestBatch, // testBatch
    flagcxP2pIgetBatch, // igetBatch
};
