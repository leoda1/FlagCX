#!/bin/bash
# Real-inference KV transfer benchmark on PPU (FlagCX p2p accl backend).
#
# Replays the two most frequent transfer patterns from a real prefill log
# (167.788 MiB / 1950 WRs and 1464.328 MiB / 1014 WRs, non-uniform lengths).
# Cross-machine, one direction (mirrors prefill->decode KV transfer):
#
#   client machine                          server machine
#   GPU0 -> server GPU0   (zmq 4566)
#   GPU1 -> server GPU1   (zmq 4576)
#
# Single NIC vsolar_0 on both sides. Same-machine pairs are NOT supported:
# the EIC connection table rejects self-connected CID paths (Send CID HW
# Error / send retry exhausted, observed 2026-09-23).
#
# Usage:
#   server machine: bash run_real_bench.sh server <own-eth0-ip> [iters]
#   client machine: bash run_real_bench.sh client <server-eth0-ip> [iters]
#
# All planes (zmq, FlagCX rpc/hello, barex unicm) MUST stay on the same
# interface (eth0 / 22.2.x RDMA net): the EIC connection table is keyed by
# the address the hello exchange advertises; mixing net0 (hello) with eth0
# (unicm bind) yields "Send CID HW Error" on first WR.
set -u
cd "$(dirname "$0")"
ROLE=${1:?server|client}
ADDR=${2:?address}
ITERS=${3:-2000}
LOG=claude-trash
mkdir -p "$LOG"

# stale processes from an aborted run hold the zmq ports / GPU memory
for pid in $(pgrep -f kv_transfer_benchmark_noncontig.py); do
  kill "$pid" 2>/dev/null
done
sleep 1

FLAGCX_ROOT=$(cd ../../.. && pwd)
export FLAGCX_P2P_TRANSPORT=accl
if [ "$ROLE" = server ]; then
  # server interface is picked by the caller (SRV_IF, default eth0); both the
  # FlagCX listener and the barex unicm bind must sit on it. Pick the one the
  # client's route egress lands on (same ethN both sides => same vsolar_N).
  SRV_IF=${SRV_IF:-eth0}
  export FLAGCX_SOCKET_IFNAME="$SRV_IF"
  export NCCL_SOCKET_IFNAME="$SRV_IF"
  export FLAGCX_IB_HCA="vsolar_${SRV_IF#eth}"
else
  :  # client derives its egress below
fi
export FLAGCX_MEM_ENABLE=1
export FLAGCX_VMM_ENABLE=0
export FLAGCX_DEBUG=INFO
export FLAGCX_DEBUG_SUBSYS=INIT
export LD_LIBRARY_PATH="$FLAGCX_ROOT/build/lib:${LD_LIBRARY_PATH:-}"

COMMON="--connector=flagcx --real-pattern real_patterns.json --iters $ITERS --warmup 20 --device gpu"

PIDS=()
if [ "$ROLE" = server ]; then
  for gpu in 0 1; do
    port=$((4566 + gpu * 10))
    python3 kv_transfer_benchmark_noncontig.py --role=server --remote-ip="$ADDR" \
        --advertise-ip="$ADDR" --zmq-port=$port --local-gpu-idx=$gpu $COMMON \
        > "$LOG/svr_gpu${gpu}.log" 2>&1 &
    PIDS+=($!)
  done
else
  # Derive the egress interface the kernel actually uses towards the server
  # (all eth0..3 are /32; the route may leave via any of them). unicm bind
  # AND the data NIC must match that egress or the EIC CID table rejects the
  # first WR with "Send CID HW Error".
  SRC_IF=$(ip route get "$ADDR" | head -1 | sed -n 's/.* dev \([a-z0-9]*\) .*/\1/p')
  NIC_ID=${SRC_IF#eth}
  export NCCL_SOCKET_IFNAME="$SRC_IF"
  export FLAGCX_SOCKET_IFNAME="$SRC_IF"
  export FLAGCX_IB_HCA="vsolar_$NIC_ID"
  echo "client egress: $SRC_IF -> $FLAGCX_IB_HCA"
  sleep 5
  for gpu in 0 1; do
    port=$((4566 + gpu * 10))
    python3 kv_transfer_benchmark_noncontig.py --role=client --remote-ip="$ADDR" \
        --zmq-port=$port --local-gpu-idx=$gpu $COMMON \
        > "$LOG/cli_gpu${gpu}.log" 2>&1 &
    PIDS+=($!)
  done
fi

rc=0
for p in "${PIDS[@]}"; do
  wait $p || rc=1
done
echo "ALL_DONE rc=$rc"
exit $rc
