#!/bin/bash
# Real-inference KV transfer benchmark on PPU (FlagCX p2p accl backend).
#
# Replays transfer patterns from a real prefill log (non-uniform lengths).
# Cross-machine, one direction (mirrors prefill->decode KV transfer):
#   client machine                        server machine
#   GPU0 -> server GPU0   (zmq 4566)
#   GPU1 -> server GPU1   (zmq 4576)
#
# Environment mirrors the production serving env (net0 for all socket
# planes, ACCL_SELECT_NIC=1 with NO FLAGCX_IB_HCA whitelist, the
# FLAGCX_P2P_*/ACCL_* tuning set). Deploy-side variables are all ruled out:
# same-host self-connect, iface mismatches and vsolar_N crossings all fail
# with the same EIC "Send CID HW Error" — do not re-derive iface logic here.
#
# Usage:
#   server machine: bash run_real_bench.sh server <own-net0-ip> [iters]
#   client machine: bash run_real_bench.sh client <server-net0-ip> [iters]
#   PATTERN_JSON=small_only.json to subset patterns; iters small to smoke.
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

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

FLAGCX_ROOT=$(cd ../../.. && pwd)
export FLAGCX_PATH="$FLAGCX_ROOT"
export LD_LIBRARY_PATH="$FLAGCX_ROOT/build/lib:${LD_LIBRARY_PATH:-}"
export FLAGCX_P2P_TRANSPORT=accl
export FLAGCX_VMM_ENABLE=0
export FLAGCX_DMABUF_ENABLE=0
export FLAGCX_P2P_QPS_PER_CONN=2
export FLAGCX_P2P_SLICE_SIZE=67108864
export PASS_ALLOC=1
# NOTE: ACCL_SELECT_NIC (from the serving env) is NOT set here — it drives
# barex's own NIC selection and made MrForAllDevices fail with res=3; the
# FlagCX accl engine does its own topo-based NIC pick per GPU.
export ACCL_WRITEBATCH_OPT=2
export ACCL_POST_RECV_SIZE=4
export ACCL_LOW_LATENCY_OPTIMIZE=1
export FIC2_OOO_DISABLE_0115=1
# IFACE must be a vsolar-backed eth (unicm bind IP must match a RoCE GID;
# net0's 10.11.x fails RegUserMr). Both FLAGCX and NCCL vars must agree or
# the EIC CID table rejects the first WR.
IFACE=${IFACE:-eth1}
export FLAGCX_SOCKET_IFNAME="$IFACE"
export NCCL_SOCKET_IFNAME="$IFACE"
export GLOO_SOCKET_IFNAME=net0
export FLAGCX_DEBUG=INFO
export FLAGCX_DEBUG_SUBSYS=INIT

PATTERN=${PATTERN_JSON:-real_patterns.json}
COMMON="--connector=flagcx --real-pattern $PATTERN --iters $ITERS --warmup 2 --device gpu"

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
