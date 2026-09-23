#!/bin/bash
# Real-inference KV transfer benchmark on PPU (FlagCX p2p accl backend).
#
# Replays the two most frequent transfer patterns from a real prefill log
# (167.788 MiB / 1950 WRs and 1464.328 MiB / 1014 WRs, non-uniform lengths),
# 2 server/client pairs over 4 GPUs (GPU0->1, GPU2->3), single NIC vsolar_0.
#
# Usage: bash run_real_bench.sh [iters]   (default 2000)
set -u
cd "$(dirname "$0")"
ITERS=${1:-2000}
LOG=claude-trash
mkdir -p "$LOG"

FLAGCX_ROOT=$(cd ../../.. && pwd)
export FLAGCX_P2P_TRANSPORT=accl
export FLAGCX_IB_HCA=vsolar_0
export FLAGCX_SOCKET_IFNAME=eth0
export NCCL_SOCKET_IFNAME=eth0
export FLAGCX_MEM_ENABLE=1
export FLAGCX_VMM_ENABLE=0
export FLAGCX_DEBUG=INFO
export FLAGCX_DEBUG_SUBSYS=INIT
export LD_LIBRARY_PATH="$FLAGCX_ROOT/build/lib:${LD_LIBRARY_PATH:-}"

COMMON="--connector=flagcx --real-pattern real_patterns.json --iters $ITERS --warmup 20 --device gpu"

PIDS=()
for pair in 0 1; do
  port=$((4566 + pair * 10))
  sgpu=$((pair * 2))
  python3 kv_transfer_benchmark_noncontig.py --role=server --remote-ip=0.0.0.0 \
      --zmq-port=$port --local-gpu-idx=$sgpu $COMMON \
      > "$LOG/p${pair}_server.log" 2>&1 &
  PIDS+=($!)
done
sleep 5
for pair in 0 1; do
  port=$((4566 + pair * 10))
  cgpu=$((pair * 2 + 1))
  python3 kv_transfer_benchmark_noncontig.py --role=client --remote-ip=127.0.0.1 \
      --zmq-port=$port --local-gpu-idx=$cgpu $COMMON \
      > "$LOG/p${pair}_client.log" 2>&1 &
  PIDS+=($!)
done

rc=0
for p in "${PIDS[@]}"; do
  wait $p || rc=1
done
echo "ALL_DONE rc=$rc"
echo "===== client results ====="
grep -hE 'small|big|lat avg' "$LOG"/p*_client.log
exit $rc
