#include "perf_common.h"

static void bufSizeFn(PerfContext &ctx, size_t &sBuf, size_t &rBuf) {
  sBuf = ctx.maxBytes / ctx.totalProcs;
  rBuf = ctx.maxBytes;
}

static void warmupFn(PerfContext &ctx, size_t count) {
  PERF_CHECK(flagcxGather(ctx.sendbuff, ctx.recvbuff, count / ctx.totalProcs,
                          ctx.datatype, 0, ctx.comm, ctx.stream));
}

static void collFn(PerfContext &ctx, size_t count, int root) {
  PERF_CHECK(flagcxGather(ctx.sendbuff, ctx.recvbuff, count / ctx.totalProcs,
                          ctx.datatype, root, ctx.comm, ctx.stream));
}

static double bwFactorFn(int totalProcs) {
  return (double)(totalProcs - 1) / (double)(totalProcs);
}

int main(int argc, char *argv[]) {
  PerfContext ctx;
  perfSetup(ctx, argc, argv, bufSizeFn);
  perfWarmup(ctx, warmupFn);
  perfRootBenchmarkLoop(ctx, collFn, bwFactorFn, nullptr, nullptr, false);
  perfTeardown(ctx);
  return 0;
}
