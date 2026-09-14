#include "perf_common.h"

static void collFn(PerfContext &ctx, size_t count) {
  PERF_CHECK(flagcxAlltoAll(ctx.sendbuff, ctx.recvbuff, count / ctx.totalProcs,
                            ctx.datatype, ctx.comm, ctx.stream));
}

static double bwFactorFn(int totalProcs) {
  return (double)(totalProcs - 1) / (double)(totalProcs);
}

int main(int argc, char *argv[]) {
  PerfContext ctx;
  perfSetup(ctx, argc, argv);
  perfWarmup(ctx, collFn);
  perfBenchmarkLoop(ctx, collFn, bwFactorFn, nullptr, nullptr, false);
  perfTeardown(ctx);
  return 0;
}
