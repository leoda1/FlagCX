#include "perf_common.h"

static void warmupFn(PerfContext &ctx, size_t count) {
  PERF_CHECK(flagcxBroadcast(ctx.sendbuff, ctx.recvbuff, count, ctx.datatype, 0,
                             ctx.comm, ctx.stream));
}

static void collFn(PerfContext &ctx, size_t count, int root) {
  PERF_CHECK(flagcxBroadcast(ctx.sendbuff, ctx.recvbuff, count, ctx.datatype,
                             root, ctx.comm, ctx.stream));
}

int main(int argc, char *argv[]) {
  PerfContext ctx;
  perfSetup(ctx, argc, argv);
  perfWarmup(ctx, warmupFn);
  perfRootBenchmarkLoop(ctx, collFn, nullptr, nullptr, nullptr, false);
  perfTeardown(ctx);
  return 0;
}
