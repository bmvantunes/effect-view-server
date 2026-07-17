export type GrpcBenchmarkMemoryLifecycle<Memory> = {
  readonly captureAfterCleanup: () => Promise<{
    readonly afterCleanup: Memory;
    readonly before: Memory;
  }>;
  readonly captureBefore: () => Promise<void>;
};

type GrpcBenchmarkMemoryLifecycleOptions<Memory> = {
  readonly capture: () => Memory;
  readonly collectGarbage: (() => void) | undefined;
  readonly explicitGc: boolean;
  readonly settle: () => Promise<void>;
};

export const grpcBenchmarkExplicitGcFromEnv = (raw: string | undefined): boolean => {
  if (raw === undefined || raw === "0") {
    return false;
  }
  if (raw === "1") {
    return true;
  }
  throw new Error("VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC must be 0 or 1.");
};

export const makeGrpcBenchmarkMemoryLifecycle = <Memory>(
  options: GrpcBenchmarkMemoryLifecycleOptions<Memory>,
): GrpcBenchmarkMemoryLifecycle<Memory> => {
  if (options.explicitGc && options.collectGarbage === undefined) {
    throw new Error(
      "gRPC benchmark explicit GC requires Node to start with NODE_OPTIONS=--expose-gc.",
    );
  }

  let before: Memory | undefined;
  let finished = false;

  const settleAndCollect = async (): Promise<void> => {
    await options.settle();
    if (options.explicitGc) {
      options.collectGarbage?.();
    }
  };

  return {
    captureAfterCleanup: async () => {
      if (before === undefined) {
        throw new Error("gRPC benchmark memory cannot finish before its initial checkpoint.");
      }
      if (finished) {
        throw new Error("gRPC benchmark memory recording already finished.");
      }
      finished = true;
      await settleAndCollect();
      return {
        afterCleanup: options.capture(),
        before,
      };
    },
    captureBefore: async () => {
      if (before !== undefined) {
        throw new Error("gRPC benchmark initial memory was already recorded.");
      }
      if (finished) {
        throw new Error("gRPC benchmark initial memory cannot be recorded after completion.");
      }
      await settleAndCollect();
      before = options.capture();
    },
  };
};
