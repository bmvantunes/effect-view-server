export type BenchmarkRunOptions = {
  readonly iterations: number;
  readonly time: number;
  readonly warmupIterations: number;
  readonly warmupTime: number;
};

export type BenchmarkIterationBoundCase = {
  readonly name: string;
  readonly options: BenchmarkRunOptions;
};

export type BenchmarkMemoryRssMetric = "process-peak-over-initial-current";

export type BenchmarkSamplingPolicy = {
  readonly iterationBoundCases: ReadonlyArray<{
    readonly name: string;
    readonly sampleCount: number;
    readonly timeMs: number;
    readonly warmupIterations: number;
    readonly warmupTimeMs: number;
  }>;
  readonly memoryRssMetric: BenchmarkMemoryRssMetric;
  readonly measured: {
    readonly minimumSampleCount: number;
    readonly timeMs: number;
    readonly warmupIterations: number;
    readonly warmupTimeMs: number;
  };
};

export const parseBenchmarkMemoryRssMetric = (
  value: string | undefined,
): BenchmarkMemoryRssMetric | undefined => {
  if (value === undefined) {
    return undefined;
  }
  if (value === "process-peak-over-initial-current") {
    return value;
  }
  throw new Error(
    "VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC must be process-peak-over-initial-current.",
  );
};

export const iterationBoundBenchmarkOptions = ({
  fallbackOptions,
  iterationCount,
}: {
  readonly fallbackOptions: BenchmarkRunOptions;
  readonly iterationCount: number | undefined;
}): BenchmarkRunOptions =>
  iterationCount === undefined
    ? fallbackOptions
    : {
        iterations: iterationCount,
        time: 0,
        warmupIterations: 0,
        warmupTime: 0,
      };

export const timedReadSamplingPolicy = ({
  iterationBoundCases,
  memoryRssMetric,
  measuredMinimumSampleCount,
  measuredOptions,
}: {
  readonly iterationBoundCases: ReadonlyArray<BenchmarkIterationBoundCase>;
  readonly memoryRssMetric: BenchmarkMemoryRssMetric | undefined;
  readonly measuredMinimumSampleCount: number | undefined;
  readonly measuredOptions: BenchmarkRunOptions;
}): BenchmarkSamplingPolicy | undefined => {
  if (measuredMinimumSampleCount === undefined) {
    if (memoryRssMetric !== undefined) {
      throw new Error("Peak RSS measurement requires timed read sampling.");
    }
    return undefined;
  }
  if (memoryRssMetric === undefined) {
    throw new Error(
      "Timed read sampling requires process-peak-over-initial-current RSS measurement.",
    );
  }
  return {
    iterationBoundCases: iterationBoundCases.map(({ name, options }) => ({
      name,
      sampleCount: options.iterations,
      timeMs: options.time,
      warmupIterations: options.warmupIterations,
      warmupTimeMs: options.warmupTime,
    })),
    memoryRssMetric,
    measured: {
      minimumSampleCount: measuredMinimumSampleCount,
      timeMs: measuredOptions.time,
      warmupIterations: measuredOptions.warmupIterations,
      warmupTimeMs: measuredOptions.warmupTime,
    },
  };
};
