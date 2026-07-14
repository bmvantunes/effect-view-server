import type { BenchmarkSamplingPolicy } from "./benchmark-sampling";

declare const process: {
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
  readonly resourceUsage: () => {
    readonly maxRSS: number;
  };
};

export type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

export type BenchmarkMemoryDelta = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

export type BenchmarkProcessPeakRss = {
  readonly afterBenchmarkBytes: number;
  readonly afterSetupBytes: number;
  readonly beforeBytes: number;
};

type BenchmarkEndpointMemoryInput = {
  readonly memoryAfterBenchmark: BenchmarkMemorySnapshot;
  readonly memoryAfterSetup: BenchmarkMemorySnapshot;
  readonly memoryBefore: BenchmarkMemorySnapshot;
};

export type BenchmarkArtifactMemoryInput = BenchmarkEndpointMemoryInput &
  (
    | {
        readonly processPeakRss?: never;
        readonly samplingPolicy?: never;
      }
    | {
        readonly processPeakRss: BenchmarkProcessPeakRss;
        readonly samplingPolicy: BenchmarkSamplingPolicy;
      }
  );

type BenchmarkMemoryCheckpoint = {
  readonly current: BenchmarkMemorySnapshot;
  readonly processPeakRssBytes: number;
};

export type BenchmarkMemoryCapture = {
  readonly memorySnapshot: () => BenchmarkMemorySnapshot;
  readonly processPeakRssBytes: () => number;
};

export type BenchmarkMemoryRecorder = {
  readonly captureAfterBenchmark: (
    samplingPolicy: BenchmarkSamplingPolicy | undefined,
  ) => BenchmarkArtifactMemoryInput;
  readonly captureAfterSetup: () => void;
};

export const memorySnapshot = (): BenchmarkMemorySnapshot => {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
};

export const processPeakRssBytesFromKibibytes = (maxRssKibibytes: number): number =>
  maxRssKibibytes * 1024;

export const processPeakRssBytes = (): number =>
  processPeakRssBytesFromKibibytes(process.resourceUsage().maxRSS);

export const memoryDelta = (
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemoryDelta => ({
  arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
  externalBytes: after.externalBytes - before.externalBytes,
  heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
  heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
  rssBytes: after.rssBytes - before.rssBytes,
});

const defaultMemoryCapture: BenchmarkMemoryCapture = {
  memorySnapshot,
  processPeakRssBytes,
};

const captureCheckpoint = (capture: BenchmarkMemoryCapture): BenchmarkMemoryCheckpoint => ({
  current: capture.memorySnapshot(),
  processPeakRssBytes: capture.processPeakRssBytes(),
});

export const makeBenchmarkMemoryRecorderWithCapture = (
  capture: BenchmarkMemoryCapture,
): BenchmarkMemoryRecorder => {
  const before = captureCheckpoint(capture);
  let afterSetup: BenchmarkMemoryCheckpoint | undefined;
  let finished = false;

  return {
    captureAfterBenchmark: (samplingPolicy) => {
      if (finished) {
        throw new Error("Benchmark memory recording already finished.");
      }
      finished = true;
      const afterBenchmark = captureCheckpoint(capture);
      const resolvedAfterSetup = afterSetup ?? before;
      const endpoints = {
        memoryAfterBenchmark: afterBenchmark.current,
        memoryAfterSetup: resolvedAfterSetup.current,
        memoryBefore: before.current,
      };
      if (samplingPolicy === undefined) {
        return endpoints;
      }
      return {
        ...endpoints,
        processPeakRss: {
          afterBenchmarkBytes: afterBenchmark.processPeakRssBytes,
          afterSetupBytes: resolvedAfterSetup.processPeakRssBytes,
          beforeBytes: before.processPeakRssBytes,
        },
        samplingPolicy,
      };
    },
    captureAfterSetup: () => {
      if (finished) {
        throw new Error("Benchmark setup memory cannot be recorded after benchmark completion.");
      }
      if (afterSetup !== undefined) {
        throw new Error("Benchmark setup memory was already recorded.");
      }
      afterSetup = captureCheckpoint(capture);
    },
  };
};

export const makeBenchmarkMemoryRecorder = (): BenchmarkMemoryRecorder =>
  makeBenchmarkMemoryRecorderWithCapture(defaultMemoryCapture);
