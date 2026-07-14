import { describe, expect, it } from "@effect/vitest";
import {
  makeBenchmarkMemoryRecorder,
  makeBenchmarkMemoryRecorderWithCapture,
  memoryDelta,
  memorySnapshot,
  processPeakRssBytes,
  processPeakRssBytesFromKibibytes,
  type BenchmarkMemorySnapshot,
} from "./benchmark-memory-recorder";
import type { BenchmarkSamplingPolicy } from "./benchmark-sampling";

const memory = (value: number): BenchmarkMemorySnapshot => ({
  arrayBuffersBytes: value,
  externalBytes: value + 1,
  heapTotalBytes: value + 2,
  heapUsedBytes: value + 3,
  rssBytes: value + 4,
});

const samplingPolicy: BenchmarkSamplingPolicy = {
  iterationBoundCases: [],
  memoryRssMetric: "process-peak-over-initial-current",
  measured: {
    minimumSampleCount: 1_000,
    timeMs: 250,
    warmupIterations: 5,
    warmupTimeMs: 100,
  },
};

describe("benchmark memory recorder", () => {
  it("converts Node maxRSS kibibytes to bytes exactly", () => {
    expect(processPeakRssBytesFromKibibytes(1_234)).toBe(1_263_616);

    const currentMemory = memorySnapshot();
    expect(processPeakRssBytes()).toBeGreaterThanOrEqual(currentMemory.rssBytes);

    const recorder = makeBenchmarkMemoryRecorder();
    recorder.captureAfterSetup();
    expect(recorder.captureAfterBenchmark(undefined).memoryBefore.rssBytes).toBeGreaterThan(0);
  });

  it("records endpoint snapshots without peak fields for legacy sampling", () => {
    const snapshots = [memory(1), memory(3), memory(5)];
    const recorder = makeBenchmarkMemoryRecorderWithCapture({
      memorySnapshot: () => snapshots.shift() ?? memory(999),
      processPeakRssBytes: () => 999,
    });

    recorder.captureAfterSetup();

    expect(recorder.captureAfterBenchmark(undefined)).toStrictEqual({
      memoryAfterBenchmark: memory(5),
      memoryAfterSetup: memory(3),
      memoryBefore: memory(1),
    });
    expect(snapshots).toStrictEqual([]);
  });

  it("records a policy-coupled process peak at every lifecycle checkpoint", () => {
    const snapshots = [memory(1), memory(3), memory(5)];
    const processPeaks = [20, 30, 40];
    const recorder = makeBenchmarkMemoryRecorderWithCapture({
      memorySnapshot: () => snapshots.shift() ?? memory(999),
      processPeakRssBytes: () => processPeaks.shift() ?? 999,
    });

    recorder.captureAfterSetup();

    expect(recorder.captureAfterBenchmark(samplingPolicy)).toStrictEqual({
      memoryAfterBenchmark: memory(5),
      memoryAfterSetup: memory(3),
      memoryBefore: memory(1),
      processPeakRss: {
        afterBenchmarkBytes: 40,
        afterSetupBytes: 30,
        beforeBytes: 20,
      },
      samplingPolicy,
    });
    expect(snapshots).toStrictEqual([]);
    expect(processPeaks).toStrictEqual([]);
  });

  it("uses the initial checkpoint when setup never completes", () => {
    const snapshots = [memory(1), memory(5)];
    const processPeaks = [20, 40];
    const recorder = makeBenchmarkMemoryRecorderWithCapture({
      memorySnapshot: () => snapshots.shift() ?? memory(999),
      processPeakRssBytes: () => processPeaks.shift() ?? 999,
    });

    expect(recorder.captureAfterBenchmark(samplingPolicy)).toStrictEqual({
      memoryAfterBenchmark: memory(5),
      memoryAfterSetup: memory(1),
      memoryBefore: memory(1),
      processPeakRss: {
        afterBenchmarkBytes: 40,
        afterSetupBytes: 20,
        beforeBytes: 20,
      },
      samplingPolicy,
    });
  });

  it("rejects duplicate and out-of-order lifecycle checkpoints", () => {
    const recorder = makeBenchmarkMemoryRecorderWithCapture({
      memorySnapshot: () => memory(1),
      processPeakRssBytes: () => 20,
    });

    recorder.captureAfterSetup();
    expect(() => recorder.captureAfterSetup()).toThrow(
      "Benchmark setup memory was already recorded.",
    );
    recorder.captureAfterBenchmark(undefined);
    expect(() => recorder.captureAfterBenchmark(undefined)).toThrow(
      "Benchmark memory recording already finished.",
    );
    expect(() => recorder.captureAfterSetup()).toThrow(
      "Benchmark setup memory cannot be recorded after benchmark completion.",
    );
  });

  it("computes endpoint memory deltas", () => {
    expect(memoryDelta(memory(1), memory(3))).toStrictEqual({
      arrayBuffersBytes: 2,
      externalBytes: 2,
      heapTotalBytes: 2,
      heapUsedBytes: 2,
      rssBytes: 2,
    });
  });
});
