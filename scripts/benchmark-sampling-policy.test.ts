import { describe, expect, it } from "@effect/vitest";
import {
  decodeBenchmarkMemoryRssTotalDeltaBytes,
  decodeBenchmarkSamplingPolicy,
  samplingPolicyRequiresExactMutationCount,
  validateBenchmarkSamplingPolicy,
  validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes,
} from "./benchmark-sampling-policy.mjs";

const iterationBoundCase = {
  name: "live case",
  sampleCount: 5,
  timeMs: 0,
  warmupIterations: 0,
  warmupTimeMs: 0,
};

const samplingPolicy = {
  iterationBoundCases: [iterationBoundCase],
  memoryRssMetric: "process-peak-over-initial-current",
  measured: {
    minimumSampleCount: 1_000,
    timeMs: 250,
    warmupIterations: 5,
    warmupTimeMs: 100,
  },
};

const readBenchmark = {
  groupName: "sampling benchmark",
  name: "read case",
  sampleCount: 1_000,
};

const liveBenchmark = {
  groupName: "sampling benchmark",
  name: "live case",
  sampleCount: 5,
};

const peakMemory = {
  before: {
    rssBytes: 512,
  },
  processPeakRss: {
    afterBenchmarkBytes: 1_536,
    afterSetupBytes: 1_024,
    beforeBytes: 768,
    benchmarkDeltaBytes: 512,
    setupDeltaBytes: 512,
    totalDeltaBytes: 1_024,
  },
  totalDelta: {
    rssBytes: -1,
  },
};

describe("benchmark sampling policy", () => {
  it("decodes the canonical policy and exact-mutation ownership", () => {
    expect(decodeBenchmarkSamplingPolicy(undefined, "policy")).toBeUndefined();
    expect(decodeBenchmarkSamplingPolicy(samplingPolicy, "policy")).toStrictEqual(
      samplingPolicy,
    );
    expect(samplingPolicyRequiresExactMutationCount(samplingPolicy)).toBe(true);
    expect(
      samplingPolicyRequiresExactMutationCount({
        ...samplingPolicy,
        iterationBoundCases: [],
      }),
    ).toBe(false);
    expect(samplingPolicyRequiresExactMutationCount(undefined)).toBe(false);
  });

  it("rejects unknown, duplicate, and non-iteration-bound policy declarations", () => {
    expect(() =>
      decodeBenchmarkSamplingPolicy(
        {
          ...samplingPolicy,
          memoryRssMetric: "endpoint-rss",
        },
        "policy",
      ),
    ).toThrow(
      "Benchmark artifact field policy.memoryRssMetric must be process-peak-over-initial-current.",
    );
    expect(() =>
      decodeBenchmarkSamplingPolicy(
        {
          ...samplingPolicy,
          iterationBoundCases: [iterationBoundCase, iterationBoundCase],
        },
        "policy",
      ),
    ).toThrow("Benchmark artifact field policy.iterationBoundCases contains duplicate name");

    const policyWith = (overrides: Record<string, number>) => ({
      ...samplingPolicy,
      iterationBoundCases: [
        {
          ...iterationBoundCase,
          ...overrides,
        },
      ],
    });
    const error =
      "Benchmark artifact field policy.iterationBoundCases[0] must disable time and warmup for an iteration-bound case.";
    expect(() => decodeBenchmarkSamplingPolicy(policyWith({ timeMs: 1 }), "policy")).toThrow(
      error,
    );
    expect(() =>
      decodeBenchmarkSamplingPolicy(policyWith({ warmupIterations: 1 }), "policy"),
    ).toThrow(error);
    expect(() =>
      decodeBenchmarkSamplingPolicy(policyWith({ warmupTimeMs: 1 }), "policy"),
    ).toThrow(error);
  });

  it("selects policy-owned process peak RSS and preserves legacy endpoint deltas", () => {
    const decodedPolicy = decodeBenchmarkSamplingPolicy(samplingPolicy, "policy");

    expect(
      decodeBenchmarkMemoryRssTotalDeltaBytes(peakMemory, "memory", decodedPolicy),
    ).toBe(1_024);
    expect(decodeBenchmarkMemoryRssTotalDeltaBytes(peakMemory, "memory", undefined)).toBe(-1);
    expect(
      decodeBenchmarkMemoryRssTotalDeltaBytes({ before: { rssBytes: 512 } }, "memory", undefined),
    ).toBeUndefined();
  });

  it("validates mixed timed and iteration-bound sample ownership", () => {
    const decodedPolicy = decodeBenchmarkSamplingPolicy(samplingPolicy, "policy");

    expect(
      validateBenchmarkSamplingPolicy(
        decodedPolicy,
        [readBenchmark, liveBenchmark],
        5,
        "task a",
      ),
    ).toBeUndefined();
    expect(
      validateBenchmarkSamplingPolicy(undefined, [readBenchmark], 1_000, "task a"),
    ).toBeUndefined();
    expect(() =>
      validateBenchmarkSamplingPolicy(decodedPolicy, [readBenchmark, liveBenchmark], 4, "task a"),
    ).toThrow("task a: minimumSampleCount must equal sampling policy minimum 5 but was 4.");
    expect(() =>
      validateBenchmarkSamplingPolicy(
        decodedPolicy,
        [{ ...readBenchmark, sampleCount: 999 }, liveBenchmark],
        5,
        "task a",
      ),
    ).toThrow("timed read sampleCount must be at least 1000 but was 999");
    expect(() =>
      validateBenchmarkSamplingPolicy(
        decodedPolicy,
        [readBenchmark, { ...liveBenchmark, sampleCount: 4 }],
        5,
        "task a",
      ),
    ).toThrow("iteration-bound sampleCount must be exactly 5 but was 4");
    expect(() =>
      validateBenchmarkSamplingPolicy(decodedPolicy, [readBenchmark], 5, "task a"),
    ).toThrow("task a: missing iteration-bound benchmark case live case.");
  });

  it("requires positive persisted memory only for policy-owned peak RSS", () => {
    const decodedPolicy = decodeBenchmarkSamplingPolicy(samplingPolicy, "policy");

    expect(
      validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes(decodedPolicy, 1, "memory"),
    ).toBeUndefined();
    expect(() =>
      validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes(decodedPolicy, 0, "memory"),
    ).toThrow("Benchmark artifact field memory must be a positive integer.");
    expect(
      validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes(undefined, -1, "memory"),
    ).toBeUndefined();
  });
});
