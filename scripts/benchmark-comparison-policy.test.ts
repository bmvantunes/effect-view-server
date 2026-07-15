import { describe, expect, it } from "@effect/vitest";

import {
  benchmarkComparisonPolicyForProfile,
  compareBenchmarkArtifacts,
} from "./benchmark-comparison-policy.mjs";

const artifactFor = (profile: string) => ({
  artifactKind: "view-server-benchmark-baseline",
  profile,
  tasks: [],
  thresholds: {},
});

const compareWithPolicy = (
  policy: ReturnType<typeof benchmarkComparisonPolicyForProfile>,
  baselineProfile = "smoke",
  actualProfile = "smoke",
) =>
  compareBenchmarkArtifacts({
    actual: artifactFor(actualProfile),
    baseline: artifactFor(baselineProfile),
    policy,
  });

describe("benchmark policy inputs", () => {
  it("makes profile applicability, required metrics, directionality, and tolerances explicit", () => {
    const policy = benchmarkComparisonPolicyForProfile("kafka-ingest");

    expect(policy).toStrictEqual({
      applicableProfiles: ["kafka-ingest"],
      metrics: {
        commitObservedMax: {
          applicability: "kafka-throughput-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 2_500,
            maxRatio: 1.5,
          },
        },
        commitObservedMean: {
          applicability: "kafka-throughput-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 2_000,
            maxRatio: 1.5,
          },
        },
        latencyMean: {
          applicability: "benchmark-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 2_000,
            maxRatio: 1.5,
          },
        },
        latencyP99: {
          applicability: "benchmark-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 2_500,
            maxRatio: 1.5,
          },
        },
        memoryRssTotalDelta: {
          applicability: "task-memory",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaBytes: 256 * 1024 * 1024,
            maxRatio: 64,
          },
        },
        throughputAggregateRowsPerSecond: {
          applicability: "throughput-case",
          direction: "higher-is-better",
          tolerance: {
            minRatio: 0.75,
          },
        },
        throughputReadSnapshotMax: {
          applicability: "kafka-throughput-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 50,
            maxRatio: 10,
          },
        },
        throughputReadSnapshotMean: {
          applicability: "kafka-throughput-case",
          direction: "lower-is-better",
          tolerance: {
            maxAbsoluteDeltaMs: 25,
            maxRatio: 8,
          },
        },
      },
      requiredMetrics: [
        "commitObservedMax",
        "commitObservedMean",
        "latencyMean",
        "latencyP99",
        "memoryRssTotalDelta",
        "throughputAggregateRowsPerSecond",
        "throughputReadSnapshotMax",
        "throughputReadSnapshotMean",
      ],
    });
  });

});

describe("benchmark policy applicability", () => {
  it("returns a precise result when the actual artifact profile is incompatible", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(compareWithPolicy(policy, "smoke", "raw-read-write")).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy applies to smoke but actual artifact uses raw-read-write.",
      ],
    });
  });

  it("returns a precise result when the baseline artifact profile is incompatible", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(compareWithPolicy(policy, "raw-read-write", "smoke")).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy applies to smoke but baseline artifact uses raw-read-write.",
      ],
    });
  });
});

describe("benchmark policy completeness", () => {
  it("returns a precise result when a required policy metric is missing", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");
    const { latencyP99: _latencyP99, ...metrics } = policy.metrics;

    expect(compareWithPolicy({ ...policy, metrics })).toStrictEqual({
      ok: false,
      regressions: ["Comparison policy smoke is missing required metric latencyP99."],
    });
  });

  it("returns a precise result when a required policy metric is unknown", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          mysteryMetric: {
            applicability: "benchmark-case",
            direction: "lower-is-better",
            tolerance: { maxRatio: 2 },
          },
        },
        requiredMetrics: [...policy.requiredMetrics, "mysteryMetric"],
      }),
    ).toStrictEqual({
      ok: false,
      regressions: ["Comparison policy smoke requires unknown metric mysteryMetric."],
    });
  });

  it("returns a precise result when metric directionality changes", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            direction: "higher-is-better",
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean direction must be lower-is-better but was higher-is-better.",
      ],
    });
  });

  it("returns a precise result when metric applicability changes", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            applicability: "task-memory",
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean applicability must be benchmark-case but was task-memory.",
      ],
    });
  });

  it("returns a precise result when a metric tolerance is absent", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            tolerance: undefined,
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean must define a tolerance object.",
      ],
    });
  });

  it("returns a precise result when a metric tolerance is null", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            tolerance: null,
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean must define a tolerance object.",
      ],
    });
  });

  it("returns a precise result when a metric tolerance is scalar", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            tolerance: 2,
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean must define a tolerance object.",
      ],
    });
  });

  it("returns a precise result when a metric tolerance is an array", () => {
    const policy = benchmarkComparisonPolicyForProfile("smoke");

    expect(
      compareWithPolicy({
        ...policy,
        metrics: {
          ...policy.metrics,
          latencyMean: {
            ...policy.metrics.latencyMean,
            tolerance: [],
          },
        },
      }),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke metric latencyMean must define a tolerance object.",
      ],
    });
  });
});
