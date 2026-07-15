import { describe, expect, it } from "@effect/vitest";

import {
  benchmarkComparisonPolicyForProfile,
  compareBenchmarkArtifacts,
} from "./benchmark-comparison-policy.mjs";

const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarks: [
    {
      groupName: "src/example.bench.ts > example benchmark group",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "case a",
      p99Ms: 3,
      sampleCount: 7,
    },
  ],
  benchmarkCases: ["case a"],
  benchmarkName: "example benchmark",
  benchmarkScope: "engine-raw-snapshot",
  browser: undefined,
  cleanupLeakCount: 0,
  groupedKeyWidthParameters: undefined,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  kafkaIngestLanes: undefined,
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1_024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  throughputCases: undefined,
  topics: ["orders"],
};

const artifactWith = (tasks: ReadonlyArray<typeof observation>) => ({
  artifactKind: "view-server-benchmark-baseline",
  profile: "smoke",
  tasks,
  thresholds: {},
});

const compare = (
  baselineTasks: ReadonlyArray<typeof observation>,
  actualTasks: ReadonlyArray<typeof observation>,
) =>
  compareBenchmarkArtifacts({
    actual: artifactWith(actualTasks),
    baseline: artifactWith(baselineTasks),
    policy: benchmarkComparisonPolicyForProfile("smoke"),
  });

describe("benchmark artifact compatibility", () => {
  it("accepts equivalent validated artifacts", () => {
    expect(compare([observation], [observation])).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("reports missing tasks by stable task identity", () => {
    expect(compare([observation], [])).toStrictEqual({
      ok: false,
      regressions: ["task a: missing benchmark task in actual run."],
    });
  });
});

describe("benchmark threshold direction", () => {
  it("reports lower-is-better latency regressions with the allowed limit", () => {
    const regressed = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 20,
        },
      ],
    };

    expect(compare([observation], [regressed])).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 2.000ms to 20.000ms; allowed <= 16.000ms.",
      ],
    });
  });

  it("reports higher-is-better throughput regressions with the allowed minimum", () => {
    const throughputCase = {
      aggregateRowsPerSecond: 1_000,
      maxCommitObservedMs: undefined,
      maxReadSnapshotMs: undefined,
      maxTotalMs: 100,
      meanCommitObservedMs: undefined,
      meanConvergenceMs: 75,
      meanProducerSendMs: 25,
      meanReadSnapshotMs: undefined,
      meanRowsPerSecond: 1_000,
      meanTotalMs: 100,
      minRowsPerSecond: 900,
      name: "case a",
      producedRowsPerSample: 100,
      readSnapshotRowsPerSample: undefined,
      sampleCount: 7,
      totalProducedRows: 700,
    };
    const baseline = {
      ...observation,
      throughputCases: [throughputCase],
    };
    const regressed = {
      ...baseline,
      throughputCases: [
        {
          ...throughputCase,
          aggregateRowsPerSecond: 400,
        },
      ],
    };

    expect(compare([baseline], [regressed])).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 400.000 rows/sec; allowed >= 500.000 rows/sec.",
      ],
    });
  });
});

describe("benchmark runtime invariants", () => {
  it("reports invariant counters independently of performance thresholds", () => {
    const leaked = {
      ...observation,
      cleanupLeakCount: 1,
    };

    expect(compare([observation], [leaked])).toStrictEqual({
      ok: false,
      regressions: ["task a: cleanupLeakCount must stay 0 but was 1."],
    });
  });
});
