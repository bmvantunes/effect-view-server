import { describe, expect, it } from "@effect/vitest";

import {
  buildBenchmarkBaseline,
  validateBenchmarkBaseline,
} from "./benchmark-baseline.mjs";
import {
  summary,
  runtimeThroughputMutationCount,
  comparableRuntimeThroughputKafkaIngestLanes,
  comparableRuntimeThroughputCases,
  runtimeMetrics,
  drainedRuntimeMetrics,
  comparableNonKafkaRuntimeThroughputCases,
  runtimeGrpcLeasedSample,
  runtimeGrpcLeasedOperationCaseFor,
  runtimeGrpcLeasedOperationCase,
  runtimeGrpcLeasedOperationCases,
  replaceGrpcLeasedOperationCase,
  runtimeGrpcMaterializedParameters,
  runtimeGrpcMaterializedSampleFor,
  runtimeGrpcMaterializedOperationCaseFor,
  runtimeGrpcMaterializedOperationCasesFor,
  runtimeGrpcMaterializedStreamSample,
  runtimeGrpcMaterializedHealthSample,
  runtimeGrpcMaterializedOperationCase,
  runtimeGrpcMaterializedHealthOperationCase,
  runtimeGrpcMaterializedOperationCases,
  runtimeGrpcMaterializedComparisonState,
  observation,
  grpcLeasedObservationFor,
  completeGrpcLeasedObservation,
  runtimeGrpcMaterializedObservationFor,
  replaceGrpcMaterializedOperationCase,
  runtimeGrpcMaterializedObservation,
} from "./benchmark-baseline-test-fixtures.ts";

import {
  benchmarkComparisonPolicyForProfile,
  compareBenchmarkArtifacts,
  defaultBenchmarkThresholds,
} from "./benchmark-comparison-policy.mjs";

const simpleObservation = {
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

const artifactWith = (tasks: ReadonlyArray<typeof simpleObservation>) => ({
  artifactKind: "view-server-benchmark-baseline",
  profile: "smoke",
  tasks,
  thresholds: {},
});

const compare = (
  baselineTasks: ReadonlyArray<typeof simpleObservation>,
  actualTasks: ReadonlyArray<typeof simpleObservation>,
) =>
  compareBenchmarkArtifacts({
    actual: artifactWith(actualTasks),
    baseline: artifactWith(baselineTasks),
    policy: benchmarkComparisonPolicyForProfile("smoke"),
  });

describe("benchmark artifact compatibility", () => {
  it("accepts equivalent validated artifacts", () => {
    expect(compare([simpleObservation], [simpleObservation])).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("reports missing tasks by stable task identity", () => {
    expect(compare([simpleObservation], [])).toStrictEqual({
      ok: false,
      regressions: ["task a: missing benchmark task in actual run."],
    });
  });

  it("rejects measurement protocol changes as structural incompatibilities", () => {
    const withMeasurementProtocol = {
      ...simpleObservation,
      measurementProtocol: {
        memoryCheckpoint: "settled-explicit-gc-after-cleanup",
      },
    };

    expect(compare([withMeasurementProtocol], [simpleObservation])).toStrictEqual({
      ok: false,
      regressions: [
        'task a: measurementProtocol changed from {"memoryCheckpoint":"settled-explicit-gc-after-cleanup"} to undefined.',
      ],
    });
  });
});

describe("benchmark threshold direction", () => {
  it("reports lower-is-better latency regressions with the allowed limit", () => {
    const regressed = {
      ...simpleObservation,
      benchmarks: [
        {
          ...simpleObservation.benchmarks[0],
          meanMs: 20,
        },
      ],
    };

    expect(compare([simpleObservation], [regressed])).toStrictEqual({
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
      ...simpleObservation,
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
      ...simpleObservation,
      cleanupLeakCount: 1,
    };

    expect(compare([simpleObservation], [leaked])).toStrictEqual({
      ok: false,
      regressions: ["task a: cleanupLeakCount must stay 0 but was 1."],
    });
  });
});

const compareArtifacts = (baseline: unknown, actual: unknown) => {
  const validatedBaseline = validateBenchmarkBaseline(baseline, "baseline");
  const validatedActual = validateBenchmarkBaseline(actual, "actual");
  return compareBenchmarkArtifacts({
    actual: validatedActual,
    baseline: validatedBaseline,
    policy: benchmarkComparisonPolicyForProfile(validatedBaseline.profile),
  });
};

describe("benchmark shared-query compatibility", () => {
  it("accepts benchmark results inside configured thresholds", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            meanMs: 15,
            p99Ms: 23,
          },
        ],
        memoryRssTotalDeltaBytes: 2048,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


  it("reports active-query sharing structural regressions", () => {
    const baseline = buildBenchmarkBaseline("active-query-sharing", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 1,
      },
    ]);
    const regressed = buildBenchmarkBaseline("active-query-sharing", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 50,
      },
    ]);

    expect(compareArtifacts(baseline, regressed)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: activeViewCountBeforeCleanup changed from 1 to 50.",
      ],
    });
  });


  it("does not force optional active-query structural counters onto older baselines", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        activeViewCountBeforeCleanup: 1,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


});

describe("benchmark gRPC comparison behavior", () => {
  it("reports gRPC benchmark parameter drift", () => {
    const changedParameters = {
      ...runtimeGrpcMaterializedParameters,
      batchSize: 200,
    };
    const changedOperationCases = runtimeGrpcMaterializedOperationCasesFor(changedParameters);
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      },
    ]);
    const actual = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(changedOperationCases),
        grpcParameters: changedParameters,
        runtimeOperationCases: changedOperationCases,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: mutationCount changed from 3500 to 7000.",
        "task a: gRPC materialized stream batch runtime operation totalRows changed from 1100 to 1200.",
        "task a: gRPC materialized stream batch runtime operation mutationCount changed from 700 to 1400.",
        `task a: gRPC materialized stream batch runtime operation sample state changed from ${JSON.stringify(runtimeGrpcMaterializedComparisonState(runtimeGrpcMaterializedOperationCases, "gRPC materialized stream batch"))} to ${JSON.stringify(runtimeGrpcMaterializedComparisonState(changedOperationCases, "gRPC materialized stream batch"))}.`,
        "task a: gRPC materialized burst runtime operation totalRows changed from 1400 to 1800.",
        "task a: gRPC materialized burst runtime operation mutationCount changed from 2800 to 5600.",
        `task a: gRPC materialized burst runtime operation sample state changed from ${JSON.stringify(runtimeGrpcMaterializedComparisonState(runtimeGrpcMaterializedOperationCases, "gRPC materialized burst"))} to ${JSON.stringify(runtimeGrpcMaterializedComparisonState(changedOperationCases, "gRPC materialized burst"))}.`,
        'task a: grpcParameters changed from {"batchSize":100,"seedRows":1000} to {"batchSize":200,"seedRows":1000}.',
      ],
    });
  });


  it("reports gRPC runtime operation regressions", () => {
    const baseline = buildBenchmarkBaseline("grpc-leased", [
      {
        ...completeGrpcLeasedObservation,
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      },
    ]);
    const regressedOperationCase = runtimeGrpcLeasedOperationCaseFor({
      ...runtimeGrpcLeasedSample,
      rowsPerSecond: 50,
      snapshotMs: 1000,
    });
    const regressedOperationCases = replaceGrpcLeasedOperationCase(regressedOperationCase);
    const actual = buildBenchmarkBaseline("grpc-leased", [
      {
        ...grpcLeasedObservationFor(regressedOperationCases),
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: regressedOperationCases,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / gRPC leased first subscriber: meanSnapshotMs regressed from 50.000ms to 1000.000ms; allowed <= 600.000ms.",
        "task a / gRPC leased first subscriber: maxSnapshotMs regressed from 50.000ms to 1000.000ms; allowed <= 800.000ms.",
        "task a / gRPC leased first subscriber: pooledRowsPerSecond throughput regressed from 1000.000 rows/sec to 50.000 rows/sec; allowed >= 500.000 rows/sec.",
      ],
    });
  });


  it("reports materialized gRPC runtime operation regressions", () => {
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      },
    ]);
    const regressedOperationCase = {
      ...runtimeGrpcMaterializedOperationCase,
      maxStreamConvergenceMs: 1500,
      meanRowsPerSecond: (6000 + 1000 / 15) / 7,
      meanStreamConvergenceMs: 300,
      pooledRowsPerSecond: 1000 / 3,
      rowsPerSecondCoefficientOfVariation:
        Math.sqrt(
          (6 * (1000 - (6000 + 1000 / 15) / 7) ** 2 +
            (1000 / 15 - (6000 + 1000 / 15) / 7) ** 2) /
            7,
        ) /
        ((6000 + 1000 / 15) / 7),
      samples: [
        ...runtimeGrpcMaterializedOperationCase.samples.slice(0, 6),
        {
          ...runtimeGrpcMaterializedStreamSample,
          rowsPerSecond: 1000 / 15,
          streamConvergenceMs: 1500,
        },
      ],
    };
    const regressedOperationCases = replaceGrpcMaterializedOperationCase(
      regressedOperationCase,
    );
    const actual = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(regressedOperationCases),
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: regressedOperationCases,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / gRPC materialized stream batch: pooledRowsPerSecond throughput regressed from 1000.000 rows/sec to 333.333 rows/sec; allowed >= 500.000 rows/sec.",
      ],
    });
  });


  it("rejects materialized gRPC runtime operation membership changes", () => {
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      },
    ]);
    const incompleteOperationCases = runtimeGrpcMaterializedOperationCases.slice(0, -1);
    const incomplete = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(incompleteOperationCases),
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: incompleteOperationCases,
      },
    ]);

    expect(() => compareArtifacts(baseline, incomplete)).toThrow(
      "Benchmark artifact field actual.tasks[0].runtimeOperationCases is missing gRPC materialized operation case: gRPC materialized health overlay.",
    );

    const unexpectedOperationCase = runtimeGrpcMaterializedOperationCaseFor(
      runtimeGrpcMaterializedSampleFor(
        runtimeGrpcMaterializedParameters,
        "unexpected materialized case",
        0,
        null,
      ),
    );
    const unexpectedOperationCases = [
      ...runtimeGrpcMaterializedOperationCases,
      unexpectedOperationCase,
    ];
    const unexpected = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(unexpectedOperationCases),
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: unexpectedOperationCases,
      },
    ]);

    expect(() => compareArtifacts(baseline, unexpected)).toThrow(
      "Benchmark artifact field actual.tasks[0].runtimeOperationCases[3].name must be one of: gRPC materialized stream batch, gRPC materialized burst, gRPC materialized health overlay.",
    );
  });


  it("rejects missing canonical leased runtime operation cases", () => {
    const baseline = buildBenchmarkBaseline("grpc-leased", [
      {
        ...completeGrpcLeasedObservation,
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      },
    ]);
    const incompleteOperationCases = runtimeGrpcLeasedOperationCases.slice(0, -1);
    const actual = buildBenchmarkBaseline("grpc-leased", [
      {
        ...grpcLeasedObservationFor(incompleteOperationCases),
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: incompleteOperationCases,
      },
    ]);

    expect(() => compareArtifacts(baseline, actual)).toThrow(
      "Benchmark artifact field actual.tasks[0].runtimeOperationCases is missing gRPC leased operation case: gRPC leased many routes.",
    );
  });


  it("rejects unexpected leased runtime operation cases", () => {
    const baseline = buildBenchmarkBaseline("grpc-leased", [
      {
        ...completeGrpcLeasedObservation,
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      },
    ]);
    const unexpectedOperationCase = runtimeGrpcLeasedOperationCaseFor({
      ...runtimeGrpcLeasedSample,
      name: "unexpected leased case",
    });
    const actual = buildBenchmarkBaseline("grpc-leased", [
      {
        ...grpcLeasedObservationFor([
          ...runtimeGrpcLeasedOperationCases,
          unexpectedOperationCase,
        ]),
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: [
          ...runtimeGrpcLeasedOperationCases,
          unexpectedOperationCase,
        ],
      },
    ]);

    expect(() => compareArtifacts(baseline, actual)).toThrow(
      "Benchmark artifact field actual.tasks[0].runtimeOperationCases[10].name must be one of: gRPC leased first subscriber",
    );
  });


  it("reports runtime operation case presence changes without throwing", () => {
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      },
    ]);
    const actual = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        benchmarkScope: "engine-raw-snapshot",
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: benchmarkScope changed from runtime-grpc-materialized to engine-raw-snapshot.",
        "task a: seedMutationCount changed from 21000 to undefined.",
        "task a: runtimeOperationCases presence changed.",
        'task a: grpcParameters changed from {"batchSize":100,"seedRows":1000} to undefined.',
      ],
    });
  });


  it("treats zero baseline runtime operation metrics as exact invariants", () => {
    const zeroHealthOverlayCase = {
      ...runtimeGrpcMaterializedHealthOperationCase,
      maxHealthOverlayMs: 0,
      meanHealthOverlayMs: 0,
      samples: runtimeGrpcMaterializedHealthOperationCase.samples.map((sample) => ({
        ...sample,
        healthOverlayMs: 0,
      })),
    };
    const zeroHealthOverlayCases = replaceGrpcMaterializedOperationCase(
      zeroHealthOverlayCase,
    );
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(zeroHealthOverlayCases),
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: zeroHealthOverlayCases,
      },
    ]);
    const actual = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservation,
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: gRPC materialized health overlay runtime operation meanHealthOverlayMs changed from 0 to 1.",
        "task a: gRPC materialized health overlay runtime operation maxHealthOverlayMs changed from 0 to 1.",
      ],
    });
  });


  it("treats zero leased operation latency as an exact invariant", () => {
    const zeroCleanupCase = {
      ...runtimeGrpcLeasedOperationCase,
      maxCleanupMs: 0,
      meanCleanupMs: 0,
      samples: runtimeGrpcLeasedOperationCase.samples.map((sample) => ({
        ...sample,
        cleanupMs: 0,
      })),
    };
    const zeroCleanupCases = replaceGrpcLeasedOperationCase(zeroCleanupCase);
    const baseline = buildBenchmarkBaseline("grpc-leased", [
      {
        ...grpcLeasedObservationFor(zeroCleanupCases),
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: zeroCleanupCases,
      },
    ]);
    const actual = buildBenchmarkBaseline("grpc-leased", [
      {
        ...completeGrpcLeasedObservation,
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: gRPC leased first subscriber runtime operation meanCleanupMs changed from 0 to 2.",
        "task a: gRPC leased first subscriber runtime operation maxCleanupMs changed from 0 to 2.",
      ],
    });
  });


  it("accepts independently seeded zero-row materialized samples and compares zero throughput exactly", () => {
    const sixSampleHealthCase = runtimeGrpcMaterializedOperationCaseFor(
      runtimeGrpcMaterializedHealthSample,
      6,
    );
    const operationCases = replaceGrpcMaterializedOperationCase(sixSampleHealthCase);
    const baseline = buildBenchmarkBaseline("grpc-materialized", [
      {
        ...runtimeGrpcMaterializedObservationFor(operationCases),
        grpcParameters: runtimeGrpcMaterializedParameters,
        runtimeOperationCases: operationCases,
      },
    ]);

    expect(compareArtifacts(baseline, baseline)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


  it("rejects gRPC runtime operation cases without operation thresholds", () => {
    const baseline = buildBenchmarkBaseline("grpc-leased", [
      {
        ...completeGrpcLeasedObservation,
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      },
    ]);

    expect(
      compareArtifacts(
        {
          ...baseline,
          profile: "smoke",
          thresholds: defaultBenchmarkThresholds,
        },
        {
          ...baseline,
          profile: "smoke",
          thresholds: defaultBenchmarkThresholds,
        },
      ),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "Comparison policy smoke does not define operationMean and operationMax metrics required by its gRPC runtime operation cases.",
      ],
    });
  });


});

describe("benchmark profile threshold behavior", () => {
  it("rejects large relative latency regressions for grouped order-neutral baselines", () => {
    const subMillisecondObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 0.2,
          p99Ms: 0.3,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("grouped-order-neutral", [
      subMillisecondObservation,
    ]);
    const actual = buildBenchmarkBaseline("grouped-order-neutral", [
      {
        ...subMillisecondObservation,
        benchmarks: [
          {
            ...subMillisecondObservation.benchmarks[0],
            meanMs: 1.3,
            p99Ms: 2,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 0.200ms to 1.300ms; allowed <= 1.200ms.",
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 0.300ms to 2.000ms; allowed <= 1.800ms.",
      ],
    });
  });


  it("keeps raw read/write sub-millisecond mean jitter report-only inside the absolute window", () => {
    const subMillisecondObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 0.067,
          p99Ms: 0.3,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("raw-read-write", [subMillisecondObservation]);
    const actual = buildBenchmarkBaseline("raw-read-write", [
      {
        ...subMillisecondObservation,
        benchmarks: [
          {
            ...subMillisecondObservation.benchmarks[0],
            meanMs: 0.239,
            p99Ms: 0.3,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


  it("rejects raw read/write mean regressions outside the focused absolute window", () => {
    const subMillisecondObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 0.125,
          p99Ms: 0.3,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("raw-read-write", [subMillisecondObservation]);
    const actual = buildBenchmarkBaseline("raw-read-write", [
      {
        ...subMillisecondObservation,
        benchmarks: [
          {
            ...subMillisecondObservation.benchmarks[0],
            meanMs: 0.7,
            p99Ms: 0.3,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 0.125ms to 0.700ms; allowed <= 0.625ms.",
      ],
    });
  });


  it("keeps raw read/write millisecond-scale runner spikes inside the focused window", () => {
    const indexedSnapshotObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 1.401,
          p99Ms: 2.284,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("raw-read-write", [indexedSnapshotObservation]);
    const actual = buildBenchmarkBaseline("raw-read-write", [
      {
        ...indexedSnapshotObservation,
        benchmarks: [
          {
            ...indexedSnapshotObservation.benchmarks[0],
            meanMs: 4.599,
            p99Ms: 20.404,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


  it("rejects raw read/write p99 regressions outside the focused tail window", () => {
    const indexedSnapshotObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          meanMs: 1.401,
          p99Ms: 2.284,
        },
      ],
    };
    const baseline = buildBenchmarkBaseline("raw-read-write", [indexedSnapshotObservation]);
    const actual = buildBenchmarkBaseline("raw-read-write", [
      {
        ...indexedSnapshotObservation,
        benchmarks: [
          {
            ...indexedSnapshotObservation.benchmarks[0],
            meanMs: 4.599,
            p99Ms: 30,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 2.284ms to 30.000ms; allowed <= 22.284ms.",
      ],
    });
  });


  it("reports missing tasks, counter regressions, memory regressions, and latency regressions", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const regressed = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        backpressureCount: 1,
        benchmarks: [
          {
            groupName: "src/example.bench.ts > example benchmark group",
            maxMs: 300,
            meanMs: 40,
            minMs: 1,
            name: "case a",
            p99Ms: 100,
            sampleCount: 7,
          },
        ],
        cleanupLeakCount: 1,
        memoryRssTotalDeltaBytes: 200 * 1024 * 1024,
        queuedEventCount: 1,
      },
    ]);
    const withMissingTask = {
      ...regressed,
      tasks: [
        ...regressed.tasks,
        {
          ...observation,
          taskLabel: "extra actual task",
        },
      ],
    };

    expect(
      compareArtifacts(
        {
          ...baseline,
          tasks: [
            ...baseline.tasks,
            {
              ...observation,
              taskLabel: "missing task",
            },
          ],
        },
        withMissingTask,
      ),
    ).toStrictEqual({
      ok: false,
      regressions: [
        "extra actual task: unexpected benchmark task in actual run.",
        "task a: cleanupLeakCount must stay 0 but was 1.",
        "task a: backpressureCount must stay 0 but was 1.",
        "task a: queuedEventCount must stay 0 but was 1.",
        "task a: total RSS delta regressed from 1024 bytes to 209715200 bytes; allowed <= 3072 bytes.",
        "task a / src/example.bench.ts > example benchmark group / case a: mean regressed from 2.000ms to 40.000ms; allowed <= 16.000ms.",
        "task a / src/example.bench.ts > example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 24.000ms.",
        "missing task: missing benchmark task in actual run.",
      ],
    });
  });

	  it("reports missing benchmark cases", () => {
	    const baseline = buildBenchmarkBaseline("smoke", [observation]);
	    const changedCases = buildBenchmarkBaseline("smoke", [
	      {
	        ...observation,
	        benchmarks: [
	          {
	            ...observation.benchmarks[0],
	            name: "case b",
	          },
	        ],
	        benchmarkCases: ["case b"],
	      },
	    ]);

	    expect(compareArtifacts(baseline, changedCases)).toStrictEqual({
	      ok: false,
	      regressions: [
	        'task a: benchmarkCases changed from ["case a"] to ["case b"].',
	        "task a: unexpected benchmark case src/example.bench.ts > example benchmark group / case b.",
	        "task a: missing benchmark case src/example.bench.ts > example benchmark group / case a.",
	      ],
    });
  });


});

describe("benchmark runtime and transport invariants", () => {
  it("does not require Kafka read snapshot gates for non-Kafka throughput profiles", () => {
    const nonKafkaThroughputObservation = {
      ...observation,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("smoke", [nonKafkaThroughputObservation]);
    const actual = buildBenchmarkBaseline("smoke", [nonKafkaThroughputObservation]);

    expect(compareArtifacts(baseline, actual)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });


  it("requires exact Kafka ingest mutation counts", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const changedMutationCount = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        mutationCount: 1400,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 2000,
            meanRowsPerSecond: 2000,
            minRowsPerSecond: 1800,
            producedRowsPerSample: 200,
            totalProducedRows: 1400,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, changedMutationCount)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: mutationCount changed from 700 to 1400.",
        "task a: case a throughput producedRowsPerSample changed from 100 to 200.",
        "task a: case a throughput totalProducedRows changed from 700 to 1400.",
      ],
    });
  });


  it("requires exact WebSocket firehose mutation counts", () => {
    const websocketObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-websocket-firehose",
      groupedWriteAdmission: undefined,
      mutationCount: 5,
    };
    const baseline = buildBenchmarkBaseline("websocket-firehose", [websocketObservation]);
    const increasedMutationCount = buildBenchmarkBaseline("websocket-firehose", [
      {
        ...websocketObservation,
        mutationCount: 10,
      },
    ]);

    expect(compareArtifacts(baseline, increasedMutationCount)).toStrictEqual({
      ok: false,
      regressions: ["task a: mutationCount changed from 5 to 10."],
    });
  });


  it("rejects task mutation counts that diverge from leased raw evidence", () => {
    const grpcObservation = {
      ...completeGrpcLeasedObservation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-grpc-leased",
      grpcParameters: {
        retainedRows: 500,
        routeCount: 25,
        rowsPerFeed: 50,
      },
      groupedWriteAdmission: undefined,
      runtimeOperationCases: runtimeGrpcLeasedOperationCases,
    };
    const baseline = buildBenchmarkBaseline("grpc-leased", [grpcObservation]);
    const increasedMutationCount = buildBenchmarkBaseline("grpc-leased", [
      {
        ...grpcObservation,
        mutationCount: completeGrpcLeasedObservation.mutationCount + 1,
      },
    ]);

    expect(() => compareArtifacts(baseline, increasedMutationCount)).toThrow(
      `Benchmark artifact field actual.tasks[0].runtimeOperationCases mutationCount total must equal task mutationCount ${completeGrpcLeasedObservation.mutationCount + 1} but was ${completeGrpcLeasedObservation.mutationCount}.`,
    );
  });


  it("reports Kafka ingest throughput regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 400,
            maxTotalMs: 250,
            meanTotalMs: 250,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, regressedThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 400.000 rows/sec; allowed >= 750.000 rows/sec.",
      ],
    });
  });


  it("reports Kafka commit-observed latency regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedCommitObserved = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 100_000 / 2_100,
            maxCommitObservedMs: 2_600,
            maxTotalMs: 2_600,
            meanCommitObservedMs: 2_076,
            meanRowsPerSecond: 100_000 / 2_100,
            meanTotalMs: 2_100,
            minRowsPerSecond: 40,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, regressedCommitObserved)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 47.619 rows/sec; allowed >= 750.000 rows/sec.",
        "task a / case a: meanCommitObservedMs regressed from 75.000ms to 2076.000ms; allowed <= 2075.000ms.",
        "task a / case a: maxCommitObservedMs regressed from 80.000ms to 2600.000ms; allowed <= 2580.000ms.",
      ],
    });
  });


  it("reports Kafka read snapshot workload and latency regressions", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const regressedReadSnapshot = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxReadSnapshotMs: 61,
            meanReadSnapshotMs: 41,
            readSnapshotRowsPerSample: 10,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, regressedReadSnapshot)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: case a throughput readSnapshotRowsPerSample changed from 25 to 10.",
        "task a / case a: meanReadSnapshotMs regressed from 5.000ms to 41.000ms; allowed <= 40.000ms.",
        "task a / case a: maxReadSnapshotMs regressed from 6.000ms to 61.000ms; allowed <= 60.000ms.",
      ],
    });
  });


  it("uses the Kafka profile throughput threshold", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const thresholdThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 750,
            maxTotalMs: 133.33333333333334,
            meanRowsPerSecond: 750,
            meanTotalMs: 133.33333333333334,
            minRowsPerSecond: 700,
          },
        ],
      },
    ]);
    const belowThresholdThroughput = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 749,
            maxTotalMs: 133.5113484646195,
            meanRowsPerSecond: 749,
            meanTotalMs: 133.5113484646195,
            minRowsPerSecond: 700,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, thresholdThroughput)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareArtifacts(baseline, belowThresholdThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / case a: aggregateRowsPerSecond throughput regressed from 1000.000 rows/sec to 749.000 rows/sec; allowed >= 750.000 rows/sec.",
      ],
    });
  });


  it("reports missing runtime metrics without gating noisy runtime metric values", () => {
    const withRuntimeMetrics = {
      ...observation,
      runtimeMetrics,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withRuntimeMetrics]);
    const changedRuntimeMetrics = buildBenchmarkBaseline("smoke", [
      {
        ...withRuntimeMetrics,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 999,
            meanMs: 777,
            p99Ms: 888,
          },
        },
      },
    ]);
    const missingRuntimeMetrics = buildBenchmarkBaseline("smoke", [observation]);

    expect(compareArtifacts(baseline, changedRuntimeMetrics)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareArtifacts(baseline, missingRuntimeMetrics)).toStrictEqual({
      ok: false,
      regressions: ["task a: runtimeMetrics presence changed."],
    });
  });


  it("requires drained final Kafka lag for sustained firehose baselines", () => {
    const twoLaneKafkaIngestLanes = [
      ...comparableRuntimeThroughputKafkaIngestLanes,
      {
        internalTopic: "trades",
        lane: "trades",
        producedRows: runtimeThroughputMutationCount,
        region: "local",
        sourceTopicAlias: "unique-topic-per-run:trades",
      },
    ];
    const sustainedFirehoseObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-sustained-firehose",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: twoLaneKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      runtimeMetrics: {
        ...drainedRuntimeMetrics,
        kafkaLag: {
          ...drainedRuntimeMetrics.kafkaLag,
          sampledRegionCount: twoLaneKafkaIngestLanes.length,
        },
      },
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      sustainedFirehoseObservation,
    ]);
    const nonZeroLagBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics,
      },
    ]);
    const partialLagSampleBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: drainedRuntimeMetrics,
      },
    ]);
    const missingMaxLagBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: {
          ...sustainedFirehoseObservation.runtimeMetrics,
          kafkaLag: {
            ...sustainedFirehoseObservation.runtimeMetrics.kafkaLag,
            maxConsumerLagMessages: null,
          },
        },
      },
    ]);
    const missingRuntimeMetricsBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        runtimeMetrics: undefined,
      },
    ]);
    const missingKafkaIngestLanesBaseline = buildBenchmarkBaseline("kafka-sustained-firehose", [
      {
        ...sustainedFirehoseObservation,
        kafkaIngestLanes: undefined,
      },
    ]);
    const missingKafkaIngestLanesWithArtifactKindDriftBaseline = buildBenchmarkBaseline(
      "kafka-sustained-firehose",
      [
        {
          ...sustainedFirehoseObservation,
          artifactKind: "engine-benchmark-summary",
          kafkaIngestLanes: undefined,
        },
      ],
    );

    expect(compareArtifacts(baseline, baseline)).toStrictEqual({
      ok: true,
      regressions: [],
    });
    expect(compareArtifacts(baseline, nonZeroLagBaseline)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: runtimeMetrics.kafkaLag sampled 1 regions but expected 2.",
        "task a: final Kafka lag must be 0 but was 9007199254740993.",
        "task a: max final Kafka lag must be 0 but was 9007199254740993.",
      ],
    });
    expect(compareArtifacts(baseline, partialLagSampleBaseline)).toStrictEqual({
      ok: false,
      regressions: ["task a: runtimeMetrics.kafkaLag sampled 1 regions but expected 2."],
    });
    expect(compareArtifacts(baseline, missingMaxLagBaseline)).toStrictEqual({
      ok: false,
      regressions: ["task a: max final Kafka lag must be 0 but was null."],
    });
    expect(compareArtifacts(baseline, missingRuntimeMetricsBaseline)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: runtimeMetrics presence changed.",
        "task a: runtimeMetrics.kafkaLag is required.",
      ],
    });
    expect(() => validateBenchmarkBaseline(missingKafkaIngestLanesBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].kafkaIngestLanes is required for runtime-kafka-sustained-firehose.",
    );
    expect(() =>
      validateBenchmarkBaseline(missingKafkaIngestLanesWithArtifactKindDriftBaseline),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].kafkaIngestLanes is required for runtime-kafka-sustained-firehose.",
    );
  });


});

describe("benchmark metadata compatibility", () => {
  it("reports throughput metadata drift", () => {
    const withThroughput = {
      ...observation,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withThroughput]);
    const missingThroughput = buildBenchmarkBaseline("smoke", [observation]);
    const renamedThroughput = buildBenchmarkBaseline("smoke", [
      {
        ...withThroughput,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            name: "case b",
          },
        ],
        benchmarkCases: ["case b"],
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            name: "case b",
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, missingThroughput)).toStrictEqual({
      ok: false,
      regressions: ["task a: throughputCases presence changed."],
    });
    expect(compareArtifacts(baseline, renamedThroughput)).toStrictEqual({
      ok: false,
      regressions: [
        'task a: benchmarkCases changed from ["case a"] to ["case b"].',
        "task a: unexpected throughput case case b.",
        "task a: missing throughput case case a.",
        "task a: unexpected benchmark case src/example.bench.ts > example benchmark group / case b.",
        "task a: missing benchmark case src/example.bench.ts > example benchmark group / case a.",
      ],
    });
  });


  it("reports benchmark sample counts below the task minimum", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const changedSampleCount = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [
          {
            ...observation.benchmarks[0],
            sampleCount: 1,
          },
        ],
      },
    ]);

    expect(compareArtifacts(baseline, changedSampleCount)).toStrictEqual({
      ok: false,
      regressions: [
        "task a / src/example.bench.ts > example benchmark group / case a: sampleCount must be at least 5 but was 1.",
      ],
    });
  });


  it("requires exact raw write sample and mutation counts", () => {
    const rawWriteObservation = {
      ...observation,
      benchmarks: [
        {
          ...observation.benchmarks[0],
          sampleCount: 10,
        },
      ],
      benchmarkScope: "engine-raw-write",
      minimumSampleCount: 10,
    };
    const baseline = buildBenchmarkBaseline("raw-read-write", [rawWriteObservation]);
    const changedRawWriteShape = buildBenchmarkBaseline("raw-read-write", [
      {
        ...rawWriteObservation,
        benchmarks: [
          {
            ...rawWriteObservation.benchmarks[0],
            sampleCount: 11,
          },
        ],
        mutationCount: rawWriteObservation.mutationCount + 1,
      },
    ]);

    expect(compareArtifacts(baseline, changedRawWriteShape)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: mutationCount changed from 100 to 101.",
        "task a / src/example.bench.ts > example benchmark group / case a: sampleCount changed from 10 to 11.",
      ],
    });
  });


  it("reports benchmark metadata drift", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const changedMetadata = buildBenchmarkBaseline("smoke", [
	      {
	        ...observation,
	        artifactKind: "react-browser-benchmark-summary",
	        benchmarks: [
	          {
	            ...observation.benchmarks[0],
	            name: "case b",
	          },
	        ],
	        benchmarkCases: ["case b"],
	        benchmarkName: "other benchmark",
        benchmarkScope: "react-in-memory-live-query",
        browser: {
          browser: "firefox",
          provider: "playwright",
        },
        groupedWriteAdmission: undefined,
        groupedKeyWidthParameters: {
          constantGroupCount: 257,
          keyWidths: [1, 2, 4, 8],
          orderedKeyCount: 8,
          semanticProbe: {
            groupByEightOrderedTotalRows: 4,
            groupByEightTotalRows: 5,
            groupByFourTotalRows: 3,
            groupByOneTotalRows: 1,
            groupByTwoTotalRows: 2,
            orderedFirstGroupKey8: "probe-8-z",
            orderedFirstRowCount: "10",
            orderedSecondGroupKey8: "probe-8-y",
            orderedSecondRowCount: "9",
            orderedWindowRows: 4,
          },
          windowLimit: 250,
        },
        latencySource: "other-source",
        memoryRssTotalDeltaBytes: undefined,
        minimumSampleCount: 1,
        mutationCount: 1,
        outputJsonPath: "different.json",
        rowCount: 10,
        seedBatchSize: 10,
        subscriberCount: 2,
        summaryPath: "different.summary.json",
        topics: ["trades"],
      },
    ]);

    expect(compareArtifacts(baseline, changedMetadata)).toStrictEqual({
      ok: false,
      regressions: [
        "task a: artifactKind changed from engine-benchmark-summary to react-browser-benchmark-summary.",
        "task a: benchmarkScope changed from engine-raw-snapshot to react-in-memory-live-query.",
        "task a: benchmarkName changed from example benchmark to other benchmark.",
        'task a: benchmarkCases changed from ["case a"] to ["case b"].',
        "task a: rowCount changed from 100 to 10.",
        "task a: mutationCount dropped from 100 to 1; allowed >= 90.",
        "task a: subscriberCount changed from 1 to 2.",
        'task a: topics changed from ["orders"] to ["trades"].',
        "task a: latencySource changed from vitest-output-json to other-source.",
        'task a: browser changed from undefined to {"browser":"firefox","provider":"playwright"}.',
        "task a: seedBatchSize changed from undefined to 10.",
        'task a: groupedKeyWidthParameters changed from undefined to {"constantGroupCount":257,"keyWidths":[1,2,4,8],"orderedKeyCount":8,"semanticProbe":{"groupByEightOrderedTotalRows":4,"groupByEightTotalRows":5,"groupByFourTotalRows":3,"groupByOneTotalRows":1,"groupByTwoTotalRows":2,"orderedFirstGroupKey8":"probe-8-z","orderedFirstRowCount":"10","orderedSecondGroupKey8":"probe-8-y","orderedSecondRowCount":"9","orderedWindowRows":4},"windowLimit":250}.',
        'task a: groupedWriteAdmission changed from {"configuredMode":"incremental","expectedAdmission":"incremental"} to undefined.',
        "task a: minimumSampleCount changed from 5 to 1.",
	        "task a: outputJsonPath changed from actual.json to different.json.",
	        "task a: summaryPath changed from actual.summary.json to different.summary.json.",
	        "task a: memoryRssTotalDeltaBytes presence changed between baseline and actual run.",
	        "task a: unexpected benchmark case src/example.bench.ts > example benchmark group / case b.",
	        "task a: missing benchmark case src/example.bench.ts > example benchmark group / case a.",
	      ],
	    });
	  });


  it("accepts browser benchmark manifests without process memory when the baseline also omits memory", () => {
    const withoutMemoryObservation = {
      ...observation,
      artifactKind: "react-browser-benchmark-summary",
      benchmarkScope: "react-in-memory-live-query",
      memoryRssTotalDeltaBytes: undefined,
    };
    const baseline = buildBenchmarkBaseline("smoke", [withoutMemoryObservation]);
    const withoutMemory = buildBenchmarkBaseline("smoke", [
      {
        ...withoutMemoryObservation,
        memoryRssTotalDeltaBytes: undefined,
      },
    ]);

    expect(compareArtifacts(baseline, withoutMemory)).toStrictEqual({
      ok: true,
      regressions: [],
    });
  });

  it("rejects duplicate task labels in baseline manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation, observation]);

    expect(() => compareArtifacts(baseline, baseline)).toThrow(
      "Benchmark artifact field baseline.tasks contains duplicate taskLabel: task a.",
    );
  });

  it("rejects duplicate benchmark cases in baseline manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarks: [observation.benchmarks[0], observation.benchmarks[0]],
      },
    ]);

    expect(() => compareArtifacts(baseline, baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[task a].benchmarks contains duplicate benchmark case: src/example.bench.ts > example benchmark group / case a.",
    );
  });

});
