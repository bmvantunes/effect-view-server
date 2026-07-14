import { describe, expect, it } from "@effect/vitest";

import {
  compareGrpcLeasedBenchmarkTask,
  decodeGrpcLeasedBenchmarkParameters,
  decodeGrpcLeasedOperationCases,
  decodeGrpcLeasedSeedMutationCount,
  validateGrpcLeasedOperationAccounting,
} from "./grpc-leased-benchmark-policy.mjs";

const operationCasesPath = "baseline.tasks[0].runtimeOperationCases";
const grpcParameters = {
  retainedRows: 500,
  routeCount: 25,
  rowsPerFeed: 50,
};
const sampleCount = 5;
const cleanMeasuredCleanup = {
  activeLeasedFeeds: 0,
  activeSubscriptions: 0,
  activeViews: 0,
  clientActiveFeeds: 0,
  leakCount: 0,
  queuedEvents: 0,
  rowCount: 0,
};

const workloadCases = [
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 50,
    mutationCount: 50,
    name: "gRPC leased first subscriber",
    rows: 50,
    seedMutationCount: 0,
    subscriberCount: 1,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 50,
    mutationCount: 50,
    name: "gRPC leased same-route reuse",
    rows: 50,
    seedMutationCount: 0,
    subscriberCount: 10,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 50,
    mutationCount: 50,
    name: "gRPC leased one route many subscribers",
    rows: 50,
    seedMutationCount: 0,
    subscriberCount: 50,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 50,
    mutationCount: 50,
    name: "gRPC leased local-filter live snapshot",
    rows: 50,
    seedMutationCount: 0,
    subscriberCount: 1,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 500,
    mutationCount: 0,
    name: "gRPC leased retained local-filter snapshot",
    rows: 500,
    seedMutationCount: 500,
    subscriberCount: 2,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 51,
    mutationCount: 50,
    name: "gRPC leased delta fanout",
    rows: 50,
    seedMutationCount: 1,
    subscriberCount: 25,
  },
  {
    activeLeasedFeeds: 25,
    feedCount: 25,
    measurementRowCount: 1_250,
    mutationCount: 1_250,
    name: "gRPC leased partitioned write convergence",
    rows: 1_250,
    seedMutationCount: 0,
    subscriberCount: 25,
  },
  {
    activeLeasedFeeds: 25,
    feedCount: 25,
    measurementRowCount: 25,
    mutationCount: 0,
    name: "gRPC leased health refresh overhead",
    rows: 0,
    seedMutationCount: 25,
    subscriberCount: 25,
  },
  {
    activeLeasedFeeds: 1,
    feedCount: 1,
    measurementRowCount: 50,
    mutationCount: 50,
    name: "gRPC leased last-subscriber cleanup",
    rows: 50,
    seedMutationCount: 0,
    subscriberCount: 1,
  },
  {
    activeLeasedFeeds: 25,
    feedCount: 25,
    measurementRowCount: 25,
    mutationCount: 25,
    name: "gRPC leased many routes",
    rows: 25,
    seedMutationCount: 0,
    subscriberCount: 25,
  },
] as const;

const sampleFor = (workloadCase: (typeof workloadCases)[number]) => {
  const snapshotMs = workloadCase.rows === 0 ? 0 : 10;
  return {
    acquiredFeedCount: workloadCase.feedCount,
    activeLeasedFeeds: workloadCase.activeLeasedFeeds,
    backpressureCount: 0,
    cleanupActiveLeasedFeeds: 0,
    cleanupClientActiveFeeds: 0,
    cleanupLeakCount: 0,
    cleanupMs: 2,
    cleanupRowCount: 0,
    deltaFanoutMs: workloadCase.name === "gRPC leased delta fanout" ? 10 : 0,
    healthOverlayMs: 1,
    measurementRowCount: workloadCase.measurementRowCount,
    measuredCleanup: cleanMeasuredCleanup,
    mutationCount: workloadCase.mutationCount,
    name: workloadCase.name,
    queuedEventCount: 0,
    releasedFeedCount: workloadCase.feedCount,
    rows: workloadCase.rows,
    rowsPerSecond: snapshotMs === 0 ? 0 : (workloadCase.rows / snapshotMs) * 1_000,
    seedMutationCount: workloadCase.seedMutationCount,
    snapshotMs,
    subscriberCount: workloadCase.subscriberCount,
    subscriptionMs: 3,
  };
};

const operationCaseFor = (workloadCase: (typeof workloadCases)[number]) => {
  const sample = sampleFor(workloadCase);
  const samples = Array.from({ length: sampleCount }, () => ({ ...sample }));
  return {
    maxActiveLeasedFeeds: sample.activeLeasedFeeds,
    maxCleanupActiveLeasedFeeds: 0,
    maxCleanupClientActiveFeeds: 0,
    maxCleanupMs: sample.cleanupMs,
    maxDeltaFanoutMs: sample.deltaFanoutMs,
    maxHealthOverlayMs: sample.healthOverlayMs,
    maxMeasuredCleanupActiveLeasedFeeds: 0,
    maxMeasuredCleanupActiveSubscriptions: 0,
    maxMeasuredCleanupActiveViews: 0,
    maxMeasuredCleanupClientActiveFeeds: 0,
    maxMeasuredCleanupLeakCount: 0,
    maxMeasuredCleanupQueuedEvents: 0,
    maxMeasuredCleanupRowCount: 0,
    maxSnapshotMs: sample.snapshotMs,
    maxSubscriptionMs: sample.subscriptionMs,
    meanCleanupMs: sample.cleanupMs,
    meanDeltaFanoutMs: sample.deltaFanoutMs,
    meanHealthOverlayMs: sample.healthOverlayMs,
    meanRowsPerSecond: sample.rowsPerSecond,
    meanSnapshotMs: sample.snapshotMs,
    meanSubscriptionMs: sample.subscriptionMs,
    medianRowsPerSecond: sample.rowsPerSecond,
    mutationCount: sample.mutationCount * sampleCount,
    name: sample.name,
    pooledRowsPerSecond: sample.rowsPerSecond,
    rowsPerSecondCoefficientOfVariation: 0,
    sampleCount,
    samples,
    seedMutationCount: sample.seedMutationCount * sampleCount,
  };
};

const operationCases = workloadCases.map(operationCaseFor);
const mutationCount = operationCases.reduce(
  (total, operationCase) => total + operationCase.mutationCount,
  0,
);
const seedMutationCount = operationCases.reduce(
  (total, operationCase) => total + operationCase.seedMutationCount,
  0,
);

const decodeAndValidate = ({
  cases = operationCases,
  parameters = grpcParameters,
  taskMutationCount = mutationCount,
  taskSeedMutationCount = seedMutationCount,
} = {}) => {
  const decodedParameters = decodeGrpcLeasedBenchmarkParameters(
    parameters,
    "baseline.tasks[0].grpcParameters",
  );
  const decodedCases = decodeGrpcLeasedOperationCases(cases, operationCasesPath);
  validateGrpcLeasedOperationAccounting(
    decodedCases,
    decodedParameters,
    taskMutationCount,
    decodeGrpcLeasedSeedMutationCount(
      taskSeedMutationCount,
      "baseline.tasks[0].seedMutationCount",
    ),
    operationCasesPath,
  );
  return decodedCases;
};

const thresholds = {
  operationMax: {
    maxAbsoluteDeltaMs: 100,
    maxRatio: 16,
  },
  operationMean: {
    maxAbsoluteDeltaMs: 25,
    maxRatio: 12,
  },
  throughputAggregateRowsPerSecond: {
    minRatio: 0.5,
  },
};

const benchmarkTask = {
  runtimeOperationCases: operationCases,
  seedMutationCount,
};

describe("gRPC leased benchmark artifact policy", () => {
  it("decodes the exact raw evidence and reconciles every workload with gRPC parameters", () => {
    expect(decodeAndValidate()).toStrictEqual(operationCases);
  });

  it("rejects extra raw and summary fields", () => {
    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...operationCases[0],
            samples: [
              {
                ...operationCases[0].samples[0],
                health: {},
              },
              ...operationCases[0].samples.slice(1),
            ],
          },
        ],
        taskMutationCount: operationCases[0].mutationCount,
        taskSeedMutationCount: operationCases[0].seedMutationCount,
      }),
    ).toThrow(
      "must contain exactly these keys: acquiredFeedCount, activeLeasedFeeds, backpressureCount, cleanupActiveLeasedFeeds, cleanupClientActiveFeeds, cleanupLeakCount, cleanupMs, cleanupRowCount, deltaFanoutMs, healthOverlayMs, measuredCleanup, measurementRowCount, mutationCount, name, queuedEventCount, releasedFeedCount, rows, rowsPerSecond, seedMutationCount, snapshotMs, subscriberCount, subscriptionMs.",
    );

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...operationCases[0],
            extra: true,
          },
        ],
        taskMutationCount: operationCases[0].mutationCount,
        taskSeedMutationCount: operationCases[0].seedMutationCount,
      }),
    ).toThrow("must contain exactly these keys: maxActiveLeasedFeeds");
  });

  it("rejects sample contamination and inconsistent per-case accounting", () => {
    const firstCase = operationCases[0];
    expect(() =>
      decodeAndValidate({
        cases: [{ ...firstCase, samples: firstCase.samples.slice(1) }],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("samples must contain exactly 5 samples but contained 4.");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], name: "gRPC leased same-route reuse" },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("name must equal operation case name gRPC leased first subscriber.");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              firstCase.samples[0],
              { ...firstCase.samples[1], subscriberCount: 2 },
              ...firstCase.samples.slice(2),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("samples must preserve identical deterministic state across samples.");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], releasedFeedCount: 0 },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("releasedFeedCount must equal acquiredFeedCount 1 but was 0.");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], cleanupRowCount: 1 },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("must record zero measured cleanup state, emergency cleanup state");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], cleanupClientActiveFeeds: 1 },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("must record zero measured cleanup state, emergency cleanup state");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              {
                ...firstCase.samples[0],
                measuredCleanup: {
                  ...firstCase.samples[0].measuredCleanup,
                  activeSubscriptions: 1,
                  leakCount: 1,
                },
              },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("must record zero measured cleanup state, emergency cleanup state");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], rowsPerSecond: 0, snapshotMs: 0 },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("snapshotMs must be positive when rows are measured.");

    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            samples: [
              { ...firstCase.samples[0], rowsPerSecond: 1 },
              ...firstCase.samples.slice(1),
            ],
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("rowsPerSecond must equal the value derived from samples (5000) but was 1.");

    expect(() =>
      decodeAndValidate({
        cases: [{ ...firstCase, mutationCount: firstCase.mutationCount - 1 }],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("mutationCount must equal measured sample mutations 250 but was 249.");

    expect(() =>
      decodeAndValidate({
        cases: [{ ...firstCase, seedMutationCount: 1 }],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("seedMutationCount must equal seeded sample mutations 0 but was 1.");
  });

  it("recomputes every summary statistic from the raw samples", () => {
    const firstCase = operationCases[0];
    for (const field of [
      "maxActiveLeasedFeeds",
      "maxCleanupActiveLeasedFeeds",
      "maxCleanupClientActiveFeeds",
      "maxCleanupMs",
      "maxDeltaFanoutMs",
      "maxHealthOverlayMs",
      "maxMeasuredCleanupActiveLeasedFeeds",
      "maxMeasuredCleanupActiveSubscriptions",
      "maxMeasuredCleanupActiveViews",
      "maxMeasuredCleanupClientActiveFeeds",
      "maxMeasuredCleanupLeakCount",
      "maxMeasuredCleanupQueuedEvents",
      "maxMeasuredCleanupRowCount",
      "maxSnapshotMs",
      "maxSubscriptionMs",
      "meanCleanupMs",
      "meanDeltaFanoutMs",
      "meanHealthOverlayMs",
      "meanRowsPerSecond",
      "meanSnapshotMs",
      "meanSubscriptionMs",
      "medianRowsPerSecond",
      "pooledRowsPerSecond",
      "rowsPerSecondCoefficientOfVariation",
    ]) {
      expect(() =>
        decodeAndValidate({
          cases: [{ ...firstCase, [field]: firstCase[field] + 1 }],
          taskMutationCount: firstCase.mutationCount,
          taskSeedMutationCount: firstCase.seedMutationCount,
        }),
      ).toThrow(`${field} must equal the value derived from samples`);
    }
  });

  it("rejects unknown, duplicate, and parameter-inconsistent workload evidence", () => {
    const firstCase = operationCases[0];
    const missingCase = operationCases[9];
    expect(() =>
      decodeAndValidate({
        cases: [
          {
            ...firstCase,
            name: "unknown case",
            samples: firstCase.samples.map((sample) => ({ ...sample, name: "unknown case" })),
          },
        ],
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("name must be one of: gRPC leased first subscriber");

    expect(() =>
      decodeAndValidate({
        cases: [firstCase, firstCase],
        taskMutationCount: firstCase.mutationCount * 2,
        taskSeedMutationCount: firstCase.seedMutationCount * 2,
      }),
    ).toThrow(
      "contains duplicate gRPC leased operation case: gRPC leased first subscriber.",
    );

    expect(() =>
      decodeAndValidate({
        cases: [firstCase],
        parameters: { ...grpcParameters, rowsPerFeed: 51 },
        taskMutationCount: firstCase.mutationCount,
        taskSeedMutationCount: firstCase.seedMutationCount,
      }),
    ).toThrow("workload state must equal");

    expect(() =>
      decodeAndValidate({
        cases: operationCases.slice(0, -1),
        taskMutationCount: mutationCount - missingCase.mutationCount,
        taskSeedMutationCount: seedMutationCount - missingCase.seedMutationCount,
      }),
    ).toThrow(
      `is missing gRPC leased operation case: ${missingCase.name}.`,
    );

    expect(() => decodeAndValidate({ taskMutationCount: mutationCount - 1 })).toThrow(
      `mutationCount total must equal task mutationCount ${mutationCount - 1} but was ${mutationCount}.`,
    );
    expect(() => decodeAndValidate({ taskSeedMutationCount: seedMutationCount - 1 })).toThrow(
      `seedMutationCount total must equal task seedMutationCount ${seedMutationCount - 1} but was ${seedMutationCount}.`,
    );
  });

  it("requires positive gRPC parameters and non-negative task seed mutations", () => {
    expect(() =>
      decodeGrpcLeasedBenchmarkParameters(
        { ...grpcParameters, routeCount: 0 },
        "baseline.tasks[0].grpcParameters",
      ),
    ).toThrow("routeCount must be a positive integer.");
    expect(() =>
      decodeGrpcLeasedSeedMutationCount(-1, "baseline.tasks[0].seedMutationCount"),
    ).toThrow("seedMutationCount must be a non-negative integer.");
  });

  it("compares state and accounting exactly while gating pooled throughput", () => {
    const firstCase = operationCases[0];
    const stateChanged = {
      ...firstCase,
      samples: firstCase.samples.map((sample) => ({ ...sample, subscriberCount: 2 })),
    };
    const regressed = {
      ...firstCase,
      maxActiveLeasedFeeds: 2,
      maxSnapshotMs: 1000,
      meanRowsPerSecond: 1,
      medianRowsPerSecond: 1,
      pooledRowsPerSecond: 2_000,
      rowsPerSecondCoefficientOfVariation: 10,
      samples: stateChanged.samples,
    };

    expect(
      compareGrpcLeasedBenchmarkTask("task a", thresholds, benchmarkTask, benchmarkTask),
    ).toStrictEqual([]);
    expect(
      compareGrpcLeasedBenchmarkTask("task a", thresholds, benchmarkTask, {
        runtimeOperationCases: [regressed, ...operationCases.slice(1)],
        seedMutationCount: seedMutationCount + 1,
      }),
    ).toStrictEqual([
      `task a: seedMutationCount changed from ${seedMutationCount} to ${seedMutationCount + 1}.`,
      "task a: gRPC leased first subscriber runtime operation maxActiveLeasedFeeds changed from 1 to 2.",
      'task a: gRPC leased first subscriber runtime operation sample state changed from [{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":1},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":1},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":1},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":1},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":1}] to [{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":2},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":2},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":2},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":2},{"acquiredFeedCount":1,"activeLeasedFeeds":1,"backpressureCount":0,"cleanupActiveLeasedFeeds":0,"cleanupLeakCount":0,"cleanupRowCount":0,"measurementRowCount":50,"mutationCount":50,"name":"gRPC leased first subscriber","queuedEventCount":0,"releasedFeedCount":1,"rows":50,"seedMutationCount":0,"subscriberCount":2}].',
      "task a / gRPC leased first subscriber: maxSnapshotMs regressed from 10.000ms to 1000.000ms; allowed <= 160.000ms.",
      "task a / gRPC leased first subscriber: pooledRowsPerSecond throughput regressed from 5000.000 rows/sec to 2000.000 rows/sec; allowed >= 2500.000 rows/sec.",
    ]);
  });

  it("keeps zero timings and zero pooled throughput exact", () => {
    const healthCase = operationCases[7];
    const changed = {
      ...healthCase,
      maxDeltaFanoutMs: 1,
      meanDeltaFanoutMs: 1,
      pooledRowsPerSecond: 1,
    };
    expect(
      compareGrpcLeasedBenchmarkTask(
        "task a",
        thresholds,
        { runtimeOperationCases: [healthCase], seedMutationCount: healthCase.seedMutationCount },
        { runtimeOperationCases: [changed], seedMutationCount: healthCase.seedMutationCount },
      ),
    ).toStrictEqual([
      "task a: gRPC leased health refresh overhead runtime operation meanDeltaFanoutMs changed from 0 to 1.",
      "task a: gRPC leased health refresh overhead runtime operation maxDeltaFanoutMs changed from 0 to 1.",
      "task a: gRPC leased health refresh overhead pooledRowsPerSecond changed from 0 to 1.",
    ]);
  });

  it("reports operation-case presence, membership, and duplicates", () => {
    const firstCase = operationCases[0];
    const secondCase = operationCases[1];
    expect(
      compareGrpcLeasedBenchmarkTask(
        "task a",
        thresholds,
        { runtimeOperationCases: undefined, seedMutationCount: 0 },
        { runtimeOperationCases: undefined, seedMutationCount: 0 },
      ),
    ).toStrictEqual([]);
    expect(
      compareGrpcLeasedBenchmarkTask(
        "task a",
        thresholds,
        { runtimeOperationCases: [firstCase], seedMutationCount: 0 },
        { runtimeOperationCases: undefined, seedMutationCount: 0 },
      ),
    ).toStrictEqual(["task a: runtimeOperationCases presence changed."]);
    expect(
      compareGrpcLeasedBenchmarkTask(
        "task a",
        thresholds,
        { runtimeOperationCases: [firstCase], seedMutationCount: 0 },
        { runtimeOperationCases: [secondCase], seedMutationCount: 0 },
      ),
    ).toStrictEqual([
      "task a: unexpected runtime operation case gRPC leased same-route reuse.",
      "task a: missing runtime operation case gRPC leased first subscriber.",
    ]);
    expect(() =>
      compareGrpcLeasedBenchmarkTask(
        "task a",
        thresholds,
        { runtimeOperationCases: [firstCase, firstCase], seedMutationCount: 0 },
        { runtimeOperationCases: [firstCase], seedMutationCount: 0 },
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[task a] contains duplicate runtime operation case: gRPC leased first subscriber.",
    );
  });
});
