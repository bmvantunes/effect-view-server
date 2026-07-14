import { describe, expect, it } from "@effect/vitest";

import {
  compareGrpcMaterializedBenchmarkTask,
  decodeGrpcMaterializedBenchmarkParameters,
  decodeGrpcMaterializedOperationCases,
  decodeGrpcMaterializedSeedMutationCount,
  validateGrpcMaterializedOperationAccounting,
} from "./grpc-materialized-benchmark-policy.mjs";

const operationCasesPath = "baseline.tasks[0].runtimeOperationCases";
const parameters = {
  batchSize: 100,
  seedRows: 1000,
};
const sampleCount = 7;

const sampleFor = (name: string, rows: number, resultRowId: string | null) => ({
  backpressureCount: 0,
  cleanupLeakCount: 0,
  cleanupMs: 2,
  healthOverlayMs: 1,
  name,
  queuedEventCount: 0,
  resultRowId,
  rows,
  rowsPerSecond: rows * 10,
  seedRows: parameters.seedRows,
  snapshotMs: rows === 0 ? 0 : 7,
  startTotalRows: parameters.seedRows,
  streamConvergenceMs: rows === 0 ? 0 : 100,
  totalRows: parameters.seedRows + rows,
});

const operationCaseFor = (sample: ReturnType<typeof sampleFor>) => ({
  maxCleanupMs: sample.cleanupMs,
  maxHealthOverlayMs: sample.healthOverlayMs,
  maxSnapshotMs: sample.snapshotMs,
  maxStreamConvergenceMs: sample.streamConvergenceMs,
  meanCleanupMs: sample.cleanupMs,
  meanHealthOverlayMs: sample.healthOverlayMs,
  meanRowsPerSecond: sample.rowsPerSecond,
  meanSnapshotMs: sample.snapshotMs,
  meanStreamConvergenceMs: sample.streamConvergenceMs,
  medianRowsPerSecond: sample.rowsPerSecond,
  mutationCount: sample.rows * sampleCount,
  name: sample.name,
  pooledRowsPerSecond: sample.rowsPerSecond,
  rowsPerSecondCoefficientOfVariation: 0,
  sampleCount,
  samples: Array.from({ length: sampleCount }, () => ({ ...sample })),
  seedMutationCount: sample.seedRows * sampleCount,
  startTotalRows: sample.startTotalRows,
  totalRows: sample.totalRows,
});

const sample = sampleFor("gRPC materialized stream batch", parameters.batchSize, "order-1099");
const operationCase = operationCaseFor(sample);
const samples = operationCase.samples;
const burstOperationCase = operationCaseFor(
  sampleFor("gRPC materialized burst", parameters.batchSize * 4, "order-1399"),
);
const healthOperationCase = operationCaseFor(
  sampleFor("gRPC materialized health overlay", 0, null),
);
const canonicalOperationCases = [operationCase, burstOperationCase, healthOperationCase];
const canonicalMutationCount = canonicalOperationCases.reduce(
  (total, currentCase) => total + currentCase.mutationCount,
  0,
);
const canonicalSeedMutationCount = canonicalOperationCases.reduce(
  (total, currentCase) => total + currentCase.seedMutationCount,
  0,
);

const thresholds = {
  operationMax: {
    maxAbsoluteDeltaMs: 50,
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
  runtimeOperationCases: [operationCase],
  seedMutationCount: 7000,
};

type OperationCase = ReturnType<typeof operationCaseFor>;

const comparisonState = (currentSamples: ReadonlyArray<ReturnType<typeof sampleFor>>) =>
  currentSamples.map((currentSample) => ({
    backpressureCount: currentSample.backpressureCount,
    cleanupLeakCount: currentSample.cleanupLeakCount,
    name: currentSample.name,
    queuedEventCount: currentSample.queuedEventCount,
    resultRowId: currentSample.resultRowId,
    rows: currentSample.rows,
    seedRows: currentSample.seedRows,
    startTotalRows: currentSample.startTotalRows,
    totalRows: currentSample.totalRows,
  }));

const validatePolicy = ({
  candidate = operationCase,
  mutationCount = canonicalMutationCount,
  operationCases,
  seedMutationCount = canonicalSeedMutationCount,
}: {
  readonly candidate?: OperationCase;
  readonly mutationCount?: number;
  readonly operationCases?: ReadonlyArray<OperationCase>;
  readonly seedMutationCount?: number;
} = {}) => {
  const decoded = decodeGrpcMaterializedOperationCases(
    operationCases ?? [candidate, burstOperationCase, healthOperationCase],
    operationCasesPath,
  );
  validateGrpcMaterializedOperationAccounting(
    decoded,
    parameters,
    mutationCount,
    seedMutationCount,
    operationCasesPath,
  );
};

describe("gRPC materialized benchmark artifact policy", () => {
  it("rejects contaminated sample evidence and inconsistent accounting", () => {
    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: samples.slice(0, 6),
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples must contain exactly 7 samples but contained 6.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            {
              ...sample,
              startTotalRows: 999,
            },
            ...samples.slice(1),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0].startTotalRows must equal seedRows 1000 but was 999.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            sample,
            {
              ...sample,
              resultRowId: "order-1098",
            },
            ...samples.slice(2),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples must preserve identical deterministic state across samples.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            {
              ...sample,
              cleanupLeakCount: 1,
            },
            ...samples.slice(1),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0] must record zero cleanup leaks, queued events, and backpressure events.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          mutationCount: 699,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].mutationCount must equal measured sample rows 700 but was 699.",
    );

    expect(() => validatePolicy({ seedMutationCount: 20_999 })).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases seedMutationCount total must equal task seedMutationCount 20999 but was 21000.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          medianRowsPerSecond: 999,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].medianRowsPerSecond must equal the value derived from samples (1000) but was 999.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          meanCleanupMs: 3,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].meanCleanupMs must be less than or equal to maxCleanupMs.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            {
              ...sample,
              name: "case b",
            },
            ...samples.slice(1),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0].name must equal operation case name gRPC materialized stream batch.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            {
              ...sample,
              totalRows: 1099,
            },
            ...samples.slice(1),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0].totalRows must equal startTotalRows plus rows (1100) but was 1099.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          samples: [
            {
              ...sample,
              rowsPerSecond: 0,
              streamConvergenceMs: 0,
            },
            ...samples.slice(1),
          ],
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0].streamConvergenceMs must be positive when rows are measured.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          startTotalRows: 999,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].startTotalRows must equal sample startTotalRows 1000 but was 999.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          totalRows: 1099,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].totalRows must equal sample totalRows 1100 but was 1099.",
    );

    expect(() =>
      validatePolicy({
        candidate: {
          ...operationCase,
          seedMutationCount: 6999,
        },
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].seedMutationCount must equal seeded sample rows 7000 but was 6999.",
    );

    expect(() => validatePolicy({ mutationCount: 3_499 })).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases mutationCount total must equal task mutationCount 3499 but was 3500.",
    );
  });

  it("owns the canonical parameter-derived materialized workloads", () => {
    expect(() => validatePolicy()).not.toThrow();

    const wrongRows = operationCaseFor(
      sampleFor("gRPC materialized stream batch", parameters.batchSize - 1, "order-1098"),
    );
    expect(() =>
      validatePolicy({
        operationCases: [wrongRows, burstOperationCase, healthOperationCase],
      }),
    ).toThrow(
      'Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0] workload state must equal {"rows":100,"seedRows":1000,"startTotalRows":1000,"totalRows":1100} but was {"rows":99,"seedRows":1000,"startTotalRows":1000,"totalRows":1099}.',
    );

    const wrongSeed = operationCaseFor({
      ...sample,
      seedRows: parameters.seedRows - 1,
      startTotalRows: parameters.seedRows - 1,
      totalRows: parameters.seedRows + parameters.batchSize - 1,
    });
    expect(() =>
      validatePolicy({
        operationCases: [wrongSeed, burstOperationCase, healthOperationCase],
      }),
    ).toThrow(
      'Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0] workload state must equal {"rows":100,"seedRows":1000,"startTotalRows":1000,"totalRows":1100} but was {"rows":100,"seedRows":999,"startTotalRows":999,"totalRows":1099}.',
    );

    const unexpected = operationCaseFor(sampleFor("gRPC materialized other", 0, null));
    expect(() =>
      validatePolicy({
        operationCases: [...canonicalOperationCases, unexpected],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[3].name must be one of: gRPC materialized stream batch, gRPC materialized burst, gRPC materialized health overlay.",
    );

    expect(() =>
      validatePolicy({
        operationCases: [operationCase, operationCase, burstOperationCase, healthOperationCase],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases contains duplicate gRPC materialized operation case: gRPC materialized stream batch.",
    );

    expect(() =>
      validatePolicy({
        operationCases: [operationCase, burstOperationCase],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases is missing gRPC materialized operation case: gRPC materialized health overlay.",
    );

    expect(() =>
      validateGrpcMaterializedOperationAccounting(
        [{ ...operationCase, mutationCount: operationCase.mutationCount - 1 }, burstOperationCase, healthOperationCase],
        parameters,
        canonicalMutationCount,
        canonicalSeedMutationCount,
        operationCasesPath,
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].mutationCount must equal workload mutations 700 but was 699.",
    );

    expect(() =>
      validateGrpcMaterializedOperationAccounting(
        [
          { ...operationCase, seedMutationCount: operationCase.seedMutationCount - 1 },
          burstOperationCase,
          healthOperationCase,
        ],
        parameters,
        canonicalMutationCount,
        canonicalSeedMutationCount,
        operationCasesPath,
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].seedMutationCount must equal workload seed mutations 7000 but was 6999.",
    );
  });

  it("rejects malformed values at each materialized artifact seam", () => {
    expect(() =>
      decodeGrpcMaterializedBenchmarkParameters(
        {
          batchSize: Number.NaN,
          seedRows: 1000,
        },
        "baseline.tasks[0].grpcParameters",
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].grpcParameters.batchSize must be a finite number.",
    );

    expect(() =>
      decodeGrpcMaterializedOperationCases(
        [
          {
            ...operationCase,
            name: "",
          },
        ],
        operationCasesPath,
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].name must be a non-empty string.",
    );

    expect(() =>
      decodeGrpcMaterializedBenchmarkParameters(null, "baseline.tasks[0].grpcParameters"),
    ).toThrow("Benchmark artifact field baseline.tasks[0].grpcParameters must be an object.");

    expect(() => decodeGrpcMaterializedOperationCases({}, operationCasesPath)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases must be an array.",
    );

    expect(() => decodeGrpcMaterializedOperationCases([], operationCasesPath)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases must be a non-empty array.",
    );

    expect(() =>
      decodeGrpcMaterializedBenchmarkParameters(
        {
          batchSize: 0,
          seedRows: 1000,
        },
        "baseline.tasks[0].grpcParameters",
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].grpcParameters.batchSize must be a positive integer.",
    );

    expect(() =>
      decodeGrpcMaterializedSeedMutationCount(-1, "baseline.tasks[0].seedMutationCount"),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].seedMutationCount must be a non-negative integer.",
    );

    expect(() =>
      decodeGrpcMaterializedOperationCases(
        [
          {
            ...operationCase,
            samples: [
              {
                ...sample,
                cleanupMs: -1,
              },
              ...samples.slice(1),
            ],
          },
        ],
        operationCasesPath,
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].samples[0].cleanupMs must be a non-negative finite number.",
    );
  });

  it("owns materialized comparison presence, identity, state, and latency policy", () => {
    expect(
      compareGrpcMaterializedBenchmarkTask(
        "task a",
        thresholds,
        {
          runtimeOperationCases: undefined,
          seedMutationCount: 0,
        },
        {
          runtimeOperationCases: undefined,
          seedMutationCount: 0,
        },
      ),
    ).toStrictEqual([]);

    expect(
      compareGrpcMaterializedBenchmarkTask(
        "task a",
        thresholds,
        {
          runtimeOperationCases: undefined,
          seedMutationCount: 0,
        },
        benchmarkTask,
      ),
    ).toStrictEqual([
      "task a: seedMutationCount changed from 0 to 7000.",
      "task a: runtimeOperationCases presence changed.",
    ]);

    expect(
      compareGrpcMaterializedBenchmarkTask(
        "task a",
        thresholds,
        benchmarkTask,
        {
          runtimeOperationCases: undefined,
          seedMutationCount: 7000,
        },
      ),
    ).toStrictEqual(["task a: runtimeOperationCases presence changed."]);

    const otherCase = {
      ...operationCase,
      name: "case b",
      samples: samples.map((currentSample) => ({
        ...currentSample,
        name: "case b",
      })),
    };
    expect(
      compareGrpcMaterializedBenchmarkTask("task a", thresholds, benchmarkTask, {
        runtimeOperationCases: [otherCase],
        seedMutationCount: 7000,
      }),
    ).toStrictEqual([
      "task a: unexpected runtime operation case case b.",
      "task a: missing runtime operation case gRPC materialized stream batch.",
    ]);

    const changedSamples = [
      {
        ...sample,
        resultRowId: "order-1098",
      },
      ...samples.slice(1),
    ];
    expect(
      compareGrpcMaterializedBenchmarkTask("task a", thresholds, benchmarkTask, {
        runtimeOperationCases: [
          {
            ...operationCase,
            meanCleanupMs: 1000,
            samples: changedSamples,
          },
        ],
        seedMutationCount: 7000,
      }),
    ).toStrictEqual([
      `task a: gRPC materialized stream batch runtime operation sample state changed from ${JSON.stringify(comparisonState(samples))} to ${JSON.stringify(comparisonState(changedSamples))}.`,
      "task a / gRPC materialized stream batch: meanCleanupMs regressed from 2.000ms to 1000.000ms; allowed <= 27.000ms.",
    ]);

    expect(() =>
      compareGrpcMaterializedBenchmarkTask(
        "task a",
        thresholds,
        {
          runtimeOperationCases: [operationCase, operationCase],
          seedMutationCount: 7000,
        },
        benchmarkTask,
      ),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[task a] contains duplicate runtime operation case: gRPC materialized stream batch.",
    );
  });
});
