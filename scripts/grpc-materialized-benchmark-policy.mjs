import {
  compareExact,
  compareExactJson,
  compareLatency,
  compareThroughput,
  exactObjectValue,
  mapByUniqueKey,
  nonEmptyArrayValue,
  nonNegativeFiniteNumber,
  nonNegativeInteger,
  positiveInteger,
  pushRegression,
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";

const grpcMaterializedBenchmarkCaseNames = [
  "gRPC materialized stream batch",
  "gRPC materialized burst",
  "gRPC materialized health overlay",
];

const nullableStringValue = (value, path) =>
  value === null ? null : stringValue(value, path);

const medianNumber = (values) => {
  const sorted = values.toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
};

const requireDerivedNumber = (actual, expected, path) => {
  const tolerance = Number.EPSILON * Math.max(1, Math.abs(actual), Math.abs(expected)) * 32;
  if (Math.abs(actual - expected) > tolerance) {
    throw new Error(
      `Benchmark artifact field ${path} must equal the value derived from samples (${expected}) but was ${actual}.`,
    );
  }
};

const decodeGrpcMaterializedSample = (value, path) => {
  const sample = exactObjectValue(value, path, [
    "backpressureCount",
    "cleanupLeakCount",
    "cleanupMs",
    "healthOverlayMs",
    "name",
    "queuedEventCount",
    "resultRowId",
    "rows",
    "rowsPerSecond",
    "seedRows",
    "snapshotMs",
    "startTotalRows",
    "streamConvergenceMs",
    "totalRows",
  ]);
  return {
    backpressureCount: nonNegativeInteger(
      sample.backpressureCount,
      `${path}.backpressureCount`,
    ),
    cleanupLeakCount: nonNegativeInteger(sample.cleanupLeakCount, `${path}.cleanupLeakCount`),
    cleanupMs: nonNegativeFiniteNumber(sample.cleanupMs, `${path}.cleanupMs`),
    healthOverlayMs: nonNegativeFiniteNumber(sample.healthOverlayMs, `${path}.healthOverlayMs`),
    name: stringValue(sample.name, `${path}.name`),
    queuedEventCount: nonNegativeInteger(sample.queuedEventCount, `${path}.queuedEventCount`),
    resultRowId: nullableStringValue(sample.resultRowId, `${path}.resultRowId`),
    rows: nonNegativeInteger(sample.rows, `${path}.rows`),
    rowsPerSecond: nonNegativeFiniteNumber(sample.rowsPerSecond, `${path}.rowsPerSecond`),
    seedRows: nonNegativeInteger(sample.seedRows, `${path}.seedRows`),
    snapshotMs: nonNegativeFiniteNumber(sample.snapshotMs, `${path}.snapshotMs`),
    startTotalRows: nonNegativeInteger(sample.startTotalRows, `${path}.startTotalRows`),
    streamConvergenceMs: nonNegativeFiniteNumber(
      sample.streamConvergenceMs,
      `${path}.streamConvergenceMs`,
    ),
    totalRows: nonNegativeInteger(sample.totalRows, `${path}.totalRows`),
  };
};

const decodeGrpcMaterializedOperationCase = (value, path) => {
  const operationCase = exactObjectValue(value, path, [
    "maxCleanupMs",
    "maxHealthOverlayMs",
    "maxSnapshotMs",
    "maxStreamConvergenceMs",
    "meanCleanupMs",
    "meanHealthOverlayMs",
    "meanRowsPerSecond",
    "meanSnapshotMs",
    "meanStreamConvergenceMs",
    "medianRowsPerSecond",
    "mutationCount",
    "name",
    "pooledRowsPerSecond",
    "rowsPerSecondCoefficientOfVariation",
    "sampleCount",
    "samples",
    "seedMutationCount",
    "startTotalRows",
    "totalRows",
  ]);
  const result = {
    maxCleanupMs: nonNegativeFiniteNumber(operationCase.maxCleanupMs, `${path}.maxCleanupMs`),
    maxHealthOverlayMs: nonNegativeFiniteNumber(
      operationCase.maxHealthOverlayMs,
      `${path}.maxHealthOverlayMs`,
    ),
    maxSnapshotMs: nonNegativeFiniteNumber(operationCase.maxSnapshotMs, `${path}.maxSnapshotMs`),
    maxStreamConvergenceMs: nonNegativeFiniteNumber(
      operationCase.maxStreamConvergenceMs,
      `${path}.maxStreamConvergenceMs`,
    ),
    meanCleanupMs: nonNegativeFiniteNumber(operationCase.meanCleanupMs, `${path}.meanCleanupMs`),
    meanHealthOverlayMs: nonNegativeFiniteNumber(
      operationCase.meanHealthOverlayMs,
      `${path}.meanHealthOverlayMs`,
    ),
    meanRowsPerSecond: nonNegativeFiniteNumber(
      operationCase.meanRowsPerSecond,
      `${path}.meanRowsPerSecond`,
    ),
    meanSnapshotMs: nonNegativeFiniteNumber(operationCase.meanSnapshotMs, `${path}.meanSnapshotMs`),
    meanStreamConvergenceMs: nonNegativeFiniteNumber(
      operationCase.meanStreamConvergenceMs,
      `${path}.meanStreamConvergenceMs`,
    ),
    medianRowsPerSecond: nonNegativeFiniteNumber(
      operationCase.medianRowsPerSecond,
      `${path}.medianRowsPerSecond`,
    ),
    mutationCount: nonNegativeInteger(operationCase.mutationCount, `${path}.mutationCount`),
    name: stringValue(operationCase.name, `${path}.name`),
    pooledRowsPerSecond: nonNegativeFiniteNumber(
      operationCase.pooledRowsPerSecond,
      `${path}.pooledRowsPerSecond`,
    ),
    rowsPerSecondCoefficientOfVariation: nonNegativeFiniteNumber(
      operationCase.rowsPerSecondCoefficientOfVariation,
      `${path}.rowsPerSecondCoefficientOfVariation`,
    ),
    sampleCount: positiveInteger(operationCase.sampleCount, `${path}.sampleCount`),
    samples: nonEmptyArrayValue(operationCase.samples, `${path}.samples`).map((sample, index) =>
      decodeGrpcMaterializedSample(sample, `${path}.samples[${index}]`),
    ),
    seedMutationCount: nonNegativeInteger(
      operationCase.seedMutationCount,
      `${path}.seedMutationCount`,
    ),
    startTotalRows: nonNegativeInteger(operationCase.startTotalRows, `${path}.startTotalRows`),
    totalRows: nonNegativeInteger(operationCase.totalRows, `${path}.totalRows`),
  };
  if (result.meanCleanupMs > result.maxCleanupMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanCleanupMs must be less than or equal to maxCleanupMs.`,
    );
  }
  if (result.meanHealthOverlayMs > result.maxHealthOverlayMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanHealthOverlayMs must be less than or equal to maxHealthOverlayMs.`,
    );
  }
  if (result.meanSnapshotMs > result.maxSnapshotMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanSnapshotMs must be less than or equal to maxSnapshotMs.`,
    );
  }
  if (result.meanStreamConvergenceMs > result.maxStreamConvergenceMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanStreamConvergenceMs must be less than or equal to maxStreamConvergenceMs.`,
    );
  }
  if (result.samples.length !== result.sampleCount) {
    throw new Error(
      `Benchmark artifact field ${path}.samples must contain exactly ${result.sampleCount} samples but contained ${result.samples.length}.`,
    );
  }

  const firstSample = result.samples[0];
  const totals = {
    cleanupMs: 0,
    healthOverlayMs: 0,
    rows: 0,
    rowsPerSecond: 0,
    seedRows: 0,
    snapshotMs: 0,
    streamConvergenceMs: 0,
  };
  for (const [index, sample] of result.samples.entries()) {
    const samplePath = `${path}.samples[${index}]`;
    if (sample.name !== result.name) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.name must equal operation case name ${result.name}.`,
      );
    }
    if (sample.seedRows !== sample.startTotalRows) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.startTotalRows must equal seedRows ${sample.seedRows} but was ${sample.startTotalRows}.`,
      );
    }
    if (sample.totalRows !== sample.startTotalRows + sample.rows) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.totalRows must equal startTotalRows plus rows (${sample.startTotalRows + sample.rows}) but was ${sample.totalRows}.`,
      );
    }
    if (
      sample.seedRows !== firstSample.seedRows ||
      sample.startTotalRows !== firstSample.startTotalRows ||
      sample.rows !== firstSample.rows ||
      sample.totalRows !== firstSample.totalRows ||
      sample.resultRowId !== firstSample.resultRowId
    ) {
      throw new Error(
        `Benchmark artifact field ${path}.samples must preserve identical deterministic state across samples.`,
      );
    }
    if (
      sample.cleanupLeakCount !== 0 ||
      sample.queuedEventCount !== 0 ||
      sample.backpressureCount !== 0
    ) {
      throw new Error(
        `Benchmark artifact field ${samplePath} must record zero cleanup leaks, queued events, and backpressure events.`,
      );
    }
    const expectedRowsPerSecond =
      sample.streamConvergenceMs === 0
        ? 0
        : (sample.rows / sample.streamConvergenceMs) * 1_000;
    if (sample.rows > 0 && sample.streamConvergenceMs === 0) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.streamConvergenceMs must be positive when rows are measured.`,
      );
    }
    requireDerivedNumber(sample.rowsPerSecond, expectedRowsPerSecond, `${samplePath}.rowsPerSecond`);
    totals.cleanupMs += sample.cleanupMs;
    totals.healthOverlayMs += sample.healthOverlayMs;
    totals.rows += sample.rows;
    totals.rowsPerSecond += sample.rowsPerSecond;
    totals.seedRows += sample.seedRows;
    totals.snapshotMs += sample.snapshotMs;
    totals.streamConvergenceMs += sample.streamConvergenceMs;
  }
  if (result.startTotalRows !== firstSample.startTotalRows) {
    throw new Error(
      `Benchmark artifact field ${path}.startTotalRows must equal sample startTotalRows ${firstSample.startTotalRows} but was ${result.startTotalRows}.`,
    );
  }
  if (result.totalRows !== firstSample.totalRows) {
    throw new Error(
      `Benchmark artifact field ${path}.totalRows must equal sample totalRows ${firstSample.totalRows} but was ${result.totalRows}.`,
    );
  }
  if (result.mutationCount !== totals.rows) {
    throw new Error(
      `Benchmark artifact field ${path}.mutationCount must equal measured sample rows ${totals.rows} but was ${result.mutationCount}.`,
    );
  }
  if (result.seedMutationCount !== totals.seedRows) {
    throw new Error(
      `Benchmark artifact field ${path}.seedMutationCount must equal seeded sample rows ${totals.seedRows} but was ${result.seedMutationCount}.`,
    );
  }

  const meanRowsPerSecond = totals.rowsPerSecond / result.sampleCount;
  const rowsPerSecondVariance =
    result.samples.reduce(
      (total, sample) => total + (sample.rowsPerSecond - meanRowsPerSecond) ** 2,
      0,
    ) / result.sampleCount;
  const derivedValues = {
    maxCleanupMs: Math.max(...result.samples.map((sample) => sample.cleanupMs)),
    maxHealthOverlayMs: Math.max(...result.samples.map((sample) => sample.healthOverlayMs)),
    maxSnapshotMs: Math.max(...result.samples.map((sample) => sample.snapshotMs)),
    maxStreamConvergenceMs: Math.max(
      ...result.samples.map((sample) => sample.streamConvergenceMs),
    ),
    meanCleanupMs: totals.cleanupMs / result.sampleCount,
    meanHealthOverlayMs: totals.healthOverlayMs / result.sampleCount,
    meanRowsPerSecond,
    meanSnapshotMs: totals.snapshotMs / result.sampleCount,
    meanStreamConvergenceMs: totals.streamConvergenceMs / result.sampleCount,
    medianRowsPerSecond: medianNumber(result.samples.map((sample) => sample.rowsPerSecond)),
    pooledRowsPerSecond:
      totals.streamConvergenceMs === 0
        ? 0
        : (totals.rows / totals.streamConvergenceMs) * 1_000,
    rowsPerSecondCoefficientOfVariation:
      meanRowsPerSecond === 0 ? 0 : Math.sqrt(rowsPerSecondVariance) / meanRowsPerSecond,
  };
  for (const [field, expected] of Object.entries(derivedValues)) {
    requireDerivedNumber(result[field], expected, `${path}.${field}`);
  }
  return result;
};

export const decodeGrpcMaterializedBenchmarkParameters = (value, path) => {
  const parameters = exactObjectValue(value, path, ["batchSize", "seedRows"]);
  return {
    batchSize: positiveInteger(parameters.batchSize, `${path}.batchSize`),
    seedRows: positiveInteger(parameters.seedRows, `${path}.seedRows`),
  };
};

export const decodeGrpcMaterializedSeedMutationCount = (value, path) =>
  nonNegativeInteger(value, path);

export const decodeGrpcMaterializedOperationCases = (value, path) =>
  nonEmptyArrayValue(value, path).map((operationCase, index) =>
    decodeGrpcMaterializedOperationCase(operationCase, `${path}[${index}]`),
  );

const workloadAccounting = (parameters) =>
  new Map([
    [
      "gRPC materialized stream batch",
      {
        rows: parameters.batchSize,
        seedRows: parameters.seedRows,
        startTotalRows: parameters.seedRows,
        totalRows: parameters.seedRows + parameters.batchSize,
      },
    ],
    [
      "gRPC materialized burst",
      {
        rows: parameters.batchSize * 4,
        seedRows: parameters.seedRows,
        startTotalRows: parameters.seedRows,
        totalRows: parameters.seedRows + parameters.batchSize * 4,
      },
    ],
    [
      "gRPC materialized health overlay",
      {
        rows: 0,
        seedRows: parameters.seedRows,
        startTotalRows: parameters.seedRows,
        totalRows: parameters.seedRows,
      },
    ],
  ]);

const validateSampleWorkloadAccounting = (sample, expected, path) => {
  const actualState = {
    rows: sample.rows,
    seedRows: sample.seedRows,
    startTotalRows: sample.startTotalRows,
    totalRows: sample.totalRows,
  };
  if (JSON.stringify(actualState) !== JSON.stringify(expected)) {
    throw new Error(
      `Benchmark artifact field ${path} workload state must equal ${JSON.stringify(expected)} but was ${JSON.stringify(actualState)}.`,
    );
  }
};

export const validateGrpcMaterializedOperationAccounting = (
  operationCases,
  parameters,
  mutationCount,
  seedMutationCount,
  path,
) => {
  const expectedByName = workloadAccounting(parameters);
  const seen = new Set();
  for (const [caseIndex, operationCase] of operationCases.entries()) {
    const expected = expectedByName.get(operationCase.name);
    if (expected === undefined) {
      throw new Error(
        `Benchmark artifact field ${path}[${caseIndex}].name must be one of: ${grpcMaterializedBenchmarkCaseNames.join(", ")}.`,
      );
    }
    if (seen.has(operationCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains duplicate gRPC materialized operation case: ${operationCase.name}.`,
      );
    }
    seen.add(operationCase.name);
    for (const [sampleIndex, sample] of operationCase.samples.entries()) {
      validateSampleWorkloadAccounting(
        sample,
        expected,
        `${path}[${caseIndex}].samples[${sampleIndex}]`,
      );
    }
    const expectedMutationCount = expected.rows * operationCase.sampleCount;
    if (operationCase.mutationCount !== expectedMutationCount) {
      throw new Error(
        `Benchmark artifact field ${path}[${caseIndex}].mutationCount must equal workload mutations ${expectedMutationCount} but was ${operationCase.mutationCount}.`,
      );
    }
    const expectedSeedMutationCount = parameters.seedRows * operationCase.sampleCount;
    if (operationCase.seedMutationCount !== expectedSeedMutationCount) {
      throw new Error(
        `Benchmark artifact field ${path}[${caseIndex}].seedMutationCount must equal workload seed mutations ${expectedSeedMutationCount} but was ${operationCase.seedMutationCount}.`,
      );
    }
  }
  for (const expectedName of expectedByName.keys()) {
    if (!seen.has(expectedName)) {
      throw new Error(
        `Benchmark artifact field ${path} is missing gRPC materialized operation case: ${expectedName}.`,
      );
    }
  }
  const measuredMutations = operationCases.reduce(
    (total, operationCase) => total + operationCase.mutationCount,
    0,
  );
  if (measuredMutations !== mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path} mutationCount total must equal task mutationCount ${mutationCount} but was ${measuredMutations}.`,
    );
  }
  const seedMutations = operationCases.reduce(
    (total, operationCase) => total + operationCase.seedMutationCount,
    0,
  );
  if (seedMutations !== seedMutationCount) {
    throw new Error(
      `Benchmark artifact field ${path} seedMutationCount total must equal task seedMutationCount ${seedMutationCount} but was ${seedMutations}.`,
    );
  }
};

const sampleState = (samples) =>
  samples.map((sample) => ({
    backpressureCount: sample.backpressureCount,
    cleanupLeakCount: sample.cleanupLeakCount,
    name: sample.name,
    queuedEventCount: sample.queuedEventCount,
    resultRowId: sample.resultRowId,
    rows: sample.rows,
    seedRows: sample.seedRows,
    startTotalRows: sample.startTotalRows,
    totalRows: sample.totalRows,
  }));

const compareLatencyField = (
  regressions,
  taskLabel,
  thresholds,
  baselineCase,
  actualCase,
  field,
  thresholdName,
) => {
  if (baselineCase[field] === 0) {
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} runtime operation ${field}`,
      baselineCase[field],
      actualCase[field],
    );
    return;
  }
  compareLatency(
    regressions,
    taskLabel,
    baselineCase.name,
    field,
    thresholds[thresholdName],
    baselineCase[field],
    actualCase[field],
  );
};

const compareOperationCase = (regressions, taskLabel, thresholds, baselineCase, actualCase) => {
  for (const field of [
    "sampleCount",
    "startTotalRows",
    "totalRows",
    "mutationCount",
    "seedMutationCount",
  ]) {
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} runtime operation ${field}`,
      baselineCase[field],
      actualCase[field],
    );
  }
  compareExactJson(
    regressions,
    taskLabel,
    `${baselineCase.name} runtime operation sample state`,
    sampleState(baselineCase.samples),
    sampleState(actualCase.samples),
  );
  for (const field of [
    "meanCleanupMs",
    "meanHealthOverlayMs",
    "meanSnapshotMs",
    "meanStreamConvergenceMs",
  ]) {
    compareLatencyField(
      regressions,
      taskLabel,
      thresholds,
      baselineCase,
      actualCase,
      field,
      "operationMean",
    );
  }
  for (const field of [
    "maxCleanupMs",
    "maxHealthOverlayMs",
    "maxSnapshotMs",
    "maxStreamConvergenceMs",
  ]) {
    compareLatencyField(
      regressions,
      taskLabel,
      thresholds,
      baselineCase,
      actualCase,
      field,
      "operationMax",
    );
  }
  compareThroughput(
    regressions,
    taskLabel,
    baselineCase.name,
    "pooledRowsPerSecond",
    thresholds.throughputAggregateRowsPerSecond,
    baselineCase.pooledRowsPerSecond,
    actualCase.pooledRowsPerSecond,
  );
};

export const compareGrpcMaterializedBenchmarkTask = (
  taskLabel,
  thresholds,
  baselineTask,
  actualTask,
) => {
  const regressions = [];
  compareExact(
    regressions,
    taskLabel,
    "seedMutationCount",
    baselineTask.seedMutationCount,
    actualTask.seedMutationCount,
  );
  const baselineCases = baselineTask.runtimeOperationCases;
  const actualCases = actualTask.runtimeOperationCases;
  if (baselineCases === undefined && actualCases === undefined) {
    return regressions;
  }
  if (baselineCases === undefined || actualCases === undefined) {
    pushRegression(regressions, `${taskLabel}: runtimeOperationCases presence changed.`);
    return regressions;
  }
  const baselineByName = mapByUniqueKey(
    baselineCases,
    (operationCase) => operationCase.name,
    `baseline.tasks[${taskLabel}]`,
    "runtime operation case",
  );
  const actualByName = mapByUniqueKey(
    actualCases,
    (operationCase) => operationCase.name,
    `actual.tasks[${taskLabel}]`,
    "runtime operation case",
  );
  for (const caseName of actualByName.keys()) {
    if (!baselineByName.has(caseName)) {
      pushRegression(regressions, `${taskLabel}: unexpected runtime operation case ${caseName}.`);
    }
  }
  for (const baselineCase of baselineCases) {
    const actualCase = actualByName.get(baselineCase.name);
    if (actualCase === undefined) {
      pushRegression(
        regressions,
        `${taskLabel}: missing runtime operation case ${baselineCase.name}.`,
      );
      continue;
    }
    compareOperationCase(regressions, taskLabel, thresholds, baselineCase, actualCase);
  }
  return regressions;
};
