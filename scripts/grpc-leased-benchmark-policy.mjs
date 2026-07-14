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
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";

const grpcLeasedBenchmarkCaseNames = [
  "gRPC leased first subscriber",
  "gRPC leased same-route reuse",
  "gRPC leased one route many subscribers",
  "gRPC leased local-filter live snapshot",
  "gRPC leased retained local-filter snapshot",
  "gRPC leased delta fanout",
  "gRPC leased partitioned write convergence",
  "gRPC leased health refresh overhead",
  "gRPC leased last-subscriber cleanup",
  "gRPC leased many routes",
];

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

const decodeGrpcLeasedMeasuredCleanup = (value, path) => {
  const cleanup = exactObjectValue(value, path, [
    "activeLeasedFeeds",
    "activeSubscriptions",
    "activeViews",
    "clientActiveFeeds",
    "leakCount",
    "queuedEvents",
    "rowCount",
  ]);
  return {
    activeLeasedFeeds: nonNegativeInteger(
      cleanup.activeLeasedFeeds,
      `${path}.activeLeasedFeeds`,
    ),
    activeSubscriptions: nonNegativeInteger(
      cleanup.activeSubscriptions,
      `${path}.activeSubscriptions`,
    ),
    activeViews: nonNegativeInteger(cleanup.activeViews, `${path}.activeViews`),
    clientActiveFeeds: nonNegativeInteger(
      cleanup.clientActiveFeeds,
      `${path}.clientActiveFeeds`,
    ),
    leakCount: nonNegativeInteger(cleanup.leakCount, `${path}.leakCount`),
    queuedEvents: nonNegativeInteger(cleanup.queuedEvents, `${path}.queuedEvents`),
    rowCount: nonNegativeInteger(cleanup.rowCount, `${path}.rowCount`),
  };
};

const decodeGrpcLeasedSample = (value, path) => {
  const sample = exactObjectValue(value, path, [
    "acquiredFeedCount",
    "activeLeasedFeeds",
    "backpressureCount",
    "cleanupActiveLeasedFeeds",
    "cleanupClientActiveFeeds",
    "cleanupLeakCount",
    "cleanupMs",
    "cleanupRowCount",
    "deltaFanoutMs",
    "healthOverlayMs",
    "measurementRowCount",
    "measuredCleanup",
    "mutationCount",
    "name",
    "queuedEventCount",
    "releasedFeedCount",
    "rows",
    "rowsPerSecond",
    "seedMutationCount",
    "snapshotMs",
    "subscriberCount",
    "subscriptionMs",
  ]);
  return {
    acquiredFeedCount: nonNegativeInteger(
      sample.acquiredFeedCount,
      `${path}.acquiredFeedCount`,
    ),
    activeLeasedFeeds: nonNegativeInteger(
      sample.activeLeasedFeeds,
      `${path}.activeLeasedFeeds`,
    ),
    backpressureCount: nonNegativeInteger(
      sample.backpressureCount,
      `${path}.backpressureCount`,
    ),
    cleanupActiveLeasedFeeds: nonNegativeInteger(
      sample.cleanupActiveLeasedFeeds,
      `${path}.cleanupActiveLeasedFeeds`,
    ),
    cleanupClientActiveFeeds: nonNegativeInteger(
      sample.cleanupClientActiveFeeds,
      `${path}.cleanupClientActiveFeeds`,
    ),
    cleanupLeakCount: nonNegativeInteger(sample.cleanupLeakCount, `${path}.cleanupLeakCount`),
    cleanupMs: nonNegativeFiniteNumber(sample.cleanupMs, `${path}.cleanupMs`),
    cleanupRowCount: nonNegativeInteger(sample.cleanupRowCount, `${path}.cleanupRowCount`),
    deltaFanoutMs: nonNegativeFiniteNumber(sample.deltaFanoutMs, `${path}.deltaFanoutMs`),
    healthOverlayMs: nonNegativeFiniteNumber(sample.healthOverlayMs, `${path}.healthOverlayMs`),
    measurementRowCount: nonNegativeInteger(
      sample.measurementRowCount,
      `${path}.measurementRowCount`,
    ),
    measuredCleanup: decodeGrpcLeasedMeasuredCleanup(
      sample.measuredCleanup,
      `${path}.measuredCleanup`,
    ),
    mutationCount: nonNegativeInteger(sample.mutationCount, `${path}.mutationCount`),
    name: stringValue(sample.name, `${path}.name`),
    queuedEventCount: nonNegativeInteger(sample.queuedEventCount, `${path}.queuedEventCount`),
    releasedFeedCount: nonNegativeInteger(
      sample.releasedFeedCount,
      `${path}.releasedFeedCount`,
    ),
    rows: nonNegativeInteger(sample.rows, `${path}.rows`),
    rowsPerSecond: nonNegativeFiniteNumber(sample.rowsPerSecond, `${path}.rowsPerSecond`),
    seedMutationCount: nonNegativeInteger(
      sample.seedMutationCount,
      `${path}.seedMutationCount`,
    ),
    snapshotMs: nonNegativeFiniteNumber(sample.snapshotMs, `${path}.snapshotMs`),
    subscriberCount: nonNegativeInteger(sample.subscriberCount, `${path}.subscriberCount`),
    subscriptionMs: nonNegativeFiniteNumber(sample.subscriptionMs, `${path}.subscriptionMs`),
  };
};

const deterministicSampleState = (sample) => ({
  acquiredFeedCount: sample.acquiredFeedCount,
  activeLeasedFeeds: sample.activeLeasedFeeds,
  backpressureCount: sample.backpressureCount,
  cleanupActiveLeasedFeeds: sample.cleanupActiveLeasedFeeds,
  cleanupLeakCount: sample.cleanupLeakCount,
  cleanupRowCount: sample.cleanupRowCount,
  measurementRowCount: sample.measurementRowCount,
  mutationCount: sample.mutationCount,
  name: sample.name,
  queuedEventCount: sample.queuedEventCount,
  releasedFeedCount: sample.releasedFeedCount,
  rows: sample.rows,
  seedMutationCount: sample.seedMutationCount,
  subscriberCount: sample.subscriberCount,
});

const decodeGrpcLeasedOperationCase = (value, path) => {
  const operationCase = exactObjectValue(value, path, [
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
    "mutationCount",
    "name",
    "pooledRowsPerSecond",
    "rowsPerSecondCoefficientOfVariation",
    "sampleCount",
    "samples",
    "seedMutationCount",
  ]);
  const result = {
    maxActiveLeasedFeeds: nonNegativeInteger(
      operationCase.maxActiveLeasedFeeds,
      `${path}.maxActiveLeasedFeeds`,
    ),
    maxCleanupActiveLeasedFeeds: nonNegativeInteger(
      operationCase.maxCleanupActiveLeasedFeeds,
      `${path}.maxCleanupActiveLeasedFeeds`,
    ),
    maxCleanupClientActiveFeeds: nonNegativeInteger(
      operationCase.maxCleanupClientActiveFeeds,
      `${path}.maxCleanupClientActiveFeeds`,
    ),
    maxCleanupMs: nonNegativeFiniteNumber(operationCase.maxCleanupMs, `${path}.maxCleanupMs`),
    maxDeltaFanoutMs: nonNegativeFiniteNumber(
      operationCase.maxDeltaFanoutMs,
      `${path}.maxDeltaFanoutMs`,
    ),
    maxHealthOverlayMs: nonNegativeFiniteNumber(
      operationCase.maxHealthOverlayMs,
      `${path}.maxHealthOverlayMs`,
    ),
    maxMeasuredCleanupActiveLeasedFeeds: nonNegativeInteger(
      operationCase.maxMeasuredCleanupActiveLeasedFeeds,
      `${path}.maxMeasuredCleanupActiveLeasedFeeds`,
    ),
    maxMeasuredCleanupActiveSubscriptions: nonNegativeInteger(
      operationCase.maxMeasuredCleanupActiveSubscriptions,
      `${path}.maxMeasuredCleanupActiveSubscriptions`,
    ),
    maxMeasuredCleanupActiveViews: nonNegativeInteger(
      operationCase.maxMeasuredCleanupActiveViews,
      `${path}.maxMeasuredCleanupActiveViews`,
    ),
    maxMeasuredCleanupClientActiveFeeds: nonNegativeInteger(
      operationCase.maxMeasuredCleanupClientActiveFeeds,
      `${path}.maxMeasuredCleanupClientActiveFeeds`,
    ),
    maxMeasuredCleanupLeakCount: nonNegativeInteger(
      operationCase.maxMeasuredCleanupLeakCount,
      `${path}.maxMeasuredCleanupLeakCount`,
    ),
    maxMeasuredCleanupQueuedEvents: nonNegativeInteger(
      operationCase.maxMeasuredCleanupQueuedEvents,
      `${path}.maxMeasuredCleanupQueuedEvents`,
    ),
    maxMeasuredCleanupRowCount: nonNegativeInteger(
      operationCase.maxMeasuredCleanupRowCount,
      `${path}.maxMeasuredCleanupRowCount`,
    ),
    maxSnapshotMs: nonNegativeFiniteNumber(operationCase.maxSnapshotMs, `${path}.maxSnapshotMs`),
    maxSubscriptionMs: nonNegativeFiniteNumber(
      operationCase.maxSubscriptionMs,
      `${path}.maxSubscriptionMs`,
    ),
    meanCleanupMs: nonNegativeFiniteNumber(operationCase.meanCleanupMs, `${path}.meanCleanupMs`),
    meanDeltaFanoutMs: nonNegativeFiniteNumber(
      operationCase.meanDeltaFanoutMs,
      `${path}.meanDeltaFanoutMs`,
    ),
    meanHealthOverlayMs: nonNegativeFiniteNumber(
      operationCase.meanHealthOverlayMs,
      `${path}.meanHealthOverlayMs`,
    ),
    meanRowsPerSecond: nonNegativeFiniteNumber(
      operationCase.meanRowsPerSecond,
      `${path}.meanRowsPerSecond`,
    ),
    meanSnapshotMs: nonNegativeFiniteNumber(
      operationCase.meanSnapshotMs,
      `${path}.meanSnapshotMs`,
    ),
    meanSubscriptionMs: nonNegativeFiniteNumber(
      operationCase.meanSubscriptionMs,
      `${path}.meanSubscriptionMs`,
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
      decodeGrpcLeasedSample(sample, `${path}.samples[${index}]`),
    ),
    seedMutationCount: nonNegativeInteger(
      operationCase.seedMutationCount,
      `${path}.seedMutationCount`,
    ),
  };
  if (result.samples.length !== result.sampleCount) {
    throw new Error(
      `Benchmark artifact field ${path}.samples must contain exactly ${result.sampleCount} samples but contained ${result.samples.length}.`,
    );
  }
  const firstSample = result.samples[0];
  const firstState = JSON.stringify(deterministicSampleState(firstSample));
  const totals = {
    cleanupMs: 0,
    deltaFanoutMs: 0,
    healthOverlayMs: 0,
    mutationCount: 0,
    rows: 0,
    rowsPerSecond: 0,
    seedMutationCount: 0,
    snapshotMs: 0,
    subscriptionMs: 0,
  };
  for (const [index, sample] of result.samples.entries()) {
    const samplePath = `${path}.samples[${index}]`;
    if (sample.name !== result.name) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.name must equal operation case name ${result.name}.`,
      );
    }
    if (JSON.stringify(deterministicSampleState(sample)) !== firstState) {
      throw new Error(
        `Benchmark artifact field ${path}.samples must preserve identical deterministic state across samples.`,
      );
    }
    if (sample.acquiredFeedCount !== sample.releasedFeedCount) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.releasedFeedCount must equal acquiredFeedCount ${sample.acquiredFeedCount} but was ${sample.releasedFeedCount}.`,
      );
    }
    if (
      sample.cleanupActiveLeasedFeeds !== 0 ||
      sample.cleanupClientActiveFeeds !== 0 ||
      sample.cleanupLeakCount !== 0 ||
      sample.cleanupRowCount !== 0 ||
      sample.measuredCleanup.activeLeasedFeeds !== 0 ||
      sample.measuredCleanup.activeSubscriptions !== 0 ||
      sample.measuredCleanup.activeViews !== 0 ||
      sample.measuredCleanup.clientActiveFeeds !== 0 ||
      sample.measuredCleanup.leakCount !== 0 ||
      sample.measuredCleanup.queuedEvents !== 0 ||
      sample.measuredCleanup.rowCount !== 0 ||
      sample.queuedEventCount !== 0 ||
      sample.backpressureCount !== 0
    ) {
      throw new Error(
        `Benchmark artifact field ${samplePath} must record zero measured cleanup state, emergency cleanup state, queued events, and backpressure events.`,
      );
    }
    if (sample.rows > 0 && sample.snapshotMs === 0) {
      throw new Error(
        `Benchmark artifact field ${samplePath}.snapshotMs must be positive when rows are measured.`,
      );
    }
    const expectedRowsPerSecond =
      sample.snapshotMs === 0 ? 0 : (sample.rows / sample.snapshotMs) * 1_000;
    requireDerivedNumber(sample.rowsPerSecond, expectedRowsPerSecond, `${samplePath}.rowsPerSecond`);
    totals.cleanupMs += sample.cleanupMs;
    totals.deltaFanoutMs += sample.deltaFanoutMs;
    totals.healthOverlayMs += sample.healthOverlayMs;
    totals.mutationCount += sample.mutationCount;
    totals.rows += sample.rows;
    totals.rowsPerSecond += sample.rowsPerSecond;
    totals.seedMutationCount += sample.seedMutationCount;
    totals.snapshotMs += sample.snapshotMs;
    totals.subscriptionMs += sample.subscriptionMs;
  }
  if (result.mutationCount !== totals.mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path}.mutationCount must equal measured sample mutations ${totals.mutationCount} but was ${result.mutationCount}.`,
    );
  }
  if (result.seedMutationCount !== totals.seedMutationCount) {
    throw new Error(
      `Benchmark artifact field ${path}.seedMutationCount must equal seeded sample mutations ${totals.seedMutationCount} but was ${result.seedMutationCount}.`,
    );
  }
  const meanRowsPerSecond = totals.rowsPerSecond / result.sampleCount;
  const rowsPerSecondVariance =
    result.samples.reduce(
      (total, sample) => total + (sample.rowsPerSecond - meanRowsPerSecond) ** 2,
      0,
    ) / result.sampleCount;
  const derivedValues = {
    maxActiveLeasedFeeds: Math.max(
      ...result.samples.map((sample) => sample.activeLeasedFeeds),
    ),
    maxCleanupActiveLeasedFeeds: Math.max(
      ...result.samples.map((sample) => sample.cleanupActiveLeasedFeeds),
    ),
    maxCleanupClientActiveFeeds: Math.max(
      ...result.samples.map((sample) => sample.cleanupClientActiveFeeds),
    ),
    maxCleanupMs: Math.max(...result.samples.map((sample) => sample.cleanupMs)),
    maxDeltaFanoutMs: Math.max(...result.samples.map((sample) => sample.deltaFanoutMs)),
    maxHealthOverlayMs: Math.max(...result.samples.map((sample) => sample.healthOverlayMs)),
    maxMeasuredCleanupActiveLeasedFeeds: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.activeLeasedFeeds),
    ),
    maxMeasuredCleanupActiveSubscriptions: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.activeSubscriptions),
    ),
    maxMeasuredCleanupActiveViews: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.activeViews),
    ),
    maxMeasuredCleanupClientActiveFeeds: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.clientActiveFeeds),
    ),
    maxMeasuredCleanupLeakCount: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.leakCount),
    ),
    maxMeasuredCleanupQueuedEvents: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.queuedEvents),
    ),
    maxMeasuredCleanupRowCount: Math.max(
      ...result.samples.map((sample) => sample.measuredCleanup.rowCount),
    ),
    maxSnapshotMs: Math.max(...result.samples.map((sample) => sample.snapshotMs)),
    maxSubscriptionMs: Math.max(...result.samples.map((sample) => sample.subscriptionMs)),
    meanCleanupMs: totals.cleanupMs / result.sampleCount,
    meanDeltaFanoutMs: totals.deltaFanoutMs / result.sampleCount,
    meanHealthOverlayMs: totals.healthOverlayMs / result.sampleCount,
    meanRowsPerSecond,
    meanSnapshotMs: totals.snapshotMs / result.sampleCount,
    meanSubscriptionMs: totals.subscriptionMs / result.sampleCount,
    medianRowsPerSecond: medianNumber(result.samples.map((sample) => sample.rowsPerSecond)),
    pooledRowsPerSecond:
      totals.snapshotMs === 0 ? 0 : (totals.rows / totals.snapshotMs) * 1_000,
    rowsPerSecondCoefficientOfVariation:
      meanRowsPerSecond === 0 ? 0 : Math.sqrt(rowsPerSecondVariance) / meanRowsPerSecond,
  };
  for (const [field, expected] of Object.entries(derivedValues)) {
    requireDerivedNumber(result[field], expected, `${path}.${field}`);
  }
  return result;
};

export const decodeGrpcLeasedBenchmarkParameters = (value, path) => {
  const parameters = exactObjectValue(value, path, [
    "retainedRows",
    "routeCount",
    "rowsPerFeed",
  ]);
  return {
    retainedRows: positiveInteger(parameters.retainedRows, `${path}.retainedRows`),
    routeCount: positiveInteger(parameters.routeCount, `${path}.routeCount`),
    rowsPerFeed: positiveInteger(parameters.rowsPerFeed, `${path}.rowsPerFeed`),
  };
};

export const decodeGrpcLeasedSeedMutationCount = (value, path) =>
  nonNegativeInteger(value, path);

export const decodeGrpcLeasedOperationCases = (value, path) =>
  nonEmptyArrayValue(value, path).map((operationCase, index) =>
    decodeGrpcLeasedOperationCase(operationCase, `${path}[${index}]`),
  );

const workloadAccounting = (parameters) => new Map([
  [
    "gRPC leased first subscriber",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 0,
      subscriberCount: 1,
    },
  ],
  [
    "gRPC leased same-route reuse",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 0,
      subscriberCount: 10,
    },
  ],
  [
    "gRPC leased one route many subscribers",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 0,
      subscriberCount: 50,
    },
  ],
  [
    "gRPC leased local-filter live snapshot",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 0,
      subscriberCount: 1,
    },
  ],
  [
    "gRPC leased retained local-filter snapshot",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.retainedRows,
      mutationCount: 0,
      rows: parameters.retainedRows,
      seedMutationCount: parameters.retainedRows,
      subscriberCount: 2,
    },
  ],
  [
    "gRPC leased delta fanout",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed + 1,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 1,
      subscriberCount: 25,
    },
  ],
  [
    "gRPC leased partitioned write convergence",
    {
      activeLeasedFeeds: parameters.routeCount,
      feedCount: parameters.routeCount,
      measurementRowCount: parameters.rowsPerFeed * parameters.routeCount,
      mutationCount: parameters.rowsPerFeed * parameters.routeCount,
      rows: parameters.rowsPerFeed * parameters.routeCount,
      seedMutationCount: 0,
      subscriberCount: parameters.routeCount,
    },
  ],
  [
    "gRPC leased health refresh overhead",
    {
      activeLeasedFeeds: parameters.routeCount,
      feedCount: parameters.routeCount,
      measurementRowCount: parameters.routeCount,
      mutationCount: 0,
      rows: 0,
      seedMutationCount: parameters.routeCount,
      subscriberCount: parameters.routeCount,
    },
  ],
  [
    "gRPC leased last-subscriber cleanup",
    {
      activeLeasedFeeds: 1,
      feedCount: 1,
      measurementRowCount: parameters.rowsPerFeed,
      mutationCount: parameters.rowsPerFeed,
      rows: parameters.rowsPerFeed,
      seedMutationCount: 0,
      subscriberCount: 1,
    },
  ],
  [
    "gRPC leased many routes",
    {
      activeLeasedFeeds: parameters.routeCount,
      feedCount: parameters.routeCount,
      measurementRowCount: parameters.routeCount,
      mutationCount: parameters.routeCount,
      rows: parameters.routeCount,
      seedMutationCount: 0,
      subscriberCount: parameters.routeCount,
    },
  ],
]);

const validateSampleWorkloadAccounting = (sample, expected, path) => {
  const actualState = {
    acquiredFeedCount: sample.acquiredFeedCount,
    activeLeasedFeeds: sample.activeLeasedFeeds,
    backpressureCount: sample.backpressureCount,
    cleanupActiveLeasedFeeds: sample.cleanupActiveLeasedFeeds,
    cleanupClientActiveFeeds: sample.cleanupClientActiveFeeds,
    cleanupLeakCount: sample.cleanupLeakCount,
    cleanupRowCount: sample.cleanupRowCount,
    measurementRowCount: sample.measurementRowCount,
    measuredCleanup: sample.measuredCleanup,
    mutationCount: sample.mutationCount,
    queuedEventCount: sample.queuedEventCount,
    releasedFeedCount: sample.releasedFeedCount,
    rows: sample.rows,
    seedMutationCount: sample.seedMutationCount,
    subscriberCount: sample.subscriberCount,
  };
  const expectedState = {
    acquiredFeedCount: expected.feedCount,
    activeLeasedFeeds: expected.activeLeasedFeeds,
    backpressureCount: 0,
    cleanupActiveLeasedFeeds: 0,
    cleanupClientActiveFeeds: 0,
    cleanupLeakCount: 0,
    cleanupRowCount: 0,
    measurementRowCount: expected.measurementRowCount,
    measuredCleanup: {
      activeLeasedFeeds: 0,
      activeSubscriptions: 0,
      activeViews: 0,
      clientActiveFeeds: 0,
      leakCount: 0,
      queuedEvents: 0,
      rowCount: 0,
    },
    mutationCount: expected.mutationCount,
    queuedEventCount: 0,
    releasedFeedCount: expected.feedCount,
    rows: expected.rows,
    seedMutationCount: expected.seedMutationCount,
    subscriberCount: expected.subscriberCount,
  };
  if (JSON.stringify(actualState) !== JSON.stringify(expectedState)) {
    throw new Error(
      `Benchmark artifact field ${path} workload state must equal ${JSON.stringify(expectedState)} but was ${JSON.stringify(actualState)}.`,
    );
  }
};

export const validateGrpcLeasedOperationAccounting = (
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
        `Benchmark artifact field ${path}[${caseIndex}].name must be one of: ${grpcLeasedBenchmarkCaseNames.join(", ")}.`,
      );
    }
    if (seen.has(operationCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains duplicate gRPC leased operation case: ${operationCase.name}.`,
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
  for (const expectedName of expectedByName.keys()) {
    if (!seen.has(expectedName)) {
      throw new Error(
        `Benchmark artifact field ${path} is missing gRPC leased operation case: ${expectedName}.`,
      );
    }
  }
};

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

const comparisonSampleState = (samples) => samples.map(deterministicSampleState);

const compareOperationCase = (regressions, taskLabel, thresholds, baselineCase, actualCase) => {
  for (const field of [
    "sampleCount",
    "maxActiveLeasedFeeds",
    "maxCleanupActiveLeasedFeeds",
    "maxCleanupClientActiveFeeds",
    "maxMeasuredCleanupActiveLeasedFeeds",
    "maxMeasuredCleanupActiveSubscriptions",
    "maxMeasuredCleanupActiveViews",
    "maxMeasuredCleanupClientActiveFeeds",
    "maxMeasuredCleanupLeakCount",
    "maxMeasuredCleanupQueuedEvents",
    "maxMeasuredCleanupRowCount",
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
    comparisonSampleState(baselineCase.samples),
    comparisonSampleState(actualCase.samples),
  );
  for (const field of [
    "meanCleanupMs",
    "meanDeltaFanoutMs",
    "meanHealthOverlayMs",
    "meanSnapshotMs",
    "meanSubscriptionMs",
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
    "maxDeltaFanoutMs",
    "maxHealthOverlayMs",
    "maxSnapshotMs",
    "maxSubscriptionMs",
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

const operationCaseByName = (operationCases, path) =>
  mapByUniqueKey(
    operationCases,
    (operationCase) => operationCase.name,
    path,
    "runtime operation case",
  );

export const compareGrpcLeasedBenchmarkTask = (
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
    regressions.push(`${taskLabel}: runtimeOperationCases presence changed.`);
    return regressions;
  }
  const baselineByName = operationCaseByName(
    baselineCases,
    `baseline.tasks[${taskLabel}]`,
  );
  const actualByName = operationCaseByName(actualCases, `actual.tasks[${taskLabel}]`);
  for (const caseName of actualByName.keys()) {
    if (!baselineByName.has(caseName)) {
      regressions.push(`${taskLabel}: unexpected runtime operation case ${caseName}.`);
    }
  }
  for (const baselineCase of baselineCases) {
    const actualCase = actualByName.get(baselineCase.name);
    if (actualCase === undefined) {
      regressions.push(`${taskLabel}: missing runtime operation case ${baselineCase.name}.`);
      continue;
    }
    compareOperationCase(regressions, taskLabel, thresholds, baselineCase, actualCase);
  }
  return regressions;
};
