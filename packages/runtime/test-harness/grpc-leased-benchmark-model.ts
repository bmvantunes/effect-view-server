import { Schema, type Duration } from "effect";

export const grpcLeasedBenchmarkCases = [
  { name: "gRPC leased first subscriber", workload: "first-subscriber" },
  { name: "gRPC leased same-route reuse", workload: "same-route-reuse" },
  {
    name: "gRPC leased one route many subscribers",
    workload: "one-route-many-subscribers",
  },
  {
    name: "gRPC leased local-filter live snapshot",
    workload: "local-filter-live-snapshot",
  },
  {
    name: "gRPC leased retained local-filter snapshot",
    workload: "retained-local-filter-snapshot",
  },
  { name: "gRPC leased delta fanout", workload: "delta-fanout" },
  {
    name: "gRPC leased partitioned write convergence",
    workload: "partitioned-write-convergence",
  },
  { name: "gRPC leased health refresh overhead", workload: "health-refresh-overhead" },
  { name: "gRPC leased last-subscriber cleanup", workload: "last-subscriber-cleanup" },
  { name: "gRPC leased many routes", workload: "many-routes" },
] as const;

export type GrpcLeasedBenchmarkWorkload = (typeof grpcLeasedBenchmarkCases)[number]["workload"];

export type GrpcLeasedBenchmarkOptions = {
  readonly convergenceTimeout: Duration.Input;
  readonly retainedRows: number;
  readonly routeCount: number;
  readonly rowsPerFeed: number;
};

export type GrpcLeasedMeasuredCleanupEvidence = {
  readonly activeLeasedFeeds: number;
  readonly activeSubscriptions: number;
  readonly activeViews: number;
  readonly clientActiveFeeds: number;
  readonly leakCount: number;
  readonly queuedEvents: number;
  readonly rowCount: number;
};

export type GrpcLeasedBenchmarkSampleEvidence = {
  readonly acquiredFeedCount: number;
  readonly activeLeasedFeeds: number;
  readonly backpressureCount: number;
  readonly cleanupActiveLeasedFeeds: number;
  readonly cleanupClientActiveFeeds: number;
  readonly cleanupLeakCount: number;
  readonly cleanupMs: number;
  readonly cleanupRowCount: number;
  readonly deltaFanoutMs: number;
  readonly healthOverlayMs: number;
  readonly measurementRowCount: number;
  readonly measuredCleanup: GrpcLeasedMeasuredCleanupEvidence;
  readonly mutationCount: number;
  readonly name: string;
  readonly queuedEventCount: number;
  readonly releasedFeedCount: number;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly seedMutationCount: number;
  readonly snapshotMs: number;
  readonly subscriberCount: number;
  readonly subscriptionMs: number;
};

export class GrpcLeasedBenchmarkSampleError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkSampleError>()(
  "GrpcLeasedBenchmarkSampleError",
  {
    acquiredFeedCount: Schema.Number,
    cleanupActiveLeasedFeeds: Schema.Number,
    cleanupClientActiveFeeds: Schema.Number,
    cleanupLeakCount: Schema.Number,
    cleanupObserved: Schema.Boolean,
    cleanupRowCount: Schema.Number,
    message: Schema.String,
    releasedFeedCount: Schema.Number,
    workload: Schema.Literals([
      "first-subscriber",
      "same-route-reuse",
      "one-route-many-subscribers",
      "local-filter-live-snapshot",
      "retained-local-filter-snapshot",
      "delta-fanout",
      "partitioned-write-convergence",
      "health-refresh-overhead",
      "last-subscriber-cleanup",
      "many-routes",
    ]),
  },
) {}

const sampleEvidence = (
  sample: GrpcLeasedBenchmarkSampleEvidence,
): GrpcLeasedBenchmarkSampleEvidence => ({
  acquiredFeedCount: sample.acquiredFeedCount,
  activeLeasedFeeds: sample.activeLeasedFeeds,
  backpressureCount: sample.backpressureCount,
  cleanupActiveLeasedFeeds: sample.cleanupActiveLeasedFeeds,
  cleanupClientActiveFeeds: sample.cleanupClientActiveFeeds,
  cleanupLeakCount: sample.cleanupLeakCount,
  cleanupMs: sample.cleanupMs,
  cleanupRowCount: sample.cleanupRowCount,
  deltaFanoutMs: sample.deltaFanoutMs,
  healthOverlayMs: sample.healthOverlayMs,
  measurementRowCount: sample.measurementRowCount,
  measuredCleanup: {
    activeLeasedFeeds: sample.measuredCleanup.activeLeasedFeeds,
    activeSubscriptions: sample.measuredCleanup.activeSubscriptions,
    activeViews: sample.measuredCleanup.activeViews,
    clientActiveFeeds: sample.measuredCleanup.clientActiveFeeds,
    leakCount: sample.measuredCleanup.leakCount,
    queuedEvents: sample.measuredCleanup.queuedEvents,
    rowCount: sample.measuredCleanup.rowCount,
  },
  mutationCount: sample.mutationCount,
  name: sample.name,
  queuedEventCount: sample.queuedEventCount,
  releasedFeedCount: sample.releasedFeedCount,
  rows: sample.rows,
  rowsPerSecond: sample.rowsPerSecond,
  seedMutationCount: sample.seedMutationCount,
  snapshotMs: sample.snapshotMs,
  subscriberCount: sample.subscriberCount,
  subscriptionMs: sample.subscriptionMs,
});

const median = (values: ReadonlyArray<number>): number => {
  const sorted = values.toSorted((left, right) => left - right);
  const lowerMiddle = Math.floor((sorted.length - 1) / 2);
  const upperMiddle = Math.floor(sorted.length / 2);
  const middleValues = sorted.slice(lowerMiddle, upperMiddle + 1);
  return middleValues.reduce((total, value) => total + value, 0) / middleValues.length;
};

const sameSampleState = (
  left: GrpcLeasedBenchmarkSampleEvidence,
  right: GrpcLeasedBenchmarkSampleEvidence,
): boolean =>
  left.acquiredFeedCount === right.acquiredFeedCount &&
  left.activeLeasedFeeds === right.activeLeasedFeeds &&
  left.cleanupActiveLeasedFeeds === right.cleanupActiveLeasedFeeds &&
  left.cleanupClientActiveFeeds === right.cleanupClientActiveFeeds &&
  left.cleanupLeakCount === right.cleanupLeakCount &&
  left.cleanupRowCount === right.cleanupRowCount &&
  left.measurementRowCount === right.measurementRowCount &&
  left.measuredCleanup.activeLeasedFeeds === right.measuredCleanup.activeLeasedFeeds &&
  left.measuredCleanup.activeSubscriptions === right.measuredCleanup.activeSubscriptions &&
  left.measuredCleanup.activeViews === right.measuredCleanup.activeViews &&
  left.measuredCleanup.clientActiveFeeds === right.measuredCleanup.clientActiveFeeds &&
  left.measuredCleanup.leakCount === right.measuredCleanup.leakCount &&
  left.measuredCleanup.queuedEvents === right.measuredCleanup.queuedEvents &&
  left.measuredCleanup.rowCount === right.measuredCleanup.rowCount &&
  left.mutationCount === right.mutationCount &&
  left.releasedFeedCount === right.releasedFeedCount &&
  left.rows === right.rows &&
  left.seedMutationCount === right.seedMutationCount &&
  left.subscriberCount === right.subscriberCount;

export const summarizeGrpcLeasedBenchmarkSamples = (
  samples: ReadonlyArray<GrpcLeasedBenchmarkSampleEvidence>,
  name: string,
  expectedSampleCount: number,
) => {
  const matching = samples.filter((sample) => sample.name === name).map(sampleEvidence);
  if (matching.length !== expectedSampleCount) {
    throw new Error(
      `gRPC leased benchmark case ${name} produced ${matching.length} sample(s), expected exactly ${expectedSampleCount}.`,
    );
  }
  const first = matching[0];
  if (first === undefined) {
    throw new Error(`gRPC leased benchmark case ${name} produced no samples.`);
  }
  for (const sample of matching) {
    if (!sameSampleState(sample, first)) {
      throw new Error(
        `gRPC leased benchmark case ${name} did not preserve identical sample state.`,
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
      sample.backpressureCount !== 0 ||
      sample.acquiredFeedCount !== sample.releasedFeedCount
    ) {
      throw new Error(
        `gRPC leased benchmark case ${name} recorded non-zero cleanup, queue, or backpressure evidence.`,
      );
    }
  }
  const totals = matching.reduce(
    (accumulator, sample) => ({
      cleanupMs: accumulator.cleanupMs + sample.cleanupMs,
      deltaFanoutMs: accumulator.deltaFanoutMs + sample.deltaFanoutMs,
      healthOverlayMs: accumulator.healthOverlayMs + sample.healthOverlayMs,
      mutationCount: accumulator.mutationCount + sample.mutationCount,
      rows: accumulator.rows + sample.rows,
      rowsPerSecond: accumulator.rowsPerSecond + sample.rowsPerSecond,
      seedMutationCount: accumulator.seedMutationCount + sample.seedMutationCount,
      snapshotMs: accumulator.snapshotMs + sample.snapshotMs,
      subscriptionMs: accumulator.subscriptionMs + sample.subscriptionMs,
    }),
    {
      cleanupMs: 0,
      deltaFanoutMs: 0,
      healthOverlayMs: 0,
      mutationCount: 0,
      rows: 0,
      rowsPerSecond: 0,
      seedMutationCount: 0,
      snapshotMs: 0,
      subscriptionMs: 0,
    },
  );
  const sampleCount = matching.length;
  const meanRowsPerSecond = totals.rowsPerSecond / sampleCount;
  const rowsPerSecondVariance =
    matching.reduce((total, sample) => total + (sample.rowsPerSecond - meanRowsPerSecond) ** 2, 0) /
    sampleCount;
  return {
    maxActiveLeasedFeeds: Math.max(...matching.map((sample) => sample.activeLeasedFeeds)),
    maxCleanupActiveLeasedFeeds: Math.max(
      ...matching.map((sample) => sample.cleanupActiveLeasedFeeds),
    ),
    maxCleanupClientActiveFeeds: Math.max(
      ...matching.map((sample) => sample.cleanupClientActiveFeeds),
    ),
    maxCleanupMs: Math.max(...matching.map((sample) => sample.cleanupMs)),
    maxDeltaFanoutMs: Math.max(...matching.map((sample) => sample.deltaFanoutMs)),
    maxHealthOverlayMs: Math.max(...matching.map((sample) => sample.healthOverlayMs)),
    maxMeasuredCleanupActiveLeasedFeeds: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.activeLeasedFeeds),
    ),
    maxMeasuredCleanupActiveSubscriptions: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.activeSubscriptions),
    ),
    maxMeasuredCleanupActiveViews: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.activeViews),
    ),
    maxMeasuredCleanupClientActiveFeeds: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.clientActiveFeeds),
    ),
    maxMeasuredCleanupLeakCount: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.leakCount),
    ),
    maxMeasuredCleanupQueuedEvents: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.queuedEvents),
    ),
    maxMeasuredCleanupRowCount: Math.max(
      ...matching.map((sample) => sample.measuredCleanup.rowCount),
    ),
    maxSnapshotMs: Math.max(...matching.map((sample) => sample.snapshotMs)),
    maxSubscriptionMs: Math.max(...matching.map((sample) => sample.subscriptionMs)),
    meanCleanupMs: totals.cleanupMs / sampleCount,
    meanDeltaFanoutMs: totals.deltaFanoutMs / sampleCount,
    meanHealthOverlayMs: totals.healthOverlayMs / sampleCount,
    meanRowsPerSecond,
    meanSnapshotMs: totals.snapshotMs / sampleCount,
    meanSubscriptionMs: totals.subscriptionMs / sampleCount,
    medianRowsPerSecond: median(matching.map((sample) => sample.rowsPerSecond)),
    mutationCount: totals.mutationCount,
    name,
    pooledRowsPerSecond: totals.snapshotMs === 0 ? 0 : (totals.rows / totals.snapshotMs) * 1_000,
    rowsPerSecondCoefficientOfVariation:
      meanRowsPerSecond === 0 ? 0 : Math.sqrt(rowsPerSecondVariance) / meanRowsPerSecond,
    sampleCount,
    samples: matching,
    seedMutationCount: totals.seedMutationCount,
  };
};
