import type { ViewServerHealth } from "@effect-view-server/config";
import type { ViewServerRuntimeCoreInternalInstance } from "@effect-view-server/runtime-core/internal";
import {
  Cause,
  Duration,
  Effect,
  Exit,
  Option,
  Queue,
  Ref,
  Schedule,
  Schema,
  Stream,
} from "effect";

import { grpcOrderValue, type GrpcOrderValueMessage } from "./grpc-config";
import { grpcMaterializedViewServer } from "./grpc-materialized";
import { makeMaterializedGrpcRuntimeHarness, readGrpcHealthOverlayNow } from "./grpc-runtime";

export type GrpcMaterializedBenchmarkWorkload = "stream-batch" | "burst" | "health-overlay";

type Topics = ReturnType<typeof grpcMaterializedViewServer>["topics"];

const makeGrpcMaterializedBenchmarkHarness = (
  config: ReturnType<typeof grpcMaterializedViewServer>,
) => makeMaterializedGrpcRuntimeHarness({ config });

type GrpcMaterializedRuntimeHarness = Effect.Success<
  ReturnType<typeof makeGrpcMaterializedBenchmarkHarness>
>;

export type GrpcMaterializedBenchmarkOptions = {
  readonly batchSize: number;
  readonly convergenceTimeout: Duration.Input;
  readonly seedRows: number;
};

type GrpcMaterializedBenchmarkMeasurement = {
  readonly healthOverlayMs: number;
  readonly name: string;
  readonly resultRowId: string | null;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly seedRows: number;
  readonly snapshotMs: number;
  readonly startTotalRows: number;
  readonly streamConvergenceMs: number;
  readonly totalRows: number;
};

type GrpcMaterializedBenchmarkCleanup = {
  readonly backpressureCount: number;
  readonly cleanupLeakCount: number;
  readonly cleanupMs: number;
  readonly queuedEventCount: number;
};

type GrpcMaterializedBenchmarkCleanupState = GrpcMaterializedBenchmarkCleanup & {
  readonly cleanupObserved: boolean;
  readonly health: ViewServerHealth<Topics> | undefined;
};

export type GrpcMaterializedBenchmarkSampleEvidence = GrpcMaterializedBenchmarkMeasurement &
  GrpcMaterializedBenchmarkCleanup;

export type GrpcMaterializedBenchmarkSample = GrpcMaterializedBenchmarkSampleEvidence & {
  readonly health: ViewServerHealth<Topics>;
};

class GrpcMaterializedBenchmarkHealthError extends Schema.TaggedErrorClass<GrpcMaterializedBenchmarkHealthError>()(
  "GrpcMaterializedBenchmarkHealthError",
  {
    message: Schema.String,
  },
) {}

class GrpcMaterializedBenchmarkConvergenceError extends Schema.TaggedErrorClass<GrpcMaterializedBenchmarkConvergenceError>()(
  "GrpcMaterializedBenchmarkConvergenceError",
  {
    message: Schema.String,
  },
) {}

export const validateGrpcMaterializedBenchmarkHealth = Effect.fn(
  "ViewServerRuntime.grpc.bench.sample.health.validate",
)(function* (status: ViewServerHealth<Topics>["status"]) {
  if (status !== "ready") {
    return yield* new GrpcMaterializedBenchmarkHealthError({
      message: `gRPC materialized benchmark health must be ready; received ${status}.`,
    });
  }
});

export class GrpcMaterializedBenchmarkSampleError extends Schema.TaggedErrorClass<GrpcMaterializedBenchmarkSampleError>()(
  "GrpcMaterializedBenchmarkSampleError",
  {
    backpressureCount: Schema.Number,
    cleanupLeakCount: Schema.Number,
    cleanupObserved: Schema.Boolean,
    message: Schema.String,
    queuedEventCount: Schema.Number,
    workload: Schema.Literals(["stream-batch", "burst", "health-overlay"]),
  },
) {}

const orderStatus = (index: number): GrpcOrderValueMessage["status"] => {
  if (index % 5 === 0) {
    return "cancelled";
  }
  if (index % 3 === 0) {
    return "closed";
  }
  return "open";
};

const rowsFrom = (start: number, count: number): ReadonlyArray<GrpcOrderValueMessage> =>
  Array.from({ length: count }, (_value, offset) => {
    const index = start + offset;
    return grpcOrderValue(`order-${index}`, index, orderStatus(index));
  });

const offerRows = Effect.fn("ViewServerRuntime.grpc.bench.sample.rows.offer")(function* (
  queue: Queue.Queue<GrpcOrderValueMessage>,
  rows: ReadonlyArray<GrpcOrderValueMessage>,
) {
  yield* Effect.forEach(rows, (row) => Queue.offer(queue, row), { discard: true });
});

const waitForTotalRows = Effect.fn("ViewServerRuntime.grpc.bench.sample.totalRows.wait")(function* (
  runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>,
  expectedTotalRows: number,
  convergenceTimeout: Duration.Input,
) {
  return yield* runtimeCore.client
    .snapshot("orders", {
      select: ["id", "price"],
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 50,
    })
    .pipe(
      Effect.repeat({
        schedule: Schedule.spaced("1 millis"),
        until: (snapshot) => snapshot.totalRows === expectedTotalRows,
      }),
      Effect.timeoutOrElse({
        duration: convergenceTimeout,
        orElse: () =>
          new GrpcMaterializedBenchmarkConvergenceError({
            message: `gRPC materialized benchmark did not converge to ${expectedTotalRows} rows.`,
          }),
      }),
    );
});

export const grpcMaterializedBenchmarkCleanupLeakCount = (
  health: ViewServerHealth<Topics>,
): number => {
  const topicHealth = health.engine.topics["orders"];
  const activeSourceFeeds = health.grpc?.clients["orders"]?.activeFeeds ?? 0;
  return (
    activeSourceFeeds +
    topicHealth.activeSubscriptions +
    topicHealth.activeViews +
    topicHealth.queuedEvents
  );
};

const cleanupProfile = Effect.fn("ViewServerRuntime.grpc.bench.sample.profile.close")(function* (
  harness: GrpcMaterializedRuntimeHarness,
  cleanup: Ref.Ref<GrpcMaterializedBenchmarkCleanupState>,
) {
  const before = performance.now();
  const health = yield* harness.ingress.close.pipe(
    Effect.andThen(readGrpcHealthOverlayNow(harness.runtimeCore.client, harness.health)),
    Effect.ensuring(harness.runtimeCore.close),
  );
  const topicHealth = health.engine.topics.orders;
  yield* Ref.set(cleanup, {
    backpressureCount: topicHealth.backpressureEvents,
    cleanupLeakCount: grpcMaterializedBenchmarkCleanupLeakCount(health),
    cleanupMs: performance.now() - before,
    cleanupObserved: true,
    health,
    queuedEventCount: topicHealth.queuedEvents,
  });
});

const runRowsWorkload = Effect.fn("ViewServerRuntime.grpc.bench.sample.rows.run")(function* (
  workload: "stream-batch" | "burst",
  options: GrpcMaterializedBenchmarkOptions,
  queue: Queue.Queue<GrpcOrderValueMessage>,
  harness: GrpcMaterializedRuntimeHarness,
  startTotalRows: number,
) {
  const rows = rowsFrom(
    options.seedRows,
    workload === "stream-batch" ? options.batchSize : options.batchSize * 4,
  );
  const before = performance.now();
  yield* offerRows(queue, rows);
  const snapshot = yield* waitForTotalRows(
    harness.runtimeCore,
    options.seedRows + rows.length,
    options.convergenceTimeout,
  );
  const afterConvergence = performance.now();
  const healthBefore = performance.now();
  const health = yield* readGrpcHealthOverlayNow(harness.runtimeCore.client, harness.health);
  const healthAfter = performance.now();
  const readBefore = performance.now();
  const result = yield* harness.runtimeCore.client.snapshot("orders", {
    select: ["id", "price", "status"],
    where: [
      { field: "status", type: "equals", filter: "open" },
      { field: "price", type: "greaterThanOrEqual", filter: 10 },
    ],
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 100,
  });
  const readAfter = performance.now();
  const streamConvergenceMs = afterConvergence - before;
  yield* validateGrpcMaterializedBenchmarkHealth(health.status);
  return {
    healthOverlayMs: healthAfter - healthBefore,
    name:
      workload === "stream-batch" ? "gRPC materialized stream batch" : "gRPC materialized burst",
    resultRowId: result.rows[0]?.id ?? null,
    rows: rows.length,
    rowsPerSecond: (rows.length / streamConvergenceMs) * 1_000,
    seedRows: options.seedRows,
    snapshotMs: readAfter - readBefore,
    startTotalRows,
    streamConvergenceMs,
    totalRows: snapshot.totalRows,
  } satisfies GrpcMaterializedBenchmarkMeasurement;
});

const runHealthOverlayWorkload = Effect.fn("ViewServerRuntime.grpc.bench.sample.healthOverlay.run")(
  function* (
    options: GrpcMaterializedBenchmarkOptions,
    harness: GrpcMaterializedRuntimeHarness,
    startTotalRows: number,
  ) {
    const before = performance.now();
    const health = yield* readGrpcHealthOverlayNow(harness.runtimeCore.client, harness.health);
    const after = performance.now();
    yield* validateGrpcMaterializedBenchmarkHealth(health.status);
    return {
      healthOverlayMs: after - before,
      name: "gRPC materialized health overlay",
      resultRowId: null,
      rows: 0,
      rowsPerSecond: 0,
      seedRows: options.seedRows,
      snapshotMs: 0,
      startTotalRows,
      streamConvergenceMs: 0,
      totalRows: health.engine.topics["orders"].rowCount,
    } satisfies GrpcMaterializedBenchmarkMeasurement;
  },
);

const sampleEvidence = (
  sample: GrpcMaterializedBenchmarkSampleEvidence,
): GrpcMaterializedBenchmarkSampleEvidence => ({
  backpressureCount: sample.backpressureCount,
  cleanupLeakCount: sample.cleanupLeakCount,
  cleanupMs: sample.cleanupMs,
  healthOverlayMs: sample.healthOverlayMs,
  name: sample.name,
  queuedEventCount: sample.queuedEventCount,
  resultRowId: sample.resultRowId,
  rows: sample.rows,
  rowsPerSecond: sample.rowsPerSecond,
  seedRows: sample.seedRows,
  snapshotMs: sample.snapshotMs,
  startTotalRows: sample.startTotalRows,
  streamConvergenceMs: sample.streamConvergenceMs,
  totalRows: sample.totalRows,
});

const median = (values: ReadonlyArray<number>): number => {
  const sorted = values.toSorted((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (Option.getOrThrow(Option.fromNullishOr(sorted[middle - 1])) +
        Option.getOrThrow(Option.fromNullishOr(sorted[middle]))) /
        2
    : Option.getOrThrow(Option.fromNullishOr(sorted[middle]));
};

export const summarizeGrpcMaterializedBenchmarkSamples = (
  samples: ReadonlyArray<GrpcMaterializedBenchmarkSampleEvidence>,
  name: string,
  expectedSampleCount: number,
) => {
  const matching = samples.filter((sample) => sample.name === name).map(sampleEvidence);
  if (matching.length !== expectedSampleCount) {
    throw new Error(
      `gRPC materialized benchmark case ${name} produced ${matching.length} sample(s), expected exactly ${expectedSampleCount}.`,
    );
  }
  const first = matching[0];
  if (first === undefined) {
    throw new Error(`gRPC materialized benchmark case ${name} produced no samples.`);
  }
  for (const sample of matching) {
    if (
      sample.seedRows !== first.seedRows ||
      sample.startTotalRows !== sample.seedRows ||
      sample.rows !== first.rows ||
      sample.totalRows !== sample.seedRows + sample.rows
    ) {
      throw new Error(
        `gRPC materialized benchmark case ${name} did not preserve identical seeded sample state.`,
      );
    }
    if (
      sample.cleanupLeakCount !== 0 ||
      sample.queuedEventCount !== 0 ||
      sample.backpressureCount !== 0
    ) {
      throw new Error(
        `gRPC materialized benchmark case ${name} recorded non-zero cleanup, queue, or backpressure evidence.`,
      );
    }
  }
  const totals = matching.reduce(
    (accumulator, sample) => ({
      cleanupMs: accumulator.cleanupMs + sample.cleanupMs,
      healthOverlayMs: accumulator.healthOverlayMs + sample.healthOverlayMs,
      rows: accumulator.rows + sample.rows,
      rowsPerSecond: accumulator.rowsPerSecond + sample.rowsPerSecond,
      seedRows: accumulator.seedRows + sample.seedRows,
      snapshotMs: accumulator.snapshotMs + sample.snapshotMs,
      streamConvergenceMs: accumulator.streamConvergenceMs + sample.streamConvergenceMs,
    }),
    {
      cleanupMs: 0,
      healthOverlayMs: 0,
      rows: 0,
      rowsPerSecond: 0,
      seedRows: 0,
      snapshotMs: 0,
      streamConvergenceMs: 0,
    },
  );
  const sampleCount = matching.length;
  const meanRowsPerSecond = totals.rowsPerSecond / sampleCount;
  const rowsPerSecondVariance =
    matching.reduce((total, sample) => total + (sample.rowsPerSecond - meanRowsPerSecond) ** 2, 0) /
    sampleCount;
  return {
    maxCleanupMs: Math.max(...matching.map((sample) => sample.cleanupMs)),
    maxHealthOverlayMs: Math.max(...matching.map((sample) => sample.healthOverlayMs)),
    maxSnapshotMs: Math.max(...matching.map((sample) => sample.snapshotMs)),
    maxStreamConvergenceMs: Math.max(...matching.map((sample) => sample.streamConvergenceMs)),
    meanCleanupMs: totals.cleanupMs / sampleCount,
    meanHealthOverlayMs: totals.healthOverlayMs / sampleCount,
    meanRowsPerSecond,
    meanSnapshotMs: totals.snapshotMs / sampleCount,
    meanStreamConvergenceMs: totals.streamConvergenceMs / sampleCount,
    medianRowsPerSecond: median(matching.map((sample) => sample.rowsPerSecond)),
    mutationCount: totals.rows,
    name,
    pooledRowsPerSecond:
      totals.streamConvergenceMs === 0 ? 0 : (totals.rows / totals.streamConvergenceMs) * 1_000,
    rowsPerSecondCoefficientOfVariation:
      meanRowsPerSecond === 0 ? 0 : Math.sqrt(rowsPerSecondVariance) / meanRowsPerSecond,
    sampleCount,
    samples: matching,
    seedMutationCount: totals.seedRows,
    startTotalRows: first.startTotalRows,
    totalRows: first.totalRows,
  };
};

export const runGrpcMaterializedBenchmarkSample = Effect.fn(
  "ViewServerRuntime.grpc.bench.sample.run",
)(function* (
  workload: GrpcMaterializedBenchmarkWorkload,
  options: GrpcMaterializedBenchmarkOptions,
) {
  const cleanup = yield* Ref.make<GrpcMaterializedBenchmarkCleanupState>({
    backpressureCount: 0,
    cleanupLeakCount: 0,
    cleanupMs: 0,
    cleanupObserved: false,
    health: undefined,
    queuedEventCount: 0,
  });
  const measurementExit = yield* Effect.exit(
    Effect.scoped(
      Effect.gen(function* () {
        const queue = yield* Effect.acquireRelease(
          Queue.unbounded<GrpcOrderValueMessage>(),
          Queue.shutdown,
        );
        const config = grpcMaterializedViewServer(Stream.fromQueue(queue));
        const harness = yield* Effect.acquireRelease(
          makeGrpcMaterializedBenchmarkHarness(config),
          (currentHarness) => cleanupProfile(currentHarness, cleanup).pipe(Effect.orDie),
        );
        yield* offerRows(queue, rowsFrom(0, options.seedRows));
        const seedSnapshot = yield* waitForTotalRows(
          harness.runtimeCore,
          options.seedRows,
          options.convergenceTimeout,
        );
        return workload === "health-overlay"
          ? yield* runHealthOverlayWorkload(options, harness, seedSnapshot.totalRows)
          : yield* runRowsWorkload(workload, options, queue, harness, seedSnapshot.totalRows);
      }),
    ),
  );
  const cleanupState = yield* Ref.get(cleanup);
  if (Exit.isFailure(measurementExit)) {
    return yield* new GrpcMaterializedBenchmarkSampleError({
      backpressureCount: cleanupState.backpressureCount,
      cleanupLeakCount: cleanupState.cleanupLeakCount,
      cleanupObserved: cleanupState.cleanupObserved,
      message: Cause.pretty(measurementExit.cause),
      queuedEventCount: cleanupState.queuedEventCount,
      workload,
    });
  }
  const health = Option.getOrThrow(Option.fromNullishOr(cleanupState.health));
  return {
    ...measurementExit.value,
    backpressureCount: cleanupState.backpressureCount,
    cleanupLeakCount: cleanupState.cleanupLeakCount,
    cleanupMs: cleanupState.cleanupMs,
    health,
    queuedEventCount: cleanupState.queuedEventCount,
  } satisfies GrpcMaterializedBenchmarkSample;
});
