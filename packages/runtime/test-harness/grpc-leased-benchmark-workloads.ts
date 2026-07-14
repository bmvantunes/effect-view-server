import { Cause, Effect, Schema, Scope } from "effect";

import type {
  GrpcLeasedBenchmarkOptions,
  GrpcLeasedMeasuredCleanupEvidence,
  GrpcLeasedBenchmarkWorkload,
} from "./grpc-leased-benchmark-model";
import {
  activeGrpcLeasedFeedCount,
  auditGrpcLeasedMeasuredCleanup,
  closeGrpcLeasedSubscriptions,
  grpcLeasedOpenRows,
  grpcLeasedRows,
  observeGrpcLeasedBenchmarkHealth,
  offerGrpcLeasedRows,
  queueForGrpcLeasedRoute,
  grpcLeasedSubscriberCount,
  readHealthyGrpcLeasedBenchmarkState,
  waitForGrpcLeasedRows,
  watchGrpcLeasedSubscription,
  type GrpcLeasedBenchmarkCleanup,
  type GrpcLeasedBenchmarkContext,
  type GrpcLeasedHealthObservation,
  type WatchedGrpcLeasedSubscription,
} from "./grpc-leased-benchmark-runtime";

export type GrpcLeasedBenchmarkMeasurement = {
  readonly activeLeasedFeeds: number;
  readonly cleanupMs: number;
  readonly deltaFanoutMs: number;
  readonly healthOverlayMs: number;
  readonly measurementRowCount: number;
  readonly measuredCleanup: GrpcLeasedMeasuredCleanupEvidence;
  readonly mutationCount: number;
  readonly name: string;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly seedMutationCount: number;
  readonly snapshotMs: number;
  readonly subscriberCount: number;
  readonly subscriptionMs: number;
};

class GrpcLeasedBenchmarkStateError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkStateError>()(
  "GrpcLeasedBenchmarkStateError",
  { message: Schema.String },
) {}

export class GrpcLeasedBenchmarkWorkloadError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkWorkloadError>()(
  "GrpcLeasedBenchmarkWorkloadError",
  { message: Schema.String },
) {}

const rowsPerSecond = (rows: number, elapsedMs: number): number => (rows / elapsedMs) * 1_000;

const latestCompleteGrpcLeasedBenchmarkHealth = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.health.latestComplete",
)(function* (
  healthResults: ReadonlyArray<GrpcLeasedBenchmarkCleanup["health"]>,
  expectedActiveLeasedFeeds: number,
) {
  const invalidHealth = healthResults.find(
    (health) => activeGrpcLeasedFeedCount(health) !== expectedActiveLeasedFeeds,
  );
  if (invalidHealth !== undefined) {
    return yield* new GrpcLeasedBenchmarkStateError({
      message: "gRPC leased benchmark health refresh observed incomplete leased feeds.",
    });
  }
  const lastHealth = healthResults.at(-1);
  if (lastHealth === undefined) {
    return yield* new GrpcLeasedBenchmarkStateError({
      message: "gRPC leased benchmark health refresh produced no health samples.",
    });
  }
  return lastHealth;
});

const closeTracked = Effect.fn("ViewServerRuntime.grpc.leased.bench.tracked.close")(function* (
  tracked: Array<WatchedGrpcLeasedSubscription>,
) {
  yield* closeGrpcLeasedSubscriptions(tracked);
  tracked.length = 0;
});

const watchAndTrack = Effect.fn("ViewServerRuntime.grpc.leased.bench.tracked.watch")(function* (
  tracked: Array<WatchedGrpcLeasedSubscription>,
  subscription: Parameters<typeof watchGrpcLeasedSubscription>[0],
) {
  const watched = yield* watchGrpcLeasedSubscription(subscription);
  tracked.push(watched);
  return watched;
});

const measureTrackedCleanup = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.tracked.measureCleanup",
)(function* (context: GrpcLeasedBenchmarkContext, tracked: Array<WatchedGrpcLeasedSubscription>) {
  const before = performance.now();
  yield* closeTracked(tracked);
  const after = performance.now();
  const measuredCleanup = yield* auditGrpcLeasedMeasuredCleanup(context);
  return {
    cleanupMs: after - before,
    measuredCleanup,
  };
});

const subscriptionMeasurement = (input: {
  readonly cleanup: {
    readonly cleanupMs: number;
    readonly measuredCleanup: GrpcLeasedMeasuredCleanupEvidence;
  };
  readonly deltaFanoutMs?: number;
  readonly health: GrpcLeasedHealthObservation;
  readonly mutationCount: number;
  readonly name: string;
  readonly rows: number;
  readonly seedMutationCount?: number;
  readonly snapshotMs: number;
  readonly subscriptionMs: number;
}): GrpcLeasedBenchmarkMeasurement => ({
  activeLeasedFeeds: input.health.activeLeasedFeeds,
  cleanupMs: input.cleanup.cleanupMs,
  deltaFanoutMs: input.deltaFanoutMs ?? 0,
  healthOverlayMs: input.health.healthOverlayMs,
  measurementRowCount: input.health.measurementRowCount,
  measuredCleanup: input.cleanup.measuredCleanup,
  mutationCount: input.mutationCount,
  name: input.name,
  rows: input.rows,
  rowsPerSecond: rowsPerSecond(input.rows, input.snapshotMs),
  seedMutationCount: input.seedMutationCount ?? 0,
  snapshotMs: input.snapshotMs,
  subscriberCount: input.health.subscriberCount,
  subscriptionMs: input.subscriptionMs,
});

const runSubscriptionCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.subscription.run")(
  function* (
    context: GrpcLeasedBenchmarkContext,
    input: {
      readonly additionalSubscriberCount: number;
      readonly name: string;
      readonly preOpenSubscriber: boolean;
      readonly region: string;
    },
  ) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      if (input.preOpenSubscriber) {
        const subscription = yield* context.harness.manager.liveClient.subscribe("orders", {
          select: ["id", "price", "status", "region"],
          where: { region: { eq: input.region } },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 100,
        });
        yield* watchAndTrack(tracked, subscription);
      }
      const beforeSubscribe = performance.now();
      const subscriptions = yield* Effect.forEach(
        Array.from({ length: input.additionalSubscriberCount }),
        () =>
          context.harness.manager.liveClient.subscribe("orders", {
            select: ["id", "price", "status", "region"],
            where: { region: { eq: input.region } },
            orderBy: [{ field: "updatedAt", direction: "desc" }],
            limit: 100,
          }),
      );
      const afterSubscribe = performance.now();
      yield* Effect.forEach(subscriptions, (subscription) => watchAndTrack(tracked, subscription));
      const rows = grpcLeasedRows(input.region, 0, context.options.rowsPerFeed);
      const beforeSnapshot = performance.now();
      yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, input.region), rows);
      yield* Effect.forEach(tracked, (subscription) =>
        waitForGrpcLeasedRows(subscription, rows.length, context.options.convergenceTimeout),
      );
      const afterSnapshot = performance.now();
      const health = yield* observeGrpcLeasedBenchmarkHealth(context);
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return subscriptionMeasurement({
        cleanup,
        health,
        mutationCount: rows.length,
        name: input.name,
        rows: rows.length,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const runLocalFilterCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.localFilter.run")(
  function* (context: GrpcLeasedBenchmarkContext) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      const beforeSubscribe = performance.now();
      const subscription = yield* context.harness.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: {
          region: { eq: "live-filter" },
          status: { eq: "open" },
          price: { gte: 10 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: context.options.rowsPerFeed,
      });
      const afterSubscribe = performance.now();
      const watched = yield* watchAndTrack(tracked, subscription);
      const rows = grpcLeasedOpenRows("live-filter", context.options.rowsPerFeed);
      const beforeSnapshot = performance.now();
      yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, "live-filter"), rows);
      yield* waitForGrpcLeasedRows(watched, rows.length, context.options.convergenceTimeout);
      const afterSnapshot = performance.now();
      const health = yield* observeGrpcLeasedBenchmarkHealth(context);
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return subscriptionMeasurement({
        cleanup,
        health,
        mutationCount: rows.length,
        name: "gRPC leased local-filter live snapshot",
        rows: rows.length,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const runRetainedLocalFilterCase = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.retainedLocalFilter.run",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const tracked: Array<WatchedGrpcLeasedSubscription> = [];
  return yield* Effect.gen(function* () {
    const holderSubscription = yield* context.harness.manager.liveClient.subscribe("orders", {
      select: ["id", "price", "status", "region"],
      where: { region: { eq: "retained-filter" } },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 100,
    });
    const holder = yield* watchAndTrack(tracked, holderSubscription);
    const rows = grpcLeasedOpenRows("retained-filter", context.options.retainedRows);
    yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, "retained-filter"), rows);
    yield* waitForGrpcLeasedRows(holder, rows.length, context.options.convergenceTimeout);

    const beforeSnapshot = performance.now();
    const beforeSubscribe = beforeSnapshot;
    const subscription = yield* context.harness.manager.liveClient.subscribe("orders", {
      select: ["id", "price", "status", "region"],
      where: {
        region: { eq: "retained-filter" },
        status: { eq: "open" },
        price: { gte: 10 },
      },
      orderBy: [{ field: "updatedAt", direction: "desc" }],
      limit: 100,
    });
    const afterSubscribe = performance.now();
    const watched = yield* watchAndTrack(tracked, subscription);
    yield* waitForGrpcLeasedRows(watched, rows.length, context.options.convergenceTimeout);
    const afterSnapshot = performance.now();
    const health = yield* observeGrpcLeasedBenchmarkHealth(context);
    const cleanup = yield* measureTrackedCleanup(context, tracked);
    return subscriptionMeasurement({
      cleanup,
      health,
      mutationCount: 0,
      name: "gRPC leased retained local-filter snapshot",
      rows: rows.length,
      seedMutationCount: rows.length,
      snapshotMs: afterSnapshot - beforeSnapshot,
      subscriptionMs: afterSubscribe - beforeSubscribe,
    });
  }).pipe(Effect.ensuring(closeTracked(tracked)));
});

const runDeltaFanoutCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.deltaFanout.run")(
  function* (context: GrpcLeasedBenchmarkContext) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      const subscriberCount = 25;
      const beforeSubscribe = performance.now();
      const subscriptions = yield* Effect.forEach(Array.from({ length: subscriberCount }), () =>
        context.harness.manager.liveClient.subscribe("orders", {
          select: ["id", "price", "status", "region"],
          where: { region: { eq: "delta-fanout" } },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 100,
        }),
      );
      const afterSubscribe = performance.now();
      yield* Effect.forEach(subscriptions, (subscription) => watchAndTrack(tracked, subscription));
      const seed = grpcLeasedRows("delta-fanout", 0, 1);
      yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, "delta-fanout"), seed);
      yield* Effect.forEach(tracked, (subscription) =>
        waitForGrpcLeasedRows(subscription, 1, context.options.convergenceTimeout),
      );
      const rows = grpcLeasedRows("delta-fanout", 1, context.options.rowsPerFeed);
      const beforeDelta = performance.now();
      yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, "delta-fanout"), rows);
      yield* Effect.forEach(tracked, (subscription) =>
        waitForGrpcLeasedRows(subscription, rows.length + 1, context.options.convergenceTimeout),
      );
      const afterDelta = performance.now();
      const deltaFanoutMs = afterDelta - beforeDelta;
      const health = yield* observeGrpcLeasedBenchmarkHealth(context);
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return subscriptionMeasurement({
        cleanup,
        deltaFanoutMs,
        health,
        mutationCount: rows.length,
        name: "gRPC leased delta fanout",
        rows: rows.length,
        seedMutationCount: seed.length,
        snapshotMs: deltaFanoutMs,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const routeRegions = (options: GrpcLeasedBenchmarkOptions): ReadonlyArray<string> =>
  Array.from({ length: options.routeCount }, (_value, index) => `route-${index}`);

const runPartitionedWriteCase = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.partitionedWrite.run",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const tracked: Array<WatchedGrpcLeasedSubscription> = [];
  return yield* Effect.gen(function* () {
    const regions = routeRegions(context.options);
    const beforeSubscribe = performance.now();
    const subscriptions = yield* Effect.forEach(regions, (region) =>
      context.harness.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: { region: { eq: region } },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: context.options.rowsPerFeed,
      }),
    );
    const afterSubscribe = performance.now();
    yield* Effect.forEach(subscriptions, (subscription) => watchAndTrack(tracked, subscription));
    const beforeSnapshot = performance.now();
    yield* Effect.forEach(
      regions,
      (region) =>
        offerGrpcLeasedRows(
          queueForGrpcLeasedRoute(context, region),
          grpcLeasedRows(region, 0, context.options.rowsPerFeed),
        ),
      { discard: true },
    );
    yield* Effect.forEach(tracked, (subscription) =>
      waitForGrpcLeasedRows(
        subscription,
        context.options.rowsPerFeed,
        context.options.convergenceTimeout,
      ),
    );
    const afterSnapshot = performance.now();
    const health = yield* observeGrpcLeasedBenchmarkHealth(context);
    const cleanup = yield* measureTrackedCleanup(context, tracked);
    const rows = context.options.rowsPerFeed * regions.length;
    return subscriptionMeasurement({
      cleanup,
      health,
      mutationCount: rows,
      name: "gRPC leased partitioned write convergence",
      rows,
      snapshotMs: afterSnapshot - beforeSnapshot,
      subscriptionMs: afterSubscribe - beforeSubscribe,
    });
  }).pipe(Effect.ensuring(closeTracked(tracked)));
});

const runHealthRefreshCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.healthRefresh.run")(
  function* (context: GrpcLeasedBenchmarkContext) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      const regions = routeRegions(context.options);
      const subscriptions = yield* Effect.forEach(regions, (region) =>
        context.harness.manager.liveClient.subscribe("orders", {
          select: ["id", "price", "status", "region"],
          where: { region: { eq: region } },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 10,
        }),
      );
      yield* Effect.forEach(subscriptions, (subscription) => watchAndTrack(tracked, subscription));
      yield* Effect.forEach(
        regions,
        (region) =>
          offerGrpcLeasedRows(
            queueForGrpcLeasedRoute(context, region),
            grpcLeasedRows(region, 0, 1),
          ),
        { discard: true },
      );
      yield* Effect.forEach(tracked, (subscription) =>
        waitForGrpcLeasedRows(subscription, 1, context.options.convergenceTimeout),
      );
      const beforeHealth = performance.now();
      const healthResults = yield* Effect.forEach(regions, () =>
        readHealthyGrpcLeasedBenchmarkState(context),
      );
      const afterHealth = performance.now();
      const lastHealth = yield* latestCompleteGrpcLeasedBenchmarkHealth(
        healthResults,
        regions.length,
      );
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return {
        activeLeasedFeeds: activeGrpcLeasedFeedCount(lastHealth),
        cleanupMs: cleanup.cleanupMs,
        deltaFanoutMs: 0,
        healthOverlayMs: afterHealth - beforeHealth,
        measurementRowCount: lastHealth.engine.topics.orders.rowCount,
        measuredCleanup: cleanup.measuredCleanup,
        mutationCount: 0,
        name: "gRPC leased health refresh overhead",
        rows: 0,
        rowsPerSecond: 0,
        seedMutationCount: regions.length,
        snapshotMs: 0,
        subscriberCount: grpcLeasedSubscriberCount(lastHealth),
        subscriptionMs: 0,
      } satisfies GrpcLeasedBenchmarkMeasurement;
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const runCleanupLatencyCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.cleanupLatency.run")(
  function* (context: GrpcLeasedBenchmarkContext) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      const beforeSubscribe = performance.now();
      const subscription = yield* context.harness.manager.liveClient.subscribe("orders", {
        select: ["id", "price", "status", "region"],
        where: { region: { eq: "cleanup-latency" } },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: 100,
      });
      const afterSubscribe = performance.now();
      const watched = yield* watchAndTrack(tracked, subscription);
      const rows = grpcLeasedRows("cleanup-latency", 0, context.options.rowsPerFeed);
      const beforeSnapshot = performance.now();
      yield* offerGrpcLeasedRows(queueForGrpcLeasedRoute(context, "cleanup-latency"), rows);
      yield* waitForGrpcLeasedRows(watched, rows.length, context.options.convergenceTimeout);
      const afterSnapshot = performance.now();
      const health = yield* observeGrpcLeasedBenchmarkHealth(context);
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return subscriptionMeasurement({
        cleanup,
        health,
        mutationCount: rows.length,
        name: "gRPC leased last-subscriber cleanup",
        rows: rows.length,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const runManyRoutesCase = Effect.fn("ViewServerRuntime.grpc.leased.bench.manyRoutes.run")(
  function* (context: GrpcLeasedBenchmarkContext) {
    const tracked: Array<WatchedGrpcLeasedSubscription> = [];
    return yield* Effect.gen(function* () {
      const regions = routeRegions(context.options);
      const beforeSubscribe = performance.now();
      const subscriptions = yield* Effect.forEach(regions, (region) =>
        context.harness.manager.liveClient.subscribe("orders", {
          select: ["id", "price", "status", "region"],
          where: { region: { eq: region } },
          orderBy: [{ field: "updatedAt", direction: "desc" }],
          limit: 10,
        }),
      );
      const afterSubscribe = performance.now();
      yield* Effect.forEach(subscriptions, (subscription) => watchAndTrack(tracked, subscription));
      const beforeSnapshot = performance.now();
      yield* Effect.forEach(
        regions,
        (region) =>
          offerGrpcLeasedRows(
            queueForGrpcLeasedRoute(context, region),
            grpcLeasedRows(region, 0, 1),
          ),
        { discard: true },
      );
      yield* Effect.forEach(tracked, (subscription) =>
        waitForGrpcLeasedRows(subscription, 1, context.options.convergenceTimeout),
      );
      const afterSnapshot = performance.now();
      const health = yield* observeGrpcLeasedBenchmarkHealth(context);
      const cleanup = yield* measureTrackedCleanup(context, tracked);
      return subscriptionMeasurement({
        cleanup,
        health,
        mutationCount: regions.length,
        name: "gRPC leased many routes",
        rows: regions.length,
        snapshotMs: afterSnapshot - beforeSnapshot,
        subscriptionMs: afterSubscribe - beforeSubscribe,
      });
    }).pipe(Effect.ensuring(closeTracked(tracked)));
  },
);

const runGrpcLeasedBenchmarkWorkloadInternal = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.workload.run",
)(function* (workload: GrpcLeasedBenchmarkWorkload, context: GrpcLeasedBenchmarkContext) {
  switch (workload) {
    case "first-subscriber":
      return yield* runSubscriptionCase(context, {
        additionalSubscriberCount: 1,
        name: "gRPC leased first subscriber",
        preOpenSubscriber: false,
        region: "first",
      });
    case "same-route-reuse":
      return yield* runSubscriptionCase(context, {
        additionalSubscriberCount: 9,
        name: "gRPC leased same-route reuse",
        preOpenSubscriber: true,
        region: "reuse",
      });
    case "one-route-many-subscribers":
      return yield* runSubscriptionCase(context, {
        additionalSubscriberCount: 50,
        name: "gRPC leased one route many subscribers",
        preOpenSubscriber: false,
        region: "many-subscribers",
      });
    case "local-filter-live-snapshot":
      return yield* runLocalFilterCase(context);
    case "retained-local-filter-snapshot":
      return yield* runRetainedLocalFilterCase(context);
    case "delta-fanout":
      return yield* runDeltaFanoutCase(context);
    case "partitioned-write-convergence":
      return yield* runPartitionedWriteCase(context);
    case "health-refresh-overhead":
      return yield* runHealthRefreshCase(context);
    case "last-subscriber-cleanup":
      return yield* runCleanupLatencyCase(context);
    case "many-routes":
      return yield* runManyRoutesCase(context);
  }
});

const failGrpcLeasedBenchmarkWorkloadCause = (
  cause: Cause.Cause<unknown>,
): Effect.Effect<never, GrpcLeasedBenchmarkWorkloadError> =>
  Cause.hasInterruptsOnly(cause)
    ? Effect.interrupt
    : new GrpcLeasedBenchmarkWorkloadError({ message: Cause.pretty(cause) });

export const runGrpcLeasedBenchmarkWorkload = (
  workload: GrpcLeasedBenchmarkWorkload,
  context: GrpcLeasedBenchmarkContext,
): Effect.Effect<GrpcLeasedBenchmarkMeasurement, GrpcLeasedBenchmarkWorkloadError, Scope.Scope> =>
  runGrpcLeasedBenchmarkWorkloadInternal(workload, context).pipe(
    Effect.catchCause(failGrpcLeasedBenchmarkWorkloadCause),
  );
