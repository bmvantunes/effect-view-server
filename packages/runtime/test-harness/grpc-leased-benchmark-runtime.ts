import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import type { ViewServerHealth } from "@effect-view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import type { ViewServerRuntimeCoreInternalInstance } from "@effect-view-server/runtime-core/internal";
import { Effect, Fiber, Queue, Schema, Stream } from "effect";

import type { ViewServerGrpcHealthLedger } from "../src/grpc-health";
import type { ViewServerGrpcLeaseManager } from "../src/grpc-lease-manager";

import { GrpcOrder, grpcOrderValue, type GrpcOrderValueMessage } from "./grpc-config";
import { grpcLeasedViewServer } from "./grpc-leased";
import { makeLeasedGrpcRuntimeHarness, readGrpcHealthOverlayNow } from "./grpc-runtime";
import type {
  GrpcLeasedBenchmarkOptions,
  GrpcLeasedMeasuredCleanupEvidence,
} from "./grpc-leased-benchmark-model";

type Topics = ReturnType<typeof grpcLeasedViewServer>["topics"];
type ProjectedOrderRow = Pick<typeof GrpcOrder.Type, "id" | "price" | "region" | "status">;

type LeasedRuntimeHarness = {
  readonly health: ViewServerGrpcHealthLedger<Topics>;
  readonly manager: ViewServerGrpcLeaseManager<Topics>;
  readonly runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>;
};

type LifecycleCounts = {
  acquiredFeedCount: number;
  releasedFeedCount: number;
};

export type GrpcLeasedBenchmarkContext = {
  readonly harness: LeasedRuntimeHarness;
  readonly lifecycle: LifecycleCounts;
  readonly options: GrpcLeasedBenchmarkOptions;
  readonly queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>;
};

export type GrpcLeasedBenchmarkCleanup = {
  readonly acquiredFeedCount: number;
  readonly backpressureCount: number;
  readonly cleanupActiveLeasedFeeds: number;
  readonly cleanupClientActiveFeeds: number;
  readonly cleanupLeakCount: number;
  readonly cleanupRowCount: number;
  readonly health: ViewServerHealth<Topics>;
  readonly queuedEventCount: number;
  readonly releasedFeedCount: number;
};

export type WatchedGrpcLeasedSubscription = {
  readonly fiber: Fiber.Fiber<void, never>;
  readonly subscription: ViewServerLiveSubscription<ProjectedOrderRow>;
  readonly totalRowsQueue: Queue.Queue<number>;
};

export type GrpcLeasedHealthObservation = {
  readonly activeLeasedFeeds: number;
  readonly healthOverlayMs: number;
  readonly measurementRowCount: number;
  readonly subscriberCount: number;
};

class GrpcLeasedBenchmarkConvergenceError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkConvergenceError>()(
  "GrpcLeasedBenchmarkConvergenceError",
  { message: Schema.String },
) {}

class GrpcLeasedBenchmarkHealthError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkHealthError>()(
  "GrpcLeasedBenchmarkHealthError",
  { message: Schema.String },
) {}

class GrpcLeasedBenchmarkCleanupError extends Schema.TaggedErrorClass<GrpcLeasedBenchmarkCleanupError>()(
  "GrpcLeasedBenchmarkCleanupError",
  { message: Schema.String },
) {}

const ignoreGrpcLeasedBenchmarkSubscriptionCloseFailure =
  ignoreLoggedTypedFailuresPreserveNonTypedFailures(
    "gRPC leased benchmark subscription close failed.",
  );

const configuredRegions = (options: GrpcLeasedBenchmarkOptions): ReadonlyArray<string> => [
  "first",
  "reuse",
  "many-subscribers",
  "live-filter",
  "retained-filter",
  "delta-fanout",
  "cleanup-latency",
  ...Array.from({ length: options.routeCount }, (_value, index) => `route-${index}`),
];

const makeQueues = Effect.fn("ViewServerRuntime.grpc.leased.bench.queues.make")(function* (
  options: GrpcLeasedBenchmarkOptions,
) {
  const entries = yield* Effect.forEach(configuredRegions(options), (region) =>
    Queue.unbounded<GrpcOrderValueMessage>().pipe(Effect.map((queue) => [region, queue] as const)),
  );
  return new Map(entries);
});

const closeQueues = Effect.fn("ViewServerRuntime.grpc.leased.bench.queues.close")(function* (
  queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>,
) {
  yield* Effect.forEach(queues.values(), Queue.shutdown, { discard: true });
});

const missingQueue = (region: string): never => {
  throw new Error(`gRPC leased benchmark route ${region} was not configured.`);
};

const queueForGrpcLeasedRegion = (
  queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>,
  region: string,
): Queue.Queue<GrpcOrderValueMessage> => queues.get(region) ?? missingQueue(region);

export const queueForGrpcLeasedRoute = (
  context: GrpcLeasedBenchmarkContext,
  region: string,
): Queue.Queue<GrpcOrderValueMessage> => queueForGrpcLeasedRegion(context.queues, region);

const benchmarkConfig = (
  queues: ReadonlyMap<string, Queue.Queue<GrpcOrderValueMessage>>,
  lifecycle: LifecycleCounts,
) =>
  grpcLeasedViewServer({
    acquired: () => {
      lifecycle.acquiredFeedCount += 1;
    },
    release: Effect.sync(() => {
      lifecycle.releasedFeedCount += 1;
    }),
    streamForRegion: (region) => Stream.fromQueue(queueForGrpcLeasedRegion(queues, region)),
  });

export const acquireGrpcLeasedBenchmarkContext = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.context.acquire",
)(function* (options: GrpcLeasedBenchmarkOptions) {
  const queues = yield* Effect.acquireRelease(makeQueues(options), closeQueues);
  const lifecycle: LifecycleCounts = {
    acquiredFeedCount: 0,
    releasedFeedCount: 0,
  };
  const harness = yield* makeLeasedGrpcRuntimeHarness({
    config: benchmarkConfig(queues, lifecycle),
  });
  return {
    harness,
    lifecycle,
    options,
    queues,
  } satisfies GrpcLeasedBenchmarkContext;
});

const topicHealth = (health: ViewServerHealth<Topics>) => health.engine.topics.orders;

export const activeGrpcLeasedFeedCount = (health: ViewServerHealth<Topics>): number =>
  Object.values(health.grpc?.feeds.orders?.leased ?? {}).length;

export const grpcLeasedClientActiveFeedCount = (health: ViewServerHealth<Topics>): number =>
  health.grpc?.clients["orders"]?.activeFeeds ?? 0;

export const grpcLeasedSubscriberCount = (health: ViewServerHealth<Topics>): number =>
  Object.values(health.grpc?.feeds.orders?.leased ?? {}).reduce(
    (count, feed) => count + feed.subscriberCount,
    0,
  );

export const grpcLeasedCleanupLeakCount = (health: ViewServerHealth<Topics>): number => {
  const orders = topicHealth(health);
  return (
    activeGrpcLeasedFeedCount(health) +
    grpcLeasedClientActiveFeedCount(health) +
    orders.activeSubscriptions +
    orders.activeViews +
    orders.queuedEvents +
    orders.rowCount
  );
};

const ensureHealthyGrpcLeasedBenchmarkState = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.health.ensureHealthy",
)(function* (health: ViewServerHealth<Topics>) {
  if (health.status !== "ready") {
    return yield* new GrpcLeasedBenchmarkHealthError({
      message: `gRPC leased benchmark health must be ready but was ${health.status}.`,
    });
  }
  return health;
});

export const readHealthyGrpcLeasedBenchmarkState = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.health.read",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const health = yield* readGrpcHealthOverlayNow(
    context.harness.runtimeCore.client,
    context.harness.health,
  );
  return yield* ensureHealthyGrpcLeasedBenchmarkState(health);
});

export const observeGrpcLeasedBenchmarkHealth = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.health.observe",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const before = performance.now();
  const health = yield* readHealthyGrpcLeasedBenchmarkState(context);
  const after = performance.now();
  return {
    activeLeasedFeeds: activeGrpcLeasedFeedCount(health),
    healthOverlayMs: after - before,
    measurementRowCount: topicHealth(health).rowCount,
    subscriberCount: grpcLeasedSubscriberCount(health),
  } satisfies GrpcLeasedHealthObservation;
});

export const auditGrpcLeasedMeasuredCleanup = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.cleanup.auditMeasured",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const health = yield* readHealthyGrpcLeasedBenchmarkState(context);
  const orders = topicHealth(health);
  const evidence = {
    activeLeasedFeeds: activeGrpcLeasedFeedCount(health),
    activeSubscriptions: orders.activeSubscriptions,
    activeViews: orders.activeViews,
    clientActiveFeeds: grpcLeasedClientActiveFeedCount(health),
    leakCount: grpcLeasedCleanupLeakCount(health),
    queuedEvents: orders.queuedEvents,
    rowCount: orders.rowCount,
  } satisfies GrpcLeasedMeasuredCleanupEvidence;
  if (evidence.leakCount !== 0) {
    return yield* new GrpcLeasedBenchmarkCleanupError({
      message: `gRPC leased benchmark measured subscription cleanup leaked state: ${JSON.stringify(evidence)}.`,
    });
  }
  return evidence;
});

export const cleanupGrpcLeasedBenchmarkContext = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.context.close",
)(function* (context: GrpcLeasedBenchmarkContext) {
  const health = yield* context.harness.manager.close.pipe(
    Effect.andThen(
      readGrpcHealthOverlayNow(context.harness.runtimeCore.client, context.harness.health),
    ),
    Effect.ensuring(context.harness.runtimeCore.close),
  );
  const orders = topicHealth(health);
  return {
    acquiredFeedCount: context.lifecycle.acquiredFeedCount,
    backpressureCount: orders.backpressureEvents,
    cleanupActiveLeasedFeeds: activeGrpcLeasedFeedCount(health),
    cleanupClientActiveFeeds: grpcLeasedClientActiveFeedCount(health),
    cleanupLeakCount: grpcLeasedCleanupLeakCount(health),
    cleanupRowCount: orders.rowCount,
    health,
    queuedEventCount: orders.queuedEvents,
    releasedFeedCount: context.lifecycle.releasedFeedCount,
  } satisfies GrpcLeasedBenchmarkCleanup;
});

const orderStatus = (index: number): GrpcOrderValueMessage["status"] => {
  if (index % 5 === 0) {
    return "cancelled";
  }
  if (index % 3 === 0) {
    return "closed";
  }
  return "open";
};

export const grpcLeasedRows = (
  region: string,
  start: number,
  count: number,
): ReadonlyArray<GrpcOrderValueMessage> =>
  Array.from({ length: count }, (_value, offset) => {
    const index = start + offset;
    return grpcOrderValue(`${region}-order-${index}`, index, orderStatus(index));
  });

export const grpcLeasedOpenRows = (
  region: string,
  count: number,
): ReadonlyArray<GrpcOrderValueMessage> =>
  Array.from({ length: count }, (_value, offset) =>
    grpcOrderValue(`${region}-order-${offset}`, 10 + offset, "open"),
  );

export const offerGrpcLeasedRows = Effect.fn("ViewServerRuntime.grpc.leased.bench.rows.offer")(
  function* (
    queue: Queue.Queue<GrpcOrderValueMessage>,
    rows: ReadonlyArray<GrpcOrderValueMessage>,
  ) {
    yield* Effect.forEach(rows, (row) => Queue.offer(queue, row), { discard: true });
  },
);

export const watchGrpcLeasedSubscription = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.subscription.watch",
)(function* (subscription: ViewServerLiveSubscription<ProjectedOrderRow>) {
  const totalRowsQueue = yield* Queue.unbounded<number>();
  const fiber = yield* subscription.events.pipe(
    Stream.runForEach((event) =>
      event.type === "snapshot" || event.type === "delta"
        ? Queue.offer(totalRowsQueue, event.totalRows).pipe(Effect.asVoid)
        : Effect.void,
    ),
    Effect.forkScoped,
  );
  return {
    fiber,
    subscription,
    totalRowsQueue,
  } satisfies WatchedGrpcLeasedSubscription;
});

export const waitForGrpcLeasedRows = Effect.fn("ViewServerRuntime.grpc.leased.bench.rows.wait")(
  function* (
    watched: WatchedGrpcLeasedSubscription,
    expectedRows: number,
    convergenceTimeout: GrpcLeasedBenchmarkOptions["convergenceTimeout"],
  ) {
    return yield* Queue.take(watched.totalRowsQueue).pipe(
      Effect.repeat({ until: (totalRows) => totalRows === expectedRows }),
      Effect.timeoutOrElse({
        duration: convergenceTimeout,
        orElse: () =>
          new GrpcLeasedBenchmarkConvergenceError({
            message: `gRPC leased benchmark subscription did not converge to ${expectedRows} rows.`,
          }),
      }),
    );
  },
);

export const closeGrpcLeasedSubscriptions = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.subscriptions.close",
)(function* (subscriptions: ReadonlyArray<WatchedGrpcLeasedSubscription>) {
  yield* Effect.forEach(
    subscriptions,
    (watched) =>
      watched.subscription
        .close()
        .pipe(
          ignoreGrpcLeasedBenchmarkSubscriptionCloseFailure,
          Effect.andThen(Fiber.interrupt(watched.fiber)),
          Effect.asVoid,
        ),
    { discard: true },
  );
});
