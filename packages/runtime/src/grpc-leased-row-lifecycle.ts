import type { RowSchema, ViewServerTopicConfig } from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import type {
  ViewServerRuntimeCoreInternalClient,
  ViewServerRuntimeCoreQueryPartition,
} from "@effect-view-server/runtime-core/internal";
import { Cause, Clock, Effect, Exit, Schema, Stream } from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import type {
  GrpcLeasedSubscription,
  GrpcLeasedUpstreamTerminal,
} from "./grpc-leased-subscription";
import {
  callLeasedGrpcSourceAcquire,
  makeGrpcSourceInput,
  makeViewServerGrpcSourceError,
  type ViewServerGrpcRuntimeCallable,
  type ViewServerGrpcSourceInput,
  ViewServerGrpcIngressError,
} from "./grpc-source-lifecycle";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type RuntimeLeasedFeedDefinition = {
  readonly lifecycle: "leased";
  readonly topic: string;
  readonly client: string;
  readonly routeBy: ReadonlyArray<string>;
  readonly request: ViewServerGrpcRuntimeCallable;
  readonly acquire: ViewServerGrpcRuntimeCallable;
  readonly release?: ViewServerGrpcRuntimeCallable;
  readonly map: ViewServerGrpcRuntimeCallable;
};

export type GrpcLeasedActiveLease = {
  readonly feedName: string;
  readonly feed: RuntimeLeasedFeedDefinition;
  readonly enginePartition: ViewServerRuntimeCoreQueryPartition;
  readonly subscription: GrpcLeasedSubscription<ViewServerGrpcIngressError>;
};

type RuntimeTopicDefinition = {
  readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
  readonly key: string;
};

type LeasedFeedRoute = Readonly<Record<string, unknown>>;

type LeasedFeedRuntimeInput = ViewServerGrpcSourceInput<LeasedFeedRoute>;

type LeasedRowWithStorageKey = {
  readonly storageKey: string;
  readonly row: object;
};

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const isRuntimeMutationEffect = (value: unknown): value is Effect.Effect<unknown, unknown, never> =>
  Effect.isEffect(value);

const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC health refresh failure.",
);

const grpcLeaseError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly phase: NonNullable<ViewServerGrpcIngressError["phase"]>;
  readonly feedName: string;
  readonly topic: string;
}) =>
  makeViewServerGrpcSourceError({
    message: input.message,
    cause: input.cause,
    phase: input.phase,
    feedName: input.feedName,
    topic: input.topic,
  });

const callFeedAcquire = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceAcquire(feedName, feed, input);

/**
 * Owns the complete Topic Row lifecycle for one leased gRPC source: mapping and admission,
 * internal Row Key ownership, batched publication, health accounting, and release cleanup.
 * Lease/subscriber orchestration stays in the manager; row mutation details stay local here.
 */
export const makeGrpcLeasedRowLifecycle = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: Effect.Effect<void>,
  health: ViewServerGrpcHealthLedger<Topics>,
) => {
  const topicDefinitionFor = (
    topic: string,
    feedName: string,
  ): Effect.Effect<RuntimeTopicDefinition, ViewServerGrpcIngressError> =>
    Effect.suspend(() => {
      const topicDefinition = config.topics[topic];
      if (topicDefinition !== undefined) {
        return Effect.succeed(topicDefinition);
      }
      return grpcLeaseError({
        message: `gRPC leased feed ${feedName} references unknown topic ${topic}`,
        cause: topic,
        phase: "configuration",
        feedName,
        topic,
      });
    });

  const isRuntimeTopic = (topic: string): topic is Extract<keyof Topics, string> =>
    Object.hasOwn(config.topics, topic);

  const runtimeTopicFor = (
    topic: string,
    feedName: string,
  ): Effect.Effect<Extract<keyof Topics, string>, ViewServerGrpcIngressError> =>
    Effect.suspend(() => {
      if (isRuntimeTopic(topic)) {
        return Effect.succeed(topic);
      }
      return grpcLeaseError({
        message: `gRPC leased feed ${feedName} references unknown topic ${topic}`,
        cause: topic,
        phase: "configuration",
        feedName,
        topic,
      });
    });

  const mapLeasedValue = Effect.fn("ViewServerRuntime.grpc.leased.map")(function* (
    lease: GrpcLeasedActiveLease,
    route: LeasedFeedRoute,
    value: unknown,
  ) {
    const { feed, feedName } = lease;
    const topicDefinition = yield* topicDefinitionFor(feed.topic, feedName);
    const row = yield* Effect.try({
      try: () =>
        Reflect.apply(feed.map, undefined, [
          {
            value,
            route,
            schema: topicDefinition.schema,
          },
        ]),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased feed mapping failed for ${feedName}`,
          cause,
          phase: "mapping",
          feedName,
          topic: feed.topic,
        }),
    });
    if (typeof row !== "object" || row === null) {
      return yield* grpcLeaseError({
        message: `gRPC leased feed mapping produced an invalid row for ${feedName}`,
        cause: row,
        phase: "mapping",
        feedName,
        topic: feed.topic,
      });
    }
    yield* Effect.fromResult(lease.subscription.validateRowRoute(row)).pipe(
      Effect.mapError((error) =>
        grpcLeaseError({
          message: error.message,
          cause: error.cause,
          phase: "mapping",
          feedName,
          topic: feed.topic,
        }),
      ),
    );
    return yield* validateDecodedRow(topicDefinition.schema, row).pipe(
      Effect.mapError((cause) =>
        grpcLeaseError({
          message: `gRPC leased feed mapping produced an invalid row for ${feedName}`,
          cause,
          phase: "mapping",
          feedName,
          topic: feed.topic,
        }),
      ),
    );
  });

  const internalizeLeasedRow = Effect.fn("ViewServerRuntime.grpc.leased.row.internalize")(
    function* <Row extends object>(lease: GrpcLeasedActiveLease, row: Row) {
      const internalKey = yield* Effect.fromResult(lease.subscription.internalizeRowKey(row)).pipe(
        Effect.mapError((error) =>
          grpcLeaseError({
            message: error.message,
            cause: error.cause,
            phase: "mapping",
            feedName: lease.feedName,
            topic: lease.feed.topic,
          }),
        ),
      );
      return {
        storageKey: internalKey.storageKey,
        row,
      } satisfies LeasedRowWithStorageKey;
    },
  );

  const callRuntimePublishMany = Effect.fn(
    "ViewServerRuntime.grpc.leased.runtime.publishManyDecoded",
  )(function* <const Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<LeasedRowWithStorageKey>,
    feedName: string,
    partitionKey: string,
  ) {
    const effect = runtimeClient.publishManyDecodedRowsWithStorageKeys(topic, rows, partitionKey);
    if (!isRuntimeMutationEffect(effect)) {
      return yield* grpcLeaseError({
        message: `Runtime publishManyDecodedRowsWithStorageKeys did not return an Effect for leased gRPC feed ${feedName}`,
        cause: effect,
        phase: "publish",
        feedName,
        topic,
      });
    }
    yield* effect.pipe(
      Effect.asVoid,
      Effect.mapError((cause) =>
        grpcLeaseError({
          message: `gRPC leased feed publish failed for ${feedName}`,
          cause,
          phase: "publish",
          feedName,
          topic,
        }),
      ),
    );
  });

  const callRuntimeDelete = Effect.fn("ViewServerRuntime.grpc.leased.runtime.delete")(function* <
    const Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, key: string, feedName: string, partitionKey: string) {
    const effect = runtimeClient.deleteStorageKey(topic, key, partitionKey);
    if (!isRuntimeMutationEffect(effect)) {
      return yield* grpcLeaseError({
        message: `Runtime delete did not return an Effect for leased gRPC feed ${feedName}`,
        cause: effect,
        phase: "release",
        feedName,
        topic,
      });
    }
    yield* effect.pipe(
      Effect.asVoid,
      Effect.mapError((cause) =>
        grpcLeaseError({
          message: `gRPC leased feed row cleanup failed for ${feedName}`,
          cause,
          phase: "release",
          feedName,
          topic,
        }),
      ),
    );
  });

  const publishBatch = Effect.fn("ViewServerRuntime.grpc.leased.publishBatch")(function* (
    lease: GrpcLeasedActiveLease,
    values: ReadonlyArray<unknown>,
  ) {
    const rows = yield* Effect.forEach(values, (value) =>
      Effect.gen(function* () {
        const route = lease.subscription.materializeRoute();
        return yield* mapLeasedValue(lease, route, value);
      }).pipe(
        Effect.tapError((error) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((nowMillis) =>
              health.mappingFailed(lease.subscription.feedKey, {
                message: error.message,
                nowMillis,
              }),
            ),
          ),
        ),
      ),
    );
    const internalRows = yield* Effect.forEach(rows, (row) => internalizeLeasedRow(lease, row));
    const topic = yield* runtimeTopicFor(lease.feed.topic, lease.feedName);
    yield* callRuntimePublishMany(
      topic,
      internalRows,
      lease.feedName,
      lease.enginePartition.key,
    ).pipe(
      Effect.tapError((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((nowMillis) =>
            health.publishFailed(lease.subscription.feedKey, {
              message: error.message,
              nowMillis,
            }),
          ),
        ),
      ),
    );
    const nowMillis = yield* Clock.currentTimeMillis;
    yield* health.rowsPublished(lease.subscription.feedKey, {
      messages: values.length,
      rows: rows.length,
      rowCount: lease.subscription.retainedRowCount(),
      nowMillis,
    });
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  });

  const internalFeedFailureMessage = (feedName: string, cause: Cause.Cause<unknown>): string =>
    `gRPC leased feed ${feedName} failed: ${Cause.pretty(cause)}`;

  const acquireStream = Effect.fn("ViewServerRuntime.grpc.leased.stream.acquire")(function* (
    lease: GrpcLeasedActiveLease,
    grpcClient: unknown,
    request: unknown,
  ) {
    const acquireRoute = lease.subscription.materializeRoute();
    const stream = yield* callFeedAcquire(
      lease.feedName,
      lease.feed,
      makeGrpcSourceInput(grpcClient, request, acquireRoute),
    );
    const runFeed = stream.pipe(
      Stream.mapError((cause) =>
        grpcLeaseError({
          message: `gRPC leased feed stream failed for ${lease.feedName}`,
          cause,
          phase: "stream",
          feedName: lease.feedName,
          topic: lease.feed.topic,
        }),
      ),
      Stream.groupedWithin(grpcMessageBatchSize, grpcMessageBatchFlushInterval),
      Stream.runForEach((values) => publishBatch(lease, values)),
      Effect.exit,
      Effect.map((exit): GrpcLeasedUpstreamTerminal => {
        if (Exit.isSuccess(exit)) {
          return {
            message: "gRPC leased upstream completed unexpectedly.",
            healthMessage: `gRPC leased feed ${lease.feedName} completed unexpectedly.`,
          };
        }
        if (Cause.hasInterruptsOnly(exit.cause)) {
          return {
            message: "gRPC leased upstream interrupted unexpectedly.",
            healthMessage: `gRPC leased feed ${lease.feedName} interrupted unexpectedly.`,
          };
        }
        return {
          message: "gRPC leased upstream failed.",
          healthMessage: internalFeedFailureMessage(lease.feedName, exit.cause),
        };
      }),
    );
    // The Subscription must receive runFeed as a value so it can fork and own the worker.
    // Strict Effect diagnostics treat direct nested-Effect returns as accidental, so wrap it
    // to make the intentional Effect<Effect<GrpcLeasedUpstreamTerminal>> explicit.
    return yield* Effect.succeed(runFeed);
  });

  const cleanupRows = Effect.fn("ViewServerRuntime.grpc.leased.rows.close")(function* (
    feedName: string,
    feed: RuntimeLeasedFeedDefinition,
    storageKeys: ReadonlySet<string>,
    partitionKey: string,
  ) {
    const topic = yield* runtimeTopicFor(feed.topic, feedName);
    yield* Effect.forEach(
      storageKeys,
      (key) => callRuntimeDelete(topic, key, feedName, partitionKey),
      { discard: true },
    );
  });

  const resetRowCount = Effect.fn("ViewServerRuntime.grpc.leased.health.rowCount.reset")(function* (
    feedKey: string,
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    yield* health.rowsPublished(feedKey, {
      messages: 0,
      rows: 0,
      rowCount: 0,
      nowMillis,
    });
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  });

  return {
    acquireStream,
    cleanupRows,
    resetRowCount,
    topicDefinitionFor,
  };
};
