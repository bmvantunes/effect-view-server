import type {
  ExactLiveQueryInputForTopic,
  GrpcRuntimeClients,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  RowSchema,
  TopicRow,
  ViewServerTopicConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import type {
  ViewServerRuntimeLiveClient,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import {
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  Result,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeGrpcLeasedIdentityContract,
  type GrpcLeasedGroupedKeyRetentionObserver,
  type GrpcLeasedIdentityContract,
} from "./grpc-leased-identity";
import {
  makeGrpcLeasedSubscription,
  type GrpcLeasedSubscription,
  type GrpcLeasedSubscriptionLease,
  type GrpcLeasedUpstreamTerminal,
} from "./grpc-leased-subscription";
import {
  callLeasedGrpcSourceAcquire,
  callLeasedGrpcSourceRelease,
  callLeasedGrpcSourceRequest,
  makeGrpcSourceInput,
  makeDefaultGrpcClient,
  makeViewServerGrpcSourceError,
  ViewServerGrpcIngressError,
  type ViewServerGrpcClientFactory,
  type ViewServerGrpcRuntimeCallable,
  type ViewServerGrpcSourceInput,
} from "./grpc-source-lifecycle";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./grpc-runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";
import { snapshotLeasedGrpcQuery } from "./grpc-leased-query-snapshot";
import {
  makeSourceOwnershipPolicy,
  type ViewServerRuntimeCoreInternalClient,
  type ViewServerRuntimeCoreInternalLiveClient,
} from "@effect-view-server/runtime-core/internal";

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

type RuntimeLeasedFeedDefinition = {
  readonly lifecycle: "leased";
  readonly topic: string;
  readonly client: string;
  readonly routeBy: ReadonlyArray<string>;
  readonly request: ViewServerGrpcRuntimeCallable;
  readonly acquire: ViewServerGrpcRuntimeCallable;
  readonly release?: ViewServerGrpcRuntimeCallable;
  readonly map: ViewServerGrpcRuntimeCallable;
};

const isRuntimeLeasedFeed = (feed: {
  readonly lifecycle: string;
}): feed is RuntimeLeasedFeedDefinition => feed.lifecycle === "leased";

type RuntimeTopicDefinition = {
  readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
  readonly key: string;
};

type LeasedFeedRoute = Readonly<Record<string, unknown>>;

type LeasedFeedRuntimeInput = ViewServerGrpcSourceInput<LeasedFeedRoute>;

type ActiveLease = {
  readonly feedName: string;
  readonly feed: RuntimeLeasedFeedDefinition;
  readonly subscription: GrpcLeasedSubscription<ViewServerGrpcIngressError>;
};

/**
 * @internal One-shot, read-only construction observer for the intentionally private grouped-key
 * retention invariant. The production path allocates no observer closure or per-event metric when
 * it is absent, and no retained keys, maps, or mutation operations escape this package-local seam.
 */
type ViewServerGrpcGroupedKeyRetentionObserver = GrpcLeasedGroupedKeyRetentionObserver;

type AcquiredLease = {
  readonly subscriptionLease: GrpcLeasedSubscriptionLease;
};

export type ViewServerGrpcLeaseManager<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const isRuntimeMutationEffect = (value: unknown): value is Effect.Effect<unknown, unknown, never> =>
  Effect.isEffect(value);

const ignoreGrpcFeedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC feed release failure.",
);
const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC health refresh failure.",
);
const ignoreLeasedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC release failure.",
);

const runtimeError = (input: {
  readonly code: Extract<
    ViewServerRuntimeError,
    { readonly _tag: "ViewServerRuntimeError" }
  >["code"];
  readonly topic: string;
  readonly message: string;
}): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: input.code,
  topic: input.topic,
  message: input.message,
});

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

const callFeedRequest = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
) => callLeasedGrpcSourceRequest(feedName, feed, route);

const callFeedAcquire = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceAcquire(feedName, feed, input);

const callFeedRelease = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceRelease(feedName, feed, input);

const topicDefinitionFor = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
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

const isRuntimeTopic = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

const runtimeTopicFor = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
  feedName: string,
): Effect.Effect<Extract<keyof Topics, string>, ViewServerGrpcIngressError> =>
  Effect.suspend(() => {
    if (isRuntimeTopic(config, topic)) {
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

const mapLeasedValue = Effect.fn("ViewServerRuntime.grpc.leased.map")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
  value: unknown,
) {
  const topicDefinition = yield* topicDefinitionFor(config, feed.topic, feedName);
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
  const decoded = yield* validateDecodedRow(topicDefinition.schema, row).pipe(
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
  return decoded;
});

type LeasedRowWithStorageKey = {
  readonly storageKey: string;
  readonly row: object;
};

const internalizeLeasedRow = Effect.fn("ViewServerRuntime.grpc.leased.row.internalize")(function* <
  Row extends object,
>(lease: ActiveLease, row: Row) {
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
});

const validateLeasedRowRoute = Effect.fn("ViewServerRuntime.grpc.leased.row.validateRoute")(
  function* <Row extends object>(lease: ActiveLease, row: Row) {
    return yield* Effect.fromResult(lease.subscription.validateRowRoute(row)).pipe(
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
  },
);

const resetLeaseRowCount = Effect.fn("ViewServerRuntime.grpc.leased.health.rowCount.reset")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
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
  },
);

const callRuntimePublishMany = Effect.fn(
  "ViewServerRuntime.grpc.leased.runtime.publishManyDecoded",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  topic: Topic,
  rows: ReadonlyArray<LeasedRowWithStorageKey>,
  feedName: string,
) {
  const effect = runtimeClient.publishManyDecodedRowsWithStorageKeys(topic, rows);
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
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  topic: Topic,
  key: string,
  feedName: string,
) {
  const effect = runtimeClient.delete(topic, key);
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

const publishLeasedBatch = Effect.fn("ViewServerRuntime.grpc.leased.publishBatch")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
  values: ReadonlyArray<unknown>,
) {
  const rows = yield* Effect.forEach(values, (value) =>
    Effect.gen(function* () {
      const route = lease.subscription.materializeRoute();
      const row = yield* mapLeasedValue(config, lease.feedName, lease.feed, route, value);
      return yield* validateLeasedRowRoute(lease, row);
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
  const topic = yield* runtimeTopicFor(config, lease.feed.topic, lease.feedName);
  yield* callRuntimePublishMany(runtimeClient, topic, internalRows, lease.feedName).pipe(
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

const acquireLeaseStream = Effect.fn("ViewServerRuntime.grpc.leased.stream.acquire")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
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
    Stream.runForEach((values) =>
      publishLeasedBatch(config, runtimeClient, requestHealthRefresh, health, lease, values),
    ),
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

const closeLeaseRows = Effect.fn("ViewServerRuntime.grpc.leased.rows.close")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  storageKeys: ReadonlySet<string>,
) {
  const topic = yield* runtimeTopicFor(config, feed.topic, feedName);
  yield* Effect.forEach(
    storageKeys,
    (key) => callRuntimeDelete(runtimeClient, topic, key, feedName),
    {
      discard: true,
    },
  );
});

const leasedFeedsByTopic = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
): Map<string, readonly [string, RuntimeLeasedFeedDefinition]> => {
  const feeds = new Map<string, readonly [string, RuntimeLeasedFeedDefinition]>();
  for (const [feedName, feed] of Object.entries(options.feeds)) {
    if (isRuntimeLeasedFeed(feed)) {
      feeds.set(feed.topic, [feedName, feed]);
    }
  }
  return feeds;
};

const normalizeAcquireLeaseError =
  (topic: string) =>
  (error: ViewServerRuntimeError | ViewServerGrpcIngressError): ViewServerRuntimeError => {
    if (error instanceof ViewServerGrpcIngressError) {
      return runtimeError({
        code: "RuntimeUnavailable",
        topic,
        message: error.message,
      });
    }
    return error;
  };

const normalizeAcquireLeaseCause =
  (topic: string) =>
  (
    cause: Cause.Cause<ViewServerRuntimeError | ViewServerGrpcIngressError>,
  ): Cause.Cause<ViewServerRuntimeError> =>
    Cause.map(cause, normalizeAcquireLeaseError(topic));

export const makeViewServerGrpcLeaseManager = Effect.fn(
  "ViewServerRuntime.grpc.leased.makeManager",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
  groupedKeyRetentionObserver?: ViewServerGrpcGroupedKeyRetentionObserver,
) {
  const leases = new Map<string, ActiveLease>();
  const feedsByTopic = leasedFeedsByTopic(options);
  const identityContracts = new Map<string, GrpcLeasedIdentityContract>();
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  const lock = yield* Semaphore.make(1);
  const managerScope = yield* Scope.make("sequential");
  let closed = false;

  const captureLeasedQuery = <
    const Topic extends Extract<keyof Topics, string>,
    Query extends Readonly<Record<string, unknown>>,
  >(
    topic: Topic,
    query: Query,
  ) =>
    Result.try(() => {
      if (!feedsByTopic.has(topic)) {
        return query;
      }
      const topicDefinition = config.topics[topic];
      return topicDefinition === undefined
        ? query
        : snapshotLeasedGrpcQuery(topicDefinition.schema, query);
    });

  const leasedQuerySnapshotError = (topic: string, cause: unknown): ViewServerRuntimeError =>
    runtimeError({
      code: "InvalidQuery",
      topic,
      message: `Leased gRPC query could not be snapshotted before acquisition: ${String(cause)}`,
    });

  const identityContractFor = Effect.fn("ViewServerRuntime.grpc.leased.identity.contract")(
    function* (topic: string, feedName: string, feed: RuntimeLeasedFeedDefinition) {
      const existing = identityContracts.get(topic);
      if (existing !== undefined) {
        return existing;
      }
      const topicDefinition = yield* topicDefinitionFor(config, topic, feedName).pipe(
        Effect.mapError((error) =>
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message: error.message,
          }),
        ),
      );
      const contract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic,
          feedName,
          routeBy: feed.routeBy,
          schema: topicDefinition.schema,
          keyField: topicDefinition.key,
        }),
      ).pipe(
        Effect.mapError((error) =>
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message: error.message,
          }),
        ),
      );
      identityContracts.set(topic, contract);
      return contract;
    },
  );

  const acquireLease = Effect.fn("ViewServerRuntime.grpc.leased.acquireLease")(function* <
    const Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, query: unknown) {
    const configuredFeed = feedsByTopic.get(topic);
    if (configuredFeed === undefined) {
      if (sourceOwnership.isGrpcLeasedTopic(topic)) {
        return yield* Effect.fail(
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message: `Leased gRPC topic ${topic} has no configured leased feed.`,
          }),
        );
      }
      return Option.none<AcquiredLease>();
    }
    const [feedName, feed] = configuredFeed;
    const identityContract = yield* identityContractFor(topic, feedName, feed);
    const identity = yield* Effect.fromResult(identityContract.leaseFromQuery(query)).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: error.message,
        }),
      ),
    );
    const feedKey = identity.feedKey;
    if (closed) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: "gRPC leased feed manager is closed.",
        }),
      );
    }
    const existing = leases.get(feedKey);
    if (existing !== undefined) {
      const subscriptionLease = yield* existing.subscription.acquire;
      if (Option.isNone(subscriptionLease)) {
        return yield* Effect.fail(
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message:
              "gRPC leased upstream is not accepting new subscribers after completion or failure.",
          }),
        );
      }
      return Option.some({
        subscriptionLease: subscriptionLease.value,
      });
    }
    const clientDefinition = options.clients[feed.client];
    if (clientDefinition === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references missing client: ${feed.client}`,
        }),
      );
    }
    const baseUrl = options.clientBaseUrls[feed.client];
    if (baseUrl === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references unresolved client URL: ${feed.client}`,
        }),
      );
    }
    const grpcClient = yield* Effect.try({
      try: () => makeClient(clientDefinition, baseUrl),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased client creation failed for ${feedName}`,
          cause,
          phase: "client",
          feedName,
          topic,
        }),
    }).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const requestRoute = identity.materializeRoute();
    const request = yield* callFeedRequest(feedName, feed, requestRoute).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const subscription = yield* makeGrpcLeasedSubscription<ViewServerGrpcIngressError>({
      parentScope: managerScope,
      topic,
      identity,
      ...(groupedKeyRetentionObserver === undefined ? {} : { groupedKeyRetentionObserver }),
      cleanupRows: (storageKeys) =>
        closeLeaseRows(config, runtimeClient, feedName, feed, storageKeys),
      onCleanupFailure: (cause) =>
        runAllFinalizers([
          ignoreLeasedReleaseFailure(Effect.failCause(cause)),
          health.feedDegraded(feedKey, `gRPC leased feed row cleanup failed for ${feedName}`),
          health.clientDegraded(feed.client, `gRPC leased feed row cleanup failed for ${feedName}`),
          ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
        ]),
      onRowsCleared: resetLeaseRowCount(requestHealthRefresh, health, feedKey),
      onStopping: health
        .feedStopping(feedKey)
        .pipe(Effect.andThen(ignoreGrpcHealthRefreshFailure(requestHealthRefresh))),
      onSubscriberAdded: health.subscriberAdded(feedKey),
      onSubscriberRemoved: health.subscriberRemoved(feedKey),
      onUpstreamTerminal: (terminal) =>
        runAllFinalizers([
          health.feedDegraded(feedKey, terminal.healthMessage),
          health.clientDegraded(feed.client, terminal.healthMessage),
          ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
        ]),
      onClosed: health
        .leasedFeedRemoved(feedKey)
        .pipe(
          Effect.andThen(Effect.sync(() => leases.delete(feedKey))),
          Effect.andThen(ignoreGrpcHealthRefreshFailure(requestHealthRefresh)),
        ),
    });
    const lease: ActiveLease = {
      feedName,
      feed,
      subscription,
    };
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        leases.set(feedKey, lease);
        const startedAt = yield* Clock.currentTimeMillis;
        yield* health.clientConnected(feed.client, startedAt);
        yield* health.leasedFeedStarting({
          feedName,
          feedKey,
          topic,
          clientName: feed.client,
        });
        const subscriptionLease = Option.getOrThrow(yield* subscription.acquire);
        yield* health.feedReady(feedKey);
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        const releaseRoute = subscription.materializeRoute();
        const startExit = yield* restore(
          subscription.start({
            acquire: acquireLeaseStream(
              config,
              runtimeClient,
              requestHealthRefresh,
              health,
              lease,
              grpcClient,
              request,
            ),
            release: callFeedRelease(
              feedName,
              feed,
              makeGrpcSourceInput(grpcClient, request, releaseRoute),
            ).pipe(
              ignoreGrpcFeedReleaseFailure,
              Effect.withSpan("ViewServerRuntime.grpc.leased.resources.release"),
            ),
          }),
        ).pipe(Effect.exit);
        if (Exit.isFailure(startExit)) {
          const cleanupExit = yield* runAllFinalizers([
            subscription.close,
            health.clientDegraded(
              feed.client,
              `gRPC leased feed ${feedName} failed to start: ${String(startExit.cause)}`,
            ),
            ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
          ]).pipe(Effect.exit);
          const cause = Exit.isFailure(cleanupExit)
            ? Cause.combine(startExit.cause, cleanupExit.cause)
            : startExit.cause;
          return yield* Effect.failCause(normalizeAcquireLeaseCause(topic)(cause));
        }
        return Option.some({
          subscriptionLease,
        });
      }),
    );
  });

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  > {
    const capturedQuery = captureLeasedQuery(topic, query);
    return Effect.gen(function* () {
      if (Result.isFailure(capturedQuery)) {
        return yield* Effect.fail(leasedQuerySnapshotError(topic, capturedQuery.failure));
      }
      const ownedQuery = capturedQuery.success;
      const lease = yield* lock
        .withPermit(acquireLease(topic, ownedQuery))
        .pipe(
          Effect.catchCause((cause) => Effect.failCause(normalizeAcquireLeaseCause(topic)(cause))),
        );
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeInternal<Topic, Query>(topic, ownedQuery);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const subscription = yield* restore(
            internalLiveClient.subscribeObservedInternal<Topic, Query>(
              topic,
              ownedQuery,
              acquired.subscriptionLease.terminalObserver,
            ),
          );
          return yield* acquired.subscriptionLease.attach({
            subscription,
            query: ownedQuery,
          });
        }),
      ).pipe(Effect.onError(() => acquired.subscriptionLease.close));
    });
  }

  const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
    topic,
    query,
  ) => {
    const capturedQuery = captureLeasedQuery(topic, query);
    return Effect.gen(function* () {
      if (Result.isFailure(capturedQuery)) {
        return yield* Effect.fail(leasedQuerySnapshotError(topic, capturedQuery.failure));
      }
      const ownedQuery = capturedQuery.success;
      const lease = yield* lock
        .withPermit(acquireLease(topic, ownedQuery))
        .pipe(
          Effect.catchCause((cause) => Effect.failCause(normalizeAcquireLeaseCause(topic)(cause))),
        );
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeRuntimeInternal(topic, ownedQuery);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const subscription = yield* restore(
            internalLiveClient.subscribeRuntimeObservedInternal(
              topic,
              ownedQuery,
              acquired.subscriptionLease.terminalObserver,
            ),
          );
          return yield* acquired.subscriptionLease.attach({
            subscription,
            query: ownedQuery,
          });
        }),
      ).pipe(Effect.onError(() => acquired.subscriptionLease.close));
    });
  };

  const snapshot: ViewServerRuntimeClient<Topics>["snapshot"] = (topic, query) =>
    sourceOwnership
      .requirePublicReadAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.snapshot(topic, query)));

  const publish: ViewServerRuntimeClient<Topics>["publish"] = (topic, row) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.publish(topic, row)));
  const publishMany: ViewServerRuntimeClient<Topics>["publishMany"] = (topic, rows) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.publishMany(topic, rows)));
  const patch: ViewServerRuntimeClient<Topics>["patch"] = (topic, key, patchValue) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.patch(topic, key, patchValue)));
  const deleteRow: ViewServerRuntimeClient<Topics>["delete"] = (topic, key) =>
    sourceOwnership
      .requirePublicMutationAllowed(topic, "managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.delete(topic, key)));

  const reset: ViewServerRuntimeClient<Topics>["reset"] = () =>
    sourceOwnership
      .requirePublicResetAllowed("managedRuntime")
      .pipe(Effect.flatMap(() => runtimeClient.reset()));

  const client: ViewServerRuntimeClient<Topics> = {
    publish,
    publishMany,
    patch,
    delete: deleteRow,
    snapshot,
    health: runtimeClient.health,
    reset,
  };

  const close = (yield* Effect.cached(
    Effect.gen(function* () {
      yield* lock.withPermit(
        Effect.sync(() => {
          closed = true;
        }),
      );
      yield* runAllFinalizers([
        Scope.close(managerScope, Exit.void),
        Effect.sync(() => leases.clear()),
        ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
      ]);
    }).pipe(Effect.withSpan("ViewServerRuntime.grpc.leased.close")),
  )).pipe(Effect.uninterruptible);

  return {
    client,
    liveClient: {
      close: liveClient.close.pipe(Effect.ensuring(close)),
      health: liveClient.health,
      subscribe,
      subscribeRuntime,
      subscribeHealth: liveClient.subscribeHealth,
      subscribeHealthSummary: liveClient.subscribeHealthSummary,
    },
    close,
  };
});
