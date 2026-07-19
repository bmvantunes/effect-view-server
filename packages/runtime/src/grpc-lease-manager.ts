import type {
  ExactLiveQueryInputForTopic,
  GrpcRuntimeClients,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerTopicConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import type {
  ViewServerRuntimeLiveClient,
  ViewServerLiveSubscription,
} from "@effect-view-server/client";
import { Cause, Clock, Effect, Exit, Option, Result, Scope, Semaphore } from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeGrpcLeasedIdentityContract,
  type GrpcLeasedGroupedKeyRetentionObserver,
  type GrpcLeasedIdentityContract,
} from "./grpc-leased-identity";
import {
  makeGrpcLeasedSubscription,
  type GrpcLeasedSubscriptionLease,
} from "./grpc-leased-subscription";
import {
  makeGrpcLeasedRowLifecycle,
  type GrpcLeasedActiveLease,
  type RuntimeLeasedFeedDefinition,
} from "./grpc-leased-row-lifecycle";
import {
  callLeasedGrpcSourceRelease,
  callLeasedGrpcSourceRequest,
  makeGrpcSourceInput,
  makeDefaultGrpcClient,
  makeViewServerGrpcSourceError,
  ViewServerGrpcIngressError,
  type ViewServerGrpcClientFactory,
  type ViewServerGrpcSourceInput,
} from "./grpc-source-lifecycle";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./grpc-runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";
import {
  engineQueryWithoutRoute,
  makeSourceOwnershipPolicy,
  type ViewServerRuntimeCoreQueryPartition,
  type ViewServerRuntimeCoreInternalClient,
  type ViewServerRuntimeCoreInternalLiveClient,
} from "@effect-view-server/runtime-core/internal";

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

const isRuntimeLeasedFeed = (feed: {
  readonly lifecycle: string;
}): feed is RuntimeLeasedFeedDefinition => feed.lifecycle === "leased";

type LeasedFeedRoute = Readonly<Record<string, unknown>>;

type LeasedFeedRuntimeInput = ViewServerGrpcSourceInput<LeasedFeedRoute>;

/**
 * @internal One-shot, read-only construction observer for the intentionally private grouped-key
 * retention invariant. The production path allocates no observer closure or per-event metric when
 * it is absent, and no retained keys, maps, or mutation operations escape this package-local seam.
 */
type ViewServerGrpcGroupedKeyRetentionObserver = GrpcLeasedGroupedKeyRetentionObserver;

type AcquiredLease = {
  readonly enginePartition: ViewServerRuntimeCoreQueryPartition;
  readonly subscriptionLease: GrpcLeasedSubscriptionLease;
};

export type ViewServerGrpcLeaseManager<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

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

const callFeedRelease = (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => callLeasedGrpcSourceRelease(feedName, feed, input);

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
  const leases = new Map<string, GrpcLeasedActiveLease>();
  const feedsByTopic = leasedFeedsByTopic(options);
  const identityContracts = new Map<string, GrpcLeasedIdentityContract>();
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  const rowLifecycle = makeGrpcLeasedRowLifecycle(
    config,
    runtimeClient,
    requestHealthRefresh,
    health,
  );
  const lock = yield* Semaphore.make(1);
  const managerScope = yield* Scope.make("sequential");
  let closed = false;

  const captureQuery = <Query extends object>(query: Query) =>
    Result.try(() => snapshotViewServerQuery<Query>(query));

  const querySnapshotError = (topic: string): ViewServerRuntimeError =>
    runtimeError({
      code: "InvalidQuery",
      topic,
      message: viewServerQuerySnapshotErrorMessage,
    });

  const identityContractFor = Effect.fn("ViewServerRuntime.grpc.leased.identity.contract")(
    function* (topic: string, feedName: string, feed: RuntimeLeasedFeedDefinition) {
      const existing = identityContracts.get(topic);
      if (existing !== undefined) {
        return existing;
      }
      const topicDefinition = yield* rowLifecycle.topicDefinitionFor(topic, feedName).pipe(
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
        enginePartition: existing.enginePartition,
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
        rowLifecycle.cleanupRows(feedName, feed, storageKeys, identity.enginePartition.key),
      onCleanupFailure: (cause) =>
        runAllFinalizers([
          ignoreLeasedReleaseFailure(Effect.failCause(cause)),
          health.feedDegraded(feedKey, `gRPC leased feed row cleanup failed for ${feedName}`),
          health.clientDegraded(feed.client, `gRPC leased feed row cleanup failed for ${feedName}`),
          ignoreGrpcHealthRefreshFailure(requestHealthRefresh),
        ]),
      onRowsCleared: rowLifecycle.resetRowCount(feedKey),
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
    const lease: GrpcLeasedActiveLease = {
      feedName,
      feed,
      enginePartition: identity.enginePartition,
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
            acquire: rowLifecycle.acquireStream(lease, grpcClient, request),
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
          enginePartition: lease.enginePartition,
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
    const capturedQuery = captureQuery<ExactLiveQueryInputForTopic<Topics, Topic, Query>>(query);
    return Effect.gen(function* () {
      if (Result.isFailure(capturedQuery)) {
        return yield* Effect.fail(querySnapshotError(topic));
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
              acquired.enginePartition,
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
    const capturedQuery = captureQuery(query);
    return Effect.gen(function* () {
      if (Result.isFailure(capturedQuery)) {
        return yield* Effect.fail(querySnapshotError(topic));
      }
      const ownedQuery = capturedQuery.success;
      if (!feedsByTopic.has(topic)) {
        const routeError = validateLiveQuerySourceRoute(config.topics, topic, ownedQuery);
        if (routeError !== undefined) {
          return yield* Effect.fail(
            runtimeError({ code: "InvalidQuery", topic, message: routeError }),
          );
        }
      }
      const engineQuery = engineQueryWithoutRoute(ownedQuery);
      const lease = yield* lock
        .withPermit(acquireLease(topic, ownedQuery))
        .pipe(
          Effect.catchCause((cause) => Effect.failCause(normalizeAcquireLeaseCause(topic)(cause))),
        );
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeRuntimeInternal(topic, engineQuery);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const subscription = yield* restore(
            internalLiveClient.subscribeRuntimeObservedInternal(
              topic,
              engineQuery,
              acquired.subscriptionLease.terminalObserver,
              acquired.enginePartition,
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
