import type {
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@effect-view-server/client";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import {
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreProtocolQuerySubscriber,
  ViewServerRuntimeCoreQueryPartition,
} from "@effect-view-server/runtime-core/internal";
import { Effect, Option, Result } from "effect";
import type { GrpcLeasedSubscriptionLease } from "./grpc-leased-subscription";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type GrpcLeaseQueryAcquisition = {
  readonly enginePartition: ViewServerRuntimeCoreQueryPartition;
  readonly subscriptionLease: GrpcLeasedSubscriptionLease;
};

export type GrpcLeaseLiveClientFacade<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics>;
};

export const assembleGrpcLeaseLiveClient = <
  Topics extends ViewServerRuntimeTopicDefinitions,
>(input: {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>;
  readonly subscribeRuntimeQuery: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly acquireQueryLease: (
    topic: Extract<keyof Topics, string>,
    query: unknown,
  ) => Effect.Effect<Option.Option<GrpcLeaseQueryAcquisition>, ViewServerRuntimeError>;
  readonly querySnapshotError: (topic: string, message: string) => ViewServerRuntimeError;
  readonly close: Effect.Effect<void>;
}): GrpcLeaseLiveClientFacade<Topics> => {
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
    const capturedQuery = Result.try(() =>
      snapshotViewServerQuery<ExactLiveQueryInputForTopic<Topics, Topic, Query>>(query),
    );
    return Effect.gen(function* () {
      if (Result.isFailure(capturedQuery)) {
        return yield* Effect.fail(
          input.querySnapshotError(topic, viewServerQuerySnapshotErrorMessage),
        );
      }
      const ownedQuery = capturedQuery.success;
      const lease = yield* input.acquireQueryLease(topic, ownedQuery);
      if (Option.isNone(lease)) {
        return yield* input.internalLiveClient.subscribeInternal<Topic, Query>(topic, ownedQuery);
      }
      const acquired = lease.value;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const subscription = yield* restore(
            input.internalLiveClient.subscribeObservedInternal<Topic, Query>(
              topic,
              ownedQuery,
              acquired.subscriptionLease.terminalObserver,
              acquired.enginePartition,
            ),
          );
          return yield* acquired.subscriptionLease.attach({ subscription, query: ownedQuery });
        }),
      ).pipe(Effect.onError(() => acquired.subscriptionLease.close));
    });
  }

  const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] =
    input.subscribeRuntimeQuery;
  return {
    liveClient: {
      close: input.liveClient.close.pipe(Effect.ensuring(input.close)),
      health: input.liveClient.health,
      subscribe,
      subscribeRuntime,
      subscribeHealth: input.liveClient.subscribeHealth,
      subscribeHealthSummary: input.liveClient.subscribeHealthSummary,
    },
    protocolQuerySubscriber: { subscribeProtocolQuery: input.subscribeRuntimeQuery },
  };
};
