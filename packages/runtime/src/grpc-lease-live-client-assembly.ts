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
import type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "@effect-view-server/runtime-core/internal";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type RuntimeSubscription = Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
>;

export type GrpcLeaseLiveClientFacade<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics>;
};

export const assembleGrpcLeaseLiveClient = <
  Topics extends ViewServerRuntimeTopicDefinitions,
>(input: {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly subscribeRuntimeQuery: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => RuntimeSubscription;
  readonly close: Effect.Effect<void>;
}): GrpcLeaseLiveClientFacade<Topics> => {
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
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
  >(topic: Topic, query: ExactLiveQueryInputForTopic<Topics, Topic, Query>): RuntimeSubscription {
    return input.subscribeRuntimeQuery(topic, query);
  }

  function subscribeRuntime<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(topic: Topic, query: ExactLiveQueryInputForTopic<Topics, Topic, Query>): RuntimeSubscription;
  function subscribeRuntime<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(topic: Topic, query: ExactLiveQueryInputForTopic<Topics, Topic, Query>): RuntimeSubscription;
  function subscribeRuntime<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(topic: Topic, query: ExactLiveQueryInputForTopic<Topics, Topic, Query>): RuntimeSubscription {
    return input.subscribeRuntimeQuery(topic, query);
  }

  return {
    liveClient: {
      close: input.liveClient.close.pipe(Effect.ensuring(input.close)),
      health: input.liveClient.health,
      subscribe,
      subscribeRuntime,
      subscribeHealth: input.liveClient.subscribeHealth,
      subscribeHealthSummary: input.liveClient.subscribeHealthSummary,
      subscribeSourceHealth: input.liveClient.subscribeSourceHealth,
    },
    protocolQuerySubscriber: { subscribeProtocolQuery: input.subscribeRuntimeQuery },
  };
};
