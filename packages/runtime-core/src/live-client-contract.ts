import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type {
  ColumnLiveViewEngineQueryPartition,
  ColumnLiveViewTerminalObserver,
} from "@effect-view-server/column-live-view-engine/internal";
import type {
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@effect-view-server/client";
import type {
  ExactLiveQueryInput,
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import type { Effect } from "effect";
import type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "./protocol-query-subscriber";

export type ViewServerRuntimeCoreEngineQueryInput<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>;

export type ViewServerRuntimeCoreTerminalObserver = ColumnLiveViewTerminalObserver;
export type ViewServerRuntimeCoreQueryPartition = ColumnLiveViewEngineQueryPartition;

export type ViewServerRuntimeCoreInternalLiveClient<Topics extends DecodableTopicDefinitions> = {
  readonly subscribeInternal: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeObservedInternal: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ) => Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeRuntimeInternal: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeRuntimeObservedInternal: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};

export type ViewServerRuntimeCoreLiveClientModule<Topics extends DecodableTopicDefinitions> = {
  readonly liveClient: ViewServerRuntimeLiveClient<Topics> &
    ViewServerRuntimeCoreInternalLiveClient<Topics>;
  readonly protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics>;
};
