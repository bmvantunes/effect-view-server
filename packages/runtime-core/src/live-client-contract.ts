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

type ViewServerObservedQuerySubscriber<Topics extends DecodableTopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  terminalObserver: ColumnLiveViewTerminalObserver,
  partition?: ViewServerRuntimeCoreQueryPartition,
) => Effect.Effect<
  ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError | ViewServerTransportError
>;

export type ViewServerRuntimeCoreInternalLiveClient<Topics extends DecodableTopicDefinitions> = {
  readonly subscribeInternal: ViewServerRuntimeLiveClient<Topics>["subscribe"];
  readonly subscribeObservedInternal: ViewServerObservedQuerySubscriber<Topics>;
  readonly subscribeRuntimeInternal: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeRuntimeRoutedInternal: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeRuntimeObservedInternal: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};

export type ViewServerRuntimeCoreLiveClientModule<Topics extends DecodableTopicDefinitions> = {
  readonly liveClient: Omit<ViewServerRuntimeLiveClient<Topics>, "close"> &
    ViewServerRuntimeCoreInternalLiveClient<Topics>;
  readonly protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics>;
};
