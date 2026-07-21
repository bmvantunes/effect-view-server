import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ColumnLiveViewTerminalObserver } from "@effect-view-server/column-live-view-engine/internal";
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
import { Effect } from "effect";
import type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreQueryPartition,
} from "./live-client-contract";

type RuntimeSubscription = Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
>;

export type RuntimeCoreLiveQuerySubstrate<Topics extends DecodableTopicDefinitions> = {
  readonly subscribeQuery: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => RuntimeSubscription;
  readonly subscribeObservedQuery: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ) => RuntimeSubscription;
  readonly requirePublicReadAllowed: (
    topic: Extract<keyof Topics, string>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
};

export type RuntimeCoreLiveQueryFacade<Topics extends DecodableTopicDefinitions> = Pick<
  ViewServerRuntimeCoreInternalLiveClient<Topics>,
  "subscribeInternal" | "subscribeObservedInternal"
> & {
  readonly subscribe: ViewServerRuntimeLiveClient<Topics>["subscribe"];
};

export const makeRuntimeCoreLiveQueryFacade = <Topics extends DecodableTopicDefinitions>(
  substrate: RuntimeCoreLiveQuerySubstrate<Topics>,
): RuntimeCoreLiveQueryFacade<Topics> => {
  function subscribeInternal<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribeInternal(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ): RuntimeSubscription {
    return substrate.subscribeQuery(topic, query);
  }

  function subscribeObservedInternal<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribeObservedInternal(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ViewServerRuntimeCoreQueryPartition,
  ): RuntimeSubscription {
    return substrate.subscribeObservedQuery(topic, query, terminalObserver, partition);
  }

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ): RuntimeSubscription {
    const acquisition = substrate.subscribeQuery(topic, query);
    return substrate.requirePublicReadAllowed(topic).pipe(Effect.flatMap(() => acquisition));
  }

  return { subscribe, subscribeInternal, subscribeObservedInternal };
};
