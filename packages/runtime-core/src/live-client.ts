import type {
  ColumnLiveViewEngineError,
  ColumnLiveViewSubscription,
  DecodableTopicDefinitions,
} from "@effect-view-server/column-live-view-engine";
import type {
  ColumnLiveViewEngineInternal,
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
  ViewServerTopicConfig,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import { Effect, Stream } from "effect";
import type { RuntimeCorePushedHealthHub } from "./pushed-health";
import { engineErrorToRuntimeError } from "./runtime-error";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";
import { acquireRuntimeCoreResourceHandoff } from "./subscription-handoff";

export type ViewServerRuntimeCoreEngineQueryInput<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>;

export type ViewServerRuntimeCoreTerminalObserver = ColumnLiveViewTerminalObserver;

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
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};

export type RuntimeCoreLiveClientInstance<Topics extends DecodableTopicDefinitions> = Omit<
  ViewServerRuntimeLiveClient<Topics>,
  "close"
> &
  ViewServerRuntimeCoreInternalLiveClient<Topics>;

export const makeRuntimeCoreLiveClient = Effect.fn("ViewServerRuntimeCore.liveClient.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    engine: ColumnLiveViewEngineInternal<Topics>,
    pushedHealth: RuntimeCorePushedHealthHub<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
  ): Effect.Effect<RuntimeCoreLiveClientInstance<Topics>> =>
    Effect.sync<RuntimeCoreLiveClientInstance<Topics>>(() => {
      const sourceOwnership = makeSourceOwnershipPolicy(config);
      function wrapEngineSubscription<Row>(
        acquisition: Effect.Effect<ColumnLiveViewSubscription<Row>, ColumnLiveViewEngineError>,
      ): Effect.Effect<ViewServerLiveSubscription<Row>, ViewServerRuntimeError> {
        return Effect.suspend(() =>
          acquireRuntimeCoreResourceHandoff((markAcquired) =>
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const subscription = yield* restore(
                  acquisition.pipe(Effect.mapError(engineErrorToRuntimeError)),
                );
                const requestRefreshAfterRelease = requestHealthRefresh;
                const closeSubscription = subscription
                  .close()
                  .pipe(Effect.ensuring(requestRefreshAfterRelease));
                yield* markAcquired(closeSubscription);
                const wrapped = {
                  events: subscription.events.pipe(Stream.ensuring(requestRefreshAfterRelease)),
                  close: () => closeSubscription,
                } satisfies ViewServerLiveSubscription<Row>;
                yield* restore(requestHealthRefresh);
                return wrapped;
              }),
            ),
          ),
        );
      }
      function subscribeInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      >;
      function subscribeInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ViewServerRuntimeCoreEngineQueryInput<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return wrapEngineSubscription(engine.subscribe<Topic, Query>(topic, query));
      }
      function subscribeObservedInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
        terminalObserver: ColumnLiveViewTerminalObserver,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      >;
      function subscribeObservedInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ViewServerRuntimeCoreEngineQueryInput<Topics, Topic, Query>,
        terminalObserver: ColumnLiveViewTerminalObserver,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return wrapEngineSubscription(
          engine.subscribeObserved<Topic, Query>(topic, query, terminalObserver),
        );
      }
      const subscribeRuntimeInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeInternal"] =
        (topic, query) => wrapEngineSubscription(engine.subscribeRuntime(topic, query));
      const subscribeRuntimeObservedInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeObservedInternal"] =
        (topic, query, terminalObserver) =>
          wrapEngineSubscription(engine.subscribeRuntimeObserved(topic, query, terminalObserver));
      function subscribe<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      >;
      function subscribe<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return sourceOwnership
          .requirePublicReadAllowed(topic, "runtimeCore")
          .pipe(Effect.flatMap(() => subscribeInternal<Topic, Query>(topic, query)));
      }
      const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
        topic,
        query,
      ) =>
        sourceOwnership
          .requirePublicReadAllowed(topic, "runtimeCore")
          .pipe(Effect.flatMap(() => subscribeRuntimeInternal(topic, query)));

      return {
        subscribe,
        subscribeRuntime,
        subscribeInternal,
        subscribeObservedInternal,
        subscribeRuntimeInternal,
        subscribeRuntimeObservedInternal,
        subscribeHealthSummary: pushedHealth.subscribeHealthSummary,
        subscribeHealth: pushedHealth.subscribeHealth,
        health: pushedHealth.health,
      };
    }),
);
