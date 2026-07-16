import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicRow,
  ViewServerHealth,
  ViewServerTopicConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import { Effect } from "effect";
import { makeCoalescedHealthReader } from "./health";
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";
import {
  makeRuntimeCoreMutationPipeline,
  type ViewServerRuntimeCoreInternalMutations,
} from "./source-mutation-pipeline";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly requestHealthRefresh: Effect.Effect<void>;
};

export type ViewServerRuntimeCoreInternalClient<Topics extends DecodableTopicDefinitions> =
  ViewServerRuntimeClient<Topics> & ViewServerRuntimeCoreInternalMutations<Topics>;

export const makeRuntimeCoreClient = Effect.fn("ViewServerRuntimeCore.client.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    engine: ColumnLiveViewEngineInternal<Topics>,
    readFreshRuntimeHealth: Effect.Effect<ViewServerHealth<Topics>>,
    requestPushedHealthRefresh: Effect.Effect<void>,
  ): Effect.Effect<RuntimeCoreClientInstance<Topics>> =>
    Effect.sync(() => {
      const sourceOwnership = makeSourceOwnershipPolicy(config);
      let freshHealthReadEpoch = 0;
      const healthReader = makeCoalescedHealthReader(
        () => readFreshRuntimeHealth,
        () => freshHealthReadEpoch,
      );
      const requestHealthRefresh = Effect.sync(() => {
        freshHealthReadEpoch += 1;
      }).pipe(Effect.andThen(requestPushedHealthRefresh));
      const mutationPipeline = makeRuntimeCoreMutationPipeline(
        config,
        engine,
        requestHealthRefresh,
      );
      const snapshot = <
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError
      > =>
        Effect.suspend(() => {
          const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
          if (routeError !== undefined) {
            return Effect.fail(invalidRuntimeQueryError(topic, routeError));
          }
          return engine
            .snapshot<Topic, Query>(topic, query)
            .pipe(Effect.mapError(engineErrorToRuntimeError));
        });
      const internalClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...mutationPipeline.internalMutations,
        snapshot,
        health: () => healthReader(),
      };
      return {
        client: {
          ...mutationPipeline.checkedMutations,
          snapshot: (topic, query) =>
            sourceOwnership
              .requirePublicReadAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.snapshot(topic, query))),
          health: internalClient.health,
        },
        internalClient,
        requestHealthRefresh,
      } satisfies RuntimeCoreClientInstance<Topics>;
    }),
);
