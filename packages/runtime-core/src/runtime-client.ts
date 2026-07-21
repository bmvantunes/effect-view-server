import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type { ColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import type {
  LiveQueryResult,
  ViewServerHealth,
  ViewServerTopicConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerRuntimeDecodedMutationClient } from "@effect-view-server/config/internal";
import { validateLiveQuerySourceRoute } from "@effect-view-server/config";
import {
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import { Effect, Result } from "effect";
import { makeCoalescedHealthReader } from "./health";
import { engineQueryWithoutRoute } from "./engine-query";
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";
import { makeRuntimeCoreSnapshotQueryFacade } from "./snapshot-query-facade";
import {
  makeRuntimeCoreMutationPipeline,
  type ViewServerRuntimeCoreInternalMutations,
} from "./source-mutation-pipeline";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly decodedMutationClient: ViewServerRuntimeDecodedMutationClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly requestHealthRefresh: Effect.Effect<void>;
};

export type ViewServerRuntimeCoreInternalClient<Topics extends DecodableTopicDefinitions> =
  ViewServerRuntimeClient<Topics> &
    ViewServerRuntimeCoreInternalMutations<Topics> & {
      readonly snapshotRuntimeInternal: (
        topic: Extract<keyof Topics, string>,
        query: Readonly<Record<string, unknown>>,
      ) => Effect.Effect<LiveQueryResult<object>, ViewServerRuntimeError>;
    };

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
      const snapshotQuery = (
        topic: Extract<keyof Topics, string>,
        query: Readonly<Record<string, unknown>>,
      ): Effect.Effect<LiveQueryResult<object>, ViewServerRuntimeError> => {
        const capturedQuery = Result.try(() => snapshotViewServerQuery(query));
        return Effect.suspend(() => {
          if (Result.isFailure(capturedQuery)) {
            return Effect.fail(
              invalidRuntimeQueryError(topic, viewServerQuerySnapshotErrorMessage),
            );
          }
          const routeError = validateLiveQuerySourceRoute(
            config.topics,
            topic,
            capturedQuery.success,
          );
          if (routeError !== undefined) {
            return Effect.fail(invalidRuntimeQueryError(topic, routeError));
          }
          const engineQuery = engineQueryWithoutRoute(capturedQuery.success);
          return engine
            .snapshotRuntime(topic, engineQuery)
            .pipe(Effect.mapError(engineErrorToRuntimeError));
        });
      };
      const { snapshotInternal, snapshot } = makeRuntimeCoreSnapshotQueryFacade<Topics>({
        snapshotQuery,
        requirePublicReadAllowed: (topic) =>
          sourceOwnership.requirePublicReadAllowed(topic, "runtimeCore"),
      });
      const internalClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...mutationPipeline.internalMutations,
        snapshot: snapshotInternal,
        snapshotRuntimeInternal: snapshotQuery,
        health: () => healthReader(),
      };
      return {
        client: {
          ...mutationPipeline.checkedMutations,
          snapshot,
          health: internalClient.health,
        },
        decodedMutationClient: mutationPipeline.decodedMutationClient,
        internalClient,
        requestHealthRefresh,
      } satisfies RuntimeCoreClientInstance<Topics>;
    }),
);
