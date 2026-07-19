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
import { Effect, type Duration } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import {
  makeHealthRefreshScheduler,
  makeCoalescedHealthReader,
  readHealth,
  type RuntimeCoreHealthOverlay,
  type RuntimeCoreTransportHealth,
} from "./health";
import { engineQueryWithoutRoute } from "./engine-query";
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";
import {
  makeRuntimeCoreMutationPipeline,
  type ViewServerRuntimeCoreInternalMutations,
} from "./source-mutation-pipeline";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly close: Effect.Effect<void>;
  readonly requestHealthRefresh: Effect.Effect<void>;
  readonly refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
};

export type ViewServerRuntimeCoreInternalClient<Topics extends DecodableTopicDefinitions> =
  ViewServerRuntimeClient<Topics> & ViewServerRuntimeCoreInternalMutations<Topics>;

export const makeRuntimeCoreClient = Effect.fn("ViewServerRuntimeCore.client.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    engine: ColumnLiveViewEngineInternal<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
    runtimeStartedAtNanos: bigint,
    transportHealth: RuntimeCoreTransportHealth<Topics>,
    healthOverlay?: RuntimeCoreHealthOverlay<Topics>,
    healthRefreshCadence?: Duration.Input,
  ): Effect.Effect<RuntimeCoreClientInstance<Topics>> =>
    Effect.gen(function* () {
      const sourceOwnership = makeSourceOwnershipPolicy(config);
      let healthReadEpoch = 0;
      let healthInstallEpoch = 0;
      const bumpHealthReadEpoch = Effect.sync(() => {
        healthReadEpoch += 1;
      });
      const readRuntimeHealth = (epoch: number, installMode: "strict" | "scheduled") => {
        const installEpoch = healthInstallEpoch;
        return readHealth(engine, health, {
          runtimeStartedAtNanos,
          transportHealth,
          ...(healthOverlay === undefined ? {} : { healthOverlay }),
          shouldInstall: () =>
            healthInstallEpoch === installEpoch &&
            (installMode === "scheduled" || healthReadEpoch === epoch),
          onInstall: () => {
            healthInstallEpoch += 1;
          },
        });
      };
      const healthReader = makeCoalescedHealthReader(
        (epoch) => readRuntimeHealth(epoch, "strict"),
        () => healthReadEpoch,
      );
      const scheduledHealthReader = makeCoalescedHealthReader(
        (epoch) => readRuntimeHealth(epoch, "scheduled"),
        () => healthReadEpoch,
      );
      const scheduledHealthRefresh = Effect.fn(
        "ViewServerRuntimeCore.client.healthRefresh.scheduled",
      )(function* () {
        yield* scheduledHealthReader();
      });
      const healthRefreshScheduler = yield* makeHealthRefreshScheduler(
        scheduledHealthRefresh(),
        healthRefreshCadence,
      );
      const requestHealthRefresh = Effect.fn("ViewServerRuntimeCore.client.healthRefresh.request")(
        function* () {
          yield* Effect.uninterruptible(
            bumpHealthReadEpoch.pipe(Effect.andThen(healthRefreshScheduler.request)),
          );
        },
      );
      const refreshHealthNow = Effect.fn("ViewServerRuntimeCore.client.healthRefresh.now")(
        function* () {
          return yield* Effect.uninterruptible(
            bumpHealthReadEpoch.pipe(Effect.andThen(healthReader())),
          );
        },
      );
      const mutationPipeline = makeRuntimeCoreMutationPipeline(
        config,
        engine,
        requestHealthRefresh(),
      );
      function snapshot<
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
      >;
      function snapshot<Topic extends Extract<keyof Topics, string>>(
        topic: Topic,
        query: Readonly<Record<string, unknown>>,
      ): Effect.Effect<LiveQueryResult<object>, ViewServerRuntimeError> {
        return Effect.suspend(() => {
          const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
          if (routeError !== undefined) {
            return Effect.fail(invalidRuntimeQueryError(topic, routeError));
          }
          const engineQuery = engineQueryWithoutRoute(query);
          return engine
            .snapshotRuntime(topic, engineQuery)
            .pipe(Effect.mapError(engineErrorToRuntimeError));
        });
      }
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
        close: healthRefreshScheduler.close,
        requestHealthRefresh: requestHealthRefresh(),
        refreshHealth: refreshHealthNow(),
      } satisfies RuntimeCoreClientInstance<Topics>;
    }),
);
