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
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";
import { makeSourceOwnershipPolicy } from "./source-ownership-policy";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly close: Effect.Effect<void>;
  readonly requestHealthRefresh: Effect.Effect<void>;
  readonly refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
};

export type ViewServerRuntimeCoreInternalClient<Topics extends DecodableTopicDefinitions> =
  ViewServerRuntimeClient<Topics> & {
    readonly publishManyDecodedRows: (
      topic: Extract<keyof Topics, string>,
      rows: ReadonlyArray<object>,
    ) => Effect.Effect<void, ViewServerRuntimeError>;
    readonly publishManyDecodedRowsWithStorageKeys: (
      topic: Extract<keyof Topics, string>,
      rows: ReadonlyArray<{
        readonly storageKey: string;
        readonly row: object;
      }>,
    ) => Effect.Effect<void, ViewServerRuntimeError>;
    readonly publishManyWithStorageKeys: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      rows: ReadonlyArray<{
        readonly storageKey: string;
        readonly row: TopicRow<Topics, Topic>;
      }>,
    ) => Effect.Effect<void, ViewServerRuntimeError>;
  };

export const makeRuntimeCoreClient = Effect.fn("ViewServerRuntimeCore.client.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    engine: ColumnLiveViewEngineInternal<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
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
        return readHealth(
          engine,
          health,
          transportHealth,
          healthOverlay,
          () =>
            healthInstallEpoch === installEpoch &&
            (installMode === "scheduled" || healthReadEpoch === epoch),
          () => {
            healthInstallEpoch += 1;
          },
        );
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
      const publish = Effect.fn("ViewServerRuntimeCore.client.publish")(function* <
        Topic extends Extract<keyof Topics, string>,
      >(topic: Topic, row: TopicRow<Topics, Topic>) {
        yield* Effect.uninterruptible(
          engine.publish(topic, row).pipe(Effect.tap(() => requestHealthRefresh())),
        ).pipe(Effect.mapError(engineErrorToRuntimeError));
      });
      const internalClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        publish,
        publishMany: (topic, rows) =>
          Effect.uninterruptible(
            engine.publishMany(topic, rows).pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        publishManyDecodedRows: (topic, rows) =>
          Effect.uninterruptible(
            engine
              .publishManyDecodedRows(topic, rows)
              .pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.uninterruptible(
            engine
              .publishManyDecodedRowsWithStorageKeys(topic, rows)
              .pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        publishManyWithStorageKeys: (topic, rows) =>
          Effect.uninterruptible(
            engine
              .publishManyWithStorageKeys(topic, rows)
              .pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        patch: (topic, key, patch) =>
          Effect.uninterruptible(
            engine.patch(topic, key, patch).pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        delete: (topic, key) =>
          Effect.uninterruptible(
            engine.delete(topic, key).pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        snapshot,
        health: () => healthReader(),
        reset: () =>
          Effect.uninterruptible(
            engine.reset().pipe(Effect.tap(() => requestHealthRefresh())),
          ).pipe(Effect.mapError(engineErrorToRuntimeError)),
      };
      return {
        client: {
          publish: (topic, row) =>
            sourceOwnership
              .requirePublicMutationAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.publish(topic, row))),
          publishMany: (topic, rows) =>
            sourceOwnership
              .requirePublicMutationAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.publishMany(topic, rows))),
          patch: (topic, key, patch) =>
            sourceOwnership
              .requirePublicMutationAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.patch(topic, key, patch))),
          delete: (topic, key) =>
            sourceOwnership
              .requirePublicMutationAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.delete(topic, key))),
          snapshot: (topic, query) =>
            sourceOwnership
              .requirePublicReadAllowed(topic, "runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.snapshot(topic, query))),
          health: internalClient.health,
          reset: () =>
            sourceOwnership
              .requirePublicResetAllowed("runtimeCore")
              .pipe(Effect.flatMap(() => internalClient.reset())),
        },
        internalClient,
        close: healthRefreshScheduler.close,
        requestHealthRefresh: requestHealthRefresh(),
        refreshHealth: refreshHealthNow(),
      } satisfies RuntimeCoreClientInstance<Topics>;
    }),
);
