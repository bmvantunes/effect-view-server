import { type DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import { createColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import type {
  ViewServerHealth,
  ViewServerTopicConfig,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { runAllFinalizers } from "@effect-view-server/effect-utils";
import { Clock, Effect } from "effect";
import { defaultRuntimeCoreTransportHealth, healthFromEngine, readHealthSnapshot } from "./health";
import { makeRuntimeCoreLiveClientModule } from "./live-client";
import type { ViewServerRuntimeCorePublicLiveClient } from "./public-client";
import { makeRuntimeCorePushedHealthHub } from "./pushed-health";
import { makeRuntimeCoreClient } from "./runtime-client";
import type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";
import { engineErrorToRuntimeError } from "./runtime-error";
import {
  acquireRuntimeCoreResourceHandoff,
  type RuntimeCoreResourceHandoffOptions,
} from "./subscription-handoff";
import {
  makeRuntimeCoreSourceManager,
  type RuntimeCoreSourceManager,
  type ViewServerSourceRequirements,
} from "./source-runtime";

export type RuntimeCoreConstructionOptions = {
  readonly afterEngineClose?: Effect.Effect<void>;
  readonly afterPushedHealthClose?: Effect.Effect<void>;
  readonly afterRuntimeHealthRead?: Effect.Effect<void>;
  readonly handoff?: RuntimeCoreResourceHandoffOptions;
};

export const makeViewServerRuntimeCoreInternalWithConstructionOptions: <
  const Topics extends DecodableTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
  constructionOptions?: RuntimeCoreConstructionOptions,
) => Effect.Effect<
  ViewServerRuntimeCoreInternalInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerRuntimeCore.internal.make")(function* <
  const Topics extends DecodableTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
  constructionOptions: RuntimeCoreConstructionOptions = {},
) {
  const transportHealth = input.transportHealth ?? defaultRuntimeCoreTransportHealth;
  const healthOverlay = input.healthOverlay;
  const engineConfig = {
    ...(input.groupedIncrementalAdmissionLimits === undefined
      ? {}
      : { groupedIncrementalAdmissionLimits: input.groupedIncrementalAdmissionLimits }),
    ...(input.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: input.subscriptionQueueCapacity }),
    topics: config.topics,
  };
  return yield* acquireRuntimeCoreResourceHandoff(
    (markAcquired) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const engine = yield* restore(
            createColumnLiveViewEngineInternal<Topics>(engineConfig).pipe(
              Effect.mapError(engineErrorToRuntimeError),
            ),
          );
          const engineClose =
            constructionOptions.afterEngineClose === undefined
              ? engine.close()
              : engine.close().pipe(Effect.ensuring(constructionOptions.afterEngineClose));
          yield* markAcquired(engineClose);
          const engineHealth = yield* restore(engine.health());
          const runtimeStartedAtMillis = yield* restore(Clock.currentTimeMillis);
          const runtimeStartedAtNanos = yield* restore(Clock.currentTimeNanos);
          let sourceManager: RuntimeCoreSourceManager<Topics> | undefined;
          const overlaySourceHealth = (
            health: ViewServerHealth<Topics>,
          ): ViewServerHealth<Topics> =>
            sourceManager === undefined ? health : sourceManager.overlayHealth(health);
          const initialHealth: ViewServerHealth<Topics> = overlaySourceHealth(
            healthFromEngine(engineHealth, {
              transportHealth,
              ...(healthOverlay === undefined ? {} : { healthOverlay }),
              timing: {
                nowMillis: runtimeStartedAtMillis,
                nowNanos: runtimeStartedAtNanos,
                runtimeStartedAtNanos,
              },
            }),
          );
          const readRuntimeHealth = readHealthSnapshot(engine, {
            runtimeStartedAtNanos,
            transportHealth,
            healthOverlay,
          }).pipe(
            Effect.map(overlaySourceHealth),
            Effect.tap(() => constructionOptions.afterRuntimeHealthRead ?? Effect.void),
          );
          const pushedHealth = yield* makeRuntimeCorePushedHealthHub(
            initialHealth,
            readRuntimeHealth,
            input.healthRefreshCadence,
          );
          const pushedHealthClose =
            constructionOptions.afterPushedHealthClose === undefined
              ? pushedHealth.close
              : pushedHealth.close.pipe(
                  Effect.ensuring(constructionOptions.afterPushedHealthClose),
                );
          const partialConstructionClose = runAllFinalizers([engineClose, pushedHealthClose]).pipe(
            Effect.uninterruptible,
          );
          yield* markAcquired(partialConstructionClose);
          const runtimeClient = yield* makeRuntimeCoreClient<Topics>(
            config,
            engine,
            readRuntimeHealth,
            pushedHealth.requestRefresh,
          );
          const finalizePushedHealth = runAllFinalizers([
            runtimeClient.requestHealthRefresh,
            pushedHealth.refresh.pipe(Effect.asVoid),
            pushedHealthClose,
          ]);
          sourceManager = yield* makeRuntimeCoreSourceManager(
            config,
            runtimeClient.internalClient,
            pushedHealth.requestRefresh,
          );
          const sourceClose = sourceManager.close;
          const sourceConstructionClose = runAllFinalizers([
            sourceClose,
            engineClose,
            finalizePushedHealth,
          ]).pipe(Effect.uninterruptible);
          yield* markAcquired(sourceConstructionClose);
          if (sourceManager.hasSources) {
            yield* pushedHealth.refresh;
          }
          const constructionClose = sourceConstructionClose;
          yield* markAcquired(constructionClose);
          const close = (yield* Effect.cached(constructionClose)).pipe(Effect.uninterruptible);
          yield* markAcquired(close);
          const liveClientModule = yield* makeRuntimeCoreLiveClientModule<Topics>(
            config,
            engine,
            pushedHealth,
            runtimeClient.requestHealthRefresh,
            sourceManager,
          );
          const liveClient = liveClientModule.liveClient;
          const publicLiveClient: ViewServerRuntimeCorePublicLiveClient<Topics> = {
            close,
            health: liveClient.health,
            subscribe: liveClient.subscribe,
            subscribeHealth: liveClient.subscribeHealth,
            subscribeHealthSummary: liveClient.subscribeHealthSummary,
            subscribeSourceHealth: liveClient.subscribeSourceHealth,
          };
          return {
            client: runtimeClient.client,
            decodedMutationClient: runtimeClient.decodedMutationClient,
            internalClient: runtimeClient.internalClient,
            publicClient: runtimeClient.client,
            liveClient: {
              ...liveClient,
              close,
            },
            serverLiveClient: {
              subscribeHealth: liveClient.subscribeHealth,
              subscribeHealthSummary: liveClient.subscribeHealthSummary,
              subscribeProtocolSourceHealth: sourceManager.subscribeProtocolSourceHealth,
              subscribeProtocolQuery:
                liveClientModule.protocolQuerySubscriber.subscribeProtocolQuery,
            },
            internalLiveClient: liveClient,
            protocolQuerySubscriber: liveClientModule.protocolQuerySubscriber,
            publicLiveClient,
            close,
            requestHealthRefresh: runtimeClient.requestHealthRefresh,
            refreshHealth: pushedHealth.refresh,
          };
        }),
      ),
    constructionOptions.handoff,
  );
});
