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
import { makeRuntimeCoreLiveClient } from "./live-client";
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

export type RuntimeCoreConstructionOptions = {
  readonly afterEngineClose?: Effect.Effect<void>;
  readonly afterPushedHealthClose?: Effect.Effect<void>;
  readonly handoff?: RuntimeCoreResourceHandoffOptions;
};

export const makeViewServerRuntimeCoreInternalWithConstructionOptions: <
  const Topics extends DecodableTopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
  constructionOptions?: RuntimeCoreConstructionOptions,
) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError> =
  Effect.fn("ViewServerRuntimeCore.internal.make")(function* <
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
            const initialHealth: ViewServerHealth<Topics> = healthFromEngine(engineHealth, {
              transportHealth,
              ...(healthOverlay === undefined ? {} : { healthOverlay }),
              timing: {
                nowMillis: runtimeStartedAtMillis,
                nowNanos: runtimeStartedAtNanos,
                runtimeStartedAtNanos,
              },
            });
            const readRuntimeHealth = readHealthSnapshot(engine, {
              runtimeStartedAtNanos,
              transportHealth,
              healthOverlay,
            });
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
            const constructionClose = runAllFinalizers([pushedHealthClose, engineClose]).pipe(
              Effect.uninterruptible,
            );
            yield* markAcquired(constructionClose);
            const close = (yield* Effect.cached(constructionClose)).pipe(Effect.uninterruptible);
            yield* markAcquired(close);
            const runtimeClient = yield* makeRuntimeCoreClient<Topics>(
              config,
              engine,
              runtimeStartedAtNanos,
              transportHealth,
              pushedHealth.requestRefresh,
              healthOverlay,
            );
            const liveClient = yield* makeRuntimeCoreLiveClient<Topics>(
              config,
              engine,
              pushedHealth,
            );
            const publicLiveClient: ViewServerRuntimeCorePublicLiveClient<Topics> = {
              close,
              health: liveClient.health,
              subscribe: liveClient.subscribe,
              subscribeHealth: liveClient.subscribeHealth,
              subscribeHealthSummary: liveClient.subscribeHealthSummary,
            };
            return {
              client: runtimeClient.client,
              internalClient: runtimeClient.internalClient,
              publicClient: runtimeClient.client,
              liveClient: {
                ...liveClient,
                close,
              },
              serverLiveClient: {
                ...liveClient,
                close,
              },
              internalLiveClient: liveClient,
              publicLiveClient,
              close,
              requestHealthRefresh: pushedHealth.requestRefresh,
              refreshHealth: pushedHealth.refresh,
            };
          }),
        ),
      constructionOptions.handoff,
    );
  });
