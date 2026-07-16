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
import { engineErrorToRuntimeError } from "./runtime-error";
import type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";
export {
  collectSourceOwnershipConflicts,
  makeSourceOwnershipPolicy,
} from "./source-ownership-policy";
export {
  makeTopicSourceBindings,
  topicGrpcSourceMetadataFromUnknown,
} from "./source-binding-resolution";
export { makeRuntimeCoreMutationPipeline } from "./source-mutation-pipeline";
export type {
  TopicDefinitionHasRequiredDefinedObjectProperty,
  TopicDefinitionHasSourceOwner,
  TopicGrpcSourceLifecycle,
  TopicGrpcSourceMetadata,
  TopicGrpcSourceValidMetadata,
  TopicSourceBinding,
  TopicSourceOwner,
} from "./source-binding-resolution";
export type {
  RuntimeCoreDecodedRowWithStorageKey,
  RuntimeCoreMutationPipeline,
  ViewServerRuntimeCoreCheckedMutations,
  ViewServerRuntimeCoreInternalMutations,
} from "./source-mutation-pipeline";
export type {
  SourceOwnershipAccessProfile,
  SourceOwnershipConflict,
  SourceOwnershipDecision,
  SourceOwnershipGrpcLifecycle,
  SourceOwnershipGrpcOptions,
  SourceOwnershipKafkaOptions,
  SourceOwnershipOwner,
  SourceOwnershipPolicy,
  SourceOwnershipTopic,
} from "./source-ownership-policy";

export type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreTerminalObserver,
} from "./live-client";
export type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";
export type {
  ViewServerRuntimeCoreInternalInstance,
  ViewServerRuntimeCoreInternalOptionsFor,
} from "./runtime-core-types";

export const makeViewServerRuntimeCoreInternal: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError> =
  Effect.fn("ViewServerRuntimeCore.internal.make")(function* <
    const Topics extends DecodableTopicDefinitions,
  >(config: ViewServerTopicConfig<Topics>, input: ViewServerRuntimeCoreInternalOptionsFor<Topics>) {
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
    const engine = yield* createColumnLiveViewEngineInternal<Topics>(engineConfig).pipe(
      Effect.mapError(engineErrorToRuntimeError),
    );
    const engineHealth = yield* engine.health();
    const runtimeStartedAtMillis = yield* Clock.currentTimeMillis;
    const runtimeStartedAtNanos = yield* Clock.currentTimeNanos;
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
    const runtimeClient = yield* makeRuntimeCoreClient<Topics>(
      config,
      engine,
      runtimeStartedAtNanos,
      transportHealth,
      pushedHealth.requestRefresh,
      healthOverlay,
    );
    const liveClient = yield* makeRuntimeCoreLiveClient<Topics>(config, engine, pushedHealth);
    const close = (yield* Effect.cached(
      runAllFinalizers([pushedHealth.close, engine.close()]),
    )).pipe(Effect.uninterruptible);
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
  });
