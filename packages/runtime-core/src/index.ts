import type {
  DecodableTopicDefinitions,
  GroupedIncrementalAdmissionLimits,
} from "@effect-view-server/column-live-view-engine";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect, type Duration } from "effect";
import { type RuntimeCoreHealthOverlay, type RuntimeCoreTransportHealth } from "./health";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import type {
  ViewServerRuntimeCorePublicClient,
  ViewServerRuntimeCorePublicLiveClient,
  ViewServerRuntimeCoreServerLiveClient,
} from "./public-client";

export type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
export type { GroupedIncrementalAdmissionLimits } from "@effect-view-server/column-live-view-engine";
export type { RuntimeCoreTransportHealth } from "./health";
export type { RuntimeCoreHealthOverlay } from "./health";
export type {
  ViewServerRuntimeCorePublicClient,
  ViewServerRuntimeCorePublicLiveClient,
  ViewServerRuntimeCoreServerLiveClient,
} from "./public-client";

export type ViewServerRuntimeCoreInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeCorePublicClient<Topics>;
  readonly liveClient: ViewServerRuntimeCorePublicLiveClient<Topics>;
  readonly serverLiveClient: ViewServerRuntimeCoreServerLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
  readonly requestHealthRefresh: Effect.Effect<void>;
  readonly refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
};

export type ViewServerRuntimeCoreOptions = {
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly transportHealth?: RuntimeCoreTransportHealth<DecodableTopicDefinitions>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<DecodableTopicDefinitions>;
  readonly healthRefreshCadence?: Duration.Input;
};

export type ViewServerRuntimeCoreOptionsFor<Topics extends DecodableTopicDefinitions> = {
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly transportHealth?: RuntimeCoreTransportHealth<Topics>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<Topics>;
  readonly healthRefreshCadence?: Duration.Input;
};

export const makeViewServerRuntimeCore: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) => Effect.Effect<ViewServerRuntimeCoreInstance<Topics>, ViewServerRuntimeError> = Effect.fn(
  "ViewServerRuntimeCore.make",
)(function* <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, input);
  return {
    client: runtimeCore.publicClient,
    liveClient: runtimeCore.publicLiveClient,
    serverLiveClient: runtimeCore.liveClient,
    close: runtimeCore.close,
    requestHealthRefresh: runtimeCore.requestHealthRefresh,
    refreshHealth: runtimeCore.refreshHealth,
  };
});

export const createViewServerRuntimeCore = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeCoreOptionsFor<Topics> = {},
): ViewServerRuntimeCoreInstance<Topics> =>
  Effect.runSync(makeViewServerRuntimeCore(config, options));
