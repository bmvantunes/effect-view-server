import type {
  DecodableTopicDefinitions,
  GroupedIncrementalAdmissionLimits,
} from "@effect-view-server/column-live-view-engine";
import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type {
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerRuntimeDecodedMutationClient } from "@effect-view-server/config/internal";
import type { Duration, Effect } from "effect";
import type { RuntimeCoreHealthOverlay, RuntimeCoreTransportHealth } from "./health";
import type { ViewServerRuntimeCoreInternalLiveClient } from "./live-client";
import type {
  ViewServerRuntimeCorePublicClient,
  ViewServerRuntimeCorePublicLiveClient,
  ViewServerRuntimeCoreServerLiveClient,
} from "./public-client";
import type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";

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

export type ViewServerRuntimeCoreInternalInstance<Topics extends DecodableTopicDefinitions> = Omit<
  ViewServerRuntimeCoreInstance<Topics>,
  "client" | "liveClient"
> & {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly decodedMutationClient: ViewServerRuntimeDecodedMutationClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly publicClient: ViewServerRuntimeCorePublicClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>;
  readonly publicLiveClient: ViewServerRuntimeCorePublicLiveClient<Topics>;
};

export type ViewServerRuntimeCoreInternalOptionsFor<Topics extends DecodableTopicDefinitions> =
  ViewServerRuntimeCoreOptionsFor<Topics>;
