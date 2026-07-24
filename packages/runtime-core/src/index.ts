import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type {
  ViewServerConfig,
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import type {
  ViewServerRuntimeCoreInstance,
  ViewServerRuntimeCoreOptionsFor,
} from "./runtime-core-types";
import type { ViewServerSourceRequirements } from "./source-runtime";

export type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
export type { GroupedIncrementalAdmissionLimits } from "@effect-view-server/column-live-view-engine";
export type { RuntimeCoreTransportHealth } from "./health";
export type { RuntimeCoreHealthOverlay } from "./health";
export type {
  ViewServerRuntimeCorePublicClient,
  ViewServerRuntimeCorePublicLiveClient,
  ViewServerRuntimeCoreServerLiveClient,
} from "./public-client";
export type {
  ViewServerRuntimeCoreInstance,
  ViewServerRuntimeCoreOptions,
  ViewServerRuntimeCoreOptionsFor,
} from "./runtime-core-types";
export type { ViewServerSourceRequirements } from "./source-runtime";

export const makeViewServerRuntimeCore: <
  const Topics extends DecodableTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) => Effect.Effect<
  ViewServerRuntimeCoreInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerRuntimeCore.make")(function* <
  const Topics extends DecodableTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, input);
  return {
    client: runtimeCore.publicClient,
    liveClient: runtimeCore.publicLiveClient,
    serverLiveClient: runtimeCore.serverLiveClient,
    close: runtimeCore.close,
    requestHealthRefresh: runtimeCore.requestHealthRefresh,
    refreshHealth: runtimeCore.refreshHealth,
  };
});

export const createViewServerRuntimeCore = <
  const Topics extends DecodableTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerRuntimeCoreOptionsFor<Topics> = {},
): ViewServerRuntimeCoreInstance<Topics> =>
  Effect.runSync(makeViewServerRuntimeCore(config, options));
