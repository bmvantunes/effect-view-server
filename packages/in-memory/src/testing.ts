import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalInstance,
  type ViewServerRuntimeCoreInternalOptionsFor,
} from "@effect-view-server/runtime-core/internal";
import { Effect } from "effect";
import type { ViewServerInMemoryOptions, ViewServerInMemoryTopicDefinitions } from "./index";

export type ViewServerInMemoryTestingInstance<Topics extends ViewServerInMemoryTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

const toRuntimeCoreInternalOptions = <const Topics extends ViewServerInMemoryTopicDefinitions>(
  input: ViewServerInMemoryOptions<Topics>,
): ViewServerRuntimeCoreInternalOptionsFor<Topics> => ({
  ...(input.groupedIncrementalAdmissionLimits === undefined
    ? {}
    : { groupedIncrementalAdmissionLimits: input.groupedIncrementalAdmissionLimits }),
  ...(input.subscriptionQueueCapacity === undefined
    ? {}
    : { subscriptionQueueCapacity: input.subscriptionQueueCapacity }),
  ...(input.healthRefreshCadence === undefined
    ? {}
    : { healthRefreshCadence: input.healthRefreshCadence }),
});

const toInMemoryTestingInstance = <const Topics extends ViewServerInMemoryTopicDefinitions>(
  runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>,
): ViewServerInMemoryTestingInstance<Topics> => ({
  client: runtimeCore.internalClient,
  close: runtimeCore.close,
  liveClient: {
    close: runtimeCore.liveClient.close,
    health: runtimeCore.liveClient.health,
    subscribe: runtimeCore.internalLiveClient.subscribeInternal,
    subscribeRuntime: runtimeCore.internalLiveClient.subscribeRuntimeInternal,
    subscribeHealth: runtimeCore.liveClient.subscribeHealth,
    subscribeHealthSummary: runtimeCore.liveClient.subscribeHealthSummary,
  },
});

export const makeInMemoryViewServerTesting: <
  const Topics extends ViewServerInMemoryTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  input: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<ViewServerInMemoryTestingInstance<Topics>, ViewServerRuntimeError> = Effect.fn(
  "ViewServerInMemory.testing.make",
)(
  <
    const Topics extends ViewServerInMemoryTopicDefinitions,
    const Regions extends RuntimeRegions,
    const GrpcClients extends GrpcRuntimeClients,
  >(
    config: ViewServerConfig<Topics, Regions, GrpcClients>,
    input: ViewServerInMemoryOptions<Topics>,
  ) =>
    makeViewServerRuntimeCoreInternal(config, toRuntimeCoreInternalOptions(input)).pipe(
      Effect.map(toInMemoryTestingInstance),
    ),
);

export const createInMemoryViewServerTesting = <
  const Topics extends ViewServerInMemoryTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryTestingInstance<Topics> =>
  Effect.runSync(makeInMemoryViewServerTesting(config, options));
