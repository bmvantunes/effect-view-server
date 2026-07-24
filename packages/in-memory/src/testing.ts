import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type { ViewServerRuntimeCoreServerLiveClient } from "@effect-view-server/runtime-core";
import {
  adaptRuntimeQuerySubscriber,
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalLiveClient,
  type ViewServerRuntimeCoreInternalOptionsFor,
  type ViewServerSourceRequirements,
} from "@effect-view-server/runtime-core/internal";
import { Effect } from "effect";
import type { ViewServerInMemoryOptions, ViewServerInMemoryTopicDefinitions } from "./index";

type SynchronousInMemoryTestingConfig<
  Topics extends ViewServerInMemoryTopicDefinitions,
  Regions extends RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients,
> = ViewServerConfig<Topics, Regions, GrpcClients> &
  ([ViewServerSourceRequirements<NoInfer<Topics>>] extends [never] ? unknown : never);

export type ViewServerInMemoryTestingInstance<Topics extends ViewServerInMemoryTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly serverLiveClient: ViewServerRuntimeCoreServerLiveClient<Topics>;
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

const makeInMemoryTestingLiveClient = <Topics extends ViewServerInMemoryTopicDefinitions>(
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
): ViewServerRuntimeLiveClient<Topics> => ({
  close: liveClient.close,
  health: liveClient.health,
  subscribe: (topic, query) => internalLiveClient.subscribeInternal(topic, query),
  subscribeRuntime: adaptRuntimeQuerySubscriber(internalLiveClient.subscribeRuntimeRoutedInternal),
  subscribeHealth: liveClient.subscribeHealth,
  subscribeHealthSummary: liveClient.subscribeHealthSummary,
  subscribeSourceHealth: liveClient.subscribeSourceHealth,
});

export const makeInMemoryViewServerTesting: <
  const Topics extends ViewServerInMemoryTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  input: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<
  ViewServerInMemoryTestingInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerInMemory.testing.make")(function* <
  const Topics extends ViewServerInMemoryTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  input: ViewServerInMemoryOptions<Topics>,
) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
    config,
    toRuntimeCoreInternalOptions(input),
  );
  const testingInstance: ViewServerInMemoryTestingInstance<Topics> = {
    client: runtimeCore.internalClient,
    close: runtimeCore.close,
    liveClient: makeInMemoryTestingLiveClient(
      runtimeCore.liveClient,
      runtimeCore.internalLiveClient,
    ),
    serverLiveClient: runtimeCore.serverLiveClient,
  };
  return testingInstance;
});

export const createInMemoryViewServerTesting = <
  const Topics extends ViewServerInMemoryTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: SynchronousInMemoryTestingConfig<Topics, Regions, GrpcClients>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryTestingInstance<Topics> =>
  Effect.runSync(makeInMemoryViewServerTesting(config, options));
