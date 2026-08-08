import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import type {
  ViewServerRuntimeCoreInstance,
  ViewServerRuntimeCoreOptionsFor,
} from "./runtime-core-types";
import type { ViewServerSourceRequirements } from "./source-runtime";

export type { TopicDefinitions } from "@effect-view-server/config";
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
export type {
  RuntimeDependency,
  RuntimeDependencyStatus,
  RuntimeHeartbeat,
  RuntimeHeartbeatStatus,
  RuntimeSourceReportingSnapshot,
} from "./source-reporting";

type SynchronousRuntimeCoreConfig<Topics extends TopicDefinitions> = ViewServerConfig<Topics> &
  ([ViewServerSourceRequirements<NoInfer<Topics>>] extends [never] ? unknown : never);

export const makeViewServerRuntimeCore: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) => Effect.Effect<
  ViewServerRuntimeCoreInstance<Topics>,
  ViewServerRuntimeError,
  ViewServerSourceRequirements<Topics>
> = Effect.fn("ViewServerRuntimeCore.make")(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, input);
  return {
    client: runtimeCore.publicClient,
    liveClient: runtimeCore.publicLiveClient,
    serverLiveClient: runtimeCore.serverLiveClient,
    fatal: runtimeCore.fatal,
    close: runtimeCore.close,
    requestHealthRefresh: runtimeCore.requestHealthRefresh,
    refreshHealth: runtimeCore.refreshHealth,
    reporting: runtimeCore.reporting,
  };
});

export const createViewServerRuntimeCore = <const Topics extends TopicDefinitions>(
  config: SynchronousRuntimeCoreConfig<Topics>,
  options: ViewServerRuntimeCoreOptionsFor<Topics> = {},
): ViewServerRuntimeCoreInstance<Topics> =>
  Effect.runSync(makeViewServerRuntimeCore(config, options));
