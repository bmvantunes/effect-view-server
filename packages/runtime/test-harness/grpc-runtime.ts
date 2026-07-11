import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerRuntimeClient,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Clock, Effect, Exit } from "effect";
import type { ViewServerGrpcHealthLedger } from "../src/grpc-health";
import { makeViewServerGrpcIngress } from "../src/grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "../src/grpc-lease-manager";
import { makeDefaultRuntimeDependencies } from "../src/runtime-dependencies";
import { resolveViewServerRuntimeOptions } from "../src/runtime-options";
import type {
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "../src/runtime-types";

type GrpcRuntimeHarnessInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions,
  Clients extends GrpcRuntimeClients,
> = {
  readonly config: ViewServerConfig<Topics, Regions, Clients>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, Clients>;
  readonly requestHealthRefresh?: Effect.Effect<void>;
};

const resolveGrpcRuntimeHarness = Effect.fn("ViewServerRuntime.test.grpc.harness.resolve")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
    const Clients extends GrpcRuntimeClients,
  >(input: GrpcRuntimeHarnessInput<Topics, Regions, Clients>) {
    const resolvedOptions = yield* resolveViewServerRuntimeOptions(
      input.config,
      input.grpc === undefined ? {} : { grpc: input.grpc },
    );
    const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
    const health = makeDefaultRuntimeDependencies<Topics>().makeGrpcHealthLedger(
      input.config,
      grpcOptions,
    );
    return { grpcOptions, health };
  },
);

export const readGrpcHealthOverlay = Effect.fn("ViewServerRuntime.test.grpc.healthOverlay.read")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    client: ViewServerRuntimeClient<Topics>,
    health: ViewServerGrpcHealthLedger<Topics>,
    nowMillis: number,
  ) {
    return health.healthOverlay(yield* client.health(), nowMillis);
  },
);

export const readGrpcHealthOverlayNow = Effect.fn(
  "ViewServerRuntime.test.grpc.healthOverlay.readNow",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  client: ViewServerRuntimeClient<Topics>,
  health: ViewServerGrpcHealthLedger<Topics>,
) {
  const nowMillis = yield* Clock.currentTimeMillis;
  return health.healthOverlay(yield* client.health(), nowMillis);
});

export const makeMaterializedGrpcRuntimeHarness = Effect.fn(
  "ViewServerRuntime.test.grpc.materializedHarness.make",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(input: {
  readonly config: ViewServerConfig<Topics, RuntimeRegions, Clients>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, Clients>;
  readonly requestHealthRefresh?: Effect.Effect<void>;
}) {
  const { grpcOptions, health } = yield* resolveGrpcRuntimeHarness(input);
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(input.config, {});
  const ingress = yield* makeViewServerGrpcIngress(
    input.config,
    runtimeCore.internalClient,
    input.requestHealthRefresh ?? Effect.void,
    grpcOptions,
    health,
  ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  return {
    health,
    ingress,
    runtimeCore,
    close: ingress.close.pipe(Effect.ensuring(runtimeCore.close)),
  };
});

export const makeLeasedGrpcRuntimeHarness = Effect.fn(
  "ViewServerRuntime.test.grpc.leasedHarness.make",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(input: {
  readonly config: ViewServerConfig<Topics, RuntimeRegions, Clients>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, Clients>;
  readonly requestHealthRefresh?: Effect.Effect<void>;
}) {
  const { grpcOptions, health } = yield* resolveGrpcRuntimeHarness(input);
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(input.config, {});
  const manager = yield* makeViewServerGrpcLeaseManager(
    input.config,
    runtimeCore.internalClient,
    runtimeCore.liveClient,
    runtimeCore.internalLiveClient,
    input.requestHealthRefresh ?? Effect.void,
    grpcOptions,
    health,
  ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  return {
    health,
    manager,
    runtimeCore,
    close: manager.close.pipe(Effect.ensuring(runtimeCore.close)),
  };
});
