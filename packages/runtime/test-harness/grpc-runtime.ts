import type {
  GrpcRuntimeClients,
  ViewServerRuntimeClient,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Clock, Effect, Exit } from "effect";
import type { ViewServerGrpcHealthLedger } from "../src/grpc-health";
import { makeViewServerGrpcIngress } from "../src/grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "../src/grpc-lease-manager";
import type { ResolvedViewServerGrpcRuntimeOptions } from "../src/runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "../src/runtime-types";

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
  readonly config: ViewServerTopicConfig<Topics>;
  readonly grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>;
  readonly health: ViewServerGrpcHealthLedger<Topics>;
  readonly requestHealthRefresh?: Effect.Effect<void>;
}) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(input.config, {});
  const ingress = yield* makeViewServerGrpcIngress(
    input.config,
    runtimeCore.internalClient,
    input.requestHealthRefresh ?? Effect.void,
    input.grpcOptions,
    input.health,
  ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  return {
    health: input.health,
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
  readonly config: ViewServerTopicConfig<Topics>;
  readonly grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>;
  readonly health: ViewServerGrpcHealthLedger<Topics>;
  readonly requestHealthRefresh?: Effect.Effect<void>;
}) {
  const runtimeCore = yield* makeViewServerRuntimeCoreInternal(input.config, {});
  const manager = yield* makeViewServerGrpcLeaseManager(
    input.config,
    runtimeCore.internalClient,
    runtimeCore.liveClient,
    runtimeCore.internalLiveClient,
    input.requestHealthRefresh ?? Effect.void,
    input.grpcOptions,
    input.health,
  ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  return {
    health: input.health,
    manager,
    runtimeCore,
    close: manager.close.pipe(Effect.ensuring(runtimeCore.close)),
  };
});
