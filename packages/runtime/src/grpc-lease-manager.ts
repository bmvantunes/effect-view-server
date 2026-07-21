import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type { GrpcRuntimeClients, ViewServerTopicConfig } from "@effect-view-server/config";
import type {
  ViewServerRuntimeCoreInternalClient,
  ViewServerRuntimeCoreInternalLiveClient,
} from "@effect-view-server/runtime-core/internal";
import { Effect, Semaphore } from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import { assembleGrpcLeaseClient } from "./grpc-lease-client-assembly";
import { assembleGrpcLeaseLiveClient } from "./grpc-lease-live-client-assembly";
import {
  assembleViewServerGrpcLeaseManager,
  type ViewServerGrpcLeaseManager,
} from "./grpc-lease-manager-assembly";
import {
  makeViewServerGrpcLeaseManagerSubstrate,
  type ViewServerGrpcGroupedKeyRetentionObserver,
} from "./grpc-lease-manager-substrate";
import { makeDefaultGrpcClient, type ViewServerGrpcClientFactory } from "./grpc-source-lifecycle";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./grpc-runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type { ViewServerGrpcLeaseManager } from "./grpc-lease-manager-assembly";

export const makeViewServerGrpcLeaseManager = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerTopicConfig<Topics>,
  runtimeClient: ViewServerRuntimeCoreInternalClient<Topics>,
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
  requestHealthRefresh: Effect.Effect<void>,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
  groupedKeyRetentionObserver?: ViewServerGrpcGroupedKeyRetentionObserver,
  acquisitionLock?: Semaphore.Semaphore,
): Effect.Effect<ViewServerGrpcLeaseManager<Topics>> =>
  makeViewServerGrpcLeaseManagerSubstrate(
    config,
    runtimeClient,
    liveClient,
    internalLiveClient,
    requestHealthRefresh,
    options,
    health,
    makeClient,
    groupedKeyRetentionObserver,
    acquisitionLock,
  ).pipe(
    Effect.map((substrate) =>
      assembleViewServerGrpcLeaseManager<Topics>({
        client: assembleGrpcLeaseClient(substrate),
        liveClient: assembleGrpcLeaseLiveClient<Topics>({
          liveClient: substrate.liveClient,
          subscribeRuntimeQuery: substrate.subscribeRuntimeQuery,
          close: substrate.close,
        }),
        close: substrate.close,
      }),
    ),
  );
