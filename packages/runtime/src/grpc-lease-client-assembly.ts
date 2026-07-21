import type { ViewServerRuntimeClient } from "@effect-view-server/config";
import { makeGrpcLeaseClientFacade } from "./grpc-lease-client-facade";
import type { ViewServerGrpcLeaseManagerSubstrate } from "./grpc-lease-manager-substrate";
import { makeGrpcLeaseMutationFacade } from "./grpc-lease-mutation-facade";
import { makeGrpcLeaseSnapshotFacade } from "./grpc-lease-snapshot-facade";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const assembleGrpcLeaseClient = <Topics extends ViewServerRuntimeTopicDefinitions>(
  substrate: ViewServerGrpcLeaseManagerSubstrate<Topics>,
): ViewServerRuntimeClient<Topics> => {
  const snapshot = makeGrpcLeaseSnapshotFacade<Topics>(
    substrate.runtimeClient.snapshotRuntimeInternal,
    substrate.requirePublicReadAllowed,
  );
  const mutations = makeGrpcLeaseMutationFacade<Topics>(
    substrate.runtimeClient,
    substrate.requirePublicMutationAllowed,
    substrate.requirePublicResetAllowed,
  );
  return makeGrpcLeaseClientFacade<Topics>({
    mutations,
    snapshot,
    health: substrate.runtimeClient.health,
  });
};
