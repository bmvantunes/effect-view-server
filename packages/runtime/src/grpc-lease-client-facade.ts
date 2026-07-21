import type { ViewServerRuntimeClient } from "@effect-view-server/config";
import type { GrpcLeaseMutationFacade } from "./grpc-lease-mutation-facade";
import type { GrpcLeaseSnapshotFacade } from "./grpc-lease-snapshot-facade";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const makeGrpcLeaseClientFacade = <Topics extends ViewServerRuntimeTopicDefinitions>(input: {
  readonly mutations: GrpcLeaseMutationFacade<Topics>;
  readonly snapshot: GrpcLeaseSnapshotFacade<Topics>;
  readonly health: ViewServerRuntimeClient<Topics>["health"];
}): ViewServerRuntimeClient<Topics> => ({
  ...input.mutations,
  snapshot: input.snapshot,
  health: input.health,
});
