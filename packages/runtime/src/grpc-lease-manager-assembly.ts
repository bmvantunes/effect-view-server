import type { ViewServerRuntimeLiveClient } from "@effect-view-server/client";
import type { ViewServerRuntimeClient } from "@effect-view-server/config";
import type { ViewServerRuntimeCoreProtocolQuerySubscriber } from "@effect-view-server/runtime-core/internal";
import type { Effect } from "effect";
import type { GrpcLeaseLiveClientFacade } from "./grpc-lease-live-client-assembly";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerGrpcLeaseManager<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly protocolQuerySubscriber: ViewServerRuntimeCoreProtocolQuerySubscriber<Topics>;
  readonly close: Effect.Effect<void>;
};

export const assembleViewServerGrpcLeaseManager = <
  Topics extends ViewServerRuntimeTopicDefinitions,
>(input: {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: GrpcLeaseLiveClientFacade<Topics>;
  readonly close: Effect.Effect<void>;
}): ViewServerGrpcLeaseManager<Topics> => ({
  client: input.client,
  liveClient: input.liveClient.liveClient,
  protocolQuerySubscriber: input.liveClient.protocolQuerySubscriber,
  close: input.close,
});
