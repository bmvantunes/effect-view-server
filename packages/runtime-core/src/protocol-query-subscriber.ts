import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import type {
  TopicDefinitions,
  ValidatedRuntimeQuery,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import type { Effect } from "effect";

export type ViewServerRuntimeCoreProtocolQuerySubscriber<Topics extends TopicDefinitions> = {
  readonly subscribeProtocolQuery: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: ValidatedRuntimeQuery,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};
