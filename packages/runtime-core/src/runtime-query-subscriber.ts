import type {
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@effect-view-server/client";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import type { Effect } from "effect";

export type RuntimeQuerySubscriberSubstrate<Topics extends TopicDefinitions> = (
  topic: Extract<keyof Topics, string>,
  query: Readonly<Record<string, unknown>>,
) => Effect.Effect<
  ViewServerLiveSubscription<object>,
  ViewServerRuntimeError | ViewServerTransportError
>;

export const adaptRuntimeQuerySubscriber = <Topics extends TopicDefinitions>(
  substrate: RuntimeQuerySubscriberSubstrate<Topics>,
): ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] => {
  function subscribeRuntime<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribeRuntime(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ): Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  > {
    return substrate(topic, query);
  }

  return subscribeRuntime;
};
