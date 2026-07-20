import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type GrpcLeaseSnapshotFacade<Topics extends ViewServerRuntimeTopicDefinitions> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

export const makeGrpcLeaseSnapshotFacade = <Topics extends ViewServerRuntimeTopicDefinitions>(
  snapshotRuntimeInternal: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<LiveQueryResult<object>, ViewServerRuntimeError>,
  requirePublicReadAllowed: (
    topic: Extract<keyof Topics, string>,
  ) => Effect.Effect<void, ViewServerRuntimeError>,
): GrpcLeaseSnapshotFacade<Topics> => {
  function snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  >;
  function snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  > {
    const acquisition = snapshotRuntimeInternal(topic, query);
    return requirePublicReadAllowed(topic).pipe(Effect.flatMap(() => acquisition));
  }

  return snapshot;
};
