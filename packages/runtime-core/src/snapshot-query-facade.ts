import type { DecodableTopicDefinitions } from "@effect-view-server/column-live-view-engine";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect } from "effect";

type RuntimeSnapshot = Effect.Effect<LiveQueryResult<object>, ViewServerRuntimeError>;

export type RuntimeCoreSnapshotQuerySubstrate<Topics extends DecodableTopicDefinitions> = {
  readonly snapshotQuery: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ) => RuntimeSnapshot;
  readonly requirePublicReadAllowed: (
    topic: Extract<keyof Topics, string>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
};

export type RuntimeCoreSnapshotQueryFacade<Topics extends DecodableTopicDefinitions> = {
  readonly snapshotInternal: ViewServerRuntimeClient<Topics>["snapshot"];
  readonly snapshot: ViewServerRuntimeClient<Topics>["snapshot"];
};

export const makeRuntimeCoreSnapshotQueryFacade = <Topics extends DecodableTopicDefinitions>(
  substrate: RuntimeCoreSnapshotQuerySubstrate<Topics>,
): RuntimeCoreSnapshotQueryFacade<Topics> => {
  function snapshotInternal<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  >;
  function snapshotInternal(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ): RuntimeSnapshot {
    return substrate.snapshotQuery(topic, query);
  }

  function snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  >;
  function snapshot(
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
  ): RuntimeSnapshot {
    const acquisition = substrate.snapshotQuery(topic, query);
    return substrate.requirePublicReadAllowed(topic).pipe(Effect.flatMap(() => acquisition));
  }

  return { snapshotInternal, snapshot };
};
