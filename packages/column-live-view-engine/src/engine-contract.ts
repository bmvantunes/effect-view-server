import type {
  DeltaEvent,
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactPatch,
  ExactRawQuery,
  GroupedQuery,
  GroupedResult,
  LiveQueryRow,
  LiveQueryResult,
  PickRawFields,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  StringFieldKey,
  TopicRow,
  ValidateLiveQuery,
} from "@effect-view-server/config";
import type { Effect, Schema, Stream } from "effect";
import type { ColumnLiveViewEngineHealth } from "./engine-health";
import type { ColumnLiveViewEngineError, EngineClosedError } from "./engine-errors";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";

export type DecodableTopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema & Schema.Codec<object, unknown, never, never>;
    readonly key: string;
  }
>;

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

type IsUnion<Value> = [Value] extends [UnionToIntersection<Value>] ? false : true;

type EngineQueryKind = "live" | "raw" | "grouped";

type ExactEngineQuery<Row, Query, Kind extends EngineQueryKind> = Kind extends "raw"
  ? ExactRawQuery<Row, Query>
  : Kind extends "grouped"
    ? ExactGroupedQuery<Row, Query>
    : ExactLiveQuery<Row, Query>;

type ExactEngineQueryForTopics<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
  Kind extends EngineQueryKind,
> = (
  Topic extends Extract<keyof Topics, string>
    ? (query: ExactEngineQuery<TopicRow<Topics, Topic>, Query, Kind>) => void
    : never
) extends (query: infer Intersection) => void
  ? Intersection
  : never;

type TopicsRejectingEngineQuery<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
  Kind extends EngineQueryKind,
> =
  Topic extends Extract<keyof Topics, string>
    ? Query extends ExactEngineQuery<TopicRow<Topics, Topic>, Query, Kind>
      ? never
      : Topic
    : never;

type RejectInvalidEngineTopicUnionQuery<Topic, RejectingTopics> =
  true extends IsUnion<Topic> ? ([RejectingTopics] extends [never] ? unknown : never) : unknown;

type ExactEngineQueryInputForTopic<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
  Kind extends EngineQueryKind,
> = Query &
  ExactEngineQueryForTopics<Topics, Topic, Query, Kind> &
  RejectInvalidEngineTopicUnionQuery<
    Topic,
    TopicsRejectingEngineQuery<Topics, Topic, Query, Kind>
  > &
  ValidateLiveQuery<Query>;

export type ExactEngineLiveQueryInputForTopic<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactEngineQueryInputForTopic<Topics, Topic, Query, "live">;

export type ExactEngineRawQueryInputForTopic<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactEngineQueryInputForTopic<Topics, Topic, Query, "raw">;

export type ExactEngineGroupedQueryInputForTopic<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactEngineQueryInputForTopic<Topics, Topic, Query, "grouped">;

type ValidateEngineTopics<Topics extends DecodableTopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema & Schema.Codec<object, unknown, never, never>;
    readonly key: infer Key extends string;
  }
    ? {
        readonly schema: S;
        readonly key: Key & StringFieldKey<RowFromSchema<S>>;
      }
    : never;
};

export type ColumnLiveViewEngineConfig<Topics extends DecodableTopicDefinitions> = {
  readonly topics: Topics & ValidateEngineTopics<Topics>;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

export type ColumnLiveViewEngineInternalConfig<Topics extends DecodableTopicDefinitions> = {
  readonly topics: Topics;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

export type ColumnLiveViewEngineEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ColumnLiveViewSubscription<Row> = {
  readonly events: Stream.Stream<ColumnLiveViewEngineEvent<Row>>;
  readonly close: () => Effect.Effect<void, never>;
};

export type ColumnLiveViewTerminalObserver = {
  readonly onQueryRegistered: (queryId: string) => Effect.Effect<void, never>;
  readonly onTerminalOccurrence: (event: StatusEvent) => Effect.Effect<void, never>;
  readonly onTerminalReady: (event: StatusEvent) => Effect.Effect<void, never>;
};

type EngineSnapshot<Topics extends DecodableTopicDefinitions> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineGroupedQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineRawQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
};

type EngineSubscribe<Topics extends DecodableTopicDefinitions> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineGroupedQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineRawQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
};

type EngineSubscribeObserved<Topics extends DecodableTopicDefinitions> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends
      | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
      | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
    observer: ColumnLiveViewTerminalObserver,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineGroupedQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
    observer: ColumnLiveViewTerminalObserver,
  ): Effect.Effect<
    ColumnLiveViewSubscription<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, NoInfer<Topic>>>,
  >(
    topic: Topic,
    query: ExactEngineRawQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
    observer: ColumnLiveViewTerminalObserver,
  ): Effect.Effect<
    ColumnLiveViewSubscription<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
};

export type AnyTopicRow<Topics extends DecodableTopicDefinitions> = TopicRow<
  Topics,
  Extract<keyof Topics, string>
>;

type TopicRowWithStorageKey<Row extends object> = {
  readonly storageKey: string;
  readonly row: Row;
};

type DecodedTopicRowWithStorageKey = {
  readonly storageKey: string;
  readonly row: object;
};

export type ColumnLiveViewEngine<Topics extends DecodableTopicDefinitions> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly patch: <
    Topic extends Extract<keyof Topics, string>,
    const Patch extends Partial<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    key: string,
    patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly snapshot: EngineSnapshot<Topics>;
  readonly subscribe: EngineSubscribe<Topics>;
  readonly subscribeRuntime: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: unknown,
  ) => Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError>;
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
  readonly reset: () => Effect.Effect<void, EngineClosedError>;
  readonly close: () => Effect.Effect<void, never>;
};

export type ColumnLiveViewEngineInternal<Topics extends DecodableTopicDefinitions> =
  ColumnLiveViewEngine<Topics> & {
    readonly deleteStorageKey: (
      topic: Extract<keyof Topics, string>,
      key: string,
      partitionKey: string,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
    readonly snapshotRuntime: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      query: unknown,
    ) => Effect.Effect<LiveQueryResult<object>, ColumnLiveViewEngineError>;
    readonly subscribeObserved: EngineSubscribeObserved<Topics>;
    readonly subscribeRuntimeObserved: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      query: unknown,
      observer: ColumnLiveViewTerminalObserver,
    ) => Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError>;
    readonly subscribeRuntimePartitioned: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      query: unknown,
      partition: ColumnLiveViewEngineQueryPartition,
    ) => Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError>;
    readonly subscribeRuntimeObservedPartitioned: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      query: unknown,
      partition: ColumnLiveViewEngineQueryPartition,
      observer: ColumnLiveViewTerminalObserver,
    ) => Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError>;
    readonly patchDecodedFields: (
      topic: Extract<keyof Topics, string>,
      key: string,
      patch: object,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
    readonly publishManyDecodedRows: (
      topic: Extract<keyof Topics, string>,
      rows: ReadonlyArray<object>,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
    readonly publishManyDecodedRowsWithStorageKeys: (
      topic: Extract<keyof Topics, string>,
      rows: ReadonlyArray<DecodedTopicRowWithStorageKey>,
      partitionKey?: string,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
    readonly publishManyWithStorageKeys: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      rows: ReadonlyArray<TopicRowWithStorageKey<TopicRow<Topics, Topic>>>,
      partitionKey?: string,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  };
