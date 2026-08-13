import type {
  DeltaEvent,
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@effect-view-server/config";
import type { Effect, Stream } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import type {
  SourceHealthForDefinition,
  SourceHealthResultForDefinition,
  SourceRouteForDefinition,
} from "@effect-view-server/source-adapter";

type RowWithKey<Row, Key extends string> = string extends Key
  ? Row
  : Row extends { readonly id: string }
    ? Row & { readonly id: Key }
    : Row;

type TopicCanChangeCardinality<Topic extends string> = Topic extends
  | typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
  | typeof VIEW_SERVER_HEALTH_TOPIC
  ? false
  : true;

type TopicSnapshotEvent<Row, Topic extends string, Key extends string> = Omit<
  SnapshotEvent<Row>,
  "topic" | "keys" | "rows" | "totalRows"
> & {
  readonly topic: Topic;
  readonly keys: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
    ? readonly ["summary"]
    : ReadonlyArray<Key>;
  readonly rows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
    ? readonly [RowWithKey<Row, Key>]
    : ReadonlyArray<Row>;
  readonly totalRows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC ? 1 : number;
};

type TopicInsertOperation<Row, Topic extends string, Key extends string> =
  TopicCanChangeCardinality<Topic> extends true
    ? Key extends string
      ? {
          readonly type: "insert";
          readonly key: Key;
          readonly row: RowWithKey<Row, Key>;
          readonly index: number;
        }
      : never
    : never;

type TopicRemoveOperation<Topic extends string, Key extends string> =
  TopicCanChangeCardinality<Topic> extends true
    ? {
        readonly type: "remove";
        readonly key: Key;
      }
    : never;

type TopicDeltaOperation<Row, Topic extends string, Key extends string> =
  | TopicInsertOperation<Row, Topic, Key>
  | (Key extends string
      ? {
          readonly type: "update";
          readonly key: Key;
          readonly row: RowWithKey<Row, Key>;
          readonly index: number;
        }
      : never)
  | {
      readonly type: "move";
      readonly key: Key;
      readonly fromIndex: number;
      readonly toIndex: number;
    }
  | TopicRemoveOperation<Topic, Key>;

type TopicDeltaEvent<Row, Topic extends string, Key extends string> = Omit<
  DeltaEvent<Row>,
  "topic" | "operations" | "totalRows"
> & {
  readonly topic: Topic;
  readonly operations: ReadonlyArray<TopicDeltaOperation<Row, Topic, Key>>;
  readonly totalRows: Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC ? 1 : number;
};

type TopicStatusEvent<Topic extends string> = StatusEvent & {
  readonly topic: Topic;
};

export type ViewServerStatusEvent<Topic extends string = string> = TopicStatusEvent<Topic>;

export type ViewServerLiveEvent<Row, Topic extends string = string, Key extends string = string> =
  | TopicSnapshotEvent<Row, Topic, Key>
  | TopicDeltaEvent<Row, Topic, Key>
  | TopicStatusEvent<Topic>;

export type ViewServerLiveSubscription<
  Row,
  Topic extends string = string,
  Key extends string = string,
> = {
  readonly events: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

type TopicSourceDefinition<
  Topics extends TopicDefinitions,
  Topic extends keyof Topics,
> = Topics[Topic] extends { readonly source: infer Source } ? Source : never;

export type ViewServerSourceOwnedTopic<Topics extends TopicDefinitions> = Extract<
  {
    readonly [Topic in keyof Topics]: TopicSourceDefinition<Topics, Topic> extends never
      ? never
      : Topic;
  }[keyof Topics],
  string
>;

type RejectExtraKeys<Candidate, Expected> = {
  readonly [Key in Exclude<keyof Candidate, keyof Expected>]: never;
};

type ExactSourceRoute<Route, Candidate> = Candidate extends Route
  ? Candidate & RejectExtraKeys<Candidate, Route>
  : never;

type SourceHealthInputShapeForDefinition<
  Definition,
  Row extends object,
  Topic extends string,
  Candidate,
> = Definition extends unknown
  ? SourceHealthInputShapeForLifecycle<
      Definition,
      Row,
      Topic,
      Candidate,
      Extract<
        Definition extends {
          readonly lifecycle: infer DefinitionLifecycle;
        }
          ? DefinitionLifecycle
          : never,
        "materialized" | "leased"
      >
    >
  : never;

type SourceHealthInputShapeForLifecycle<
  Definition,
  Row extends object,
  Topic extends string,
  Candidate,
  Lifecycle extends "materialized" | "leased",
> = Lifecycle extends "leased"
  ? {
      readonly topic: Topic;
      readonly routeBy: Candidate extends { readonly routeBy: infer Route }
        ? ExactSourceRoute<SourceRouteForDefinition<Definition, Row>, Route>
        : SourceRouteForDefinition<Definition, Row>;
    }
  : {
      readonly topic: Topic;
      readonly routeBy?: never;
    };

type SourceHealthInputShape<
  Topics extends TopicDefinitions,
  Topic extends ViewServerSourceOwnedTopic<Topics>,
  Candidate,
> = SourceHealthInputShapeForDefinition<
  TopicSourceDefinition<Topics, Topic>,
  TopicRow<Topics, Topic>,
  Topic,
  Candidate
>;

export type ViewServerSourceHealthForTopic<
  Topics extends TopicDefinitions,
  Topic extends ViewServerSourceOwnedTopic<Topics>,
> = SourceHealthForDefinition<TopicSourceDefinition<Topics, Topic>, TopicRow<Topics, Topic>>;

export type ViewServerSourceHealthResultForTopic<
  Topics extends TopicDefinitions,
  Topic extends ViewServerSourceOwnedTopic<Topics>,
> = SourceHealthResultForDefinition<TopicSourceDefinition<Topics, Topic>, TopicRow<Topics, Topic>>;

export type ViewServerSourceHealthSubscription<Result> = {
  readonly events: Stream.Stream<Result, ViewServerRuntimeError | ViewServerTransportError>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

type IsUnion<Value, Whole = Value> = Value extends Whole
  ? [Whole] extends [Value]
    ? false
    : true
  : never;

type ExactSourceHealthInput<Expected, Candidate> = Expected extends unknown
  ? Candidate & NoInfer<Expected & RejectExtraKeys<Candidate, Expected>>
  : never;

export type ViewServerSourceHealthInputForTopic<
  Topics extends TopicDefinitions,
  Topic extends ViewServerSourceOwnedTopic<Topics>,
  Candidate,
> =
  IsUnion<Topic> extends true
    ? never
    : ExactSourceHealthInput<SourceHealthInputShape<Topics, Topic, Candidate>, Candidate>;

export type ViewServerSourceHealthSubscriber<
  Topics extends TopicDefinitions,
  Error = ViewServerRuntimeError | ViewServerTransportError,
> = <
  const Input extends {
    readonly topic: ViewServerSourceOwnedTopic<Topics>;
  },
>(
  input: ViewServerSourceHealthInputForTopic<Topics, Input["topic"], Input>,
) => Effect.Effect<
  ViewServerSourceHealthSubscription<ViewServerSourceHealthResultForTopic<Topics, Input["topic"]>>,
  Error
>;

type ViewServerQuerySubscriber<Topics extends TopicDefinitions, EraseRow extends boolean> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
) => Effect.Effect<
  ViewServerLiveSubscription<
    EraseRow extends true ? object : LiveQueryRow<TopicRow<Topics, Topic>, Query>
  >,
  ViewServerRuntimeError | ViewServerTransportError
>;

export type ViewServerLiveClient<Topics extends TopicDefinitions> = {
  readonly subscribe: ViewServerQuerySubscriber<Topics, false>;
  readonly subscribeHealthSummary: () => Effect.Effect<
    ViewServerLiveSubscription<
      ViewServerHealthSummaryRow<Topics>,
      typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      "summary"
    >,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeHealth: () => Effect.Effect<
    ViewServerLiveSubscription<
      ViewServerHealthTopicRow<Extract<keyof Topics, string>>,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeSourceHealth: ViewServerSourceHealthSubscriber<Topics>;
  readonly health: AtomRef.ReadonlyRef<ViewServerHealth<Topics>>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerRuntimeLiveClient<Topics extends TopicDefinitions> =
  ViewServerLiveClient<Topics> & {
    readonly subscribeRuntime: ViewServerQuerySubscriber<Topics, true>;
  };
