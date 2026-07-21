import type { Config, Effect, Schema } from "effect";
import type { ViewServerHealth } from "./health-contract";
import type { ExactLiveQueryInputForTopic } from "./source-query-contract";
import type {
  ExactPatch,
  GroupedQuery,
  LiveQueryResult,
  LiveQueryRow,
  RawQuery,
  RowSchema,
  TopicDefinitions,
  TopicRow,
} from "./topic-contract";

export type ViewServerBackpressureError = {
  readonly _tag: "ViewServerBackpressureError";
  readonly code: "BackpressureExceeded";
  readonly message: string;
  readonly topic?: string;
  readonly queryId?: string;
  readonly queuedEvents?: number;
  readonly maxQueueDepth?: number;
};

export type ViewServerRuntimeError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerRuntimeError";
      readonly code:
        | "InvalidTopic"
        | "InvalidRow"
        | "InvalidQuery"
        | "UnsupportedQuery"
        | "SnapshotStale"
        | "RuntimeUnavailable"
        | "RuntimeResetFailed";
      readonly message: string;
      readonly topic?: string;
    };

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
      readonly key: string;
    }
  >;

type ViewServerRuntimeDecodedMutationForTopic<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> =
  | {
      readonly _tag: "CheckMutationAllowed";
      readonly topic: Topic;
    }
  | {
      readonly _tag: "PublishDecodedRows";
      readonly topic: Topic;
      readonly rows: ReadonlyArray<Topics[Topic]["schema"]["Type"]>;
    }
  | {
      readonly _tag: "PatchDecodedFields";
      readonly topic: Topic;
      readonly key: string;
      readonly patch: Partial<Topics[Topic]["schema"]["Type"]>;
    }
  | {
      readonly _tag: "DeleteDecodedRow";
      readonly topic: Topic;
      readonly key: string;
    };

export type ViewServerRuntimeDecodedMutation<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly [Topic in Extract<keyof Topics, string>]: ViewServerRuntimeDecodedMutationForTopic<
    Topics,
    Topic
  >;
}[Extract<keyof Topics, string>];

type ExactDecodedValueMember<Allowed, Value> = Allowed extends Allowed
  ? [Value] extends [Allowed]
    ? Exclude<keyof Value, keyof Allowed> extends never
      ? Value
      : never
    : never
  : never;

type KeysOfUnion<Value> = Value extends Value ? keyof Value : never;

type DecodedValueMemberIsExact<Allowed, Value> = [ExactDecodedValueMember<Allowed, Value>] extends [
  never,
]
  ? false
  : true;

type DecodedValueExactness<Allowed, Value> = Value extends Value
  ? DecodedValueMemberIsExact<Allowed, Value>
  : never;

type ExactDecodedValue<Allowed, Value> =
  Exclude<KeysOfUnion<Value>, KeysOfUnion<Allowed>> extends never
    ? false extends DecodedValueExactness<Allowed, Value>
      ? never
      : Value
    : never;

type ExactDecodedRows<Row, Rows extends ReadonlyArray<unknown>> = [Rows[number]] extends [
  ExactDecodedValue<Row, Rows[number]>,
]
  ? Rows
  : never;

type DecodedPatch<Row> = Row extends Row ? Partial<Row> : never;

type ExactDecodedPatch<Row, Patch> = [Patch] extends [ExactDecodedValue<DecodedPatch<Row>, Patch>]
  ? Patch
  : never;

type ExactDecodedMutation<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Mutation extends ViewServerRuntimeDecodedMutation<Topics>,
> = Mutation extends {
  readonly _tag: "PublishDecodedRows";
  readonly topic: infer Topic extends Extract<keyof Topics, string>;
  readonly rows: infer Rows extends ReadonlyArray<unknown>;
}
  ? {
      readonly rows: ExactDecodedRows<Topics[Topic]["schema"]["Type"], Rows>;
    }
  : Mutation extends {
        readonly _tag: "PatchDecodedFields";
        readonly topic: infer Topic extends Extract<keyof Topics, string>;
        readonly patch: infer Patch;
      }
    ? {
        readonly patch: ExactDecodedPatch<Topics[Topic]["schema"]["Type"], Patch>;
      }
    : unknown;

type ViewServerRuntimeTrustedDecodedMutation<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
> =
  | {
      readonly _tag: "PublishDecodedRows";
      readonly topic: Topic;
      readonly rows: ReadonlyArray<Topics[NoInfer<Topic>]["schema"]["Type"]>;
    }
  | {
      readonly _tag: "PatchDecodedFields";
      readonly topic: Topic;
      readonly key: string;
      readonly patch: Partial<Topics[NoInfer<Topic>]["schema"]["Type"]>;
    };

export type ViewServerRuntimeDecodedMutationClient<
  Topics extends ViewServerRuntimeTopicDefinitions,
> = {
  readonly execute: {
    <const Mutation extends ViewServerRuntimeDecodedMutation<Topics>>(
      mutation: Mutation & ExactDecodedMutation<Topics, Mutation>,
    ): Effect.Effect<void, ViewServerRuntimeError>;
    <const Topic extends Extract<keyof Topics, string>>(
      mutation: ViewServerRuntimeTrustedDecodedMutation<Topics, Topic>,
      trust: typeof viewServerRuntimeDecodedMutationTrust,
    ): Effect.Effect<void, ViewServerRuntimeError>;
  };
};

export const viewServerRuntimeDecodedMutationTrust: unique symbol = Symbol(
  "ViewServerRuntimeDecodedMutationTrust",
);

export type ViewServerTransportError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerTransportError";
      readonly code: "TransportError" | "SubscriptionClosed";
      readonly message: string;
      readonly topic?: string;
      readonly queryId?: string;
    };

type RuntimeSnapshot<Topics extends object> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends
    | RawQuery<TopicRow<Topics, NoInfer<Topic>>>
    | GroupedQuery<TopicRow<Topics, NoInfer<Topic>>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

export type ViewServerRuntimeClient<Topics extends object> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends Extract<keyof Topics, string>, const Patch>(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimeSnapshot<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly reset: () => Effect.Effect<void, ViewServerRuntimeError>;
};

export type RuntimeEnvironmentConfig = {
  readonly websocketPort: Config.Config<number>;
};
