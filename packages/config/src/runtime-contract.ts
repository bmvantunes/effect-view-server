import type { Config, Effect, Schema } from "effect";
import type { ViewServerHealth } from "./health-contract";
import type { ExactLiveQueryInputForTopic } from "./source-query-contract";
import type {
  ExactPatch,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
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

type KeysOfUnion<Value> = Value extends Value ? keyof Value : never;

type ExactDecodedRows<Row, Rows extends ReadonlyArray<unknown>> = [Rows[number]] extends [Row]
  ? Exclude<KeysOfUnion<Rows[number]>, KeysOfUnion<Row>> extends never
    ? Rows
    : never
  : never;

type ExactDecodedPatch<Row, Patch> = [Patch] extends [Partial<Row>]
  ? Exclude<KeysOfUnion<Patch>, KeysOfUnion<Row>> extends never
    ? Patch
    : never
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

type ViewServerRuntimeTrustedDecodedPatchMutation<
  Topics extends ViewServerRuntimeTopicDefinitions,
> = {
  readonly _tag: "PatchDecodedFields";
  readonly topic: Extract<keyof Topics, string>;
  readonly key: string;
  readonly patch: Partial<Topics[Extract<keyof Topics, string>]["schema"]["Type"]>;
};

export type ViewServerRuntimeDecodedMutationClient<
  Topics extends ViewServerRuntimeTopicDefinitions,
> = {
  readonly execute: {
    <const Mutation extends ViewServerRuntimeDecodedMutation<Topics>>(
      mutation: Mutation & ExactDecodedMutation<Topics, Mutation>,
    ): Effect.Effect<void, ViewServerRuntimeError>;
    (
      mutation: ViewServerRuntimeTrustedDecodedPatchMutation<Topics>,
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
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
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
