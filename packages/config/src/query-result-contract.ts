import type { ExactGroupedQuery, GroupedQuery, GroupedResult } from "./grouped-query-contract";
import type { ExactRawQuery, PickRawFields, RawQuery } from "./raw-query-contract";

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

export type ExactLiveQuery<Row, Query> = Query extends {
  readonly groupBy: ReadonlyArray<unknown>;
}
  ? ExactGroupedQuery<Row, Query>
  : ExactRawQuery<Row, Query>;

export type LiveQueryRow<Row, Query> = Query extends { readonly groupBy: ReadonlyArray<unknown> }
  ? GroupedResult<Row, Query>
  : PickRawFields<Row, Query>;

export type LiveQueryResult<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: "loading" | "ready" | "stale" | "closed" | "error";
  readonly statusCode?:
    | "Ready"
    | "SnapshotStale"
    | "SubscriptionClosed"
    | "TransportError"
    | "BackpressureExceeded"
    | "InvalidTopic"
    | "InvalidRow"
    | "InvalidQuery"
    | "UnsupportedQuery"
    | "RuntimeUnavailable"
    | "RuntimeResetFailed"
    | undefined;
  readonly message?: string | undefined;
};

type AggregateAliases<Query> = Query extends {
  readonly aggregates: infer Aggs;
}
  ? Extract<keyof Aggs, string>
  : never;

type GroupedFields<Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer Field>;
}
  ? Extract<Field, string>
  : never;

type RejectAggregateAliasCollisions<Query> =
  Extract<AggregateAliases<Query>, GroupedFields<Query>> extends never
    ? unknown
    : { readonly aggregates: never };

type RejectBroadAggregateAliases<Query> = Query extends {
  readonly aggregates: infer Aggs;
}
  ? string extends keyof Aggs
    ? { readonly aggregates: never }
    : unknown
  : unknown;

type RejectEmptyAggregates<Query> = Query extends {
  readonly aggregates: infer Aggs;
}
  ? keyof Aggs extends never
    ? { readonly aggregates: never }
    : unknown
  : unknown;

type DangerousAggregateAlias = "__proto__" | "prototype" | "constructor";

type RejectDangerousAggregateAliases<Query> =
  Extract<AggregateAliases<Query>, DangerousAggregateAlias> extends never
    ? unknown
    : { readonly aggregates: never };

export type ValidateLiveQuery<Query> = RejectAggregateAliasCollisions<Query> &
  RejectBroadAggregateAliases<Query> &
  RejectEmptyAggregates<Query> &
  RejectDangerousAggregateAliases<Query>;

export type ExactLiveQueryInput<Row, Query> = Query &
  ExactLiveQuery<Row, Query> &
  ValidateLiveQuery<Query>;
