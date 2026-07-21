import type { ExactGroupedQuery, GroupedQuery, GroupedResult } from "./grouped-query-contract";
import type { ExactRawQuery, PickRawFields, RawQuery } from "./raw-query-contract";

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

type ExactLiveQueryMember<Row, Query> = ExactRawQuery<Row, Query> | ExactGroupedQuery<Row, Query>;

type InvalidExactLiveQueryMember<Row, Query> = Query extends unknown
  ? [ExactLiveQueryMember<Row, Query>] extends [never]
    ? Query
    : never
  : never;

type ExactLiveQueryMembers<Row, Query> = Query extends unknown
  ? ExactLiveQueryMember<Row, Query>
  : never;

export type ExactLiveQuery<Row, Query> = [InvalidExactLiveQueryMember<Row, Query>] extends [never]
  ? ExactLiveQueryMembers<Row, Query>
  : never;

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

type ValidateLiveQueryMember<Query> = RejectAggregateAliasCollisions<Query> &
  RejectBroadAggregateAliases<Query> &
  RejectEmptyAggregates<Query> &
  RejectDangerousAggregateAliases<Query>;

type InvalidLiveQueryMember<Query> = Query extends unknown
  ? Query extends Query & ValidateLiveQueryMember<Query>
    ? never
    : Query
  : never;

export type ValidateLiveQuery<Query> = [InvalidLiveQueryMember<Query>] extends [never]
  ? unknown
  : never;

export type ExactLiveQueryInput<Row, Query> = Query &
  NoInfer<
    ExactLiveQuery<Row, Query> &
      ValidateLiveQuery<Query> & {
        readonly routeBy?: never;
      }
  >;
