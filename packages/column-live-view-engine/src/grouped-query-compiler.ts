import type { GroupedQuery, GroupedResult } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  decodeGroupedQuery,
  decodeTypedGroupedQuery,
  type RuntimeGroupedQuery,
} from "./grouped-query-decoder";
import { evaluateGroupedRows } from "./grouped-query-evaluation";
import {
  makeGroupedQueryPlan,
  makeRuntimeGroupedQueryPlan,
  type GroupedQueryPlan,
} from "./grouped-query-plan";
import {
  ensureRawQueryCompilerMetadata,
  type RawQueryCompilerMetadata,
  prepareRuntimeRawQuery,
} from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import { groupedQueryResultSemantics } from "./query-result-semantics";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export type { RuntimeGroupedQuery };

export type CompiledGroupedQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly plan: GroupedQueryPlan<Row, ResultRow>;
  readonly cacheKey: string;
  readonly matches: (row: Row) => boolean;
  readonly evaluate: (store: TopicRowScan<Row>) => QueryEvaluation<RowObject>;
};

export const prepareGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.prepare")(
  function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
    topic: string,
    metadata: RawQueryCompilerMetadata<Row>,
    query: Query,
  ) {
    yield* ensureRawQueryCompilerMetadata(topic, metadata);
    const decoded = yield* decodeTypedGroupedQuery(topic, metadata, query);
    const rawFilter = yield* prepareRuntimeRawQuery(topic, metadata, {
      select: decoded.groupBy,
      ...(decoded.where === undefined ? {} : { where: decoded.where }),
    });
    const { matches } = rawFilter.plan.predicate;
    const plan = makeGroupedQueryPlan<RowObject, GroupedResult<Row, Query>>(
      decoded,
      metadata.valueSemantics,
      rawFilter.plan.queryCacheKey,
      () => groupedQueryResultSemantics<Row, Query>(metadata.valueSemantics, decoded),
    );
    return Object.freeze({
      plan,
      cacheKey: plan.cacheKey,
      matches,
      evaluate: (store: TopicRowScan<RowObject>) => evaluateGroupedRows(store, plan, matches),
    } satisfies CompiledGroupedQuery<RowObject, GroupedResult<Row, Query>>);
  },
);

export const prepareRuntimeGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.groupedQuery.prepareRuntime",
)(function* <Row extends RowObject>(
  topic: string,
  metadata: RawQueryCompilerMetadata<Row>,
  query: unknown,
) {
  yield* ensureRawQueryCompilerMetadata(topic, metadata);
  const decoded = yield* decodeGroupedQuery(topic, metadata, query);
  const rawFilter = yield* prepareRuntimeRawQuery(topic, metadata, {
    select: decoded.groupBy,
    ...(decoded.where === undefined ? {} : { where: decoded.where }),
  });
  const { matches } = rawFilter.plan.predicate;
  const plan = makeRuntimeGroupedQueryPlan(
    decoded,
    metadata.valueSemantics,
    rawFilter.plan.queryCacheKey,
  );
  return Object.freeze({
    plan,
    cacheKey: plan.cacheKey,
    matches,
    evaluate: (store: TopicRowScan<RowObject>) => evaluateGroupedRows(store, plan, matches),
  } satisfies CompiledGroupedQuery<RowObject, object>);
});

export const evaluateCompiledGroupedQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): QueryEvaluation<RowObject> => compiled.evaluate(store);
