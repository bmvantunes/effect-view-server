import type { GroupedQuery, GroupedResult } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  decodeGroupedQuery,
  decodeTypedGroupedQuery,
  type RuntimeGroupedQuery,
} from "./grouped-query-decoder";
import { evaluateGroupedRows } from "./grouped-query-evaluation";
import {
  groupedQueryPlanCacheKey,
  makeGroupedQueryPlan,
  makeRuntimeGroupedQueryPlan,
  type GroupedQueryPlan,
} from "./grouped-query-plan";
import {
  ensureRawQueryCompilerMetadata,
  compileDecodedRuntimeRawQuery,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { rawQueryPlanIdentity, type RawQueryPlanIdentity } from "./raw-query-plan";
import type { QueryEvaluation } from "./query-result";
import { groupedQueryResultSemantics } from "./query-result-semantics";
import type { TopicRowScan } from "./row-scan";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";

type RowObject = object;

export type { RuntimeGroupedQuery };

export type CompiledGroupedQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly plan: GroupedQueryPlan<Row, ResultRow>;
  readonly cacheKey: string;
  readonly matches: (row: Row, storageKey?: string) => boolean;
  readonly ownedStorageKeys?: () => Iterable<string>;
  readonly partitionKey?: string;
  readonly evaluate: (store: TopicRowScan<Row>) => QueryEvaluation<RowObject>;
};

export type PreparedRuntimeGroupedQuery = {
  readonly cacheKey: string;
  readonly metadata: RawQueryCompilerMetadata;
  readonly partition?: ColumnLiveViewEngineQueryPartition;
  readonly query: RuntimeGroupedQuery;
  readonly rawFilterIdentity: RawQueryPlanIdentity;
};

export const prepareGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.prepare")(
  function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
    topic: string,
    metadata: RawQueryCompilerMetadata<Row>,
    query: Query,
  ) {
    yield* ensureRawQueryCompilerMetadata(topic, metadata);
    const decoded = yield* decodeTypedGroupedQuery(topic, metadata, query);
    const rawFilter = compileDecodedRuntimeRawQuery(metadata, {
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
  partition?: ColumnLiveViewEngineQueryPartition,
) {
  const prepared = yield* prepareRuntimeGroupedQueryAdmission(topic, metadata, query, partition);
  return compilePreparedRuntimeGroupedQuery(prepared);
});

export const prepareRuntimeGroupedQueryAdmission = Effect.fn(
  "ColumnLiveViewEngine.groupedQuery.prepareRuntimeAdmission",
)(function* <Row extends RowObject>(
  topic: string,
  metadata: RawQueryCompilerMetadata<Row>,
  query: unknown,
  partition?: ColumnLiveViewEngineQueryPartition,
) {
  yield* ensureRawQueryCompilerMetadata(topic, metadata);
  const decoded = yield* decodeGroupedQuery(topic, metadata, query);
  const rawFilterIdentity = rawQueryPlanIdentity(
    {
      select: decoded.groupBy,
      ...(decoded.where === undefined ? {} : { where: decoded.where }),
    },
    partition,
  );
  return Object.freeze({
    cacheKey: groupedQueryPlanCacheKey(decoded, rawFilterIdentity.queryCacheKey),
    metadata,
    ...(partition === undefined ? {} : { partition }),
    query: decoded,
    rawFilterIdentity,
  } satisfies PreparedRuntimeGroupedQuery);
});

export const compilePreparedRuntimeGroupedQuery = (
  prepared: PreparedRuntimeGroupedQuery,
): CompiledGroupedQuery<RowObject, RowObject> => {
  const { metadata, partition, query } = prepared;
  const rawFilter = compileDecodedRuntimeRawQuery(
    metadata,
    {
      select: query.groupBy,
      ...(query.where === undefined ? {} : { where: query.where }),
    },
    partition,
  );
  const { matches } = rawFilter.plan.predicate;
  const plan = makeRuntimeGroupedQueryPlan(
    query,
    metadata.valueSemantics,
    prepared.rawFilterIdentity.queryCacheKey,
  );
  return Object.freeze({
    plan,
    cacheKey: plan.cacheKey,
    matches,
    ...(partition === undefined ? {} : { ownedStorageKeys: partition.ownedStorageKeys }),
    ...(partition === undefined ? {} : { partitionKey: partition.key }),
    evaluate: (store: TopicRowScan<RowObject>) =>
      evaluateGroupedRows(store, plan, matches, partition?.ownedStorageKeys),
  } satisfies CompiledGroupedQuery<RowObject, RowObject>);
};

export const evaluateCompiledGroupedQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): QueryEvaluation<RowObject> => compiled.evaluate(store);
