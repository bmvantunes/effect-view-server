import type { LiveQueryResult } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  evaluateRawQueryResult,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
} from "./active-query";
import {
  evaluateCompiledGroupedQuery,
  prepareRuntimeGroupedQuery,
  type CompiledGroupedQuery,
} from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import { prepareRuntimeRawQuery, type CompiledRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";
import { topicStoreQueryResources, type TopicStore } from "./topic-store-state";

type RowObject = object;

export const prepareTopicStoreRuntimeRawQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.prepareRuntime",
)(function* (store: TopicStore, query: unknown, partition?: ColumnLiveViewEngineQueryPartition) {
  return yield* prepareRuntimeRawQuery(
    store.topic,
    topicStoreQueryResources(store).metadata,
    query,
    partition,
  );
});

export const prepareTopicStoreRuntimeGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.prepareRuntime",
)(function* (store: TopicStore, query: unknown, partition?: ColumnLiveViewEngineQueryPartition) {
  return yield* prepareRuntimeGroupedQuery(
    store.topic,
    topicStoreQueryResources(store).metadata,
    query,
    partition,
  );
});

export const evaluateTopicStoreRawQueryResult = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): LiveQueryResult<ResultRow> =>
  evaluateRawQueryResult(topicStoreQueryResources(store).queryInterface, compiled);

export const evaluateTopicStoreGroupedQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): QueryEvaluation<RowObject> =>
  evaluateCompiledGroupedQuery(topicStoreQueryResources(store).queryInterface, compiled);

export const acquireTopicStoreRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
) {
  const { activeQueries, queryInterface } = topicStoreQueryResources(store);
  return yield* acquireRawQueryExecution(queryInterface, activeQueries, compiled);
});

export const releaseTopicStoreRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseRawQueryExecution(topicStoreQueryResources(store).activeQueries, compiled);

export const acquireTopicStoreMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.materialized.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
  groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits,
) {
  const { activeQueries, queryInterface } = topicStoreQueryResources(store);
  return yield* acquireMaterializedQueryExecution(
    queryInterface,
    activeQueries,
    compiled.cacheKey,
    compiled.plan.resultSemantics,
    (releaseRetainedChanges) =>
      makeIncrementalGroupedQueryExecution(
        queryInterface,
        compiled,
        releaseRetainedChanges,
        groupedIncrementalAdmissionLimits,
      ),
    compiled.partitionKey,
  );
});

export const releaseTopicStoreMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(
    topicStoreQueryResources(store).activeQueries,
    compiled.cacheKey,
  );
