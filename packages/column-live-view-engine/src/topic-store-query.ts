import type { LiveQueryResult } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  acquirePreparedMaterializedQueryExecution,
  acquirePreparedRawQueryExecution,
  evaluateRawQueryResult,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecutionToken,
} from "./active-query";
import type { RawQueryExecutionReleaseToken } from "./active-query-contract";
import {
  compilePreparedRuntimeGroupedQuery,
  evaluateCompiledGroupedQuery,
  prepareRuntimeGroupedQueryAdmission,
  prepareRuntimeGroupedQuery,
  type CompiledGroupedQuery,
} from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import {
  prepareRuntimeRawQuery,
  prepareRuntimeRawQueryAdmission,
  type CompiledRawQuery,
} from "./raw-query-compiler";
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

export const acquireTopicStoreRuntimeRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquireRuntime",
)(function* (store: TopicStore, query: unknown, partition?: ColumnLiveViewEngineQueryPartition) {
  const { activeQueries, metadata, queryInterface } = topicStoreQueryResources(store);
  const prepared = yield* prepareRuntimeRawQueryAdmission(store.topic, metadata, query, partition);
  return yield* acquirePreparedRawQueryExecution(queryInterface, activeQueries, prepared);
});

export const releaseTopicStoreRawQueryExecution = (
  store: TopicStore,
  token: RawQueryExecutionReleaseToken,
): Effect.Effect<void> =>
  releaseRawQueryExecutionToken(topicStoreQueryResources(store).activeQueries, token);

export const acquireTopicStoreRuntimeGroupedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.acquireRuntime",
)(function* (
  store: TopicStore,
  query: unknown,
  groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits,
  partition?: ColumnLiveViewEngineQueryPartition,
) {
  const { activeQueries, metadata, queryInterface } = topicStoreQueryResources(store);
  const prepared = yield* prepareRuntimeGroupedQueryAdmission(
    store.topic,
    metadata,
    query,
    partition,
  );
  const execution = yield* acquirePreparedMaterializedQueryExecution(
    queryInterface,
    activeQueries,
    prepared.cacheKey,
    (releaseRetainedChanges) => {
      const compiled = compilePreparedRuntimeGroupedQuery(prepared);
      return {
        canonicalCompiled: compiled,
        execution: makeIncrementalGroupedQueryExecution(
          queryInterface,
          compiled,
          releaseRetainedChanges,
          groupedIncrementalAdmissionLimits,
        ),
        resultSemantics: compiled.plan.resultSemantics,
      };
    },
    prepared.partition?.key,
  );
  return Object.freeze({
    execution,
    releaseToken: prepared.cacheKey,
  });
});

export const releaseTopicStoreMaterializedQueryExecutionToken = (
  store: TopicStore,
  cacheKey: string,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(topicStoreQueryResources(store).activeQueries, cacheKey);
