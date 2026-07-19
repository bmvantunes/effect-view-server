import type { GroupedQuery, LiveQueryResult, RawQuery } from "@effect-view-server/config";
import { Effect, type Schema } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  evaluateRawQueryResult,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
} from "./active-query";
import {
  evaluateCompiledGroupedQuery,
  prepareGroupedQuery,
  prepareRuntimeGroupedQuery,
  type CompiledGroupedQuery,
} from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import {
  prepareRawQuery,
  prepareRuntimeRawQuery,
  rawQueryCompilerMetadataMatchesSchema,
  type CompiledRawQuery,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { InvalidQueryError } from "./raw-query-decoder";
import type { QueryEvaluation } from "./query-result";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";
import { topicStoreQueryResources, type TopicStore } from "./topic-store-state";

type RowObject = object;

export const topicStoreQueryMetadata = Effect.fn("ColumnLiveViewEngine.topicStore.query.metadata")(
  function* <SchemaValue extends Schema.Codec<RowObject, unknown, never, never>>(
    store: TopicStore,
    schema: SchemaValue,
  ) {
    const { metadata } = topicStoreQueryResources(store);
    if (!rawQueryCompilerMetadataMatchesSchema(metadata, schema)) {
      return yield* InvalidQueryError.make({
        topic: store.topic,
        message: "Topic Store schema does not match the compiled query proof schema.",
      });
    }
    return metadata;
  },
);

export const prepareTopicStoreRawQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.prepare",
)(function* <Row extends RowObject, const Query extends RawQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  yield* topicStoreQueryMetadata(store, metadata.schema);
  return yield* prepareRawQuery(store.topic, metadata, query);
});

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

export const prepareTopicStoreGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.prepare",
)(function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  yield* topicStoreQueryMetadata(store, metadata.schema);
  return yield* prepareGroupedQuery(store.topic, metadata, query);
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
