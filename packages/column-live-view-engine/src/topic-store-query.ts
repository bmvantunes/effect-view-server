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
import {
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  type TopicStore,
} from "./topic-store-state";

type RowObject = object;

export const topicStoreQueryMetadata = Effect.fn("ColumnLiveViewEngine.topicStore.query.metadata")(
  function* <SchemaValue extends Schema.Codec<RowObject, unknown, never, never>>(
    store: TopicStore,
    schema: SchemaValue,
  ) {
    const metadata = topicStoreRawQueryMetadata(store);
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
)(function* (store: TopicStore, query: unknown) {
  return yield* prepareRuntimeRawQuery(store.topic, topicStoreRawQueryMetadata(store), query);
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
)(function* (store: TopicStore, query: unknown) {
  return yield* prepareRuntimeGroupedQuery(store.topic, topicStoreRawQueryMetadata(store), query);
});

export const evaluateTopicStoreRawQueryResult = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): LiveQueryResult<ResultRow> => evaluateRawQueryResult(topicStoreReadModel(store), compiled);

export const evaluateTopicStoreGroupedQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): QueryEvaluation<RowObject> => evaluateCompiledGroupedQuery(topicStoreReadModel(store), compiled);

export const acquireTopicStoreRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
) {
  return yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);
});

export const releaseTopicStoreRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): Effect.Effect<void> => releaseRawQueryExecution(topicStoreReadModel(store), compiled);

export const acquireTopicStoreMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.materialized.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
  groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits,
) {
  const readModel = topicStoreReadModel(store);
  return yield* acquireMaterializedQueryExecution(
    readModel,
    compiled.cacheKey,
    compiled.plan.resultSemantics,
    (releaseRetainedChanges) =>
      makeIncrementalGroupedQueryExecution(
        readModel,
        compiled,
        releaseRetainedChanges,
        groupedIncrementalAdmissionLimits,
      ),
  );
});

export const releaseTopicStoreMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(topicStoreReadModel(store), compiled.cacheKey);
