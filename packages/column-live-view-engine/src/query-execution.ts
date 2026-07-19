import type { GroupedQuery, RawQuery } from "@effect-view-server/config";
import { Effect } from "effect";
import type { ColumnLiveViewTerminalObserver } from "./engine-contract";
import { makeLiveSubscription } from "./live-subscription";
import type { CompiledGroupedQuery } from "./grouped-query-compiler";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import type { CompiledRawQuery } from "./raw-query-compiler";
import { liveQueryResultFromOwnedEvaluation } from "./query-result";
import {
  acquireTopicStoreMaterializedQueryExecution,
  acquireTopicStoreRawQueryExecution,
  evaluateTopicStoreGroupedQuery,
  evaluateTopicStoreRawQueryResult,
  prepareTopicStoreGroupedQuery,
  prepareTopicStoreRawQuery,
  prepareTopicStoreRuntimeGroupedQuery,
  prepareTopicStoreRuntimeRawQuery,
  releaseTopicStoreMaterializedQueryExecution,
  releaseTopicStoreRawQueryExecution,
  type TopicStore,
  type TopicStoreSubscriptionPermit,
} from "./topic-store";
import type { RawQueryCompilerMetadata } from "./raw-query-compiler";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";

type RowObject = object;

export type ExecutableQuery<ResultRow extends RowObject> =
  | {
      readonly kind: "raw";
      readonly compiled: CompiledRawQuery<object, ResultRow>;
    }
  | {
      readonly kind: "grouped";
      readonly compiled: CompiledGroupedQuery<object, ResultRow>;
    };

export const isGroupedQuery = (
  query: unknown,
): query is { readonly aggregates: object; readonly groupBy: ReadonlyArray<unknown> } =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

export const prepareRawExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.prepareRaw",
)(function* <Row extends RowObject, const Query extends RawQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  const compiled = yield* prepareTopicStoreRawQuery(store, metadata, query);
  return Object.freeze({
    kind: "raw",
    compiled,
  } satisfies ExecutableQuery<ReturnType<typeof compiled.plan.resultSemantics.projectRow>>);
});

export const prepareGroupedExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.prepareGrouped",
)(function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  const compiled = yield* prepareTopicStoreGroupedQuery(store, metadata, query);
  return Object.freeze({
    kind: "grouped",
    compiled,
  } satisfies ExecutableQuery<ReturnType<typeof compiled.plan.resultSemantics.projectRow>>);
});

export const prepareRuntimeExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.prepareRuntime",
)(function* (store: TopicStore, query: unknown, partition?: ColumnLiveViewEngineQueryPartition) {
  if (isGroupedQuery(query)) {
    const compiled = yield* prepareTopicStoreRuntimeGroupedQuery(store, query, partition);
    return Object.freeze({
      kind: "grouped",
      compiled,
    } satisfies ExecutableQuery<RowObject>);
  }
  const compiled = yield* prepareTopicStoreRuntimeRawQuery(store, query, partition);
  return Object.freeze({
    kind: "raw",
    compiled,
  } satisfies ExecutableQuery<RowObject>);
});

export const snapshotRawExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.snapshotRaw",
)(function* <Row extends RowObject, const Query extends RawQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  const executable = yield* prepareRawExecutableQuery(store, metadata, query);
  return evaluateTopicStoreRawQueryResult(store, executable.compiled);
});

export const snapshotGroupedExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.snapshotGrouped",
)(function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
  store: TopicStore,
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
) {
  const executable = yield* prepareGroupedExecutableQuery(store, metadata, query);
  return liveQueryResultFromOwnedEvaluation(
    evaluateTopicStoreGroupedQuery(store, executable.compiled),
    executable.compiled.plan.resultSemantics,
  );
});

export const snapshotRuntimeExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.snapshotRuntime",
)(function* (store: TopicStore, query: unknown) {
  const executable = yield* prepareRuntimeExecutableQuery(store, query);
  return executable.kind === "raw"
    ? evaluateTopicStoreRawQueryResult(store, executable.compiled)
    : liveQueryResultFromOwnedEvaluation(
        evaluateTopicStoreGroupedQuery(store, executable.compiled),
        executable.compiled.plan.resultSemantics,
      );
});

type SubscribeExecutableQueryInput = {
  readonly groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits;
  readonly permit: TopicStoreSubscriptionPermit;
  readonly queryId: string;
  readonly queueCapacity: number;
  readonly terminalObserver: ColumnLiveViewTerminalObserver;
};

export const subscribeRawExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.subscribeRaw",
)(function* <Row extends RowObject, const Query extends RawQuery<NoInfer<Row>>>(
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
  input: SubscribeExecutableQueryInput,
) {
  const { store } = input.permit;
  const executable = yield* prepareRawExecutableQuery(store, metadata, query);
  const execution = yield* acquireTopicStoreRawQueryExecution(store, executable.compiled);
  return yield* makeLiveSubscription({
    permit: input.permit,
    queryId: input.queryId,
    execution,
    queueCapacity: input.queueCapacity,
    release: releaseTopicStoreRawQueryExecution(store, executable.compiled),
    terminalObserver: input.terminalObserver,
  });
});

export const subscribeGroupedExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.subscribeGrouped",
)(function* <Row extends RowObject, const Query extends GroupedQuery<NoInfer<Row>>>(
  metadata: RawQueryCompilerMetadata<Row>,
  query: Query,
  input: SubscribeExecutableQueryInput,
) {
  const { store } = input.permit;
  const executable = yield* prepareGroupedExecutableQuery(store, metadata, query);
  const execution = yield* acquireTopicStoreMaterializedQueryExecution(
    store,
    executable.compiled,
    input.groupedIncrementalAdmissionLimits,
  );
  return yield* makeLiveSubscription({
    permit: input.permit,
    queryId: input.queryId,
    execution,
    queueCapacity: input.queueCapacity,
    release: releaseTopicStoreMaterializedQueryExecution(store, executable.compiled),
    terminalObserver: input.terminalObserver,
  });
});

export const subscribeRuntimeExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.subscribeRuntime",
)(function* (
  query: unknown,
  input: SubscribeExecutableQueryInput,
  partition?: ColumnLiveViewEngineQueryPartition,
) {
  const { store } = input.permit;
  const executable = yield* prepareRuntimeExecutableQuery(store, query, partition);
  if (executable.kind === "raw") {
    const execution = yield* acquireTopicStoreRawQueryExecution(store, executable.compiled);
    return yield* makeLiveSubscription({
      permit: input.permit,
      queryId: input.queryId,
      execution,
      ...(partition === undefined ? {} : { partitionKey: partition.key }),
      queueCapacity: input.queueCapacity,
      release: releaseTopicStoreRawQueryExecution(store, executable.compiled),
      terminalObserver: input.terminalObserver,
    });
  }

  const execution = yield* acquireTopicStoreMaterializedQueryExecution(
    store,
    executable.compiled,
    input.groupedIncrementalAdmissionLimits,
  );
  return yield* makeLiveSubscription({
    permit: input.permit,
    queryId: input.queryId,
    execution,
    ...(partition === undefined ? {} : { partitionKey: partition.key }),
    queueCapacity: input.queueCapacity,
    release: releaseTopicStoreMaterializedQueryExecution(store, executable.compiled),
    terminalObserver: input.terminalObserver,
  });
});
