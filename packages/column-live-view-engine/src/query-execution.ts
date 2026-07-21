import { Effect } from "effect";
import type { ColumnLiveViewTerminalObserver } from "./engine-contract";
import { makeLiveSubscription } from "./live-subscription";
import type { CompiledGroupedQuery } from "./grouped-query-compiler";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import type { CompiledRawQuery } from "./raw-query-compiler";
import { liveQueryResultFromOwnedEvaluation } from "./query-result";
import {
  acquireTopicStoreRuntimeGroupedQueryExecution,
  acquireTopicStoreRuntimeRawQueryExecution,
  evaluateTopicStoreGroupedQuery,
  evaluateTopicStoreRawQueryResult,
  prepareTopicStoreRuntimeGroupedQuery,
  prepareTopicStoreRuntimeRawQuery,
  releaseTopicStoreMaterializedQueryExecutionToken,
  releaseTopicStoreRawQueryExecution,
  type TopicStore,
  type TopicStoreSubscriptionPermit,
} from "./topic-store";
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

// Either marker makes the grouped decoder responsible for reporting a missing counterpart.
export const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  (Object.hasOwn(query, "groupBy") || Object.hasOwn(query, "aggregates"));

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

export const subscribeRuntimeExecutableQuery = Effect.fn(
  "ColumnLiveViewEngine.queryExecution.subscribeRuntime",
)(function* (
  query: unknown,
  input: SubscribeExecutableQueryInput,
  partition?: ColumnLiveViewEngineQueryPartition,
) {
  const { store } = input.permit;
  if (!isGroupedQuery(query)) {
    const acquired = yield* acquireTopicStoreRuntimeRawQueryExecution(store, query, partition);
    return yield* makeLiveSubscription({
      permit: input.permit,
      queryId: input.queryId,
      execution: acquired.execution,
      ...(partition === undefined ? {} : { partitionKey: partition.key }),
      queueCapacity: input.queueCapacity,
      release: releaseTopicStoreRawQueryExecution(store, acquired.releaseToken),
      terminalObserver: input.terminalObserver,
    });
  }

  const acquired = yield* acquireTopicStoreRuntimeGroupedQueryExecution(
    store,
    query,
    input.groupedIncrementalAdmissionLimits,
    partition,
  );
  return yield* makeLiveSubscription({
    permit: input.permit,
    queryId: input.queryId,
    execution: acquired.execution,
    ...(partition === undefined ? {} : { partitionKey: partition.key }),
    queueCapacity: input.queueCapacity,
    release: releaseTopicStoreMaterializedQueryExecutionToken(store, acquired.releaseToken),
    terminalObserver: input.terminalObserver,
  });
});
