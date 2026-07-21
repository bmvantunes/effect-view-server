import {
  acquireMaterializedQueryExecution as acquireMaterializedQueryExecutionFromInterface,
  acquireRawQueryExecution as acquireRawQueryExecutionFromInterface,
  activeStoreRawQueryExecutionCount as activeStoreRawQueryExecutionCountFromRegistry,
  clearStoreRawQueryExecutions as clearStoreRawQueryExecutionsFromRegistry,
  releaseMaterializedQueryExecution as releaseMaterializedQueryExecutionFromRegistry,
  releaseRawQueryExecution as releaseRawQueryExecutionFromRegistry,
} from "../src/active-query";
import { preparedRawQueryPlanCompilationCount } from "../src/active-raw-query";
import { preparedGroupedQueryPlanCompilationCount } from "../src/active-materialized-query";
import {
  createActiveQueryRegistry,
  type ActiveQueryRegistry,
  type MaterializedQueryExecution,
} from "../src/active-query-contract";
import type { CompiledRawQuery, RawQueryCompilerMetadata } from "../src/raw-query-compiler";
import type { QueryResultSemantics } from "../src/query-result-semantics";
import { TopicRowStorage } from "../src/topic-row-storage";
import { topicStoreQueryResources, type TopicStore } from "../src/topic-store-state";
import type { TopicStoreQueryInterface } from "../src/topic-store-query-interface";

type RowObject = object;

export type ActiveQueryTestInterface = TopicStoreQueryInterface & {
  readonly activeQueries: ActiveQueryRegistry;
};

const storageRegistries = new WeakMap<TopicRowStorage, ActiveQueryRegistry>();

const combineTestInterface = (
  queryInterface: TopicStoreQueryInterface,
  activeQueries: ActiveQueryRegistry,
): ActiveQueryTestInterface => ({
  ...queryInterface,
  activeQueries,
});

export const activeQueryTestInterface = (store: TopicStore): ActiveQueryTestInterface => {
  const { activeQueries, queryInterface } = topicStoreQueryResources(store);
  return combineTestInterface(queryInterface, activeQueries);
};

export const activeQueryTestInterfaceForStorage = (
  storage: TopicRowStorage,
): ActiveQueryTestInterface => {
  const existing = storageRegistries.get(storage);
  if (existing !== undefined) {
    return combineTestInterface(storage.queryInterface, existing);
  }
  const activeQueries = createActiveQueryRegistry();
  storageRegistries.set(storage, activeQueries);
  return combineTestInterface(storage.queryInterface, activeQueries);
};

export const activeQueryTestMetadata = (store: TopicStore): RawQueryCompilerMetadata =>
  topicStoreQueryResources(store).metadata;

export const acquireRawQueryExecution = <ResultRow extends RowObject>(
  queryInterface: ActiveQueryTestInterface,
  compiled: CompiledRawQuery<object, ResultRow>,
) => acquireRawQueryExecutionFromInterface(queryInterface, queryInterface.activeQueries, compiled);

export const releaseRawQueryExecution = <ResultRow extends RowObject>(
  queryInterface: ActiveQueryTestInterface,
  compiled: CompiledRawQuery<object, ResultRow>,
) => releaseRawQueryExecutionFromRegistry(queryInterface.activeQueries, compiled);

export const acquireMaterializedQueryExecution = <ResultRow extends RowObject>(
  queryInterface: ActiveQueryTestInterface,
  cacheKey: string,
  resultSemantics: QueryResultSemantics<ResultRow>,
  makeExecution: (releaseRetainedChanges: () => void) => MaterializedQueryExecution,
) =>
  acquireMaterializedQueryExecutionFromInterface(
    queryInterface,
    queryInterface.activeQueries,
    cacheKey,
    resultSemantics,
    makeExecution,
  );

export const releaseMaterializedQueryExecution = (
  queryInterface: ActiveQueryTestInterface,
  cacheKey: string,
) => releaseMaterializedQueryExecutionFromRegistry(queryInterface.activeQueries, cacheKey);

export const clearStoreRawQueryExecutions = (queryInterface: ActiveQueryTestInterface) =>
  clearStoreRawQueryExecutionsFromRegistry(queryInterface.activeQueries);

export const activeStoreRawQueryExecutionCount = (queryInterface: ActiveQueryTestInterface) =>
  activeStoreRawQueryExecutionCountFromRegistry(queryInterface.activeQueries);

export const preparedRawPlanCompilationCount = (queryInterface: ActiveQueryTestInterface) =>
  preparedRawQueryPlanCompilationCount(queryInterface.activeQueries);

export const preparedGroupedPlanCompilationCount = (queryInterface: ActiveQueryTestInterface) =>
  preparedGroupedQueryPlanCompilationCount(queryInterface.activeQueries);

export { createActiveQueryRegistry };
