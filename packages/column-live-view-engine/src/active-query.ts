import { Effect } from "effect";
import { activeRawQueryExecutionCount, clearRawQueryExecutions } from "./active-raw-query";
import {
  activeMaterializedQueryExecutionCount,
  activeMaterializedQueryExecutionModeCounts,
  clearMaterializedQueryExecutions,
} from "./active-materialized-query";
import type { ActiveQueryExecutionCounts, ActiveQueryRegistry } from "./active-query-contract";

export {
  acquireRawQueryExecution,
  evaluateRawQuery,
  evaluateRawQueryResult,
  releaseRawQueryExecution,
} from "./active-raw-query";
export {
  acquireMaterializedQueryExecution,
  releaseMaterializedQueryExecution,
} from "./active-materialized-query";
export {
  createActiveQueryRegistry,
  type ActiveQueryExecutionCounts,
  type ActiveQueryRegistry,
  type LiveQueryExecution,
  type LiveQueryExecutionCursor,
  type MaterializedQueryExecution,
  type RawQueryExecution,
} from "./active-query-contract";

export const clearStoreRawQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.clearStore",
)((registry: ActiveQueryRegistry) =>
  Effect.uninterruptible(
    Effect.gen(function* () {
      yield* clearRawQueryExecutions(registry);
      yield* clearMaterializedQueryExecutions(registry);
    }),
  ),
);

export const activeStoreRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStore",
)((registry: ActiveQueryRegistry) =>
  Effect.gen(function* () {
    const rawCount = yield* activeRawQueryExecutionCount(registry);
    const materializedCount = yield* activeMaterializedQueryExecutionCount(registry);
    return rawCount + materializedCount;
  }),
);

export const activeStoreQueryExecutionCounts = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStoreModes",
)((registry: ActiveQueryRegistry) =>
  Effect.gen(function* () {
    const rawCount = yield* activeRawQueryExecutionCount(registry);
    const materializedCounts = yield* activeMaterializedQueryExecutionModeCounts(registry);
    return {
      activeFallbackGroupedViews: materializedCounts.activeFallback,
      activeIncrementalGroupedViews: materializedCounts.activeIncremental,
      activeViews: rawCount + materializedCounts.activeTotal,
      groupedFullEvaluationCount: materializedCounts.groupedFullEvaluationCount,
      groupedPatchedEvaluationCount: materializedCounts.groupedPatchedEvaluationCount,
    } satisfies ActiveQueryExecutionCounts;
  }),
);
