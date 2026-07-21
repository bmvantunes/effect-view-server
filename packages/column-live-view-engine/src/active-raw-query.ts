import { Effect, Option } from "effect";
import type { DeltaEvent, LiveQueryResult } from "@effect-view-server/config";
import type {
  ActiveQueryBaseEvaluation,
  ActiveQueryBaseExecution,
  AcquiredRawQueryExecution,
  ActiveQueryRegistry,
  RawQueryExecution,
  RawQueryExecutionReleaseToken,
  RawQueryExecutionSlot,
  RawQueryExecutionWindowSlot,
  RetainedWindowEntry,
} from "./active-query-contract";
import {
  compilePreparedRuntimeRawQuery,
  type CompiledRawQuery,
  type PreparedRuntimeRawQuery,
} from "./raw-query-compiler";
import {
  rawQueryPlanWindow,
  rawQueryWindowScanPlan,
  type RawQueryPlanWindow,
} from "./raw-query-plan";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation } from "./query-result";
import type { TopicRawWindowScan } from "./raw-window-scan";
import {
  bindTopicStorageProjection,
  type TopicStorageProjectionSession,
} from "./topic-storage-projection";
import type { TopicStoreQueryInterface } from "./topic-store-query-interface";
import type { TopicStorageProjectableQueryResultSemantics } from "./query-result-semantics";

type RowObject = object;

type RawQueryLeasePlan<ResultRow extends RowObject> = {
  readonly resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>;
  readonly window: RawQueryPlanWindow;
};

const retainedWindowFilled = (
  window: ReadonlyArray<{ readonly key: string; readonly row: RowObject }>,
  totalRows: number,
  queryWindow: RawQueryPlanWindow,
): boolean =>
  queryWindow.limit === undefined || window.length >= Math.min(totalRows, queryWindow.limit);

const retainedWindowLookahead = (window: RawQueryPlanWindow): number => {
  if (window.limit === undefined || window.limit === 0) {
    return 0;
  }
  return window.limit >= 128 ? 64 : 1;
};

const getActiveRawQueryMap = (
  registry: ActiveQueryRegistry,
): Map<string, RawQueryExecutionSlot> => {
  return registry.raw;
};

const retainedWindowKeyIndex = (
  windowEntries: ReadonlyArray<RetainedWindowEntry>,
): ReadonlyMap<string, number> => {
  const keyIndex = new Map<string, number>();
  for (const [index, entry] of windowEntries.entries()) {
    keyIndex.set(entry.key, index);
  }
  return keyIndex;
};

const evaluateBaseQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: RawQueryPlanWindow = compiled.plan.window,
): ActiveQueryBaseEvaluation<Row> => {
  const version = store.version();
  const scanResult = store.scanRawWindow(rawQueryWindowScanPlan(compiled.plan, queryWindow));
  const window = scanResult.window.map((entry) =>
    entry.slot === undefined
      ? {
          key: entry.key,
          row: entry.row,
        }
      : {
          key: entry.key,
          row: entry.row,
          slot: entry.slot,
        },
  );
  return {
    ...scanResult,
    keyIndex: retainedWindowKeyIndex(window),
    retainedWindowFilled: retainedWindowFilled(window, scanResult.totalRows, queryWindow),
    version,
    window,
  };
};

export const replaceRetainedMatchingEntryAtIndex = <Row extends RowObject>(
  windowEntries: Array<RetainedWindowEntry<Row>>,
  previousIndex: number,
  key: string,
  row: Row,
  comparePrevious: (left: RetainedWindowEntry<Row>, right: RetainedWindowEntry<Row>) => number,
  retainedLimit: number | undefined,
): boolean | undefined => {
  const previousEntry = windowEntries[previousIndex];
  if (previousEntry === undefined || previousEntry.key !== key) {
    return undefined;
  }
  const nextEntry: RetainedWindowEntry<Row> = { key, row };
  if (retainedLimit !== undefined && comparePrevious(nextEntry, previousEntry) > 0) {
    return undefined;
  }
  windowEntries[previousIndex] = nextEntry;
  return true;
};

const retainedEntrySortComparator = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
  queryWindow: RawQueryPlanWindow,
): ((left: RetainedWindowEntry<Row>, right: RetainedWindowEntry<Row>) => number) => {
  const compareSlots = store.compareRawSlots?.(rawQueryWindowScanPlan(compiled.plan, queryWindow));
  const slotForKey = store.slotForKey;
  if (compareSlots === undefined || slotForKey === undefined) {
    return compiled.plan.compare;
  }
  return (left, right) => {
    const leftSlot = slotForKey(left.key);
    const rightSlot = slotForKey(right.key);
    return leftSlot === undefined || rightSlot === undefined
      ? compiled.plan.compare(left, right)
      : compareSlots(leftSlot, rightSlot);
  };
};

const retainedLimitAfterInsertedChanges = (
  compareRetainedEntries: (
    left: RetainedWindowEntry<RowObject>,
    right: RetainedWindowEntry<RowObject>,
  ) => number,
  insertedWindowEntries: ReadonlyMap<string, RetainedWindowEntry>,
  queryWindow: RawQueryPlanWindow,
  requiredWindowEntries: number | undefined,
  removedRetainedEntry: boolean,
  windowEntries: ReadonlyArray<RetainedWindowEntry>,
): number | undefined => {
  if (!removedRetainedEntry || requiredWindowEntries === undefined) {
    return queryWindow.limit;
  }
  const retainedTail = windowEntries[windowEntries.length - 1]!;
  let safeInsertedEntries = 0;
  for (const insertedEntry of insertedWindowEntries.values()) {
    if (compareRetainedEntries(insertedEntry, retainedTail) <= 0) {
      safeInsertedEntries += 1;
    }
  }
  return Math.min(queryWindow.limit!, windowEntries.length + safeInsertedEntries);
};

const updateBaseEvaluationFromRetainedChanges = (
  store: TopicStoreQueryInterface,
  compiled: CompiledRawQuery<object, object>,
  evaluation: ActiveQueryBaseEvaluation<object>,
  baseWindow: RawQueryPlanWindow,
  queryWindow: RawQueryPlanWindow,
): ActiveQueryBaseEvaluation<object> | undefined => {
  const currentVersion = store.version();
  const batches = store.changesSince(evaluation.version, compiled.plan.partitionKey);
  if (batches === undefined) {
    return undefined;
  }

  let totalRows = evaluation.totalRows;
  let windowEntries = evaluation.window;
  let mutableWindowEntries: Array<RetainedWindowEntry> | undefined;
  let removedRetainedEntry = false;
  let replacedRetainedEntry = false;
  const insertedWindowEntries = new Map<string, RetainedWindowEntry>();
  const removedRetainedKeys = new Set<string>();
  const compareRetainedEntries = retainedEntrySortComparator(store, compiled, queryWindow);
  for (const batch of batches) {
    for (const change of batch.changes) {
      const previousMatches =
        change.previous !== undefined &&
        compiled.plan.predicate.matches(change.previous, change.key);
      const nextMatches =
        change.next !== undefined && compiled.plan.predicate.matches(change.next, change.key);

      if (queryWindow.limit === 0) {
        if (previousMatches && !nextMatches) {
          totalRows -= 1;
        } else if (!previousMatches && nextMatches) {
          totalRows += 1;
        }
        continue;
      }

      if (change.previous !== undefined) {
        if (previousMatches && nextMatches) {
          const pendingInsertedEntry = insertedWindowEntries.get(change.key);
          if (pendingInsertedEntry !== undefined) {
            insertedWindowEntries.set(change.key, {
              key: change.key,
              row: change.next,
            });
            continue;
          }
          const previousIndex = evaluation.keyIndex.get(change.key);
          if (previousIndex === undefined || removedRetainedKeys.has(change.key)) {
            return undefined;
          }
          if (mutableWindowEntries === undefined) {
            mutableWindowEntries = [...windowEntries];
            windowEntries = mutableWindowEntries;
          }
          const replaced = replaceRetainedMatchingEntryAtIndex(
            mutableWindowEntries,
            previousIndex,
            change.key,
            change.next,
            compiled.plan.compare,
            queryWindow.limit,
          );
          if (replaced === undefined) {
            return undefined;
          }
          replacedRetainedEntry = true;
          continue;
        }
        if (previousMatches) {
          totalRows -= 1;
          insertedWindowEntries.delete(change.key);
          const previousIndex = evaluation.keyIndex.get(change.key);
          if (previousIndex !== undefined) {
            removedRetainedKeys.add(change.key);
            removedRetainedEntry = true;
          }
          continue;
        }
        if (nextMatches) {
          totalRows += 1;
          insertedWindowEntries.set(change.key, {
            key: change.key,
            row: change.next,
          });
        }
        continue;
      }
      if (change.next !== undefined && nextMatches) {
        totalRows += 1;
        insertedWindowEntries.set(change.key, {
          key: change.key,
          row: change.next,
        });
      }
    }
  }

  if (removedRetainedKeys.size > 0) {
    windowEntries = windowEntries.filter((entry) => !removedRetainedKeys.has(entry.key));
    mutableWindowEntries = undefined;
  }

  if (queryWindow.limit === 0) {
    return {
      keyIndex: new Map(),
      keys: [],
      retainedWindowFilled: true,
      totalRows,
      version: currentVersion,
      window: [],
    };
  }

  if (replacedRetainedEntry) {
    windowEntries = [...windowEntries].sort(compareRetainedEntries);
  }

  const requiredWindowEntries = baseWindow.limit;
  if (
    requiredWindowEntries !== undefined &&
    windowEntries.length < Math.min(totalRows, requiredWindowEntries)
  ) {
    return undefined;
  }

  if (insertedWindowEntries.size === 0) {
    if (windowEntries === evaluation.window && totalRows === evaluation.totalRows) {
      return {
        ...evaluation,
        retainedWindowFilled: retainedWindowFilled(windowEntries, totalRows, queryWindow),
        version: currentVersion,
      };
    }
    return {
      ...evaluation,
      keyIndex: retainedWindowKeyIndex(windowEntries),
      keys: windowEntries.map((entry) => entry.key),
      retainedWindowFilled: retainedWindowFilled(windowEntries, totalRows, queryWindow),
      totalRows,
      version: currentVersion,
      window: windowEntries,
    };
  }

  if (!evaluation.retainedWindowFilled) {
    return undefined;
  }

  const window = [...windowEntries, ...insertedWindowEntries.values()].sort(compareRetainedEntries);
  const retainedLimit = retainedLimitAfterInsertedChanges(
    compareRetainedEntries,
    insertedWindowEntries,
    queryWindow,
    requiredWindowEntries,
    removedRetainedEntry,
    windowEntries,
  );
  const limitedWindow = retainedLimit === undefined ? window : window.slice(0, retainedLimit);
  return {
    keyIndex: retainedWindowKeyIndex(limitedWindow),
    keys: limitedWindow.map((entry) => entry.key),
    retainedWindowFilled: retainedWindowFilled(limitedWindow, totalRows, queryWindow),
    totalRows,
    version: currentVersion,
    window: limitedWindow,
  };
};

const projectBaseEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
): QueryEvaluation<ResultRow> => {
  const storageProjection = bindStoreProjection(store, compiled.plan.resultSemantics);
  const window = evaluation.window.map((entry) => ({
    key: entry.key,
    row: projectRetainedEntry(store, compiled.plan.resultSemantics, entry, storageProjection),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: evaluation.keys,
    window,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  };
};

const projectWindowEvaluation = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  leasePlan: RawQueryLeasePlan<ResultRow>,
  evaluation: ActiveQueryBaseEvaluation<Row>,
  storageProjection: TopicStorageProjectionSession<ResultRow> | undefined,
): QueryEvaluation<ResultRow> => {
  const end =
    leasePlan.window.limit === undefined
      ? undefined
      : leasePlan.window.offset + leasePlan.window.limit;
  const sourceWindow = evaluation.window.slice(leasePlan.window.offset, end);
  const window = sourceWindow.map((entry) => ({
    key: entry.key,
    row: projectRetainedEntry(store, leasePlan.resultSemantics, entry, storageProjection),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  };
};

export const evaluateRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> =>
  projectBaseEvaluation(store, compiled, evaluateBaseQuery(store, compiled));

const bindStoreProjection = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
): TopicStorageProjectionSession<ResultRow> | undefined => {
  const storageProjection = store.storageProjection;
  return storageProjection === undefined
    ? undefined
    : bindTopicStorageProjection(storageProjection, resultSemantics.topicStorageProjectionProof);
};

const projectRetainedEntry = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
  entry: RetainedWindowEntry<Row>,
  storageProjection: TopicStorageProjectionSession<ResultRow> | undefined,
): ResultRow => {
  const slot = retainedEntrySlot(store, entry);
  return storageProjection === undefined || slot === undefined
    ? resultSemantics.projectRow(entry.row)
    : storageProjection.projectResultRow(slot);
};

const retainedEntrySlot = <Row extends RowObject>(
  store: TopicRawWindowScan<Row>,
  entry: RetainedWindowEntry<Row>,
): number | undefined => {
  const carriedSlot =
    entry.slot !== undefined && store.keyAtSlot?.(entry.slot) === entry.key
      ? entry.slot
      : undefined;
  return carriedSlot ?? store.slotForKey?.(entry.key);
};

const projectOwnedRetainedEntry = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row>,
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
  entry: RetainedWindowEntry<Row>,
  storageProjection: TopicStorageProjectionSession<ResultRow> | undefined,
): ResultRow => {
  const slot = retainedEntrySlot(store, entry);
  return storageProjection === undefined || slot === undefined
    ? resultSemantics.projectOwnedRow(entry.row)
    : storageProjection.projectOwnedResultRow(slot);
};

export const evaluateRawQueryResult = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRawWindowScan<Row> & { readonly version: () => number },
  compiled: CompiledRawQuery<Row, ResultRow>,
): LiveQueryResult<ResultRow> => {
  const version = store.version();
  const scanResult = store.scanRawWindow(
    rawQueryWindowScanPlan(compiled.plan, compiled.plan.window),
  );
  const storageProjection = bindStoreProjection(store, compiled.plan.resultSemantics);
  return {
    rows: scanResult.window.map((entry) =>
      projectOwnedRetainedEntry(store, compiled.plan.resultSemantics, entry, storageProjection),
    ),
    totalRows: scanResult.totalRows,
    version,
    status: "ready",
    statusCode: "Ready",
  };
};

const leaseRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStoreQueryInterface,
  execution: ActiveQueryBaseExecution,
  leasePlan: RawQueryLeasePlan<ResultRow>,
  storageProjection: TopicStorageProjectionSession<ResultRow> | undefined,
): RawQueryExecution<ResultRow> => {
  const latestEvaluation = () =>
    projectWindowEvaluation(store, leasePlan, execution.latest(), storageProjection);

  return Object.freeze({
    initial: (queryId) =>
      snapshotEvent(store, queryId, latestEvaluation(), leasePlan.resultSemantics),
    createCursor: () => ({
      evaluation: latestEvaluation(),
    }),
    next: (queryId, cursor): Effect.Effect<Option.Option<DeltaEvent<ResultRow>>> =>
      Effect.sync(() => {
        const previous = cursor.evaluation;
        const next = latestEvaluation();
        const operations = deltaOperations(previous, next, leasePlan.resultSemantics);
        if (operations.length === 0 && previous.totalRows === next.totalRows) {
          return Option.none();
        }
        cursor.evaluation = next;
        return Option.some(
          deltaEvent(store, queryId, previous.version, next, operations, leasePlan.resultSemantics),
        );
      }),
  });
};

const baseWindowForActiveWindows = (
  windows: ReadonlyMap<string, RawQueryExecutionWindowSlot>,
): RawQueryPlanWindow => {
  let limit = 0;
  for (const { window } of windows.values()) {
    if (window.limit === undefined) {
      return rawQueryPlanWindow(0, undefined);
    }
    const windowEnd = window.limit === 0 ? 0 : window.offset + window.limit;
    limit = Math.max(limit, windowEnd);
  }
  return rawQueryPlanWindow(0, limit);
};

const retainedWindowForBaseWindow = (window: RawQueryPlanWindow): RawQueryPlanWindow => {
  if (window.limit === undefined) {
    return window;
  }
  return rawQueryPlanWindow(window.offset, window.limit + retainedWindowLookahead(window));
};

const acquireRawQueryWindow = (
  windows: Map<string, RawQueryExecutionWindowSlot>,
  window: RawQueryPlanWindow,
): RawQueryPlanWindow => {
  const key = window.cacheKey;
  const existing = windows.get(key);
  if (existing !== undefined) {
    existing.refs += 1;
    return existing.window;
  }
  windows.set(key, {
    window,
    refs: 1,
  });
  return window;
};

const releaseRawQueryWindow = (
  windows: Map<string, RawQueryExecutionWindowSlot>,
  window: RawQueryPlanWindow,
): boolean => {
  const key = window.cacheKey;
  const existing = windows.get(key);
  if (existing === undefined) {
    return false;
  }
  if (existing.refs > 1) {
    existing.refs -= 1;
    return true;
  }
  windows.delete(key);
  return true;
};

const makeRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.make")(
  (
    store: TopicStoreQueryInterface,
    canonicalCompiled: CompiledRawQuery<object, object>,
    windows: ReadonlyMap<string, RawQueryExecutionWindowSlot>,
  ) =>
    Effect.sync(() => {
      let baseWindow = baseWindowForActiveWindows(windows);
      let retainedWindow = retainedWindowForBaseWindow(baseWindow);
      let snapshot = {
        evaluation: evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
        version: store.version(),
      };

      const latest = () => {
        const storeVersion = store.version();
        const nextBaseWindow = baseWindowForActiveWindows(windows);
        const windowChanged =
          nextBaseWindow.offset !== baseWindow.offset || nextBaseWindow.limit !== baseWindow.limit;
        if (windowChanged) {
          baseWindow = nextBaseWindow;
          retainedWindow = retainedWindowForBaseWindow(baseWindow);
          snapshot = {
            evaluation: evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
            version: storeVersion,
          };
          return snapshot.evaluation;
        }
        if (snapshot.version !== storeVersion) {
          const incrementalEvaluation = updateBaseEvaluationFromRetainedChanges(
            store,
            canonicalCompiled,
            snapshot.evaluation,
            baseWindow,
            retainedWindow,
          );
          snapshot = {
            evaluation:
              incrementalEvaluation ?? evaluateBaseQuery(store, canonicalCompiled, retainedWindow),
            version: storeVersion,
          };
        }
        return snapshot.evaluation;
      };

      return {
        latest,
      };
    }),
);

const rawQueryLeasePlan = <ResultRow extends RowObject>(
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
  window: RawQueryPlanWindow,
): RawQueryLeasePlan<ResultRow> => Object.freeze({ resultSemantics, window });

const acquireExistingRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStoreQueryInterface,
  registry: ActiveQueryRegistry,
  cacheKey: string,
  leasePlan: RawQueryLeasePlan<ResultRow>,
): AcquiredRawQueryExecution<ResultRow> | undefined => {
  const existing = getActiveRawQueryMap(registry).get(cacheKey);
  if (existing === undefined) {
    return undefined;
  }
  const storageProjection = bindStoreProjection(store, leasePlan.resultSemantics);
  existing.refs += 1;
  const window = acquireRawQueryWindow(existing.windows, leasePlan.window);
  const canonicalLeasePlan = rawQueryLeasePlan(leasePlan.resultSemantics, window);
  return Object.freeze({
    execution: leaseRawQueryExecution(
      store,
      existing.execution,
      canonicalLeasePlan,
      storageProjection,
    ),
    releaseToken: Object.freeze({ cacheKey: existing.cacheKey, window }),
  });
};

const registerRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.register")(
  function* <ResultRow extends RowObject>(
    store: TopicStoreQueryInterface,
    registry: ActiveQueryRegistry,
    compiled: CompiledRawQuery<object, ResultRow>,
    recordCompilation: boolean,
  ) {
    const storageProjection = bindStoreProjection(store, compiled.plan.resultSemantics);
    const windows = new Map<string, RawQueryExecutionWindowSlot>();
    const window = acquireRawQueryWindow(windows, compiled.plan.window);
    const execution = yield* makeRawQueryExecution(store, compiled, windows);
    const cacheKey = compiled.plan.queryCacheKey;
    return yield* Effect.sync(() => {
      store.retainChanges(compiled.plan.partitionKey);
      const entry: RawQueryExecutionSlot = {
        cacheKey,
        canonicalPlan: compiled.plan,
        execution,
        releaseRetainedChanges: () => store.releaseChanges(compiled.plan.partitionKey),
        windows,
        refs: 1,
      };
      getActiveRawQueryMap(registry).set(cacheKey, entry);
      if (recordCompilation) {
        registry.preparedRawPlanCompilationCount += 1;
      }
      const leasePlan = rawQueryLeasePlan(compiled.plan.resultSemantics, window);
      return Object.freeze({
        execution: leaseRawQueryExecution(store, execution, leasePlan, storageProjection),
        releaseToken: Object.freeze({ cacheKey: entry.cacheKey, window }),
      });
    });
  },
);

export const acquirePreparedRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.acquirePrepared",
)(function* <ResultRow extends RowObject>(
  store: TopicStoreQueryInterface,
  registry: ActiveQueryRegistry,
  prepared: PreparedRuntimeRawQuery<ResultRow>,
) {
  const leasePlan = rawQueryLeasePlan(prepared.resultSemantics, prepared.identity.window);
  const existing = acquireExistingRawQueryExecution(
    store,
    registry,
    prepared.identity.queryCacheKey,
    leasePlan,
  );
  if (existing !== undefined) {
    return existing;
  }
  const compiled = compilePreparedRuntimeRawQuery(prepared);
  return yield* registerRawQueryExecution(store, registry, compiled, true);
});

export const acquireRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.acquire")(
  function* <ResultRow extends RowObject>(
    store: TopicStoreQueryInterface,
    registry: ActiveQueryRegistry,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) {
    const leasePlan = rawQueryLeasePlan(compiled.plan.resultSemantics, compiled.plan.window);
    const existing = acquireExistingRawQueryExecution(
      store,
      registry,
      compiled.plan.queryCacheKey,
      leasePlan,
    );
    if (existing !== undefined) {
      return existing.execution;
    }
    const acquired = yield* registerRawQueryExecution(store, registry, compiled, false);
    return acquired.execution;
  },
);

export const releaseRawQueryExecutionToken = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.releaseToken",
)((registry: ActiveQueryRegistry, token: RawQueryExecutionReleaseToken) =>
  Effect.sync(() => {
    const map = getActiveRawQueryMap(registry);
    const entry = map.get(token.cacheKey);
    if (entry === undefined) {
      return undefined;
    }
    if (!releaseRawQueryWindow(entry.windows, token.window)) {
      return undefined;
    }
    if (entry.refs > 1) {
      entry.refs -= 1;
      entry.execution.latest();
      return undefined;
    }
    entry.releaseRetainedChanges();
    map.delete(entry.cacheKey);
    return undefined;
  }),
);

export const releaseRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.release")(
  <ResultRow extends RowObject>(
    registry: ActiveQueryRegistry,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) =>
    releaseRawQueryExecutionToken(registry, {
      cacheKey: compiled.plan.queryCacheKey,
      window: compiled.plan.window,
    }),
);

export const clearRawQueryExecutions = Effect.fn("ColumnLiveViewEngine.activeQuery.raw.clearStore")(
  (registry: ActiveQueryRegistry) =>
    Effect.sync(() => {
      const map = getActiveRawQueryMap(registry);
      for (const entry of map.values()) {
        entry.releaseRetainedChanges();
      }
      map.clear();
    }),
);

export const activeRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.countStore",
)((registry: ActiveQueryRegistry) => Effect.sync(() => getActiveRawQueryMap(registry).size));

export const preparedRawQueryPlanCompilationCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.raw.preparedPlanCompilationCount",
)((registry: ActiveQueryRegistry) => Effect.sync(() => registry.preparedRawPlanCompilationCount));
