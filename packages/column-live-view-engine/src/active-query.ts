import { Effect, Option } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import type { DeltaEvent, SnapshotEvent } from "@view-server/config";
import {
  evaluateCompiledRawQuery,
  type CompiledRawQuery,
  type RuntimeRawQuery,
} from "./raw-query-compiler";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation } from "./query-result";
import { isPlainRecord } from "./row-values";

type RowObject = object;

type ActiveQueryStoreState = {
  readonly rows: ReadonlyMap<string, object>;
  readonly version: number;
  readonly topic: string;
};

type RawQueryExecutionCursor<ResultRow extends RowObject> = {
  evaluation: QueryEvaluation<ResultRow>;
};

type RawQueryExecutionUpdate<ResultRow extends RowObject> = Effect.Effect<
  Option.Option<DeltaEvent<ResultRow>>,
  never,
  never
>;

export type RawQueryExecution<ResultRow extends RowObject> = {
  readonly initial: (queryId: string) => SnapshotEvent<ResultRow>;
  readonly createCursor: () => RawQueryExecutionCursor<ResultRow>;
  readonly next: (
    queryId: string,
    cursor: RawQueryExecutionCursor<ResultRow>,
  ) => RawQueryExecutionUpdate<ResultRow>;
};

type RawQueryExecutionSlot = {
  readonly execution: RawQueryExecution<object>;
  refs: number;
};

const coerceRawQueryExecution = <ToResultRow extends RowObject, FromResultRow extends RowObject>(
  execution: RawQueryExecution<FromResultRow>,
): RawQueryExecution<ToResultRow> => {
  // Internal cache seam: executions are shared by normalized runtime query shape and retyped per subscriber.
  return execution as unknown as RawQueryExecution<ToResultRow>;
};

type QueryExecutionCache = WeakMap<ActiveQueryStoreState, Map<string, RawQueryExecutionSlot>>;

const activeQueryExecutionCache: QueryExecutionCache = new WeakMap();

const isRecordLike = (value: unknown): value is Record<string, unknown> =>
  isPlainRecord(value) || isBigDecimal(value);

const encodeQueryValue = (value: unknown): string => {
  if (isBigDecimal(value)) {
    return `bigdecimal:${value.toString()}`;
  }
  if (value === null || value === undefined) {
    return `${value}`;
  }
  if (Array.isArray(value)) {
    return `[${value.map(encodeQueryValue).join(",")}]`;
  }
  if (isRecordLike(value)) {
    return `object({${Object.keys(value)
      .toSorted()
      .map((key) => `${key}:${encodeQueryValue(value[key])}`)
      .join(",")}})`;
  }
  if (typeof value === "string") {
    return `string:${JSON.stringify(value)}`;
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) {
      return "number:NaN";
    }
    if (!Number.isFinite(value)) {
      return `number:${String(value)}`;
    }
    return `number:${value}`;
  }
  const fallback =
    JSON.stringify(value, (_key, entry) =>
      typeof entry === "bigint" ? entry.toString() : entry,
    ) ?? "[unserializable]";
  return `${typeof value}:${fallback}`;
};

const canonicalizeWhere = (where: Record<string, unknown>): string =>
  `{${Object.keys(where)
    .toSorted()
    .map((field) => `${field}:${encodeQueryValue(where[field])}`)
    .join(",")}}`;

const queryCacheKey = (query: RuntimeRawQuery): string => {
  const orderBy =
    query.orderBy === undefined
      ? "orderBy:"
      : `orderBy:${query.orderBy.map((entry) => `${entry.field}:${entry.direction}`).join(";")}`;
  const selectKey = `select:[${query.select.map(encodeQueryValue).join(",")}]`;
  const whereKey = query.where === undefined ? "where:" : `where:${canonicalizeWhere(query.where)}`;
  const offsetKey = query.offset === undefined ? "offset:" : `offset:${query.offset}`;
  const limitKey = query.limit === undefined ? "limit:" : `limit:${query.limit}`;
  return `${selectKey}|${whereKey}|${orderBy}|${offsetKey}|${limitKey}`;
};

const getActiveQueryMap = (store: ActiveQueryStoreState): Map<string, RawQueryExecutionSlot> => {
  const existing = activeQueryExecutionCache.get(store);
  if (existing !== undefined) {
    return existing;
  }
  const created = new Map<string, RawQueryExecutionSlot>();
  activeQueryExecutionCache.set(store, created);
  return created;
};

const getActiveQueryEntry = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, ResultRow>,
): {
  map: Map<string, RawQueryExecutionSlot>;
  key: string;
} => {
  const key = queryCacheKey(compiled.query);
  const map = getActiveQueryMap(store);
  return { map, key };
};

const evaluateQuery = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  compiled: CompiledRawQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> =>
  evaluateCompiledRawQuery(
    {
      rows: store.rows,
      version: store.version,
    },
    compiled,
  );

export const makeRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.make")(function* <
  ResultRow extends RowObject,
>(store: ActiveQueryStoreState, compiled: CompiledRawQuery<object, ResultRow>) {
  let snapshot = {
    evaluation: evaluateCompiledRawQuery(store, compiled),
    version: store.version,
  };

  const latestEvaluation = () => {
    if (snapshot.version !== store.version) {
      snapshot = {
        evaluation: evaluateQuery(store, compiled),
        version: store.version,
      };
    }
    return snapshot.evaluation;
  };

  const createCursor = () => ({
    evaluation: latestEvaluation(),
  });

  return yield* Effect.succeed({
    initial: (queryId: string) => snapshotEvent(store, queryId, latestEvaluation()),
    createCursor,
    next: (queryId: string, cursor: RawQueryExecutionCursor<ResultRow>) =>
      Effect.sync(() => {
        const previous = cursor.evaluation;
        const next = latestEvaluation();
        const operations = deltaOperations(previous, next);
        if (operations.length === 0 && previous.totalRows === next.totalRows) {
          return Option.none();
        }
        cursor.evaluation = next;
        return Option.some(deltaEvent(store, queryId, previous.version, next, operations));
      }),
  });
});

export const acquireRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.acquire")(
  function* <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) {
    const { map, key } = getActiveQueryEntry(store, compiled);
    const existing = map.get(key);
    if (existing !== undefined) {
      const entry = existing;
      entry.refs += 1;
      return yield* Effect.succeed(coerceRawQueryExecution<ResultRow, object>(entry.execution));
    }

    const execution = yield* makeRawQueryExecution(store, compiled);
    map.set(key, {
      // Internal cache seam: store and later rebind execution type for each caller row projection.
      execution: execution as unknown as RawQueryExecution<object>,
      refs: 1,
    });
    return yield* Effect.succeed(coerceRawQueryExecution<ResultRow, ResultRow>(execution));
  },
);

export const releaseRawQueryExecution = Effect.fn("ColumnLiveViewEngine.activeQuery.release")(
  function* <ResultRow extends RowObject>(
    store: ActiveQueryStoreState,
    compiled: CompiledRawQuery<object, ResultRow>,
  ) {
    const { map, key } = getActiveQueryEntry(store, compiled);
    const existing = map.get(key);
    if (existing === undefined) {
      return yield* Effect.succeed(undefined);
    }
    const entry = existing;
    if (entry.refs > 1) {
      entry.refs -= 1;
      return yield* Effect.succeed(undefined);
    }
    map.delete(key);
    if (map.size === 0) {
      activeQueryExecutionCache.delete(store);
    }
    return yield* Effect.succeed(undefined);
  },
);

export const clearStoreRawQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.clearStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => {
    activeQueryExecutionCache.delete(store);
  }),
);
