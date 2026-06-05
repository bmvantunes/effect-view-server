import { Effect } from "effect";
import {
  divideUnsafe,
  fromBigInt,
  fromNumberUnsafe,
  isBigDecimal,
  sum as sumBigDecimal,
  type BigDecimal,
} from "effect/BigDecimal";
import {
  InvalidQueryError,
  type RawQueryCompilerMetadata,
  prepareRawQuery,
} from "./raw-query-compiler";
import { compareQueryValue, stableQueryValueString } from "./raw-query-compiler";
import { cloneUnknown, fieldValue, isPlainRecord } from "./row-values";
import type { QueryEvaluation, StoredRowOf } from "./query-result";
import type { TopicRowChangeBatch, TopicRowScan } from "./row-scan";

type RowObject = object;

type RuntimeGroupedAggregate =
  | {
      readonly aggFunc: "count";
    }
  | {
      readonly aggFunc: "countDistinct" | "min" | "max" | "avg";
      readonly field: string;
    }
  | {
      readonly aggFunc: "sum";
      readonly field: string;
      readonly resultKind: "bigint" | "bigDecimal";
    };

type RuntimeGroupedOrderBy =
  | {
      readonly field: string;
      readonly direction: "asc" | "desc";
    }
  | {
      readonly aggregate: string;
      readonly direction: "asc" | "desc";
    };

export type RuntimeGroupedQuery = {
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly where?: Record<string, unknown>;
  readonly orderBy?: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly offset?: number;
  readonly limit?: number;
};

export type CompiledGroupedQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly query: RuntimeGroupedQuery;
  readonly cacheKey: string;
  readonly matches: (row: Row) => boolean;
  readonly evaluate: (store: TopicRowScan<Row>) => QueryEvaluation<ResultRow>;
};

type AggregateState =
  | {
      readonly aggFunc: "count";
      count: bigint;
    }
  | {
      readonly aggFunc: "countDistinct";
      readonly values: Set<string>;
      count: bigint;
    }
  | {
      readonly aggFunc: "sum";
      readonly resultKind: "bigint";
      bigintTotal: bigint;
    }
  | {
      readonly aggFunc: "sum";
      readonly resultKind: "bigDecimal";
      decimalTotal: BigDecimal;
    }
  | {
      readonly aggFunc: "avg";
      count: bigint;
      total: BigDecimal;
    }
  | {
      readonly aggFunc: "min" | "max";
      value: unknown;
      hasValue: boolean;
    };

type GroupState = {
  readonly key: string;
  readonly row: Record<string, unknown>;
  readonly aggregates: Record<string, AggregateState>;
};

type IncrementalGroupState = GroupState & {
  readonly members: Map<string, RowObject>;
};

export type IncrementalGroupedQueryExecution<ResultRow extends RowObject> = {
  readonly incremental: boolean;
  readonly latest: () => QueryEvaluation<ResultRow>;
};

const maxIncrementalGroupedMembers = 65_536;
const maxIncrementalGroupedMembersPerGroup = 4_096;
const maxIncrementalGroupedGroups = 8_192;

const groupedQueryKeys = new Set([
  "groupBy",
  "aggregates",
  "select",
  "where",
  "orderBy",
  "offset",
  "limit",
]);
const dangerousRecordKeys = new Set(["__proto__", "prototype", "constructor"]);
const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      return false;
    }
  }
  return true;
};

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const bigintToDecimal = (value: bigint): BigDecimal => fromBigInt(value);

const numberToDecimal = (value: number): BigDecimal => fromNumberUnsafe(value);

const runtimeValueToDecimal = (value: unknown): BigDecimal | undefined => {
  if (isBigDecimal(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return bigintToDecimal(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return numberToDecimal(value);
  }
  return undefined;
};

const emptyAggregateState = (aggregate: RuntimeGroupedAggregate): AggregateState => {
  if (aggregate.aggFunc === "count") {
    return {
      aggFunc: "count",
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "countDistinct") {
    return {
      aggFunc: "countDistinct",
      values: new Set<string>(),
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "sum") {
    return aggregate.resultKind === "bigint"
      ? {
          aggFunc: "sum",
          resultKind: "bigint",
          bigintTotal: 0n,
        }
      : {
          aggFunc: "sum",
          resultKind: "bigDecimal",
          decimalTotal: fromBigInt(0n),
        };
  }
  if (aggregate.aggFunc === "avg") {
    return {
      aggFunc: "avg",
      count: 0n,
      total: fromBigInt(0n),
    };
  }
  return {
    aggFunc: aggregate.aggFunc,
    value: undefined,
    hasValue: false,
  };
};

const aggregateFieldValue = (row: RowObject, aggregate: RuntimeGroupedAggregate): unknown =>
  "field" in aggregate ? fieldValue(row, aggregate.field) : undefined;

const updateAggregateState = (
  state: AggregateState,
  aggregate: RuntimeGroupedAggregate,
  row: RowObject,
): void => {
  const value = aggregateFieldValue(row, aggregate);
  if (state.aggFunc === "count") {
    state.count += 1n;
    return;
  }
  if (state.aggFunc === "countDistinct") {
    const sizeBefore = state.values.size;
    state.values.add(stableQueryValueString(value));
    if (state.values.size !== sizeBefore) {
      state.count += 1n;
    }
    return;
  }
  if (state.aggFunc === "sum") {
    if (state.resultKind === "bigint") {
      if (typeof value === "bigint") {
        state.bigintTotal += value;
      }
      return;
    }
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.decimalTotal = sumBigDecimal(state.decimalTotal, decimal);
    }
    return;
  }
  if (state.aggFunc === "avg") {
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.count += 1n;
      state.total = sumBigDecimal(state.total, decimal);
    }
    return;
  }
  if (!state.hasValue) {
    state.value = cloneUnknown(value);
    state.hasValue = true;
    return;
  }
  const comparison = compareQueryValue(value, state.value);
  if (comparison === 0) {
    return;
  }
  if ((state.aggFunc === "min" && comparison < 0) || (state.aggFunc === "max" && comparison > 0)) {
    state.value = cloneUnknown(value);
  }
};

const aggregateStateValue = (state: AggregateState): unknown => {
  if (state.aggFunc === "count") {
    return state.count;
  }
  if (state.aggFunc === "countDistinct") {
    return state.count;
  }
  if (state.aggFunc === "sum") {
    return state.resultKind === "bigint" ? state.bigintTotal : state.decimalTotal;
  }
  if (state.aggFunc === "avg") {
    return state.count === 0n ? fromBigInt(0n) : divideUnsafe(state.total, fromBigInt(state.count));
  }
  return cloneUnknown(state.value);
};

const groupKey = (groupBy: ReadonlyArray<string>, row: RowObject): string =>
  stableQueryValueString(groupBy.map((field) => [field, fieldValue(row, field)]));

const newGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
  row: RowObject,
): GroupState => {
  const resultRow: Record<string, unknown> = {};
  for (const field of groupBy) {
    resultRow[field] = cloneUnknown(fieldValue(row, field));
  }
  const aggregateStates: Record<string, AggregateState> = {};
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    aggregateStates[alias] = emptyAggregateState(aggregate);
  }
  return {
    key,
    row: resultRow,
    aggregates: aggregateStates,
  };
};

const newIncrementalGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
  row: RowObject,
): IncrementalGroupState => ({
  ...newGroupState(key, groupBy, aggregates, row),
  members: new Map(),
});

const resetAggregateStates = (
  group: GroupState,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): void => {
  for (const key of Object.keys(group.aggregates)) {
    delete group.aggregates[key];
  }
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    group.aggregates[alias] = emptyAggregateState(aggregate);
  }
};

const recomputeIncrementalGroupState = (
  group: IncrementalGroupState,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): void => {
  resetAggregateStates(group, aggregates);
  for (const row of group.members.values()) {
    for (const [alias, aggregate] of Object.entries(aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
  }
};

const finalizeGroup = (group: GroupState): StoredRowOf<RowObject> => {
  const row: Record<string, unknown> = { ...group.row };
  for (const [alias, state] of Object.entries(group.aggregates)) {
    row[alias] = aggregateStateValue(state);
  }
  return {
    key: group.key,
    row,
  };
};

const compareGroupedRows = (
  left: StoredRowOf<RowObject>,
  right: StoredRowOf<RowObject>,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): number => {
  for (const order of orderBy) {
    const field = "field" in order ? order.field : order.aggregate;
    const comparison = compareQueryValue(fieldValue(left.row, field), fieldValue(right.row, field));
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

const groupedEvaluationFromEntries = (
  entries: ReadonlyArray<StoredRowOf<RowObject>>,
  query: RuntimeGroupedQuery,
  version: number,
): QueryEvaluation<RowObject> => {
  const ordered = entries.toSorted((left, right) =>
    compareGroupedRows(left, right, query.orderBy ?? []),
  );
  const offset = query.offset ?? 0;
  const window = ordered.slice(
    offset,
    query.limit === undefined ? undefined : offset + query.limit,
  );
  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: ordered.length,
    version,
  };
};

const decodeGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.decode")((
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<RuntimeGroupedQuery, InvalidQueryError> => {
  if (!isPlainRecord(query)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query must be a plain object.",
    });
  }
  for (const key of Object.keys(query)) {
    if (!groupedQueryKeys.has(key)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query contains unsupported key: ${key}.`,
      });
    }
  }
  if (Object.hasOwn(query, "select")) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query must not include select.",
    });
  }

  const groupBy = query["groupBy"];
  if (!Array.isArray(groupBy) || groupBy.length === 0 || !isDenseArray(groupBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query groupBy must be a non-empty array of strings.",
    });
  }
  const decodedGroupBy: Array<string> = [];
  for (const field of groupBy) {
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: "Grouped query groupBy must be a non-empty array of strings.",
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query groupBy contains unknown field: ${field}.`,
      });
    }
    decodedGroupBy.push(field);
  }

  const aggregates = query["aggregates"];
  if (!isPlainRecord(aggregates) || Object.keys(aggregates).length === 0) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query aggregates must be a non-empty plain object.",
    });
  }
  const aggregateAliases = new Set(Object.keys(aggregates));
  for (const field of decodedGroupBy) {
    if (aggregateAliases.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate alias collides with groupBy field: ${field}.`,
      });
    }
  }
  const decodedAggregates: Record<string, RuntimeGroupedAggregate> = {};
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    if (dangerousRecordKeys.has(alias)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate alias is not allowed: ${alias}.`,
      });
    }
    if (!isPlainRecord(aggregate)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} must be a plain object.`,
      });
    }
    const aggregateKeys = Object.keys(aggregate);
    const aggFunc = aggregate["aggFunc"];
    if (
      aggFunc !== "count" &&
      aggFunc !== "countDistinct" &&
      aggFunc !== "sum" &&
      aggFunc !== "min" &&
      aggFunc !== "max" &&
      aggFunc !== "avg"
    ) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} has an unsupported aggFunc.`,
      });
    }
    if (aggFunc === "count") {
      if (aggregateKeys.some((key) => key !== "aggFunc")) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query count aggregate ${alias} must not include a field.`,
        });
      }
      decodedAggregates[alias] = { aggFunc };
      continue;
    }
    for (const key of aggregateKeys) {
      if (key !== "aggFunc" && key !== "field") {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} contains unsupported key: ${key}.`,
        });
      }
    }
    const field = aggregate["field"];
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} field must be a string.`,
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} contains unknown field: ${field}.`,
      });
    }
    if ((aggFunc === "sum" || aggFunc === "avg") && !metadata.numericFieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} must reference a numeric field.`,
      });
    }
    if (aggFunc === "sum") {
      const resultKind = metadata.fieldMetadata.get(field)?.sumResultKind;
      if (resultKind === undefined) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} must reference a numeric field.`,
        });
      }
      decodedAggregates[alias] = {
        aggFunc,
        field,
        resultKind,
      };
    } else {
      decodedAggregates[alias] = {
        aggFunc,
        field,
      };
    }
  }

  const where = query["where"];
  if (where !== undefined && !isPlainRecord(where)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query where must be a plain object.",
    });
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query orderBy must be an array.",
    });
  }
  const decodedOrderBy: Array<RuntimeGroupedOrderBy> = [];
  if (Array.isArray(orderBy)) {
    for (const entry of orderBy) {
      if (!isPlainRecord(entry)) {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy entries must be plain objects.",
        });
      }
      for (const key of Object.keys(entry)) {
        if (key !== "field" && key !== "aggregate" && key !== "direction") {
          return InvalidQueryError.make({
            topic,
            message: `Grouped query orderBy contains unsupported key: ${key}.`,
          });
        }
      }
      const direction = entry["direction"];
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy direction must be asc or desc.",
        });
      }
      const hasField = Object.hasOwn(entry, "field");
      const hasAggregate = Object.hasOwn(entry, "aggregate");
      if (hasField === hasAggregate) {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy entries must choose field or aggregate.",
        });
      }
      if (hasField) {
        const field = entry["field"];
        if (typeof field !== "string" || !decodedGroupBy.includes(field)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy field must be present in groupBy.",
          });
        }
        decodedOrderBy.push({ field, direction });
      } else {
        const aggregate = entry["aggregate"];
        if (typeof aggregate !== "string" || !aggregateAliases.has(aggregate)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy aggregate must reference an aggregate alias.",
          });
        }
        decodedOrderBy.push({ aggregate, direction });
      }
    }
  }

  const offset = query["offset"];
  if (offset !== undefined && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query offset must be a non-negative safe integer.",
    });
  }

  const limit = query["limit"];
  if (limit !== undefined && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query limit must be a non-negative safe integer.",
    });
  }

  return Effect.succeed({
    groupBy: decodedGroupBy,
    aggregates: decodedAggregates,
    ...(where === undefined ? {} : { where }),
    ...(decodedOrderBy.length === 0 ? {} : { orderBy: decodedOrderBy }),
    ...(offset === undefined ? {} : { offset }),
    ...(limit === undefined ? {} : { limit }),
  });
});

const groupedCacheKey = (query: RuntimeGroupedQuery): string =>
  stableQueryValueString([
    "grouped",
    query.groupBy,
    Object.entries(query.aggregates).toSorted(
      ([left], [right]) => Number(left > right) - Number(left < right),
    ),
    query.where === undefined ? [] : stableQueryValueString(query.where),
    query.orderBy ?? [],
    query.offset ?? null,
    query.limit ?? null,
  ]);

function typedEvaluation<ResultRow extends RowObject>(
  evaluation: QueryEvaluation<RowObject>,
): QueryEvaluation<ResultRow>;
function typedEvaluation(evaluation: QueryEvaluation<RowObject>): QueryEvaluation<RowObject> {
  return evaluation;
}

export const prepareGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.prepare")(
  function* <Row extends RowObject, ResultRow extends RowObject>(
    topic: string,
    metadata: RawQueryCompilerMetadata,
    query: unknown,
  ) {
    const decoded = yield* decodeGroupedQuery(topic, metadata, query);
    const rawFilter = yield* prepareRawQuery<Row, RowObject>(topic, metadata, {
      select: decoded.groupBy,
      ...(decoded.where === undefined ? {} : { where: decoded.where }),
    });
    const { matches } = rawFilter.predicate;
    return {
      query: decoded,
      cacheKey: groupedCacheKey(decoded),
      matches,
      evaluate: (store) => typedEvaluation<ResultRow>(evaluateGroupedRows(store, decoded, matches)),
    } satisfies CompiledGroupedQuery<Row, ResultRow>;
  },
);

const evaluateGroupedRows = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
): QueryEvaluation<RowObject> => {
  const groups = new Map<string, GroupState>();
  store.scanRows((_key, row) => {
    if (!matches(row)) {
      return;
    }
    const key = groupKey(query.groupBy, row);
    let group = groups.get(key);
    if (group === undefined) {
      group = newGroupState(key, query.groupBy, query.aggregates, row);
      groups.set(key, group);
    }
    for (const [alias, aggregate] of Object.entries(query.aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
  });
  return groupedEvaluationFromEntries(
    Array.from(groups.values(), finalizeGroup),
    query,
    store.version(),
  );
};

type IncrementalGroupedQueryState = {
  readonly groups: Map<string, IncrementalGroupState>;
  evaluation: QueryEvaluation<RowObject>;
  memberCount: number;
  version: number;
};

type IncrementalGroupedQueryBuildState =
  | {
      readonly admitted: false;
    }
  | {
      readonly admitted: true;
      readonly state: IncrementalGroupedQueryState;
    };

const evaluateIncrementalGroupedQuery = (
  groups: ReadonlyMap<string, IncrementalGroupState>,
  query: RuntimeGroupedQuery,
  version: number,
): QueryEvaluation<RowObject> =>
  groupedEvaluationFromEntries(Array.from(groups.values(), finalizeGroup), query, version);

const clearIncrementalGroupedQueryState = (
  state: IncrementalGroupedQueryState,
  query: RuntimeGroupedQuery,
  version: number,
): void => {
  state.groups.clear();
  state.evaluation = groupedEvaluationFromEntries([], query, version);
  state.memberCount = 0;
  state.version = version;
};

const buildIncrementalGroupedQueryState = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
): IncrementalGroupedQueryBuildState => {
  const groups = new Map<string, IncrementalGroupState>();
  let memberCount = 0;
  let admitted = true;
  store.scanRows((key, row) => {
    if (!admitted) {
      return undefined;
    }
    if (!matches(row)) {
      return undefined;
    }
    const groupedKey = groupKey(query.groupBy, row);
    let group = groups.get(groupedKey);
    if (group === undefined) {
      group = newIncrementalGroupState(groupedKey, query.groupBy, query.aggregates, row);
      groups.set(groupedKey, group);
    }
    group.members.set(key, row);
    memberCount += 1;
    if (
      memberCount > maxIncrementalGroupedMembers ||
      group.members.size > maxIncrementalGroupedMembersPerGroup ||
      groups.size > maxIncrementalGroupedGroups
    ) {
      groups.clear();
      admitted = false;
      return false;
    }
    for (const [alias, aggregate] of Object.entries(query.aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
    return undefined;
  });
  if (!admitted) {
    return {
      admitted: false,
    };
  }
  const version = store.version();
  return {
    admitted: true,
    state: {
      groups,
      evaluation: evaluateIncrementalGroupedQuery(groups, query, version),
      memberCount,
      version,
    },
  };
};

const removeIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, IncrementalGroupState>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
  key: string,
  row: Row,
  dirtyGroups: Set<IncrementalGroupState>,
): boolean => {
  if (!matches(row)) {
    return false;
  }
  const groupedKey = groupKey(query.groupBy, row);
  const group = groups.get(groupedKey);
  if (group === undefined) {
    return false;
  }
  const removed = group.members.delete(key);
  if (!removed) {
    return false;
  }
  if (group.members.size === 0) {
    groups.delete(groupedKey);
    dirtyGroups.delete(group);
    return true;
  }
  dirtyGroups.add(group);
  return true;
};

type UpsertIncrementalGroupedMemberResult = {
  readonly groupSize: number;
  readonly inserted: boolean;
};

const upsertIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, IncrementalGroupState>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
  key: string,
  row: Row,
  dirtyGroups: Set<IncrementalGroupState>,
): UpsertIncrementalGroupedMemberResult | undefined => {
  if (!matches(row)) {
    return undefined;
  }
  const groupedKey = groupKey(query.groupBy, row);
  let group = groups.get(groupedKey);
  if (group === undefined) {
    group = newIncrementalGroupState(groupedKey, query.groupBy, query.aggregates, row);
    groups.set(groupedKey, group);
  }
  const inserted = !group.members.has(key);
  group.members.set(key, row);
  dirtyGroups.add(group);
  return {
    groupSize: group.members.size,
    inserted,
  };
};

const applyIncrementalGroupedQueryBatch = <Row extends RowObject>(
  state: IncrementalGroupedQueryState,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
  batch: TopicRowChangeBatch<Row>,
  dirtyGroups: Set<IncrementalGroupState>,
): boolean => {
  for (const change of batch.changes) {
    if (change.previous !== undefined) {
      const removed = removeIncrementalGroupedMember(
        state.groups,
        query,
        matches,
        change.key,
        change.previous,
        dirtyGroups,
      );
      if (removed) {
        state.memberCount -= 1;
      }
    }
    if (change.next !== undefined) {
      const upserted = upsertIncrementalGroupedMember(
        state.groups,
        query,
        matches,
        change.key,
        change.next,
        dirtyGroups,
      );
      if (upserted === undefined) {
        continue;
      }
      if (upserted.groupSize > maxIncrementalGroupedMembersPerGroup) {
        return false;
      }
      if (state.groups.size > maxIncrementalGroupedGroups) {
        return false;
      }
      if (upserted.inserted) {
        state.memberCount += 1;
        if (state.memberCount > maxIncrementalGroupedMembers) {
          return false;
        }
      }
    }
  }
  return true;
};

const applyIncrementalGroupedQueryBatches = <Row extends RowObject>(
  state: IncrementalGroupedQueryState,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
  batches: ReadonlyArray<TopicRowChangeBatch<Row>>,
): boolean => {
  const dirtyGroups = new Set<IncrementalGroupState>();
  for (const batch of batches) {
    if (!applyIncrementalGroupedQueryBatch(state, query, matches, batch, dirtyGroups)) {
      state.groups.clear();
      state.memberCount = 0;
      return false;
    }
    state.version = batch.version;
  }
  for (const group of dirtyGroups) {
    recomputeIncrementalGroupState(group, query.aggregates);
  }
  state.evaluation = evaluateIncrementalGroupedQuery(state.groups, query, state.version);
  return true;
};

const makeFallbackGroupedQueryExecution = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): IncrementalGroupedQueryExecution<ResultRow> => {
  let snapshot = {
    evaluation: compiled.evaluate(store),
    version: store.version(),
  };
  return {
    incremental: false,
    latest: () => {
      const storeVersion = store.version();
      if (snapshot.version !== storeVersion) {
        snapshot = {
          evaluation: compiled.evaluate(store),
          version: storeVersion,
        };
      }
      return snapshot.evaluation;
    },
  };
};

export const makeIncrementalGroupedQueryExecution = <
  Row extends RowObject,
  ResultRow extends RowObject,
>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
  releaseRetainedChanges: () => void,
): IncrementalGroupedQueryExecution<ResultRow> => {
  let build = buildIncrementalGroupedQueryState(store, compiled.query, compiled.matches);
  if (!build.admitted) {
    return makeFallbackGroupedQueryExecution(store, compiled);
  }
  let state = build.state;
  let fallback: IncrementalGroupedQueryExecution<ResultRow> | undefined;
  const activateFallback = (): IncrementalGroupedQueryExecution<ResultRow> => {
    clearIncrementalGroupedQueryState(state, compiled.query, store.version());
    const nextFallback = makeFallbackGroupedQueryExecution(store, compiled);
    fallback = nextFallback;
    releaseRetainedChanges();
    return nextFallback;
  };
  return {
    get incremental() {
      return fallback === undefined;
    },
    latest: () => {
      if (fallback !== undefined) {
        return fallback.latest();
      }
      const storeVersion = store.version();
      if (state.version === storeVersion) {
        return typedEvaluation<ResultRow>(state.evaluation);
      }
      const batches = store.changesSince(state.version);
      if (batches === undefined) {
        build = buildIncrementalGroupedQueryState(store, compiled.query, compiled.matches);
        if (!build.admitted) {
          return activateFallback().latest();
        }
        state = build.state;
        return typedEvaluation<ResultRow>(state.evaluation);
      }
      if (!applyIncrementalGroupedQueryBatches(state, compiled.query, compiled.matches, batches)) {
        return activateFallback().latest();
      }
      return typedEvaluation<ResultRow>(state.evaluation);
    },
  };
};

export const evaluateCompiledGroupedQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> => compiled.evaluate(store);
