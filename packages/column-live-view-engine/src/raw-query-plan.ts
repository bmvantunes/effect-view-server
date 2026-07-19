import { compareWireSafeBigDecimal } from "@effect-view-server/effect-utils";
import { isBigDecimal } from "effect/BigDecimal";
import { compareQueryValue, stableQueryValueString } from "./query-value";
import { compileRawPredicate, type CompiledRawPredicate } from "./raw-predicate-compiler";
import type { RuntimeRawQuery } from "./raw-query-decoder";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawOrderByPlan, TopicRawWindowScanPlan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import type { TopicStorageProjectableQueryResultSemantics } from "./query-result-semantics";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";
import { trustedFieldValue } from "./row-values";

type RowObject = object;

type QueryCacheToken =
  | readonly ["raw", string, string]
  | readonly ["raw", string, string, readonly ["partition", string]];
type QueryWindowCacheToken = readonly ["window", string, string];

export type RawQueryPlanWindow = {
  readonly cacheKey: string;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type RawQueryPlan<Row extends RowObject, ResultRow extends RowObject> = {
  readonly candidateStorageKeys?: () => Iterable<string>;
  readonly partitionKey?: string;
  readonly queryCacheKey: string;
  readonly selectedFields: ReadonlyArray<string>;
  readonly predicate: CompiledRawPredicate<Row>;
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly storageOrderBy?: ReadonlyArray<TopicRawOrderByPlan>;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
  readonly project: (row: Row) => ResultRow;
  readonly resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>;
  readonly window: RawQueryPlanWindow;
};

type RawRowOrderColumn<Row extends RowObject> = {
  readonly compareRows: (left: Row, right: Row) => number;
  readonly direction: "asc" | "desc";
};

const rawQueryShapeCacheKey = (
  query: RuntimeRawQuery,
  partition: ColumnLiveViewEngineQueryPartition | undefined,
): string => {
  const orderBy: ReadonlyArray<readonly [string, "asc" | "desc"]> =
    query.orderBy === undefined ? [] : query.orderBy.map((entry) => [entry.field, entry.direction]);
  const where = query.where?.key ?? null;
  const base = ["raw", stableQueryValueString(where), stableQueryValueString(orderBy)] as const;
  const token: QueryCacheToken =
    partition === undefined ? base : ["raw", base[1], base[2], ["partition", partition.key]];
  return JSON.stringify(token);
};

const rawQueryWindowCacheKey = (offset: number, limit: number | undefined): string => {
  const token: QueryWindowCacheToken = [
    "window",
    stableQueryValueString(offset),
    stableQueryValueString(limit ?? null),
  ];
  return JSON.stringify(token);
};

export const rawQueryPlanWindow = (offset: number, limit: number | undefined): RawQueryPlanWindow =>
  Object.freeze({
    cacheKey: rawQueryWindowCacheKey(offset, limit),
    offset,
    limit,
  });

const rawQueryPlanWindowFromQuery = (query: RuntimeRawQuery): RawQueryPlanWindow =>
  rawQueryPlanWindow(query.offset ?? 0, query.limit);

const compareStringRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = trustedFieldValue(left, field);
  const rightValue = trustedFieldValue(right, field);
  if (typeof leftValue === "string" && typeof rightValue === "string") {
    return Number(leftValue > rightValue) - Number(leftValue < rightValue);
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareNumberRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = trustedFieldValue(left, field);
  const rightValue = trustedFieldValue(right, field);
  if (
    typeof leftValue === "number" &&
    typeof rightValue === "number" &&
    Number.isFinite(leftValue) &&
    Number.isFinite(rightValue)
  ) {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareBigintRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = trustedFieldValue(left, field);
  const rightValue = trustedFieldValue(right, field);
  if (typeof leftValue === "bigint" && typeof rightValue === "bigint") {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareBigDecimalRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = trustedFieldValue(left, field);
  const rightValue = trustedFieldValue(right, field);
  if (isBigDecimal(leftValue) && isBigDecimal(rightValue)) {
    const comparison = compareWireSafeBigDecimal(leftValue, rightValue);
    if (comparison !== undefined) {
      return comparison;
    }
  }
  return compareQueryValue(leftValue, rightValue);
};

const rawRowOrderColumnComparator = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  field: string,
): ((left: Row, right: Row) => number) => {
  const exactScalarEquality = metadata.exactScalarEqualityFieldNames.has(field);
  if (exactScalarEquality && metadata.stringFieldNames.has(field)) {
    return (left, right) => compareStringRowFieldValues(left, right, field);
  }
  if (exactScalarEquality && metadata.numberFieldNames.has(field)) {
    return (left, right) => compareNumberRowFieldValues(left, right, field);
  }
  if (exactScalarEquality && metadata.bigintFieldNames.has(field)) {
    return (left, right) => compareBigintRowFieldValues(left, right, field);
  }
  if (exactScalarEquality && metadata.bigDecimalFieldNames.has(field)) {
    return (left, right) => compareBigDecimalRowFieldValues(left, right, field);
  }
  const compare = metadata.valueSemantics.field(field).compare;
  return (left, right) => compare(trustedFieldValue(left, field), trustedFieldValue(right, field));
};

const storageOrderBy = (
  metadata: RawQueryCompilerMetadata,
  orderBy: ReadonlyArray<TopicRawOrderByPlan>,
): ReadonlyArray<TopicRawOrderByPlan> | undefined => {
  for (const order of orderBy) {
    if (
      !metadata.exactScalarEqualityFieldNames.has(order.field) ||
      (!metadata.stringFieldNames.has(order.field) &&
        !metadata.numberFieldNames.has(order.field) &&
        !metadata.bigintFieldNames.has(order.field) &&
        !metadata.bigDecimalFieldNames.has(order.field))
    ) {
      return undefined;
    }
  }
  return orderBy;
};

const compiledRawRowOrder = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  orderBy: ReadonlyArray<TopicRawOrderByPlan>,
): ReadonlyArray<RawRowOrderColumn<Row>> =>
  Object.freeze(
    orderBy.map((order) =>
      Object.freeze({
        compareRows: rawRowOrderColumnComparator<Row>(metadata, order.field),
        direction: order.direction,
      }),
    ),
  );

const compareRows = <Row extends RowObject>(
  left: TopicRowEntry<Row>,
  right: TopicRowEntry<Row>,
  orderBy: ReadonlyArray<RawRowOrderColumn<Row>>,
): number => {
  for (const order of orderBy) {
    const comparison = order.compareRows(left.row, right.row);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

export const makeRawQueryPlan = <
  Row extends RowObject,
  ResultRow extends RowObject,
  SchemaRow extends RowObject = RowObject,
>(
  metadata: RawQueryCompilerMetadata<SchemaRow>,
  query: RuntimeRawQuery,
  resultSemantics: TopicStorageProjectableQueryResultSemantics<ResultRow>,
  partition?: ColumnLiveViewEngineQueryPartition,
): RawQueryPlan<Row, ResultRow> => {
  const orderBy = Object.freeze(
    (query.orderBy ?? []).map((order) =>
      Object.freeze({
        field: order.field,
        direction: order.direction,
      }),
    ),
  );
  const rowOrderBy = compiledRawRowOrder<Row>(metadata, orderBy);
  const selectedFields = Object.freeze([...query.select]);
  const localPredicate = compileRawPredicate<Row>(metadata, query.where);
  const predicate: CompiledRawPredicate<Row> =
    partition === undefined
      ? localPredicate
      : Object.freeze({
          plan: Object.freeze({
            filters: localPredicate.plan.filters,
            callbackRequired: true,
            callbackSkippable: false,
          }),
          matches: (row: Row, storageKey?: string) =>
            partition.matches(row, storageKey) && localPredicate.matches(row, storageKey),
        });
  const storageOrder = storageOrderBy(metadata, orderBy);
  return Object.freeze({
    ...(partition === undefined ? {} : { candidateStorageKeys: partition.ownedStorageKeys }),
    ...(partition === undefined ? {} : { partitionKey: partition.key }),
    queryCacheKey: rawQueryShapeCacheKey(query, partition),
    selectedFields,
    predicate,
    orderBy,
    ...(storageOrder === undefined ? {} : { storageOrderBy: storageOrder }),
    compare: (left, right) => compareRows(left, right, rowOrderBy),
    project: resultSemantics.projectRow,
    resultSemantics,
    window: rawQueryPlanWindowFromQuery(query),
  });
};

export const rawQueryWindowScanPlan = <Row extends RowObject, ResultRow extends RowObject>(
  plan: RawQueryPlan<Row, ResultRow>,
  window: RawQueryPlanWindow,
): TopicRawWindowScanPlan<Row> => ({
  ...(plan.candidateStorageKeys === undefined
    ? {}
    : { candidateStorageKeys: plan.candidateStorageKeys }),
  predicate: plan.predicate.plan,
  orderBy: plan.orderBy,
  ...(plan.storageOrderBy === undefined ? {} : { storageOrderBy: plan.storageOrderBy }),
  matches: plan.predicate.matches,
  compare: plan.compare,
  offset: window.offset,
  limit: window.limit,
});
