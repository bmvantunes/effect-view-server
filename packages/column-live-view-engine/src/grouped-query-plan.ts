import {
  groupAggregateStateCompareValue,
  type GroupedAggregateInput,
  type GroupedAggregateInputSemantics,
  type GroupedAggregatePlan,
  type GroupState,
  type RuntimeGroupedAggregate,
} from "./grouped-aggregate-state";
import {
  compileGroupedKeyIdentity,
  missingSchemaValuePresenceToken,
  presentSchemaValuePresenceToken,
  schemaValuePresenceKey,
  type GroupedKeyIdentityField,
} from "@effect-view-server/effect-utils";
import { Option } from "effect";
import {
  isRuntimeGroupedFieldOrderBy,
  type RuntimeGroupedOrderBy,
  type RuntimeGroupedQuery,
} from "./grouped-query-decoder";
import { stableQueryValueString } from "./query-value";
import type { StoredRowOf } from "./query-result";
import {
  groupedResultAggregateSemantics,
  runtimeGroupedQueryResultSemantics,
  type QueryResultSemantics,
} from "./query-result-semantics";
import { trustedFieldValue } from "./row-values";
import type { SchemaValueSemantics, TopicRowValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

export type GroupedQueryPlanInput = RuntimeGroupedQuery;
export type { RuntimeGroupedOrderBy };

export type CompiledGroupedOrderBy = {
  readonly compare: (left: unknown, right: unknown) => number;
  readonly direction: "asc" | "desc";
  readonly groupValue: (group: GroupState) => unknown;
  readonly rowValue: (entry: StoredRowOf<RowObject>) => unknown;
};

const missingGroupedValueKey = schemaValuePresenceKey(missingSchemaValuePresenceToken);

export type GroupedQueryPlan<Row extends RowObject, ResultRow extends RowObject = RowObject> = {
  readonly cacheKey: string;
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly aggregatePlans: ReadonlyArray<GroupedAggregatePlan>;
  readonly orderBy: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly compiledOrderBy: ReadonlyArray<CompiledGroupedOrderBy>;
  readonly offset: number;
  readonly limit: number | undefined;
  readonly resultSemantics: QueryResultSemantics<ResultRow>;
  readonly zeroLimit: boolean;
  readonly groupKey: (row: Row) => string;
};

const groupedQueryPlanCacheKey = (
  query: GroupedQueryPlanInput,
  rawPredicateCacheKey: string,
): string =>
  stableQueryValueString([
    "grouped",
    query.groupBy,
    Object.entries(query.aggregates).toSorted(
      ([left], [right]) => Number(left > right) - Number(left < right),
    ),
    rawPredicateCacheKey,
    query.orderBy ?? [],
    query.offset ?? null,
    query.limit ?? null,
  ]);

const immutableGroupedAggregate = (aggregate: RuntimeGroupedAggregate): RuntimeGroupedAggregate => {
  const aggFunc = aggregate.aggFunc;
  if (aggFunc === "count") {
    return Object.freeze({ aggFunc });
  }
  const field = aggregate.field;
  if (aggFunc === "sum") {
    return Object.freeze({
      aggFunc,
      field,
      resultKind: aggregate.resultKind,
    });
  }
  return Object.freeze({ aggFunc, field });
};

const immutableGroupedAggregates = (
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): Readonly<Record<string, RuntimeGroupedAggregate>> =>
  Object.freeze(
    Object.fromEntries(
      Object.entries(aggregates).map(([alias, aggregate]) => [
        alias,
        immutableGroupedAggregate(aggregate),
      ]),
    ),
  );

const immutableGroupedOrderBy = (
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy> | undefined,
): ReadonlyArray<RuntimeGroupedOrderBy> =>
  Object.freeze(
    (orderBy ?? []).map((order) =>
      isRuntimeGroupedFieldOrderBy(order)
        ? Object.freeze({ field: order.field, direction: order.direction })
        : Object.freeze({ aggregate: order.aggregate, direction: order.direction }),
    ),
  );

const immutableGroupedQuery = (query: GroupedQueryPlanInput): GroupedQueryPlanInput => {
  const groupBy = Object.freeze([...query.groupBy]);
  const aggregates = immutableGroupedAggregates(query.aggregates);
  const orderBy = immutableGroupedOrderBy(query.orderBy);
  const offset = query.offset;
  const limit = query.limit;
  return Object.freeze({
    groupBy,
    aggregates,
    ...(orderBy.length === 0 ? {} : { orderBy }),
    ...(offset === undefined ? {} : { offset }),
    ...(limit === undefined ? {} : { limit }),
  });
};

const compileGroupedKeyFields = (
  valueSemantics: TopicRowValueSemantics,
  groupBy: ReadonlyArray<string>,
): ReadonlyArray<GroupedKeyIdentityField> =>
  Object.freeze(
    groupBy.map((field) => {
      const semantics = valueSemantics.field(field);
      return Object.freeze({
        field,
        canonicalKey: semantics.canonicalKey,
      });
    }),
  );

const compileGroupedAggregates = (
  valueSemantics: TopicRowValueSemantics,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): ReadonlyArray<GroupedAggregatePlan> =>
  Object.freeze(
    Object.entries(aggregates).map(([alias, aggregate], stateIndex) => {
      const resultSemantics = groupedResultAggregateSemantics(valueSemantics, aggregate);
      if (aggregate.aggFunc === "count") {
        return Object.freeze({
          kind: "count",
          alias,
          aggregate,
          resultSemantics,
          stateIndex,
        });
      }
      const field = aggregate.field;
      const fieldSemantics = valueSemantics.field(field);
      const input: GroupedAggregateInputSemantics = Object.freeze({
        field,
        canonicalKey: (aggregateInput) =>
          aggregateInput._tag === "Missing"
            ? missingGroupedValueKey
            : schemaValuePresenceKey(
                presentSchemaValuePresenceToken(fieldSemantics.canonicalKey(aggregateInput.value)),
              ),
        compare: (left, right) => {
          if (left._tag === "Missing") {
            return right._tag === "Missing" ? 0 : -1;
          }
          if (right._tag === "Missing") {
            return 1;
          }
          return fieldSemantics.compare(left.value, right.value);
        },
        equivalent: (left, right) => {
          if (left._tag === "Missing") {
            return right._tag === "Missing";
          }
          return right._tag === "Present" && fieldSemantics.equivalent(left.value, right.value);
        },
        read: (row) =>
          Object.prototype.propertyIsEnumerable.call(row, field)
            ? {
                _tag: "Present",
                value: trustedFieldValue(row, field),
              }
            : missingGroupedAggregateInput,
      });
      return Object.freeze({
        kind: "field",
        alias,
        aggregate,
        input,
        resultSemantics,
        stateIndex,
      });
    }),
  );

const missingGroupedAggregateInput: GroupedAggregateInput = Object.freeze({
  _tag: "Missing",
});

const groupedFieldOrderColumn = (
  field: string,
  direction: "asc" | "desc",
  semantics: SchemaValueSemantics,
): CompiledGroupedOrderBy =>
  Object.freeze({
    compare: semantics.compare,
    direction,
    groupValue: (group) => trustedFieldValue(group.row, field),
    rowValue: (entry) => trustedFieldValue(entry.row, field),
  });

const groupedAggregateOrderColumn = (
  aggregatePlan: GroupedAggregatePlan,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy =>
  Object.freeze({
    compare: aggregatePlan.resultSemantics.compare,
    direction,
    groupValue: (group) => groupAggregateStateCompareValue(group, aggregatePlan.stateIndex),
    rowValue: (entry) => trustedFieldValue(entry.row, aggregatePlan.alias),
  });

const compileGroupedOrderBy = (
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
  valueSemantics: TopicRowValueSemantics,
  aggregatePlans: ReadonlyArray<GroupedAggregatePlan>,
): ReadonlyArray<CompiledGroupedOrderBy> =>
  Object.freeze(
    orderBy.map((order) =>
      isRuntimeGroupedFieldOrderBy(order)
        ? groupedFieldOrderColumn(order.field, order.direction, valueSemantics.field(order.field))
        : groupedAggregateOrderColumn(
            Option.getOrThrow(
              Option.fromNullishOr(
                aggregatePlans.find((aggregatePlan) => aggregatePlan.alias === order.aggregate),
              ),
            ),
            order.direction,
          ),
    ),
  );

export const makeGroupedQueryPlan = <Row extends RowObject, ResultRow extends RowObject>(
  query: GroupedQueryPlanInput,
  valueSemantics: TopicRowValueSemantics,
  rawPredicateCacheKey: string,
  makeResultSemantics: (
    groupBy: ReadonlyArray<string>,
    aggregatePlans: ReadonlyArray<GroupedAggregatePlan>,
  ) => QueryResultSemantics<ResultRow>,
): GroupedQueryPlan<Row, ResultRow> => {
  const immutableQuery = immutableGroupedQuery(query);
  const groupBy = immutableQuery.groupBy;
  const groupedKeyIdentity = compileGroupedKeyIdentity<Row>(
    compileGroupedKeyFields(valueSemantics, groupBy),
    "throw",
  );
  const aggregatePlans = compileGroupedAggregates(valueSemantics, immutableQuery.aggregates);
  const resultSemantics = Object.freeze(makeResultSemantics(groupBy, aggregatePlans));
  const orderBy = immutableQuery.orderBy ?? Object.freeze([]);
  return Object.freeze({
    cacheKey: groupedQueryPlanCacheKey(immutableQuery, rawPredicateCacheKey),
    groupBy,
    aggregates: immutableQuery.aggregates,
    aggregatePlans,
    orderBy,
    compiledOrderBy: compileGroupedOrderBy(orderBy, valueSemantics, aggregatePlans),
    offset: immutableQuery.offset ?? 0,
    limit: immutableQuery.limit,
    resultSemantics,
    zeroLimit: immutableQuery.limit === 0,
    groupKey: groupedKeyIdentity.key,
  });
};

export const makeRuntimeGroupedQueryPlan = <Row extends RowObject = RowObject>(
  query: GroupedQueryPlanInput,
  valueSemantics: TopicRowValueSemantics,
  rawPredicateCacheKey: string,
): GroupedQueryPlan<Row, RowObject> =>
  makeGroupedQueryPlan(query, valueSemantics, rawPredicateCacheKey, (groupBy, aggregatePlans) =>
    runtimeGroupedQueryResultSemantics(valueSemantics, groupBy, aggregatePlans),
  );
