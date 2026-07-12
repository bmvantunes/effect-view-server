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
import { stableQueryValueString } from "./query-value";
import type { StoredRowOf } from "./query-result";
import {
  groupedQueryResultSemantics,
  groupedResultAggregateSemantics,
  type QueryResultSemantics,
} from "./query-result-semantics";
import { trustedFieldValue } from "./row-values";
import type { SchemaValueSemantics, TopicRowValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

export type RuntimeGroupedOrderBy =
  | {
      readonly field: string;
      readonly direction: "asc" | "desc";
    }
  | {
      readonly aggregate: string;
      readonly direction: "asc" | "desc";
    };

export type GroupedQueryPlanInput = {
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly where?: Record<string, unknown>;
  readonly orderBy?: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly offset?: number;
  readonly limit?: number;
};

export type CompiledGroupedOrderBy = {
  readonly compare: (left: unknown, right: unknown) => number;
  readonly direction: "asc" | "desc";
  readonly groupValue: (group: GroupState) => unknown;
  readonly rowValue: (entry: StoredRowOf<RowObject>) => unknown;
};

const missingGroupedValueKey = schemaValuePresenceKey(missingSchemaValuePresenceToken);

export type GroupedQueryPlan<Row extends RowObject> = {
  readonly query: GroupedQueryPlanInput;
  readonly cacheKey: string;
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly aggregatePlans: ReadonlyArray<GroupedAggregatePlan>;
  readonly orderBy: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly compiledOrderBy: ReadonlyArray<CompiledGroupedOrderBy>;
  readonly offset: number;
  readonly limit: number | undefined;
  readonly resultSemantics: QueryResultSemantics;
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

const compileGroupedKeyFields = (
  valueSemantics: TopicRowValueSemantics,
  groupBy: ReadonlyArray<string>,
): ReadonlyArray<GroupedKeyIdentityField> =>
  groupBy.map((field) => {
    const semantics = valueSemantics.field(field);
    return {
      field,
      canonicalKey: semantics.canonicalKey,
    };
  });

const compileGroupedAggregates = (
  valueSemantics: TopicRowValueSemantics,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): ReadonlyArray<GroupedAggregatePlan> =>
  Object.entries(aggregates).map(([alias, aggregate], stateIndex) => {
    const resultSemantics = groupedResultAggregateSemantics(valueSemantics, aggregate);
    if (aggregate.aggFunc === "count") {
      return {
        kind: "count",
        alias,
        aggregate,
        resultSemantics,
        stateIndex,
      };
    }
    const fieldSemantics = valueSemantics.field(aggregate.field);
    const input: GroupedAggregateInputSemantics = {
      field: aggregate.field,
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
        Object.prototype.propertyIsEnumerable.call(row, aggregate.field)
          ? {
              _tag: "Present",
              value: trustedFieldValue(row, aggregate.field),
            }
          : missingGroupedAggregateInput,
    };
    return {
      kind: "field",
      alias,
      aggregate,
      input,
      resultSemantics,
      stateIndex,
    };
  });

const missingGroupedAggregateInput: GroupedAggregateInput = {
  _tag: "Missing",
};

const groupedFieldOrderColumn = (
  field: string,
  direction: "asc" | "desc",
  semantics: SchemaValueSemantics,
): CompiledGroupedOrderBy => ({
  compare: semantics.compare,
  direction,
  groupValue: (group) => trustedFieldValue(group.row, field),
  rowValue: (entry) => trustedFieldValue(entry.row, field),
});

const groupedAggregateOrderColumn = (
  aggregatePlan: GroupedAggregatePlan,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy => ({
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
  orderBy.map((order) =>
    "field" in order
      ? groupedFieldOrderColumn(order.field, order.direction, valueSemantics.field(order.field))
      : groupedAggregateOrderColumn(
          Option.getOrThrow(
            Option.fromNullishOr(
              aggregatePlans.find((aggregatePlan) => aggregatePlan.alias === order.aggregate),
            ),
          ),
          order.direction,
        ),
  );

export const makeGroupedQueryPlan = <Row extends RowObject>(
  query: GroupedQueryPlanInput,
  valueSemantics: TopicRowValueSemantics,
  rawPredicateCacheKey: string,
): GroupedQueryPlan<Row> => {
  const groupBy = [...query.groupBy];
  const groupedKeyIdentity = compileGroupedKeyIdentity<Row>(
    compileGroupedKeyFields(valueSemantics, groupBy),
    "throw",
  );
  const aggregatePlans = compileGroupedAggregates(valueSemantics, query.aggregates);
  const orderBy = query.orderBy === undefined ? [] : [...query.orderBy];
  return {
    query,
    cacheKey: groupedQueryPlanCacheKey(query, rawPredicateCacheKey),
    groupBy,
    aggregates: query.aggregates,
    aggregatePlans,
    orderBy,
    compiledOrderBy: compileGroupedOrderBy(orderBy, valueSemantics, aggregatePlans),
    offset: query.offset ?? 0,
    limit: query.limit,
    resultSemantics: groupedQueryResultSemantics(valueSemantics, groupBy, aggregatePlans),
    zeroLimit: query.limit === 0,
    groupKey: groupedKeyIdentity.key,
  };
};
