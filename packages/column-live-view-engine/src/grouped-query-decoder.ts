import { Effect, Result } from "effect";
import type { GroupedQuery } from "@effect-view-server/config";
import type { RuntimeGroupedAggregate } from "./grouped-aggregate-state";
import { denseArraySnapshot, InvalidQueryError } from "./raw-query-decoder";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import {
  FilterExpressionError,
  normalizeWhere,
  type RuntimeFilterExpression,
} from "./filter-expression";
import { isPlainRecord } from "./row-values";

export type RuntimeGroupedOrderBy =
  | {
      readonly field: string;
      readonly direction: "asc" | "desc";
    }
  | {
      readonly aggregate: string;
      readonly direction: "asc" | "desc";
    };

export type RuntimeGroupedFieldOrderBy = Extract<RuntimeGroupedOrderBy, { readonly field: string }>;

export type RuntimeGroupedAggregateOrderBy = Extract<
  RuntimeGroupedOrderBy,
  { readonly aggregate: string }
>;

export const isRuntimeGroupedFieldOrderBy = (
  order: RuntimeGroupedOrderBy,
): order is RuntimeGroupedFieldOrderBy => Object.hasOwn(order, "field");

export const isRuntimeGroupedAggregateOrderBy = (
  order: RuntimeGroupedOrderBy,
): order is RuntimeGroupedAggregateOrderBy => Object.hasOwn(order, "aggregate");

export type RuntimeGroupedQuery = {
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly where?: RuntimeFilterExpression;
  readonly orderBy?: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly offset?: number;
  readonly limit?: number;
};

const typedRuntimeGroupedQueryBrand: unique symbol = Symbol("TypedRuntimeGroupedQuery");
const typedRuntimeGroupedQueryMetadata = new WeakMap<object, RawQueryCompilerMetadata>();

class TypedRuntimeGroupedQueryInvariant<Row, Query> {
  declare private readonly input: (value: { readonly query: Query; readonly row: Row }) => {
    readonly query: Query;
    readonly row: Row;
  };
}

export type TypedRuntimeGroupedQuery<
  Row extends object,
  Query extends GroupedQuery<Row>,
> = RuntimeGroupedQuery & {
  readonly [typedRuntimeGroupedQueryBrand]: TypedRuntimeGroupedQueryInvariant<Row, Query>;
};

export const typedRuntimeGroupedQueryMatchesSemantics = (
  query: object,
  valueSemantics: object,
): boolean => typedRuntimeGroupedQueryMetadata.get(query)?.valueSemantics === valueSemantics;

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

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const ownValue = (value: Readonly<Record<string, unknown>>, key: string): unknown =>
  Object.hasOwn(value, key) ? value[key] : undefined;

export const decodeGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.decode")((
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

  const groupBy = ownValue(query, "groupBy");
  const groupBySnapshot = denseArraySnapshot(groupBy);
  if (groupBySnapshot === undefined || groupBySnapshot.length === 0) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query groupBy must be a non-empty array of strings.",
    });
  }
  const decodedGroupBy: Array<string> = [];
  for (const field of groupBySnapshot) {
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

  const aggregates = ownValue(query, "aggregates");
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
    const aggFunc = ownValue(aggregate, "aggFunc");
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
      decodedAggregates[alias] = Object.freeze({ aggFunc });
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
    const field = ownValue(aggregate, "field");
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
    const sumResultKind = metadata.fieldMetadata.get(field)?.sumResultKind;
    if (aggFunc === "sum") {
      if (sumResultKind === undefined) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} must reference a numeric field.`,
        });
      }
      decodedAggregates[alias] = Object.freeze({
        aggFunc,
        field,
        resultKind: sumResultKind,
      });
    } else {
      if (aggFunc === "avg" && sumResultKind === undefined) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} must reference a numeric field.`,
        });
      }
      decodedAggregates[alias] = Object.freeze({
        aggFunc,
        field,
      });
    }
  }

  let where: RuntimeFilterExpression | undefined;
  if (Object.hasOwn(query, "where")) {
    const normalized = Result.try(() => normalizeWhere(query["where"], metadata.filterFields));
    if (Result.isFailure(normalized)) {
      return InvalidQueryError.make({
        topic,
        message:
          normalized.failure instanceof FilterExpressionError
            ? normalized.failure.message
            : "Grouped query where contains an unsupported query value.",
      });
    }
    where = normalized.success;
  }

  const orderBy = ownValue(query, "orderBy");
  const orderBySnapshot = denseArraySnapshot(orderBy);
  if (Object.hasOwn(query, "orderBy") && orderBySnapshot === undefined) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query orderBy must be a dense array without extra properties.",
    });
  }
  const decodedOrderBy: Array<RuntimeGroupedOrderBy> = [];
  if (orderBySnapshot !== undefined) {
    for (const entry of orderBySnapshot) {
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
      const direction = ownValue(entry, "direction");
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
        const field = ownValue(entry, "field");
        if (typeof field !== "string" || !decodedGroupBy.includes(field)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy field must be present in groupBy.",
          });
        }
        decodedOrderBy.push(Object.freeze({ field, direction }));
      } else {
        const aggregate = ownValue(entry, "aggregate");
        if (typeof aggregate !== "string" || !aggregateAliases.has(aggregate)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy aggregate must reference an aggregate alias.",
          });
        }
        decodedOrderBy.push(Object.freeze({ aggregate, direction }));
      }
    }
  }

  const offset = ownValue(query, "offset");
  if (Object.hasOwn(query, "offset") && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query offset must be a non-negative safe integer.",
    });
  }

  const limit = ownValue(query, "limit");
  if (Object.hasOwn(query, "limit") && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query limit must be a non-negative safe integer.",
    });
  }

  return Effect.succeed(
    Object.freeze({
      groupBy: Object.freeze(decodedGroupBy),
      aggregates: Object.freeze(decodedAggregates),
      ...(where === undefined ? {} : { where }),
      ...(decodedOrderBy.length === 0 ? {} : { orderBy: Object.freeze(decodedOrderBy) }),
      ...(isValidWindowNumber(offset) ? { offset } : {}),
      ...(isValidWindowNumber(limit) ? { limit } : {}),
    }),
  );
});

export const decodeTypedGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.decodeTyped")(
  function* <Row extends object, const Query extends GroupedQuery<NoInfer<Row>>>(
    topic: string,
    metadata: RawQueryCompilerMetadata<Row>,
    query: Query,
  ) {
    const decoded = yield* decodeGroupedQuery(topic, metadata, query);
    const typed = Object.freeze({
      ...decoded,
      [typedRuntimeGroupedQueryBrand]: new TypedRuntimeGroupedQueryInvariant<Row, Query>(),
    } satisfies TypedRuntimeGroupedQuery<Row, Query>);
    typedRuntimeGroupedQueryMetadata.set(typed, metadata);
    return typed;
  },
);
