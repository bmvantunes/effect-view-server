import { Effect, Result, Schema } from "effect";
import type { RawQuery } from "@effect-view-server/config";
import { inspectDenseArrayData } from "@effect-view-server/effect-utils";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import {
  FilterExpressionError,
  normalizeWhere,
  type RuntimeFilterExpression,
} from "./filter-expression";
import { isPlainRecord } from "./row-values";

export class InvalidQueryError extends Schema.TaggedErrorClass<InvalidQueryError>()(
  "InvalidQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export type RuntimeRawQuery = {
  readonly select: ReadonlyArray<string>;
  readonly where?: RuntimeFilterExpression;
  readonly orderBy?: ReadonlyArray<{
    readonly field: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly offset?: number;
  readonly limit?: number;
};

const typedRuntimeRawQueryBrand: unique symbol = Symbol("TypedRuntimeRawQuery");
const typedRuntimeRawQueryMetadata = new WeakMap<object, RawQueryCompilerMetadata>();

class TypedRuntimeRawQueryInvariant<Row, Query> {
  declare private readonly input: (value: { readonly query: Query; readonly row: Row }) => {
    readonly query: Query;
    readonly row: Row;
  };
}

export type TypedRuntimeRawQuery<Row extends object, Query> = RuntimeRawQuery & {
  readonly [typedRuntimeRawQueryBrand]: TypedRuntimeRawQueryInvariant<Row, Query>;
};

export const typedRuntimeRawQueryMatchesSemantics = (
  query: object,
  valueSemantics: object,
): boolean => typedRuntimeRawQueryMetadata.get(query)?.valueSemantics === valueSemantics;

const rawQueryKeys = new Set(["where", "orderBy", "offset", "limit", "select"]);

export const denseArraySnapshot = (value: unknown): ReadonlyArray<unknown> | undefined => {
  const inspection = inspectDenseArrayData(value);
  return inspection._tag === "Success" ? inspection.values : undefined;
};

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const ownValue = (value: Readonly<Record<string, unknown>>, key: string): unknown =>
  Object.hasOwn(value, key) ? value[key] : undefined;

export const decodeRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.decode")((
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<RuntimeRawQuery, InvalidQueryError> => {
  if (query === undefined) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  if (!isPlainRecord(query)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query must be a plain object.",
    });
  }
  for (const key of Object.keys(query)) {
    if (!rawQueryKeys.has(key)) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query contains unsupported key: ${key}.`,
      });
    }
  }

  let normalizedWhere: RuntimeFilterExpression | undefined;
  if (Object.hasOwn(query, "where")) {
    const normalized = Result.try(() => normalizeWhere(query["where"], metadata.filterFields));
    if (Result.isFailure(normalized)) {
      return InvalidQueryError.make({
        topic,
        message:
          normalized.failure instanceof FilterExpressionError
            ? normalized.failure.message
            : "Raw query where contains an unsupported query value.",
      });
    }
    normalizedWhere = normalized.success;
  }

  const orderBy = ownValue(query, "orderBy");
  const orderBySnapshot = denseArraySnapshot(orderBy);
  if (Object.hasOwn(query, "orderBy") && orderBySnapshot === undefined) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query orderBy must be a dense array without extra properties.",
    });
  }

  const select = ownValue(query, "select");
  const selectSnapshot = denseArraySnapshot(select);
  if (selectSnapshot === undefined || selectSnapshot.length === 0) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  const selectedFields: Array<string> = [];
  for (const field of selectSnapshot) {
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: "Raw query select must be a non-empty array of strings.",
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query select contains unknown field: ${field}.`,
      });
    }
    selectedFields.push(field);
  }

  const offset = ownValue(query, "offset");
  if (Object.hasOwn(query, "offset") && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query offset must be a non-negative safe integer.",
    });
  }

  const limit = ownValue(query, "limit");
  if (Object.hasOwn(query, "limit") && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query limit must be a non-negative safe integer.",
    });
  }

  const decoded: {
    select: ReadonlyArray<string>;
    where?: RuntimeFilterExpression;
    orderBy?: ReadonlyArray<{ readonly field: string; readonly direction: "asc" | "desc" }>;
    offset?: number;
    limit?: number;
  } = {
    select: Object.freeze(selectedFields),
  };

  if (normalizedWhere !== undefined) {
    decoded.where = normalizedWhere;
  }
  if (isValidWindowNumber(offset)) {
    decoded.offset = offset;
  }
  if (isValidWindowNumber(limit)) {
    decoded.limit = limit;
  }
  const clonedOrderBy: Array<{ readonly field: string; readonly direction: "asc" | "desc" }> = [];
  if (orderBySnapshot !== undefined) {
    for (const entry of orderBySnapshot) {
      if (!isPlainRecord(entry)) {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy entries must be plain objects.",
        });
      }
      for (const key of Object.keys(entry)) {
        if (key !== "field" && key !== "direction") {
          return InvalidQueryError.make({
            topic,
            message: `Raw query orderBy contains unsupported key: ${key}.`,
          });
        }
      }
      const field = ownValue(entry, "field");
      if (typeof field !== "string") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy field must be a string.",
        });
      }
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query orderBy contains unknown field: ${field}.`,
        });
      }
      const direction = ownValue(entry, "direction");
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy direction must be asc or desc.",
        });
      }
      clonedOrderBy.push(Object.freeze({ field, direction }));
    }
  }
  if (clonedOrderBy.length > 0) {
    decoded.orderBy = Object.freeze(clonedOrderBy);
  }

  return Effect.succeed(Object.freeze(decoded));
});

export const decodeTypedRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.decodeTyped")(
  function* <Row extends object, const Query extends RawQuery<NoInfer<Row>>>(
    topic: string,
    metadata: RawQueryCompilerMetadata<Row>,
    query: Query,
  ) {
    const decoded = yield* decodeRawQuery(topic, metadata, query);
    const typed = Object.freeze({
      ...decoded,
      [typedRuntimeRawQueryBrand]: new TypedRuntimeRawQueryInvariant<Row, Query>(),
    });
    typedRuntimeRawQueryMetadata.set(typed, metadata);
    return typed;
  },
);

export const validateRuntimeQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.validate")(function* (
  _topic: string,
  _metadata: RawQueryCompilerMetadata,
  _query: RuntimeRawQuery,
) {
  return yield* Effect.void;
});
