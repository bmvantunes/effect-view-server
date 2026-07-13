import { Effect, Result, Schema } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import { isRawQueryRangeFilterOperatorKey } from "@effect-view-server/config/internal";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { filterOperatorKeys, isDenseArray } from "./raw-query-filter";
import { materializeRawQueryFilter, RawQuerySchemaValueError } from "./raw-query-value-semantics";
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
  readonly where?: Record<string, unknown>;
  readonly orderBy?: ReadonlyArray<{
    readonly field: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly offset?: number;
  readonly limit?: number;
};

const rawQueryKeys = new Set(["where", "orderBy", "offset", "limit", "select"]);
export { filterOperatorKeys, isDenseArray } from "./raw-query-filter";

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

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

  const where = query["where"];
  if (where !== undefined && !isPlainRecord(where)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query where must be a plain object.",
    });
  }
  let normalizedWhere: Record<string, unknown> | undefined;
  if (where !== undefined) {
    const candidate: Record<string, unknown> = {};
    for (const field of Object.keys(where)) {
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where contains unknown field: ${field}.`,
        });
      }
      const materialized = Result.try(() =>
        materializeRawQueryFilter(
          metadata.valueSemantics.field(field),
          metadata.structuredFieldNames.has(field),
          where[field],
        ),
      );
      if (Result.isFailure(materialized)) {
        return InvalidQueryError.make({
          topic,
          message:
            materialized.failure instanceof RawQuerySchemaValueError
              ? `Raw query where field ${field} does not satisfy its configured schema.`
              : `Raw query where field ${field} contains unsupported query value.`,
        });
      }
      Object.defineProperty(candidate, field, {
        configurable: true,
        enumerable: true,
        value: materialized.success,
        writable: true,
      });
    }
    normalizedWhere = candidate;
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query orderBy must be an array.",
    });
  }

  const select = query["select"];
  if (!Array.isArray(select)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  if (select.length === 0 || !isDenseArray(select)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  const selectedFields: Array<string> = [];
  for (const field of select) {
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

  const offset = query["offset"];
  if (offset !== undefined && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query offset must be a non-negative safe integer.",
    });
  }

  const limit = query["limit"];
  if (limit !== undefined && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query limit must be a non-negative safe integer.",
    });
  }

  const decoded: {
    select: Array<string>;
    where?: Record<string, unknown>;
    orderBy?: Array<{ readonly field: string; readonly direction: "asc" | "desc" }>;
    offset?: number;
    limit?: number;
  } = {
    select: selectedFields,
  };

  if (normalizedWhere !== undefined) {
    decoded.where = normalizedWhere;
  }
  if (offset !== undefined) {
    decoded.offset = offset;
  }
  if (limit !== undefined) {
    decoded.limit = limit;
  }
  const clonedOrderBy: Array<{ readonly field: string; readonly direction: "asc" | "desc" }> = [];
  if (Array.isArray(orderBy)) {
    for (const entry of orderBy) {
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
      const field = entry["field"];
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
      const direction = entry["direction"];
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy direction must be asc or desc.",
        });
      }
      clonedOrderBy.push({
        field,
        direction,
      });
    }
  }
  if (clonedOrderBy.length > 0) {
    decoded.orderBy = clonedOrderBy;
  }

  return Effect.succeed(decoded);
});

export const validateRuntimeQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.validate")(function* (
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
) {
  if (query.where === undefined) {
    return;
  }

  for (const [field, filter] of Object.entries(query.where)) {
    if (!isPlainRecord(filter) || isBigDecimal(filter)) {
      continue;
    }
    const keys = Object.keys(filter);
    const operatorKeyCount = keys.filter((key) => filterOperatorKeys.has(key)).length;
    if (metadata.structuredObjectFieldNames.has(field)) {
      continue;
    }
    if (operatorKeyCount > 0 && operatorKeyCount !== keys.length) {
      return yield* InvalidQueryError.make({
        topic,
        message: `Raw query where field ${field} contains unsupported filter operator.`,
      });
    }
    if (operatorKeyCount > 0) {
      if (keys.includes("startsWith") && !metadata.stringFieldNames.has(field)) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} does not support startsWith.`,
        });
      }
      if (
        keys.some(isRawQueryRangeFilterOperatorKey) &&
        (!metadata.numericFieldNames.has(field) || metadata.rangeValueKinds.get(field)?.size !== 1)
      ) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} does not support range operators.`,
        });
      }
    }
    if (operatorKeyCount === 0) {
      return yield* InvalidQueryError.make({
        topic,
        message: `Raw query where field ${field} contains unsupported filter operator.`,
      });
    }
  }
});
