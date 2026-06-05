import { Effect, Schema } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { cloneRecord, isPlainRecord } from "./row-values";

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
export const filterOperatorKeys = new Set([
  "eq",
  "neq",
  "in",
  "gt",
  "gte",
  "lt",
  "lte",
  "startsWith",
]);
const rangeFilterOperatorKeys = new Set(["gt", "gte", "lt", "lte"]);

export const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      return false;
    }
  }
  return true;
};

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const isQueryValueSafe = (value: unknown, active: WeakSet<object> = new WeakSet()): boolean => {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    isBigDecimal(value)
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      return false;
    }
    active.add(value);
    const safe = isDenseArray(value) && value.every((entry) => isQueryValueSafe(entry, active));
    active.delete(value);
    return safe;
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      return false;
    }
    active.add(value);
    const safe = Object.values(value).every((entry) => isQueryValueSafe(entry, active));
    active.delete(value);
    return safe;
  }
  return false;
};

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
  if (where !== undefined) {
    for (const field of Object.keys(where)) {
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where contains unknown field: ${field}.`,
        });
      }
      if (!isQueryValueSafe(where[field])) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} contains unsupported query value.`,
        });
      }
    }
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

  if (where !== undefined) {
    let clonedWhere: Record<string, unknown>;
    try {
      clonedWhere = cloneRecord(where);
    } catch (cause) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query where could not be cloned: ${String(cause)}`,
      });
    }
    decoded.where = clonedWhere;
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
        keys.some((key) => rangeFilterOperatorKeys.has(key)) &&
        !metadata.numericFieldNames.has(field)
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
