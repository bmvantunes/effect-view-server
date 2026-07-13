import { Result } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import { stableQueryValueString } from "./query-value";
import { isDenseArray, isOperatorFilterObject } from "./raw-query-filter";
import { cloneUnknown, isPlainRecord } from "./row-values";
import type { SchemaValueSemantics } from "./topic-row-value-semantics";

export class RawQuerySchemaValueError extends Error {}

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

const unsupportedQueryValue = (): never => {
  throw new TypeError("Unsupported raw query value.");
};

const invalidSchemaQueryValue = (): never => {
  throw new RawQuerySchemaValueError("Raw query value does not satisfy its configured schema.");
};

const cloneSafeQueryValue = (value: unknown): unknown =>
  isQueryValueSafe(value) ? cloneUnknown(value) : unsupportedQueryValue();

const materializedValue = (
  semantics: SchemaValueSemantics,
  value: unknown,
): Result.Result<unknown, unknown> => Result.try(() => semantics.materialize(value));

const materializeSchemaQueryValue = (
  semantics: SchemaValueSemantics,
  value: unknown,
  schemaRequired = false,
): unknown => {
  const materialized = materializedValue(semantics, value);
  if (Result.isSuccess(materialized)) {
    return materialized.success;
  }
  if (!isQueryValueSafe(value)) {
    return unsupportedQueryValue();
  }
  const encoded = Result.try(() => semantics.decodeEncoded(value));
  return Result.isSuccess(encoded) || schemaRequired
    ? invalidSchemaQueryValue()
    : cloneSafeQueryValue(value);
};

const setFilterValue = (
  filter: Record<string, unknown>,
  operator: string,
  value: unknown,
): void => {
  Object.defineProperty(filter, operator, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const materializeOperatorFilter = (
  semantics: SchemaValueSemantics,
  filter: Readonly<Record<string, unknown>>,
): Record<string, unknown> => {
  const materialized: Record<string, unknown> = {};
  for (const [operator, value] of Object.entries(filter)) {
    if (operator === "in" && Array.isArray(value)) {
      if (!isDenseArray(value)) {
        return unsupportedQueryValue();
      }
      setFilterValue(
        materialized,
        operator,
        value.map((entry) => materializeSchemaQueryValue(semantics, entry)),
      );
    } else {
      setFilterValue(materialized, operator, materializeSchemaQueryValue(semantics, value));
    }
  }
  return materialized;
};

export const materializeRawQueryFilter = (
  semantics: SchemaValueSemantics,
  structuredField: boolean,
  filter: unknown,
): unknown => {
  if (isOperatorFilterObject(filter)) {
    if (structuredField) {
      const literal = materializedValue(semantics, filter);
      if (Result.isSuccess(literal)) {
        return literal.success;
      }
    }
    return materializeOperatorFilter(semantics, filter);
  }
  if (!structuredField && isPlainRecord(filter)) {
    return cloneSafeQueryValue(filter);
  }
  return materializeSchemaQueryValue(semantics, filter, structuredField);
};

type CanonicalOperandToken = readonly ["schema" | "generic", string];

const canonicalOperandToken = (
  semantics: SchemaValueSemantics,
  value: unknown,
): CanonicalOperandToken => {
  const canonical = Result.try(() => semantics.canonicalKey(value));
  return Result.isSuccess(canonical)
    ? ["schema", canonical.success]
    : ["generic", stableQueryValueString(value)];
};

export const canonicalRawQueryFilterKey = (
  semantics: SchemaValueSemantics,
  structuredField: boolean,
  filter: unknown,
): string => {
  if (!isOperatorFilterObject(filter)) {
    return JSON.stringify(["literal", canonicalOperandToken(semantics, filter)]);
  }
  if (structuredField) {
    const literal = Result.try(() => semantics.canonicalKey(filter));
    if (Result.isSuccess(literal)) {
      return JSON.stringify(["literal", ["schema", literal.success]]);
    }
  }
  const operators = Object.entries(filter)
    .toSorted(([left], [right]) => Number(left > right) - Number(left < right))
    .map(([operator, value]) => {
      if (operator === "in" && Array.isArray(value)) {
        return [operator, value.map((entry) => canonicalOperandToken(semantics, entry))] as const;
      }
      if (operator === "startsWith") {
        return [operator, ["generic", stableQueryValueString(value)]] as const;
      }
      return [operator, canonicalOperandToken(semantics, value)] as const;
    });
  return JSON.stringify(["operators", operators]);
};

export type CompiledSchemaEquality = {
  readonly valid: boolean;
  readonly matches: (value: unknown) => boolean;
};

export const compileSchemaEquality = (
  semantics: SchemaValueSemantics,
  operand: unknown,
): CompiledSchemaEquality => {
  const materialized = materializedValue(semantics, operand);
  if (Result.isFailure(materialized)) {
    return {
      valid: false,
      matches: () => false,
    };
  }
  const expected = materialized.success;
  return {
    valid: true,
    matches: (value) => semantics.equivalent(value, expected),
  };
};
