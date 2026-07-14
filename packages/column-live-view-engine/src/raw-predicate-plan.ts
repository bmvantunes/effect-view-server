import {
  isBigDecimal,
  make as makeBigDecimal,
  normalize as normalizeBigDecimal,
  type BigDecimal,
} from "effect/BigDecimal";
import { immutableReadonlySet } from "./immutable-readonly-collection";
import { filterOperatorKeys, isDenseArray } from "./raw-query-filter";
import type { RangeValueKind, RawQueryCompilerMetadata } from "./raw-query-metadata";
import { isPlainRecord, scalarEqualityKey, type ScalarEqualityKeyValue } from "./row-values";

export type TopicRawPredicateFilterPlan =
  | {
      readonly field: string;
      readonly operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "startsWith";
      readonly value: unknown;
    }
  | {
      readonly field: string;
      readonly operator: "in";
      readonly values: ReadonlyArray<unknown>;
      readonly valueKeys?: ReadonlySet<string>;
    };

export type TopicRawPredicatePlan = {
  /**
   * Safe scalar hints that storage can use to narrow a raw scan.
   * `matches` remains the correctness guard unless an adapter implements a
   * proven equivalent for every emitted hint.
   */
  readonly filters: ReadonlyArray<TopicRawPredicateFilterPlan>;
  /**
   * True when the compiler intentionally omitted part of the predicate from
   * `filters`, for example structured fields or malformed runtime filters.
   */
  readonly callbackRequired: boolean;
  /**
   * True when the compiler proved that `filters` fully represent `matches`.
   * Hand-written plans omit this and stay guarded by the row callback.
   */
  readonly callbackSkippable?: boolean;
};

export type PredicateFieldPlan = {
  readonly filters: TopicRawPredicatePlan["filters"];
  readonly callbackRequired: boolean;
};

const rangeValueKind = (value: unknown): RangeValueKind | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return "number";
  }
  if (typeof value === "bigint") {
    return "bigint";
  }
  if (isBigDecimal(value)) {
    return "bigDecimal";
  }
  return undefined;
};

const isScalarPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  typeof value === "bigint" ||
  isBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

export const isRangePlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): value is number | bigint | BigDecimal => {
  const kind = rangeValueKind(value);
  const fieldKinds = metadata.rangeValueKinds.get(field);
  return kind !== undefined && fieldKinds?.size === 1 && fieldKinds.has(kind);
};

const isEqualityPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  isScalarPlanValue(value);

const immutableBigDecimal = (value: BigDecimal): BigDecimal => {
  const owned = makeBigDecimal(value.value, value.scale);
  const normalized = normalizeBigDecimal(makeBigDecimal(value.value, value.scale));
  const normalizedOwned = makeBigDecimal(normalized.value, normalized.scale);
  Object.defineProperty(normalizedOwned, "normalized", {
    configurable: false,
    enumerable: false,
    value: normalizedOwned,
    writable: false,
  });
  Object.freeze(normalizedOwned);
  Object.defineProperty(owned, "normalized", {
    configurable: false,
    enumerable: false,
    value:
      owned.value === normalizedOwned.value && owned.scale === normalizedOwned.scale
        ? owned
        : normalizedOwned,
    writable: false,
  });
  return Object.freeze(owned);
};

const immutableScalarPlanValue = (value: ScalarEqualityKeyValue): ScalarEqualityKeyValue =>
  isBigDecimal(value) ? immutableBigDecimal(value) : value;

const isNotEqualPlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): value is ScalarEqualityKeyValue => {
  if (!isEqualityPlanValue(value)) {
    return false;
  }
  if (metadata.numericFieldNames.has(field)) {
    return isRangePlanValue(field, value, metadata);
  }
  if (metadata.stringFieldNames.has(field)) {
    return typeof value === "string";
  }
  return false;
};

const isInPlanValues = (value: unknown): value is ReadonlyArray<ScalarEqualityKeyValue> =>
  Array.isArray(value) &&
  isDenseArray(value) &&
  value.every((candidate) => isEqualityPlanValue(candidate));

const scalarEqualityKeys = (values: ReadonlyArray<ScalarEqualityKeyValue>): ReadonlySet<string> => {
  const keys = new Set<string>();
  for (const value of values) {
    keys.add(scalarEqualityKey(value));
  }
  return immutableReadonlySet(keys);
};

export const predicateFilterPlans = (
  field: string,
  filter: unknown,
  metadata: RawQueryCompilerMetadata,
): PredicateFieldPlan => {
  if (
    metadata.structuredFieldNames.has(field) ||
    !metadata.exactScalarEqualityFieldNames.has(field) ||
    filter === undefined
  ) {
    return Object.freeze({
      filters: Object.freeze([]),
      callbackRequired: true,
    });
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    if (!isScalarPlanValue(filter)) {
      return Object.freeze({
        filters: Object.freeze([]),
        callbackRequired: true,
      });
    }
    return Object.freeze({
      filters: Object.freeze([
        Object.freeze({
          field,
          operator: "eq",
          value: immutableScalarPlanValue(filter),
        }),
      ]),
      callbackRequired: false,
    });
  }

  const operatorKeys = Object.keys(filter).filter((key) => filterOperatorKeys.has(key));
  let callbackRequired = operatorKeys.length === 0;
  const plans: Array<TopicRawPredicatePlan["filters"][number]> = [];
  if ("eq" in filter) {
    if (isEqualityPlanValue(filter["eq"])) {
      plans.push(
        Object.freeze({
          field,
          operator: "eq",
          value: immutableScalarPlanValue(filter["eq"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("neq" in filter) {
    if (isNotEqualPlanValue(field, filter["neq"], metadata)) {
      plans.push(
        Object.freeze({
          field,
          operator: "neq",
          value: immutableScalarPlanValue(filter["neq"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("in" in filter) {
    if (isInPlanValues(filter["in"])) {
      const values = Object.freeze(filter["in"].map(immutableScalarPlanValue));
      plans.push(
        Object.freeze({
          field,
          operator: "in",
          values,
          valueKeys: scalarEqualityKeys(values),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("gt" in filter) {
    if (isRangePlanValue(field, filter["gt"], metadata)) {
      plans.push(
        Object.freeze({
          field,
          operator: "gt",
          value: immutableScalarPlanValue(filter["gt"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("gte" in filter) {
    if (isRangePlanValue(field, filter["gte"], metadata)) {
      plans.push(
        Object.freeze({
          field,
          operator: "gte",
          value: immutableScalarPlanValue(filter["gte"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("lt" in filter) {
    if (isRangePlanValue(field, filter["lt"], metadata)) {
      plans.push(
        Object.freeze({
          field,
          operator: "lt",
          value: immutableScalarPlanValue(filter["lt"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("lte" in filter) {
    if (isRangePlanValue(field, filter["lte"], metadata)) {
      plans.push(
        Object.freeze({
          field,
          operator: "lte",
          value: immutableScalarPlanValue(filter["lte"]),
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  if ("startsWith" in filter) {
    if (typeof filter["startsWith"] === "string") {
      plans.push(
        Object.freeze({
          field,
          operator: "startsWith",
          value: filter["startsWith"],
        }),
      );
    } else {
      callbackRequired = true;
    }
  }
  return Object.freeze({
    filters: Object.freeze(plans),
    callbackRequired,
  });
};
