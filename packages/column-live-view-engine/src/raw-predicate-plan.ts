import { isBigDecimal, type BigDecimal } from "effect/BigDecimal";
import { immutableReadonlySet } from "./immutable-readonly-collection";
import type { RuntimeFilterCondition, RuntimeFilterScalar } from "./filter-expression";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { scalarEqualityKey, type ScalarEqualityKeyValue } from "./row-values";

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
    }
  | {
      readonly accentSensitive: boolean;
      readonly caseSensitive: boolean;
      readonly field: string;
      readonly operator: "textEq";
      readonly value: string;
    }
  | {
      readonly accentSensitive: boolean;
      readonly caseSensitive: boolean;
      readonly field: string;
      readonly operator: "textIn";
      readonly values: ReadonlyArray<string>;
      readonly valueSet: ReadonlySet<string>;
    };

export type TopicRawPredicatePlan = {
  readonly filters: ReadonlyArray<TopicRawPredicateFilterPlan>;
  readonly callbackRequired: boolean;
  readonly callbackSkippable?: boolean;
};

export type PredicateFieldPlan = {
  readonly filters: TopicRawPredicatePlan["filters"];
  readonly callbackRequired: boolean;
};

const isScalarPlanValue = (value: RuntimeFilterScalar): value is ScalarEqualityKeyValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  typeof value === "bigint" ||
  isBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

const isNumericPlanValue = (value: RuntimeFilterScalar): value is number | bigint | BigDecimal =>
  typeof value === "bigint" ||
  isBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

export const isRangePlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): value is number | bigint | BigDecimal => {
  const kind =
    typeof value === "number" && Number.isFinite(value)
      ? "number"
      : typeof value === "bigint"
        ? "bigint"
        : isBigDecimal(value)
          ? "bigDecimal"
          : undefined;
  const fieldKinds = metadata.rangeValueKinds.get(field);
  return kind !== undefined && fieldKinds?.size === 1 && fieldKinds.has(kind);
};

const isScalarArray = (
  value: RuntimeFilterScalar | ReadonlyArray<RuntimeFilterScalar>,
): value is ReadonlyArray<RuntimeFilterScalar> => Array.isArray(value);

const scalarEqualityKeys = (values: ReadonlyArray<ScalarEqualityKeyValue>): ReadonlySet<string> => {
  const keys = new Set<string>();
  for (const value of values) {
    keys.add(scalarEqualityKey(value));
  }
  return immutableReadonlySet(keys);
};

const stringValueSet = (values: ReadonlyArray<string>): ReadonlySet<string> =>
  immutableReadonlySet(new Set(values));

const unsupportedPlan = (): PredicateFieldPlan =>
  Object.freeze({ filters: Object.freeze([]), callbackRequired: true });

const onePlan = (filter: TopicRawPredicateFilterPlan): PredicateFieldPlan =>
  Object.freeze({ filters: Object.freeze([Object.freeze(filter)]), callbackRequired: false });

export const predicateFilterPlans = (
  condition: RuntimeFilterCondition,
  metadata: RawQueryCompilerMetadata,
): PredicateFieldPlan => {
  const field = metadata.filterFields.get(condition.field);
  if (
    field === undefined ||
    field.segments.length !== 1 ||
    !metadata.exactScalarEqualityFieldNames.has(condition.field)
  ) {
    return unsupportedPlan();
  }
  const filter = condition.filter;
  if (condition.type === "equals" || condition.type === "notEqual") {
    if (filter === undefined || isScalarArray(filter) || !isScalarPlanValue(filter)) {
      return unsupportedPlan();
    }
    if (typeof filter === "string") {
      return condition.type === "notEqual"
        ? unsupportedPlan()
        : onePlan({
            field: condition.field,
            operator: "textEq",
            value: filter,
            caseSensitive: condition.caseSensitive,
            accentSensitive: condition.accentSensitive,
          });
    }
    return onePlan({
      field: condition.field,
      operator: condition.type === "equals" ? "eq" : "neq",
      value: filter,
    });
  }
  if (condition.type === "in") {
    if (filter === undefined || !isScalarArray(filter)) {
      return unsupportedPlan();
    }
    if (filter.every((candidate): candidate is string => typeof candidate === "string")) {
      return onePlan({
        field: condition.field,
        operator: "textIn",
        values: filter,
        valueSet: stringValueSet(filter),
        caseSensitive: condition.caseSensitive,
        accentSensitive: condition.accentSensitive,
      });
    }
    if (
      filter.some((candidate) => typeof candidate === "string" || !isScalarPlanValue(candidate))
    ) {
      return unsupportedPlan();
    }
    return onePlan({
      field: condition.field,
      operator: "in",
      values: filter,
      valueKeys: scalarEqualityKeys(filter),
    });
  }
  if (
    condition.type === "greaterThan" ||
    condition.type === "greaterThanOrEqual" ||
    condition.type === "lessThan" ||
    condition.type === "lessThanOrEqual"
  ) {
    if (filter === undefined || isScalarArray(filter) || !isNumericPlanValue(filter)) {
      return unsupportedPlan();
    }
    const operator =
      condition.type === "greaterThan"
        ? "gt"
        : condition.type === "greaterThanOrEqual"
          ? "gte"
          : condition.type === "lessThan"
            ? "lt"
            : "lte";
    return onePlan({ field: condition.field, operator, value: filter });
  }
  if (condition.type === "inRange") {
    const filterTo = condition.filterTo;
    if (
      filter === undefined ||
      isScalarArray(filter) ||
      !isNumericPlanValue(filter) ||
      filterTo === undefined ||
      !isNumericPlanValue(filterTo)
    ) {
      return unsupportedPlan();
    }
    return Object.freeze({
      filters: Object.freeze([
        Object.freeze({ field: condition.field, operator: "gte", value: filter }),
        Object.freeze({ field: condition.field, operator: "lt", value: filterTo }),
      ]),
      callbackRequired: false,
    });
  }
  return unsupportedPlan();
};
