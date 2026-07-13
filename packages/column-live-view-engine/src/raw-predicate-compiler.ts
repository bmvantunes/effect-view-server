import { isBigDecimal, make as makeBigDecimal } from "effect/BigDecimal";
import { compareFilterValue } from "./query-value";
import { isDenseArray, type RuntimeRawQuery } from "./raw-query-decoder";
import { isOperatorFilterObject } from "./raw-query-filter";
import { compileSchemaEquality } from "./raw-query-value-semantics";
import { predicateFilterPlans, type TopicRawPredicatePlan } from "./raw-predicate-plan";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { isPlainRecord, trustedFieldValue } from "./row-values";
import type { SchemaValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

export type CompiledRawPredicate<Row extends RowObject> = {
  readonly plan: TopicRawPredicatePlan;
  readonly matches: (row: Row) => boolean;
};

type CompiledRawPredicateClause = {
  readonly field: string;
  readonly matches: (value: unknown) => boolean;
};

type CompiledRawPredicateParts = {
  readonly clauses: ReadonlyArray<CompiledRawPredicateClause>;
  readonly plan: TopicRawPredicatePlan;
};

const clonePredicateFilter = (value: unknown): unknown => {
  if (isBigDecimal(value)) {
    return makeBigDecimal(value.value, value.scale);
  }
  if (Array.isArray(value)) {
    return value.map(clonePredicateFilter);
  }
  if (!isPlainRecord(value)) {
    return value;
  }
  const cloned: Record<string, unknown> = {};
  for (const [key, fieldValue] of Object.entries(value)) {
    Object.defineProperty(cloned, key, {
      configurable: true,
      enumerable: true,
      value: clonePredicateFilter(fieldValue),
      writable: true,
    });
  }
  return cloned;
};

const isStructuredQueryValue = (value: unknown): boolean =>
  (isPlainRecord(value) && !isBigDecimal(value)) || Array.isArray(value);

const compileStructuredFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
  semantics: SchemaValueSemantics,
): ((value: unknown) => boolean) => {
  const literalMatcher = compileSchemaEquality(semantics, filter);
  const oneOf = filter["in"];
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (() => {
            const candidates = oneOf.map((candidate) =>
              compileSchemaEquality(semantics, candidate),
            );
            return candidates.every((candidate) => candidate.valid)
              ? (value: unknown) => candidates.some((candidate) => candidate.matches(value))
              : () => false;
          })()
        : () => false;
  const eq = filter["eq"];
  const neq = filter["neq"];
  const eqMatcher = eq === undefined ? undefined : compileSchemaEquality(semantics, eq);
  const neqMatcher = neq === undefined ? undefined : compileSchemaEquality(semantics, neq);

  return (value) => {
    if (literalMatcher.valid && literalMatcher.matches(value)) {
      return true;
    }
    if (eqMatcher?.valid === false || neqMatcher?.valid === false) {
      return false;
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (eqMatcher !== undefined && !eqMatcher.matches(value)) {
      return false;
    }
    if (neqMatcher !== undefined && neqMatcher.matches(value)) {
      return false;
    }
    return eq !== undefined || oneOfMatcher !== undefined || neq !== undefined;
  };
};

const compileScalarOperatorFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
  semantics: SchemaValueSemantics,
): ((value: unknown) => boolean) => {
  if (
    ("eq" in filter && filter["eq"] === undefined) ||
    ("neq" in filter && filter["neq"] === undefined) ||
    ("in" in filter && filter["in"] === undefined) ||
    ("gt" in filter && filter["gt"] === undefined) ||
    ("gte" in filter && filter["gte"] === undefined) ||
    ("lt" in filter && filter["lt"] === undefined) ||
    ("lte" in filter && filter["lte"] === undefined) ||
    ("startsWith" in filter && filter["startsWith"] === undefined)
  ) {
    return () => false;
  }

  const eq = filter["eq"];
  const neq = filter["neq"];
  const oneOf = filter["in"];
  const startsWith = filter["startsWith"];
  const gt = filter["gt"];
  const gte = filter["gte"];
  const lt = filter["lt"];
  const lte = filter["lte"];
  const eqMatcher = eq === undefined ? undefined : compileSchemaEquality(semantics, eq);
  const neqMatcher = neq === undefined ? undefined : compileSchemaEquality(semantics, neq);
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (() => {
            const candidates = oneOf.map((candidate) =>
              compileSchemaEquality(semantics, candidate),
            );
            return candidates.every((candidate) => candidate.valid)
              ? (value: unknown) => candidates.some((candidate) => candidate.matches(value))
              : () => false;
          })()
        : () => false;

  if (eqMatcher?.valid === false || neqMatcher?.valid === false) {
    return () => false;
  }

  return (value) => {
    if (eqMatcher !== undefined && !eqMatcher.matches(value)) {
      return false;
    }
    if (neqMatcher !== undefined && neqMatcher.matches(value)) {
      return false;
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (startsWith !== undefined) {
      if (
        typeof startsWith !== "string" ||
        typeof value !== "string" ||
        !value.startsWith(startsWith)
      ) {
        return false;
      }
    }

    if (gt !== undefined) {
      const comparison = compareFilterValue(value, gt);
      if (comparison === undefined || comparison <= 0) {
        return false;
      }
    }
    if (gte !== undefined) {
      const comparison = compareFilterValue(value, gte);
      if (comparison === undefined || comparison < 0) {
        return false;
      }
    }
    if (lt !== undefined) {
      const comparison = compareFilterValue(value, lt);
      if (comparison === undefined || comparison >= 0) {
        return false;
      }
    }
    if (lte !== undefined) {
      const comparison = compareFilterValue(value, lte);
      if (comparison === undefined || comparison > 0) {
        return false;
      }
    }

    return true;
  };
};

const compileFilterMatcher = (
  filter: unknown,
  semantics: SchemaValueSemantics,
): ((value: unknown) => boolean) => {
  if (filter === undefined) {
    return () => false;
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return compileSchemaEquality(semantics, filter).matches;
  }

  const structuredMatcher = compileStructuredFilterMatcher(filter, semantics);
  if (!isOperatorFilterObject(filter)) {
    const literalMatcher = compileSchemaEquality(semantics, filter);
    return (value) =>
      isStructuredQueryValue(value) ? structuredMatcher(value) : literalMatcher.matches(value);
  }

  const scalarMatcher = compileScalarOperatorFilterMatcher(filter, semantics);
  return (value) =>
    isStructuredQueryValue(value) ? structuredMatcher(value) : scalarMatcher(value);
};

const compilePredicateParts = (
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicateParts => {
  if (where === undefined) {
    return Object.freeze({
      clauses: Object.freeze([]),
      plan: Object.freeze({
        filters: Object.freeze([]),
        callbackRequired: false,
        callbackSkippable: true,
      }),
    });
  }

  const filters: Array<TopicRawPredicatePlan["filters"][number]> = [];
  const clauses: Array<CompiledRawPredicateClause> = [];
  let callbackRequired = false;
  for (const [field, filter] of Object.entries(where)) {
    const fieldPlan = predicateFilterPlans(field, filter, metadata);
    filters.push(...fieldPlan.filters);
    callbackRequired ||= fieldPlan.callbackRequired;
    clauses.push(
      Object.freeze({
        field,
        matches: compileFilterMatcher(
          clonePredicateFilter(filter),
          metadata.valueSemantics.field(field),
        ),
      }),
    );
  }
  return Object.freeze({
    clauses: Object.freeze(clauses),
    plan: Object.freeze({
      filters: Object.freeze(filters),
      callbackRequired,
      callbackSkippable: !callbackRequired,
    }),
  });
};

export const compileRawPredicate = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicate<Row> => {
  const parts = compilePredicateParts(metadata, where);
  if (parts.clauses.length === 0) {
    return Object.freeze({
      plan: parts.plan,
      matches: () => true,
    });
  }

  return Object.freeze({
    plan: parts.plan,
    matches: (row) => {
      for (const clause of parts.clauses) {
        if (!clause.matches(trustedFieldValue(row, clause.field))) {
          return false;
        }
      }
      return true;
    },
  });
};
