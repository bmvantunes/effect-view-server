import {
  collectCanonicalFilterGraphLeaves,
  compareCanonicalFilterGraphs,
  complementCanonicalFilterType,
  isWireSafeBigDecimal,
} from "@effect-view-server/effect-utils";
import {
  isBigDecimal,
  make as makeBigDecimal,
  normalize as normalizeBigDecimal,
  type BigDecimal,
} from "effect/BigDecimal";
import { Result } from "effect";
import type { FilterFieldMetadata, FilterNumericKind } from "./filter-field-metadata";
import { compareFilterValue, stableQueryValueString } from "./query-value";
import { isPlainRecord } from "./row-values";

export type RuntimeFilterConditionType =
  | "equals"
  | "notEqual"
  | "in"
  | "greaterThan"
  | "greaterThanOrEqual"
  | "lessThan"
  | "lessThanOrEqual"
  | "inRange"
  | "contains"
  | "notContains"
  | "startsWith"
  | "endsWith"
  | "blank"
  | "notBlank";

export type RuntimeFilterScalar = null | string | number | bigint | boolean | BigDecimal;

export type RuntimeFilterCondition = {
  readonly _tag: "condition";
  readonly key: string;
  readonly field: string;
  readonly type: RuntimeFilterConditionType;
  readonly caseSensitive: boolean;
  readonly accentSensitive: boolean;
  readonly filter?: RuntimeFilterScalar | ReadonlyArray<RuntimeFilterScalar>;
  readonly filterTo?: RuntimeFilterScalar;
};

export type RuntimeFilterGroup = {
  readonly _tag: "group";
  readonly key: string;
  readonly type: "AND" | "OR";
  readonly conditions: ReadonlyArray<RuntimeFilterExpression>;
};

export type RuntimeFilterNegation = {
  readonly _tag: "NOT";
  readonly key: string;
  readonly condition: RuntimeFilterExpression;
};

export type RuntimeFilterExpression =
  | RuntimeFilterCondition
  | RuntimeFilterGroup
  | RuntimeFilterNegation;

export class FilterExpressionError extends Error {}

type DeferredExpressionSequence =
  | {
      readonly _tag: "one";
      readonly expression: RuntimeFilterExpression;
    }
  | {
      readonly _tag: "concat";
      readonly left: DeferredExpressionSequence;
      readonly right: DeferredExpressionSequence;
    };

type DeferredFilterGroup = {
  readonly _tag: "deferredGroup";
  readonly type: "AND" | "OR";
  readonly sequence: DeferredExpressionSequence;
};

type NormalizedExpression = RuntimeFilterExpression | DeferredFilterGroup | undefined;

const textSearchTypes = new Set<RuntimeFilterConditionType>([
  "contains",
  "notContains",
  "startsWith",
  "endsWith",
]);
const numericComparisonTypes = new Set<RuntimeFilterConditionType>([
  "greaterThan",
  "greaterThanOrEqual",
  "lessThan",
  "lessThanOrEqual",
]);
const blankTypes = new Set<RuntimeFilterConditionType>(["blank", "notBlank"]);
const isConditionType = (value: unknown): value is RuntimeFilterConditionType => {
  switch (value) {
    case "equals":
    case "notEqual":
    case "in":
    case "greaterThan":
    case "greaterThanOrEqual":
    case "lessThan":
    case "lessThanOrEqual":
    case "inRange":
    case "contains":
    case "notContains":
    case "startsWith":
    case "endsWith":
    case "blank":
    case "notBlank":
      return true;
    default:
      return false;
  }
};

const fail = (message: string): never => {
  throw new FilterExpressionError(message);
};

const requirePlainRecord = (value: unknown): Readonly<Record<string, unknown>> =>
  isPlainRecord(value) ? value : fail("Every filter expression must be a plain object.");

const ownEnumerableDataValue = (
  record: Readonly<Record<string, unknown>>,
  key: string,
): unknown => {
  const descriptor = Object.getOwnPropertyDescriptor(record, key);
  if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
    return fail(`Filter expression property ${key} must be an own enumerable data property.`);
  }
  return descriptor.value;
};

const validateExactRecordKeys = (
  record: Readonly<Record<string, unknown>>,
  allowed: ReadonlySet<string>,
): void => {
  if (Object.getOwnPropertySymbols(record).length > 0) {
    fail("Filter expressions must not contain symbol properties.");
  }
  for (const key of Object.getOwnPropertyNames(record)) {
    if (!allowed.has(key)) {
      fail(`Filter expression contains unsupported property: ${key}.`);
    }
    ownEnumerableDataValue(record, key);
  }
};

const denseArraySnapshot = (value: unknown, label: string): ReadonlyArray<unknown> => {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) {
    return fail(`${label} must be an array.`);
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    return fail(`${label} must not contain symbol properties.`);
  }
  const allowed = new Set<string>(["length"]);
  const snapshot: Array<unknown> = [];
  for (let index = 0; index < value.length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return fail(`${label} must be a dense array of data values.`);
    }
    snapshot.push(descriptor.value);
  }
  for (const key of Object.getOwnPropertyNames(value)) {
    if (!allowed.has(key)) {
      return fail(`${label} contains unsupported property: ${key}.`);
    }
  }
  return snapshot;
};

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

const ownScalar = (value: unknown): RuntimeFilterScalar => {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return fail("Filter numbers must be finite.");
    }
    return Object.is(value, -0) ? 0 : value;
  }
  if (typeof value === "bigint") {
    return value;
  }
  if (isWireSafeBigDecimal(value)) {
    return immutableBigDecimal(value);
  }
  return fail("Filter operands must be supported scalar values.");
};

const normalizeText = (value: string, caseSensitive: boolean, accentSensitive: boolean): string => {
  let normalized = value.normalize("NFD");
  if (!accentSensitive) {
    normalized = normalized.replace(/\p{M}+/gu, "");
  }
  return caseSensitive ? normalized : normalized.toLowerCase();
};

const textOptions = (
  record: Readonly<Record<string, unknown>>,
): { readonly caseSensitive: boolean; readonly accentSensitive: boolean } => {
  const caseSensitive = Object.hasOwn(record, "caseSensitive")
    ? ownEnumerableDataValue(record, "caseSensitive")
    : false;
  const accentSensitive = Object.hasOwn(record, "accentSensitive")
    ? ownEnumerableDataValue(record, "accentSensitive")
    : false;
  if (typeof caseSensitive !== "boolean" || typeof accentSensitive !== "boolean") {
    return fail("Text Matching options must be booleans when present.");
  }
  return { caseSensitive, accentSensitive };
};

const numericKind = (value: RuntimeFilterScalar): FilterNumericKind | undefined => {
  if (typeof value === "number") {
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

const numericOperand = (value: unknown, field: FilterFieldMetadata): RuntimeFilterScalar => {
  const owned = ownScalar(value);
  const kind = numericKind(owned);
  if (kind === undefined || !field.numericKinds.has(kind)) {
    return fail(`Filter field ${field.path} does not support this numeric operand domain.`);
  }
  return owned;
};

const equalityOperand = (
  value: unknown,
  field: FilterFieldMetadata,
  caseSensitive: boolean,
  accentSensitive: boolean,
): RuntimeFilterScalar => {
  if (value === undefined) {
    return fail("Filter operands must not be undefined.");
  }
  if (isBigDecimal(value) && !isWireSafeBigDecimal(value)) {
    return fail("Filter operands must be supported scalar values.");
  }
  const materialized = Result.try(() => field.semantics.materialize(value));
  if (Result.isFailure(materialized)) {
    return fail(`Filter operand for ${field.path} does not satisfy its configured schema.`);
  }
  const owned = ownScalar(materialized.success);
  return typeof owned === "string" ? normalizeText(owned, caseSensitive, accentSensitive) : owned;
};

const operandKey = (value: RuntimeFilterScalar): string => stableQueryValueString(value);

const isRuntimeFilterScalarArray = (
  value: RuntimeFilterScalar | ReadonlyArray<RuntimeFilterScalar>,
): value is ReadonlyArray<RuntimeFilterScalar> => Array.isArray(value);

const conditionKey = (condition: Omit<RuntimeFilterCondition, "key">): string =>
  JSON.stringify([
    "condition",
    condition.field,
    condition.type,
    condition.caseSensitive,
    condition.accentSensitive,
    condition.filter === undefined
      ? null
      : isRuntimeFilterScalarArray(condition.filter)
        ? condition.filter.map(operandKey)
        : operandKey(condition.filter),
    condition.filterTo === undefined ? null : operandKey(condition.filterTo),
  ]);

const freezeCondition = (condition: Omit<RuntimeFilterCondition, "key">): RuntimeFilterCondition =>
  Object.freeze({
    ...condition,
    key: conditionKey(condition),
  });

const conditionAllowedKeys = (
  type: RuntimeFilterConditionType,
  supportsText: boolean,
): ReadonlySet<string> => {
  const allowed = new Set(["field", "type"]);
  if (!blankTypes.has(type)) {
    allowed.add("filter");
  }
  if (type === "inRange") {
    allowed.add("filterTo");
  }
  if (supportsText) {
    allowed.add("caseSensitive");
    allowed.add("accentSensitive");
  }
  return allowed;
};

const makeCondition = (
  record: Readonly<Record<string, unknown>>,
  fields: ReadonlyMap<string, FilterFieldMetadata>,
): RuntimeFilterCondition | undefined => {
  const fieldName = ownEnumerableDataValue(record, "field");
  const type = ownEnumerableDataValue(record, "type");
  if (typeof fieldName !== "string") {
    return fail("Filter condition field must be a string.");
  }
  if (!isConditionType(type)) {
    return fail(`Filter condition for ${fieldName} has an unsupported type.`);
  }
  const field = fields.get(fieldName);
  if (field === undefined) {
    return fail(`Filter condition references unknown or non-filterable field: ${fieldName}.`);
  }
  const supportsText =
    textSearchTypes.has(type) ||
    ((type === "equals" || type === "notEqual" || type === "in") && field.hasString);
  validateExactRecordKeys(record, conditionAllowedKeys(type, supportsText));
  const options = textOptions(record);

  if (blankTypes.has(type)) {
    return freezeCondition({
      _tag: "condition",
      field: fieldName,
      type,
      caseSensitive: false,
      accentSensitive: false,
    });
  }
  const filter = ownEnumerableDataValue(record, "filter");
  if (type === "equals" || type === "notEqual") {
    const operand = equalityOperand(filter, field, options.caseSensitive, options.accentSensitive);
    const stringOperand = typeof operand === "string";
    return freezeCondition({
      _tag: "condition",
      field: fieldName,
      type,
      caseSensitive: stringOperand && options.caseSensitive,
      accentSensitive: stringOperand && options.accentSensitive,
      filter: operand,
    });
  }
  if (type === "in") {
    const candidates = denseArraySnapshot(filter, `Filter condition ${fieldName} in.filter`);
    const unique = new Map<string, RuntimeFilterScalar>();
    let hasStringCandidate = false;
    for (const candidate of candidates) {
      const normalized = equalityOperand(
        candidate,
        field,
        options.caseSensitive,
        options.accentSensitive,
      );
      hasStringCandidate ||= typeof normalized === "string";
      unique.set(operandKey(normalized), normalized);
    }
    const normalized = [...unique.entries()]
      .toSorted(([left], [right]) => Number(left > right) - Number(left < right))
      .map(([, candidate]) => candidate);
    if (normalized.length === 0) {
      return undefined;
    }
    return freezeCondition({
      _tag: "condition",
      field: fieldName,
      type,
      caseSensitive: hasStringCandidate && options.caseSensitive,
      accentSensitive: hasStringCandidate && options.accentSensitive,
      filter: Object.freeze(normalized),
    });
  }
  if (textSearchTypes.has(type)) {
    if (!field.hasString || typeof filter !== "string") {
      return fail(`Filter field ${fieldName} does not support ${type}.`);
    }
    const normalized = normalizeText(filter, options.caseSensitive, options.accentSensitive);
    if (normalized.length === 0) {
      return fail(`Filter condition ${fieldName} ${type} requires a non-empty search value.`);
    }
    return freezeCondition({
      _tag: "condition",
      field: fieldName,
      type,
      caseSensitive: options.caseSensitive,
      accentSensitive: options.accentSensitive,
      filter: normalized,
    });
  }
  if (numericComparisonTypes.has(type)) {
    return freezeCondition({
      _tag: "condition",
      field: fieldName,
      type,
      caseSensitive: false,
      accentSensitive: false,
      filter: numericOperand(filter, field),
    });
  }
  const filterTo = ownEnumerableDataValue(record, "filterTo");
  const from = numericOperand(filter, field);
  const to = numericOperand(filterTo, field);
  const comparison = compareFilterValue(from, to);
  if (comparison === undefined || comparison >= 0) {
    return fail(`Filter condition ${fieldName} inRange requires filter < filterTo.`);
  }
  return freezeCondition({
    _tag: "condition",
    field: fieldName,
    type,
    caseSensitive: false,
    accentSensitive: false,
    filter: from,
    filterTo: to,
  });
};

const complementCondition = (
  condition: RuntimeFilterCondition,
  type: RuntimeFilterConditionType,
): RuntimeFilterCondition =>
  freezeCondition({
    _tag: "condition",
    field: condition.field,
    type,
    caseSensitive: condition.caseSensitive,
    accentSensitive: condition.accentSensitive,
    ...(condition.filter === undefined ? {} : { filter: condition.filter }),
  });

const boundedExpressionKey = (
  tag: string,
  expressions: ReadonlyArray<RuntimeFilterExpression>,
): string => `expression:${tag}:${expressions.length}`;

type StructuralKeyFrame =
  | { readonly _tag: "enter"; readonly expression: RuntimeFilterExpression }
  | { readonly _tag: "exit"; readonly expression: RuntimeFilterExpression };

const expressionStructuralKey = (expression: RuntimeFilterExpression): string => {
  const definitions: Array<string> = [];
  const identities = new WeakMap<object, number>();
  const identitiesByDefinition = new Map<string, number>();
  const frames: Array<StructuralKeyFrame> = [{ _tag: "enter", expression }];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    const current = frame.expression;
    if (frame._tag === "enter") {
      if (identities.has(current)) {
        continue;
      }
      frames.push({ _tag: "exit", expression: current });
      if (current._tag === "NOT") {
        frames.push({ _tag: "enter", expression: current.condition });
      } else if (current._tag === "group") {
        for (let index = current.conditions.length - 1; index >= 0; index -= 1) {
          frames.push({ _tag: "enter", expression: current.conditions[index]! });
        }
      }
      continue;
    }

    const definition =
      current._tag === "condition"
        ? `condition:${current.key.length}:${current.key}`
        : current._tag === "NOT"
          ? `NOT:${identities.get(current.condition)!}`
          : `${current.type}:${current.conditions.length}:${current.conditions
              .map((condition) => identities.get(condition)!)
              .join(",")}`;
    const existing = identitiesByDefinition.get(definition);
    const identity = existing ?? definitions.length;
    if (existing === undefined) {
      identitiesByDefinition.set(definition, identity);
      definitions.push(definition);
    }
    identities.set(current, identity);
  }
  const rootIdentity = identities.get(expression)!;
  return `expression-graph:${rootIdentity}:${definitions
    .map((definition, identity) => `${identity}:${definition.length}:${definition}`)
    .join("|")}`;
};

const compareCodeUnits = (left: string, right: string): number =>
  Number(left > right) - Number(left < right);

type StructuralIdentityTrie = {
  readonly branches: Map<number, StructuralIdentityTrie>;
  identity: number | undefined;
};

type StructuralIdentityModule = {
  readonly identityFor: (expression: RuntimeFilterExpression) => number;
};

const makeStructuralIdentityModule = (): StructuralIdentityModule => {
  let nextIdentity = 0;
  const byExpression = new WeakMap<object, number>();
  const conditionIdentities = new Map<string, number>();
  const negationIdentities = new Map<number, number>();
  const groupIdentityTries = new Map<string, StructuralIdentityTrie>();
  const allocateIdentity = (): number => {
    const identity = nextIdentity;
    nextIdentity += 1;
    return identity;
  };
  const identityFor = (expression: RuntimeFilterExpression): number => {
    const existing = byExpression.get(expression);
    if (existing !== undefined) {
      return existing;
    }
    let identity: number;
    if (expression._tag === "condition") {
      const conditionIdentity = conditionIdentities.get(expression.key);
      identity = conditionIdentity ?? allocateIdentity();
      conditionIdentities.set(expression.key, identity);
    } else if (expression._tag === "NOT") {
      const childIdentity = identityFor(expression.condition);
      const negationIdentity = negationIdentities.get(childIdentity);
      identity = negationIdentity ?? allocateIdentity();
      negationIdentities.set(childIdentity, identity);
    } else {
      const trieKey = `${expression.type}:${expression.conditions.length}`;
      const existingTrie = groupIdentityTries.get(trieKey);
      let trie: StructuralIdentityTrie;
      if (existingTrie === undefined) {
        const createdTrie: StructuralIdentityTrie = {
          branches: new Map(),
          identity: undefined,
        };
        trie = createdTrie;
        groupIdentityTries.set(trieKey, trie);
      } else {
        trie = existingTrie;
      }
      for (const condition of expression.conditions) {
        const childIdentity = identityFor(condition);
        let branch: StructuralIdentityTrie | undefined = trie.branches.get(childIdentity);
        if (branch === undefined) {
          branch = { branches: new Map(), identity: undefined };
          trie.branches.set(childIdentity, branch);
        }
        trie = branch;
      }
      identity = trie.identity ?? allocateIdentity();
      trie.identity = identity;
    }
    byExpression.set(expression, identity);
    return identity;
  };
  return { identityFor };
};

type StructuralComparisonNode = {
  readonly tag: string;
  readonly value: string;
  readonly children: ReadonlyArray<RuntimeFilterExpression>;
};

const structuralComparisonNode = (
  expression: RuntimeFilterExpression,
): StructuralComparisonNode => {
  switch (expression._tag) {
    case "condition":
      return { tag: expression._tag, value: expression.key, children: [] };
    case "NOT":
      return { tag: expression._tag, value: "", children: [expression.condition] };
    case "group":
      return {
        tag: expression._tag,
        value: `${expression.type}:${expression.conditions.length}`,
        children: expression.conditions,
      };
  }
};

export const compareRuntimeFilterExpressionStructure = (
  left: RuntimeFilterExpression,
  right: RuntimeFilterExpression,
): number => compareCanonicalFilterGraphs(left, right, structuralComparisonNode);

const canonicalGroup = (
  type: "AND" | "OR",
  candidates: ReadonlyArray<RuntimeFilterExpression>,
  identities: StructuralIdentityModule,
): RuntimeFilterExpression => {
  if (candidates.length === 1) {
    return candidates[0]!;
  }
  const conditions = candidates.toSorted(
    (left, right) =>
      compareCodeUnits(left.key, right.key) || compareRuntimeFilterExpressionStructure(left, right),
  );
  const frozen = Object.freeze(conditions);
  const group = Object.freeze({
    _tag: "group",
    key: boundedExpressionKey(type, frozen),
    type,
    conditions: frozen,
  } satisfies RuntimeFilterGroup);
  identities.identityFor(group);
  return group;
};

type FilterMaterializationModule = {
  readonly materializeDefined: (
    expression: RuntimeFilterExpression | DeferredFilterGroup,
  ) => RuntimeFilterExpression;
  readonly materialize: (expression: NormalizedExpression) => RuntimeFilterExpression | undefined;
};

const makeFilterMaterializationModule = (
  identities: StructuralIdentityModule,
): FilterMaterializationModule => {
  const materializedGroups = new WeakMap<DeferredFilterGroup, RuntimeFilterExpression>();
  const materializeGroup = (group: DeferredFilterGroup): RuntimeFilterExpression => {
    const existing = materializedGroups.get(group);
    if (existing !== undefined) {
      return existing;
    }
    const candidates = collectCanonicalFilterGraphLeaves<
      DeferredExpressionSequence | RuntimeFilterExpression,
      RuntimeFilterExpression,
      number
    >(
      [group.sequence],
      (current) => {
        if (current._tag === "one") {
          return { _tag: "expand", children: [current.expression] };
        }
        if (current._tag === "concat") {
          return { _tag: "expand", children: [current.left, current.right] };
        }
        if (current._tag === "group" && current.type === group.type) {
          return { _tag: "expand", children: current.conditions };
        }
        return { _tag: "leaf", leaf: current };
      },
      identities.identityFor,
    );
    const materialized = canonicalGroup(group.type, candidates, identities);
    materializedGroups.set(group, materialized);
    return materialized;
  };
  const materializeDefined = (
    expression: RuntimeFilterExpression | DeferredFilterGroup,
  ): RuntimeFilterExpression =>
    expression._tag === "deferredGroup" ? materializeGroup(expression) : expression;
  const materialize = (expression: NormalizedExpression): RuntimeFilterExpression | undefined =>
    expression === undefined ? undefined : materializeDefined(expression);
  return { materialize, materializeDefined };
};

const expressionSequence = (expression: RuntimeFilterExpression): DeferredExpressionSequence =>
  Object.freeze({ _tag: "one", expression });

const concatenateExpressionSequences = (
  left: DeferredExpressionSequence,
  right: DeferredExpressionSequence,
): DeferredExpressionSequence => Object.freeze({ _tag: "concat", left, right });

const normalizeGroup = (
  type: "AND" | "OR",
  children: ReadonlyArray<NormalizedExpression>,
  materialization: FilterMaterializationModule,
): NormalizedExpression => {
  let sequence: DeferredExpressionSequence | undefined;
  for (const child of children) {
    if (child === undefined) {
      continue;
    }
    const next =
      child._tag === "deferredGroup" && child.type === type
        ? child.sequence
        : expressionSequence(materialization.materializeDefined(child));
    sequence = sequence === undefined ? next : concatenateExpressionSequences(sequence, next);
  }
  if (sequence === undefined) {
    return undefined;
  }
  if (sequence._tag === "one") {
    return sequence.expression;
  }
  return Object.freeze({ _tag: "deferredGroup", type, sequence });
};

const normalizeNegation = (
  child: NormalizedExpression,
  identities: StructuralIdentityModule,
  materialization: FilterMaterializationModule,
): NormalizedExpression => {
  const materializedChild = materialization.materialize(child);
  if (materializedChild === undefined) {
    return undefined;
  }
  if (materializedChild._tag === "NOT") {
    return materializedChild.condition;
  }
  if (materializedChild._tag === "condition") {
    const complement = complementCanonicalFilterType(materializedChild.type);
    if (complement !== undefined) {
      const condition = complementCondition(materializedChild, complement);
      identities.identityFor(condition);
      return condition;
    }
  }
  const negation = Object.freeze({
    _tag: "NOT",
    key: boundedExpressionKey("NOT", [materializedChild]),
    condition: materializedChild,
  } satisfies RuntimeFilterNegation);
  identities.identityFor(negation);
  return negation;
};

type EnterFrame = { readonly _tag: "enter"; readonly value: unknown };
type ExitFrame = {
  readonly _tag: "exit";
  readonly source: Readonly<Record<string, unknown>>;
  readonly kind: "AND" | "OR" | "NOT";
  readonly childCount: number;
};
type NormalizeFrame = EnterFrame | ExitFrame;

const normalizeExpression = (
  value: unknown,
  fields: ReadonlyMap<string, FilterFieldMetadata>,
  memo: WeakMap<object, NormalizedExpression>,
  completed: WeakSet<object>,
  active: WeakSet<object>,
  identities: StructuralIdentityModule,
  materialization: FilterMaterializationModule,
): NormalizedExpression => {
  const frames: Array<NormalizeFrame> = [{ _tag: "enter", value }];
  const results: Array<NormalizedExpression> = [];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      const children = results.splice(results.length - frame.childCount, frame.childCount);
      const normalized =
        frame.kind === "NOT"
          ? normalizeNegation(children[0], identities, materialization)
          : normalizeGroup(frame.kind, children, materialization);
      active.delete(frame.source);
      memo.set(frame.source, normalized);
      completed.add(frame.source);
      results.push(normalized);
      continue;
    }
    const source = requirePlainRecord(frame.value);
    if (completed.has(source)) {
      results.push(memo.get(source));
      continue;
    }
    if (active.has(source)) {
      fail("Filter expressions must not contain cycles.");
    }
    const type = ownEnumerableDataValue(source, "type");
    if (type === "AND" || type === "OR") {
      validateExactRecordKeys(source, new Set(["type", "conditions"]));
      const children = denseArraySnapshot(
        ownEnumerableDataValue(source, "conditions"),
        `Filter group ${type}.conditions`,
      );
      active.add(source);
      frames.push({
        _tag: "exit",
        source,
        kind: type,
        childCount: children.length,
      });
      for (let index = children.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: children[index] });
      }
      continue;
    }
    if (type === "NOT") {
      validateExactRecordKeys(source, new Set(["type", "condition"]));
      const child = ownEnumerableDataValue(source, "condition");
      active.add(source);
      frames.push({ _tag: "exit", source, kind: "NOT", childCount: 1 });
      frames.push({ _tag: "enter", value: child });
      continue;
    }
    const normalized = makeCondition(source, fields);
    if (normalized !== undefined) {
      identities.identityFor(normalized);
    }
    memo.set(source, normalized);
    completed.add(source);
    results.push(normalized);
  }
  return results[0];
};

export const normalizeWhere = (
  where: unknown,
  fields: ReadonlyMap<string, FilterFieldMetadata>,
): RuntimeFilterExpression | undefined => {
  const roots = denseArraySnapshot(where, "Query where");
  const memo = new WeakMap<object, NormalizedExpression>();
  const completed = new WeakSet<object>();
  const active = new WeakSet<object>();
  const identities = makeStructuralIdentityModule();
  const materialization = makeFilterMaterializationModule(identities);
  const normalized: Array<NormalizedExpression> = [];
  for (const root of roots) {
    normalized.push(
      normalizeExpression(root, fields, memo, completed, active, identities, materialization),
    );
  }
  const root = materialization.materialize(normalizeGroup("AND", normalized, materialization));
  if (root === undefined || root._tag === "condition") {
    return root;
  }
  const key = expressionStructuralKey(root);
  return root._tag === "group"
    ? Object.freeze({ _tag: "group", key, type: root.type, conditions: root.conditions })
    : Object.freeze({ _tag: "NOT", key, condition: root.condition });
};

export const normalizeFilterText = normalizeText;
