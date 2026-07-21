import {
  collectCanonicalFilterGraphLeaves,
  compareCanonicalFilterGraphs,
  complementCanonicalFilterType,
  wireSafeBigDecimalSemanticKey,
} from "@effect-view-server/effect-utils";
import { Result } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import {
  denseArrayValues as structuralDenseArrayValues,
  plainRecordSnapshot as structuralPlainRecordSnapshot,
} from "./query-structural-data";

type CanonicalConditionType =
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

type CanonicalCondition = {
  readonly _tag: "condition";
  readonly field: string;
  readonly type: CanonicalConditionType;
  readonly caseSensitive: boolean;
  readonly accentSensitive: boolean;
  readonly filterKey: string | undefined;
  readonly filterToKey: string | undefined;
  readonly key: string;
};

type CanonicalGroup = {
  readonly _tag: "group";
  readonly type: "AND" | "OR";
  readonly conditions: ReadonlyArray<CanonicalExpression>;
};

type CanonicalNegation = {
  readonly _tag: "NOT";
  readonly condition: CanonicalExpression;
};

type CanonicalExpression = CanonicalCondition | CanonicalGroup | CanonicalNegation;

export type CanonicalWhereFieldContract = {
  readonly materialize: (value: unknown) => unknown;
  readonly supportsText: boolean;
};

export type CanonicalWhereFieldContracts = ReadonlyMap<string, CanonicalWhereFieldContract>;

type DeferredExpressionSequence =
  | {
      readonly _tag: "one";
      readonly expression: CanonicalExpression;
    }
  | {
      readonly _tag: "concat";
      readonly left: DeferredExpressionSequence;
      readonly right: DeferredExpressionSequence;
    };

type DeferredCanonicalGroup = {
  readonly _tag: "deferredGroup";
  readonly type: "AND" | "OR";
  readonly sequence: DeferredExpressionSequence;
};

type NormalizedExpression = CanonicalExpression | DeferredCanonicalGroup | undefined;

type ExpressionIdentityTrie = {
  readonly branches: Map<number, ExpressionIdentityTrie>;
  identity: number | undefined;
};

type ExpressionIdentityModule = {
  readonly identityFor: (expression: CanonicalExpression) => number;
};

const failInvalidWhere = (): never => {
  throw new TypeError("Query where cannot be used as a stable identity.");
};

const plainRecordSnapshot = (value: unknown) =>
  structuralPlainRecordSnapshot(value, failInvalidWhere, failInvalidWhere);

const denseArrayValues = (value: unknown): ReadonlyArray<unknown> =>
  structuralDenseArrayValues(value, failInvalidWhere, failInvalidWhere, failInvalidWhere);

type RecordValues = {
  readonly source: object;
  readonly values: ReadonlyMap<string, unknown>;
};

const recordValues = (value: unknown): RecordValues => {
  const snapshot = plainRecordSnapshot(value);
  return { source: snapshot.source, values: new Map(snapshot.entries) };
};

const requiredValue = (values: ReadonlyMap<string, unknown>, key: string): unknown =>
  values.has(key) ? values.get(key) : failInvalidWhere();

const requireExactKeys = (
  values: ReadonlyMap<string, unknown>,
  allowed: ReadonlySet<string>,
): void => {
  if (values.size !== allowed.size) {
    failInvalidWhere();
  }
  for (const key of values.keys()) {
    if (!allowed.has(key)) {
      failInvalidWhere();
    }
  }
};

const requireAllowedKeys = (
  values: ReadonlyMap<string, unknown>,
  required: ReadonlySet<string>,
  allowed: ReadonlySet<string>,
): void => {
  for (const key of required) {
    if (!values.has(key)) {
      failInvalidWhere();
    }
  }
  for (const key of values.keys()) {
    if (!allowed.has(key)) {
      failInvalidWhere();
    }
  }
};

const normalizeText = (value: string, caseSensitive: boolean, accentSensitive: boolean): string => {
  let normalized = value.normalize("NFD");
  if (!accentSensitive) {
    normalized = normalized.replace(/\p{M}+/gu, "");
  }
  return caseSensitive ? normalized : normalized.toLowerCase();
};

const textOptions = (
  values: ReadonlyMap<string, unknown>,
): {
  readonly accentSensitive: boolean;
  readonly caseSensitive: boolean;
} => {
  const hasCaseSensitive = values.has("caseSensitive");
  const hasAccentSensitive = values.has("accentSensitive");
  const caseSensitive = hasCaseSensitive ? values.get("caseSensitive") : false;
  const accentSensitive = hasAccentSensitive ? values.get("accentSensitive") : false;
  if (typeof caseSensitive !== "boolean" || typeof accentSensitive !== "boolean") {
    return failInvalidWhere();
  }
  return {
    caseSensitive,
    accentSensitive,
  };
};

const optionSyntax = (
  values: ReadonlyMap<string, unknown>,
  key: "caseSensitive" | "accentSensitive",
): ReadonlyArray<unknown> => (values.has(key) ? ["present", values.get(key)] : ["absent"]);

const preserveValidationSensitiveSyntax = (
  tokens: Set<string>,
  values: ReadonlyMap<string, unknown>,
  field: string,
  type: CanonicalConditionType,
  reason: "emptyIn" | "nonStringTextOptions",
  fieldContracts: CanonicalWhereFieldContracts | undefined,
): void => {
  const fieldContract = fieldContracts?.get(field);
  const hasTextOptions = values.has("caseSensitive") || values.has("accentSensitive");
  const syntaxIsValidNoOp =
    fieldContract !== undefined &&
    (reason === "emptyIn"
      ? !hasTextOptions || fieldContract.supportsText
      : fieldContract.supportsText);
  if (syntaxIsValidNoOp) {
    return;
  }
  tokens.add(
    JSON.stringify([
      reason,
      field,
      type,
      optionSyntax(values, "caseSensitive"),
      optionSyntax(values, "accentSensitive"),
    ]),
  );
};

const semanticScalarKey = (
  value: unknown,
  caseSensitive: boolean,
  accentSensitive: boolean,
): string => {
  if (value === null) {
    return JSON.stringify(["null"]);
  }
  if (typeof value === "string") {
    return JSON.stringify(["string", normalizeText(value, caseSensitive, accentSensitive)]);
  }
  if (typeof value === "boolean") {
    return JSON.stringify(["boolean", value]);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return JSON.stringify(["number", Object.is(value, -0) ? "0" : String(value)]);
  }
  if (typeof value === "bigint") {
    return JSON.stringify(["bigint", value.toString()]);
  }
  if (isBigDecimal(value)) {
    const semanticKey = wireSafeBigDecimalSemanticKey(value);
    if (semanticKey !== undefined) {
      return JSON.stringify(["bigDecimal", semanticKey]);
    }
  }
  return failInvalidWhere();
};

const exactScalarKey = (value: unknown): string => {
  if (value === null) {
    return JSON.stringify(["null"]);
  }
  if (typeof value === "string") {
    return JSON.stringify(["string", value]);
  }
  if (typeof value === "boolean") {
    return JSON.stringify(["boolean", value]);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return JSON.stringify(["number", Object.is(value, -0) ? "-0" : String(value)]);
  }
  if (typeof value === "bigint") {
    return JSON.stringify(["bigint", value.toString()]);
  }
  if (isBigDecimal(value) && wireSafeBigDecimalSemanticKey(value) !== undefined) {
    return JSON.stringify(["bigDecimal", value.value.toString(), String(value.scale)]);
  }
  return failInvalidWhere();
};

const materializeEqualityOperand = (
  value: unknown,
  field: string,
  type: CanonicalConditionType,
  validationSensitiveSyntax: Set<string>,
  fieldContracts: CanonicalWhereFieldContracts | undefined,
): unknown => {
  const fieldContract = fieldContracts?.get(field);
  if (fieldContract === undefined) {
    return value;
  }
  const materialized = Result.try(() => fieldContract.materialize(value));
  if (Result.isSuccess(materialized)) {
    return materialized.success;
  }
  validationSensitiveSyntax.add(
    JSON.stringify(["invalidFieldOperand", field, type, exactScalarKey(value)]),
  );
  return value;
};

const numericScalarKey = (value: unknown): string => {
  if (
    (typeof value !== "number" || !Number.isFinite(value)) &&
    typeof value !== "bigint" &&
    !(isBigDecimal(value) && wireSafeBigDecimalSemanticKey(value) !== undefined)
  ) {
    return failInvalidWhere();
  }
  return semanticScalarKey(value, false, false);
};

const numericKind = (value: unknown): "number" | "bigint" | "bigDecimal" => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return "number";
  }
  if (typeof value === "bigint") {
    return "bigint";
  }
  if (isBigDecimal(value) && wireSafeBigDecimalSemanticKey(value) !== undefined) {
    return "bigDecimal";
  }
  return failInvalidWhere();
};

const makeCondition = (input: Omit<CanonicalCondition, "_tag" | "key">): CanonicalCondition => ({
  _tag: "condition",
  ...input,
  key: JSON.stringify([
    "condition",
    input.field,
    input.type,
    input.caseSensitive,
    input.accentSensitive,
    input.filterKey ?? null,
    input.filterToKey ?? null,
  ]),
});

const conditionType = (value: unknown): CanonicalConditionType => {
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
      return value;
    default:
      return failInvalidWhere();
  }
};

const canonicalCondition = (
  values: ReadonlyMap<string, unknown>,
  validationSensitiveSyntax: Set<string>,
  fieldContracts: CanonicalWhereFieldContracts | undefined,
): CanonicalCondition | undefined => {
  const field = requiredValue(values, "field");
  const type = conditionType(requiredValue(values, "type"));
  if (typeof field !== "string") {
    return failInvalidWhere();
  }
  if (type === "blank" || type === "notBlank") {
    requireExactKeys(values, new Set(["field", "type"]));
    return makeCondition({
      field,
      type,
      caseSensitive: false,
      accentSensitive: false,
      filterKey: undefined,
      filterToKey: undefined,
    });
  }
  const textSearch =
    type === "contains" || type === "notContains" || type === "startsWith" || type === "endsWith";
  if (textSearch || type === "equals" || type === "notEqual" || type === "in") {
    requireAllowedKeys(
      values,
      new Set(["field", "type", "filter"]),
      new Set(["field", "type", "filter", "caseSensitive", "accentSensitive"]),
    );
    const options = textOptions(values);
    const filter = requiredValue(values, "filter");
    if (type === "in") {
      const candidates = denseArrayValues(filter);
      const unique = new Set<string>();
      let hasString = false;
      for (const candidate of candidates) {
        const materializedCandidate = materializeEqualityOperand(
          candidate,
          field,
          type,
          validationSensitiveSyntax,
          fieldContracts,
        );
        hasString ||= typeof materializedCandidate === "string";
        unique.add(
          semanticScalarKey(materializedCandidate, options.caseSensitive, options.accentSensitive),
        );
      }
      if (unique.size === 0) {
        preserveValidationSensitiveSyntax(
          validationSensitiveSyntax,
          values,
          field,
          type,
          "emptyIn",
          fieldContracts,
        );
        return undefined;
      }
      if (!hasString && (values.has("caseSensitive") || values.has("accentSensitive"))) {
        preserveValidationSensitiveSyntax(
          validationSensitiveSyntax,
          values,
          field,
          type,
          "nonStringTextOptions",
          fieldContracts,
        );
      }
      return makeCondition({
        field,
        type,
        caseSensitive: hasString && options.caseSensitive,
        accentSensitive: hasString && options.accentSensitive,
        filterKey: JSON.stringify([...unique].toSorted()),
        filterToKey: undefined,
      });
    }
    if (textSearch && typeof filter !== "string") {
      return failInvalidWhere();
    }
    const materializedFilter = textSearch
      ? filter
      : materializeEqualityOperand(filter, field, type, validationSensitiveSyntax, fieldContracts);
    const hasString = typeof materializedFilter === "string";
    if (!hasString && (values.has("caseSensitive") || values.has("accentSensitive"))) {
      preserveValidationSensitiveSyntax(
        validationSensitiveSyntax,
        values,
        field,
        type,
        "nonStringTextOptions",
        fieldContracts,
      );
    }
    const normalizedText =
      hasString && textSearch
        ? normalizeText(materializedFilter, options.caseSensitive, options.accentSensitive)
        : undefined;
    if (normalizedText === "") {
      return failInvalidWhere();
    }
    const filterKey =
      normalizedText === undefined
        ? semanticScalarKey(materializedFilter, options.caseSensitive, options.accentSensitive)
        : JSON.stringify(["string", normalizedText]);
    return makeCondition({
      field,
      type,
      caseSensitive: hasString && options.caseSensitive,
      accentSensitive: hasString && options.accentSensitive,
      filterKey,
      filterToKey: undefined,
    });
  }
  if (type === "inRange") {
    requireExactKeys(values, new Set(["field", "type", "filter", "filterTo"]));
    const filter = requiredValue(values, "filter");
    const filterTo = requiredValue(values, "filterTo");
    if (numericKind(filter) !== numericKind(filterTo)) {
      return failInvalidWhere();
    }
    return makeCondition({
      field,
      type,
      caseSensitive: false,
      accentSensitive: false,
      filterKey: numericScalarKey(filter),
      filterToKey: numericScalarKey(filterTo),
    });
  }
  requireExactKeys(values, new Set(["field", "type", "filter"]));
  return makeCondition({
    field,
    type,
    caseSensitive: false,
    accentSensitive: false,
    filterKey: numericScalarKey(requiredValue(values, "filter")),
    filterToKey: undefined,
  });
};

const complementedCondition = (
  condition: CanonicalCondition,
  type: CanonicalConditionType,
): CanonicalCondition =>
  makeCondition({
    field: condition.field,
    type,
    caseSensitive: condition.caseSensitive,
    accentSensitive: condition.accentSensitive,
    filterKey: condition.filterKey,
    filterToKey: condition.filterToKey,
  });

const makeExpressionIdentityModule = (): ExpressionIdentityModule => {
  let nextIdentity = 0;
  const byExpression = new WeakMap<object, number>();
  const conditions = new Map<string, number>();
  const negations = new Map<number, number>();
  const groups = new Map<string, ExpressionIdentityTrie>();
  const allocate = (): number => {
    const identity = nextIdentity;
    nextIdentity += 1;
    return identity;
  };
  const identityFor = (expression: CanonicalExpression): number => {
    const existing = byExpression.get(expression);
    if (existing !== undefined) {
      return existing;
    }
    let identity: number;
    if (expression._tag === "condition") {
      identity = conditions.get(expression.key) ?? allocate();
      conditions.set(expression.key, identity);
    } else if (expression._tag === "NOT") {
      const childIdentity = identityFor(expression.condition);
      identity = negations.get(childIdentity) ?? allocate();
      negations.set(childIdentity, identity);
    } else {
      const trieKey = `${expression.type}:${expression.conditions.length}`;
      const existingTrie = groups.get(trieKey);
      let trie: ExpressionIdentityTrie;
      if (existingTrie === undefined) {
        const created: ExpressionIdentityTrie = { branches: new Map(), identity: undefined };
        trie = created;
        groups.set(trieKey, created);
      } else {
        trie = existingTrie;
      }
      for (const child of expression.conditions) {
        const childIdentity = identityFor(child);
        let branch: ExpressionIdentityTrie | undefined = trie.branches.get(childIdentity);
        if (branch === undefined) {
          branch = { branches: new Map(), identity: undefined };
          trie.branches.set(childIdentity, branch);
        }
        trie = branch;
      }
      identity = trie.identity ?? allocate();
      trie.identity = identity;
    }
    byExpression.set(expression, identity);
    return identity;
  };
  return { identityFor };
};

type ComparisonNode = {
  readonly tag: string;
  readonly value: string;
  readonly children: ReadonlyArray<CanonicalExpression>;
};

const comparisonNode = (expression: CanonicalExpression): ComparisonNode => {
  if (expression._tag === "condition") {
    return { tag: "condition", value: expression.key, children: [] };
  }
  if (expression._tag === "NOT") {
    return { tag: "NOT", value: "", children: [expression.condition] };
  }
  return {
    tag: "group",
    value: `${expression.type}:${expression.conditions.length}`,
    children: expression.conditions,
  };
};

export const compareCanonicalWhereExpressions = (
  left: CanonicalExpression,
  right: CanonicalExpression,
): number => compareCanonicalFilterGraphs(left, right, comparisonNode);

const canonicalGroup = (
  type: "AND" | "OR",
  unique: ReadonlyArray<CanonicalExpression>,
  identities: ExpressionIdentityModule,
): CanonicalExpression => {
  if (unique.length === 1) {
    return unique[0]!;
  }
  const group: CanonicalGroup = {
    _tag: "group",
    type,
    conditions: unique.toSorted(compareCanonicalWhereExpressions),
  };
  identities.identityFor(group);
  return group;
};

const canonicalNegation = (
  child: NormalizedExpression,
  identities: ExpressionIdentityModule,
  materialization: FilterMaterializationModule,
): CanonicalExpression | undefined => {
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
      const condition = complementedCondition(materializedChild, complement);
      identities.identityFor(condition);
      return condition;
    }
  }
  const negation: CanonicalNegation = { _tag: "NOT", condition: materializedChild };
  identities.identityFor(negation);
  return negation;
};

type FilterMaterializationModule = {
  readonly materializeDefined: (
    expression: CanonicalExpression | DeferredCanonicalGroup,
  ) => CanonicalExpression;
  readonly materialize: (expression: NormalizedExpression) => CanonicalExpression | undefined;
};

const makeFilterMaterializationModule = (
  identities: ExpressionIdentityModule,
): FilterMaterializationModule => {
  const materializedGroups = new WeakMap<DeferredCanonicalGroup, CanonicalExpression>();
  const materializeGroup = (group: DeferredCanonicalGroup): CanonicalExpression => {
    const existing = materializedGroups.get(group);
    if (existing !== undefined) {
      return existing;
    }
    const candidates = collectCanonicalFilterGraphLeaves<
      DeferredExpressionSequence | CanonicalExpression,
      CanonicalExpression,
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
    expression: CanonicalExpression | DeferredCanonicalGroup,
  ): CanonicalExpression =>
    expression._tag === "deferredGroup" ? materializeGroup(expression) : expression;
  const materialize = (expression: NormalizedExpression): CanonicalExpression | undefined =>
    expression === undefined ? undefined : materializeDefined(expression);
  return { materialize, materializeDefined };
};

const expressionSequence = (expression: CanonicalExpression): DeferredExpressionSequence => ({
  _tag: "one",
  expression,
});

const concatenateExpressionSequences = (
  left: DeferredExpressionSequence,
  right: DeferredExpressionSequence,
): DeferredExpressionSequence => ({ _tag: "concat", left, right });

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
  return { _tag: "deferredGroup", type, sequence };
};

type NormalizeFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | {
      readonly _tag: "exit";
      readonly source: object;
      readonly kind: "AND" | "OR" | "NOT";
      readonly childCount: number;
    };

const normalizeExpression = (
  input: unknown,
  memo: WeakMap<object, NormalizedExpression>,
  complete: WeakSet<object>,
  active: WeakSet<object>,
  identities: ExpressionIdentityModule,
  materialization: FilterMaterializationModule,
  validationSensitiveSyntax: Set<string>,
  fieldContracts: CanonicalWhereFieldContracts | undefined,
): NormalizedExpression => {
  const frames: Array<NormalizeFrame> = [{ _tag: "enter", value: input }];
  const results: Array<NormalizedExpression> = [];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      const children = results.splice(results.length - frame.childCount, frame.childCount);
      const normalized =
        frame.kind === "NOT"
          ? canonicalNegation(children[0], identities, materialization)
          : normalizeGroup(frame.kind, children, materialization);
      active.delete(frame.source);
      memo.set(frame.source, normalized);
      complete.add(frame.source);
      results.push(normalized);
      continue;
    }
    const { source, values } = recordValues(frame.value);
    if (complete.has(source)) {
      results.push(memo.get(source));
      continue;
    }
    if (active.has(source)) {
      return failInvalidWhere();
    }
    const type = requiredValue(values, "type");
    if (type === "AND" || type === "OR") {
      requireExactKeys(values, new Set(["type", "conditions"]));
      const children = denseArrayValues(requiredValue(values, "conditions"));
      active.add(source);
      frames.push({ _tag: "exit", source, kind: type, childCount: children.length });
      for (let index = children.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: children[index] });
      }
      continue;
    }
    if (type === "NOT") {
      requireExactKeys(values, new Set(["type", "condition"]));
      active.add(source);
      frames.push({ _tag: "exit", source, kind: "NOT", childCount: 1 });
      frames.push({ _tag: "enter", value: requiredValue(values, "condition") });
      continue;
    }
    const condition = canonicalCondition(values, validationSensitiveSyntax, fieldContracts);
    if (condition !== undefined) {
      identities.identityFor(condition);
    }
    memo.set(source, condition);
    complete.add(source);
    results.push(condition);
  }
  return results[0];
};

type SerializeFrame =
  | { readonly _tag: "enter"; readonly expression: CanonicalExpression }
  | { readonly _tag: "exit"; readonly expression: CanonicalExpression };

const serializeExpression = (expression: CanonicalExpression): string => {
  const definitions: Array<string> = [];
  const identities = new WeakMap<object, number>();
  const identitiesByDefinition = new Map<string, number>();
  const frames: Array<SerializeFrame> = [{ _tag: "enter", expression }];
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
        ? JSON.stringify(["condition", current.key])
        : current._tag === "NOT"
          ? JSON.stringify(["NOT", identities.get(current.condition)!])
          : JSON.stringify([
              "group",
              current.type,
              current.conditions.map((condition) => identities.get(condition)!),
            ]);
    const existingIdentity = identitiesByDefinition.get(definition);
    const identity = existingIdentity ?? definitions.length;
    if (existingIdentity === undefined) {
      identitiesByDefinition.set(definition, identity);
      definitions.push(definition);
    }
    identities.set(current, identity);
  }
  return JSON.stringify(["expressionGraph", identities.get(expression)!, definitions]);
};

export const canonicalWhereKey = (
  where: unknown,
  fieldContracts?: CanonicalWhereFieldContracts,
): string | undefined => {
  const roots = denseArrayValues(where);
  const memo = new WeakMap<object, NormalizedExpression>();
  const complete = new WeakSet<object>();
  const active = new WeakSet<object>();
  const identities = makeExpressionIdentityModule();
  const materialization = makeFilterMaterializationModule(identities);
  const validationSensitiveSyntax = new Set<string>();
  const normalized: Array<NormalizedExpression> = [];
  for (const root of roots) {
    normalized.push(
      normalizeExpression(
        root,
        memo,
        complete,
        active,
        identities,
        materialization,
        validationSensitiveSyntax,
        fieldContracts,
      ),
    );
  }
  const expression = materialization.materialize(
    normalizeGroup("AND", normalized, materialization),
  );
  const semanticKey = expression === undefined ? undefined : serializeExpression(expression);
  return validationSensitiveSyntax.size === 0
    ? semanticKey
    : JSON.stringify([
        "whereWithValidationSensitiveSyntax",
        semanticKey ?? null,
        [...validationSensitiveSyntax].toSorted(),
      ]);
};
