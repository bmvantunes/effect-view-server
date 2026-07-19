import { isWireSafeBigDecimal } from "@effect-view-server/effect-utils";
import { format, isBigDecimal, normalize } from "effect/BigDecimal";

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

type PlainRecordSnapshot = {
  readonly source: object;
  readonly entries: ReadonlyArray<readonly [string, unknown]>;
};

const plainRecordSnapshot = (value: unknown): PlainRecordSnapshot => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    Object.getPrototypeOf(value) !== Object.prototype ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    return failInvalidWhere();
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return failInvalidWhere();
    }
    entries.push([key, descriptor.value]);
  }
  return { source: value, entries };
};

const denseArrayValues = (value: unknown): ReadonlyArray<unknown> => {
  if (
    !Array.isArray(value) ||
    Object.getPrototypeOf(value) !== Array.prototype ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    return failInvalidWhere();
  }
  // Array.isArray plus the exact Array prototype guarantees the non-configurable data descriptor.
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")!;
  const length: number = lengthDescriptor.value;
  const values: Array<unknown> = [];
  const allowed = new Set(["length"]);
  for (let index = 0; index < length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return failInvalidWhere();
    }
    values.push(descriptor.value);
  }
  if (Object.getOwnPropertyNames(value).some((key) => !allowed.has(key))) {
    return failInvalidWhere();
  }
  return values;
};

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
  if (isBigDecimal(value) && isWireSafeBigDecimal(value)) {
    return JSON.stringify(["bigDecimal", format(normalize(value))]);
  }
  return failInvalidWhere();
};

const numericScalarKey = (value: unknown): string => {
  if (
    (typeof value !== "number" || !Number.isFinite(value)) &&
    typeof value !== "bigint" &&
    !(isBigDecimal(value) && isWireSafeBigDecimal(value))
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
  if (isBigDecimal(value) && isWireSafeBigDecimal(value)) {
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
        hasString ||= typeof candidate === "string";
        unique.add(semanticScalarKey(candidate, options.caseSensitive, options.accentSensitive));
      }
      if (unique.size === 0) {
        return undefined;
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
    const hasString = typeof filter === "string";
    const normalizedText =
      hasString && textSearch
        ? normalizeText(filter, options.caseSensitive, options.accentSensitive)
        : undefined;
    if (normalizedText === "") {
      return failInvalidWhere();
    }
    const filterKey =
      normalizedText === undefined
        ? semanticScalarKey(filter, options.caseSensitive, options.accentSensitive)
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

const complementType = (type: CanonicalConditionType): CanonicalConditionType | undefined => {
  switch (type) {
    case "equals":
      return "notEqual";
    case "notEqual":
      return "equals";
    case "contains":
      return "notContains";
    case "notContains":
      return "contains";
    case "blank":
      return "notBlank";
    case "notBlank":
      return "blank";
    default:
      return undefined;
  }
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

const compareStrings = (left: string, right: string): number =>
  Number(left > right) - Number(left < right);

export const compareCanonicalWhereExpressions = (
  left: CanonicalExpression,
  right: CanonicalExpression,
): number => {
  const frames: Array<readonly [CanonicalExpression, CanonicalExpression]> = [[left, right]];
  const compared = new WeakMap<object, WeakSet<object>>();
  while (frames.length > 0) {
    const [leftExpression, rightExpression] = frames.pop()!;
    if (leftExpression === rightExpression) {
      continue;
    }
    const existing = compared.get(leftExpression);
    if (existing?.has(rightExpression) === true) {
      continue;
    }
    if (existing === undefined) {
      compared.set(leftExpression, new WeakSet([rightExpression]));
    } else {
      existing.add(rightExpression);
    }
    const leftNode = comparisonNode(leftExpression);
    const rightNode = comparisonNode(rightExpression);
    const tagComparison = compareStrings(leftNode.tag, rightNode.tag);
    if (tagComparison !== 0) {
      return tagComparison;
    }
    const valueComparison = compareStrings(leftNode.value, rightNode.value);
    if (valueComparison !== 0) {
      return valueComparison;
    }
    for (let index = leftNode.children.length - 1; index >= 0; index -= 1) {
      frames.push([leftNode.children[index]!, rightNode.children[index]!]);
    }
  }
  return 0;
};

const canonicalGroup = (
  type: "AND" | "OR",
  children: ReadonlyArray<CanonicalExpression | undefined>,
  identities: ExpressionIdentityModule,
): CanonicalExpression | undefined => {
  const pending = [...children].reverse();
  const unique: Array<CanonicalExpression> = [];
  const seen = new Set<number>();
  while (pending.length > 0) {
    const child = pending.pop();
    if (child === undefined) {
      continue;
    }
    if (child._tag === "group" && child.type === type) {
      for (let index = child.conditions.length - 1; index >= 0; index -= 1) {
        pending.push(child.conditions[index]);
      }
      continue;
    }
    const identity = identities.identityFor(child);
    if (!seen.has(identity)) {
      seen.add(identity);
      unique.push(child);
    }
  }
  if (unique.length === 0) {
    return undefined;
  }
  if (unique.length === 1) {
    return unique[0];
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
  child: CanonicalExpression | undefined,
  identities: ExpressionIdentityModule,
): CanonicalExpression | undefined => {
  if (child === undefined) {
    return undefined;
  }
  if (child._tag === "NOT") {
    return child.condition;
  }
  if (child._tag === "condition") {
    const complement = complementType(child.type);
    if (complement !== undefined) {
      const condition = complementedCondition(child, complement);
      identities.identityFor(condition);
      return condition;
    }
  }
  const negation: CanonicalNegation = { _tag: "NOT", condition: child };
  identities.identityFor(negation);
  return negation;
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
  memo: WeakMap<object, CanonicalExpression | undefined>,
  complete: WeakSet<object>,
  active: WeakSet<object>,
  identities: ExpressionIdentityModule,
): CanonicalExpression | undefined => {
  const frames: Array<NormalizeFrame> = [{ _tag: "enter", value: input }];
  const results: Array<CanonicalExpression | undefined> = [];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      const children = results.splice(results.length - frame.childCount, frame.childCount);
      const normalized =
        frame.kind === "NOT"
          ? canonicalNegation(children[0], identities)
          : canonicalGroup(frame.kind, children, identities);
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
    const condition = canonicalCondition(values);
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

export const canonicalWhereKey = (where: unknown): string | undefined => {
  const roots = denseArrayValues(where);
  const memo = new WeakMap<object, CanonicalExpression | undefined>();
  const complete = new WeakSet<object>();
  const active = new WeakSet<object>();
  const identities = makeExpressionIdentityModule();
  const normalized: Array<CanonicalExpression | undefined> = [];
  for (const root of roots) {
    normalized.push(normalizeExpression(root, memo, complete, active, identities));
  }
  const expression = canonicalGroup("AND", normalized, identities);
  return expression === undefined ? undefined : serializeExpression(expression);
};
