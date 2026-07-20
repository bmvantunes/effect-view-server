import type {
  RuntimeFilterCondition,
  RuntimeFilterExpression,
  RuntimeFilterScalar,
} from "./filter-expression";
import { normalizeFilterText } from "./filter-expression";
import { compareFilterValue } from "./query-value";
import { predicateFilterPlans, type TopicRawPredicatePlan } from "./raw-predicate-plan";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { scalarEqualityKey } from "./row-values";

type RowObject = object;

export type CompiledRawPredicate<Row extends RowObject> = {
  readonly plan: TopicRawPredicatePlan;
  readonly matches: (row: Row, storageKey?: string) => boolean;
};

type PredicateInstruction<Row extends RowObject> =
  | {
      readonly _tag: "condition";
      readonly matches: (row: Row) => boolean;
      readonly whenFalse: number;
      readonly whenTrue: number;
    }
  | { readonly _tag: "return"; readonly value: boolean };

type PredicateLabel = { index: number };

type UnresolvedPredicateInstruction<Row extends RowObject> =
  | {
      readonly _tag: "condition";
      readonly matches: (row: Row) => boolean;
      readonly whenFalse: PredicateLabel;
      readonly whenTrue: PredicateLabel;
    }
  | { readonly _tag: "return"; readonly value: boolean };

type CompileTask =
  | {
      readonly _tag: "expression";
      readonly expression: RuntimeFilterExpression;
      readonly whenFalse: PredicateLabel;
      readonly whenTrue: PredicateLabel;
    }
  | { readonly _tag: "mark"; readonly label: PredicateLabel; readonly value?: boolean };

type DagPredicateInstruction<Row extends RowObject> =
  | { readonly _tag: "condition"; readonly matches: (row: Row) => boolean }
  | { readonly _tag: "NOT"; readonly condition: number }
  | {
      readonly _tag: "group";
      readonly type: "AND" | "OR";
      readonly conditions: ReadonlyArray<number>;
    };

type DagCompileFrame =
  | { readonly _tag: "enter"; readonly expression: RuntimeFilterExpression }
  | { readonly _tag: "exit"; readonly expression: RuntimeFilterExpression };

type DagEvaluationScratch = {
  readonly values: Uint8Array;
  readonly evaluated: Uint8Array;
  readonly visited: Uint32Array;
  readonly nodeStack: Uint32Array;
  readonly childPositions: Uint32Array;
  visitedCount: number;
};

const isScalarArray = (
  value: RuntimeFilterScalar | ReadonlyArray<RuntimeFilterScalar>,
): value is ReadonlyArray<RuntimeFilterScalar> => Array.isArray(value);

const fieldPathValue = (row: object, segments: ReadonlyArray<string>): unknown => {
  let value: unknown = row;
  for (const segment of segments) {
    if ((typeof value !== "object" || value === null) && typeof value !== "function") {
      return undefined;
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, segment);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }
    value = descriptor.value;
  }
  return value;
};

const blank = (value: unknown): boolean => value === undefined || value === null || value === "";

const compileCondition = <Row extends RowObject>(
  condition: RuntimeFilterCondition,
  metadata: RawQueryCompilerMetadata,
): ((row: Row) => boolean) => {
  const field = metadata.filterFields.get(condition.field);
  if (field === undefined) {
    return () => false;
  }
  const filter = condition.filter;
  const membershipKeys =
    condition.type === "in" && filter !== undefined && isScalarArray(filter)
      ? new Set(filter.map(scalarEqualityKey))
      : undefined;
  const textValue = (value: unknown): string | undefined =>
    typeof value === "string"
      ? normalizeFilterText(value, condition.caseSensitive, condition.accentSensitive)
      : undefined;
  const equals = (value: unknown, operand: RuntimeFilterScalar): boolean => {
    if (typeof operand === "string") {
      return textValue(value) === operand;
    }
    return field.semantics.is(value) && field.semantics.equivalent(value, operand);
  };
  const membershipKey = (value: unknown): string | undefined => {
    if (typeof value === "string") {
      return scalarEqualityKey(
        normalizeFilterText(value, condition.caseSensitive, condition.accentSensitive),
      );
    }
    return field.semantics.is(value) ? scalarEqualityKey(value) : undefined;
  };
  const matchesValue = (value: unknown): boolean => {
    switch (condition.type) {
      case "blank":
        return blank(value);
      case "notBlank":
        return !blank(value);
      case "equals":
      case "notEqual": {
        const matches =
          filter !== undefined && !isScalarArray(filter) ? equals(value, filter) : false;
        return condition.type === "equals" ? matches : !matches;
      }
      case "in":
        return membershipKeys?.has(membershipKey(value) ?? "") === true;
      case "contains":
      case "notContains":
      case "startsWith":
      case "endsWith": {
        const actual = textValue(value);
        const expected = typeof filter === "string" ? filter : undefined;
        const positive =
          actual !== undefined && expected !== undefined
            ? condition.type === "startsWith"
              ? actual.startsWith(expected)
              : condition.type === "endsWith"
                ? actual.endsWith(expected)
                : actual.includes(expected)
            : false;
        return condition.type === "notContains" ? !positive : positive;
      }
      case "greaterThan":
      case "greaterThanOrEqual":
      case "lessThan":
      case "lessThanOrEqual": {
        if (filter === undefined || isScalarArray(filter)) {
          return false;
        }
        const comparison = compareFilterValue(value, filter);
        if (comparison === undefined) {
          return false;
        }
        return condition.type === "greaterThan"
          ? comparison > 0
          : condition.type === "greaterThanOrEqual"
            ? comparison >= 0
            : condition.type === "lessThan"
              ? comparison < 0
              : comparison <= 0;
      }
      case "inRange": {
        if (filter === undefined || isScalarArray(filter) || condition.filterTo === undefined) {
          return false;
        }
        const lower = compareFilterValue(value, filter);
        const upper = compareFilterValue(value, condition.filterTo);
        return lower !== undefined && upper !== undefined && lower >= 0 && upper < 0;
      }
    }
  };
  return (row) => matchesValue(fieldPathValue(row, field.segments));
};

const compileInstructions = <Row extends RowObject>(
  where: RuntimeFilterExpression,
  metadata: RawQueryCompilerMetadata,
): ReadonlyArray<PredicateInstruction<Row>> => {
  const instructions: Array<UnresolvedPredicateInstruction<Row>> = [];
  const whenTrue: PredicateLabel = { index: -1 };
  const whenFalse: PredicateLabel = { index: -1 };
  const tasks: Array<CompileTask> = [
    { _tag: "mark", label: whenFalse, value: false },
    { _tag: "mark", label: whenTrue, value: true },
    { _tag: "expression", expression: where, whenTrue, whenFalse },
  ];
  while (tasks.length > 0) {
    const task = tasks.pop()!;
    if (task._tag === "mark") {
      task.label.index = instructions.length;
      if (task.value !== undefined) {
        instructions.push({ _tag: "return", value: task.value });
      }
      continue;
    }
    const expression = task.expression;
    if (expression._tag === "condition") {
      instructions.push({
        _tag: "condition",
        matches: compileCondition(expression, metadata),
        whenFalse: task.whenFalse,
        whenTrue: task.whenTrue,
      });
      continue;
    }
    if (expression._tag === "NOT") {
      tasks.push({
        _tag: "expression",
        expression: expression.condition,
        whenFalse: task.whenTrue,
        whenTrue: task.whenFalse,
      });
      continue;
    }
    const continuationLabels = expression.conditions.slice(1).map(() => ({ index: -1 }));
    const groupTasks: Array<CompileTask> = [];
    for (let index = 0; index < expression.conditions.length; index += 1) {
      if (index > 0) {
        groupTasks.push({ _tag: "mark", label: continuationLabels[index - 1]! });
      }
      const continuation = continuationLabels[index];
      groupTasks.push({
        _tag: "expression",
        expression: expression.conditions[index]!,
        whenFalse: expression.type === "AND" ? task.whenFalse : (continuation ?? task.whenFalse),
        whenTrue: expression.type === "OR" ? task.whenTrue : (continuation ?? task.whenTrue),
      });
    }
    for (let index = groupTasks.length - 1; index >= 0; index -= 1) {
      tasks.push(groupTasks[index]!);
    }
  }
  return Object.freeze(
    instructions.map(
      (instruction): PredicateInstruction<Row> =>
        instruction._tag === "condition"
          ? Object.freeze({
              _tag: "condition",
              matches: instruction.matches,
              whenFalse: instruction.whenFalse.index,
              whenTrue: instruction.whenTrue.index,
            })
          : Object.freeze(instruction),
    ),
  );
};

const expressionHasSharedNodes = (where: RuntimeFilterExpression): boolean => {
  const seen = new WeakSet<object>();
  const pending: Array<RuntimeFilterExpression> = [where];
  while (pending.length > 0) {
    const expression = pending.pop()!;
    if (seen.has(expression)) {
      return true;
    }
    seen.add(expression);
    if (expression._tag === "NOT") {
      pending.push(expression.condition);
    } else if (expression._tag === "group") {
      for (const condition of expression.conditions) {
        pending.push(condition);
      }
    }
  }
  return false;
};

const makeDagEvaluationScratch = (instructionCount: number): DagEvaluationScratch => ({
  values: new Uint8Array(instructionCount),
  evaluated: new Uint8Array(instructionCount),
  visited: new Uint32Array(instructionCount),
  nodeStack: new Uint32Array(instructionCount),
  childPositions: new Uint32Array(instructionCount),
  visitedCount: 0,
});

const compileDagMatcher = <Row extends RowObject>(
  where: RuntimeFilterExpression,
  metadata: RawQueryCompilerMetadata,
): ((row: Row) => boolean) => {
  const instructions: Array<DagPredicateInstruction<Row>> = [];
  const instructionByExpression = new WeakMap<object, number>();
  const frames: Array<DagCompileFrame> = [{ _tag: "enter", expression: where }];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    const expression = frame.expression;
    if (frame._tag === "enter") {
      if (instructionByExpression.has(expression)) {
        continue;
      }
      frames.push({ _tag: "exit", expression });
      if (expression._tag === "NOT") {
        frames.push({ _tag: "enter", expression: expression.condition });
      } else if (expression._tag === "group") {
        for (let index = expression.conditions.length - 1; index >= 0; index -= 1) {
          frames.push({ _tag: "enter", expression: expression.conditions[index]! });
        }
      }
      continue;
    }

    const instruction: DagPredicateInstruction<Row> =
      expression._tag === "condition"
        ? { _tag: "condition", matches: compileCondition(expression, metadata) }
        : expression._tag === "NOT"
          ? { _tag: "NOT", condition: instructionByExpression.get(expression.condition)! }
          : {
              _tag: "group",
              type: expression.type,
              conditions: Object.freeze(
                expression.conditions.map((condition) => instructionByExpression.get(condition)!),
              ),
            };
    instructionByExpression.set(expression, instructions.length);
    instructions.push(Object.freeze(instruction));
  }

  const rootInstruction = instructionByExpression.get(where)!;
  const scratch = makeDagEvaluationScratch(instructions.length);
  return (row) => {
    for (let index = 0; index < scratch.visitedCount; index += 1) {
      scratch.evaluated[scratch.visited[index]!] = 0;
    }
    scratch.visitedCount = 0;
    let depth = 0;
    scratch.nodeStack[0] = rootInstruction;
    scratch.childPositions[0] = 0;
    while (depth >= 0) {
      const instructionIndex = scratch.nodeStack[depth]!;
      const instruction = instructions[instructionIndex]!;
      if (instruction._tag === "condition") {
        scratch.values[instructionIndex] = instruction.matches(row) ? 1 : 0;
        scratch.evaluated[instructionIndex] = 1;
        scratch.visited[scratch.visitedCount] = instructionIndex;
        scratch.visitedCount += 1;
        depth -= 1;
        continue;
      }
      if (instruction._tag === "NOT") {
        if (scratch.evaluated[instruction.condition] === 0) {
          depth += 1;
          scratch.nodeStack[depth] = instruction.condition;
          scratch.childPositions[depth] = 0;
          continue;
        }
        scratch.values[instructionIndex] = scratch.values[instruction.condition] === 0 ? 1 : 0;
        scratch.evaluated[instructionIndex] = 1;
        scratch.visited[scratch.visitedCount] = instructionIndex;
        scratch.visitedCount += 1;
        depth -= 1;
        continue;
      }
      const childPosition = scratch.childPositions[depth]!;
      if (childPosition >= instruction.conditions.length) {
        scratch.values[instructionIndex] = instruction.type === "AND" ? 1 : 0;
        scratch.evaluated[instructionIndex] = 1;
        scratch.visited[scratch.visitedCount] = instructionIndex;
        scratch.visitedCount += 1;
        depth -= 1;
        continue;
      }
      const childInstruction = instruction.conditions[childPosition]!;
      if (scratch.evaluated[childInstruction] === 0) {
        depth += 1;
        scratch.nodeStack[depth] = childInstruction;
        scratch.childPositions[depth] = 0;
        continue;
      }
      const childMatches = scratch.values[childInstruction] === 1;
      if (instruction.type === "AND" ? !childMatches : childMatches) {
        scratch.values[instructionIndex] = instruction.type === "OR" ? 1 : 0;
        scratch.evaluated[instructionIndex] = 1;
        scratch.visited[scratch.visitedCount] = instructionIndex;
        scratch.visitedCount += 1;
        depth -= 1;
        continue;
      }
      scratch.childPositions[depth] = childPosition + 1;
    }
    return scratch.values[rootInstruction] === 1;
  };
};

const compileInstructionMatcher = <Row extends RowObject>(
  where: RuntimeFilterExpression,
  metadata: RawQueryCompilerMetadata,
): ((row: Row) => boolean) => {
  const instructions = compileInstructions<Row>(where, metadata);
  return (row) => {
    let instructionIndex = 0;
    while (true) {
      const instruction = instructions[instructionIndex]!;
      if (instruction._tag === "condition") {
        instructionIndex = instruction.matches(row) ? instruction.whenTrue : instruction.whenFalse;
        continue;
      }
      return instruction.value;
    }
  };
};

const compilePlan = (
  where: RuntimeFilterExpression,
  metadata: RawQueryCompilerMetadata,
): TopicRawPredicatePlan => {
  const conditions = where._tag === "group" && where.type === "AND" ? where.conditions : [where];
  const filters: Array<TopicRawPredicatePlan["filters"][number]> = [];
  let callbackRequired = false;
  for (const expression of conditions) {
    if (expression._tag !== "condition") {
      callbackRequired = true;
      continue;
    }
    const plan = predicateFilterPlans(expression, metadata);
    for (const filter of plan.filters) {
      filters.push(filter);
    }
    callbackRequired ||= plan.callbackRequired;
  }
  return Object.freeze({
    filters: Object.freeze(filters),
    callbackRequired,
    callbackSkippable: !callbackRequired,
  });
};

export const compileRawPredicate = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  where: RuntimeFilterExpression | undefined,
): CompiledRawPredicate<Row> => {
  if (where === undefined) {
    return Object.freeze({
      plan: Object.freeze({
        filters: Object.freeze([]),
        callbackRequired: false,
        callbackSkippable: true,
      }),
      matches: () => true,
    });
  }
  const matches = expressionHasSharedNodes(where)
    ? compileDagMatcher<Row>(where, metadata)
    : compileInstructionMatcher<Row>(where, metadata);
  return Object.freeze({
    plan: compilePlan(where, metadata),
    matches,
  });
};
