import {
  divideUnsafe,
  fromBigInt,
  fromNumberUnsafe,
  isBigDecimal,
  subtract as subtractBigDecimal,
  sum as sumBigDecimal,
  type BigDecimal,
} from "effect/BigDecimal";
import * as Arr from "effect/Array";
import { trustedFieldValue } from "./row-values";
import type { SchemaValueSemantics } from "./topic-row-value-semantics";

type RowObject = object;

export type RuntimeGroupedAggregate =
  | {
      readonly aggFunc: "count";
    }
  | {
      readonly aggFunc: "countDistinct" | "min" | "max" | "avg";
      readonly field: string;
    }
  | {
      readonly aggFunc: "sum";
      readonly field: string;
      readonly resultKind: "bigint" | "bigDecimal";
    };

export type MissingGroupedAggregateInput = {
  readonly _tag: "Missing";
};

export type PresentGroupedAggregateInput = {
  readonly _tag: "Present";
  readonly value: unknown;
};

export type GroupedAggregateInput = MissingGroupedAggregateInput | PresentGroupedAggregateInput;

export type GroupedAggregateInputSemantics = {
  readonly field: string;
  readonly canonicalKey: (input: GroupedAggregateInput) => string;
  readonly compare: (left: GroupedAggregateInput, right: GroupedAggregateInput) => number;
  readonly equivalent: (left: GroupedAggregateInput, right: GroupedAggregateInput) => boolean;
  readonly read: (row: RowObject) => GroupedAggregateInput;
};

type RuntimeFieldGroupedAggregate = Exclude<RuntimeGroupedAggregate, { readonly aggFunc: "count" }>;

type CountGroupedAggregatePlan = {
  readonly kind: "count";
  readonly alias: string;
  readonly aggregate: Extract<RuntimeGroupedAggregate, { readonly aggFunc: "count" }>;
  readonly resultSemantics: SchemaValueSemantics;
  readonly stateIndex: number;
};

export type FieldGroupedAggregatePlan = {
  readonly kind: "field";
  readonly alias: string;
  readonly aggregate: RuntimeFieldGroupedAggregate;
  readonly input: GroupedAggregateInputSemantics;
  readonly resultSemantics: SchemaValueSemantics;
  readonly stateIndex: number;
};

export type GroupedAggregatePlan = CountGroupedAggregatePlan | FieldGroupedAggregatePlan;

type CountAggregateState = {
  readonly alias: string;
  readonly aggFunc: "count";
  count: bigint;
};

type CountDistinctAggregateState = {
  readonly alias: string;
  readonly aggFunc: "countDistinct";
  readonly inputSemantics: GroupedAggregateInputSemantics;
  readonly values: Map<string, number>;
  count: bigint;
};

type BigIntSumAggregateState = {
  readonly alias: string;
  readonly aggFunc: "sum";
  readonly inputSemantics: GroupedAggregateInputSemantics;
  readonly resultKind: "bigint";
  bigintTotal: bigint;
};

type BigDecimalSumAggregateState = {
  readonly alias: string;
  readonly aggFunc: "sum";
  readonly inputSemantics: GroupedAggregateInputSemantics;
  readonly resultKind: "bigDecimal";
  decimalTotal: BigDecimal;
};

type AverageAggregateState = {
  readonly alias: string;
  readonly aggFunc: "avg";
  readonly inputSemantics: GroupedAggregateInputSemantics;
  count: bigint;
  total: BigDecimal;
};

type MinMaxAggregateValueState = {
  count: number;
  readonly input: GroupedAggregateInput;
};

type MinMaxAggregateSelection =
  | {
      readonly _tag: "Empty";
    }
  | {
      readonly _tag: "Selected";
      readonly input: GroupedAggregateInput;
    };

type BaseMinMaxAggregateState = {
  readonly alias: string;
  readonly aggFunc: "min" | "max";
  readonly inputSemantics: GroupedAggregateInputSemantics;
  selection: MinMaxAggregateSelection;
};

export type RetainedMinMaxAggregateState = BaseMinMaxAggregateState & {
  readonly values: Map<string, MinMaxAggregateValueState>;
};

type NonMinMaxAggregateState =
  | CountAggregateState
  | CountDistinctAggregateState
  | BigIntSumAggregateState
  | BigDecimalSumAggregateState
  | AverageAggregateState;

type AggregateState =
  | NonMinMaxAggregateState
  | BaseMinMaxAggregateState
  | RetainedMinMaxAggregateState;

export type ReversibleAggregateState = NonMinMaxAggregateState | RetainedMinMaxAggregateState;

export type GroupState = {
  readonly key: string;
  readonly row: Record<string, unknown>;
  readonly aggregates: ReadonlyArray<AggregateState>;
};

export type MaterializedIncrementalGroupState = Omit<GroupState, "aggregates"> & {
  readonly aggregates: ReadonlyArray<ReversibleAggregateState>;
  readonly members: Map<string, RowObject>;
};

const bigintToDecimal = (value: bigint): BigDecimal => fromBigInt(value);

const numberToDecimal = (value: number): BigDecimal => fromNumberUnsafe(value);

const runtimeValueToDecimal = (value: unknown): BigDecimal | undefined => {
  if (isBigDecimal(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return bigintToDecimal(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return numberToDecimal(value);
  }
  return undefined;
};

const emptyAggregateState = (plan: GroupedAggregatePlan): AggregateState => {
  if (plan.kind === "count") {
    return {
      alias: plan.alias,
      aggFunc: "count",
      count: 0n,
    };
  }
  const aggregate = plan.aggregate;
  if (aggregate.aggFunc === "countDistinct") {
    return {
      alias: plan.alias,
      aggFunc: "countDistinct",
      inputSemantics: plan.input,
      values: new Map<string, number>(),
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "sum") {
    return aggregate.resultKind === "bigint"
      ? {
          alias: plan.alias,
          aggFunc: "sum",
          inputSemantics: plan.input,
          resultKind: "bigint",
          bigintTotal: 0n,
        }
      : {
          alias: plan.alias,
          aggFunc: "sum",
          inputSemantics: plan.input,
          resultKind: "bigDecimal",
          decimalTotal: fromBigInt(0n),
        };
  }
  if (aggregate.aggFunc === "avg") {
    return {
      alias: plan.alias,
      aggFunc: "avg",
      inputSemantics: plan.input,
      count: 0n,
      total: fromBigInt(0n),
    };
  }
  return {
    alias: plan.alias,
    aggFunc: aggregate.aggFunc,
    inputSemantics: plan.input,
    selection: { _tag: "Empty" },
  };
};

const emptyReversibleAggregateState = (plan: GroupedAggregatePlan): ReversibleAggregateState => {
  if (plan.kind === "count") {
    return {
      alias: plan.alias,
      aggFunc: "count",
      count: 0n,
    };
  }
  const aggregate = plan.aggregate;
  if (aggregate.aggFunc === "countDistinct") {
    return {
      alias: plan.alias,
      aggFunc: "countDistinct",
      inputSemantics: plan.input,
      values: new Map<string, number>(),
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "sum") {
    return aggregate.resultKind === "bigint"
      ? {
          alias: plan.alias,
          aggFunc: "sum",
          inputSemantics: plan.input,
          resultKind: "bigint",
          bigintTotal: 0n,
        }
      : {
          alias: plan.alias,
          aggFunc: "sum",
          inputSemantics: plan.input,
          resultKind: "bigDecimal",
          decimalTotal: fromBigInt(0n),
        };
  }
  if (aggregate.aggFunc === "avg") {
    return {
      alias: plan.alias,
      aggFunc: "avg",
      inputSemantics: plan.input,
      count: 0n,
      total: fromBigInt(0n),
    };
  }
  return {
    alias: plan.alias,
    aggFunc: aggregate.aggFunc,
    inputSemantics: plan.input,
    values: new Map<string, MinMaxAggregateValueState>(),
    selection: { _tag: "Empty" },
  };
};

const mapValueOr = <Key, Value>(
  values: ReadonlyMap<Key, Value>,
  key: Key,
  fallback: Value,
): Value => {
  const value = values.get(key);
  return value === undefined ? fallback : value;
};

const minMaxAggregateValueOr = (
  values: ReadonlyMap<string, MinMaxAggregateValueState>,
  key: string,
  input: GroupedAggregateInput,
  missingCount: number,
): MinMaxAggregateValueState => {
  const value = values.get(key);
  return value === undefined ? { count: missingCount, input } : value;
};

const updateAggregateState = (state: AggregateState, row: RowObject): void => {
  if (state.aggFunc === "count") {
    state.count += 1n;
    return;
  }
  const input = state.inputSemantics.read(row);
  const value = input._tag === "Missing" ? undefined : input.value;
  if (state.aggFunc === "countDistinct") {
    const key = state.inputSemantics.canonicalKey(input);
    const count = mapValueOr(state.values, key, 0);
    if (count === 0) {
      state.values.set(key, 1);
      state.count += 1n;
    } else {
      state.values.set(key, count + 1);
    }
    return;
  }
  if (state.aggFunc === "sum") {
    if (state.resultKind === "bigint") {
      if (typeof value === "bigint") {
        state.bigintTotal += value;
      }
      return;
    }
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.decimalTotal = sumBigDecimal(state.decimalTotal, decimal);
    }
    return;
  }
  if (state.aggFunc === "avg") {
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.count += 1n;
      state.total = sumBigDecimal(state.total, decimal);
    }
    return;
  }
  if ("values" in state) {
    const values = state.values;
    const key = state.inputSemantics.canonicalKey(input);
    const entry = minMaxAggregateValueOr(values, key, input, 0);
    if (entry.count === 0) {
      entry.count = 1;
      values.set(key, entry);
    } else {
      entry.count += 1;
    }
  }
  if (state.selection._tag === "Empty") {
    state.selection = {
      _tag: "Selected",
      input,
    };
    return;
  }
  const comparison = state.inputSemantics.compare(input, state.selection.input);
  if (comparison === 0) {
    return;
  }
  if ((state.aggFunc === "min" && comparison < 0) || (state.aggFunc === "max" && comparison > 0)) {
    state.selection = {
      _tag: "Selected",
      input,
    };
  }
};

const recomputeMinMaxAggregateState = (state: RetainedMinMaxAggregateState): void => {
  let nextSelection: MinMaxAggregateSelection = { _tag: "Empty" };
  for (const entry of state.values.values()) {
    if (nextSelection._tag === "Empty") {
      nextSelection = {
        _tag: "Selected",
        input: entry.input,
      };
      continue;
    }
    const comparison = state.inputSemantics.compare(entry.input, nextSelection.input);
    const isBetterValue = state.aggFunc === "min" ? comparison < 0 : comparison > 0;
    if (isBetterValue) {
      nextSelection = {
        _tag: "Selected",
        input: entry.input,
      };
    }
  }
  state.selection = nextSelection;
};

const removeAggregateState = (
  state: ReversibleAggregateState,
  row: RowObject,
): RetainedMinMaxAggregateState | undefined => {
  if (state.aggFunc === "count") {
    state.count -= 1n;
    return undefined;
  }
  const input = state.inputSemantics.read(row);
  const value = input._tag === "Missing" ? undefined : input.value;
  if (state.aggFunc === "countDistinct") {
    const key = state.inputSemantics.canonicalKey(input);
    const count = mapValueOr(state.values, key, 1);
    if (count === 1) {
      state.values.delete(key);
      state.count -= 1n;
    } else {
      state.values.set(key, count - 1);
    }
    return undefined;
  }
  if (state.aggFunc === "sum") {
    if (state.resultKind === "bigint") {
      if (typeof value === "bigint") {
        state.bigintTotal -= value;
      }
      return undefined;
    }
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.decimalTotal = subtractBigDecimal(state.decimalTotal, decimal);
    }
    return undefined;
  }
  if (state.aggFunc === "avg") {
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.count -= 1n;
      state.total = subtractBigDecimal(state.total, decimal);
    }
    return undefined;
  }
  const key = state.inputSemantics.canonicalKey(input);
  const values = state.values;
  const entry = minMaxAggregateValueOr(values, key, input, 1);
  if (entry.count > 1) {
    entry.count -= 1;
    return undefined;
  }
  values.delete(key);
  if (
    state.selection._tag === "Selected" &&
    state.inputSemantics.canonicalKey(state.selection.input) === key
  ) {
    return state;
  }
  return undefined;
};

export const recomputeRetainedMinMaxAggregateState = (
  state: RetainedMinMaxAggregateState,
): void => {
  recomputeMinMaxAggregateState(state);
};

const aggregateStateResultValue = (state: AggregateState): unknown => {
  if (state.aggFunc === "count") {
    return state.count;
  }
  if (state.aggFunc === "countDistinct") {
    return state.count;
  }
  if (state.aggFunc === "sum") {
    return state.resultKind === "bigint" ? state.bigintTotal : state.decimalTotal;
  }
  if (state.aggFunc === "avg") {
    return state.count === 0n ? fromBigInt(0n) : divideUnsafe(state.total, fromBigInt(state.count));
  }
  return state.selection._tag === "Selected" && state.selection.input._tag === "Present"
    ? state.selection.input.value
    : undefined;
};

export const groupAggregateStateCompareValue = (group: GroupState, stateIndex: number): unknown =>
  aggregateStateResultValue(Arr.getUnsafe(group.aggregates, stateIndex));

export const updateGroupAggregateState = (state: AggregateState, row: RowObject): void => {
  updateAggregateState(state, row);
};

export const removeGroupAggregateState = (
  state: ReversibleAggregateState,
  row: RowObject,
): RetainedMinMaxAggregateState | undefined => removeAggregateState(state, row);

const projectGroupFields = (
  groupBy: ReadonlyArray<string>,
  row: RowObject,
): Record<string, unknown> => {
  const projected: Record<string, unknown> = {};
  for (const field of groupBy) {
    if (!Object.prototype.propertyIsEnumerable.call(row, field)) {
      continue;
    }
    Object.defineProperty(projected, field, {
      configurable: true,
      enumerable: true,
      value: trustedFieldValue(row, field),
      writable: true,
    });
  }
  return projected;
};

export const newGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: ReadonlyArray<GroupedAggregatePlan>,
  row: RowObject,
): GroupState => {
  const resultRow = projectGroupFields(groupBy, row);
  const aggregateStates = aggregates.map(emptyAggregateState);
  return {
    key,
    row: resultRow,
    aggregates: aggregateStates,
  };
};

export const newIncrementalGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: ReadonlyArray<GroupedAggregatePlan>,
  row: RowObject,
): MaterializedIncrementalGroupState => {
  const resultRow = projectGroupFields(groupBy, row);
  const aggregateStates = aggregates.map(emptyReversibleAggregateState);
  return {
    key,
    row: resultRow,
    aggregates: aggregateStates,
    members: new Map(),
  };
};

export const finalizeGroup = (group: GroupState) => {
  const row: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(group.row)) {
    Object.defineProperty(row, field, {
      configurable: true,
      enumerable: true,
      value,
      writable: true,
    });
  }
  for (const state of group.aggregates) {
    Object.defineProperty(row, state.alias, {
      configurable: true,
      enumerable: true,
      value: aggregateStateResultValue(state),
      writable: true,
    });
  }
  return {
    key: group.key,
    row,
  };
};
