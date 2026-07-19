import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import { normalizeFilterText } from "./filter-expression";
import type { RawOrderedWindowIndexState } from "./topic-raw-ordered-window-index";
import {
  existingRawWindowOrderedSlotIndex,
  rawWindowOrderedSpans,
} from "./topic-raw-ordered-window-index";
import {
  orderedRawWindowSpanSlotCount,
  orderedRangeBoundsForField,
  rangeBoundsAreEmpty,
} from "./topic-ordered-window";
import { scalarEqualityKey } from "./row-values";
import { columnScalarEqualityKey, type TopicColumnValues } from "./topic-column-vector";

type RangePredicateFilter = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
};

type ScalarPredicateFieldIndex = {
  readonly buckets: Map<string, Set<number>>;
  readonly indexedKeys: Set<string>;
  readonly normalizedStringBuckets: Map<string, NormalizedStringBucket>;
  readonly normalizedStringModes: Map<string, NormalizedStringMode>;
  readonly orderedBucketSlots: Map<string, ReadonlyArray<number>>;
};

type NormalizedStringBucket = {
  readonly accentSensitive: boolean;
  readonly caseSensitive: boolean;
  readonly normalizedValue: string;
};

type NormalizedStringMode = {
  readonly accentSensitive: boolean;
  readonly caseSensitive: boolean;
  count: number;
};

export const maxRetainedScalarPredicateBucketSlots = 100_000;

export type ScalarPredicateIndexes = Map<string, ScalarPredicateFieldIndex>;

export type PredicateCandidateSlotIndexState = RawOrderedWindowIndexState & {
  readonly scalarPredicateIndexes: ScalarPredicateIndexes;
};

export type PredicateCandidateSlots = {
  readonly coveredFilters: ReadonlySet<TopicRawPredicateFilterPlan>;
  readonly slots: ReadonlyArray<number>;
};

type PredicateCandidateSelectionOptions = {
  readonly allowScalarIndexBuild: boolean;
  readonly exactRangeCandidates: boolean;
  readonly excludedField?: string;
  readonly maxSlotCount?: number;
};

export const createScalarPredicateIndexes = (): ScalarPredicateIndexes => new Map();

export const addSlotToScalarPredicateIndexes = (
  indexes: ScalarPredicateIndexes,
  columns: ReadonlyMap<string, TopicColumnValues>,
  slot: number,
): void => {
  for (const [field, index] of indexes) {
    addSlotToScalarPredicateIndex(index, columns.get(field), slot);
    pruneEmptyScalarPredicateIndex(indexes, field, index);
  }
};

export const removeSlotFromScalarPredicateIndexes = (
  indexes: ScalarPredicateIndexes,
  columns: ReadonlyMap<string, TopicColumnValues>,
  slot: number,
): void => {
  for (const [field, index] of indexes) {
    removeSlotFromScalarPredicateIndex(index, columns.get(field), slot);
    pruneEmptyScalarPredicateIndex(indexes, field, index);
  }
};

export const selectedPredicateCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  options: PredicateCandidateSelectionOptions,
): PredicateCandidateSlots | undefined => {
  let selected: PredicateCandidateSlots | undefined;
  const maxSlotCount = options.maxSlotCount ?? state.slots.length;
  const plannedRangeFields = new Set<string>();
  const rangeFiltersByField = rangePredicateFiltersByField(filters, options.exactRangeCandidates);
  for (const filter of filters) {
    if (filter.field === options.excludedField) {
      continue;
    }
    if (options.exactRangeCandidates && isRangePredicateFilter(filter)) {
      if (plannedRangeFields.has(filter.field)) {
        continue;
      }
      plannedRangeFields.add(filter.field);
    }
    const candidateMaxSlotCount =
      selected === undefined ? maxSlotCount : Math.min(maxSlotCount, selected.slots.length);
    const candidate = predicateCandidateSlots(state, filter, rangeFiltersByField, {
      ...options,
      maxSlotCount: candidateMaxSlotCount,
    });
    if (candidate === undefined) {
      continue;
    }
    if (candidate.slots.length >= maxSlotCount && selected === undefined) {
      continue;
    }
    if (selected === undefined) {
      selected = candidate;
    } else {
      selected = intersectPredicateCandidateSlots(selected, candidate);
    }
    if (selected.slots.length === 0) {
      return selected;
    }
  }
  if (selected === undefined || selected.slots.length >= state.slots.length) {
    return undefined;
  }
  return selected;
};

const intersectPredicateCandidateSlots = (
  left: PredicateCandidateSlots,
  right: PredicateCandidateSlots,
): PredicateCandidateSlots => {
  const slots: Array<number> = [];
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.slots.length && rightIndex < right.slots.length) {
    const leftSlot = left.slots[leftIndex]!;
    const rightSlot = right.slots[rightIndex]!;
    if (leftSlot === rightSlot) {
      slots.push(leftSlot);
      leftIndex += 1;
      rightIndex += 1;
    } else if (leftSlot < rightSlot) {
      leftIndex += 1;
    } else {
      rightIndex += 1;
    }
  }
  const coveredFilters = new Set(left.coveredFilters);
  for (const filter of right.coveredFilters) {
    coveredFilters.add(filter);
  }
  return { coveredFilters, slots };
};

const rangePredicateFiltersByField = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  exactRangeCandidates: boolean,
): ReadonlyMap<string, ReadonlyArray<RangePredicateFilter>> => {
  const byField = new Map<string, Array<RangePredicateFilter>>();
  if (!exactRangeCandidates) {
    return byField;
  }
  for (const filter of filters) {
    if (!isRangePredicateFilter(filter)) {
      continue;
    }
    const existing = byField.get(filter.field);
    if (existing === undefined) {
      byField.set(filter.field, [filter]);
    } else {
      existing.push(filter);
    }
  }
  return byField;
};

const predicateCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  filter: TopicRawPredicateFilterPlan,
  rangeFiltersByField: ReadonlyMap<string, ReadonlyArray<RangePredicateFilter>>,
  options: PredicateCandidateSelectionOptions,
): PredicateCandidateSlots | undefined => {
  if (filter.operator === "eq") {
    const key = scalarEqualityKey(filter.value);
    if (key === undefined) {
      return undefined;
    }
    const candidate = scalarEqualityCandidateSlots(
      state,
      filter.field,
      [key],
      options.allowScalarIndexBuild,
      options.maxSlotCount,
    );
    return candidate === undefined
      ? undefined
      : {
          ...candidate,
          coveredFilters: new Set([filter]),
        };
  }
  if (filter.operator === "in") {
    const valueKeys =
      filter.valueKeys === undefined ? scalarEqualityKeys(filter.values) : [...filter.valueKeys];
    if (valueKeys === undefined) {
      return undefined;
    }
    const candidate = scalarEqualityCandidateSlots(
      state,
      filter.field,
      valueKeys,
      options.allowScalarIndexBuild,
      options.maxSlotCount,
    );
    return candidate === undefined
      ? undefined
      : { ...candidate, coveredFilters: new Set([filter]) };
  }
  if (filter.operator === "textEq" || filter.operator === "textIn") {
    const candidate = normalizedStringCandidateSlots(
      state,
      filter.field,
      filter.operator === "textEq" ? [filter.value] : filter.values,
      filter.caseSensitive,
      filter.accentSensitive,
      options.allowScalarIndexBuild,
      options.maxSlotCount,
    );
    return candidate === undefined
      ? undefined
      : { ...candidate, coveredFilters: new Set([filter]) };
  }
  if (options.exactRangeCandidates && isRangePredicateFilter(filter)) {
    const rangeFilters = rangeFiltersByField.get(filter.field)!;
    const candidate = rangeCandidateSlots(state, filter.field, rangeFilters, options.maxSlotCount);
    return candidate === undefined
      ? undefined
      : {
          ...candidate,
          coveredFilters: new Set(rangeFilters),
        };
  }
  return undefined;
};

const scalarEqualityCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  valueKeys: ReadonlyArray<string>,
  allowIndexBuild: boolean,
  maxSlotCount: number | undefined,
): Omit<PredicateCandidateSlots, "coveredFilters"> | undefined => {
  if (!state.columns.has(field)) {
    return undefined;
  }
  const index = scalarPredicateIndexForField(state, field, allowIndexBuild);
  if (index === undefined) {
    return undefined;
  }
  const column = state.columns.get(field)!;
  const slots = unionScalarPredicateSlots(index, column, valueKeys, allowIndexBuild, maxSlotCount);
  pruneEmptyScalarPredicateIndex(state.scalarPredicateIndexes, field, index);
  if (slots === undefined) {
    return undefined;
  }
  return slots;
};

const normalizedStringCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  values: ReadonlyArray<string>,
  caseSensitive: boolean,
  accentSensitive: boolean,
  allowIndexBuild: boolean,
  maxSlotCount: number | undefined,
): Omit<PredicateCandidateSlots, "coveredFilters"> | undefined => {
  const column = state.columns.get(field);
  if (column?.kind !== "string") {
    return undefined;
  }
  const index = scalarPredicateIndexForField(state, field, allowIndexBuild);
  if (index === undefined) {
    return undefined;
  }
  const descriptors = new Map<string, NormalizedStringBucket>();
  for (const normalizedValue of values) {
    const key = normalizedStringBucketKey(normalizedValue, caseSensitive, accentSensitive);
    descriptors.set(key, { normalizedValue, caseSensitive, accentSensitive });
  }
  const slots = unionScalarPredicateSlots(
    index,
    column,
    [...descriptors.keys()],
    allowIndexBuild,
    maxSlotCount,
    (_valuesColumn, slot) => {
      const value = column.stringAt(slot);
      return value === undefined
        ? undefined
        : normalizedStringBucketKey(
            normalizeFilterText(value, caseSensitive, accentSensitive),
            caseSensitive,
            accentSensitive,
          );
    },
  );
  for (const [key, descriptor] of descriptors) {
    if (index.indexedKeys.has(key) && !index.normalizedStringBuckets.has(key)) {
      index.normalizedStringBuckets.set(key, descriptor);
      retainNormalizedStringMode(index, descriptor);
    }
  }
  pruneEmptyScalarPredicateIndex(state.scalarPredicateIndexes, field, index);
  return slots;
};

const normalizedStringBucketKey = (
  normalizedValue: string,
  caseSensitive: boolean,
  accentSensitive: boolean,
): string =>
  `normalized-string:${caseSensitive ? 1 : 0}:${accentSensitive ? 1 : 0}:${normalizedValue.length}:${normalizedValue}`;

const normalizedStringModeKey = (caseSensitive: boolean, accentSensitive: boolean): string =>
  `${caseSensitive ? 1 : 0}:${accentSensitive ? 1 : 0}`;

const retainNormalizedStringMode = (
  index: ScalarPredicateFieldIndex,
  descriptor: NormalizedStringBucket,
): void => {
  const key = normalizedStringModeKey(descriptor.caseSensitive, descriptor.accentSensitive);
  const existing = index.normalizedStringModes.get(key);
  if (existing !== undefined) {
    existing.count += 1;
    return;
  }
  index.normalizedStringModes.set(key, {
    accentSensitive: descriptor.accentSensitive,
    caseSensitive: descriptor.caseSensitive,
    count: 1,
  });
};

const releaseNormalizedStringMode = (
  index: ScalarPredicateFieldIndex,
  descriptor: NormalizedStringBucket,
): void => {
  const key = normalizedStringModeKey(descriptor.caseSensitive, descriptor.accentSensitive);
  const existing = index.normalizedStringModes.get(key);
  if (existing === undefined || existing.count === 1) {
    index.normalizedStringModes.delete(key);
    return;
  }
  existing.count -= 1;
};

const scalarEqualityKeys = (values: ReadonlyArray<unknown>): ReadonlyArray<string> | undefined => {
  const keys: Array<string> = [];
  const seen = new Set<string>();
  for (const value of values) {
    const key = scalarEqualityKey(value);
    if (key === undefined) {
      return undefined;
    }
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    keys.push(key);
  }
  return keys;
};

const scalarPredicateIndexForField = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  allowIndexBuild: boolean,
): ScalarPredicateFieldIndex | undefined => {
  const existing = state.scalarPredicateIndexes.get(field);
  if (existing !== undefined) {
    return existing;
  }
  if (!allowIndexBuild) {
    return undefined;
  }
  const index: ScalarPredicateFieldIndex = {
    buckets: new Map(),
    indexedKeys: new Set(),
    normalizedStringBuckets: new Map(),
    normalizedStringModes: new Map(),
    orderedBucketSlots: new Map(),
  };
  state.scalarPredicateIndexes.set(field, index);
  return index;
};

const unionScalarPredicateSlots = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues,
  valueKeys: ReadonlyArray<string>,
  allowBucketBuild: boolean,
  maxSlotCount: number | undefined,
  columnValueKey: (
    column: TopicColumnValues,
    slot: number,
  ) => string | undefined = columnScalarEqualityKey,
): Omit<PredicateCandidateSlots, "coveredFilters"> | undefined => {
  if (valueKeys.length === 0) {
    return { slots: [] };
  }
  if (valueKeys.length === 1) {
    const bucket = ensureScalarPredicateBucket(
      index,
      column,
      valueKeys[0]!,
      allowBucketBuild,
      maxSlotCount,
      columnValueKey,
    );
    if (bucket === undefined) {
      return undefined;
    }
    if (maxSlotCount !== undefined && bucket.size > maxSlotCount) {
      return undefined;
    }
    return { slots: orderedScalarPredicateBucketSlots(index, valueKeys[0]!, bucket) };
  }
  const slots = new Set<number>();
  const missingKeys: Array<string> = [];
  for (const key of valueKeys) {
    if (!index.indexedKeys.has(key)) {
      missingKeys.push(key);
      continue;
    }
    const bucket = index.buckets.get(key)!;
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, key);
      return undefined;
    }
    for (const slot of bucket) {
      slots.add(slot);
      if (maxSlotCount !== undefined && slots.size > maxSlotCount) {
        return undefined;
      }
    }
  }
  if (missingKeys.length === 0) {
    return { slots: stableCandidateSlotOrder([...slots]) };
  }
  if (!allowBucketBuild) {
    return undefined;
  }
  const missingBuckets = new Map<string, Set<number>>();
  for (const key of missingKeys) {
    missingBuckets.set(key, new Set());
  }
  for (let slot = 0; slot < column.length; slot += 1) {
    const key = columnValueKey(column, slot);
    if (key === undefined) {
      continue;
    }
    const bucket = missingBuckets.get(key);
    if (bucket === undefined) {
      continue;
    }
    bucket.add(slot);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      return undefined;
    }
    slots.add(slot);
    if (maxSlotCount !== undefined && slots.size > maxSlotCount) {
      return undefined;
    }
  }
  for (const [key, bucket] of missingBuckets) {
    if (bucket.size === 0) {
      continue;
    }
    index.indexedKeys.add(key);
    index.buckets.set(key, bucket);
  }
  return { slots: stableCandidateSlotOrder([...slots]) };
};

const orderedScalarPredicateBucketSlots = (
  index: ScalarPredicateFieldIndex,
  key: string,
  bucket: Set<number>,
): ReadonlyArray<number> => {
  const existing = index.orderedBucketSlots.get(key);
  if (existing !== undefined) {
    return existing;
  }
  const ordered = stableCandidateSlotOrder([...bucket]);
  index.orderedBucketSlots.set(key, ordered);
  return ordered;
};

const ensureScalarPredicateBucket = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues,
  valueKey: string,
  allowBucketBuild: boolean,
  maxSlotCount: number | undefined,
  columnValueKey: (column: TopicColumnValues, slot: number) => string | undefined,
): Set<number> | undefined => {
  if (index.indexedKeys.has(valueKey)) {
    const bucket = index.buckets.get(valueKey)!;
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, valueKey);
      return undefined;
    }
    return bucket;
  }
  if (!allowBucketBuild) {
    return undefined;
  }

  const bucket = new Set<number>();
  for (let slot = 0; slot < column.length; slot += 1) {
    if (columnValueKey(column, slot) !== valueKey) {
      continue;
    }
    bucket.add(slot);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      return undefined;
    }
    if (maxSlotCount !== undefined && bucket.size > maxSlotCount) {
      return undefined;
    }
  }
  if (bucket.size > 0) {
    index.indexedKeys.add(valueKey);
    index.buckets.set(valueKey, bucket);
  }
  return bucket;
};

const rangeCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  maxSlotCount: number | undefined,
): Omit<PredicateCandidateSlots, "coveredFilters"> | undefined => {
  if (!state.columns.has(field)) {
    return undefined;
  }
  const bounds = orderedRangeBoundsForField(filters, field, state.rawQueryMetadata);
  if (bounds === undefined) {
    return undefined;
  }
  if (rangeBoundsAreEmpty(bounds)) {
    return { slots: [] };
  }
  const index = existingRawWindowOrderedSlotIndex(state, [{ field, direction: "asc" }]);
  if (index === undefined) {
    return undefined;
  }
  const spans = rawWindowOrderedSpans(state, index, bounds, undefined);
  const spanSlotCount = orderedRawWindowSpanSlotCount(spans);
  if (spanSlotCount >= state.slots.length) {
    return undefined;
  }
  if (maxSlotCount !== undefined && spanSlotCount > maxSlotCount) {
    return undefined;
  }
  const slots = stableCandidateSlotOrder(slotsFromOrderedSpans(index.slots, spans));
  return { slots };
};

const stableCandidateSlotOrder = (slots: ReadonlyArray<number>): ReadonlyArray<number> =>
  slots.toSorted((left, right) => left - right);

const slotsFromOrderedSpans = (
  orderedSlots: ReadonlyArray<number>,
  spans: ReturnType<typeof rawWindowOrderedSpans>,
): ReadonlyArray<number> => {
  const slots: Array<number> = [];
  for (const span of spans) {
    for (let index = span.startIndex; index < span.endIndex; index += 1) {
      slots.push(orderedSlots[index]!);
    }
  }
  return slots;
};

const addSlotToScalarPredicateIndex = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues | undefined,
  slot: number,
): void => {
  if (column === undefined) {
    return;
  }
  const key = columnScalarEqualityKey(column, slot);
  if (key !== undefined && index.indexedKeys.has(key)) {
    const bucket = index.buckets.get(key)!;
    bucket.add(slot);
    index.orderedBucketSlots.delete(key);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, key);
    }
  }
  updateNormalizedStringPredicateBuckets(index, column, slot, "add");
};

const removeSlotFromScalarPredicateIndex = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues | undefined,
  slot: number,
): void => {
  if (column === undefined) {
    return;
  }
  const key = columnScalarEqualityKey(column, slot);
  if (key !== undefined && index.indexedKeys.has(key)) {
    const bucket = index.buckets.get(key)!;
    bucket.delete(slot);
    index.orderedBucketSlots.delete(key);
    if (bucket.size === 0) {
      evictScalarPredicateBucket(index, key);
    }
  }
  updateNormalizedStringPredicateBuckets(index, column, slot, "remove");
};

const updateNormalizedStringPredicateBuckets = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues,
  slot: number,
  operation: "add" | "remove",
): void => {
  if (column.kind !== "string" || index.normalizedStringBuckets.size === 0) {
    return;
  }
  const value = column.stringAt(slot);
  if (value === undefined) {
    return;
  }
  for (const mode of index.normalizedStringModes.values()) {
    const normalized = normalizeFilterText(value, mode.caseSensitive, mode.accentSensitive);
    const key = normalizedStringBucketKey(normalized, mode.caseSensitive, mode.accentSensitive);
    if (!index.normalizedStringBuckets.has(key)) {
      continue;
    }
    const bucket = index.buckets.get(key)!;
    if (operation === "remove") {
      bucket.delete(slot);
      index.orderedBucketSlots.delete(key);
      if (bucket.size === 0) {
        evictScalarPredicateBucket(index, key);
      }
      continue;
    }
    bucket.add(slot);
    index.orderedBucketSlots.delete(key);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, key);
    }
  }
};

const scalarPredicateBucketIsOverRetainedBudget = (bucket: Set<number>): boolean =>
  bucket.size > maxRetainedScalarPredicateBucketSlots;

const evictScalarPredicateBucket = (index: ScalarPredicateFieldIndex, key: string): void => {
  const normalizedDescriptor = index.normalizedStringBuckets.get(key);
  if (normalizedDescriptor !== undefined) {
    releaseNormalizedStringMode(index, normalizedDescriptor);
  }
  index.indexedKeys.delete(key);
  index.buckets.delete(key);
  index.normalizedStringBuckets.delete(key);
  index.orderedBucketSlots.delete(key);
};

const pruneEmptyScalarPredicateIndex = (
  indexes: ScalarPredicateIndexes,
  field: string,
  index: ScalarPredicateFieldIndex,
): void => {
  if (index.indexedKeys.size === 0) {
    indexes.delete(field);
  }
};

const isRangePredicateFilter = (
  filter: TopicRawPredicateFilterPlan,
): filter is RangePredicateFilter =>
  filter.operator === "gt" ||
  filter.operator === "gte" ||
  filter.operator === "lt" ||
  filter.operator === "lte";
