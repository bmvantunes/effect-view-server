import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawPredicateFilterPlan, TopicRawPredicatePlan } from "./raw-predicate-plan";
import type {
  TopicRawWindowEntry,
  TopicRawWindowScanPlan,
  TopicRawWindowScanResult,
} from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import type { TopicColumnValues } from "./topic-column-vector";
import {
  maxRetainedScalarPredicateBucketSlots,
  selectedPredicateCandidateSlots,
  type PredicateCandidateSlotIndexState,
  type PredicateCandidateSlots,
} from "./topic-predicate-candidate-index";
import {
  insertSlotIntoRawWindowIndexes,
  rawWindowOrderedWindow,
  rawWindowSlotComparator,
} from "./topic-raw-ordered-window-index";
import {
  orderedRawWindowSlotCount,
  orderedSlotIndexInsertionPoint,
  type OrderedRawWindow,
  type OrderedSlotIndex,
} from "./topic-ordered-window";
import { rawPredicateSlotFilterMatcher, type SlotFilterMatcher } from "./topic-slot-predicate";

type RowObject = object;

export type TopicRawWindowScanState = {
  readonly columns: ReadonlyMap<string, TopicColumnValues>;
  readonly keyToSlot?: ReadonlyMap<string, number>;
  readonly orderedSlotIndexes: Map<string, OrderedSlotIndex>;
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly rawPredicateSlotMatchers?: WeakMap<TopicRawPredicatePlan, SlotFilterMatcher>;
  readonly scalarPredicateIndexes: PredicateCandidateSlotIndexState["scalarPredicateIndexes"];
  readonly slots: ReadonlyArray<TopicRowEntry<object>>;
};

const maxSortedBoundedRawWindowEnd = 1_024;
const maxHeapBoundedRawWindowEnd = 100_000;
const maxMaterializedPredicateCandidateSlots = maxRetainedScalarPredicateBucketSlots;
const materializedPredicateCandidateSlotBudget = maxMaterializedPredicateCandidateSlots + 1;

type BoundedRawWindowStrategy = {
  readonly kind: "heap" | "quickselect" | "sorted";
  readonly windowEnd: number;
};

export const scanTopicRawWindow = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
): TopicRawWindowScanResult<object> => {
  const storageKeyCandidates = ownedStorageKeyCandidateSlots(state, plan.candidateStorageKeys);
  if (storageKeyCandidates !== undefined) {
    const matchesSlot = rawPredicateMatchesSlot(state, plan);
    if (plan.limit === 0) {
      return countRawWindowCandidateSlots(state, matchesSlot, storageKeyCandidates);
    }
    const compareSlots =
      rawWindowSlotComparator(state, plan) ??
      ((left, right) => plan.compare(state.slots[left]!, state.slots[right]!));
    return scanRawWindowCandidateSlots(
      state,
      plan,
      compareSlots,
      matchesSlot,
      storageKeyCandidates,
    );
  }
  if (plan.limit === 0) {
    return countRawWindowSlots(state, plan);
  }

  const orderedWindow = rawWindowOrderedWindow(state, plan);
  if (orderedWindow !== undefined) {
    const orderedSlotCount = orderedRawWindowSlotCount(orderedWindow);
    const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
      allowScalarIndexBuild: true,
      exactRangeCandidates: plan.predicate.callbackSkippable === true,
      excludedField: orderedWindow.candidateExcludedField,
      maxSlotCount: Math.min(orderedSlotCount, materializedPredicateCandidateSlotBudget),
    });
    if (candidateSlots !== undefined && candidateSlots.slots.length < orderedSlotCount) {
      if (
        plan.predicate.callbackSkippable === true &&
        candidateSlots.coveredFilters.size === plan.predicate.filters.length
      ) {
        if (exactCandidatesShouldUseOrderedScan(candidateSlots.slots.length, orderedSlotCount)) {
          return scanExactCandidateSlotsInOrderedWindow(state, plan, orderedWindow, candidateSlots);
        }
        const compareSlots = rawWindowSlotComparator(state, plan)!;
        return scanRawWindowCandidateSlots(
          state,
          plan,
          compareSlots,
          rawPredicateMatchesSlot(state, plan, candidateSlots.coveredFilters),
          candidateSlots,
        );
      }
      const compareSlots = rawWindowSlotComparator(state, plan)!;
      return scanRawWindowCandidateSlots(
        state,
        plan,
        compareSlots,
        rawPredicateMatchesSlot(state, plan, candidateSlots.coveredFilters),
        candidateSlots,
      );
    }
    return scanRawWindowOrderedSlots(
      state,
      plan,
      rawPredicateMatchesSlot(state, plan),
      orderedWindow,
    );
  }

  const compareSlots =
    rawWindowSlotComparator(state, plan) ??
    ((left, right) => plan.compare(state.slots[left]!, state.slots[right]!));
  const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
    allowScalarIndexBuild: true,
    exactRangeCandidates: plan.predicate.callbackSkippable === true,
    maxSlotCount: Math.min(state.slots.length, materializedPredicateCandidateSlotBudget),
  });
  if (candidateSlots !== undefined) {
    return scanRawWindowCandidateSlots(
      state,
      plan,
      compareSlots,
      rawPredicateMatchesSlot(state, plan, candidateSlots.coveredFilters),
      candidateSlots,
    );
  }
  return scanRawWindowSlots(state, plan, compareSlots, rawPredicateMatchesSlot(state, plan));
};

const noCoveredPredicateFilters: ReadonlySet<TopicRawPredicateFilterPlan> = new Set();

const ownedStorageKeyCandidateSlots = (
  state: TopicRawWindowScanState,
  candidateStorageKeys: (() => Iterable<string>) | undefined,
): PredicateCandidateSlots | undefined => {
  if (candidateStorageKeys === undefined || state.keyToSlot === undefined) {
    return undefined;
  }
  const slots: Array<number> = [];
  for (const key of candidateStorageKeys()) {
    const slot = state.keyToSlot.get(key);
    if (slot !== undefined) {
      slots.push(slot);
    }
  }
  return {
    coveredFilters: noCoveredPredicateFilters,
    slots,
  };
};

export { insertSlotIntoRawWindowIndexes };
export {
  insertSlotIntoRawWindowIndex,
  removeSlotFromRawWindowIndex,
  removeSlotFromRawWindowIndexes,
} from "./topic-raw-ordered-window-index";

const countRawWindowSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
): TopicRawWindowScanResult<object> => {
  const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
    allowScalarIndexBuild: true,
    exactRangeCandidates: plan.predicate.callbackSkippable === true,
    maxSlotCount: Math.min(state.slots.length, materializedPredicateCandidateSlotBudget),
  });
  if (candidateSlots !== undefined) {
    return countRawWindowCandidateSlots(
      state,
      rawPredicateMatchesSlot(state, plan, candidateSlots.coveredFilters),
      candidateSlots,
    );
  }

  const matchesSlot = rawPredicateMatchesSlot(state, plan);
  let totalRows = 0;
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (matchesSlot(slot)) {
      totalRows += 1;
    }
  }
  return rawWindowScanResult(state, [], totalRows);
};

const countRawWindowCandidateSlots = (
  state: TopicRawWindowScanState,
  matchesSlot: (slot: number) => boolean,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  for (const slot of candidateSlots.slots) {
    if (matchesSlot(slot)) {
      totalRows += 1;
    }
  }
  return rawWindowScanResult(state, [], totalRows);
};

const scanRawWindowOrderedSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  matchesSlot: (slot: number) => boolean,
  orderedWindow: OrderedRawWindow,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  const windowEnd = plan.offset + orderedWindow.limit;
  for (const span of orderedWindow.spans) {
    for (let slotIndex = span.startIndex; slotIndex < span.endIndex; slotIndex += 1) {
      const slot = orderedWindow.slots[slotIndex]!;
      if (!matchesSlot(slot)) {
        continue;
      }
      const matchIndex = totalRows;
      totalRows += 1;
      if (matchIndex >= plan.offset && matchIndex < windowEnd) {
        windowSlots.push(slot);
      }
    }
  }
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const candidateSlotsContain = (slots: ReadonlyArray<number>, candidate: number): boolean => {
  let low = 0;
  let high = slots.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const current = slots[middle]!;
    if (current === candidate) {
      return true;
    }
    if (current < candidate) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return false;
};

const exactCandidatesShouldUseOrderedScan = (
  candidateCount: number,
  orderedSlotCount: number,
): boolean =>
  candidateCount > 0 &&
  candidateCount * Math.max(1, Math.ceil(Math.log2(candidateCount + 1))) >= orderedSlotCount;

const scanExactCandidateSlotsInOrderedWindow = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  orderedWindow: OrderedRawWindow,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  const windowSlots: Array<number> = [];
  const windowEnd = plan.offset + orderedWindow.limit;
  let matchIndex = 0;
  for (const span of orderedWindow.spans) {
    for (let index = span.startIndex; index < span.endIndex; index += 1) {
      const slot = orderedWindow.slots[index]!;
      if (!candidateSlotsContain(candidateSlots.slots, slot)) {
        continue;
      }
      if (matchIndex >= plan.offset) {
        windowSlots.push(slot);
      }
      matchIndex += 1;
      if (matchIndex >= windowEnd) {
        return rawWindowScanResult(state, windowSlots, candidateSlots.slots.length);
      }
    }
  }
  return rawWindowScanResult(state, windowSlots, candidateSlots.slots.length);
};

const scanRawWindowCandidateSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  const boundedWindow = boundedRawWindowStrategy(plan, candidateSlots.slots.length, true);
  if (boundedWindow?.kind === "quickselect") {
    return scanRawWindowBoundedQuickselectSlotCandidates(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindow.windowEnd,
      candidateSlots,
    );
  }
  if (boundedWindow?.kind === "sorted") {
    return scanRawWindowBoundedSortedSlotCandidates(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindow.windowEnd,
      candidateSlots,
    );
  }
  if (boundedWindow?.kind === "heap") {
    return scanRawWindowBoundedHeapSlotCandidates(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindow.windowEnd,
      candidateSlots,
    );
  }

  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    filteredSlots.push(slot);
  }
  filteredSlots.sort(compareSlots);
  const windowSlots = filteredSlots.slice(
    plan.offset,
    plan.limit === undefined ? undefined : plan.offset + plan.limit,
  );
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const scanRawWindowSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
): TopicRawWindowScanResult<object> => {
  const boundedWindow = boundedRawWindowStrategy(plan, state.slots.length);
  if (boundedWindow?.kind === "sorted") {
    return scanRawWindowBoundedSortedSlots(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindow.windowEnd,
    );
  }
  if (boundedWindow?.kind === "heap") {
    return scanRawWindowBoundedHeapSlots(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindow.windowEnd,
    );
  }

  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    filteredSlots.push(slot);
  }
  filteredSlots.sort(compareSlots);
  const windowSlots = filteredSlots.slice(
    plan.offset,
    plan.limit === undefined ? undefined : plan.offset + plan.limit,
  );
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const scanRawWindowBoundedSortedSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    if (windowSlots.length < windowEnd) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      continue;
    }
    const worstSlot = windowSlots[windowSlots.length - 1]!;
    if (compareSlots(slot, worstSlot) < 0) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      windowSlots.pop();
    }
  }
  return rawWindowScanResult(state, windowSlots.slice(plan.offset), totalRows);
};

const scanRawWindowBoundedSortedSlotCandidates = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    if (windowSlots.length < windowEnd) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      continue;
    }
    const worstSlot = windowSlots[windowSlots.length - 1]!;
    if (compareSlots(slot, worstSlot) < 0) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      windowSlots.pop();
    }
  }
  return rawWindowScanResult(state, windowSlots.slice(plan.offset), totalRows);
};

const scanRawWindowBoundedHeapSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const heap: Array<number> = [];
  const compareStableSlots = stableRawWindowSlotComparator(compareSlots);
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    retainBoundedRawWindowSlot(heap, slot, windowEnd, compareStableSlots);
  }
  heap.sort(compareStableSlots);
  return rawWindowScanResult(state, heap.slice(plan.offset), totalRows);
};

const scanRawWindowBoundedHeapSlotCandidates = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const heap: Array<number> = [];
  const compareStableSlots = stableRawWindowSlotComparator(compareSlots);
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    retainBoundedRawWindowSlot(heap, slot, windowEnd, compareStableSlots);
  }
  heap.sort(compareStableSlots);
  return rawWindowScanResult(state, heap.slice(plan.offset), totalRows);
};

const scanRawWindowBoundedQuickselectSlotCandidates = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    filteredSlots.push(slot);
  }
  const compareStableSlots = stableRawWindowSlotComparator(compareSlots);
  if (filteredSlots.length > windowEnd) {
    retainLowestRawWindowSlots(filteredSlots, windowEnd, compareStableSlots);
    filteredSlots.length = windowEnd;
  }
  filteredSlots.sort(compareStableSlots);
  return rawWindowScanResult(state, filteredSlots.slice(plan.offset), totalRows);
};

const retainLowestRawWindowSlots = (
  slots: Array<number>,
  count: number,
  compareSlots: (left: number, right: number) => number,
): void => {
  let left = 0;
  let right = slots.length - 1;
  const target = count - 1;
  while (left < right) {
    const rangeSize = right - left + 1;
    const pivot = partitionRawWindowSlots(slots, left, right, compareSlots);
    if (pivot === target) {
      return;
    }
    if (target < pivot) {
      right = pivot - 1;
    } else {
      left = pivot + 1;
    }
    const retainedSize = right - left + 1;
    if (retainedSize * 8 > rangeSize * 7) {
      slots.sort(compareSlots);
      return;
    }
  }
};

const partitionRawWindowSlots = (
  slots: Array<number>,
  left: number,
  right: number,
  compareSlots: (left: number, right: number) => number,
): number => {
  const pivotIndex = left + Math.floor((right - left) / 2);
  swapRawWindowHeapSlots(slots, pivotIndex, right);
  const pivot = slots[right]!;
  let nextLower = left;
  for (let index = left; index < right; index += 1) {
    if (compareSlots(slots[index]!, pivot) >= 0) {
      continue;
    }
    swapRawWindowHeapSlots(slots, nextLower, index);
    nextLower += 1;
  }
  swapRawWindowHeapSlots(slots, nextLower, right);
  return nextLower;
};

const stableRawWindowSlotComparator =
  (compareSlots: (left: number, right: number) => number) =>
  (left: number, right: number): number => {
    const comparison = compareSlots(left, right);
    return comparison === 0 ? left - right : comparison;
  };

const retainBoundedRawWindowSlot = (
  heap: Array<number>,
  slot: number,
  windowEnd: number,
  compareSlots: (left: number, right: number) => number,
): void => {
  if (heap.length < windowEnd) {
    heap.push(slot);
    siftRawWindowSlotUp(heap, heap.length - 1, compareSlots);
    return;
  }
  if (compareSlots(slot, heap[0]!) >= 0) {
    return;
  }
  heap[0] = slot;
  siftRawWindowSlotDown(heap, 0, compareSlots);
};

const siftRawWindowSlotUp = (
  heap: Array<number>,
  index: number,
  compareSlots: (left: number, right: number) => number,
): void => {
  let current = index;
  while (current > 0) {
    const parent = Math.floor((current - 1) / 2);
    if (compareSlots(heap[parent]!, heap[current]!) >= 0) {
      return;
    }
    swapRawWindowHeapSlots(heap, parent, current);
    current = parent;
  }
};

const siftRawWindowSlotDown = (
  heap: Array<number>,
  index: number,
  compareSlots: (left: number, right: number) => number,
): void => {
  let current = index;
  while (true) {
    const left = current * 2 + 1;
    const right = left + 1;
    let largest = current;
    if (left < heap.length && compareSlots(heap[left]!, heap[largest]!) > 0) {
      largest = left;
    }
    if (right < heap.length && compareSlots(heap[right]!, heap[largest]!) > 0) {
      largest = right;
    }
    if (largest === current) {
      return;
    }
    swapRawWindowHeapSlots(heap, current, largest);
    current = largest;
  }
};

const swapRawWindowHeapSlots = (heap: Array<number>, left: number, right: number): void => {
  const value = heap[left]!;
  heap[left] = heap[right]!;
  heap[right] = value;
};

const rawPredicateMatchesSlot = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  excludedFilters?: ReadonlySet<TopicRawPredicateFilterPlan>,
): ((slot: number) => boolean) => {
  if (plan.predicate.callbackSkippable !== true) {
    const matchesFilters = rawPredicateSlotFilterMatcher(
      plan.predicate.filters,
      state.columns,
      false,
      excludedFilters,
    );
    return (slot) => {
      const entry = state.slots[slot]!;
      return matchesFilters(slot) && plan.matches(entry.row, entry.key);
    };
  }

  let matchesSlot =
    excludedFilters === undefined ? state.rawPredicateSlotMatchers?.get(plan.predicate) : undefined;
  if (matchesSlot === undefined) {
    matchesSlot = rawPredicateSlotFilterMatcher(
      plan.predicate.filters,
      state.columns,
      true,
      excludedFilters,
    );
    if (excludedFilters === undefined) {
      state.rawPredicateSlotMatchers?.set(plan.predicate, matchesSlot);
    }
  }
  return matchesSlot;
};

const rawWindowScanResult = (
  state: TopicRawWindowScanState,
  windowSlots: ReadonlyArray<number>,
  totalRows: number,
): TopicRawWindowScanResult<RowObject> => {
  const window: Array<TopicRawWindowEntry<RowObject>> = windowSlots.map((slot) => ({
    ...state.slots[slot]!,
    slot,
  }));
  return {
    keys: window.map((entry) => entry.key),
    window,
    totalRows,
  };
};

const boundedRawWindowStrategy = (
  plan: TopicRawWindowScanPlan<object>,
  candidateCount: number,
  preferQuickselectForLargeCandidateSet = false,
): BoundedRawWindowStrategy | undefined => {
  if (plan.limit === undefined || plan.limit <= 0) {
    return undefined;
  }
  if (!Number.isSafeInteger(plan.offset) || plan.offset < 0 || !Number.isSafeInteger(plan.limit)) {
    return undefined;
  }
  const windowEnd = plan.offset + plan.limit;
  if (!Number.isSafeInteger(windowEnd)) {
    return undefined;
  }
  if (
    preferQuickselectForLargeCandidateSet &&
    windowEnd <= maxSortedBoundedRawWindowEnd &&
    windowEnd <= maxHeapBoundedRawWindowEnd &&
    windowEnd * 4 <= candidateCount
  ) {
    return {
      kind: "quickselect",
      windowEnd,
    };
  }
  if (windowEnd <= maxSortedBoundedRawWindowEnd) {
    return {
      kind: "sorted",
      windowEnd,
    };
  }
  if (windowEnd <= maxHeapBoundedRawWindowEnd && windowEnd * 4 <= candidateCount) {
    return {
      kind: "heap",
      windowEnd,
    };
  }
  return undefined;
};
