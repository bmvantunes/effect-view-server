import {
  aggregateStateCompareValue,
  finalizeGroup,
  type GroupState,
} from "./grouped-aggregate-state";
import { compareQueryValue } from "./raw-query-compiler";
import type { QueryEvaluation, StoredRowOf } from "./query-result";
import { fieldValue } from "./row-values";

type RowObject = object;

export type RuntimeGroupedOrderBy =
  | {
      readonly field: string;
      readonly direction: "asc" | "desc";
    }
  | {
      readonly aggregate: string;
      readonly direction: "asc" | "desc";
    };

type RuntimeGroupedWindowQuery = {
  readonly orderBy?: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly offset?: number;
  readonly limit?: number;
};

type BoundedGroupEntry = {
  group: GroupState;
  orderValues: Array<unknown>;
};

const maxBoundedGroupedWindowEnd = 1_024;
const emptyBoundedGroupOrderValues: Array<unknown> = [];

const compareGroupedRows = (
  left: StoredRowOf<RowObject>,
  right: StoredRowOf<RowObject>,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): number => {
  for (const order of orderBy) {
    const field = "field" in order ? order.field : order.aggregate;
    const comparison = compareQueryValue(fieldValue(left.row, field), fieldValue(right.row, field));
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

const groupedWindowEnd = (query: RuntimeGroupedWindowQuery): number | undefined => {
  if (query.limit === undefined) {
    return undefined;
  }
  const windowEnd = (query.offset ?? 0) + query.limit;
  if (!Number.isSafeInteger(windowEnd) || windowEnd > maxBoundedGroupedWindowEnd) {
    return undefined;
  }
  return windowEnd;
};

const writeGroupOrderValues = (
  target: Array<unknown>,
  group: GroupState,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): Array<unknown> => {
  target.length = 0;
  for (const order of orderBy) {
    target.push(
      "field" in order
        ? fieldValue(group.row, order.field)
        : aggregateStateCompareValue(group.aggregates[order.aggregate]!),
    );
  }
  return target;
};

const newBoundedGroupEntry = (
  group: GroupState,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): BoundedGroupEntry => {
  if (orderBy.length === 0) {
    return {
      group,
      orderValues: emptyBoundedGroupOrderValues,
    };
  }
  return {
    group,
    orderValues: writeGroupOrderValues([], group, orderBy),
  };
};

const compareBoundedGroupEntries = (
  left: BoundedGroupEntry,
  right: BoundedGroupEntry,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): number => {
  for (let index = 0; index < orderBy.length; index += 1) {
    const order = orderBy[index]!;
    const comparison = compareQueryValue(left.orderValues[index], right.orderValues[index]);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.group.key > right.group.key) - Number(left.group.key < right.group.key);
};

const compareGroupToBoundedGroupEntry = (
  group: GroupState,
  orderValues: ReadonlyArray<unknown>,
  right: BoundedGroupEntry,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): number => {
  for (let index = 0; index < orderBy.length; index += 1) {
    const order = orderBy[index]!;
    const comparison = compareQueryValue(orderValues[index], right.orderValues[index]);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(group.key > right.group.key) - Number(group.key < right.group.key);
};

const boundedGroupEntryIsWorse = (
  left: BoundedGroupEntry,
  right: BoundedGroupEntry,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): boolean => compareBoundedGroupEntries(left, right, orderBy) > 0;

const swapBoundedGroupEntries = (
  groups: Array<BoundedGroupEntry>,
  left: number,
  right: number,
): void => {
  const leftGroup = groups[left]!;
  groups[left] = groups[right]!;
  groups[right] = leftGroup;
};

const siftWorstBoundedGroupEntryUp = (
  groups: Array<BoundedGroupEntry>,
  index: number,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): void => {
  let current = index;
  while (current > 0) {
    const parent = (current - 1) >>> 1;
    if (!boundedGroupEntryIsWorse(groups[current]!, groups[parent]!, orderBy)) {
      return;
    }
    swapBoundedGroupEntries(groups, current, parent);
    current = parent;
  }
};

const siftWorstBoundedGroupEntryDown = (
  groups: Array<BoundedGroupEntry>,
  index: number,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): void => {
  let current = index;
  while (true) {
    const left = current * 2 + 1;
    const right = left + 1;
    let worst = current;
    if (left < groups.length && boundedGroupEntryIsWorse(groups[left]!, groups[worst]!, orderBy)) {
      worst = left;
    }
    if (
      right < groups.length &&
      boundedGroupEntryIsWorse(groups[right]!, groups[worst]!, orderBy)
    ) {
      worst = right;
    }
    if (worst === current) {
      return;
    }
    swapBoundedGroupEntries(groups, current, worst);
    current = worst;
  }
};

const retainBoundedGroup = (
  groups: Array<BoundedGroupEntry>,
  group: GroupState,
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
  windowEnd: number,
  scratchOrderValues: Array<unknown>,
): Array<unknown> => {
  if (groups.length < windowEnd) {
    groups.push(newBoundedGroupEntry(group, orderBy));
    siftWorstBoundedGroupEntryUp(groups, groups.length - 1, orderBy);
    return scratchOrderValues;
  }
  const worstGroup = groups[0]!;
  const candidateOrderValues =
    orderBy.length === 0
      ? emptyBoundedGroupOrderValues
      : writeGroupOrderValues(scratchOrderValues, group, orderBy);
  if (compareGroupToBoundedGroupEntry(group, candidateOrderValues, worstGroup, orderBy) >= 0) {
    return scratchOrderValues;
  }
  const nextScratchOrderValues =
    worstGroup.orderValues === emptyBoundedGroupOrderValues
      ? scratchOrderValues
      : worstGroup.orderValues;
  worstGroup.group = group;
  worstGroup.orderValues = candidateOrderValues;
  siftWorstBoundedGroupEntryDown(groups, 0, orderBy);
  return nextScratchOrderValues;
};

const boundedGroupedEvaluationFromGroups = (
  groups: Iterable<GroupState>,
  query: RuntimeGroupedWindowQuery,
  version: number,
  windowEnd: number,
): QueryEvaluation<RowObject> => {
  const orderBy = query.orderBy ?? [];
  const retainedGroups: Array<BoundedGroupEntry> = [];
  let scratchOrderValues: Array<unknown> = [];
  let totalRows = 0;
  for (const group of groups) {
    totalRows += 1;
    scratchOrderValues = retainBoundedGroup(
      retainedGroups,
      group,
      orderBy,
      windowEnd,
      scratchOrderValues,
    );
  }
  const window = retainedGroups
    .toSorted((left, right) => compareBoundedGroupEntries(left, right, orderBy))
    .slice(query.offset ?? 0)
    .map((entry) => finalizeGroup(entry.group));
  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows,
    version,
  };
};

export const emptyGroupedEvaluation = (
  totalRows: number,
  version: number,
): QueryEvaluation<RowObject> => ({
  rows: [],
  keys: [],
  window: [],
  totalRows,
  version,
});

export const groupedEvaluationFromGroups = (
  groups: Iterable<GroupState>,
  query: RuntimeGroupedWindowQuery,
  version: number,
): QueryEvaluation<RowObject> => {
  const windowEnd = groupedWindowEnd(query);
  if (windowEnd !== undefined) {
    return boundedGroupedEvaluationFromGroups(groups, query, version, windowEnd);
  }
  return groupedEvaluationFromEntries(Array.from(groups, finalizeGroup), query, version);
};

export const groupedEvaluationFromEntries = (
  entries: ReadonlyArray<StoredRowOf<RowObject>>,
  query: RuntimeGroupedWindowQuery,
  version: number,
): QueryEvaluation<RowObject> => {
  const ordered = entries.toSorted((left, right) =>
    compareGroupedRows(left, right, query.orderBy ?? []),
  );
  const offset = query.offset ?? 0;
  const window = ordered.slice(
    offset,
    query.limit === undefined ? undefined : offset + query.limit,
  );
  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: ordered.length,
    version,
  };
};
