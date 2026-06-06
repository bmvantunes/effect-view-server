import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import type { TopicRawWindowScanPlan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import { scalarEqualityKey, valuesEqual } from "./row-values";
import {
  columnValueDoesNotEqual,
  compareExactRangeColumnValue,
  compareRangeColumnValue,
  isComparableRangeValue,
} from "./topic-range-value";
import { columnValue, type TopicColumnValues } from "./topic-column-vector";

type RowObject = object;

type SlotFilterMatcher = (slot: number) => boolean;
type RangePredicateFilter = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
  readonly value: unknown;
};

export type RawPredicateSlotMatcher<Row extends RowObject> =
  | {
      readonly kind: "slot";
      readonly matchesSlot: (slot: number) => boolean;
    }
  | {
      readonly kind: "entry";
      readonly matchesEntry: (slot: number, entry: TopicRowEntry<Row>) => boolean;
    };

export const rawPredicateSlotMatcher = <Row extends RowObject>(
  plan: TopicRawWindowScanPlan<Row>,
  columns: ReadonlyMap<string, TopicColumnValues>,
): RawPredicateSlotMatcher<Row> => {
  const exact = plan.predicate.callbackSkippable === true;
  const filterMatchers = slotFilterMatchers(plan.predicate.filters, columns, exact);
  const matchesFilters = (slot: number): boolean => {
    for (const matcher of filterMatchers) {
      if (!matcher(slot)) {
        return false;
      }
    }
    return true;
  };

  if (exact) {
    return {
      kind: "slot",
      matchesSlot: matchesFilters,
    };
  }

  return {
    kind: "entry",
    matchesEntry: (slot, entry) => matchesFilters(slot) && plan.matches(entry.row),
  };
};

const slotFilterMatchers = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  columns: ReadonlyMap<string, TopicColumnValues>,
  exact: boolean,
): ReadonlyArray<SlotFilterMatcher> => {
  const matchers: Array<SlotFilterMatcher> = [];
  for (const filter of filters) {
    matchers.push(slotFilterMatcher(filter, columns, exact));
  }
  return matchers;
};

const slotFilterMatcher = (
  filter: TopicRawPredicateFilterPlan,
  columns: ReadonlyMap<string, TopicColumnValues>,
  exact: boolean,
): SlotFilterMatcher => {
  const column = columns.get(filter.field);
  if (column === undefined) {
    return () => true;
  }

  switch (filter.operator) {
    case "eq": {
      if (column.kind === "number" && typeof filter.value === "number") {
        return (slot) => Object.is(column.numberAt(slot), filter.value);
      }
      return (slot) => valuesEqual(columnValue(column, slot), filter.value);
    }
    case "neq": {
      if (exact) {
        if (column.kind === "number" && typeof filter.value === "number") {
          return (slot) => {
            const value = column.numberAt(slot);
            return value !== undefined && !Object.is(value, filter.value);
          };
        }
        return (slot) => columnValueDoesNotEqual(columnValue(column, slot), filter.value);
      }
      return (slot) => !valuesEqual(columnValue(column, slot), filter.value);
    }
    case "in": {
      if (filter.valueKeys !== undefined) {
        const valueKeys = filter.valueKeys;
        return (slot) => {
          const key = scalarEqualityKey(columnValue(column, slot));
          return key !== undefined && valueKeys.has(key);
        };
      }
      return (slot) => {
        const value = columnValue(column, slot);
        return filter.values.some((candidate) => valuesEqual(value, candidate));
      };
    }
    case "startsWith": {
      if (typeof filter.value !== "string") {
        return () => true;
      }
      const prefix = filter.value;
      return (slot) => {
        const value = columnValue(column, slot);
        return typeof value === "string" && value.startsWith(prefix);
      };
    }
    case "gt":
    case "gte":
    case "lt":
    case "lte": {
      const rangeFilter: RangePredicateFilter = {
        field: filter.field,
        operator: filter.operator,
        value: filter.value,
      };
      return rangeSlotFilterMatcher(column, rangeFilter, exact);
    }
  }
};

const rangeSlotFilterMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher => {
  if (exact && !isComparableRangeValue(filter.value)) {
    return () => true;
  }
  const numberRangeMatcher = numberColumnRangeMatcher(column, filter, exact);
  if (numberRangeMatcher !== undefined) {
    return numberRangeMatcher;
  }

  if (exact) {
    return (slot) => {
      const exactComparison = compareExactRangeColumnValue(columnValue(column, slot), filter.value);
      if (exactComparison === undefined) {
        return false;
      }
      return rangeComparisonMatches(filter.operator, exactComparison);
    };
  }

  return (slot) => {
    const comparison = compareRangeColumnValue(columnValue(column, slot), filter.value);
    return comparison === undefined || rangeComparisonMatches(filter.operator, comparison);
  };
};

const numberColumnRangeMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher | undefined => {
  if (
    column.kind !== "number" ||
    typeof filter.value !== "number" ||
    !Number.isFinite(filter.value)
  ) {
    return undefined;
  }
  const expected = filter.value;
  if (filter.operator === "gt") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value > expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value > expected;
        };
  }
  if (filter.operator === "gte") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value >= expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value >= expected;
        };
  }
  if (filter.operator === "lt") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value < expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value < expected;
        };
  }
  return exact
    ? (slot) => {
        const value = column.numberAt(slot);
        return value !== undefined && Number.isFinite(value) && value <= expected;
      }
    : (slot) => {
        const value = column.numberAt(slot);
        return value === undefined || !Number.isFinite(value) || value <= expected;
      };
};

const rangeComparisonMatches = (
  operator: TopicRawPredicateFilterPlan["operator"],
  comparison: number,
): boolean => {
  if (operator === "gt") {
    return comparison > 0;
  }
  if (operator === "gte") {
    return comparison >= 0;
  }
  if (operator === "lt") {
    return comparison < 0;
  }
  return comparison <= 0;
};
