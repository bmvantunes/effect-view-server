const rawQueryFilterOperatorKeyValues = [
  "eq",
  "neq",
  "in",
  "gt",
  "gte",
  "lt",
  "lte",
  "startsWith",
] as const;

const rawQueryRangeFilterOperatorKeyValues = ["gt", "gte", "lt", "lte"] as const;

export const rawQueryFilterOperatorKeys: ReadonlySet<string> = new Set(
  rawQueryFilterOperatorKeyValues,
);

export const isRawQueryFilterOperatorKey = (key: string): boolean =>
  rawQueryFilterOperatorKeys.has(key);

export const rawQueryRangeFilterOperatorKeys: ReadonlySet<string> = new Set(
  rawQueryRangeFilterOperatorKeyValues,
);

export const isRawQueryRangeFilterOperatorKey = (key: string): boolean =>
  rawQueryRangeFilterOperatorKeys.has(key);
