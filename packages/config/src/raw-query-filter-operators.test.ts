import { describe, expect, it } from "@effect/vitest";
import {
  isRawQueryFilterOperatorKey,
  isRawQueryRangeFilterOperatorKey,
  rawQueryFilterOperatorKeys,
  rawQueryRangeFilterOperatorKeys,
} from "./raw-query-filter-operators";

describe("raw query filter operators", () => {
  it("owns the exact operator grammar and rejects unknown keys", () => {
    expect([...rawQueryFilterOperatorKeys]).toStrictEqual([
      "eq",
      "neq",
      "in",
      "gt",
      "gte",
      "lt",
      "lte",
      "startsWith",
    ]);
    for (const operator of rawQueryFilterOperatorKeys) {
      expect(isRawQueryFilterOperatorKey(operator)).toBe(true);
    }
    expect(isRawQueryFilterOperatorKey("contains")).toBe(false);
    expect([...rawQueryRangeFilterOperatorKeys]).toStrictEqual(["gt", "gte", "lt", "lte"]);
    for (const operator of rawQueryRangeFilterOperatorKeys) {
      expect(isRawQueryRangeFilterOperatorKey(operator)).toBe(true);
    }
    expect(isRawQueryRangeFilterOperatorKey("eq")).toBe(false);
  });
});
