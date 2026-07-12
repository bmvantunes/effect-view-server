import { describe, expect, it } from "@effect/vitest";
import { rawQueryFilterOperatorKeys } from "@effect-view-server/config/internal";
import { filterOperatorKeys, isFilterObject } from "./protocol-field-filter-codec";

describe("protocol filter operator ownership", () => {
  it("uses the canonical raw query operator grammar by identity", () => {
    expect(filterOperatorKeys).toBe(rawQueryFilterOperatorKeys);
    expect(isFilterObject({ eq: "open", lt: 10 })).toBe(true);
    expect(isFilterObject({ contains: "open" })).toBe(false);
  });
});
