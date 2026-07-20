import { describe, expect, it } from "@effect/vitest";
import { trustDecodedRuntimeQuery } from "./validated-runtime-query";

describe("validated runtime query proof", () => {
  it("brands a decoded query without cloning or changing its wire value", () => {
    const query = { select: ["id"] };

    expect(trustDecodedRuntimeQuery(query)).toBe(query);
  });
});
