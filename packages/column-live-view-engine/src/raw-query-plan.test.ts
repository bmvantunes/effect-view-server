import { describe, expect, it } from "@effect/vitest";
import { order, Order } from "../test-harness/public-engine";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import type { RuntimeRawQuery } from "./raw-query-decoder";
import { makeRawQueryPlan } from "./raw-query-plan";
import { runtimeRawQueryResultSemantics } from "./query-result-semantics";

describe("Raw Query Plan", () => {
  it("owns and freezes selected fields, order, row order, and window", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const selectedFields = ["id", "price"];
    const sourceOrder: {
      field: string;
      direction: "asc" | "desc";
    } = {
      field: "price",
      direction: "asc",
    };
    const orderBy = [sourceOrder];
    const query: RuntimeRawQuery = {
      select: selectedFields,
      orderBy,
      offset: 1,
      limit: 2,
    };
    const plan = makeRawQueryPlan(
      metadata,
      query,
      runtimeRawQueryResultSemantics(metadata.valueSemantics, selectedFields),
    );
    const compiledOrder = Reflect.get(plan.orderBy, "0");

    expect(Object.isFrozen(plan)).toBe(true);
    expect(Object.isFrozen(plan.selectedFields)).toBe(true);
    expect(Object.isFrozen(plan.orderBy)).toBe(true);
    expect(Object.isFrozen(plan.storageOrderBy)).toBe(true);
    expect(Object.isFrozen(compiledOrder)).toBe(true);
    expect(Object.isFrozen(plan.window)).toBe(true);

    expect(() => Array.prototype.push.call(plan.selectedFields, "status")).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(plan.orderBy, sourceOrder)).toThrowError(TypeError);
    expect(() => Object.assign(compiledOrder, { direction: "desc" })).toThrowError(TypeError);
    expect(() => Object.assign(plan.window, { limit: 100 })).toThrowError(TypeError);
    expect(() => Object.assign(plan, { selectedFields: [] })).toThrowError(TypeError);

    selectedFields.push("status");
    sourceOrder.direction = "desc";
    orderBy.push({ field: "id", direction: "desc" });

    expect(plan.selectedFields).toStrictEqual(["id", "price"]);
    expect(plan.orderBy).toStrictEqual([{ field: "price", direction: "asc" }]);
    expect(plan.window).toStrictEqual({
      cacheKey: '["window","[\\\"number\\\",\\\"1\\\"]","[\\\"number\\\",\\\"2\\\"]"]',
      limit: 2,
      offset: 1,
    });
    expect(
      plan.compare(
        { key: "low", row: order("low", "open", 10, 1) },
        { key: "high", row: order("high", "open", 20, 2) },
      ),
    ).toBe(-1);
  });
});
