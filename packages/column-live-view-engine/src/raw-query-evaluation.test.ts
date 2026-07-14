import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { format, fromStringUnsafe } from "effect/BigDecimal";
import { evaluateRawQuery } from "./active-query";
import { compareQueryValue, prepareRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { compileRawPredicate } from "./raw-predicate-compiler";
import { order, Order, position, Position } from "../test-harness/public-engine";

describe("Raw query evaluation", () => {
  it.effect("preserves compiled predicate miss behavior for custom storage scanners", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id"],
        where: {
          status: { eq: "open" },
          customerId: { startsWith: "customer-" },
        },
      });
      const rows = [
        order("1", "closed", 10, 1),
        { ...order("2", "open", 20, 2), customerId: "account-2" },
      ];

      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            const window = rows
              .filter((row) => plan.matches(row))
              .map((row) => ({
                key: row.id,
                row,
              }));
            return {
              keys: window.map((entry) => entry.key),
              window,
              totalRows: window.length,
            };
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
    }),
  );

  it("does not match scalar union members with structured literal filters", () => {
    const UnionRow = Schema.Struct({
      id: Schema.String,
      value: Schema.Union([
        Schema.Number,
        Schema.Struct({
          eq: Schema.Number,
          kind: Schema.String,
        }),
      ]),
    });
    const predicate = compileRawPredicate<typeof UnionRow.Type>(
      rawQueryCompilerMetadata(UnionRow),
      {
        value: {
          eq: 1,
          kind: "literal",
        },
      },
    );

    expect([
      predicate.matches({ id: "scalar", value: 1 }),
      predicate.matches({ id: "literal", value: { eq: 1, kind: "literal" } }),
      predicate.matches({ id: "other", value: { eq: 1, kind: "other" } }),
    ]).toStrictEqual([false, true, false]);
  });

  it("fails closed when scalar or structured in filters contain invalid schema values", () => {
    const ScalarRow = Schema.Struct({
      id: Schema.String,
      status: Schema.Literals(["open", "closed"]),
    });
    const StructuredRow = Schema.Struct({
      id: Schema.String,
      profile: Schema.Struct({ code: Schema.String }),
    });
    const scalarPredicate = compileRawPredicate<typeof ScalarRow.Type>(
      rawQueryCompilerMetadata(ScalarRow),
      {
        status: { in: ["open", 1] },
      },
    );
    const structuredPredicate = compileRawPredicate<typeof StructuredRow.Type>(
      rawQueryCompilerMetadata(StructuredRow),
      {
        profile: { in: [{ code: "alpha" }, "alpha"] },
      },
    );

    expect({
      scalar: scalarPredicate.matches({ id: "scalar", status: "open" }),
      structured: structuredPredicate.matches({ id: "structured", profile: { code: "alpha" } }),
    }).toStrictEqual({
      scalar: false,
      structured: false,
    });
  });

  it("owns and freezes the full compiled predicate proof graph", () => {
    const statuses: Array<"open" | "closed" | "cancelled"> = ["open", "closed"];
    const predicate = compileRawPredicate<typeof Order.Type>(rawQueryCompilerMetadata(Order), {
      status: { in: statuses },
      price: { gte: 10 },
    });
    const inFilter = Reflect.get(predicate.plan.filters, "0");
    const rangeFilter = Reflect.get(predicate.plan.filters, "1");
    const values = Reflect.get(inFilter, "values");
    const valueKeys = Reflect.get(inFilter, "valueKeys");

    expect(Object.isFrozen(predicate)).toBe(true);
    expect(Object.isFrozen(predicate.plan)).toBe(true);
    expect(Object.isFrozen(predicate.plan.filters)).toBe(true);
    expect(Object.isFrozen(inFilter)).toBe(true);
    expect(Object.isFrozen(rangeFilter)).toBe(true);
    expect(Object.isFrozen(values)).toBe(true);
    expect(Object.isFrozen(valueKeys)).toBe(true);
    expect(values).toStrictEqual(["open", "closed"]);
    expect(valueKeys.has("string:4:open")).toBe(true);
    expect(valueKeys.has("string:6:closed")).toBe(true);

    expect(() => Object.assign(predicate, { matches: () => false })).toThrowError(TypeError);
    expect(() => Object.assign(predicate.plan, { callbackRequired: true })).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(predicate.plan.filters, rangeFilter)).toThrowError(
      TypeError,
    );
    expect(() => Object.assign(inFilter, { field: "price" })).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(values, "cancelled")).toThrowError(TypeError);
    expect(() => Set.prototype.add.call(valueKeys, "string:9:cancelled")).toThrowError(TypeError);

    statuses.push("cancelled");
    expect(values).toStrictEqual(["open", "closed"]);
    expect(predicate.matches(order("open", "open", 10, 1))).toBe(true);
    expect(predicate.matches(order("cancelled", "cancelled", 10, 1))).toBe(false);
  });

  it("owns immutable BigDecimal values exposed by predicate plans", () => {
    const source = fromStringUnsafe("1.00");
    const predicate = compileRawPredicate<typeof Position.Type>(
      rawQueryCompilerMetadata(Position),
      {
        price: { gte: source },
      },
    );
    const filter = Reflect.get(predicate.plan.filters, "0");
    const planValue = Reflect.get(filter, "value");
    const normalized = Reflect.get(planValue, "normalized");

    expect(Object.isFrozen(planValue)).toBe(true);
    expect(Object.isFrozen(normalized)).toBe(true);
    expect(format(planValue)).toBe("1");
    expect(() => Object.assign(planValue, { value: 999n })).toThrowError(TypeError);

    Object.assign(source, { value: 999n });
    expect(format(planValue)).toBe("1");
    expect(predicate.matches(position("low", "LOW", 1n, "0.5"))).toBe(false);
    expect(predicate.matches(position("high", "HIGH", 1n, "2"))).toBe(true);
  });

  it("orders array and object fallback values by their stable query ranks", () => {
    expect({
      arrayBeforeObject: compareQueryValue([], {}),
      objectAfterArray: compareQueryValue({}, []),
    }).toStrictEqual({
      arrayBeforeObject: -1,
      objectAfterArray: 1,
    });
  });
});
