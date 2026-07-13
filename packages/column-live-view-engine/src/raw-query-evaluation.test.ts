import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { evaluateRawQuery } from "./active-query";
import { compareQueryValue, prepareRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { compileRawPredicate } from "./raw-predicate-compiler";
import { order, Order } from "../test-harness/public-engine";

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
