import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { evaluateRawQuery } from "./active-query";
import { prepareRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
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
});
