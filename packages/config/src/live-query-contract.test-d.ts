import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ExactGroupedQuery, ExactRawQuery } from "./index";
import { viewServer } from "../test-harness/live-query";
import type { LiveQueryCall } from "../test-harness/live-query";
import { Order } from "../test-harness/schemas";

describe("Live query generic contracts", () => {
  it("infers recursive raw and grouped query results", () => {
    const assertLiveQueryContracts = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      const raw = useLiveQuery("orders", {
        select: ["id", "price"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          {
            type: "OR",
            conditions: [
              { field: "region", type: "equals", filter: "emea" },
              { field: "customerId", type: "startsWith", filter: "priority-" },
            ],
          },
        ],
      });
      expectTypeOf(raw.rows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string; readonly price: number }>
      >();

      const grouped = useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
      });
      expectTypeOf(grouped.rows).toEqualTypeOf<
        ReadonlyArray<{
          readonly status: "open" | "closed" | "cancelled";
          readonly rowCount: bigint;
        }>
      >();
    };

    expectTypeOf(assertLiveQueryContracts).toBeFunction();
  });

  it("rejects invalid raw and grouped query contracts", () => {
    const assertLiveQueryContracts = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      // @ts-expect-error raw queries must explicitly select projected fields.
      useLiveQuery("orders", { where: [] });

      const unknownWhere = {
        select: ["id"],
        where: [{ field: "missing", type: "equals", filter: "open" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: ReadonlyArray<unknown>;
      };
      // @ts-expect-error filters reject fields absent from the Topic Row.
      useLiveQuery("orders", unknownWhere);

      const wrongOperand = {
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: "ten" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: ReadonlyArray<unknown>;
      };
      // @ts-expect-error filter operands must match the selected field.
      useLiveQuery("orders", wrongOperand);

      const unknownOrder = {
        select: ["id"],
        orderBy: [{ field: "missing", direction: "asc" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: ReadonlyArray<unknown>;
      };
      // @ts-expect-error orderBy fields must exist on the Topic Row.
      useLiveQuery("orders", unknownOrder);

      const explicitUndefined = { select: ["id"], where: undefined } satisfies {
        readonly select: readonly ["id"];
        readonly where: undefined;
      };
      // @ts-expect-error optional query properties must be omitted, never set to undefined.
      useLiveQuery("orders", explicitUndefined);

      const invalidGroupBy = {
        groupBy: ["missing"],
        aggregates: { rowCount: { aggFunc: "count" } },
      };
      // @ts-expect-error groupBy fields must exist on the Topic Row.
      const _invalidGrouped: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupBy> =
        invalidGroupBy;

      const invalidRaw = {
        select: ["id"],
        unexpected: true,
      } satisfies {
        readonly select: readonly ["id"];
        readonly unexpected: true;
      };
      // @ts-expect-error exact raw queries reject surplus properties.
      const _invalidRaw: ExactRawQuery<typeof Order.Type, typeof invalidRaw> = invalidRaw;
    };

    expectTypeOf(assertLiveQueryContracts).toBeFunction();
  });
});
