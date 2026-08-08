import { describe, expectTypeOf, it } from "@effect/vitest";
import type { FalseExpression } from "./index";
import type { ExactWhere, Where } from "./query-filter";
import type { GroupedQuery, RawQuery } from "./index";

type Row = {
  readonly id: string;
  readonly status: "open" | "closed";
};

describe("explicit match-none filter types", () => {
  it("accepts FALSE in raw and grouped where expressions", () => {
    const falseExpression = { type: "FALSE" } satisfies FalseExpression;
    const where = [falseExpression] satisfies Where<Row>;
    const directWhere = [{ type: "FALSE" }] satisfies Where<Row>;
    const composed = [
      {
        type: "AND",
        conditions: [{ type: "FALSE" }, { type: "NOT", condition: { type: "FALSE" } }],
      },
    ] satisfies Where<Row>;
    const raw = { select: ["id"], where } satisfies RawQuery<Row>;
    const directRaw = { select: ["id"], where: [{ type: "FALSE" }] } satisfies RawQuery<Row>;
    const grouped = {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where,
    } satisfies GroupedQuery<Row>;
    const directGrouped = {
      groupBy: ["status"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [{ type: "FALSE" }],
    } satisfies GroupedQuery<Row>;

    expectTypeOf<FalseExpression>().toEqualTypeOf<{ readonly type: "FALSE" }>();
    expectTypeOf(falseExpression).toExtend<FalseExpression>();
    expectTypeOf(raw.where).toExtend<Where<Row>>();
    expectTypeOf(directRaw.where).toExtend<Where<Row>>();
    expectTypeOf(grouped.where).toExtend<Where<Row> | undefined>();
    expectTypeOf(directGrouped.where).toExtend<Where<Row> | undefined>();
    expectTypeOf(directWhere).toExtend<Where<Row>>();
    expectTypeOf(composed).toExtend<Where<Row>>();
    expectTypeOf<ExactWhere<Row, { readonly where: typeof where }>>().toEqualTypeOf<{
      readonly where: typeof where;
    }>();
  });

  it("rejects operands and extra keys on FALSE", () => {
    const withOperand = {
      where: [{ type: "FALSE", field: "status" }],
    } as const;
    expectTypeOf<ExactWhere<Row, typeof withOperand>>().toBeNever();
    // @ts-expect-error FALSE has no field or operand.
    const _withOperand: ExactWhere<Row, typeof withOperand> = withOperand;

    const withConditions = {
      where: [{ type: "FALSE", conditions: [] }],
    } as const;
    expectTypeOf<ExactWhere<Row, typeof withConditions>>().toBeNever();
    // @ts-expect-error FALSE cannot contain Boolean group children.
    const _withConditions: ExactWhere<Row, typeof withConditions> = withConditions;
  });
});
