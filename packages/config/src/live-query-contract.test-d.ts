import { describe, expectTypeOf, it } from "@effect/vitest";
import type { ExactGroupedQuery, ExactRawQuery, FilterExpression } from "./index";
import { viewServer } from "../test-harness/live-query";
import type { LiveQueryCall } from "../test-harness/live-query";
import { Order } from "../test-harness/schemas";

type GeneratedInvalidLeaf = {
  readonly field: "price";
  readonly type: "contains";
  readonly filter: "invalid";
};

type GeneratedNestedNot<
  Depth extends number,
  Levels extends ReadonlyArray<unknown> = readonly [],
> = Levels["length"] extends Depth
  ? GeneratedInvalidLeaf
  : {
      readonly type: "NOT";
      readonly condition: GeneratedNestedNot<Depth, readonly [...Levels, unknown]>;
    };

type WidenedSurplusFilterGroup = {
  readonly type: "AND";
  readonly conditions: ReadonlyArray<{
    readonly field: "id";
    readonly type: "equals";
    readonly filter: "valid";
    readonly unexpected: true;
  }>;
};

type ValidIdFilter = {
  readonly field: "id";
  readonly type: "equals";
  readonly filter: "valid";
};

type VariadicSurplusWhere = readonly [
  ValidIdFilter,
  ...Array<ValidIdFilter & { readonly unexpected: true }>,
];

type UnionWithInvalidGroupWhere = readonly [ValidIdFilter | WidenedSurplusFilterGroup];

type GroupWithInvalidConditionsUnion = {
  readonly type: "OR";
  readonly conditions: readonly [] | ReadonlyArray<ValidIdFilter & { readonly unexpected: true }>;
};

type RecursiveOrderFilter =
  | ValidIdFilter
  | { readonly type: "NOT"; readonly condition: RecursiveOrderFilter };

type DecoratedInFilterUnion =
  | readonly ["valid"]
  | (readonly ["valid"] & { readonly metadata: true });

type RawQueryUnionWithInvalidWhere =
  | { readonly select: readonly ["id"] }
  | {
      readonly select: readonly ["id"];
      readonly where: readonly [ValidIdFilter & { readonly unexpected: true }];
    };

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

      const emptyWhere = useLiveQuery("orders", {
        select: ["id"],
        where: [],
      });
      expectTypeOf(emptyWhere.rows).toEqualTypeOf<ReadonlyArray<{ readonly id: string }>>();

      const nestedEmptyGroups = useLiveQuery("orders", {
        select: ["id"],
        where: [
          { type: "AND", conditions: [] },
          { type: "OR", conditions: [] },
          { type: "NOT", condition: { type: "AND", conditions: [] } },
        ],
      });
      expectTypeOf(nestedEmptyGroups.rows).toEqualTypeOf<ReadonlyArray<{ readonly id: string }>>();

      const canonicalExpression: FilterExpression<typeof Order.Type> = {
        field: "id",
        type: "equals",
        filter: "order-1",
      };
      const canonicalTuple: readonly [FilterExpression<typeof Order.Type>] = [canonicalExpression];
      const canonicalTupleQuery = useLiveQuery("orders", {
        select: ["id"],
        where: canonicalTuple,
      });
      expectTypeOf(canonicalTupleQuery.rows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string }>
      >();

      const recursiveFilter: readonly [RecursiveOrderFilter] = [
        { type: "NOT", condition: { field: "id", type: "equals", filter: "valid" } },
      ];
      const recursiveFilterQuery = useLiveQuery("orders", {
        select: ["id"],
        where: recursiveFilter,
      });
      expectTypeOf(recursiveFilterQuery.rows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string }>
      >();

      type RawOrder = { readonly field: "id"; readonly direction: "asc" };
      const variadicRawOrderBy: readonly [RawOrder, ...Array<RawOrder>] = [
        { field: "id", direction: "asc" },
      ];
      const variadicRaw = useLiveQuery("orders", {
        select: ["id"],
        orderBy: variadicRawOrderBy,
      });
      expectTypeOf(variadicRaw.rows).toEqualTypeOf<ReadonlyArray<{ readonly id: string }>>();

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

      type GroupedOrder = { readonly aggregate: "rowCount"; readonly direction: "desc" };
      const variadicGroupedOrderBy: [GroupedOrder, ...Array<GroupedOrder>] = [
        { aggregate: "rowCount", direction: "desc" },
      ];
      const variadicGrouped = useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: variadicGroupedOrderBy,
      });
      expectTypeOf(variadicGrouped.rows).toEqualTypeOf<
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

      const rejectGeneratedInvalidQuery = (query: {
        readonly select: readonly ["id"];
        readonly where: readonly [GeneratedNestedNot<5>];
      }) => {
        // @ts-expect-error recursively generated aliases cannot hide an invalid leaf operator.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectGeneratedInvalidQuery).toBeFunction();

      const rejectWidenedNestedSurplus = (query: {
        readonly select: readonly ["id"];
        readonly where: readonly [WidenedSurplusFilterGroup];
      }) => {
        // @ts-expect-error widened nested arrays still enforce exact child expressions.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectWidenedNestedSurplus).toBeFunction();

      const rejectVariadicSurplus = (query: {
        readonly select: readonly ["id"];
        readonly where: VariadicSurplusWhere;
      }) => {
        // @ts-expect-error variadic where tails still enforce exact expressions.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectVariadicSurplus).toBeFunction();

      const rejectUnionWithInvalidGroup = (query: {
        readonly select: readonly ["id"];
        readonly where: UnionWithInvalidGroupWhere;
      }) => {
        // @ts-expect-error every union member must be an exact recursive expression.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectUnionWithInvalidGroup).toBeFunction();

      const rejectGroupWithInvalidConditionsUnion = (query: {
        readonly select: readonly ["id"];
        readonly where: readonly [GroupWithInvalidConditionsUnion];
      }) => {
        // @ts-expect-error every conditions-array union member must be exact.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectGroupWithInvalidConditionsUnion).toBeFunction();

      const rejectDecoratedInFilterUnion = (filter: DecoratedInFilterUnion) => {
        const query = {
          select: ["id"],
          where: [{ field: "id", type: "in", filter }],
        } satisfies {
          readonly select: readonly ["id"];
          readonly where: readonly [
            {
              readonly field: "id";
              readonly type: "in";
              readonly filter: DecoratedInFilterUnion;
            },
          ];
        };
        // @ts-expect-error every membership-array union member must be undecorated.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectDecoratedInFilterUnion).toBeFunction();

      const rejectQueryUnionWithInvalidWhere = (query: RawQueryUnionWithInvalidWhere) => {
        // @ts-expect-error an invalid whole-query union member cannot collapse out of validation.
        useLiveQuery("orders", query);
      };
      expectTypeOf(rejectQueryUnionWithInvalidWhere).toBeFunction();
      expectTypeOf<ExactRawQuery<typeof Order.Type, RawQueryUnionWithInvalidWhere>>().toBeNever();

      const decoratedSelect = Object.assign(["id"] satisfies ["id"], {
        metadata: "id" as const,
      });
      // @ts-expect-error select arrays reject valid-field-valued string decorations.
      useLiveQuery("orders", { select: decoratedSelect });

      const selectDecorationSymbol = Symbol("select-decoration");
      const symbolDecoratedSelect = Object.assign(["id"] satisfies ["id"], {
        [selectDecorationSymbol]: "id" as const,
      });
      // @ts-expect-error select arrays reject valid-field-valued symbol decorations.
      useLiveQuery("orders", { select: symbolDecoratedSelect });

      const decoratedGroupBy = Object.assign(["status"] satisfies ["status"], {
        metadata: "status" as const,
      });
      const decoratedGroupByQuery = {
        groupBy: decoratedGroupBy,
        aggregates: { rowCount: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: typeof decoratedGroupBy;
        readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
      };
      // @ts-expect-error groupBy arrays reject valid-field-valued string decorations.
      useLiveQuery("orders", decoratedGroupByQuery);

      const groupByDecorationSymbol = Symbol("group-by-decoration");
      const symbolDecoratedGroupBy = Object.assign(["status"] satisfies ["status"], {
        [groupByDecorationSymbol]: "status" as const,
      });
      const symbolDecoratedGroupByQuery = {
        groupBy: symbolDecoratedGroupBy,
        aggregates: { rowCount: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: typeof symbolDecoratedGroupBy;
        readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
      };
      // @ts-expect-error groupBy arrays reject valid-field-valued symbol decorations.
      useLiveQuery("orders", symbolDecoratedGroupByQuery);

      const aggregateAliasSymbol = Symbol("aggregate-alias");
      const symbolDecoratedAggregates = {
        rowCount: { aggFunc: "count" as const },
        [aggregateAliasSymbol]: { aggFunc: "count" as const },
      };
      const symbolDecoratedAggregatesQuery = {
        groupBy: ["status"],
        aggregates: symbolDecoratedAggregates,
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: typeof symbolDecoratedAggregates;
      };
      // @ts-expect-error aggregate maps reject symbol-keyed aliases that runtime snapshots reject.
      useLiveQuery("orders", symbolDecoratedAggregatesQuery);

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

      const decoratedRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies Array<{
          readonly field: "id";
          readonly direction: "asc";
        }>,
        { metadata: true },
      );
      const decoratedRawOrderQuery = {
        select: ["id"],
        orderBy: decoratedRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof decoratedRawOrderBy;
      };
      // @ts-expect-error orderBy arrays reject array-level surplus properties.
      useLiveQuery("orders", decoratedRawOrderQuery);

      const negativeIndexRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies Array<{
          readonly field: "id";
          readonly direction: "asc";
        }>,
        { "-1": true },
      );
      const negativeIndexRawOrderQuery = {
        select: ["id"],
        orderBy: negativeIndexRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof negativeIndexRawOrderBy;
      };
      // @ts-expect-error orderBy arrays reject negative numeric-looking surplus keys.
      useLiveQuery("orders", negativeIndexRawOrderQuery);

      const fractionalIndexRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies Array<{
          readonly field: "id";
          readonly direction: "asc";
        }>,
        { "1.5": true },
      );
      const fractionalIndexRawOrderQuery = {
        select: ["id"],
        orderBy: fractionalIndexRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof fractionalIndexRawOrderBy;
      };
      // @ts-expect-error orderBy arrays reject fractional numeric-looking surplus keys.
      useLiveQuery("orders", fractionalIndexRawOrderQuery);

      const paddedIndexRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies Array<{
          readonly field: "id";
          readonly direction: "asc";
        }>,
        { "01": true },
      );
      const paddedIndexRawOrderQuery = {
        select: ["id"],
        orderBy: paddedIndexRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof paddedIndexRawOrderBy;
      };
      // @ts-expect-error orderBy arrays reject padded numeric-looking surplus keys.
      useLiveQuery("orders", paddedIndexRawOrderQuery);

      type IndexedRawOrder = { readonly field: "id"; readonly direction: "asc" };
      const outOfBoundsRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies [IndexedRawOrder],
        { "2": { field: "id", direction: "asc" } satisfies IndexedRawOrder },
      );
      const outOfBoundsRawOrderQuery = {
        select: ["id"],
        orderBy: outOfBoundsRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof outOfBoundsRawOrderBy;
      };
      // @ts-expect-error orderBy tuple indices must belong to the required tuple prefix.
      useLiveQuery("orders", outOfBoundsRawOrderQuery);

      const beyondArrayIndexRawOrderBy = Object.assign(
        [{ field: "id", direction: "asc" }] satisfies [IndexedRawOrder],
        { "4294967295": { field: "id", direction: "asc" } satisfies IndexedRawOrder },
      );
      const beyondArrayIndexRawOrderQuery = {
        select: ["id"],
        orderBy: beyondArrayIndexRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof beyondArrayIndexRawOrderBy;
      };
      // @ts-expect-error properties beyond JavaScript's array-index range are decorations.
      useLiveQuery("orders", beyondArrayIndexRawOrderQuery);

      type ReadonlyRawOrder = { readonly field: "id"; readonly direction: "asc" };
      const readonlyRawOrderBy: readonly [ReadonlyRawOrder] = [{ field: "id", direction: "asc" }];
      const mutatorDecoratedRawOrderBy = Object.assign(readonlyRawOrderBy, {
        push: (...entries: Array<ReadonlyRawOrder>): number => entries.length,
      });
      const mutatorDecoratedRawOrderQuery = {
        select: ["id"],
        orderBy: mutatorDecoratedRawOrderBy,
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: typeof mutatorDecoratedRawOrderBy;
      };
      // @ts-expect-error readonly orderBy arrays reject own mutable-array method decorations.
      useLiveQuery("orders", mutatorDecoratedRawOrderQuery);

      const decoratedGroupedOrderBy = Object.assign(
        [{ aggregate: "rowCount", direction: "desc" }] satisfies Array<{
          readonly aggregate: "rowCount";
          readonly direction: "desc";
        }>,
        { metadata: true },
      );
      const decoratedGroupedOrderQuery = {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: decoratedGroupedOrderBy,
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: { readonly rowCount: { readonly aggFunc: "count" } };
        readonly orderBy: typeof decoratedGroupedOrderBy;
      };
      // @ts-expect-error grouped orderBy arrays reject array-level surplus properties.
      useLiveQuery("orders", decoratedGroupedOrderQuery);

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
