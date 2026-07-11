import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  type ExactGroupedQuery,
  type ExactRawQuery,
  type RawQuery,
  type ValidateLiveQuery,
} from "./index";

import { viewServer } from "../test-harness/live-query";
import { Order, Position } from "../test-harness/schemas";

import type { LiveQueryCall } from "../test-harness/live-query";

describe("Live query generic contracts", () => {
  it("accepts valid contracts and rejects invalid contracts", () => {
    const assertLiveQueryContracts = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      // @ts-expect-error raw queries must explicitly select projected fields.
      useLiveQuery("orders", {
        where: { status: "open" },
      });

      const unknownWhereFieldQuery = {
        select: ["id"],
        where: {
          missing: "open",
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly missing: "open" };
      };
      // @ts-expect-error raw queries reject fields not present on the selected topic.
      useLiveQuery("orders", unknownWhereFieldQuery);

      const wrongFilterValueQuery = {
        select: ["id"],
        where: {
          price: "not-a-number",
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly price: "not-a-number" };
      };
      // @ts-expect-error filter values must match the selected field type.
      useLiveQuery("orders", wrongFilterValueQuery);

      const stringRangeFilterQuery = {
        select: ["id"],
        where: {
          status: { gte: "open" },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly status: { readonly gte: "open" } };
      };
      // @ts-expect-error string filters do not accept range operators.
      useLiveQuery("orders", stringRangeFilterQuery);

      const invalidStatusInFilter = {
        select: ["id"],
        where: {
          status: { in: ["open", "pending"] },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: {
          readonly status: {
            readonly in: readonly ["open", "pending"];
          };
        };
      };
      // @ts-expect-error filter arrays must contain selected field values
      const _invalidStatusInFilter: RawQuery<typeof Order.Type> &
        ExactRawQuery<typeof Order.Type, typeof invalidStatusInFilter> = invalidStatusInFilter;

      const numericStartsWithFilterQuery = {
        select: ["id"],
        where: {
          price: { startsWith: "1" },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly price: { readonly startsWith: "1" } };
      };
      // @ts-expect-error number filters do not accept string-only operators.
      useLiveQuery("orders", numericStartsWithFilterQuery);

      const booleanRangeFilterQuery = {
        select: ["id"],
        where: {
          active: { gte: true },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly active: { readonly gte: true } };
      };
      // @ts-expect-error boolean filters do not accept range operators.
      useLiveQuery("positions", booleanRangeFilterQuery);

      const booleanStartsWithFilterQuery = {
        select: ["id"],
        where: {
          active: { startsWith: "t" },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly active: { readonly startsWith: "t" } };
      };
      // @ts-expect-error boolean filters do not accept string-only operators.
      useLiveQuery("positions", booleanStartsWithFilterQuery);

      const bigDecimalStringFilterQuery = {
        select: ["id"],
        where: {
          price: { gte: "10.00" },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly price: { readonly gte: "10.00" } };
      };
      // @ts-expect-error BigDecimal filters require BigDecimal values, not strings.
      useLiveQuery("positions", bigDecimalStringFilterQuery);

      const bigDecimalStartsWithFilterQuery = {
        select: ["id"],
        where: {
          price: { startsWith: "10" },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly price: { readonly startsWith: "10" } };
      };
      // @ts-expect-error BigDecimal filters do not accept string-only operators.
      useLiveQuery("positions", bigDecimalStartsWithFilterQuery);

      const bigintNumberFilterQuery = {
        select: ["id"],
        where: {
          quantity: { gte: 1 },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly quantity: { readonly gte: 1 } };
      };
      // @ts-expect-error bigint filters require bigint values, not numbers.
      useLiveQuery("positions", bigintNumberFilterQuery);

      const optionalNumericEqualityRows = useLiveQuery("positions", {
        select: ["id"],
        where: {
          optionalQuantity: { eq: 1n },
          optionalNotional: 100,
        },
      }).rows;
      expectTypeOf(optionalNumericEqualityRows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string }>
      >();

      const optionalBigintUndefinedFilterQuery = {
        select: ["id"],
        where: {
          optionalQuantity: undefined,
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: undefined };
      };
      // @ts-expect-error optional filters reject present undefined values.
      useLiveQuery("positions", optionalBigintUndefinedFilterQuery);

      const optionalBigintUnionFilterQuery = (optionalQuantity: bigint | undefined) =>
        ({
          select: ["id"],
          where: {
            optionalQuantity,
          },
        }) satisfies {
          readonly select: readonly ["id"];
          readonly where: { readonly optionalQuantity: bigint | undefined };
        };
      // @ts-expect-error optional filters reject unions that can contain undefined.
      useLiveQuery("positions", optionalBigintUnionFilterQuery(1n));

      const optionalBigintUndefinedEqualityFilterQuery = {
        select: ["id"],
        where: {
          optionalQuantity: { eq: undefined },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: { readonly eq: undefined } };
      };
      // @ts-expect-error optional equality filters reject present undefined values.
      useLiveQuery("positions", optionalBigintUndefinedEqualityFilterQuery);

      const optionalBigintUnionEqualityFilterQuery = (eq: bigint | undefined) =>
        ({
          select: ["id"],
          where: {
            optionalQuantity: { eq },
          },
        }) satisfies {
          readonly select: readonly ["id"];
          readonly where: { readonly optionalQuantity: { readonly eq: bigint | undefined } };
        };
      // @ts-expect-error optional equality filters reject unions that can contain undefined.
      useLiveQuery("positions", optionalBigintUnionEqualityFilterQuery(1n));

      const optionalBigintRangeFilterQuery = {
        select: ["id"],
        where: {
          optionalQuantity: { gte: 1n },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: { readonly gte: 1n } };
      };
      // @ts-expect-error optional numeric fields only support equality filters.
      useLiveQuery("positions", optionalBigintRangeFilterQuery);

      const optionalBigintEqualityWithRangeFilterQuery = {
        select: ["id"],
        where: {
          optionalQuantity: { eq: 1n, gte: 1n },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: { readonly eq: 1n; readonly gte: 1n } };
      };
      // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
      useLiveQuery("positions", optionalBigintEqualityWithRangeFilterQuery);

      const optionalNumberRangeFilterQuery = {
        select: ["id"],
        where: {
          optionalNotional: { lte: 100 },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalNotional: { readonly lte: 100 } };
      };
      // @ts-expect-error optional numeric fields only support equality filters.
      useLiveQuery("positions", optionalNumberRangeFilterQuery);

      const optionalNumberEqualityWithRangeFilterQuery = {
        select: ["id"],
        where: {
          optionalNotional: { eq: 100, lte: 100 },
        },
      } satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalNotional: { readonly eq: 100; readonly lte: 100 } };
      };
      // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
      useLiveQuery("positions", optionalNumberEqualityWithRangeFilterQuery);

      const unknownOrderByFieldQuery = {
        select: ["id"],
        orderBy: [{ field: "missing", direction: "asc" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: readonly [{ readonly field: "missing"; readonly direction: "asc" }];
      };
      // @ts-expect-error orderBy fields are constrained to the selected topic row.
      useLiveQuery("orders", unknownOrderByFieldQuery);

      const invalidOrderByDirectionQuery = {
        select: ["id"],
        orderBy: [{ field: "price", direction: "ascending" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: readonly [{ readonly field: "price"; readonly direction: "ascending" }];
      };
      // @ts-expect-error sort direction is constrained to asc or desc.
      useLiveQuery("orders", invalidOrderByDirectionQuery);

      const rawAggregateOrderByQuery = {
        select: ["id"],
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: readonly [
          { readonly aggregate: "totalPrice"; readonly direction: "desc" },
        ];
      };
      // @ts-expect-error raw orderBy cannot reference aggregate aliases.
      useLiveQuery("orders", rawAggregateOrderByQuery);

      const invalidSelectedFields = {
        select: ["id", "missing"],
      } satisfies {
        readonly select: readonly ["id", "missing"];
      };
      // @ts-expect-error projected fields are constrained to the selected topic row
      useLiveQuery("orders", invalidSelectedFields);

      const invalidGroupByField = {
        groupBy: ["missing"],
        aggregates: { count: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: readonly ["missing"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
      };
      // @ts-expect-error grouped queries reject groupBy fields not present on the topic row
      const _invalidGroupByField: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupByField> =
        invalidGroupByField;

      const invalidGroupedSelect = {
        groupBy: ["status"],
        select: ["id"],
        aggregates: { count: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly select: readonly ["id"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
      };
      // @ts-expect-error grouped queries cannot select raw fields.
      const _invalidGroupedSelect: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedSelect
      > = invalidGroupedSelect;

      const invalidAggregateAliasCollision = {
        groupBy: ["status"],
        aggregates: { status: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly status: {
            readonly aggFunc: "count";
          };
        };
      };
      // @ts-expect-error aggregate aliases cannot collide with groupBy fields
      const _invalidAggregateAliasCollision: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidAggregateAliasCollision
      > &
        ValidateLiveQuery<typeof invalidAggregateAliasCollision> = invalidAggregateAliasCollision;

      const invalidEmptyAggregates = {
        groupBy: ["status"],
        aggregates: {},
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {};
      };
      // @ts-expect-error grouped queries require at least one aggregate alias.
      const _invalidEmptyAggregates: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidEmptyAggregates
      > &
        ValidateLiveQuery<typeof invalidEmptyAggregates> = invalidEmptyAggregates;

      const invalidDangerousAggregateAlias = {
        groupBy: ["status"],
        aggregates: { constructor: { aggFunc: "count" } },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly constructor: {
            readonly aggFunc: "count";
          };
        };
      };
      // @ts-expect-error grouped aggregate aliases must not use dangerous object keys.
      const _invalidDangerousAggregateAlias: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidDangerousAggregateAlias
      > &
        ValidateLiveQuery<typeof invalidDangerousAggregateAlias> = invalidDangerousAggregateAlias;

      const invalidGroupedOrderByRawField = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ field: "price", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly field: "price";
            readonly direction: "desc";
          },
        ];
      };
      // @ts-expect-error grouped orderBy only accepts groupBy fields or aggregate aliases.
      const _invalidGroupedOrderByRawField: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByRawField
      > = invalidGroupedOrderByRawField;

      const invalidGroupedOrderByDirection = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ aggregate: "count", direction: "descending" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly aggregate: "count";
            readonly direction: "descending";
          },
        ];
      };
      // @ts-expect-error grouped orderBy direction is constrained to asc or desc.
      const _invalidGroupedOrderByDirection: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByDirection
      > = invalidGroupedOrderByDirection;

      const invalidGroupedOrderByAggregate = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly aggregate: "totalPrice";
            readonly direction: "desc";
          },
        ];
      };
      // @ts-expect-error grouped orderBy aggregate aliases must exist in aggregates.
      const _invalidGroupedOrderByAggregate: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByAggregate
      > = invalidGroupedOrderByAggregate;

      const invalidGroupedOrderByFieldKey = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ orderByField: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly orderByField: "status";
            readonly direction: "asc";
          },
        ];
      };
      // @ts-expect-error grouped orderBy group fields use field, not orderByField.
      const _invalidGroupedOrderByFieldKey: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByFieldKey
      > = invalidGroupedOrderByFieldKey;

      const invalidGroupedOrderByAggregateKey = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ field: "count", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly field: "count";
            readonly direction: "desc";
          },
        ];
      };
      // @ts-expect-error grouped orderBy aggregate aliases use aggregate, not field.
      const _invalidGroupedOrderByAggregateKey: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByAggregateKey
      > = invalidGroupedOrderByAggregateKey;

      const invalidGroupedOrderByBothFieldAndAggregate = {
        groupBy: ["status"],
        aggregates: { count: { aggFunc: "count" } },
        orderBy: [{ field: "status", aggregate: "count", direction: "desc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly count: {
            readonly aggFunc: "count";
          };
        };
        readonly orderBy: readonly [
          {
            readonly field: "status";
            readonly aggregate: "count";
            readonly direction: "desc";
          },
        ];
      };
      // @ts-expect-error grouped orderBy entries must choose field or aggregate, not both.
      const _invalidGroupedOrderByBothFieldAndAggregate: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidGroupedOrderByBothFieldAndAggregate
      > = invalidGroupedOrderByBothFieldAndAggregate;

      const rawOrderByFieldAndAggregateQuery = {
        select: ["id"],
        orderBy: [{ field: "price", aggregate: "totalPrice", direction: "desc" }],
      } satisfies {
        readonly select: readonly ["id"];
        readonly orderBy: readonly [
          {
            readonly field: "price";
            readonly aggregate: "totalPrice";
            readonly direction: "desc";
          },
        ];
      };
      // @ts-expect-error raw orderBy entries cannot also include aggregate.
      useLiveQuery("orders", rawOrderByFieldAndAggregateQuery);

      const invalidOrderSumField = {
        groupBy: ["status"],
        aggregates: {
          badTotal: { aggFunc: "sum", field: "status" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly badTotal: {
            readonly aggFunc: "sum";
            readonly field: "status";
          };
        };
      };
      // @ts-expect-error sum and avg aggregate fields must be numeric
      const _invalidOrderSumField: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidOrderSumField
      > = invalidOrderSumField;

      const invalidAggregateExtraKey = {
        groupBy: ["status"],
        aggregates: {
          totalPrice: { aggFunc: "sum", field: "price", typo: true },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly totalPrice: {
            readonly aggFunc: "sum";
            readonly field: "price";
            readonly typo: true;
          };
        };
      };
      // @ts-expect-error aggregate definitions reject extra keys through variables.
      const _invalidAggregateExtraKey: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidAggregateExtraKey
      > = invalidAggregateExtraKey;

      const invalidCountAggregateField = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count", field: "price" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: {
            readonly aggFunc: "count";
            readonly field: "price";
          };
        };
      };
      // @ts-expect-error count aggregate definitions must not include a field.
      const _invalidCountAggregateField: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidCountAggregateField
      > = invalidCountAggregateField;

      const invalidPositionSumField = {
        groupBy: ["accountId"],
        aggregates: {
          badSymbolTotal: { aggFunc: "sum", field: "symbol" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly badSymbolTotal: {
            readonly aggFunc: "sum";
            readonly field: "symbol";
          };
        };
      };
      // @ts-expect-error sum aggregate fields must be numeric, bigint, or BigDecimal
      const _invalidPositionSumField: ExactGroupedQuery<
        typeof Position.Type,
        typeof invalidPositionSumField
      > = invalidPositionSumField;

      const invalidOrderAverageField = {
        groupBy: ["status"],
        aggregates: {
          badAverage: { aggFunc: "avg", field: "status" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly badAverage: {
            readonly aggFunc: "avg";
            readonly field: "status";
          };
        };
      };
      // @ts-expect-error avg aggregate fields must be numeric
      const _invalidOrderAverageField: ExactGroupedQuery<
        typeof Order.Type,
        typeof invalidOrderAverageField
      > = invalidOrderAverageField;

      const invalidPositionAverageField = {
        groupBy: ["accountId"],
        aggregates: {
          badSymbolAverage: { aggFunc: "avg", field: "symbol" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly badSymbolAverage: {
            readonly aggFunc: "avg";
            readonly field: "symbol";
          };
        };
      };
      // @ts-expect-error avg aggregate fields must be numeric, bigint, or BigDecimal
      const _invalidPositionAverageField: ExactGroupedQuery<
        typeof Position.Type,
        typeof invalidPositionAverageField
      > = invalidPositionAverageField;
    };

    expectTypeOf(assertLiveQueryContracts).toBeFunction();
  });
});
