import { describe, expectTypeOf, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import { type ExactGroupedQuery, type ValidateLiveQuery } from "./index";

import { viewServer } from "../test-harness/live-query";
import { Order } from "../test-harness/schemas";

import type { LiveQueryCall } from "../test-harness/live-query";

declare const decimal: (value: string) => BigDecimal.BigDecimal;

describe("Query result contracts", () => {
  it("derives query result rows from select and grouped aggregates", () => {
    const assertQueryTypes = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      const selectedRawResult = useLiveQuery("orders", {
        select: ["id", "customerId", "status", "price", "region", "updatedAt"],
        where: {
          status: { eq: "open" },
        },
      });

      expectTypeOf(selectedRawResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const selectedResult = useLiveQuery("orders", {
        select: ["customerId", "status", "updatedAt"],
        where: {
          customerId: { startsWith: "customer-" },
          status: "open",
          updatedAt: { gte: 1, lte: 10 },
        },
      });

      expectTypeOf(selectedResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const rawRows = useLiveQuery("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 50,
      }).rows;

      const groupedRows = useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: {
          count: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averageUpdatedAt: { aggFunc: "avg", field: "updatedAt" },
          firstStatus: { aggFunc: "min", field: "status" },
        },
        where: {
          region: "london",
        },
        orderBy: [
          { aggregate: "totalPrice", direction: "desc" },
          { field: "status", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(rawRows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string; readonly price: number }>
      >();
      type GroupedRow = (typeof groupedRows)[number];
      expectTypeOf<GroupedRow>().toEqualTypeOf<{
        readonly status: "open" | "closed" | "cancelled";
        readonly count: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly averageUpdatedAt: BigDecimal.BigDecimal;
        readonly firstStatus: "open" | "closed" | "cancelled";
      }>();

      const singleAggregateResult = useLiveQuery("orders", {
        groupBy: ["region"],
        aggregates: { uniqueCustomers: { aggFunc: "countDistinct", field: "customerId" } },
      });

      expectTypeOf(singleAggregateResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly region: string;
          readonly uniqueCustomers: bigint;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const positionRows = useLiveQuery("positions", {
        select: ["id", "price", "quantity"],
        where: {
          accountId: { startsWith: "acct-" },
          active: true,
          quantity: { gte: 1n, lte: 100n },
          price: { gt: decimal("10.00") },
          notional: { lt: 1_000_000 },
        },
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "quantity", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(positionRows).toEqualTypeOf<
        ReadonlyArray<{
          readonly id: string;
          readonly price: BigDecimal.BigDecimal;
          readonly quantity: bigint;
        }>
      >();

      const groupedPositionRows = useLiveQuery("positions", {
        groupBy: ["accountId", "active"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          symbolCount: { aggFunc: "countDistinct", field: "symbol" },
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          totalPrice: { aggFunc: "sum", field: "price" },
          totalNotional: { aggFunc: "sum", field: "notional" },
          averagePrice: { aggFunc: "avg", field: "price" },
          firstAccountId: { aggFunc: "min", field: "accountId" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [
          { aggregate: "totalQuantity", direction: "desc" },
          { field: "accountId", direction: "asc" },
        ],
      }).rows;

      expectTypeOf<(typeof groupedPositionRows)[number]>().toEqualTypeOf<{
        readonly accountId: string;
        readonly active: boolean;
        readonly rowCount: bigint;
        readonly symbolCount: bigint;
        readonly totalQuantity: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly totalNotional: BigDecimal.BigDecimal;
        readonly averagePrice: BigDecimal.BigDecimal;
        readonly firstAccountId: string;
        readonly maxQuantity: bigint;
      }>();

      const optionalNumericSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalQuantity: {
            readonly aggFunc: "sum";
            readonly field: "optionalQuantity";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumericSumQuery);

      const optionalNumberSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalNotional: { aggFunc: "sum", field: "optionalNotional" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalNotional: {
            readonly aggFunc: "sum";
            readonly field: "optionalNotional";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumberSumQuery);

      const dynamicAggregateAlias: string = "dynamicTotal";
      const dynamicAggregateQuery = {
        groupBy: ["status"],
        aggregates: {
          [dynamicAggregateAlias]: { aggFunc: "sum", field: "price" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly [key: string]: {
            readonly aggFunc: "sum";
            readonly field: "price";
          };
        };
      };
      // @ts-expect-error aggregate aliases must be literal object keys.
      const _invalidDynamicAggregateAlias: ExactGroupedQuery<
        typeof Order.Type,
        typeof dynamicAggregateQuery
      > &
        ValidateLiveQuery<typeof dynamicAggregateQuery> = dynamicAggregateQuery;

      void _invalidDynamicAggregateAlias;
    };

    expectTypeOf(assertQueryTypes).toBeFunction();
  });
});
