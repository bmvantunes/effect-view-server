import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { createColumnLiveViewEngine, InvalidQueryError } from "./index";
import { makeEngine, order, position } from "../test-harness/public-engine";
import { normalizeDecimalAndBigIntFields, normalizeDecimalFields } from "../test-harness/rows";

describe("ColumnLiveViewEngine grouped snapshots", () => {
  it.effect("evaluates grouped snapshots with aggregate ordering and windows", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 20, 2, "amer"),
        order("3", "closed", 5, 3, "emea"),
        order("4", "closed", 20, 4, "amer"),
        order("5", "cancelled", 0, 5, "emea"),
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          distinctRegions: { aggFunc: "countDistinct", field: "region" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minUpdatedAt: { aggFunc: "min", field: "updatedAt" },
          maxUpdatedAt: { aggFunc: "max", field: "updatedAt" },
        },
        orderBy: [
          { aggregate: "totalPrice", direction: "desc" },
          { field: "status", direction: "asc" },
        ],
        offset: 0,
        limit: 2,
      });

      expect(snapshot.totalRows).toBe(3);
      expect(normalizeDecimalFields(snapshot.rows)).toStrictEqual([
        {
          status: "open",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "30",
          averagePrice: "15",
          minUpdatedAt: 1,
          maxUpdatedAt: 2,
        },
        {
          status: "closed",
          rowCount: 2n,
          distinctRegions: 2n,
          totalPrice: "25",
          averagePrice: "12.5",
          minUpdatedAt: 3,
          maxUpdatedAt: 4,
        },
      ]);

      const filteredSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: "emea",
        },
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(filteredSnapshot.totalRows).toBe(3);
      expect(filteredSnapshot.rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 1n },
        { status: "open", rowCount: 1n },
      ]);

      const noExplicitOrderSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      expect(noExplicitOrderSnapshot.rows).toStrictEqual([
        { status: "cancelled", rowCount: 1n },
        { status: "closed", rowCount: 2n },
        { status: "open", rowCount: 2n },
      ]);

      const delimiterEngine = yield* makeEngine();
      yield* delimiterEngine.publishMany("orders", [
        {
          ...order("1", "open", 10, 1, 'region:string:"emea|x'),
          customerId: 'customer|region:string:"emea',
        },
        {
          ...order("2", "open", 20, 2, "x"),
          customerId: 'customer|region:string:"emea',
        },
      ]);
      const delimiterSnapshot = yield* delimiterEngine.snapshot("orders", {
        groupBy: ["customerId", "region"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [
          { field: "customerId", direction: "asc" },
          { field: "region", direction: "asc" },
        ],
      });
      expect(delimiterSnapshot.totalRows).toBe(2);
      expect(delimiterSnapshot.rows).toStrictEqual([
        {
          customerId: 'customer|region:string:"emea',
          region: 'region:string:"emea|x',
          rowCount: 1n,
        },
        {
          customerId: 'customer|region:string:"emea',
          region: "x",
          rowCount: 1n,
        },
      ]);

      yield* engine.patch("orders", "1", { note: "same" });
      yield* engine.patch("orders", "2", { note: "same" });
      const equalMinMaxSnapshot = yield* engine.snapshot("orders", {
        groupBy: ["status"],
        aggregates: {
          minNote: { aggFunc: "min", field: "note" },
          maxNote: { aggFunc: "max", field: "note" },
        },
        where: {
          status: "open",
        },
      });
      expect(equalMinMaxSnapshot.rows).toStrictEqual([
        { status: "open", minNote: "same", maxNote: "same" },
      ]);

      const emptyNumericQuery: object = {
        groupBy: ["status"],
        aggregates: {
          noteTotal: { aggFunc: "sum", field: "note" },
          averageNote: { aggFunc: "avg", field: "note" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      };
      const nonNumericAggregateError = yield* Effect.flip(
        engine.snapshot(
          "orders",
          // @ts-expect-error hostile runtime callers can still aggregate non-numeric fields.
          emptyNumericQuery,
        ),
      );
      expect(nonNumericAggregateError).toBeInstanceOf(InvalidQueryError);
      expect(nonNumericAggregateError.message).toBe(
        "Grouped query aggregate noteTotal must reference a numeric field.",
      );
    }),
  );

  it.effect("normalizes BigDecimal values for grouped keys and distinct aggregates", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 10n, "1.50"),
        position("2", "AAPL", 20n, "1.5"),
      ]);

      const groupedByPrice = yield* engine.snapshot("positions", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      expect(normalizeDecimalFields(groupedByPrice.rows)).toStrictEqual([
        {
          price: "1.5",
          rowCount: 2n,
        },
      ]);

      const distinctPrice = yield* engine.snapshot("positions", {
        groupBy: ["symbol"],
        aggregates: {
          distinctPrice: { aggFunc: "countDistinct", field: "price" },
        },
      });
      expect(distinctPrice.rows).toStrictEqual([
        {
          symbol: "AAPL",
          distinctPrice: 1n,
        },
      ]);
    }),
  );

  it.effect("evaluates grouped bigint aggregate states", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 10n, "1.00"),
        position("2", "AAPL", 20n, "2.00"),
        position("3", "MSFT", 5n, "3.00"),
      ]);

      const bigintSnapshot = yield* engine.snapshot("positions", {
        groupBy: ["symbol"],
        aggregates: {
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          averageQuantity: { aggFunc: "avg", field: "quantity" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minQuantity: { aggFunc: "min", field: "quantity" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
      });
      expect(normalizeDecimalAndBigIntFields(bigintSnapshot.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "30",
          averageQuantity: "15",
          totalPrice: "3",
          averagePrice: "1.5",
          minQuantity: "10",
          maxQuantity: "20",
        },
        {
          symbol: "MSFT",
          totalQuantity: "5",
          averageQuantity: "5",
          totalPrice: "3",
          averagePrice: "3",
          minQuantity: "5",
          maxQuantity: "5",
        },
      ]);
    }),
  );

  it.effect("evaluates grouped BigDecimal aggregate states", () =>
    Effect.gen(function* () {
      const Mixed = Schema.Struct({
        id: Schema.String,
        group: Schema.String,
        amount: Schema.BigDecimal,
        optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
      });
      const mixedViewServer = defineViewServerConfig({
        topics: {
          mixed: {
            schema: Mixed,
            key: "id",
          },
        },
      });
      const mixedEngine = yield* createColumnLiveViewEngine({
        topics: mixedViewServer.topics,
      });
      yield* mixedEngine.publishMany("mixed", [
        { id: "1", group: "x", amount: fromStringUnsafe("1"), optionalQuantity: 5n },
        { id: "2", group: "x", amount: fromStringUnsafe("2"), optionalQuantity: undefined },
        { id: "3", group: "x", amount: fromStringUnsafe("3.5"), optionalQuantity: 7n },
        { id: "4", group: "y", amount: fromStringUnsafe("0"), optionalQuantity: undefined },
        { id: "5", group: "z", amount: fromStringUnsafe("1"), optionalQuantity: 1n },
        { id: "6", group: "z", amount: fromStringUnsafe("2"), optionalQuantity: 2n },
      ]);
      const mixedQuery = {
        groupBy: ["group"],
        aggregates: {
          totalAmount: { aggFunc: "sum", field: "amount" },
          averageAmount: { aggFunc: "avg", field: "amount" },
        },
      } satisfies GroupedQuery<typeof Mixed.Type>;
      const mixedSnapshot = yield* mixedEngine.snapshot("mixed", mixedQuery);
      expect(normalizeDecimalAndBigIntFields(mixedSnapshot.rows)).toStrictEqual([
        {
          group: "x",
          totalAmount: "6.5",
          averageAmount:
            "2.166666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666667e+0",
        },
        {
          group: "y",
          totalAmount: "0",
          averageAmount: "0",
        },
        {
          group: "z",
          totalAmount: "3",
          averageAmount: "1.5",
        },
      ]);
    }),
  );

  it.effect("keeps non-plain object grouped keys distinct by stable value", () =>
    Effect.gen(function* () {
      const Payload = Schema.Struct({
        id: Schema.String,
        payload: Schema.ObjectKeyword,
      });
      const payloadViewServer = defineViewServerConfig({
        topics: {
          payloads: {
            schema: Payload,
            key: "id",
          },
        },
      });
      const payloadEngine = yield* createColumnLiveViewEngine({
        topics: payloadViewServer.topics,
      });
      yield* payloadEngine.publishMany("payloads", [
        { id: "map-a-1", payload: new Map([["venue", "xnys"]]) },
        { id: "map-b", payload: new Map([["venue", "xlon"]]) },
        { id: "map-a-2", payload: new Map([["venue", "xnys"]]) },
      ]);

      const snapshot = yield* payloadEngine.snapshot("payloads", {
        groupBy: ["payload"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      });

      expect(snapshot.totalRows).toBe(2);
      expect(snapshot.rows).toStrictEqual([
        {
          payload: new Map([["venue", "xnys"]]),
          rowCount: 2n,
        },
        {
          payload: new Map([["venue", "xlon"]]),
          rowCount: 1n,
        },
      ]);
    }),
  );
});
