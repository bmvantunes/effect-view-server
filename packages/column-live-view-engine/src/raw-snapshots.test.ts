import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import {
  instrument,
  instrumentSelect,
  makeEngine,
  order,
  orderSelect,
  position,
  withObjectPrototypeValue,
} from "../test-harness/public-engine";
import { rowIds } from "../test-harness/rows";

describe("ColumnLiveViewEngine raw snapshots", () => {
  it.effect("publishes rows and snapshots a filtered, sorted, windowed raw query", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1, "emea"),
        order("2", "open", 40, 2, "amer"),
        order("3", "closed", 30, 3, "emea"),
        order("4", "open", 20, 4, "emea"),
        order("5", "open", 50, 5, "emea"),
        {
          ...order("6", "open", 15, 4, "emea"),
          customerId: "account-6",
        },
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: orderSelect,
        where: {
          customerId: { startsWith: "customer-" },
          status: "open",
          price: { gte: 10, lt: 50 },
          updatedAt: { lte: 4 },
          region: { eq: "emea" },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 1,
      });

      expect(snapshot).toStrictEqual({
        rows: [order("1", "open", 10, 1, "emea")],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });

      const equalStringSort = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSort.rows)).toStrictEqual(["1", "2", "4", "5", "6"]);

      const reverseInsertEngine = yield* makeEngine();
      yield* reverseInsertEngine.publishMany("orders", [
        order("b", "open", 10, 1),
        order("a", "open", 20, 2),
      ]);
      const equalStringSortReverseInsert = yield* reverseInsertEngine.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "status", direction: "asc" }],
      });
      expect(rowIds(equalStringSortReverseInsert.rows)).toStrictEqual(["a", "b"]);
    }),
  );

  it.effect("returns only selected fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const snapshot = yield* engine.snapshot("orders", {
        select: ["customerId", "status", "updatedAt"],
        where: {
          status: "open",
        },
      });

      expect(snapshot.rows).toStrictEqual([
        {
          customerId: "customer-1",
          status: "open",
          updatedAt: 1,
        },
      ]);
    }),
  );

  it.effect("projects selected scalar fields after slot compaction", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("positions", [
        position("deleted", "AAPL", 1n, "1.25", true),
        position("kept", "MSFT", 2n, "2.50", false),
      ]);
      yield* engine.delete("positions", "deleted");

      const snapshot = yield* engine.snapshot("positions", {
        select: ["id", "symbol", "active", "quantity", "price"],
        where: {
          symbol: "MSFT",
        },
        orderBy: [{ field: "quantity", direction: "desc" }],
        limit: 1,
      });

      expect(snapshot.rows).toStrictEqual([
        {
          active: false,
          id: "kept",
          price: fromStringUnsafe("2.50"),
          quantity: 2n,
          symbol: "MSFT",
        },
      ]);
      expect(snapshot.totalRows).toBe(1);
    }),
  );

  it.effect("returns exact totalRows while windowing a sorted raw snapshot", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("a", "open", 1, 1),
        order("b", "open", 8, 2),
        order("c", "open", 3, 3),
        order("d", "open", 10, 4),
        order("e", "open", 5, 5),
        order("f", "open", 7, 6),
        order("g", "open", 2, 7),
        order("h", "closed", 99, 8),
      ]);

      const windowed = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 2,
        limit: 3,
      });

      expect(windowed.rows).toStrictEqual([
        { id: "f", price: 7 },
        { id: "e", price: 5 },
        { id: "c", price: 3 },
      ]);
      expect(windowed.totalRows).toBe(7);

      const countOnly = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 0,
      });

      expect(countOnly.rows).toStrictEqual([]);
      expect(countOnly.totalRows).toBe(7);

      yield* engine.publish("orders", order("bb", "open", 8, 9));

      const afterTieAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterTieAppend.rows).toStrictEqual([
        { id: "d", price: 10 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
        { id: "f", price: 7 },
      ]);
      expect(afterTieAppend.totalRows).toBe(8);

      yield* engine.publish("orders", order("f", "open", 11, 10));

      const afterReplace = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterReplace.rows).toStrictEqual([
        { id: "f", price: 11 },
        { id: "d", price: 10 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterReplace.totalRows).toBe(8);

      yield* engine.publishMany("orders", [
        order("f", "open", 12, 11),
        order("i", "open", 9, 12),
        order("j", "open", 0, 13),
      ]);

      const afterAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 5,
      });

      expect(afterAppend.rows).toStrictEqual([
        { id: "f", price: 12 },
        { id: "d", price: 10 },
        { id: "i", price: 9 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterAppend.totalRows).toBe(10);

      yield* engine.delete("orders", "d");

      const afterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(afterDelete.rows).toStrictEqual([
        { id: "f", price: 12 },
        { id: "i", price: 9 },
        { id: "b", price: 8 },
        { id: "bb", price: 8 },
      ]);
      expect(afterDelete.totalRows).toBe(9);
    }),
  );

  it.effect("uses ordered range seeks for raw snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("orders", [
        order("a", "open", 1, 1),
        order("b", "open", 2, 2),
        order("c", "open", 3, 3),
        order("d", "open", 4, 4),
        order("e", "open", 5, 5),
        order("f", "open", 6, 6),
        order("g", "open", 7, 7),
        order("h", "open", 8, 8),
        order("z", "closed", 99, 9),
      ]);

      const ascendingInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 3 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 1,
        limit: 2,
      });

      expect(ascendingInclusive.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
      ]);
      expect(ascendingInclusive.totalRows).toBe(7);

      const scalarFilteredOrderedWindow = yield* engine.snapshot("orders", {
        select: ["id", "price", "status"],
        where: {
          status: "closed",
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredOrderedWindow.rows).toStrictEqual([
        { id: "z", price: 99, status: "closed" },
      ]);
      expect(scalarFilteredOrderedWindow.totalRows).toBe(1);

      const scalarFilteredEmptyOrderedWindow = yield* engine.snapshot("orders", {
        select: ["id", "customerId", "price"],
        where: {
          customerId: "missing-customer",
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredEmptyOrderedWindow.rows).toStrictEqual([]);
      expect(scalarFilteredEmptyOrderedWindow.totalRows).toBe(0);

      const numericEquality = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(numericEquality.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(numericEquality.totalRows).toBe(1);

      const numericInAscending = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [8, 2, 99, 2] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(numericInAscending.rows).toStrictEqual([
        { id: "b", price: 2 },
        { id: "h", price: 8 },
        { id: "z", price: 99 },
      ]);
      expect(numericInAscending.totalRows).toBe(3);

      const numericInDescending = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [8, 2, 99, 2] },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInDescending.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInDescending.totalRows).toBe(3);

      const emptyIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(emptyIn.rows).toStrictEqual([]);
      expect(emptyIn.totalRows).toBe(0);

      const stringEquality = yield* engine.snapshot("orders", {
        select: ["id", "status"],
        where: {
          status: { eq: "closed" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });

      expect(stringEquality.rows).toStrictEqual([{ id: "z", status: "closed" }]);
      expect(stringEquality.totalRows).toBe(1);

      const stringIn = yield* engine.snapshot("orders", {
        select: ["id", "customerId"],
        where: {
          customerId: { in: ["customer-h", "customer-b", "customer-z", "customer-b"] },
        },
        orderBy: [{ field: "customerId", direction: "asc" }],
        limit: 10,
      });

      expect(stringIn.rows).toStrictEqual([
        { id: "b", customerId: "customer-b" },
        { id: "h", customerId: "customer-h" },
        { id: "z", customerId: "customer-z" },
      ]);
      expect(stringIn.totalRows).toBe(3);

      const numericInWithRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { in: [2, 5, 8, 99, 2], gte: 5, lt: 99 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInWithRange.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInWithRange.totalRows).toBe(2);

      const equalityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, in: [2, 5, 8] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityInIntersection.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(equalityInIntersection.totalRows).toBe(1);

      const contradictoryEqualityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, in: [2, 8] },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(contradictoryEqualityInIntersection.rows).toStrictEqual([]);
      expect(contradictoryEqualityInIntersection.totalRows).toBe(0);

      const equalityOutsideRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 5, gt: 5 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityOutsideRange.rows).toStrictEqual([]);
      expect(equalityOutsideRange.totalRows).toBe(0);

      const ascendingExclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3, lt: 7 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(ascendingExclusive.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
        { id: "f", price: 6 },
      ]);
      expect(ascendingExclusive.totalRows).toBe(3);

      const strongerDifferentBounds = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3, gte: 4, lt: 8, lte: 6 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(strongerDifferentBounds.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
        { id: "f", price: 6 },
      ]);
      expect(strongerDifferentBounds.totalRows).toBe(3);

      const strongerEqualBounds = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 3, gt: 3, lte: 6, lt: 6 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(strongerEqualBounds.rows).toStrictEqual([
        { id: "d", price: 4 },
        { id: "e", price: 5 },
      ]);
      expect(strongerEqualBounds.totalRows).toBe(2);

      const descendingInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { lte: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 2,
      });

      expect(descendingInclusive.rows).toStrictEqual([
        { id: "e", price: 5 },
        { id: "d", price: 4 },
      ]);
      expect(descendingInclusive.totalRows).toBe(6);

      const descendingExclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { lt: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 2,
      });

      expect(descendingExclusive.rows).toStrictEqual([
        { id: "e", price: 5 },
        { id: "d", price: 4 },
      ]);
      expect(descendingExclusive.totalRows).toBe(5);

      const descendingLowerBound = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 3 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 2,
      });

      expect(descendingLowerBound.rows).toStrictEqual([
        { id: "z", price: 99 },
        { id: "h", price: 8 },
      ]);
      expect(descendingLowerBound.totalRows).toBe(6);

      const descendingLowerInclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 6 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 4,
      });

      expect(descendingLowerInclusive.rows).toStrictEqual([
        { id: "z", price: 99 },
        { id: "h", price: 8 },
        { id: "g", price: 7 },
        { id: "f", price: 6 },
      ]);
      expect(descendingLowerInclusive.totalRows).toBe(4);

      const exactInclusiveRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 4, lte: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 2,
      });

      expect(exactInclusiveRange.rows).toStrictEqual([{ id: "d", price: 4 }]);
      expect(exactInclusiveRange.totalRows).toBe(1);

      const impossibleRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 4, lte: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(impossibleRange.rows).toStrictEqual([]);
      expect(impossibleRange.totalRows).toBe(0);

      const invertedRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gt: 7, lt: 4 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(invertedRange.rows).toStrictEqual([]);
      expect(invertedRange.totalRows).toBe(0);

      const nonOrderFieldRange = yield* engine.snapshot("orders", {
        select: ["id", "price", "updatedAt"],
        where: {
          price: { gte: 4 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        limit: 2,
      });

      expect(nonOrderFieldRange.rows).toStrictEqual([
        { id: "z", price: 99, updatedAt: 9 },
        { id: "h", price: 8, updatedAt: 8 },
      ]);
      expect(nonOrderFieldRange.totalRows).toBe(6);

      yield* engine.publish("orders", order("hh", "open", 8, 10));

      const duplicateEqualityValue = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { eq: 8 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(duplicateEqualityValue.rows).toStrictEqual([
        { id: "h", price: 8 },
        { id: "hh", price: 8 },
      ]);
      expect(duplicateEqualityValue.totalRows).toBe(2);

      yield* engine.publish("orders", order("i", "open", 9, 11));

      const afterAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          price: { gte: 8 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });

      expect(afterAppend.rows).toStrictEqual([
        { id: "h", price: 8 },
        { id: "hh", price: 8 },
        { id: "i", price: 9 },
      ]);
      expect(afterAppend.totalRows).toBe(4);
    }),
  );

  it.effect("keeps exact predicate candidate indexes current after row mutations", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("a", "open", 10, 1),
        order("b", "closed", 20, 2),
        order("c", "open", 30, 3),
      ]);

      const initial = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(initial.rows).toStrictEqual([{ id: "a" }, { id: "c" }]);
      expect(initial.totalRows).toBe(2);

      yield* engine.publish("orders", order("a", "closed", 10, 4));
      yield* engine.delete("orders", "b");

      const afterUpdateAndSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterUpdateAndSlotSwapDelete.rows).toStrictEqual([{ id: "c" }]);
      expect(afterUpdateAndSlotSwapDelete.totalRows).toBe(1);

      yield* engine.publish("orders", order("d", "open", 40, 5));
      yield* engine.delete("orders", "c");

      const afterInsertAndSecondSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterInsertAndSecondSlotSwapDelete.rows).toStrictEqual([{ id: "d" }]);
      expect(afterInsertAndSecondSlotSwapDelete.totalRows).toBe(1);

      yield* engine.reset();

      const afterReset = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterReset.rows).toStrictEqual([]);
      expect(afterReset.totalRows).toBe(0);
    }),
  );

  it.effect("does not expose stored row objects through snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const snapshot = yield* engine.snapshot("orders", { select: orderSelect });
      expect(snapshot.rows).toHaveLength(1);
      Object.assign(snapshot.rows[0]!, { price: 999 });

      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.rows).toStrictEqual([order("1", "open", 10, 1)]);
    }),
  );

  it.effect("deep-clones nested rows and supports object-valued equality filters", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const emptyStructuredQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
        },
      });
      expect(emptyStructuredQuery.rows).toStrictEqual([]);

      yield* engine.publishMany("instruments", [
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const metadataQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
        },
      });
      expect(rowIds(metadataQuery.rows)).toStrictEqual(["1"]);

      const arrayQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          tags: ["equity", "us"],
        },
      });
      expect(rowIds(arrayQuery.rows)).toStrictEqual(["1"]);

      const operatorObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          metadata: {
            eq: {
              venue: "xlon",
              risk: {
                tier: 2,
                lot: 2n,
              },
            },
          },
        },
      });
      expect(rowIds(operatorObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeDirectObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            eq: "xnys",
          },
        },
      });
      expect(rowIds(operatorLikeDirectObjectQuery.rows)).toStrictEqual(["1"]);

      const operatorRangeLikeDirectObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorRangeLike: {
            gte: 2,
          },
        },
      });
      expect(rowIds(operatorRangeLikeDirectObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeWrappedObjectQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            eq: {
              eq: "xlon",
            },
          },
        },
      });
      expect(rowIds(operatorLikeWrappedObjectQuery.rows)).toStrictEqual(["2"]);

      const operatorLikeObjectNeq = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            neq: {
              eq: "not-present",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeq.rows)).toStrictEqual(["1", "2"]);

      const operatorLikeObjectNeqEqual = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            neq: {
              eq: "xnys",
            },
          },
        },
      });
      expect(rowIds(operatorLikeObjectNeqEqual.rows)).toStrictEqual(["2"]);

      const objectInQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: {
          operatorLike: {
            in: [
              {
                eq: "xlon",
              },
            ],
          },
        },
      });
      expect(rowIds(objectInQuery.rows)).toStrictEqual(["2"]);

      const invalidObjectRuntimeQuery: object = {
        select: ["id"],
        where: {
          operatorLike: { in: [undefined] },
        },
      };
      // @ts-expect-error runtime validation handles hostile untyped structured filters.
      const invalidObjectInQuery = yield* engine.snapshot("instruments", invalidObjectRuntimeQuery);
      expect(rowIds(invalidObjectInQuery.rows)).toStrictEqual([]);

      const fullSnapshot = yield* engine.snapshot("instruments", {
        select: ["id", "metadata", "tags"],
      });
      expect(fullSnapshot.rows).toHaveLength(2);
      Object.assign(Object(fullSnapshot.rows[0]).metadata.risk, { tier: 999 });
      Object(fullSnapshot.rows[0]).tags.push("mutated");

      const projectedSnapshot = yield* engine.snapshot("instruments", {
        select: ["metadata", "tags"],
        where: {
          id: "1",
        },
      });
      expect(projectedSnapshot.rows).toStrictEqual([
        {
          metadata: {
            venue: "xnys",
            risk: {
              tier: 1,
              lot: 1n,
            },
          },
          tags: ["equity", "us"],
        },
      ]);
      Object.assign(Object(projectedSnapshot.rows[0]).metadata.risk, { tier: 777 });
      Object(projectedSnapshot.rows[0]).tags.push("projected-mutation");

      const fresh = yield* engine.snapshot("instruments", {
        select: instrumentSelect,
        where: {
          id: "1",
        },
      });
      expect(fresh.rows).toStrictEqual([instrument("1", "xnys", 1, ["equity", "us"])]);
    }),
  );

  it.effect("does not retain nested publish or patch input references", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const original = instrument("1", "xnys", 1, ["equity", "us"]);
      yield* engine.publish("instruments", original);

      Object.assign(original.metadata.risk, { tier: 999 });
      Object(original.tags).push("mutated-after-publish");

      const afterPublishMutation = yield* engine.snapshot("instruments", {
        select: instrumentSelect,
      });
      expect(afterPublishMutation.rows).toStrictEqual([
        instrument("1", "xnys", 1, ["equity", "us"]),
      ]);

      const patch = {
        metadata: {
          venue: "xlon",
          risk: {
            tier: 2,
            lot: 2n,
          },
        },
        operatorLike: {
          eq: "xlon",
        },
        operatorRangeLike: {
          gte: 2,
        },
        tags: ["equity", "uk"],
      };
      yield* engine.patch("instruments", "1", patch);

      patch.metadata.risk.tier = 777;
      patch.operatorLike.eq = "mutated-after-patch";
      patch.operatorRangeLike.gte = 777;
      patch.tags.push("mutated-after-patch");

      const afterPatchMutation = yield* engine.snapshot("instruments", {
        select: instrumentSelect,
      });
      expect(afterPatchMutation.rows).toStrictEqual([instrument("1", "xlon", 2, ["equity", "uk"])]);
    }),
  );

  it.effect("supports bigint and BigDecimal raw comparison semantics", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      yield* engine.publishMany("positions", [
        {
          id: "position-1",
          accountId: "account-1",
          symbol: "AAPL",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("3.00"),
        },
        {
          id: "position-2",
          accountId: "account-1",
          symbol: "MSFT",
          active: false,
          quantity: 20n,
          price: fromStringUnsafe("2.00"),
        },
        {
          id: "position-3",
          accountId: "account-1",
          symbol: "TSLA",
          active: true,
          quantity: 9n,
          price: fromStringUnsafe("1.00"),
        },
        {
          id: "position-4",
          accountId: "account-1",
          symbol: "NVDA",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("1.00"),
        },
      ]);

      const snapshot = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          quantity: { gt: 9n },
          price: { gte: fromStringUnsafe("2.00") },
        },
        orderBy: [
          { field: "active", direction: "asc" },
          { field: "price", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["position-2", "position-1"]);

      const fallbackOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "active", direction: "asc" }],
      });
      expect(rowIds(fallbackOrdered.rows)).toStrictEqual([
        "position-2",
        "position-1",
        "position-3",
        "position-4",
      ]);

      const symbolOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "symbol", direction: "desc" }],
        where: {
          price: { eq: fromStringUnsafe("1.00") },
        },
      });
      expect(rowIds(symbolOrdered.rows)).toStrictEqual(["position-3", "position-4"]);

      const decimalEqualityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          price: { eq: fromStringUnsafe("1.00") },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      expect(rowIds(decimalEqualityOrdered.rows)).toStrictEqual(["position-3", "position-4"]);
      expect(decimalEqualityOrdered.totalRows).toBe(2);

      const quantityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "quantity", direction: "asc" }],
      });
      expect(rowIds(quantityOrdered.rows)).toStrictEqual([
        "position-3",
        "position-1",
        "position-4",
        "position-2",
      ]);

      const bigintEqualityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          quantity: { eq: 10n },
        },
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      expect(rowIds(bigintEqualityOrdered.rows)).toStrictEqual(["position-1", "position-4"]);
      expect(bigintEqualityOrdered.totalRows).toBe(2);

      const booleanNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          active: { neq: false },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(booleanNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);

      const decimalNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          price: { neq: fromStringUnsafe("2.00") },
        },
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(decimalNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);
    }),
  );

  it.effect("sorts object, array, and missing values deterministically", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("instruments", [
        instrument("3", "xnas", 3, ["bond"]),
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const objectOrdered = yield* engine.snapshot("instruments", {
        select: ["id"],
        orderBy: [{ field: "metadata", direction: "asc" }],
      });
      expect(rowIds(objectOrdered.rows)).toStrictEqual(["1", "2", "3"]);

      const arrayOrdered = yield* engine.snapshot("instruments", {
        select: ["id"],
        orderBy: [{ field: "tags", direction: "desc" }],
      });
      expect(rowIds(arrayOrdered.rows)).toStrictEqual(["1", "2", "3"]);

      yield* engine.publish("orders", order("1", "open", 10, 1));
      yield* engine.publish("orders", { ...order("2", "open", 20, 2), note: "visible" });
      yield* engine.publish("orders", order("3", "open", 30, 3));
      const missingOrdered = yield* engine.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "note", direction: "asc" }],
      });
      expect(rowIds(missingOrdered.rows)).toStrictEqual(["1", "3", "2"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal ascending sort select",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          select: ["id"],
          orderBy: [{ field: "price", direction: "asc" }],
        });

        expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key as the default order without explicit sort select", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "open", 30, 3),
        order("a", "closed", 10, 1),
        order("b", "cancelled", 20, 2),
      ]);

      const snapshot = yield* engine.snapshot("orders", { select: ["id"] });

      expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
    }),
  );

  it.effect(
    "uses the configured row key as the final tiebreaker for equal descending sort select",
    () =>
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("c", "open", 10, 1),
          order("a", "open", 10, 1),
          order("b", "open", 10, 1),
        ]);

        const snapshot = yield* engine.snapshot("orders", {
          select: ["id"],
          orderBy: [{ field: "price", direction: "desc" }],
        });

        expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
      }),
  );

  it.effect("uses the configured row key after all sort select compare equal", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "closed", 10, 1, "emea"),
        order("a", "closed", 10, 1, "emea"),
        order("b", "closed", 10, 1, "emea"),
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "status", direction: "asc" },
          { field: "region", direction: "desc" },
          { field: "updatedAt", direction: "asc" },
        ],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["a", "b", "c"]);
    }),
  );

  it.effect("exercises raw filter exclusion branches through public snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        { ...order("1", "open", 10, 1, "emea"), customerId: "account-1" },
        order("2", "open", 9, 1, "emea"),
        order("3", "open", 10, 0, "emea"),
        order("4", "cancelled", 10, 1, "emea"),
        order("5", "open", 10, 1, "amer"),
        order("6", "open", 10, 5, "emea"),
        order("7", "open", 10, 1, "emea"),
      ]);

      const allRows = yield* engine.snapshot("orders", { select: ["id"] });
      expect(allRows.totalRows).toBe(7);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          customerId: { startsWith: "customer-" },
          price: { gt: 9 },
          updatedAt: { gte: 1, lte: 4 },
          status: { in: ["open"] },
          region: { eq: "emea" },
        },
      });
      expect(rowIds(snapshot.rows)).toStrictEqual(["7"]);

      const notOpen = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          status: { neq: "open" },
        },
      });
      expect(rowIds(notOpen.rows)).toStrictEqual(["4"]);
    }),
  );

  it.effect("keeps column slot values in sync across replace, delete, reuse, and patch", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "closed", 10, 1),
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);

      yield* engine.publish("orders", order("1", "open", 40, 4));

      const afterReplace = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterReplace.rows).toStrictEqual([{ id: "1", price: 40 }]);
      expect(afterReplace.totalRows).toBe(1);

      yield* engine.delete("orders", "1");

      const afterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterDelete.rows).toStrictEqual([]);
      expect(afterDelete.totalRows).toBe(0);

      const movedRowAfterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { lt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(movedRowAfterDelete.rows).toStrictEqual([
        { id: "3", price: 30 },
        { id: "2", price: 20 },
      ]);
      expect(movedRowAfterDelete.totalRows).toBe(2);

      yield* engine.publish("orders", order("4", "open", 50, 5));

      const afterSlotReuse = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterSlotReuse.rows).toStrictEqual([{ id: "4", price: 50 }]);
      expect(afterSlotReuse.totalRows).toBe(1);

      yield* engine.patch("orders", "4", { status: "closed", price: 5 });

      const afterPatchOut = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterPatchOut.rows).toStrictEqual([]);
      expect(afterPatchOut.totalRows).toBe(0);

      yield* engine.patch("orders", "4", { status: "open", price: 45 });

      const afterPatchIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
          price: { gt: 35 },
        },
      });
      expect(afterPatchIn.rows).toStrictEqual([{ id: "4", price: 45 }]);
      expect(afterPatchIn.totalRows).toBe(1);
    }),
  );

  it.effect("uses bigint column range narrowing for less-than filters", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("positions", [
        position("1", "AAPL", 5n, "10"),
        position("2", "MSFT", 15n, "20"),
      ]);

      const snapshot = yield* engine.snapshot("positions", {
        select: ["id"],
        where: {
          quantity: { lt: 10n },
        },
      });

      expect(snapshot.rows).toStrictEqual([{ id: "1" }]);
      expect(snapshot.totalRows).toBe(1);
    }),
  );

  it.effect("keeps optional not-equal semantics aligned with public raw snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("missing-note", "open", 10, 1),
        { ...order("matched-note", "open", 20, 2), note: "hello" },
        { ...order("other-note", "open", 30, 3), note: "bye" },
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          note: { neq: "bye" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["matched-note"]);
      expect(snapshot.totalRows).toBe(1);
    }),
  );

  it.effect("keeps missing optional fields isolated from object prototype values", () =>
    withObjectPrototypeValue(
      "note",
      "polluted",
      Effect.gen(function* () {
        const engine = yield* makeEngine();
        yield* engine.publishMany("orders", [
          order("missing-note", "open", 10, 1),
          { ...order("own-note", "open", 20, 2), note: "polluted" },
        ]);

        const projected = yield* engine.snapshot("orders", {
          select: ["id", "note"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const polluted = yield* engine.snapshot("orders", {
          select: ["id"],
          where: {
            note: { eq: "polluted" },
          },
          orderBy: [{ field: "id", direction: "asc" }],
        });

        expect(projected.rows).toStrictEqual([
          { id: "missing-note", note: undefined },
          { id: "own-note", note: "polluted" },
        ]);
        expect(rowIds(polluted.rows)).toStrictEqual(["own-note"]);
      }),
    ),
  );
});
