import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { fromStringUnsafe, make as makeBigDecimal } from "effect/BigDecimal";
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
        where: [
          { field: "customerId", type: "startsWith", filter: "customer-" },
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThanOrEqual", filter: 10 },
          { field: "price", type: "lessThan", filter: 50 },
          { field: "updatedAt", type: "lessThanOrEqual", filter: 4 },
          { field: "region", type: "equals", filter: "emea" },
        ],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "symbol", type: "equals", filter: "MSFT" }],
        orderBy: [{ field: "quantity", direction: "desc" }],
        limit: 1,
      });

      expect(snapshot.rows).toStrictEqual([
        {
          active: false,
          id: "kept",
          price: fromStringUnsafe("2.5"),
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 0,
      });

      expect(countOnly.rows).toStrictEqual([]);
      expect(countOnly.totalRows).toBe(7);

      yield* engine.publish("orders", order("bb", "open", 8, 9));

      const afterTieAppend = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
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
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 3 }],
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
        where: [{ field: "status", type: "equals", filter: "closed" }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredOrderedWindow.rows).toStrictEqual([
        { id: "z", price: 99, status: "closed" },
      ]);
      expect(scalarFilteredOrderedWindow.totalRows).toBe(1);

      const scalarFilteredEmptyOrderedWindow = yield* engine.snapshot("orders", {
        select: ["id", "customerId", "price"],
        where: [{ field: "customerId", type: "equals", filter: "missing-customer" }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });

      expect(scalarFilteredEmptyOrderedWindow.rows).toStrictEqual([]);
      expect(scalarFilteredEmptyOrderedWindow.totalRows).toBe(0);

      const numericEquality = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [{ field: "price", type: "equals", filter: 5 }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(numericEquality.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(numericEquality.totalRows).toBe(1);

      const numericInAscending = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [{ field: "price", type: "in", filter: [8, 2, 99, 2] }],
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
        where: [{ field: "price", type: "in", filter: [8, 2, 99, 2] }],
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInDescending.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInDescending.totalRows).toBe(3);

      const emptyIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [{ field: "price", type: "in", filter: [] }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(emptyIn.rows).toStrictEqual([
        { id: "a", price: 1 },
        { id: "b", price: 2 },
        { id: "c", price: 3 },
        { id: "d", price: 4 },
        { id: "e", price: 5 },
        { id: "f", price: 6 },
        { id: "g", price: 7 },
        { id: "h", price: 8 },
        { id: "z", price: 99 },
      ]);
      expect(emptyIn.totalRows).toBe(9);

      const stringEquality = yield* engine.snapshot("orders", {
        select: ["id", "status"],
        where: [{ field: "status", type: "equals", filter: "closed" }],
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });

      expect(stringEquality.rows).toStrictEqual([{ id: "z", status: "closed" }]);
      expect(stringEquality.totalRows).toBe(1);

      const stringIn = yield* engine.snapshot("orders", {
        select: ["id", "customerId"],
        where: [
          {
            field: "customerId",
            type: "in",
            filter: ["customer-h", "customer-b", "customer-z", "customer-b"],
          },
        ],
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
        where: [
          { field: "price", type: "in", filter: [2, 5, 8, 99, 2] },
          { field: "price", type: "greaterThanOrEqual", filter: 5 },
          { field: "price", type: "lessThan", filter: 99 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 1,
        limit: 1,
      });

      expect(numericInWithRange.rows).toStrictEqual([{ id: "h", price: 8 }]);
      expect(numericInWithRange.totalRows).toBe(2);

      const equalityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "equals", filter: 5 },
          { field: "price", type: "in", filter: [2, 5, 8] },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityInIntersection.rows).toStrictEqual([{ id: "e", price: 5 }]);
      expect(equalityInIntersection.totalRows).toBe(1);

      const contradictoryEqualityInIntersection = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "equals", filter: 5 },
          { field: "price", type: "in", filter: [2, 8] },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(contradictoryEqualityInIntersection.rows).toStrictEqual([]);
      expect(contradictoryEqualityInIntersection.totalRows).toBe(0);

      const equalityOutsideRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "equals", filter: 5 },
          { field: "price", type: "greaterThan", filter: 5 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      expect(equalityOutsideRange.rows).toStrictEqual([]);
      expect(equalityOutsideRange.totalRows).toBe(0);

      const ascendingExclusive = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "greaterThan", filter: 3 },
          { field: "price", type: "lessThan", filter: 7 },
        ],
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
        where: [
          { field: "price", type: "greaterThan", filter: 3 },
          { field: "price", type: "greaterThanOrEqual", filter: 4 },
          { field: "price", type: "lessThan", filter: 8 },
          { field: "price", type: "lessThanOrEqual", filter: 6 },
        ],
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
        where: [
          { field: "price", type: "greaterThanOrEqual", filter: 3 },
          { field: "price", type: "greaterThan", filter: 3 },
          { field: "price", type: "lessThanOrEqual", filter: 6 },
          { field: "price", type: "lessThan", filter: 6 },
        ],
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
        where: [{ field: "price", type: "lessThanOrEqual", filter: 6 }],
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
        where: [{ field: "price", type: "lessThan", filter: 6 }],
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
        where: [{ field: "price", type: "greaterThan", filter: 3 }],
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
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 6 }],
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
        where: [
          { field: "price", type: "greaterThanOrEqual", filter: 4 },
          { field: "price", type: "lessThanOrEqual", filter: 4 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 2,
      });

      expect(exactInclusiveRange.rows).toStrictEqual([{ id: "d", price: 4 }]);
      expect(exactInclusiveRange.totalRows).toBe(1);

      const impossibleRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "greaterThan", filter: 4 },
          { field: "price", type: "lessThanOrEqual", filter: 4 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(impossibleRange.rows).toStrictEqual([]);
      expect(impossibleRange.totalRows).toBe(0);

      const invertedRange = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "price", type: "greaterThan", filter: 7 },
          { field: "price", type: "lessThan", filter: 4 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 5,
      });

      expect(invertedRange.rows).toStrictEqual([]);
      expect(invertedRange.totalRows).toBe(0);

      const nonOrderFieldRange = yield* engine.snapshot("orders", {
        select: ["id", "price", "updatedAt"],
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 4 }],
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
        where: [{ field: "price", type: "equals", filter: 8 }],
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
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 8 }],
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
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(initial.rows).toStrictEqual([{ id: "a" }, { id: "c" }]);
      expect(initial.totalRows).toBe(2);

      yield* engine.publish("orders", order("a", "closed", 10, 4));
      yield* engine.delete("orders", "b");

      const afterUpdateAndSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterUpdateAndSlotSwapDelete.rows).toStrictEqual([{ id: "c" }]);
      expect(afterUpdateAndSlotSwapDelete.totalRows).toBe(1);

      yield* engine.publish("orders", order("d", "open", 40, 5));
      yield* engine.delete("orders", "c");

      const afterInsertAndSecondSlotSwapDelete = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(afterInsertAndSecondSlotSwapDelete.rows).toStrictEqual([{ id: "d" }]);
      expect(afterInsertAndSecondSlotSwapDelete.totalRows).toBe(1);

      yield* engine.reset();

      const afterReset = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ field: "status", type: "equals", filter: "open" }],
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

  it.effect("deep-clones nested rows and filters statically named scalar paths", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const emptyNestedQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: [{ field: "metadata.venue", type: "equals", filter: "xnys" }],
      });
      expect(emptyNestedQuery.rows).toStrictEqual([]);

      yield* engine.publishMany("instruments", [
        instrument("1", "xnys", 1, ["equity", "us"]),
        instrument("2", "xlon", 2, ["equity", "uk"]),
      ]);

      const metadataQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: [
          { field: "metadata.venue", type: "equals", filter: "xnys" },
          { field: "metadata.risk.tier", type: "equals", filter: 1 },
          { field: "metadata.risk.lot", type: "equals", filter: 1n },
        ],
      });
      expect(rowIds(metadataQuery.rows)).toStrictEqual(["1"]);

      const operatorLikeLeafQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: [{ field: "operatorLike.eq", type: "equals", filter: "xnys" }],
      });
      expect(rowIds(operatorLikeLeafQuery.rows)).toStrictEqual(["1"]);

      const operatorRangeLikeLeafQuery = yield* engine.snapshot("instruments", {
        select: ["id"],
        where: [{ field: "operatorRangeLike.gte", type: "greaterThanOrEqual", filter: 2 }],
      });
      expect(rowIds(operatorRangeLikeLeafQuery.rows)).toStrictEqual(["2"]);

      const fullSnapshot = yield* engine.snapshot("instruments", {
        select: ["id", "metadata", "tags"],
      });
      expect(fullSnapshot.rows).toHaveLength(2);
      Object.assign(Object(fullSnapshot.rows[0]).metadata.risk, { tier: 999 });
      Object(fullSnapshot.rows[0]).tags.push("mutated");

      const projectedSnapshot = yield* engine.snapshot("instruments", {
        select: ["metadata", "tags"],
        where: [{ field: "id", type: "equals", filter: "1" }],
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
        where: [{ field: "id", type: "equals", filter: "1" }],
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
        where: [
          { field: "quantity", type: "greaterThan", filter: 9n },
          { field: "price", type: "greaterThanOrEqual", filter: fromStringUnsafe("2.00") },
        ],
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
        where: [{ field: "price", type: "equals", filter: fromStringUnsafe("1.00") }],
      });
      expect(rowIds(symbolOrdered.rows)).toStrictEqual(["position-3", "position-4"]);

      const decimalEqualityOrdered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: fromStringUnsafe("1.00") }],
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
        where: [{ field: "quantity", type: "equals", filter: 10n }],
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      expect(rowIds(bigintEqualityOrdered.rows)).toStrictEqual(["position-1", "position-4"]);
      expect(bigintEqualityOrdered.totalRows).toBe(2);

      const booleanNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: [{ field: "active", type: "notEqual", filter: false }],
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(booleanNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);

      const decimalNotEqual = yield* engine.snapshot("positions", {
        select: ["id"],
        where: [{ field: "price", type: "notEqual", filter: fromStringUnsafe("2.00") }],
        orderBy: [{ field: "symbol", direction: "asc" }],
      });
      expect(rowIds(decimalNotEqual.rows)).toStrictEqual([
        "position-1",
        "position-4",
        "position-3",
      ]);
    }),
  );

  it.effect("filters and orders extreme-scale BigDecimals without scale alignment", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const tiny = makeBigDecimal(1n, Number.MAX_SAFE_INTEGER);
      const middle = makeBigDecimal(5n, Number.MAX_SAFE_INTEGER);
      const upper = makeBigDecimal(1n, Number.MAX_SAFE_INTEGER - 1);
      const huge = makeBigDecimal(1n, Number.MIN_SAFE_INTEGER);
      yield* engine.publishMany("positions", [
        { ...position("huge", "HUGE", 4n, "1"), price: huge },
        { ...position("upper", "UPPER", 3n, "1"), price: upper },
        { ...position("tiny", "TINY", 1n, "1"), price: tiny },
        { ...position("middle", "MIDDLE", 2n, "1"), price: middle },
      ]);

      const filtered = yield* engine.snapshot("positions", {
        select: ["id"],
        where: [{ field: "price", type: "inRange", filter: tiny, filterTo: upper }],
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const indexed = yield* engine.snapshot("positions", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 4,
      });

      expect(rowIds(filtered.rows)).toStrictEqual(["tiny", "middle"]);
      expect(rowIds(indexed.rows)).toStrictEqual(["tiny", "middle", "upper", "huge"]);
      yield* engine.close();
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
        where: [
          { field: "customerId", type: "startsWith", filter: "customer-" },
          { field: "price", type: "greaterThan", filter: 9 },
          { field: "updatedAt", type: "greaterThanOrEqual", filter: 1 },
          { field: "updatedAt", type: "lessThanOrEqual", filter: 4 },
          { field: "status", type: "in", filter: ["open"] },
          { field: "region", type: "equals", filter: "emea" },
        ],
      });
      expect(rowIds(snapshot.rows)).toStrictEqual(["7"]);

      const notOpen = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ field: "status", type: "notEqual", filter: "open" }],
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
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThan", filter: 35 },
        ],
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterReplace.rows).toStrictEqual([{ id: "1", price: 40 }]);
      expect(afterReplace.totalRows).toBe(1);

      yield* engine.delete("orders", "1");

      const afterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThan", filter: 35 },
        ],
      });
      expect(afterDelete.rows).toStrictEqual([]);
      expect(afterDelete.totalRows).toBe(0);

      const movedRowAfterDelete = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "lessThan", filter: 35 },
        ],
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
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThan", filter: 35 },
        ],
        orderBy: [{ field: "price", direction: "desc" }],
      });
      expect(afterSlotReuse.rows).toStrictEqual([{ id: "4", price: 50 }]);
      expect(afterSlotReuse.totalRows).toBe(1);

      yield* engine.patch("orders", "4", { status: "closed", price: 5 });

      const afterPatchOut = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThan", filter: 35 },
        ],
      });
      expect(afterPatchOut.rows).toStrictEqual([]);
      expect(afterPatchOut.totalRows).toBe(0);

      yield* engine.patch("orders", "4", { status: "open", price: 45 });

      const afterPatchIn = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "price", type: "greaterThan", filter: 35 },
        ],
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
        where: [{ field: "quantity", type: "lessThan", filter: 10n }],
      });

      expect(snapshot.rows).toStrictEqual([{ id: "1" }]);
      expect(snapshot.totalRows).toBe(1);
    }),
  );

  it.effect("treats notEqual as the exact complement of equals for optional fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("missing-note", "open", 10, 1),
        { ...order("matched-note", "open", 20, 2), note: "hello" },
        { ...order("other-note", "open", 30, 3), note: "bye" },
      ]);

      const snapshot = yield* engine.snapshot("orders", {
        select: ["id"],
        where: [{ field: "note", type: "notEqual", filter: "bye" }],
        orderBy: [{ field: "id", direction: "asc" }],
      });

      expect(rowIds(snapshot.rows)).toStrictEqual(["matched-note", "missing-note"]);
      expect(snapshot.totalRows).toBe(2);
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
          where: [{ field: "note", type: "equals", filter: "polluted" }],
          orderBy: [{ field: "id", direction: "asc" }],
        });

        expect(projected.rows).toStrictEqual([
          { id: "missing-note" },
          { id: "own-note", note: "polluted" },
        ]);
        expect(rowIds(polluted.rows)).toStrictEqual(["own-note"]);
      }),
    ),
  );
});
