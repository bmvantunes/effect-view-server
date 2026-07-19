import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import { InvalidRowError } from "./index";
import { TopicRowStorage } from "./topic-row-storage";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import { fieldValue } from "./row-values";
import type { TopicRawOrderByPlan } from "./raw-window-scan";
import {
  deleteTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  TopicStore,
} from "./topic-store";
import { topicStoreTestQueryInterface } from "../test-harness/topic-store";
import { createTopicColumnValuesFromArray } from "./topic-column-vector";
import { rawWindowOrderedSlotIndex } from "./topic-raw-ordered-window-index";
import {
  distinctOrderedEqualityValues,
  orderedEqualityValuesForField,
  orderedSlotBoundIndex,
} from "./topic-ordered-window";
import { numericRowField } from "../test-harness/columns";
import { expectDefined } from "../test-harness/events";
import { makeEngine, order, Order, position, Position } from "../test-harness/public-engine";
import { rowField, rowIds } from "../test-harness/rows";

describe("Topic Store ordered-window execution", () => {
  it.effect("scans only existing owned storage keys and supports early termination", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 10, 1), invalidRow));
      storage.setPrepared(yield* storage.prepareRow(order("b", "open", 20, 2), invalidRow));

      const visited: Array<string> = [];
      storage.scanRowsByStorageKeys(["missing", "a", "b"], (key) => {
        visited.push(key);
        return false;
      });

      expect(visited).toStrictEqual(["a"]);
    }),
  );

  it.effect("uses owned storage-key candidates for raw windows and zero-limit counts", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 10, 1), invalidRow));
      storage.setPrepared(yield* storage.prepareRow(order("b", "open", 20, 2), invalidRow));
      const basePlan = {
        candidateStorageKeys: () => ["missing", "b", "a"],
        predicate: {
          callbackRequired: true,
          callbackSkippable: false,
          filters: [],
        },
        orderBy: [],
        matches: () => true,
        compare: (
          left: { readonly key: string; readonly row: object },
          right: { readonly key: string; readonly row: object },
        ) => left.key.localeCompare(right.key),
        offset: 0,
      };

      const window = storage.scanRawWindow({ ...basePlan, limit: undefined });
      const count = storage.scanRawWindow({ ...basePlan, limit: 0 });

      expect(window.keys).toStrictEqual(["a", "b"]);
      expect(window.totalRows).toBe(2);
      expect(count.keys).toStrictEqual([]);
      expect(count.totalRows).toBe(2);
    }),
  );

  it("keeps defensive BigDecimal column comparisons total for malformed scales", () => {
    const metadata = rawQueryCompilerMetadata(Position);
    const malformed = makeBigDecimal(1n, Number.NaN);
    const valid = makeBigDecimal(1n, 0);
    const priceColumn = createTopicColumnValuesFromArray("price", metadata, [malformed, valid]);
    const bound = orderedSlotBoundIndex([0], priceColumn, valid, (comparison) => comparison >= 0);
    const index = rawWindowOrderedSlotIndex(
      {
        columns: new Map([["price", priceColumn]]),
        orderedSlotIndexes: new Map(),
        rawQueryMetadata: metadata,
        slots: [
          { key: "malformed", row: { ...position("malformed", "BAD", 1n, "1"), price: malformed } },
          { key: "valid", row: position("valid", "GOOD", 1n, "1") },
        ],
      },
      [{ field: "price", direction: "asc" }],
    );

    expect(Number.isSafeInteger(bound)).toBe(true);
    expect(index.slots.toSorted((left, right) => left - right)).toStrictEqual([0, 1]);
  });

  it("treats an empty indexed in-filter as an empty candidate set", () => {
    expect(
      orderedEqualityValuesForField(
        [{ field: "price", operator: "in", values: [] }],
        "price",
        rawQueryCompilerMetadata(Order),
      ),
    ).toStrictEqual([]);
    expect(distinctOrderedEqualityValues([2, 1, 2])).toStrictEqual([1, 2]);
  });

  it("admits safe scalar equality seeks and rejects unsafe values", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    expect(
      orderedEqualityValuesForField(
        [{ field: "price", operator: "eq", value: 10 }],
        "price",
        metadata,
      ),
    ).toStrictEqual([10]);
    expect(
      orderedEqualityValuesForField(
        [{ field: "price", operator: "eq", value: { amount: 10 } }],
        "price",
        metadata,
      ),
    ).toBeUndefined();
  });

  it.effect("keeps deleted slots out of scans and handles hostile plans conservatively", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("2", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("3", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* deleteTopicStoreRow(store, "1");

      const readModel = topicStoreTestQueryInterface(store);
      const scannedKeys: Array<string> = [];
      readModel.scanRows((key) => {
        scannedKeys.push(key);
      });
      expect(scannedKeys.toSorted()).toStrictEqual(["2", "3"]);

      const compareByKey = (left: { readonly key: string }, right: { readonly key: string }) =>
        left.key.localeCompare(right.key);
      const compareByKeyDescending = (
        left: { readonly key: string },
        right: { readonly key: string },
      ) => right.key.localeCompare(left.key);
      const matchesOnlySecondRow = (row: object) => fieldValue(row, "id") === "2";
      const missingColumn = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "missing", operator: "eq", value: "anything" }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingColumn.keys).toStrictEqual(["2"]);

      const missingOrderColumn = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(missingOrderColumn.keys).toStrictEqual(["3", "2"]);

      const existingOrderColumnCustomCompare = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(existingOrderColumnCustomCompare.keys).toStrictEqual(["3", "2"]);

      const invalidStorageOrderColumn = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: undefined,
      });
      expect(invalidStorageOrderColumn.keys).toStrictEqual(["3", "2"]);

      const invalidStorageOrderColumnLimited = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "missing", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 1,
      });
      expect(invalidStorageOrderColumnLimited.keys).toStrictEqual(["3"]);
      expect(invalidStorageOrderColumnLimited.totalRows).toBe(2);

      expect(
        readModel.compareRawSlots?.({
          predicate: {
            filters: [],
            callbackRequired: false,
          },
          orderBy: [],
          matches: () => true,
          compare: compareByKey,
          offset: 0,
          limit: 10,
        }),
      ).toBeUndefined();

      expect(
        readModel.compareRawSlots?.({
          predicate: {
            filters: [],
            callbackRequired: false,
          },
          orderBy: [{ field: "price", direction: "asc" }],
          storageOrderBy: [{ field: "missing", direction: "asc" }],
          matches: () => true,
          compare: compareByKey,
          offset: 0,
          limit: 10,
        }),
      ).toBeUndefined();

      const cachedStorageOrderBy: ReadonlyArray<TopicRawOrderByPlan> = [
        { field: "price", direction: "asc" },
      ];
      const cachedComparatorPlan = {
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: cachedStorageOrderBy,
        storageOrderBy: cachedStorageOrderBy,
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      };
      const firstCachedComparator = expectDefined(
        readModel.compareRawSlots?.(cachedComparatorPlan),
      );
      const secondCachedComparator = expectDefined(
        readModel.compareRawSlots?.(cachedComparatorPlan),
      );
      expect(firstCachedComparator(0, 1)).toBe(1);
      expect(secondCachedComparator(0, 1)).toBe(1);

      const missingOrderedSlotIndex = rawWindowOrderedSlotIndex(
        {
          columns: new Map(),
          orderedSlotIndexes: new Map(),
          rawQueryMetadata: rawQueryCompilerMetadata(Order),
          slots: [],
        },
        [{ field: "missing", direction: "asc" }],
      );
      expect(missingOrderedSlotIndex).toStrictEqual({
        orderBy: [{ field: "missing", direction: "asc" }],
        orderColumns: [],
        slots: [],
      });

      const multiFieldStorageOrderZeroLimit = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [
          { field: "price", direction: "asc" },
          { field: "updatedAt", direction: "desc" },
        ],
        storageOrderBy: [
          { field: "price", direction: "asc" },
          { field: "updatedAt", direction: "desc" },
        ],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(multiFieldStorageOrderZeroLimit.keys).toStrictEqual([]);
      expect(multiFieldStorageOrderZeroLimit.totalRows).toBe(2);

      const negativeLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: -1,
      });
      expect(negativeLimitPlan.keys).toStrictEqual(["2"]);
      expect(negativeLimitPlan.totalRows).toBe(2);

      const nanLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: Number.NaN,
      });
      expect(nanLimitPlan.keys).toStrictEqual([]);
      expect(nanLimitPlan.totalRows).toBe(2);

      const infiniteLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKeyDescending,
        offset: 0,
        limit: Number.POSITIVE_INFINITY,
      });
      expect(infiniteLimitPlan.keys).toStrictEqual(["2", "3"]);
      expect(infiniteLimitPlan.totalRows).toBe(2);

      const callbackRequiredStorageOrderPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: matchesOnlySecondRow,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 10,
      });
      expect(callbackRequiredStorageOrderPlan.keys).toStrictEqual(["2"]);
      expect(callbackRequiredStorageOrderPlan.totalRows).toBe(1);

      const manuallyOrderedEqualExclusiveBounds = readModel.scanRawWindow({
        predicate: {
          filters: [
            { field: "price", operator: "gte", value: 20 },
            { field: "price", operator: "gt", value: 20 },
            { field: "price", operator: "lte", value: 30 },
            { field: "price", operator: "lt", value: 30 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(manuallyOrderedEqualExclusiveBounds.keys).toStrictEqual([]);
      expect(manuallyOrderedEqualExclusiveBounds.totalRows).toBe(0);

      const unsafeRangeHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: "10" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeRangeHintPlan.keys).toStrictEqual(["2", "3"]);
      expect(unsafeRangeHintPlan.totalRows).toBe(2);

      const equalityHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "eq", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(equalityHintPlan.keys).toStrictEqual(["2"]);
      expect(equalityHintPlan.totalRows).toBe(1);

      const unsafeEqualityHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "eq", value: "20" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeEqualityHintPlan.keys).toStrictEqual([]);
      expect(unsafeEqualityHintPlan.totalRows).toBe(0);

      const duplicateInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [20, 20, 30] }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(duplicateInHintPlan.keys).toStrictEqual(["2", "3"]);
      expect(duplicateInHintPlan.totalRows).toBe(2);

      const emptyInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [] }],
          callbackRequired: false,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(emptyInHintPlan.keys).toStrictEqual([]);
      expect(emptyInHintPlan.totalRows).toBe(0);

      const unsafeMixedInHintPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "in", values: [20, "30"] }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(unsafeMixedInHintPlan.keys).toStrictEqual(["2"]);
      expect(unsafeMixedInHintPlan.totalRows).toBe(1);

      const missingOrderColumnLimitedMisses = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [{ field: "missing", direction: "asc" }],
        matches: matchesOnlySecondRow,
        compare: compareByKeyDescending,
        offset: 0,
        limit: 1,
      });
      expect(missingOrderColumnLimitedMisses.keys).toStrictEqual(["2"]);
      expect(missingOrderColumnLimitedMisses.totalRows).toBe(1);

      const invalidStartsWithPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "customerId", operator: "startsWith", value: 1 }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(invalidStartsWithPlan.keys).toStrictEqual(["2"]);

      const invalidRangePlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: "10" }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(invalidRangePlan.keys).toStrictEqual(["2"]);

      const nonFiniteRangePlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: Number.NaN }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: matchesOnlySecondRow,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(nonFiniteRangePlan.keys).toStrictEqual(["2"]);

      const zeroLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(zeroLimitPlan.keys).toStrictEqual([]);
      expect(zeroLimitPlan.totalRows).toBe(2);

      let zeroLimitCompareCount = 0;
      let zeroLimitMatchCount = 0;
      const callbackRequiredZeroLimitPlan = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gte", value: 20 }],
          callbackRequired: true,
        },
        orderBy: [{ field: "price", direction: "desc" }],
        storageOrderBy: [{ field: "price", direction: "desc" }],
        matches: (row) => {
          zeroLimitMatchCount += 1;
          return fieldValue(row, "id") === "3";
        },
        compare: () => {
          zeroLimitCompareCount += 1;
          return 0;
        },
        offset: 1,
        limit: 0,
      });
      expect(callbackRequiredZeroLimitPlan.keys).toStrictEqual([]);
      expect(callbackRequiredZeroLimitPlan.totalRows).toBe(1);
      expect(zeroLimitMatchCount).toBe(2);
      expect(zeroLimitCompareCount).toBe(0);

      const unsafeWindowEndPlan = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: Number.MAX_SAFE_INTEGER,
        limit: 1,
      });
      expect(unsafeWindowEndPlan.keys).toStrictEqual([]);
      expect(unsafeWindowEndPlan.totalRows).toBe(2);
    }),
  );

  it.effect("uses bounded fallback scans for finite windows with stable row-key ties", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("d", "open", 20, 4), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("b", "open", 10, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("a", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("c", "open", 10, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("e", "open", 30, 5), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) => {
        const priceComparison =
          numericRowField(left.row, "price") - numericRowField(right.row, "price");
        return priceComparison === 0 ? left.key.localeCompare(right.key) : priceComparison;
      };

      const readModel = topicStoreTestQueryInterface(store);
      const boundedWindow = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 1,
        limit: 3,
      });

      expect(boundedWindow.keys).toStrictEqual(["b", "c", "d"]);
      expect(boundedWindow.window.map((entry) => entry.key)).toStrictEqual(["b", "c", "d"]);
      expect(rowIds(boundedWindow.window.map((entry) => entry.row))).toStrictEqual(["b", "c", "d"]);
      expect(boundedWindow.totalRows).toBe(5);

      const cappedLargeWindow = readModel.scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 1_024,
        limit: 1,
      });

      expect(cappedLargeWindow.keys).toStrictEqual([]);
      expect(cappedLargeWindow.window).toStrictEqual([]);
      expect(cappedLargeWindow.totalRows).toBe(5);
    }),
  );

  it.effect("keeps ordered raw indexes current after single-row replacements", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("a", "open", 10, 1),
        order("b", "open", 20, 2),
        order("c", "open", 30, 3),
      ]);

      const warmed = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });
      expect(rowIds(warmed.rows)).toStrictEqual(["a", "b", "c"]);

      yield* engine.publish("orders", order("b", "open", 5, 4));

      const replaced = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });
      expect(replaced.rows).toStrictEqual([
        { id: "b", price: 5 },
        { id: "a", price: 10 },
        { id: "c", price: 30 },
      ]);

      yield* engine.close();
    }),
  );

  it.effect("keeps ordered raw indexes current after patches that do not change order fields", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("a", "open", 10, 1),
        order("b", "open", 20, 2),
        order("c", "open", 30, 3),
      ]);

      const warmed = yield* engine.snapshot("orders", {
        select: ["id", "price", "updatedAt"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });
      expect(rowIds(warmed.rows)).toStrictEqual(["a", "b", "c"]);

      yield* engine.patch("orders", "b", { updatedAt: 9 });

      const patched = yield* engine.snapshot("orders", {
        select: ["id", "price", "updatedAt"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 3,
      });
      expect(patched.rows).toStrictEqual([
        { id: "a", price: 10, updatedAt: 1 },
        { id: "b", price: 20, updatedAt: 9 },
        { id: "c", price: 30, updatedAt: 3 },
      ]);

      yield* engine.close();
    }),
  );

  it.effect("ignores prepared no-op patches in storage replacement paths", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) => {
        const priceComparison =
          numericRowField(left.row, "price") - numericRowField(right.row, "price");
        return priceComparison === 0 ? left.key.localeCompare(right.key) : priceComparison;
      };
      const initial = order("a", "open", 10, 1);
      storage.setPrepared(yield* storage.prepareRow(initial, invalidRow));
      const firstNoOpPatch = yield* storage.preparePatch("a", { price: 10 }, invalidRow);
      storage.setPrepared(firstNoOpPatch);
      const secondNoOpPatch = yield* storage.preparePatch("a", { updatedAt: 1 }, invalidRow);
      storage.setPreparedMany([secondNoOpPatch]);
      const secondRow = order("b", "open", 20, 2);
      storage.setPrepared(yield* storage.prepareRow(secondRow, invalidRow));
      storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 2,
      });
      storage.setPreparedMany([
        yield* storage.preparePatch("a", { price: 10 }, invalidRow),
        yield* storage.preparePatch("b", { price: 20 }, invalidRow),
      ]);

      const rows: Array<object> = [];
      storage.scanRows((_key, row) => {
        rows.push(row);
      });
      const orderedWindow = storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 2,
      });

      expect(storage.rowCount).toBe(2);
      expect(rows).toStrictEqual([initial, secondRow]);
      expect(orderedWindow.keys).toStrictEqual(["a", "b"]);
    }),
  );

  it.effect("applies stale prepared no-op patches when the current row changed", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) => {
        const priceComparison =
          numericRowField(left.row, "price") - numericRowField(right.row, "price");
        return priceComparison === 0 ? left.key.localeCompare(right.key) : priceComparison;
      };
      const initial = order("a", "open", 10, 1);
      storage.setPrepared(yield* storage.prepareRow(initial, invalidRow));

      const staleSinglePatch = yield* storage.preparePatch("a", { price: 10 }, invalidRow);
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 20, 2), invalidRow));
      storage.setPrepared(staleSinglePatch);

      const rowsAfterSingle: Array<object> = [];
      storage.scanRows((_key, row) => {
        rowsAfterSingle.push(row);
      });
      expect(rowsAfterSingle).toStrictEqual([initial]);

      const staleBatchPatch = yield* storage.preparePatch("a", { price: 10 }, invalidRow);
      const retained = order("b", "open", 40, 4);
      storage.setPrepared(yield* storage.prepareRow(retained, invalidRow));
      storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 2,
      });
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 30, 3), invalidRow));
      storage.setPreparedMany([
        staleBatchPatch,
        yield* storage.prepareRow(order("c", "open", 50, 5), invalidRow),
      ]);

      const rowsAfterBatch: Array<object> = [];
      storage.scanRows((_key, row) => {
        rowsAfterBatch.push(row);
      });
      const orderedWindow = storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 3,
      });

      expect(rowsAfterBatch).toStrictEqual([initial, retained, order("c", "open", 50, 5)]);
      expect(orderedWindow.keys).toStrictEqual(["a", "b", "c"]);
    }),
  );

  it.effect("applies later stale no-op patches against earlier rows in the same batch", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const initial = order("a", "open", 10, 1);
      storage.setPrepared(yield* storage.prepareRow(initial, invalidRow));

      const pricePatch = yield* storage.preparePatch("a", { price: 20 }, invalidRow);
      const staleNoOpPatch = yield* storage.preparePatch("a", { updatedAt: 1 }, invalidRow);
      storage.setPreparedMany([pricePatch, staleNoOpPatch]);

      const rows: Array<object> = [];
      storage.scanRows((_key, row) => {
        rows.push(row);
      });
      expect(rows).toStrictEqual([initial]);
    }),
  );

  it.effect("updates ordered indexes using apply-time fields for stale prepared patches", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) => {
        const priceComparison =
          numericRowField(left.row, "price") - numericRowField(right.row, "price");
        return priceComparison === 0 ? left.key.localeCompare(right.key) : priceComparison;
      };
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 10, 1), invalidRow));
      storage.setPrepared(yield* storage.prepareRow(order("b", "open", 20, 2), invalidRow));
      storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 2,
      });

      const stalePatch = yield* storage.preparePatch("a", { updatedAt: 2 }, invalidRow);
      storage.setPrepared(yield* storage.prepareRow(order("a", "open", 30, 3), invalidRow));
      storage.setPrepared(stalePatch);

      const orderedWindow = storage.scanRawWindow({
        predicate: {
          callbackRequired: false,
          callbackSkippable: true,
          filters: [],
        },
        orderBy: [{ direction: "asc", field: "price" }],
        storageOrderBy: [{ direction: "asc", field: "price" }],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 0,
        limit: 2,
      });

      expect(orderedWindow.keys).toStrictEqual(["a", "b"]);
    }),
  );

  it.effect("keeps ordered raw indexes current after deletes move the last slot", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("a", "open", 10, 1),
        order("b", "open", 20, 2),
        order("c", "open", 30, 3),
        order("d", "open", 40, 4),
      ]);

      const warmed = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 4,
      });
      expect(rowIds(warmed.rows)).toStrictEqual(["a", "b", "c", "d"]);

      yield* engine.delete("orders", "b");

      const deleted = yield* engine.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 4,
      });
      expect(deleted.rows).toStrictEqual([
        { id: "a", price: 10 },
        { id: "c", price: 30 },
        { id: "d", price: 40 },
      ]);

      yield* engine.close();
    }),
  );

  it.effect("uses heap bounded scans for larger finite windows with stable row-key ties", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const rows = Array.from({ length: 5_000 }, (_value, index) => {
        const id = `order-${String(index).padStart(4, "0")}`;
        const region = index < 4_900 ? "emea" : "amer";
        return order(id, "open", Math.floor(index / 2), 5_000 - index, region);
      });
      yield* publishTopicStoreRows(store, rows, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const compareByPriceThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) =>
        numericRowField(left.row, "price") - numericRowField(right.row, "price") ||
        left.key.localeCompare(right.key);

      const heapWindow = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: (row) => rowField(row, "region") === "emea",
        compare: compareByPriceThenKey,
        offset: 1_200,
        limit: 10,
      });

      expect(heapWindow.keys).toStrictEqual([
        "order-1200",
        "order-1201",
        "order-1202",
        "order-1203",
        "order-1204",
        "order-1205",
        "order-1206",
        "order-1207",
        "order-1208",
        "order-1209",
      ]);
      expect(heapWindow.window.map((entry) => entry.key)).toStrictEqual([
        "order-1200",
        "order-1201",
        "order-1202",
        "order-1203",
        "order-1204",
        "order-1205",
        "order-1206",
        "order-1207",
        "order-1208",
        "order-1209",
      ]);
      expect(rowIds(heapWindow.window.map((entry) => entry.row))).toStrictEqual([
        "order-1200",
        "order-1201",
        "order-1202",
        "order-1203",
        "order-1204",
        "order-1205",
        "order-1206",
        "order-1207",
        "order-1208",
        "order-1209",
      ]);
      expect(heapWindow.totalRows).toBe(4_900);

      const compareByPriceOnly = (
        left: { readonly row: object },
        right: { readonly row: object },
      ) => numericRowField(left.row, "price") - numericRowField(right.row, "price");

      const priceOnlyTieWindow = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: (row) => rowField(row, "region") === "emea",
        compare: compareByPriceOnly,
        offset: 1_200,
        limit: 10,
      });

      expect(priceOnlyTieWindow.keys).toStrictEqual([
        "order-1200",
        "order-1201",
        "order-1202",
        "order-1203",
        "order-1204",
        "order-1205",
        "order-1206",
        "order-1207",
        "order-1208",
        "order-1209",
      ]);
      expect(priceOnlyTieWindow.totalRows).toBe(4_900);

      const compareByUpdatedAtThenKey = (
        left: { readonly key: string; readonly row: object },
        right: { readonly key: string; readonly row: object },
      ) =>
        numericRowField(left.row, "updatedAt") - numericRowField(right.row, "updatedAt") ||
        left.key.localeCompare(right.key);

      const candidateHeapWindow = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [{ field: "region", operator: "eq", value: "emea" }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: (row) => rowField(row, "id") !== "order-0000",
        compare: compareByUpdatedAtThenKey,
        offset: 1_200,
        limit: 10,
      });

      expect(candidateHeapWindow.keys).toStrictEqual([
        "order-3699",
        "order-3698",
        "order-3697",
        "order-3696",
        "order-3695",
        "order-3694",
        "order-3693",
        "order-3692",
        "order-3691",
        "order-3690",
      ]);
      expect(candidateHeapWindow.window.map((entry) => entry.key)).toStrictEqual([
        "order-3699",
        "order-3698",
        "order-3697",
        "order-3696",
        "order-3695",
        "order-3694",
        "order-3693",
        "order-3692",
        "order-3691",
        "order-3690",
      ]);
      expect(rowIds(candidateHeapWindow.window.map((entry) => entry.row))).toStrictEqual([
        "order-3699",
        "order-3698",
        "order-3697",
        "order-3696",
        "order-3695",
        "order-3694",
        "order-3693",
        "order-3692",
        "order-3691",
        "order-3690",
      ]);
      expect(candidateHeapWindow.totalRows).toBe(4_899);

      const unboundedFallbackWindow = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByPriceThenKey,
        offset: 5_000,
        limit: 1,
      });

      expect(unboundedFallbackWindow.keys).toStrictEqual([]);
      expect(unboundedFallbackWindow.window).toStrictEqual([]);
      expect(unboundedFallbackWindow.totalRows).toBe(5_000);
    }),
  );
});
