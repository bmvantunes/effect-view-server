import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { fromStringUnsafe, make as makeBigDecimal } from "effect/BigDecimal";
import { InvalidRowError } from "./index";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import { normalizeFilterText } from "./filter-expression";
import { fieldValue, scalarEqualityKey } from "./row-values";
import { scanTopicRawWindow } from "./topic-raw-window-scanner";
import {
  addSlotToScalarPredicateIndexes,
  createScalarPredicateIndexes,
  maxRetainedScalarPredicateBucketSlots,
  removeSlotFromScalarPredicateIndexes,
  selectedPredicateCandidateSlots,
} from "./topic-predicate-candidate-index";
import { rawPredicateSlotFilterMatcher } from "./topic-slot-predicate";
import {
  columnValue,
  createTopicColumnValuesFromArray,
  type TopicColumnValues,
} from "./topic-column-vector";
import {
  deleteTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  resetTopicStore,
  TopicStore,
} from "./topic-store";
import { topicStoreTestQueryInterface } from "../test-harness/topic-store";
import { makeColumns } from "../test-harness/columns";
import { order, Order, position, Position } from "../test-harness/public-engine";

const TextRow = Schema.Struct({
  id: Schema.String,
  status: Schema.String,
  price: Schema.Number,
  updatedAt: Schema.Number,
});

describe("Topic predicate candidate index", () => {
  it.effect("rebuilds an exact scalar bucket after its last row is deleted", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const readModel = topicStoreTestQueryInterface(store);
      const scanOpen = () =>
        readModel.scanRawWindow({
          predicate: {
            filters: [{ field: "status", operator: "eq", value: "open" }],
            callbackRequired: false,
            callbackSkippable: true,
          },
          orderBy: [],
          matches: () => {
            throw new Error("complete exact predicates should not call row callbacks");
          },
          compare: (left, right) => left.key.localeCompare(right.key),
          offset: 0,
          limit: undefined,
        });

      yield* publishTopicStoreRow(store, order("first", "open", 1, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      expect(scanOpen().keys).toStrictEqual(["first"]);
      yield* deleteTopicStoreRow(store, "first");
      yield* publishTopicStoreRow(store, order("second", "open", 2, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      expect(scanOpen().keys).toStrictEqual(["second"]);
    }),
  );

  it.effect("quickselects small ordered windows from large filtered candidate sets", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const prices = [9, 3, 8, 10, 7, 4, 6, 2, 1, 0];
      const priceByKey = new Map(prices.map((price, index) => [`open-${index}`, price]));
      yield* publishTopicStoreRows(
        store,
        [
          ...prices.map((price, index) => order(`open-${index}`, "open", price, index)),
          order("closed-1", "closed", 10, 10),
          order("closed-2", "closed", 11, 11),
        ],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const result = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
            { field: "price", operator: "gt", value: 1 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete quickselect predicates should not call row callbacks");
        },
        compare: (left, right) => priceByKey.get(left.key)! - priceByKey.get(right.key)!,
        offset: 1,
        limit: 1,
      });

      expect(result.keys).toStrictEqual(["open-1"]);
      expect(result.totalRows).toBe(8);

      const alreadyBounded = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
            { field: "price", operator: "gt", value: 8 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("bounded quickselect predicates should not call row callbacks");
        },
        compare: (left, right) => priceByKey.get(left.key)! - priceByKey.get(right.key)!,
        offset: 1,
        limit: 1,
      });

      expect(alreadyBounded.keys).toStrictEqual(["open-3"]);
      expect(alreadyBounded.totalRows).toBe(2);
    }),
  );

  it.effect("falls back from adversarial quickselect partitions with bounded comparisons", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const rowCount = 4_096;
      const pivotSlot = Math.floor((rowCount - 1) / 2);
      const prices = Array.from({ length: rowCount }, (_value, index) =>
        index === pivotSlot ? 1_000_000 : index,
      );
      const priceByKey = new Map(prices.map((price, index) => [`open-${index}`, price]));
      yield* publishTopicStoreRows(
        store,
        [
          ...prices.map((price, index) => order(`open-${index}`, "open", price, index)),
          order("closed", "closed", -1, rowCount),
        ],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      let comparisons = 0;
      const result = topicStoreTestQueryInterface(store).scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete adversarial predicates should not call row callbacks");
        },
        compare: (left, right) => {
          comparisons += 1;
          return priceByKey.get(left.key)! - priceByKey.get(right.key)!;
        },
        offset: 0,
        limit: 10,
      });

      expect(result.keys).toStrictEqual(
        Array.from({ length: 10 }, (_value, index) => `open-${index}`),
      );
      expect(result.totalRows).toBe(rowCount);
      expect(comparisons).toBeLessThan(rowCount * 40);
    }),
  );

  it("does not build normalized text indexes when index creation is disabled", () => {
    const rows = [
      { id: "row", status: "Résumé", price: 10, updatedAt: 1 },
      { id: "other", status: "other", price: 20, updatedAt: 2 },
    ];
    const metadata = rawQueryCompilerMetadata(TextRow);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["status", rows.map((row) => row.status)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };

    const candidate = selectedPredicateCandidateSlots(
      state,
      [
        {
          field: "status",
          operator: "textEq",
          value: "resume",
          caseSensitive: false,
          accentSensitive: false,
        },
      ],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
      },
    );

    expect(candidate).toBeUndefined();
    expect(state.scalarPredicateIndexes.size).toBe(0);

    const nonStringCandidate = selectedPredicateCandidateSlots(
      state,
      [
        {
          field: "price",
          operator: "textEq",
          value: "10",
          caseSensitive: false,
          accentSensitive: false,
        },
      ],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
      },
    );
    const missingCandidate = selectedPredicateCandidateSlots(
      state,
      [
        {
          field: "missing",
          operator: "textEq",
          value: "resume",
          caseSensitive: false,
          accentSensitive: false,
        },
      ],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
      },
    );

    expect(nonStringCandidate).toBeUndefined();
    expect(missingCandidate).toBeUndefined();

    const sensitivityCandidates = [
      {
        caseSensitive: true,
        accentSensitive: false,
        value: normalizeFilterText("Résumé", true, false),
      },
      {
        caseSensitive: false,
        accentSensitive: true,
        value: normalizeFilterText("Résumé", false, true),
      },
      {
        caseSensitive: true,
        accentSensitive: true,
        value: normalizeFilterText("Résumé", true, true),
      },
    ].map((mode) =>
      selectedPredicateCandidateSlots(
        state,
        [
          {
            field: "status",
            operator: "textEq",
            value: mode.value,
            caseSensitive: mode.caseSensitive,
            accentSensitive: mode.accentSensitive,
          },
        ],
        {
          allowScalarIndexBuild: true,
          exactRangeCandidates: true,
        },
      ),
    );
    const repeatedCandidate = selectedPredicateCandidateSlots(
      state,
      [
        {
          field: "status",
          operator: "textEq",
          value: normalizeFilterText("Résumé", true, false),
          caseSensitive: true,
          accentSensitive: false,
        },
      ],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
      },
    );

    expect(sensitivityCandidates.map((entry) => entry?.slots)).toStrictEqual([[0], [0], [0]]);
    expect(repeatedCandidate?.slots).toBe(sensitivityCandidates[0]?.slots);
  });

  it("maintains every normalized text sensitivity index and ignores missing strings", () => {
    const metadata = rawQueryCompilerMetadata(TextRow);
    const value = "Résumé";
    const modes = [
      { caseSensitive: false, accentSensitive: false },
      { caseSensitive: true, accentSensitive: false },
      { caseSensitive: false, accentSensitive: true },
      { caseSensitive: true, accentSensitive: true },
    ];
    const buckets = new Map<string, Set<number>>();
    const normalizedStringBuckets = new Map<
      string,
      {
        readonly caseSensitive: boolean;
        readonly accentSensitive: boolean;
        readonly normalizedValue: string;
      }
    >();
    for (const mode of modes) {
      const normalizedValue = normalizeFilterText(value, mode.caseSensitive, mode.accentSensitive);
      const key = `normalized-string:${mode.caseSensitive ? 1 : 0}:${mode.accentSensitive ? 1 : 0}:${normalizedValue.length}:${normalizedValue}`;
      buckets.set(key, new Set());
      normalizedStringBuckets.set(key, { ...mode, normalizedValue });
    }
    const indexes = createScalarPredicateIndexes();
    indexes.set("status", {
      buckets,
      indexedKeys: new Set(buckets.keys()),
      normalizedStringBuckets,
      normalizedStringModes: new Map(
        modes.map((mode) => [
          `${mode.caseSensitive ? 1 : 0}:${mode.accentSensitive ? 1 : 0}`,
          { ...mode, count: 1 },
        ]),
      ),
      orderedBucketSlots: new Map(),
    });
    const valueColumn = createTopicColumnValuesFromArray("status", metadata, [value]);
    const missingColumn = createTopicColumnValuesFromArray("status", metadata, [undefined]);

    addSlotToScalarPredicateIndexes(indexes, new Map([["status", missingColumn]]), 0);
    removeSlotFromScalarPredicateIndexes(indexes, new Map([["status", missingColumn]]), 0);
    addSlotToScalarPredicateIndexes(indexes, new Map([["status", valueColumn]]), 0);
    expect([...buckets.values()].map((bucket) => [...bucket])).toStrictEqual([[0], [0], [0], [0]]);

    removeSlotFromScalarPredicateIndexes(indexes, new Map([["status", valueColumn]]), 0);
    expect(buckets.size).toBe(0);
    expect(indexes.size).toBe(0);
  });

  it("retains shared normalized modes until their final bucket is removed", () => {
    const rows = [
      { id: "resume", status: "Résumé", price: 10, updatedAt: 1 },
      { id: "other", status: "other", price: 20, updatedAt: 2 },
      { id: "excluded", status: "excluded", price: 30, updatedAt: 3 },
    ];
    const metadata = rawQueryCompilerMetadata(TextRow);
    const columns = makeColumns(metadata, [
      ["id", rows.map((row) => row.id)],
      ["status", rows.map((row) => row.status)],
      ["price", rows.map((row) => row.price)],
    ]);
    const state = {
      columns,
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    const resume = normalizeFilterText("Résumé", false, false);
    const other = normalizeFilterText("other", false, false);

    expect(
      selectedPredicateCandidateSlots(
        state,
        [
          {
            field: "status",
            operator: "textIn",
            values: [resume, other],
            valueSet: new Set([resume, other]),
            caseSensitive: false,
            accentSensitive: false,
          },
        ],
        { allowScalarIndexBuild: true, exactRangeCandidates: true },
      )?.slots,
    ).toStrictEqual([0, 1]);

    const index = state.scalarPredicateIndexes.get("status")!;
    expect(index.normalizedStringModes.get("0:0")?.count).toBe(2);
    removeSlotFromScalarPredicateIndexes(state.scalarPredicateIndexes, columns, 0);
    expect(index.normalizedStringModes.get("0:0")?.count).toBe(1);
    removeSlotFromScalarPredicateIndexes(state.scalarPredicateIndexes, columns, 1);
    expect(state.scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("matches exact string equality directly and does not build missing buckets when disabled", () => {
    const rows = [
      { id: "open", status: "open", price: 10, updatedAt: 1 },
      { id: "closed", status: "closed", price: 20, updatedAt: 2 },
    ];
    const metadata = rawQueryCompilerMetadata(TextRow);
    const columns = makeColumns(metadata, [
      ["id", rows.map((row) => row.id)],
      ["status", rows.map((row) => row.status)],
      ["price", rows.map((row) => row.price)],
    ]);
    const matcher = rawPredicateSlotFilterMatcher(
      [{ field: "status", operator: "eq", value: "open" }],
      columns,
      true,
    );
    expect([matcher(0), matcher(1)]).toStrictEqual([true, false]);

    const state = {
      columns,
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    expect(
      selectedPredicateCandidateSlots(
        state,
        [{ field: "status", operator: "eq", value: "missing" }],
        { allowScalarIndexBuild: false, exactRangeCandidates: true },
      ),
    ).toBeUndefined();
    expect(
      selectedPredicateCandidateSlots(state, [{ field: "status", operator: "eq", value: "open" }], {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
      })?.slots,
    ).toStrictEqual([0]);
    expect(
      selectedPredicateCandidateSlots(
        state,
        [{ field: "status", operator: "eq", value: "missing" }],
        { allowScalarIndexBuild: false, exactRangeCandidates: true },
      ),
    ).toBeUndefined();
  });

  it("matches BigDecimal slot equality without aligning extreme scales", () => {
    const metadata = rawQueryCompilerMetadata(Position);
    const tiny = makeBigDecimal(1n, Number.MAX_SAFE_INTEGER);
    const huge = makeBigDecimal(1n, Number.MIN_SAFE_INTEGER);
    const columns = makeColumns(metadata, [["price", [tiny, huge, makeBigDecimal(10n, 1)]]]);
    const extremeMatcher = rawPredicateSlotFilterMatcher(
      [{ field: "price", operator: "eq", value: huge }],
      columns,
      true,
    );
    const normalizedMatcher = rawPredicateSlotFilterMatcher(
      [{ field: "price", operator: "eq", value: makeBigDecimal(1n, 0) }],
      columns,
      true,
    );

    expect([extremeMatcher(0), extremeMatcher(1), extremeMatcher(2)]).toStrictEqual([
      false,
      true,
      false,
    ]);
    expect([normalizedMatcher(0), normalizedMatcher(1), normalizedMatcher(2)]).toStrictEqual([
      false,
      false,
      true,
    ]);
  });

  it.effect("indexes ordered normalized text equality across row mutations", () =>
    Effect.gen(function* () {
      const store = new TopicStore("textRows", TextRow, "id", () => {});
      yield* publishTopicStoreRows(
        store,
        [
          { id: "accented", status: "Résumé", price: 10, updatedAt: 1 },
          { id: "plain", status: "resume", price: 20, updatedAt: 2 },
          { id: "other", status: "closed", price: 30, updatedAt: 3 },
        ],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      const readModel = topicStoreTestQueryInterface(store);
      const plan = {
        predicate: {
          filters: [
            {
              field: "status",
              operator: "textEq" as const,
              value: "resume",
              caseSensitive: false,
              accentSensitive: false,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "updatedAt", direction: "asc" as const }],
        storageOrderBy: [{ field: "updatedAt", direction: "asc" as const }],
        matches: () => {
          throw new Error("normalized text candidates should not call row callbacks");
        },
        compare: (left: { readonly key: string }, right: { readonly key: string }) =>
          left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      };

      expect(readModel.scanRawWindow(plan).keys).toStrictEqual(["accented", "plain"]);

      yield* publishTopicStoreRow(
        store,
        { id: "added", status: "RÉSUMÉ", price: 40, updatedAt: 4 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { id: "other", status: "résumé", price: 30, updatedAt: 5 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* deleteTopicStoreRow(store, "plain");

      expect(readModel.scanRawWindow(plan).keys).toStrictEqual(["accented", "added", "other"]);
    }),
  );

  it("plans repeated same-field ranges in linear filter reads", () => {
    const rows = Array.from({ length: 1_000 }, (_value, index) => ({
      id: `row-${index}`,
      status: "open",
      price: index,
      updatedAt: index,
    }));
    const metadata = rawQueryCompilerMetadata(TextRow);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    scanTopicRawWindow(state, {
      predicate: { filters: [], callbackRequired: false, callbackSkippable: true },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("ordered-index warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 10,
    });

    const filters: ReadonlyArray<TopicRawPredicateFilterPlan> = Array.from(
      { length: 200 },
      (): TopicRawPredicateFilterPlan => ({ field: "price", operator: "gte", value: 900 }),
    );
    let numericReads = 0;
    const countedFilters = new Proxy(filters, {
      get: (target, property, receiver) => {
        if (typeof property === "string" && /^\d+$/u.test(property)) {
          numericReads += 1;
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const result = scanTopicRawWindow(state, {
      predicate: {
        filters: countedFilters,
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("consolidated range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(result.totalRows).toBe(100);
    expect(numericReads).toBeLessThan(1_000);
  });

  it("stops repeated scalar planning after the first empty candidate", () => {
    const rowCount = 1_000;
    const statusValues = Array.from({ length: rowCount }, () => "open");
    let scalarReads = 0;
    const statusColumn: TopicColumnValues = {
      kind: "string",
      length: rowCount,
      get: (slot) => {
        scalarReads += 1;
        return statusValues[slot];
      },
      stringAt: (slot) => {
        scalarReads += 1;
        return statusValues[slot];
      },
    };
    const rows = statusValues.map((status, index) => ({
      id: `row-${index}`,
      status,
      price: index,
      updatedAt: index,
    }));
    const state = {
      columns: new Map<string, TopicColumnValues>([["status", statusColumn]]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: rawQueryCompilerMetadata(TextRow),
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    const filters: ReadonlyArray<TopicRawPredicateFilterPlan> = Array.from(
      { length: 200 },
      (_value, index) => ({ field: "status", operator: "eq", value: `missing-${index}` }),
    );

    const candidate = selectedPredicateCandidateSlots(state, filters, {
      allowScalarIndexBuild: true,
      exactRangeCandidates: true,
    });

    expect(candidate?.slots).toStrictEqual([]);
    expect(scalarReads).toBeLessThan(rowCount * 2);
  });

  it("intersects contradictory present scalar candidates before planning later filters", () => {
    const rowCount = 1_000;
    const statusValues = Array.from({ length: rowCount }, (_value, index) => `status-${index}`);
    let scalarReads = 0;
    const statusColumn: TopicColumnValues = {
      kind: "string",
      length: rowCount,
      get: (slot) => statusValues[slot],
      stringAt: (slot) => {
        scalarReads += 1;
        return statusValues[slot];
      },
    };
    const rows = statusValues.map((status, index) => ({
      id: `row-${index}`,
      status,
      price: index,
      updatedAt: index,
    }));
    const state = {
      columns: new Map<string, TopicColumnValues>([["status", statusColumn]]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: rawQueryCompilerMetadata(TextRow),
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    const filters: ReadonlyArray<TopicRawPredicateFilterPlan> = Array.from(
      { length: 200 },
      (_value, index) => ({ field: "status", operator: "eq", value: `status-${index}` }),
    );

    const candidate = selectedPredicateCandidateSlots(state, filters, {
      allowScalarIndexBuild: true,
      exactRangeCandidates: true,
    });

    expect(candidate?.slots).toStrictEqual([]);
    expect(scalarReads).toBeLessThan(rowCount * 3);
  });

  it("sorts sparse exact candidates instead of scanning a different ordered index", () => {
    const rowCount = 10_000;
    const rows = Array.from({ length: rowCount }, (_value, index) => ({
      id: `row-${index}`,
      status: index === rowCount - 1 ? "rare" : "open",
      price: index,
      updatedAt: rowCount - index,
    }));
    const metadata = rawQueryCompilerMetadata(TextRow);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["status", rows.map((row) => row.status)],
        ["updatedAt", rows.map((row) => row.updatedAt)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };
    const orderedPlan = {
      predicate: {
        filters: [],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "updatedAt", direction: "asc" as const }],
      storageOrderBy: [{ field: "updatedAt", direction: "asc" as const }],
      matches: () => true,
      compare: (left: { readonly key: string }, right: { readonly key: string }) =>
        left.key.localeCompare(right.key),
      offset: 0,
      limit: 10,
    };
    scanTopicRawWindow(state, orderedPlan);
    const orderedIndex = [...state.orderedSlotIndexes.values()][0]!;
    const orderedSlots = orderedIndex.slots;
    let orderedSlotReads = 0;
    Object.defineProperty(orderedIndex, "slots", {
      value: new Proxy(orderedSlots, {
        get: (target, property, receiver) => {
          if (typeof property === "string" && /^\d+$/u.test(property)) {
            orderedSlotReads += 1;
          }
          return Reflect.get(target, property, receiver);
        },
      }),
    });

    const result = scanTopicRawWindow(state, {
      ...orderedPlan,
      predicate: {
        filters: [{ field: "status", operator: "eq", value: "rare" }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      matches: () => {
        throw new Error("exact sparse candidates should not call row callbacks");
      },
    });

    expect(result.keys).toStrictEqual([`row-${rowCount - 1}`]);
    expect(result.totalRows).toBe(1);
    expect(orderedSlotReads).toBeLessThan(100);
  });

  it.effect("skips row callbacks for complete column predicate plans", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("missing-note", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("matched-note", "open", 20, 2), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("other-note", "open", 30, 3), note: "bye" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreTestQueryInterface(store);
      const callbackSkipped = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "note", operator: "startsWith", value: "he" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete column predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(callbackSkipped.keys).toStrictEqual(["matched-note"]);
      expect(callbackSkipped.totalRows).toBe(1);

      const optionalNotEqual = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "note", operator: "neq", value: "bye" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete optional not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(optionalNotEqual.keys).toStrictEqual(["matched-note", "missing-note"]);
      expect(optionalNotEqual.totalRows).toBe(2);
    }),
  );

  it.effect("uses indexed scalar in predicate filters without row callbacks", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("matched", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("expensive", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("noted", "open", 40, 4), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      const readModel = topicStoreTestQueryInterface(store);
      const indexedNumberIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("indexed numeric in predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(indexedNumberIn.keys).toStrictEqual(["matched"]);
      expect(indexedNumberIn.totalRows).toBe(1);

      const indexedOptionalIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("indexed optional in predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(indexedOptionalIn.keys).toStrictEqual(["noted"]);
      expect(indexedOptionalIn.totalRows).toBe(1);
    }),
  );

  it.effect("uses scalar predicate candidate scans across row mutations", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(store, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("matched", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { ...order("noted", "open", 40, 4), note: "hello" },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("cold", "closed", 99, 9), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreTestQueryInterface(store);
      const compareByKey = (left: { readonly key: string }, right: { readonly key: string }) =>
        left.key.localeCompare(right.key);

      const initialPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar in predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialPriceMatch.keys).toStrictEqual(["matched"]);

      const initialPriceMatchLargeLimit = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("large-limit scalar candidate predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 2_000,
      });
      expect(initialPriceMatchLargeLimit.keys).toStrictEqual(["matched"]);
      expect(initialPriceMatchLargeLimit.totalRows).toBe(1);

      const initialPriceCountOnly = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar count-only predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(initialPriceCountOnly.keys).toStrictEqual([]);
      expect(initialPriceCountOnly.totalRows).toBe(1);

      const selectiveCountOnlyWithCallbackFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20, 40],
              valueKeys: new Set(["number:20", "number:40"]),
            },
          ],
          callbackRequired: true,
        },
        orderBy: [],
        matches: (row) => fieldValue(row, "id") === "noted",
        compare: () => {
          throw new Error("selective count-only candidates should not compare rows");
        },
        offset: 0,
        limit: 0,
      });
      expect(selectiveCountOnlyWithCallbackFilter.keys).toStrictEqual([]);
      expect(selectiveCountOnlyWithCallbackFilter.totalRows).toBe(1);

      const initialRangeGreaterThan = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range gt predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeGreaterThan.keys).toStrictEqual(["cold", "noted"]);

      const initialRangeGreaterThanOrEqual = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 40,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range gte predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeGreaterThanOrEqual.keys).toStrictEqual(["cold", "noted"]);

      const initialRangeLessThan = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "lt",
              value: 20,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range lt predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeLessThan.keys).toStrictEqual(["cheap"]);

      const initialRangeLessThanOrEqual = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "lte",
              value: 10,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range lte predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(initialRangeLessThanOrEqual.keys).toStrictEqual(["cheap"]);

      const initialRangeCountOnly = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range count-only predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 0,
      });
      expect(initialRangeCountOnly.keys).toStrictEqual([]);
      expect(initialRangeCountOnly.totalRows).toBe(2);

      const rangeCandidateRejectedBySecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("compound range candidate predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rangeCandidateRejectedBySecondFilter.keys).toStrictEqual(["noted"]);

      const rangeCandidateRejectedByIncompatibleSecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 30,
            },
            {
              field: "note",
              operator: "gt",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("incompatible compound range predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rangeCandidateRejectedByIncompatibleSecondFilter.keys).toStrictEqual([]);

      const warmedStatusBucket = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("status bucket warmup should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(warmedStatusBucket.keys).toStrictEqual(["cold"]);

      const orderedRangeCandidateRejectedBySecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gt",
              value: 0,
            },
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => {
          throw new Error("ordered compound range candidates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 10,
      });
      expect(orderedRangeCandidateRejectedBySecondFilter.keys).toStrictEqual(["cold"]);
      expect(orderedRangeCandidateRejectedBySecondFilter.totalRows).toBe(1);

      const partialPriceBuckets = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20, 999],
              valueKeys: new Set(["number:20", "number:999"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("missing scalar buckets should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(partialPriceBuckets.keys).toStrictEqual(["matched"]);

      const stableTieWindow = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20, 10],
              valueKeys: new Set(["number:20", "number:10"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar in tie windows should not call row callbacks");
        },
        compare: () => 0,
        offset: 0,
        limit: 1,
      });
      expect(stableTieWindow.keys).toStrictEqual(["cheap"]);

      const boundedCandidateRejectedBySecondFilter = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
            {
              field: "price",
              operator: "in",
              values: [10, 20],
              valueKeys: new Set(["number:10", "number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("bounded scalar candidate scans should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: 1,
      });
      expect(boundedCandidateRejectedBySecondFilter.keys).toStrictEqual([]);

      const boundedCandidateReplacement = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [10, 20],
              valueKeys: new Set(["number:10", "number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("bounded scalar candidate top-k scans should not call row callbacks");
        },
        compare: (left, right) => right.key.localeCompare(left.key),
        offset: 0,
        limit: 1,
      });
      expect(boundedCandidateReplacement.keys).toStrictEqual(["matched"]);

      const notedMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete optional scalar in predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(notedMatch.keys).toStrictEqual(["noted"]);

      const closedStatusMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("selective status predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(closedStatusMatch.keys).toStrictEqual(["cold"]);

      const sameSizeCandidateMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["closed"],
              valueKeys: new Set(["string:6:closed"]),
            },
            {
              field: "note",
              operator: "in",
              values: ["hello"],
              valueKeys: new Set(["string:5:hello"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("same-size scalar candidates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(sameSizeCandidateMatch.keys).toStrictEqual([]);

      const broadStatusMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("broad complete scalar predicates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(broadStatusMatch.keys).toStrictEqual(["cheap", "matched", "noted"]);

      yield* publishTopicStoreRow(store, order("also-matched", "open", 20, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("other", "open", 30, 5), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, order("missing-note", "open", 50, 6), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const insertedPriceMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans should not call row callbacks after append");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(insertedPriceMatches.keys).toStrictEqual(["also-matched", "matched"]);

      const newPriceBucket = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [30],
              valueKeys: new Set(["number:30"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("new scalar candidate buckets should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(newPriceBucket.keys).toStrictEqual(["other"]);

      const insertedRangeMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 50,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("range candidate scans should not call row callbacks after append");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(insertedRangeMatches.keys).toStrictEqual(["cold", "missing-note"]);

      const smallerCandidateWins = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open"],
              valueKeys: new Set(["string:4:open"]),
            },
            {
              field: "price",
              operator: "eq",
              value: 20,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete scalar predicate candidates should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(smallerCandidateWins.keys).toStrictEqual(["also-matched", "matched"]);

      yield* publishTopicStoreRow(store, order("matched", "open", 30, 7), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const rebuiltPriceMatches = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans should not call row callbacks after replace");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(rebuiltPriceMatches.keys).toStrictEqual(["also-matched"]);

      const manualObjectEquality = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "eq",
              value: { price: 20 },
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(manualObjectEquality.keys).toStrictEqual([]);

      const missingFieldEq = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "eq",
              value: "anything",
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldEq.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      const missingFieldIn = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "in",
              values: ["anything"],
              valueKeys: new Set(["string:8:anything"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldIn.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      const missingFieldRange = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "missing",
              operator: "gt",
              value: 1,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => true,
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(missingFieldRange.keys).toStrictEqual([
        "also-matched",
        "cheap",
        "cold",
        "matched",
        "missing-note",
        "noted",
        "other",
      ]);

      yield* deleteTopicStoreRow(store, "other");

      const afterDeleteRangeMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "gte",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("range candidate scans after delete should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterDeleteRangeMatch.keys).toStrictEqual([
        "cold",
        "matched",
        "missing-note",
        "noted",
      ]);

      const afterDeletePriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "eq",
              value: 30,
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans after delete should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterDeletePriceMatch.keys).toStrictEqual(["matched"]);

      yield* publishTopicStoreRows(
        store,
        [order("bulk-a", "open", 20, 8), order("bulk-b", "closed", 60, 9)],
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const afterBulkPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error(
            "scalar candidate scans after bulk publish should not call row callbacks",
          );
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterBulkPriceMatch.keys).toStrictEqual(["also-matched", "bulk-a"]);

      yield* resetTopicStore(store);

      const afterResetPriceMatch = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "price",
              operator: "in",
              values: [20],
              valueKeys: new Set(["number:20"]),
            },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("scalar candidate scans after reset should not call row callbacks");
        },
        compare: compareByKey,
        offset: 0,
        limit: undefined,
      });
      expect(afterResetPriceMatch.keys).toStrictEqual([]);
    }),
  );

  it.effect("keeps optional column values out of exact range scans without row callbacks", () =>
    Effect.gen(function* () {
      const OptionalPrice = Schema.Struct({
        group: Schema.String,
        id: Schema.String,
        price: Schema.optionalKey(Schema.Finite),
      });
      const store = new TopicStore("optional-prices", OptionalPrice, "id", () => {});
      yield* publishTopicStoreRow(store, { group: "candidate", id: "missing" }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { group: "other", id: "cheap", price: 5 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        store,
        { group: "other", id: "expensive", price: 20 },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreTestQueryInterface(store);
      const callbackSkipped = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 10 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("complete range predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(callbackSkipped.keys).toStrictEqual(["expensive"]);
      expect(callbackSkipped.totalRows).toBe(1);

      const scalarCandidateRejectedByMissingRangeValue = readModel.scanRawWindow({
        predicate: {
          filters: [
            {
              field: "group",
              operator: "in",
              values: ["candidate"],
              valueKeys: new Set(["string:9:candidate"]),
            },
            { field: "price", operator: "gt", value: 0 },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("missing optional range values should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(scalarCandidateRejectedByMissingRangeValue.keys).toStrictEqual([]);
      expect(scalarCandidateRejectedByMissingRangeValue.totalRows).toBe(0);
    }),
  );

  it.effect(
    "keeps broad exact scalar predicates correct without row callbacks",
    () =>
      Effect.gen(function* () {
        const SkewedRow = Schema.Struct({
          id: Schema.String,
          status: Schema.String,
        });
        const rowCount = 20_000;
        const skippedSlots = new Set(Array.from({ length: 4_096 }, (_value, index) => index * 2));
        const rows = Array.from({ length: rowCount }, (_value, index) => ({
          id: `row-${index.toString().padStart(5, "0")}`,
          status: skippedSlots.has(index) ? "skip" : "match",
        }));
        const store = new TopicStore("skewed", SkewedRow, "id", () => {});
        yield* publishTopicStoreRows(store, rows, (topic, message) =>
          InvalidRowError.make({ topic, message }),
        );

        const readModel = topicStoreTestQueryInterface(store);
        const fallbackResult = readModel.scanRawWindow({
          predicate: {
            filters: [{ field: "status", operator: "eq", value: "match" }],
            callbackRequired: false,
            callbackSkippable: true,
          },
          orderBy: [],
          matches: () => {
            throw new Error("complete skewed predicates should not call row callbacks");
          },
          compare: (left, right) => left.key.localeCompare(right.key),
          offset: 0,
          limit: 0,
        });

        expect(fallbackResult.keys).toStrictEqual([]);
        expect(fallbackResult.totalRows).toBe(rowCount - skippedSlots.size);
      }),
    10_000,
  );

  it("does not touch non-candidate row entries for exact scalar and range scans", () => {
    const first = order("first", "open", 10, 1);
    const second = order("second", "closed", 20, 2);
    const slots = [
      {
        key: first.id,
        row: first,
      },
      {
        key: second.id,
        row: second,
      },
    ];
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", [first.id, second.id]],
        ["price", [first.price, second.price]],
        ["status", [first.status, second.status]],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots,
    };

    const priceColumn = state.columns.get("price")!;
    const statusColumn = state.columns.get("status")!;
    expect(priceColumn.kind).toBe("number");
    expect(statusColumn.kind).toBe("string");

    const warmRangeIndex = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 0 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("exact range warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(warmRangeIndex.keys).toStrictEqual(["first", "second"]);
    expect(warmRangeIndex.totalRows).toBe(2);

    Object.defineProperty(slots, "0", {
      get: () => {
        throw new Error("non-candidate scalar slot should not be read");
      },
    });

    const scalarCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "eq", value: 20 }],
        callbackRequired: true,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(scalarCandidate.keys).toStrictEqual(["second"]);
    expect(scalarCandidate.totalRows).toBe(1);

    const rangeCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 10 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(rangeCandidate.keys).toStrictEqual(["second"]);
    expect(rangeCandidate.totalRows).toBe(1);

    const emptyRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "price", operator: "gt", value: 30 },
          { field: "price", operator: "lt", value: 10 },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("empty exact range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(emptyRange.keys).toStrictEqual([]);
    expect(emptyRange.totalRows).toBe(0);
  });

  it("keeps count-only scans candidate-aware for selective scalar predicates", () => {
    const rows = [
      order("first", "open", 10, 1),
      order("second", "closed", 20, 2),
      order("third", "open", 30, 3),
      order("fourth", "open", 40, 4),
    ];
    const slots = rows.map((row) => ({
      key: row.id,
      row,
    }));
    const metadata = rawQueryCompilerMetadata(Order);
    const sourceStatusColumn = createTopicColumnValuesFromArray(
      "status",
      metadata,
      rows.map((row) => row.status),
    );
    let statusReadCount = 0;
    const observedStatusColumn: TopicColumnValues = {
      kind: "generic",
      get length() {
        return sourceStatusColumn.length;
      },
      get(slot) {
        statusReadCount += 1;
        return columnValue(sourceStatusColumn, slot);
      },
    };
    const state = {
      columns: new Map<string, TopicColumnValues>([
        [
          "id",
          createTopicColumnValuesFromArray(
            "id",
            metadata,
            rows.map((row) => row.id),
          ),
        ],
        ["status", observedStatusColumn],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots,
    };

    const warmClosedBucket = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set(["string:6:closed"]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("scalar bucket warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(warmClosedBucket.keys).toStrictEqual(["second"]);
    expect(statusReadCount).toBeGreaterThan(1);

    statusReadCount = 0;
    const countClosedRows = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set(["string:6:closed"]),
          },
        ],
        callbackRequired: true,
      },
      orderBy: [],
      matches: (row) => fieldValue(row, "id") === "second",
      compare: () => {
        throw new Error("count-only scalar candidates should not compare rows");
      },
      offset: 0,
      limit: 0,
    });

    expect(countClosedRows.keys).toStrictEqual([]);
    expect(countClosedRows.totalRows).toBe(1);
    expect(statusReadCount).toBe(0);
  });

  it("keeps exact candidate scans aligned with bounded and ordered raw windows", () => {
    const low = position("low", "AAPL", 5n, "10");
    const equal = position("equal", "AAPL", 10n, "10");
    const high = position("high", "MSFT", 20n, "10");
    const missingQuantity = {
      ...position("missing-quantity", "TSLA", 0n, "10"),
      quantity: undefined,
    };
    const rows = [low, equal, high, missingQuantity];
    const slots = rows.map((row) => ({
      key: row.id,
      row,
    }));
    const metadata = rawQueryCompilerMetadata(Position);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["symbol", rows.map((row) => row.symbol)],
        ["quantity", rows.map((row) => row.quantity)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots,
    };

    const quantityColumn = state.columns.get("quantity")!;
    const priceColumn = state.columns.get("price")!;
    expect(quantityColumn.kind).toBe("bigint");
    expect(priceColumn.kind).toBe("bigDecimal");

    const boundedReplacement = scanTopicRawWindow(state, {
      predicate: {
        filters: [],
        callbackRequired: true,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) =>
        Number(fieldValue(right.row, "quantity")) - Number(fieldValue(left.row, "quantity")),
      offset: 0,
      limit: 1,
    });

    expect(boundedReplacement.keys).toStrictEqual(["high"]);
    expect(boundedReplacement.totalRows).toBe(4);

    const exactBigIntRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gte", value: 10n }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact BigInt range candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(exactBigIntRange.keys).toStrictEqual(["equal", "high"]);
    expect(exactBigIntRange.totalRows).toBe(2);

    const exactBigDecimalRange = scanTopicRawWindow(
      {
        ...state,
        columns: makeColumns(metadata, [
          ["id", rows.map((row) => row.id)],
          ["symbol", rows.map((row) => row.symbol)],
          ["quantity", rows.map((row) => row.quantity)],
          ["price", [fromStringUnsafe("5"), fromStringUnsafe("10"), fromStringUnsafe("20")]],
        ]),
      },
      {
        predicate: {
          filters: [{ field: "price", operator: "gte", value: fromStringUnsafe("10") }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact BigDecimal range candidates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      },
    );

    expect(exactBigDecimalRange.keys).toStrictEqual(["equal", "high"]);
    expect(exactBigDecimalRange.totalRows).toBe(2);

    const orderedBigDecimalRange = scanTopicRawWindow(
      {
        ...state,
        columns: makeColumns(metadata, [
          ["id", rows.map((row) => row.id)],
          ["symbol", rows.map((row) => row.symbol)],
          ["quantity", rows.map((row) => row.quantity)],
          ["price", [fromStringUnsafe("20"), fromStringUnsafe("10"), fromStringUnsafe("5")]],
        ]),
      },
      {
        predicate: {
          filters: [{ field: "price", operator: "gte", value: fromStringUnsafe("5") }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "price", direction: "asc" }],
        storageOrderBy: [{ field: "price", direction: "asc" }],
        matches: () => {
          throw new Error("ordered exact BigDecimal range should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: 3,
      },
    );

    expect(orderedBigDecimalRange.keys).toStrictEqual(["high", "equal", "low"]);
    expect(orderedBigDecimalRange.totalRows).toBe(3);

    const nonExactBigIntRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gte", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntRange.keys).toStrictEqual(["equal", "high", "missing-quantity"]);
    expect(nonExactBigIntRange.totalRows).toBe(3);

    const nonExactBigIntUpperRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "lte", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntUpperRange.keys).toStrictEqual(["equal", "low", "missing-quantity"]);
    expect(nonExactBigIntUpperRange.totalRows).toBe(3);

    const nonExactBigIntLowerRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "quantity", operator: "gt", value: 10n }],
        callbackRequired: false,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(nonExactBigIntLowerRange.keys).toStrictEqual(["high", "missing-quantity"]);
    expect(nonExactBigIntLowerRange.totalRows).toBe(2);

    const GenericName = Schema.Struct({
      id: Schema.String,
      label: Schema.Unknown,
    });
    const genericMetadata = rawQueryCompilerMetadata(GenericName);
    const genericRows = [
      { id: "match", label: "customer-a" },
      { id: "miss", label: "account-a" },
    ];
    const genericStartsWith = scanTopicRawWindow(
      {
        columns: makeColumns(genericMetadata, [
          ["id", genericRows.map((row) => row.id)],
          ["label", genericRows.map((row) => row.label)],
        ]),
        orderedSlotIndexes: new Map(),
        rawQueryMetadata: genericMetadata,
        scalarPredicateIndexes: createScalarPredicateIndexes(),
        slots: genericRows.map((row) => ({
          key: row.id,
          row,
        })),
      },
      {
        predicate: {
          filters: [{ field: "label", operator: "startsWith", value: "customer-" }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact generic startsWith should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      },
    );

    expect(genericStartsWith.keys).toStrictEqual(["match"]);
    expect(genericStartsWith.totalRows).toBe(1);

    const orderedCandidate = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "quantity", operator: "gt", value: 0n },
          {
            field: "symbol",
            operator: "in",
            values: ["MSFT"],
            valueKeys: new Set(["string:4:MSFT"]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "quantity", direction: "asc" }],
      storageOrderBy: [{ field: "quantity", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedCandidate.keys).toStrictEqual(["high"]);
    expect(orderedCandidate.totalRows).toBe(1);
    expect(state.scalarPredicateIndexes.has("symbol")).toBe(true);

    const openScalarBucket = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "eq", value: "AAPL" }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(openScalarBucket?.slots).toStrictEqual([0, 1]);

    const existingBucketOverBudget = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "in", values: ["AAPL", "NVDA"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(existingBucketOverBudget).toBeUndefined();

    const existingIndexWithMissingBucketBuildDisabled = selectedPredicateCandidateSlots(
      state,
      [{ field: "symbol", operator: "in", values: ["AAPL", "NVDA"] }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(existingIndexWithMissingBucketBuildDisabled).toBeUndefined();

    const orderedMissingScalarBucket = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          { field: "quantity", operator: "gt", value: 0n },
          { field: "symbol", operator: "eq", value: "NVDA" },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "quantity", direction: "asc" }],
      storageOrderBy: [{ field: "quantity", direction: "asc" }],
      matches: () => {
        throw new Error("ordered missing scalar candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedMissingScalarBucket.keys).toStrictEqual([]);
    expect(orderedMissingScalarBucket.totalRows).toBe(0);
    expect(state.scalarPredicateIndexes.has("symbol")).toBe(true);
    const symbolIndex = state.scalarPredicateIndexes.get("symbol")!;
    expect(symbolIndex.buckets.has("string:4:NVDA")).toBe(false);
  });

  it("skips scalar index maintenance when an indexed field is absent from stored columns", () => {
    const scalarPredicateIndexes = createScalarPredicateIndexes();
    scalarPredicateIndexes.set("missing", {
      buckets: new Map([["string:4:open", new Set([0])]]),
      indexedKeys: new Set(["string:4:open"]),
      normalizedStringBuckets: new Map(),
      normalizedStringModes: new Map(),
      orderedBucketSlots: new Map(),
    });

    addSlotToScalarPredicateIndexes(scalarPredicateIndexes, new Map(), 0);
    removeSlotFromScalarPredicateIndexes(scalarPredicateIndexes, new Map(), 0);

    const missingIndex = scalarPredicateIndexes.get("missing")!;
    const openBucket = missingIndex.buckets.get("string:4:open")!;
    expect(openBucket.has(0)).toBe(true);
  });

  it("rejects exact candidates that are not smaller than their scan budget", () => {
    const rows = [order("a", "open", 1, 1), order("b", "closed", 2, 2), order("c", "open", 3, 3)];
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", [rows[0]!.status, rows[1]!.status, { nonScalar: true }]],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const valueKeysTakePrecedence = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact value-key candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(valueKeysTakePrecedence.keys).toStrictEqual(["a"]);
    expect(valueKeysTakePrecedence.totalRows).toBe(1);

    const orderedInWithoutValueKeys = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["open"],
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact in candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedInWithoutValueKeys.keys).toStrictEqual(["a"]);
    expect(orderedInWithoutValueKeys.totalRows).toBe(1);

    const orderedValueKeysTakePrecedence = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["closed"],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact value-key candidates should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedValueKeysTakePrecedence.keys).toStrictEqual(["a"]);
    expect(orderedValueKeysTakePrecedence.totalRows).toBe(1);

    const orderedValueKeySizeMismatch = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: [],
            valueKeys: new Set([scalarEqualityKey("open")!]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "status", direction: "asc" }],
      storageOrderBy: [{ field: "status", direction: "asc" }],
      matches: () => {
        throw new Error("ordered exact value-key mismatch should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 2,
    });

    expect(orderedValueKeySizeMismatch.keys).toStrictEqual(["a"]);
    expect(orderedValueKeySizeMismatch.totalRows).toBe(1);

    const nonScalarInWithoutValueKeys = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: [{ structured: true }] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(nonScalarInWithoutValueKeys).toBeUndefined();

    const broadScalar = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["open", "closed"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(broadScalar).toBeUndefined();

    const missingScalarWithNonScalarColumnEntry = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["missing", "absent"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(missingScalarWithNonScalarColumnEntry?.slots).toStrictEqual([]);
    const statusIndex = state.scalarPredicateIndexes.get("status")!;
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("missing")!)).toBe(false);
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("absent")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("missing")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("absent")!)).toBe(false);

    const singleMissingScalar = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "eq", value: "single-missing" }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: state.slots.length,
      },
    );

    expect(singleMissingScalar?.slots).toStrictEqual([]);
    expect(statusIndex.indexedKeys.has(scalarEqualityKey("single-missing")!)).toBe(false);
    expect(statusIndex.buckets.has(scalarEqualityKey("single-missing")!)).toBe(false);

    const zeroBudgetCandidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: [] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 0,
      },
    );

    expect(zeroBudgetCandidate).toBeUndefined();

    const warmPriceIndex = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 0 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("exact range index warmup should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 3,
    });

    expect(warmPriceIndex.totalRows).toBe(3);

    const fullRange = selectedPredicateCandidateSlots(
      state,
      [{ field: "price", operator: "gte", value: 1 }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
      },
    );

    expect(fullRange).toBeUndefined();

    const rangeNotSmallerThanBudget = selectedPredicateCandidateSlots(
      state,
      [{ field: "price", operator: "gte", value: 2 }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      },
    );

    expect(rangeNotSmallerThanBudget).toBeUndefined();
  });

  it("abandons scalar and range candidates as soon as they exceed the scan budget", () => {
    const rows = [
      order("a", "open", 1, 1),
      order("b", "open", 2, 2),
      order("c", "closed", 3, 3),
      order("d", "cancelled", 4, 4),
    ];
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({ key: row.id, row })),
    };

    expect(
      selectedPredicateCandidateSlots(state, [{ field: "status", operator: "eq", value: "open" }], {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: rows.length,
      })?.slots,
    ).toStrictEqual([0, 1]);
    expect(
      selectedPredicateCandidateSlots(state, [{ field: "status", operator: "eq", value: "open" }], {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: 1,
      }),
    ).toBeUndefined();
    expect(
      selectedPredicateCandidateSlots(
        state,
        [{ field: "status", operator: "eq", value: "closed" }],
        {
          allowScalarIndexBuild: true,
          exactRangeCandidates: true,
          maxSlotCount: 0,
        },
      ),
    ).toBeUndefined();
    expect(
      selectedPredicateCandidateSlots(
        state,
        [{ field: "status", operator: "in", values: ["closed", "cancelled"] }],
        {
          allowScalarIndexBuild: true,
          exactRangeCandidates: true,
          maxSlotCount: 1,
        },
      ),
    ).toBeUndefined();

    scanTopicRawWindow(state, {
      predicate: { filters: [], callbackRequired: false, callbackSkippable: true },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 1,
    });
    expect(
      selectedPredicateCandidateSlots(state, [{ field: "price", operator: "gte", value: 2 }], {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: 2,
      }),
    ).toBeUndefined();
  });

  it("does not materialize broad scalar candidate buckets during raw scans", () => {
    const openKey = scalarEqualityKey("open");
    const rows = [
      ...Array.from({ length: 100_001 }, (_value, index) =>
        order(`open-${index}`, "open", index, index),
      ),
      order("closed-row", "closed", 0, 0),
    ];
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const result = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["open"],
            valueKeys: new Set([openKey]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("broad exact scalar fallback should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 1,
    });

    expect(result.keys).toStrictEqual(["open-0"]);
    expect(result.totalRows).toBe(rows.length - 1);
    expect(state.scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("evicts exact and normalized predicate buckets that grow past the retained budget", () => {
    const openKey = scalarEqualityKey("open");
    const normalizedOpenKey = "normalized-string:0:0:4:open";
    const rows = Array.from(
      { length: maxRetainedScalarPredicateBucketSlots + 1 },
      (_value, index) => order(`open-${index}`, "open", index, index),
    );
    const metadata = rawQueryCompilerMetadata(Order);
    const scalarPredicateIndexes = createScalarPredicateIndexes();
    const broadBucket = new Set(
      Array.from({ length: maxRetainedScalarPredicateBucketSlots }, (_value, index) => index),
    );
    scalarPredicateIndexes.set("status", {
      buckets: new Map([
        [openKey, broadBucket],
        [normalizedOpenKey, broadBucket],
      ]),
      indexedKeys: new Set([openKey, normalizedOpenKey]),
      normalizedStringBuckets: new Map([
        [
          normalizedOpenKey,
          {
            normalizedValue: "open",
            caseSensitive: false,
            accentSensitive: false,
          },
        ],
      ]),
      normalizedStringModes: new Map([
        [
          "0:0",
          {
            accentSensitive: false,
            caseSensitive: false,
            count: 1,
          },
        ],
      ]),
      orderedBucketSlots: new Map(),
    });

    addSlotToScalarPredicateIndexes(
      scalarPredicateIndexes,
      makeColumns(metadata, [["status", rows.map((row) => row.status)]]),
      maxRetainedScalarPredicateBucketSlots,
    );

    expect(scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("does not retain newly built single-key scalar predicate buckets beyond the retained budget", () => {
    const rows = Array.from(
      { length: maxRetainedScalarPredicateBucketSlots + 1 },
      (_value, index) => order(`open-${index}`, "open", index, index),
    );
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const candidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "eq", value: "open" }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: maxRetainedScalarPredicateBucketSlots + 2,
      },
    );

    expect(candidate).toBeUndefined();
    expect(state.scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("does not retain newly built multi-key scalar predicate buckets beyond the retained budget", () => {
    const rows = Array.from(
      { length: maxRetainedScalarPredicateBucketSlots + 1 },
      (_value, index) => order(`open-${index}`, "open", index, index),
    );
    rows.push(order("closed-row", "closed", 0, 0));
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const candidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["open", "closed"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: maxRetainedScalarPredicateBucketSlots + 3,
      },
    );

    expect(candidate).toBeUndefined();
    expect(state.scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("evicts already broad single-key scalar predicate buckets during candidate selection", () => {
    const openKey = scalarEqualityKey("open");
    const rows = Array.from(
      { length: maxRetainedScalarPredicateBucketSlots + 1 },
      (_value, index) => order(`open-${index}`, "open", index, index),
    );
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };
    state.scalarPredicateIndexes.set("status", {
      buckets: new Map([
        [
          openKey,
          new Set(
            Array.from(
              { length: maxRetainedScalarPredicateBucketSlots + 1 },
              (_value, index) => index,
            ),
          ),
        ],
      ]),
      indexedKeys: new Set([openKey]),
      normalizedStringBuckets: new Map(),
      normalizedStringModes: new Map(),
      orderedBucketSlots: new Map(),
    });

    const candidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "eq", value: "open" }],
      {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        maxSlotCount: maxRetainedScalarPredicateBucketSlots + 1,
      },
    );

    expect(candidate).toBeUndefined();
    expect(state.scalarPredicateIndexes.has("status")).toBe(false);
  });

  it("evicts already broad scalar predicate buckets during candidate selection", () => {
    const openKey = scalarEqualityKey("open");
    const closedKey = scalarEqualityKey("closed");
    const rows = Array.from(
      { length: maxRetainedScalarPredicateBucketSlots + 1 },
      (_value, index) => order(`open-${index}`, "open", index, index),
    );
    rows.push(order("closed-row", "closed", 0, 0));
    const metadata = rawQueryCompilerMetadata(Order);
    const statusColumn: TopicColumnValues = {
      get: () => {
        throw new Error("over-budget cached scalar buckets should not be rebuilt");
      },
      kind: "generic",
      length: rows.length,
    };
    const state = {
      columns: new Map([["status", statusColumn]]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };
    state.scalarPredicateIndexes.set("status", {
      buckets: new Map([
        [
          openKey,
          new Set(
            Array.from(
              { length: maxRetainedScalarPredicateBucketSlots + 1 },
              (_value, index) => index,
            ),
          ),
        ],
        [closedKey, new Set([maxRetainedScalarPredicateBucketSlots + 1])],
      ]),
      indexedKeys: new Set([openKey, closedKey]),
      normalizedStringBuckets: new Map(),
      normalizedStringModes: new Map(),
      orderedBucketSlots: new Map(),
    });

    const candidate = selectedPredicateCandidateSlots(
      state,
      [{ field: "status", operator: "in", values: ["open", "closed"] }],
      {
        allowScalarIndexBuild: true,
        exactRangeCandidates: true,
        maxSlotCount: maxRetainedScalarPredicateBucketSlots + 1,
      },
    );

    const statusIndex = state.scalarPredicateIndexes.get("status")!;
    expect(candidate).toBeUndefined();
    expect(statusIndex.indexedKeys.has(openKey)).toBe(false);
    expect(statusIndex.buckets.has(openKey)).toBe(false);
    expect(statusIndex.indexedKeys.has(closedKey)).toBe(true);
    expect(statusIndex.buckets.get(closedKey)).toStrictEqual(
      new Set([maxRetainedScalarPredicateBucketSlots + 1]),
    );
  });

  it("counts exact broad zero-row scans without reading row objects", () => {
    const openKey = scalarEqualityKey("open");
    const rows = [
      ...Array.from({ length: 100_001 }, (_value, index) =>
        order(`open-${index}`, "open", index, index),
      ),
      order("closed-row", "closed", 0, 0),
    ];
    const metadata = rawQueryCompilerMetadata(Order);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
        ["status", rows.map((row) => row.status)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => {
        const entry = {
          key: row.id,
          row,
        };
        Object.defineProperty(entry, "row", {
          get: () => {
            throw new Error("exact zero-row count scan should not read row objects");
          },
        });
        return entry;
      }),
    };

    const result = scanTopicRawWindow(state, {
      predicate: {
        filters: [
          {
            field: "status",
            operator: "in",
            values: ["open"],
            valueKeys: new Set([openKey]),
          },
        ],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact zero-row count scan should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: 0,
    });

    expect(result.keys).toStrictEqual([]);
    expect(result.totalRows).toBe(rows.length - 1);
  });

  it("keeps generic numeric range predicate fallbacks conservative", () => {
    const GenericMetric = Schema.Struct({
      id: Schema.String,
      price: Schema.Unknown,
    });
    const rows = [
      { id: "nan", price: Number.NaN },
      { id: "low", price: 1 },
      { id: "high", price: 3 },
    ];
    const metadata = rawQueryCompilerMetadata(GenericMetric);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const exactRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 2 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact generic numeric range should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(exactRange.keys).toStrictEqual(["high"]);
    expect(exactRange.totalRows).toBe(1);

    const exactRangeBoundary = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gte", value: 1 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact generic numeric boundary range should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(exactRangeBoundary.keys).toStrictEqual(["high", "low"]);
    expect(exactRangeBoundary.totalRows).toBe(2);

    const exactNonFiniteRange = scanTopicRawWindow(
      {
        ...state,
        columns: makeColumns(metadata, [
          ["id", rows.map((row) => row.id)],
          ["price", [Number.POSITIVE_INFINITY, 1, 3]],
        ]),
      },
      {
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 2 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact non-finite numeric range should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      },
    );
    expect(exactNonFiniteRange.keys).toStrictEqual(["high"]);
    expect(exactNonFiniteRange.totalRows).toBe(1);

    const nonExactRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 2 }],
        callbackRequired: true,
      },
      orderBy: [],
      matches: (row) => fieldValue(row, "id") !== "nan",
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(nonExactRange.keys).toStrictEqual(["high"]);
    expect(nonExactRange.totalRows).toBe(1);

    const nonExactNonFiniteRange = scanTopicRawWindow(
      {
        ...state,
        columns: makeColumns(metadata, [
          ["id", rows.map((row) => row.id)],
          ["price", [Number.POSITIVE_INFINITY, 1, 3]],
        ]),
      },
      {
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 2 }],
          callbackRequired: true,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      },
    );
    expect(nonExactNonFiniteRange.keys).toStrictEqual(["high", "nan"]);
    expect(nonExactNonFiniteRange.totalRows).toBe(2);

    const exactNotEqual = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "neq", value: 1 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact generic numeric neq should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(exactNotEqual.keys).toStrictEqual(["high", "nan"]);
    expect(exactNotEqual.totalRows).toBe(2);
  });

  it("keeps number-column non-finite range predicates aligned with query semantics", () => {
    const NumericMetric = Schema.Struct({
      id: Schema.String,
      price: Schema.Number,
    });
    const rows = [
      { id: "infinite", price: Number.POSITIVE_INFINITY },
      { id: "low", price: 1 },
      { id: "high", price: 3 },
    ];
    const metadata = rawQueryCompilerMetadata(NumericMetric);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const exactRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 2 }],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [],
      matches: () => {
        throw new Error("exact number-column non-finite range should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(exactRange.keys).toStrictEqual(["high"]);
    expect(exactRange.totalRows).toBe(1);

    const nonExactRange = scanTopicRawWindow(state, {
      predicate: {
        filters: [{ field: "price", operator: "gt", value: 2 }],
        callbackRequired: true,
      },
      orderBy: [],
      matches: () => true,
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });
    expect(nonExactRange.keys).toStrictEqual(["high", "infinite"]);
    expect(nonExactRange.totalRows).toBe(2);
  });

  it("falls back to query-value ordering for invalid number-column slots", () => {
    const NumericMetric = Schema.Struct({
      id: Schema.String,
      price: Schema.Number,
    });
    const rows = [
      { id: "valid", price: 1 },
      { id: "invalid", price: "not-a-number" },
      { id: "infinite", price: Number.POSITIVE_INFINITY },
      { id: "nan", price: Number.NaN },
    ];
    const metadata = rawQueryCompilerMetadata(NumericMetric);
    const state = {
      columns: makeColumns(metadata, [
        ["id", rows.map((row) => row.id)],
        ["price", rows.map((row) => row.price)],
      ]),
      orderedSlotIndexes: new Map(),
      rawQueryMetadata: metadata,
      scalarPredicateIndexes: createScalarPredicateIndexes(),
      slots: rows.map((row) => ({
        key: row.id,
        row,
      })),
    };

    const result = scanTopicRawWindow(state, {
      predicate: {
        filters: [],
        callbackRequired: false,
        callbackSkippable: true,
      },
      orderBy: [{ field: "price", direction: "asc" }],
      storageOrderBy: [{ field: "price", direction: "asc" }],
      matches: () => {
        throw new Error("exact ordered numeric scan should not call row callbacks");
      },
      compare: (left, right) => left.key.localeCompare(right.key),
      offset: 0,
      limit: undefined,
    });

    expect(result.keys).toStrictEqual(["invalid", "valid", "infinite", "nan"]);
    expect(result.totalRows).toBe(4);
  });

  it.effect("uses bigint range hints conservatively for manual plans", () =>
    Effect.gen(function* () {
      const store = new TopicStore("positions", Position, "id", () => {});
      yield* publishTopicStoreRow(store, position("low", "AAPL", 5n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, position("equal", "AAPL", 10n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(store, position("high", "AAPL", 20n, "10"), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readModel = topicStoreTestQueryInterface(store);
      const rangeHinted = readModel.scanRawWindow({
        predicate: {
          filters: [{ field: "quantity", operator: "gt", value: 10n }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(rangeHinted.keys).toStrictEqual(["high"]);
      expect(rangeHinted.totalRows).toBe(1);
    }),
  );

  it.effect("handles exact not-equal column predicates without row callbacks", () =>
    Effect.gen(function* () {
      const orderStore = new TopicStore("orders", Order, "id", () => {});
      yield* publishTopicStoreRow(orderStore, order("cheap", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(orderStore, order("excluded", "open", 20, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(orderStore, order("expensive", "open", 30, 3), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const orderReadModel = topicStoreTestQueryInterface(orderStore);
      const exactNumberNotEqual = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "neq", value: 20 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact numeric not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(exactNumberNotEqual.keys).toStrictEqual(["cheap", "expensive"]);
      expect(exactNumberNotEqual.totalRows).toBe(2);

      const MixedScalar = Schema.Struct({
        id: Schema.String,
        value: Schema.optionalKey(Schema.Union([Schema.Number, Schema.BigInt])),
      });
      const mixedStore = new TopicStore("mixed-scalars", MixedScalar, "id", () => {});
      yield* publishTopicStoreRow(mixedStore, { id: "missing" }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(mixedStore, { id: "matching", value: 1 }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(mixedStore, { id: "different", value: 2n }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      const mixedNotEqual = topicStoreTestQueryInterface(mixedStore).scanRawWindow({
        predicate: {
          filters: [{ field: "value", operator: "neq", value: 1 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact mixed-union not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(mixedNotEqual.keys).toStrictEqual(["different", "missing"]);
      expect(mixedNotEqual.totalRows).toBe(2);

      const manualRangeHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: 10 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualRangeHint.keys).toStrictEqual(["excluded", "expensive"]);
      expect(manualRangeHint.totalRows).toBe(2);

      const manualNotEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "neq", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualNotEqualHint.keys).toStrictEqual(["cheap", "expensive"]);
      expect(manualNotEqualHint.totalRows).toBe(2);

      const manualGreaterThanOrEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gte", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualGreaterThanOrEqualHint.keys).toStrictEqual(["excluded", "expensive"]);
      expect(manualGreaterThanOrEqualHint.totalRows).toBe(2);

      const manualLessThanHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "lt", value: 30 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualLessThanHint.keys).toStrictEqual(["cheap", "excluded"]);
      expect(manualLessThanHint.totalRows).toBe(2);

      const manualLessThanOrEqualHint = orderReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "lte", value: 20 }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualLessThanOrEqualHint.keys).toStrictEqual(["cheap", "excluded"]);
      expect(manualLessThanOrEqualHint.totalRows).toBe(2);

      const positionStore = new TopicStore("positions", Position, "id", () => {});
      yield* publishTopicStoreRow(
        positionStore,
        position("drop", "DROP", 20n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("excluded-price", "AAPL", 20n, "99"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("excluded-quantity", "AAPL", 10n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(
        positionStore,
        position("keep", "AAPL", 20n, "10"),
        (topic, message) => InvalidRowError.make({ topic, message }),
      );

      const positionReadModel = topicStoreTestQueryInterface(positionStore);
      const manualBigDecimalRangeHint = positionReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "price", operator: "gt", value: fromStringUnsafe("20") }],
          callbackRequired: false,
        },
        orderBy: [],
        matches: () => true,
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });
      expect(manualBigDecimalRangeHint.keys).toStrictEqual(["excluded-price"]);
      expect(manualBigDecimalRangeHint.totalRows).toBe(1);

      const exactMixedNotEqual = positionReadModel.scanRawWindow({
        predicate: {
          filters: [
            { field: "symbol", operator: "neq", value: "DROP" },
            { field: "quantity", operator: "neq", value: 10n },
            { field: "price", operator: "neq", value: fromStringUnsafe("99") },
          ],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact mixed not-equal predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(exactMixedNotEqual.keys).toStrictEqual(["keep"]);
      expect(exactMixedNotEqual.totalRows).toBe(1);

      const LooseNumber = Schema.Struct({
        id: Schema.String,
        value: Schema.Number,
      });
      const looseNumberStore = new TopicStore("loose-numbers", LooseNumber, "id", () => {});
      yield* publishTopicStoreRow(
        looseNumberStore,
        { id: "nan", value: Number.NaN },
        (topic, message) => InvalidRowError.make({ topic, message }),
      );
      yield* publishTopicStoreRow(looseNumberStore, { id: "real", value: 20 }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const looseNumberReadModel = topicStoreTestQueryInterface(looseNumberStore);
      const exactFiniteRange = looseNumberReadModel.scanRawWindow({
        predicate: {
          filters: [{ field: "value", operator: "gt", value: 10 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [],
        matches: () => {
          throw new Error("exact finite range predicates should not call row callbacks");
        },
        compare: (left, right) => left.key.localeCompare(right.key),
        offset: 0,
        limit: undefined,
      });

      expect(exactFiniteRange.keys).toStrictEqual(["real"]);
      expect(exactFiniteRange.totalRows).toBe(1);
    }),
  );
});
