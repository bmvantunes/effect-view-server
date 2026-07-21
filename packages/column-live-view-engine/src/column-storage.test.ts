import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import type { TopicRowEntry } from "./row-scan";
import type { TopicRawOrderByPlan } from "./raw-window-scan";
import {
  columnScalarEqualityKey,
  columnValue,
  createTopicColumnValues,
  createTopicColumnValuesFromArray,
} from "./topic-column-vector";
import {
  columnValueDoesNotEqual,
  compareExactRangeColumnValue,
  compareRangeColumnValue,
} from "./topic-range-value";
import { orderedSlotBoundIndex, type OrderedSlotIndex } from "./topic-ordered-window";
import { removeSlotFromRawWindowIndexes } from "./topic-raw-ordered-window-index";

import { Order, Position } from "../test-harness/public-engine";

import type { PositionRow } from "../test-harness/public-engine";

it("derives topic column vectors from schema metadata and preserves slot mutation semantics", () => {
  const Metric = Schema.Struct({
    finitePrice: Schema.Finite,
    id: Schema.String,
    decimalPrice: Schema.BigDecimal,
    optionalPrice: Schema.optionalKey(Schema.Number),
    price: Schema.Number,
    quantity: Schema.BigInt,
    status: Schema.String,
  });
  const metadata = rawQueryCompilerMetadata(Metric);

  const price = createTopicColumnValues("price", metadata);
  const finitePrice = createTopicColumnValues("finitePrice", metadata);
  price.reserve(32);
  price.set(20, 42);
  price.set(21, undefined);
  price.copySlot(0, 20);
  price.copySlot(1, 21);
  price.copySlot(24, 20);
  price.pop();

  expect(price.kind).toBe("number");
  expect(finitePrice.kind).toBe("number");
  expect(price.length).toBe(24);
  expect(columnValue(price, 0)).toBe(42);
  expect(columnValue(price, 1)).toBeUndefined();
  expect(columnValue(price, 23)).toBeUndefined();
  expect(columnValue(price, 24)).toBeUndefined();
  expect(columnValue(price, -1)).toBeUndefined();

  price.clear();
  price.copySlot(2, 99);
  expect(price.length).toBe(3);
  expect(columnValue(price, 2)).toBeUndefined();

  price.clear();
  price.pop();
  expect(price.length).toBe(0);

  const status = createTopicColumnValues("status", metadata);
  status.reserve(4);
  status.set(0, "open");
  status.set(1, { structured: true });
  status.copySlot(0, 1);
  status.pop();

  expect(status.kind).toBe("string");
  expect(status.length).toBe(1);
  expect(columnValue(status, 0)).toBeUndefined();
  expect(columnValue(status, -1)).toBeUndefined();
  expect(columnValue(status, 1)).toBeUndefined();

  status.clear();
  expect(status.length).toBe(0);

  const statusKeys = createTopicColumnValues("status", metadata);
  const optionalPrice = createTopicColumnValuesFromArray("optionalPrice", metadata, [1, undefined]);
  const numberKeys = createTopicColumnValues("optionalPrice", metadata);
  const quantity = createTopicColumnValues("quantity", metadata);
  const decimalPrice = createTopicColumnValues("decimalPrice", metadata);
  const generic = createTopicColumnValues("unknown", metadata);
  statusKeys.reserve(8);
  statusKeys.set(0, "a:b");
  numberKeys.reserve(8);
  numberKeys.set(0, -0);
  numberKeys.set(1, Number.NaN);
  numberKeys.set(2, Number.POSITIVE_INFINITY);
  numberKeys.set(3, Number.NEGATIVE_INFINITY);
  quantity.reserve(8);
  quantity.set(0, 1n);
  quantity.set(1, 2n);
  decimalPrice.reserve(8);
  decimalPrice.set(0, fromStringUnsafe("1.25"));
  decimalPrice.set(1, "not-a-decimal");
  generic.reserve(8);

  expect(optionalPrice.kind).toBe("number");
  expect(quantity.kind).toBe("bigint");
  expect(decimalPrice.kind).toBe("bigDecimal");
  expect(generic.kind).toBe("generic");
  expect(columnScalarEqualityKey(statusKeys, 0)).toBe("string:3:a:b");
  expect(columnScalarEqualityKey(statusKeys, 1)).toBeUndefined();
  expect(columnValue(optionalPrice, 0)).toBe(1);
  expect(columnValue(optionalPrice, 1)).toBeUndefined();
  expect(columnValue(quantity, 0)).toBe(1n);
  expect(columnValue(quantity, -1)).toBeUndefined();
  expect(columnValue(decimalPrice, 0)).toStrictEqual(fromStringUnsafe("1.25"));
  expect(columnValue(decimalPrice, 1)).toBeUndefined();
  expect(generic.length).toBe(0);

  expect(columnScalarEqualityKey(optionalPrice, 0)).toBe("number:1");
  expect(columnScalarEqualityKey(optionalPrice, 1)).toBeUndefined();
  expect(columnScalarEqualityKey(numberKeys, 0)).toBe("number:0");
  expect(columnScalarEqualityKey(numberKeys, 1)).toBe("number:NaN");
  expect(columnScalarEqualityKey(numberKeys, 2)).toBe("number:Infinity");
  expect(columnScalarEqualityKey(numberKeys, 3)).toBe("number:-Infinity");
  expect(columnScalarEqualityKey(quantity, 0)).toBe("bigint:1");
  expect(columnScalarEqualityKey(quantity, -1)).toBeUndefined();
  expect(columnScalarEqualityKey(decimalPrice, 0)).toBe('bigDecimal:["125","2"]');
  expect(columnScalarEqualityKey(decimalPrice, 1)).toBeUndefined();

  generic.set(0, "generic");
  generic.set(1, { unsupported: true });
  expect(columnScalarEqualityKey(generic, 0)).toBe("string:7:generic");
  expect(columnScalarEqualityKey(generic, 1)).toBeUndefined();
});

it("keeps scalar range helpers exact for numeric runtime domains", () => {
  expect(compareExactRangeColumnValue(1, Number.NaN)).toBeUndefined();
  expect(compareExactRangeColumnValue(2n, 2n)).toBe(0);
  expect(compareExactRangeColumnValue(1n, 2n)).toBe(-1);
  expect(compareExactRangeColumnValue(3n, 2n)).toBe(1);
  expect(compareExactRangeColumnValue(fromStringUnsafe("2"), fromStringUnsafe("1"))).toBe(1);
  expect(compareExactRangeColumnValue("2", 2)).toBeUndefined();

  expect(compareRangeColumnValue(Number.POSITIVE_INFINITY, 1)).toBeUndefined();
  expect(compareRangeColumnValue(2n, 2n)).toBe(0);
  expect(compareRangeColumnValue(1n, 2n)).toBe(-1);
  expect(compareRangeColumnValue(3n, 2n)).toBe(1);
  expect(compareRangeColumnValue(fromStringUnsafe("1"), fromStringUnsafe("2"))).toBe(-1);
  expect(compareRangeColumnValue("2", 2)).toBeUndefined();

  expect(columnValueDoesNotEqual(fromStringUnsafe("1"), fromStringUnsafe("2"))).toBe(true);
  expect(columnValueDoesNotEqual(fromStringUnsafe("1"), 1)).toBe(false);
  expect(columnValueDoesNotEqual(1, Number.NaN)).toBe(false);
  expect(columnValueDoesNotEqual(true, false)).toBe(true);
  expect(columnValueDoesNotEqual("1", 1)).toBe(false);
});

it("uses typed column values for ordered slot bound indexes", () => {
  const Metric = Schema.Struct({
    decimalPrice: Schema.BigDecimal,
    id: Schema.String,
    price: Schema.Number,
    quantity: Schema.BigInt,
    status: Schema.String,
  });
  const metadata = rawQueryCompilerMetadata(Metric);
  const status = createTopicColumnValuesFromArray("status", metadata, ["closed", "open", "open"]);
  const sparseStatus = createTopicColumnValuesFromArray("status", metadata, [undefined, "open"]);
  const price = createTopicColumnValuesFromArray("price", metadata, [1, 2, 3]);
  const nonFinitePrice = createTopicColumnValuesFromArray("price", metadata, [Number.NaN]);
  const quantity = createTopicColumnValuesFromArray("quantity", metadata, [1n, 2n, 3n]);
  const decimalPrice = createTopicColumnValuesFromArray("decimalPrice", metadata, [
    fromStringUnsafe("1"),
    fromStringUnsafe("2"),
    fromStringUnsafe("3"),
  ]);
  const generic = createTopicColumnValuesFromArray("unknown", metadata, ["closed", "open"]);

  expect(orderedSlotBoundIndex([0, 1, 2], status, "open", (comparison) => comparison >= 0)).toBe(1);
  expect(
    orderedSlotBoundIndex([0, 1, 2], status, "aardvark", (comparison) => comparison >= 0),
  ).toBe(0);
  expect(orderedSlotBoundIndex([0, 1, 2], status, "open", (comparison) => comparison > 0)).toBe(3);
  expect(orderedSlotBoundIndex([0, 1], sparseStatus, "open", (comparison) => comparison >= 0)).toBe(
    1,
  );
  expect(orderedSlotBoundIndex([0, 1, 2], price, 2, (comparison) => comparison >= 0)).toBe(1);
  expect(orderedSlotBoundIndex([0], nonFinitePrice, 1, (comparison) => comparison > 0)).toBe(0);
  expect(orderedSlotBoundIndex([0, 1, 2], quantity, 2n, (comparison) => comparison >= 0)).toBe(1);
  expect(
    orderedSlotBoundIndex(
      [0, 1, 2],
      decimalPrice,
      fromStringUnsafe("2"),
      (comparison) => comparison >= 0,
    ),
  ).toBe(1);
  expect(orderedSlotBoundIndex([0, 1], generic, "open", (comparison) => comparison >= 0)).toBe(1);

  const priceAscendingOrderBy: ReadonlyArray<TopicRawOrderByPlan> = [
    { field: "price", direction: "asc" },
  ];
  const orderedSlotIndexes = new Map<string, OrderedSlotIndex>([
    [
      "5:price:asc",
      {
        orderBy: priceAscendingOrderBy,
        orderColumns: [],
        slots: [0, 1, 2],
      },
    ],
  ]);
  removeSlotFromRawWindowIndexes(
    {
      columns: new Map(),
      orderedSlotIndexes,
      rawQueryMetadata: metadata,
      slots: [],
    },
    99,
  );
  expect(orderedSlotIndexes.get("5:price:asc")?.slots).toStrictEqual([0, 1, 2]);
});

it("keeps schema field order for aligned column writes", () => {
  expect(rawQueryCompilerMetadata(Order).fieldOrder).toStrictEqual([
    "id",
    "customerId",
    "status",
    "price",
    "region",
    "updatedAt",
    "note",
  ]);
});

it.effect("uses compiled raw row comparators for schema scalar fields and stable key ties", () =>
  Effect.gen(function* () {
    const compiled = yield* prepareRuntimeRawQuery(
      "positions",
      rawQueryCompilerMetadata(Position),
      {
        select: ["id", "accountId", "symbol", "active", "quantity", "price"],
        orderBy: [
          { field: "symbol", direction: "asc" },
          { field: "quantity", direction: "desc" },
          { field: "price", direction: "asc" },
        ],
      },
    );
    const rows: ReadonlyArray<TopicRowEntry<PositionRow>> = [
      {
        key: "tie-b",
        row: {
          id: "tie-b",
          accountId: "A",
          symbol: "MSFT",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("2"),
        },
      },
      {
        key: "aapl",
        row: {
          id: "aapl",
          accountId: "A",
          symbol: "AAPL",
          active: true,
          quantity: 1n,
          price: fromStringUnsafe("9"),
        },
      },
      {
        key: "tie-a",
        row: {
          id: "tie-a",
          accountId: "A",
          symbol: "MSFT",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("2"),
        },
      },
      {
        key: "msft-cheap",
        row: {
          id: "msft-cheap",
          accountId: "A",
          symbol: "MSFT",
          active: true,
          quantity: 10n,
          price: fromStringUnsafe("1"),
        },
      },
      {
        key: "msft-low-quantity",
        row: {
          id: "msft-low-quantity",
          accountId: "A",
          symbol: "MSFT",
          active: true,
          quantity: 1n,
          price: fromStringUnsafe("1"),
        },
      },
    ];

    expect(rows.toSorted(compiled.plan.compare).map((entry) => entry.key)).toStrictEqual([
      "aapl",
      "msft-cheap",
      "tie-a",
      "tie-b",
      "msft-low-quantity",
    ]);
  }),
);

it.effect("falls back to stable raw row comparison for abnormal scalar order values", () =>
  Effect.gen(function* () {
    const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
      select: ["id", "price", "note", "updatedAt"],
      orderBy: [
        { field: "updatedAt", direction: "asc" },
        { field: "note", direction: "asc" },
      ],
    });
    const rows: ReadonlyArray<TopicRowEntry<object>> = [
      { key: "nan", row: { id: "nan", price: 1, note: "b", updatedAt: Number.NaN } },
      { key: "finite-b", row: { id: "finite-b", price: 1, note: "b", updatedAt: 1 } },
      { key: "finite-a", row: { id: "finite-a", price: 1, note: "a", updatedAt: 1 } },
      { key: "missing-note", row: { id: "missing-note", price: 1, updatedAt: 1 } },
      { key: "infinite", row: { id: "infinite", price: 1, note: "c", updatedAt: Infinity } },
    ];

    expect(rows.toSorted(compiled.plan.compare).map((entry) => entry.key)).toStrictEqual([
      "missing-note",
      "finite-a",
      "finite-b",
      "infinite",
      "nan",
    ]);
  }),
);

it.effect(
  "falls back to stable raw row comparison for abnormal bigint and BigDecimal order values",
  () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeRawQuery(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id", "quantity", "price"],
          orderBy: [
            { field: "quantity", direction: "asc" },
            { field: "price", direction: "asc" },
          ],
        },
      );
      const rows: ReadonlyArray<TopicRowEntry<object>> = [
        {
          key: "bigint-smaller",
          row: { id: "bigint-smaller", quantity: 1n, price: fromStringUnsafe("9") },
        },
        {
          key: "bigint-good-expensive",
          row: { id: "bigint-good-expensive", quantity: 2n, price: fromStringUnsafe("2") },
        },
        {
          key: "bigint-good-cheap",
          row: { id: "bigint-good-cheap", quantity: 2n, price: fromStringUnsafe("1") },
        },
        { key: "missing-price", row: { id: "missing-price", quantity: 2n } },
        {
          key: "string-quantity",
          row: { id: "string-quantity", quantity: "2", price: fromStringUnsafe("1") },
        },
      ];

      expect(rows.toSorted(compiled.plan.compare).map((entry) => entry.key)).toStrictEqual([
        "bigint-smaller",
        "missing-price",
        "bigint-good-cheap",
        "bigint-good-expensive",
        "string-quantity",
      ]);
    }),
);
