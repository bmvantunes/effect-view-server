import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { format, fromStringUnsafe, isBigDecimal } from "effect/BigDecimal";
import { InvalidRowError } from "./index";
import { TopicRowStorage } from "./topic-row-storage";
import { evaluateRawQuery } from "./active-query";
import { prepareRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import {
  instrument,
  Instrument,
  order,
  Order,
  position,
  Position,
} from "../test-harness/public-engine";

describe("Topic Store query execution", () => {
  it.effect("evaluates raw queries through the storage scan interface", () =>
    Effect.gen(function* () {
      const rows = [
        { key: "closed", row: order("closed", "closed", 1, 1) },
        { key: "open-z", row: order("open-z", "open", 20, 3) },
        { key: "open-a", row: order("open-a", "open", 20, 4) },
        { key: "open-low", row: order("open-low", "open", 10, 2) },
      ];
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id", "price"],
          where: {
            status: "open",
          },
          orderBy: [
            {
              field: "price",
              direction: "desc",
            },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "status",
                  operator: "eq",
                  value: "open",
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.orderBy).toStrictEqual([
              {
                field: "price",
                direction: "desc",
              },
            ]);
            const filtered = rows.filter((entry) => plan.matches(entry.row));
            const ordered = filtered.toSorted(plan.compare);
            const window = ordered.slice(
              plan.offset,
              plan.limit === undefined ? undefined : plan.offset + plan.limit,
            );
            return {
              keys: window.map((entry) => entry.key),
              window,
              totalRows: filtered.length,
            };
          },
          version: () => 7,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual(["open-a", "open-z", "open-low"]);
      expect(evaluation.rows).toStrictEqual([
        {
          id: "open-a",
          price: 20,
        },
        {
          id: "open-z",
          price: 20,
        },
        {
          id: "open-low",
          price: 10,
        },
      ]);
      expect(evaluation.totalRows).toBe(3);
      expect(evaluation.version).toBe(7);
    }),
  );

  it.effect("projects raw snapshots from carried storage slots", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id", "price"],
        },
      );
      let slotForKeyCalls = 0;
      const evaluation = evaluateRawQuery(
        {
          keyAtSlot: (slot) => `key-at-slot-${slot}`,
          projectRawRow: (slot) => ({
            id: `projected-from-slot-${slot}`,
            price: slot * 10,
          }),
          scanRawWindow: () => ({
            keys: ["key-at-slot-2"],
            window: [
              {
                key: "key-at-slot-2",
                row: order("row-object-should-not-project", "open", 1, 1),
                slot: 2,
              },
            ],
            totalRows: 1,
          }),
          slotForKey: () => {
            slotForKeyCalls += 1;
            return 99;
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: ["key-at-slot-2"],
        rows: [
          {
            id: "projected-from-slot-2",
            price: 20,
          },
        ],
        totalRows: 1,
        version: 1,
        window: [
          {
            key: "key-at-slot-2",
            row: {
              id: "projected-from-slot-2",
              price: 20,
            },
          },
        ],
      });
      expect(slotForKeyCalls).toBe(0);
    }),
  );

  it.effect("falls back to key lookup when a carried storage slot is stale", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id", "price"],
        },
      );
      let slotForKeyCalls = 0;
      const slotsByKey = new Map([["moved-row", 3]]);
      const evaluation = evaluateRawQuery(
        {
          keyAtSlot: (slot) => `different-key-at-slot-${slot}`,
          projectRawRow: (slot) => ({
            id: `projected-from-slot-${slot}`,
            price: slot * 10,
          }),
          scanRawWindow: () => ({
            keys: ["moved-row"],
            window: [
              {
                key: "moved-row",
                row: order("row-object-should-not-project", "open", 1, 1),
                slot: 0,
              },
            ],
            totalRows: 1,
          }),
          slotForKey: (key) => {
            slotForKeyCalls += 1;
            return slotsByKey.get(key);
          },
          version: () => 2,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: ["moved-row"],
        rows: [
          {
            id: "projected-from-slot-3",
            price: 30,
          },
        ],
        totalRows: 1,
        version: 2,
        window: [
          {
            key: "moved-row",
            row: {
              id: "projected-from-slot-3",
              price: 30,
            },
          },
        ],
      });
      expect(slotForKeyCalls).toBe(1);
    }),
  );

  it.effect("projects raw snapshots correctly after storage delete compacts slots", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("deleted", "open", 10, 1), invalidRow));
      storage.advanceVersion();
      storage.setPrepared(yield* storage.prepareRow(order("retained", "open", 20, 2), invalidRow));
      storage.advanceVersion();
      storage.setPrepared(yield* storage.prepareRow(order("moved", "open", 30, 3), invalidRow));
      storage.advanceVersion();

      storage.delete("retained");
      storage.advanceVersion();

      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id", "price", "updatedAt"],
          orderBy: [{ field: "price", direction: "desc" }],
          limit: 2,
        },
      );
      const evaluation = evaluateRawQuery(storage.readModel, compiled);

      expect(evaluation).toStrictEqual({
        keys: ["moved", "deleted"],
        rows: [
          {
            id: "moved",
            price: 30,
            updatedAt: 3,
          },
          {
            id: "deleted",
            price: 10,
            updatedAt: 1,
          },
        ],
        totalRows: 2,
        version: 4,
        window: [
          {
            key: "moved",
            row: {
              id: "moved",
              price: 30,
              updatedAt: 3,
            },
          },
          {
            key: "deleted",
            row: {
              id: "deleted",
              price: 10,
              updatedAt: 1,
            },
          },
        ],
      });
    }),
  );

  it.effect("passes scalar ordering semantics to custom storage scanners", () =>
    Effect.gen(function* () {
      const active = { key: "active", row: position("active", "AAPL", 1n, "1", true) };
      const activeTie = { key: "active-tie", row: position("active-tie", "AAPL", 1n, "1", true) };
      const inactive = { key: "inactive", row: position("inactive", "AAPL", 1n, "1", false) };
      const orderRows = [
        { key: "closed", row: order("closed", "closed", 1, 1) },
        { key: "open-high", row: order("open-high", "open", 20, 2) },
        { key: "open-low", row: order("open-low", "open", 10, 3) },
      ];
      const booleanCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          orderBy: [{ field: "active", direction: "asc" }],
        },
      );
      const orderCompiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: "open",
          },
          orderBy: [{ field: "price", direction: "asc" }],
        },
      );

      const booleanEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.compare(active, inactive)).toBe(1);
            expect(plan.compare(inactive, active)).toBe(-1);
            expect(plan.compare(active, activeTie)).toBe(-1);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 1,
        },
        booleanCompiled,
      );
      const orderEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            const filtered = orderRows.filter((entry) => plan.matches(entry.row));
            const ordered = filtered.toSorted(plan.compare);
            return {
              keys: ordered.map((entry) => entry.key),
              window: ordered,
              totalRows: filtered.length,
            };
          },
          version: () => 2,
        },
        orderCompiled,
      );

      expect(booleanEvaluation.totalRows).toBe(0);
      expect(orderEvaluation.keys).toStrictEqual(["open-low", "open-high"]);
      expect(orderEvaluation.version).toBe(2);
    }),
  );

  it.effect("passes typed scalar predicate plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: {
              neq: "cancelled",
              in: ["open", "closed"],
            },
            price: {
              neq: 50,
              gt: 1,
              gte: 2,
              lt: 100,
              lte: 99,
            },
            customerId: {
              startsWith: "customer-",
            },
            region: "emea",
          },
          orderBy: [
            {
              field: "region",
              direction: "asc",
            },
            {
              field: "price",
              direction: "desc",
            },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "status",
                  operator: "neq",
                  value: "cancelled",
                },
                {
                  field: "status",
                  operator: "in",
                  values: ["open", "closed"],
                  valueKeys: new Set(["string:4:open", "string:6:closed"]),
                },
                {
                  field: "price",
                  operator: "neq",
                  value: 50,
                },
                {
                  field: "price",
                  operator: "gt",
                  value: 1,
                },
                {
                  field: "price",
                  operator: "gte",
                  value: 2,
                },
                {
                  field: "price",
                  operator: "lt",
                  value: 100,
                },
                {
                  field: "price",
                  operator: "lte",
                  value: 99,
                },
                {
                  field: "customerId",
                  operator: "startsWith",
                  value: "customer-",
                },
                {
                  field: "region",
                  operator: "eq",
                  value: "emea",
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.orderBy).toStrictEqual([
              {
                field: "region",
                direction: "asc",
              },
              {
                field: "price",
                direction: "desc",
              },
            ]);
            expect(plan.matches(order("open-low", "open", 10, 2))).toBe(true);
            expect(plan.matches(order("cancelled", "cancelled", 10, 2))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 11,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(11);
    }),
  );

  it.effect("passes indexed scalar in predicate keys to the storage scan interface", () =>
    Effect.gen(function* () {
      const matchedPrice = fromStringUnsafe("1.0");
      const positionCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            active: {
              in: [true],
            },
            quantity: {
              in: [20n],
            },
            price: {
              in: [matchedPrice],
            },
          },
        },
      );

      const positionEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect({
              ...plan.predicate,
              filters: plan.predicate.filters.map((filter) =>
                filter.operator === "in"
                  ? {
                      ...filter,
                      values: filter.values.map((value) =>
                        isBigDecimal(value) ? format(value) : value,
                      ),
                    }
                  : filter,
              ),
            }).toStrictEqual({
              filters: [
                {
                  field: "active",
                  operator: "in",
                  values: [true],
                  valueKeys: new Set(["boolean:true"]),
                },
                {
                  field: "quantity",
                  operator: "in",
                  values: [20n],
                  valueKeys: new Set(["bigint:20"]),
                },
                {
                  field: "price",
                  operator: "in",
                  values: ["1"],
                  valueKeys: new Set(["bigDecimal:1"]),
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches(position("matched", "AAPL", 20n, "1", true))).toBe(true);
            expect(plan.matches(position("wrong-active", "AAPL", 20n, "1", false))).toBe(false);
            expect(plan.matches(position("wrong-quantity", "AAPL", 21n, "1", true))).toBe(false);
            expect(plan.matches(position("wrong-price", "AAPL", 20n, "2", true))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 3,
        },
        positionCompiled,
      );

      expect(positionEvaluation.keys).toStrictEqual([]);
      expect(positionEvaluation.rows).toStrictEqual([]);
      expect(positionEvaluation.totalRows).toBe(0);
      expect(positionEvaluation.version).toBe(3);

      const NullableMetric = Schema.Struct({
        id: Schema.String,
        note: Schema.NullOr(Schema.String),
      });
      const nullableCompiled = yield* prepareRawQuery<object, object>(
        "nullableMetrics",
        rawQueryCompilerMetadata(NullableMetric),
        {
          select: ["id"],
          where: {
            note: {
              in: [null, "x"],
            },
          },
        },
      );

      const nullableEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "note",
                  operator: "in",
                  values: [null, "x"],
                  valueKeys: new Set(["null", "string:1:x"]),
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches({ id: "null", note: null })).toBe(true);
            expect(plan.matches({ id: "x", note: "x" })).toBe(true);
            expect(plan.matches({ id: "y", note: "y" })).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 4,
        },
        nullableCompiled,
      );

      expect(nullableEvaluation.keys).toStrictEqual([]);
      expect(nullableEvaluation.rows).toStrictEqual([]);
      expect(nullableEvaluation.totalRows).toBe(0);
      expect(nullableEvaluation.version).toBe(4);
    }),
  );

  it.effect("passes typed bigint and bigdecimal range plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const excludedPrice = fromStringUnsafe("0");
      const maxPrice = fromStringUnsafe("100");
      const compiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            quantity: {
              gte: 10n,
            },
            price: {
              neq: excludedPrice,
              lt: maxPrice,
            },
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "quantity",
                  operator: "gte",
                  value: 10n,
                },
                {
                  field: "price",
                  operator: "neq",
                  value: excludedPrice,
                },
                {
                  field: "price",
                  operator: "lt",
                  value: maxPrice,
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            expect(plan.matches(position("aapl", "AAPL", 20n, "10"))).toBe(true);
            expect(plan.matches(position("goog", "GOOG", 1n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 12,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(12);
    }),
  );

  it.effect("passes typed numeric literal range plans to the storage scan interface", () =>
    Effect.gen(function* () {
      const LiteralMetrics = Schema.Struct({
        id: Schema.String,
        score: Schema.Literal(1),
        bucket: Schema.Literal(1n),
      });
      const compiled = yield* prepareRawQuery<object, object>(
        "literalMetrics",
        rawQueryCompilerMetadata(LiteralMetrics),
        {
          select: ["id"],
          where: {
            score: {
              gte: 1,
            },
            bucket: {
              lte: 1n,
            },
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "score",
                  operator: "gte",
                  value: 1,
                },
                {
                  field: "bucket",
                  operator: "lte",
                  value: 1n,
                },
              ],
              callbackRequired: false,
              callbackSkippable: true,
            });
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 18,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(18);
    }),
  );

  it.effect("keeps malformed scalar operators callback-only in the storage scan plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: {
              eq: undefined,
              in: [undefined],
            },
            price: {
              gt: undefined,
              gte: "9",
              lt: Number.NaN,
              lte: fromStringUnsafe("50"),
            },
            customerId: {
              startsWith: 1,
            },
            note: undefined,
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(order("open", "open", 10, 1))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 14,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
      expect(evaluation.version).toBe(14);

      const structuredScalarCompiled = yield* prepareRawQuery<object, object>(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: {
            status: ["open"],
            customerId: {
              eq: ["customer-open"],
            },
            region: {
              in: [["emea"]],
            },
            price: Number.NaN,
          },
        },
      );
      const structuredScalarEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(order("open", "open", 10, 1))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 17,
        },
        structuredScalarCompiled,
      );

      expect(structuredScalarEvaluation.keys).toStrictEqual([]);
      expect(structuredScalarEvaluation.rows).toStrictEqual([]);
      expect(structuredScalarEvaluation.totalRows).toBe(0);
      expect(structuredScalarEvaluation.version).toBe(17);

      const bigintCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            quantity: {
              neq: 1,
            },
          },
        },
      );
      const bigintEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("bad", "BAD", 10n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 15,
        },
        bigintCompiled,
      );

      expect(bigintEvaluation.keys).toStrictEqual([]);
      expect(bigintEvaluation.rows).toStrictEqual([]);
      expect(bigintEvaluation.totalRows).toBe(0);
      expect(bigintEvaluation.version).toBe(15);

      const bigDecimalCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            price: {
              lt: 100,
            },
          },
        },
      );
      const bigDecimalEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("bad-price", "BAD", 10n, "10"))).toBe(false);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 16,
        },
        bigDecimalCompiled,
      );

      expect(bigDecimalEvaluation.keys).toStrictEqual([]);
      expect(bigDecimalEvaluation.rows).toStrictEqual([]);
      expect(bigDecimalEvaluation.totalRows).toBe(0);
      expect(bigDecimalEvaluation.version).toBe(16);

      const booleanCompiled = yield* prepareRawQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: {
            active: {
              neq: true,
            },
          },
        },
      );
      const booleanEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(position("active", "ACT", 10n, "10", true))).toBe(false);
            expect(plan.matches(position("inactive", "INA", 10n, "10", false))).toBe(true);
            return {
              keys: [],
              window: [],
              totalRows: 0,
            };
          },
          version: () => 19,
        },
        booleanCompiled,
      );

      expect(booleanEvaluation.keys).toStrictEqual([]);
      expect(booleanEvaluation.rows).toStrictEqual([]);
      expect(booleanEvaluation.totalRows).toBe(0);
      expect(booleanEvaluation.version).toBe(19);

      const MixedNumeric = Schema.Struct({
        id: Schema.String,
        amount: Schema.Union([Schema.Number, Schema.BigInt, Schema.BigDecimal]),
      });
      const mixedNumericRangeError = yield* Effect.flip(
        prepareRawQuery<object, object>("mixedNumeric", rawQueryCompilerMetadata(MixedNumeric), {
          select: ["id"],
          where: {
            amount: {
              gt: 1,
            },
          },
        }),
      );
      expect(mixedNumericRangeError.message).toBe(
        "Raw query where field amount does not support range operators.",
      );
    }),
  );

  it.effect("keeps structured object predicates callback-only in the storage scan plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery<object, object>(
        "instruments",
        rawQueryCompilerMetadata(Instrument),
        {
          select: ["id"],
          where: {
            operatorLike: {
              eq: "xnys",
            },
            operatorRangeLike: {
              gte: 2,
            },
            tags: ["equity"],
          },
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
            });
            expect(plan.matches(instrument("1", "xnys", 1, ["equity"]))).toBe(false);
            expect(plan.matches(instrument("2", "xlon", 2, ["equity"]))).toBe(false);
            const directMatch = instrument("3", "xnys", 2, ["equity"]);
            expect(plan.matches(directMatch)).toBe(true);
            return {
              keys: ["3"],
              window: [
                {
                  key: "3",
                  row: directMatch,
                },
              ],
              totalRows: 1,
            };
          },
          version: () => 13,
        },
        compiled,
      );

      expect(evaluation.keys).toStrictEqual(["3"]);
      expect(evaluation.rows).toStrictEqual([
        {
          id: "3",
        },
      ]);
      expect(evaluation.totalRows).toBe(1);
      expect(evaluation.version).toBe(13);
    }),
  );
});
