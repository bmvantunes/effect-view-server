import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { format, fromStringUnsafe, isBigDecimal } from "effect/BigDecimal";
import { evaluateRawQuery } from "./active-query";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
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
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
        where: [{ field: "status", type: "equals", filter: "open" }],
        orderBy: [
          {
            field: "price",
            direction: "desc",
          },
        ],
      });
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "status",
                  operator: "textEq",
                  value: "open",
                  caseSensitive: false,
                  accentSensitive: false,
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
      const booleanCompiled = yield* prepareRuntimeRawQuery(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          orderBy: [{ field: "active", direction: "asc" }],
        },
      );
      const orderCompiled = yield* prepareRuntimeRawQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
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
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id"],
        where: [
          { field: "status", type: "notEqual", filter: "cancelled" },
          { field: "status", type: "in", filter: ["open", "closed"] },
          { field: "price", type: "notEqual", filter: 50 },
          { field: "price", type: "greaterThan", filter: 1 },
          { field: "price", type: "greaterThanOrEqual", filter: 2 },
          { field: "price", type: "lessThan", filter: 100 },
          { field: "price", type: "lessThanOrEqual", filter: 99 },
          { field: "customerId", type: "startsWith", filter: "customer-" },
          { field: "region", type: "equals", filter: "emea" },
        ],
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
      });
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect({
              ...plan.predicate,
              filters: plan.predicate.filters.map((filter) =>
                filter.operator === "in"
                  ? {
                      ...filter,
                      valueKeys: [...(filter.valueKeys ?? [])],
                    }
                  : filter.operator === "textIn"
                    ? {
                        ...filter,
                        valueSet: [...filter.valueSet],
                      }
                    : filter,
              ),
            }).toStrictEqual({
              filters: [
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
                  field: "price",
                  operator: "neq",
                  value: 50,
                },
                {
                  field: "region",
                  operator: "textEq",
                  value: "emea",
                  caseSensitive: false,
                  accentSensitive: false,
                },
                {
                  field: "status",
                  operator: "textIn",
                  values: ["closed", "open"],
                  valueSet: ["closed", "open"],
                  caseSensitive: false,
                  accentSensitive: false,
                },
              ],
              callbackRequired: true,
              callbackSkippable: false,
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
      const positionCompiled = yield* prepareRuntimeRawQuery(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: [
            { field: "active", type: "in", filter: [true] },
            { field: "quantity", type: "in", filter: [20n] },
            { field: "price", type: "in", filter: [matchedPrice] },
          ],
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
                      valueKeys: [...(filter.valueKeys ?? [])],
                    }
                  : filter,
              ),
            }).toStrictEqual({
              filters: [
                {
                  field: "active",
                  operator: "in",
                  values: [true],
                  valueKeys: ["boolean:true"],
                },
                {
                  field: "price",
                  operator: "in",
                  values: ["1"],
                  valueKeys: ['bigDecimal:["1","0"]'],
                },
                {
                  field: "quantity",
                  operator: "in",
                  values: [20n],
                  valueKeys: ["bigint:20"],
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
      const nullableCompiled = yield* prepareRuntimeRawQuery(
        "nullableMetrics",
        rawQueryCompilerMetadata(NullableMetric),
        {
          select: ["id"],
          where: [{ field: "note", type: "in", filter: [null, "x"] }],
        },
      );

      const nullableEvaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect({
              ...plan.predicate,
              filters: plan.predicate.filters.map((filter) =>
                filter.operator === "in"
                  ? {
                      ...filter,
                      valueKeys: [...(filter.valueKeys ?? [])],
                    }
                  : filter,
              ),
            }).toStrictEqual({
              filters: [],
              callbackRequired: true,
              callbackSkippable: false,
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
      const compiled = yield* prepareRuntimeRawQuery(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          select: ["id"],
          where: [
            { field: "quantity", type: "greaterThanOrEqual", filter: 10n },
            { field: "price", type: "notEqual", filter: excludedPrice },
            { field: "price", type: "lessThan", filter: maxPrice },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect({
              ...plan.predicate,
              filters: plan.predicate.filters.map((filter) =>
                "value" in filter && isBigDecimal(filter.value)
                  ? {
                      ...filter,
                      value: format(filter.value),
                    }
                  : filter,
              ),
            }).toStrictEqual({
              filters: [
                {
                  field: "price",
                  operator: "lt",
                  value: "100",
                },
                {
                  field: "price",
                  operator: "neq",
                  value: "0",
                },
                {
                  field: "quantity",
                  operator: "gte",
                  value: 10n,
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
      const compiled = yield* prepareRuntimeRawQuery(
        "literalMetrics",
        rawQueryCompilerMetadata(LiteralMetrics),
        {
          select: ["id"],
          where: [
            { field: "score", type: "greaterThanOrEqual", filter: 1 },
            { field: "bucket", type: "lessThanOrEqual", filter: 1n },
          ],
        },
      );
      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            expect(plan.predicate).toStrictEqual({
              filters: [
                {
                  field: "bucket",
                  operator: "lte",
                  value: 1n,
                },
                {
                  field: "score",
                  operator: "gte",
                  value: 1,
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

  it.effect("rejects malformed scalar operands before storage planning", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: undefined }],
        }),
      );

      expect(error).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Filter operands must not be undefined.",
      });
    }),
  );

  it.effect("filters statically named scalar paths inside structured fields", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeRawQuery(
        "instruments",
        rawQueryCompilerMetadata(Instrument),
        {
          select: ["id"],
          where: [
            { field: "operatorLike.eq", type: "equals", filter: "xnys" },
            { field: "operatorRangeLike.gte", type: "greaterThanOrEqual", filter: 2 },
          ],
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
              window: [{ key: "3", row: directMatch }],
              totalRows: 1,
            };
          },
          version: () => 13,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: ["3"],
        rows: [{ id: "3" }],
        window: [{ key: "3", row: { id: "3" } }],
        totalRows: 1,
        version: 13,
      });
    }),
  );
});
