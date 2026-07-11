import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import { evaluateCompiledGroupedQuery, prepareGroupedQuery } from "./grouped-query-compiler";
import { makeEngine, Order, position, Position } from "../test-harness/public-engine";
import { normalizeDecimalAndBigIntFields } from "../test-harness/rows";

describe("Grouped query compilation and evaluation", () => {
  it.effect("evaluates bounded grouped windows without changing ordered aggregate results", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 1_100 }, (_value, index) => [
          `row-${index}`,
          position(`row-${index}`, `symbol-${index}`, BigInt(index), "1"),
        ]),
      );
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
          orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
          offset: 2,
          limit: 3,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          symbol: "symbol-1097",
          totalQuantity: "1097",
        },
        {
          symbol: "symbol-1096",
          totalQuantity: "1096",
        },
        {
          symbol: "symbol-1095",
          totalQuantity: "1095",
        },
      ]);
      expect(evaluation.keys).toStrictEqual([
        '["array",[["array",[["string","symbol"],["string","symbol-1097"]]]]]',
        '["array",[["array",[["string","symbol"],["string","symbol-1096"]]]]]',
        '["array",[["array",[["string","symbol"],["string","symbol-1095"]]]]]',
      ]);
      expect(evaluation.totalRows).toBe(1_100);
    }),
  );

  it.effect("keeps grouped total rows for zero-limit windows", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["1", position("1", "AAPL", 10n, "1")],
        ["2", position("2", "MSFT", 20n, "1")],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            symbol: "AAPL",
          },
          orderBy: [{ field: "symbol", direction: "asc" }],
          offset: 10_000,
          limit: 0,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.keys).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(1);
    }),
  );

  it.effect("compares bounded grouped windows by avg, max, and stable group key", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>([
        ["a-1", position("a-1", "AAPL", 0n, "1")],
        ["a-2", position("a-2", "AAPL", 20n, "1")],
        ["m-1", position("m-1", "MSFT", 5n, "1")],
        ["m-2", position("m-2", "MSFT", 15n, "1")],
        ["z-1", position("z-1", "ZZZZ", 1n, "1")],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            averageQuantity: { aggFunc: "avg", field: "quantity" },
            maxQuantity: { aggFunc: "max", field: "quantity" },
          },
          orderBy: [
            { aggregate: "averageQuantity", direction: "asc" },
            { aggregate: "maxQuantity", direction: "desc" },
          ],
          limit: 3,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          averageQuantity: "1",
          maxQuantity: "1",
          symbol: "ZZZZ",
        },
        {
          averageQuantity: "10",
          maxQuantity: "20",
          symbol: "AAPL",
        },
        {
          averageQuantity: "10",
          maxQuantity: "15",
          symbol: "MSFT",
        },
      ]);

      const tiedCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
          limit: 2,
        },
      );
      const tiedEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        tiedCount,
      );
      expect(tiedEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
        {
          rowCount: 1n,
          symbol: "BBBB",
        },
      ]);

      const tiedSingleCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ aggregate: "rowCount", direction: "desc" }],
          limit: 1,
        },
      );
      const tiedSingleEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        tiedSingleCount,
      );
      expect(tiedSingleEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
      ]);

      const distinctCount = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            distinctPrices: { aggFunc: "countDistinct", field: "price" },
          },
          orderBy: [{ aggregate: "distinctPrices", direction: "desc" }],
          limit: 1,
        },
      );
      const distinctEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("a-1", position("a-1", "AAPL", 1n, "1"));
            visitor("a-2", position("a-2", "AAPL", 1n, "2"));
            visitor("m-1", position("m-1", "MSFT", 1n, "1"));
          },
          version: () => 1,
        },
        distinctCount,
      );
      expect(distinctEvaluation.rows).toStrictEqual([
        {
          distinctPrices: 2n,
          symbol: "AAPL",
        },
      ]);

      const fieldOrder = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "symbol", direction: "desc" }],
          limit: 1,
        },
      );
      const fieldOrderEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("a", position("a", "AAAA", 1n, "1"));
            visitor("b", position("b", "BBBB", 1n, "1"));
          },
          version: () => 1,
        },
        fieldOrder,
      );
      expect(fieldOrderEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "BBBB",
        },
      ]);

      const defaultOrder = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          limit: 1,
        },
      );
      const defaultOrderEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("b", position("b", "BBBB", 1n, "1"));
            visitor("a", position("a", "AAAA", 1n, "1"));
          },
          version: () => 1,
        },
        defaultOrder,
      );
      expect(defaultOrderEvaluation.rows).toStrictEqual([
        {
          rowCount: 1n,
          symbol: "AAAA",
        },
      ]);

      const PositionForCompiler = Schema.Struct({
        id: Schema.String,
        quantity: Schema.BigInt,
        symbol: Schema.String,
      });
      const zeroAverage = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(PositionForCompiler),
        {
          groupBy: ["symbol"],
          aggregates: {
            averageQuantity: { aggFunc: "avg", field: "quantity" },
          },
          orderBy: [{ aggregate: "averageQuantity", direction: "asc" }],
          limit: 1,
        },
      );
      const zeroAverageEvaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            visitor("bad", { id: "bad", quantity: "not-a-bigint", symbol: "BAD" });
            visitor("good", { id: "good", quantity: 10n, symbol: "GOOD" });
          },
          version: () => 1,
        },
        zeroAverage,
      );
      expect(normalizeDecimalAndBigIntFields(zeroAverageEvaluation.rows)).toStrictEqual([
        {
          averageQuantity: "0",
          symbol: "BAD",
        },
      ]);
    }),
  );

  it.effect("uses full grouped ordering when requested grouped window is too large", () =>
    Effect.gen(function* () {
      const rows = new Map<string, object>(
        Array.from({ length: 1_100 }, (_value, index) => [
          `row-${index}`,
          position(`row-${index}`, `symbol-${index}`, BigInt(index), "1"),
        ]),
      );
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(Position),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
          orderBy: [{ aggregate: "totalQuantity", direction: "desc" }],
          limit: 1_025,
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );

      expect(normalizeDecimalAndBigIntFields(evaluation.rows.slice(0, 2))).toStrictEqual([
        {
          symbol: "symbol-1099",
          totalQuantity: "1099",
        },
        {
          symbol: "symbol-1098",
          totalQuantity: "1098",
        },
      ]);
      expect(evaluation.rows.length).toBe(1_025);
      expect(evaluation.totalRows).toBe(1_100);
    }),
  );

  it.effect("ignores malformed runtime values for bigint grouped sums", () =>
    Effect.gen(function* () {
      const PositionForCompiler = Schema.Struct({
        id: Schema.String,
        symbol: Schema.String,
        quantity: Schema.BigInt,
      });
      const rows = new Map<string, object>([
        ["bad", { id: "bad", symbol: "AAPL", quantity: "not-a-bigint" }],
        ["good", { id: "good", symbol: "AAPL", quantity: 3n }],
      ]);
      const compiled = yield* prepareGroupedQuery<object, object>(
        "positions",
        rawQueryCompilerMetadata(PositionForCompiler),
        {
          groupBy: ["symbol"],
          aggregates: {
            totalQuantity: { aggFunc: "sum", field: "quantity" },
          },
        },
      );
      const evaluation = evaluateCompiledGroupedQuery(
        {
          changesSince: () => [],
          scanRows: (visitor) => {
            for (const [key, row] of rows) {
              visitor(key, row);
            }
          },
          version: () => 1,
        },
        compiled,
      );
      expect(normalizeDecimalAndBigIntFields(evaluation.rows)).toStrictEqual([
        {
          symbol: "AAPL",
          totalQuantity: "3",
        },
      ]);
    }),
  );

  it.effect("rejects malformed grouped queries through the typed error channel", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const sparseGroupBy = Array<string>();
      sparseGroupBy[1] = "status";
      const nonPlainGroupedQuery: object = Object.assign(new Map(), {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const invalidCases: ReadonlyArray<{
        readonly query: unknown;
        readonly message: string;
      }> = [
        { query: null, message: "plain object" },
        { query: nonPlainGroupedQuery, message: "plain object" },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            typo: true,
          },
          message: "unsupported key: typo",
        },
        {
          query: {
            groupBy: ["status"],
            select: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
          },
          message: "must not include select",
        },
        {
          query: { groupBy: [], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: sparseGroupBy, aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: [1], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "groupBy",
        },
        {
          query: { groupBy: ["missing"], aggregates: { rowCount: { aggFunc: "count" } } },
          message: "unknown field: missing",
        },
        { query: { groupBy: ["status"], aggregates: [] }, message: "aggregates" },
        {
          query: { groupBy: ["status"], aggregates: { status: { aggFunc: "count" } } },
          message: "collides",
        },
        {
          query: { groupBy: ["status"], aggregates: { constructor: { aggFunc: "count" } } },
          message: "aggregate alias is not allowed",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: "count" } },
          message: "plain object",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: { aggFunc: "median" } } },
          message: "unsupported aggFunc",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count", field: "price" } },
          },
          message: "must not include a field",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { total: { aggFunc: "sum", field: "price", typo: true } },
          },
          message: "unsupported key: typo",
        },
        {
          query: { groupBy: ["status"], aggregates: { total: { aggFunc: "sum" } } },
          message: "field must be a string",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { total: { aggFunc: "sum", field: "missing" } },
          },
          message: "unknown field: missing",
        },
        {
          query: { groupBy: ["status"], aggregates: { rowCount: { aggFunc: "count" } }, where: [] },
          message: "where",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: "bad",
          },
          message: "orderBy",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: ["bad"],
          },
          message: "plain objects",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", direction: "asc", typo: true }],
          },
          message: "unsupported key: typo",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", direction: "sideways" }],
          },
          message: "direction",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ direction: "asc" }],
          },
          message: "choose field or aggregate",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "status", aggregate: "rowCount", direction: "asc" }],
          },
          message: "choose field or aggregate",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "price", direction: "asc" }],
          },
          message: "field must be present in groupBy",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ aggregate: "missing", direction: "asc" }],
          },
          message: "aggregate must reference an aggregate alias",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            offset: -1,
          },
          message: "offset",
        },
        {
          query: {
            groupBy: ["status"],
            aggregates: { rowCount: { aggFunc: "count" } },
            limit: "1",
          },
          message: "limit",
        },
      ];

      for (const invalidCase of invalidCases) {
        const error = yield* Effect.flip(
          // @ts-expect-error hostile untyped runtime grouped query is still handled by runtime guards.
          engine.snapshot("orders", invalidCase.query),
        );
        expect(error._tag).toBe("InvalidQueryError");
        expect(error.message).toContain(invalidCase.message);
      }

      const orderMetadata = rawQueryCompilerMetadata(Order);
      const inconsistentMetadata = {
        ...orderMetadata,
        fieldMetadata: new Map(),
      };
      const missingSumResultKind = yield* Effect.flip(
        prepareGroupedQuery<typeof Order.Type, object>("orders", inconsistentMetadata, {
          groupBy: ["status"],
          aggregates: { totalPrice: { aggFunc: "sum", field: "price" } },
        }),
      );
      expect(missingSumResultKind).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("must reference a numeric field"),
      });
    }),
  );
});
