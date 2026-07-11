import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeEngine, order } from "../test-harness/public-engine";

describe("ColumnLiveViewEngine subscription query validation", () => {
  it.effect("does not broaden results for invalid runtime range operands", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const invalidGtQuery: object = {
        select: ["id"],
        where: {
          price: { gt: "9" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidGt = yield* engine.snapshot("orders", invalidGtQuery);
      expect(invalidGt.rows).toStrictEqual([]);

      const invalidGtNaN = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          price: {
            gt: Number.NaN,
          },
        },
      });
      expect(invalidGtNaN.rows).toStrictEqual([]);

      const invalidGteQuery: object = {
        select: ["id"],
        where: {
          price: { gte: "9" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidGte = yield* engine.snapshot("orders", invalidGteQuery);
      expect(invalidGte.rows).toStrictEqual([]);

      const invalidLtQuery: object = {
        select: ["id"],
        where: {
          price: { lt: "11" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidLt = yield* engine.snapshot("orders", invalidLtQuery);
      expect(invalidLt.rows).toStrictEqual([]);

      const invalidLteQuery: object = {
        select: ["id"],
        where: {
          price: { lte: "11" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidLte = yield* engine.snapshot("orders", invalidLteQuery);
      expect(invalidLte.rows).toStrictEqual([]);

      const invalidInQuery: object = {
        select: ["id"],
        where: {
          status: {
            in: 1,
          },
        },
      };
      // @ts-expect-error malformed runtime queries must not throw or broaden results.
      const invalidIn = yield* engine.snapshot("orders", invalidInQuery);
      expect(invalidIn.rows).toStrictEqual([]);

      const cyclicFilter: Array<unknown> = [];
      cyclicFilter.push(cyclicFilter);
      const cyclicQueryValue: object = {
        select: ["id"],
        where: {
          status: cyclicFilter,
        },
      };
      const cyclicQueryValueError = yield* Effect.flip(
        // @ts-expect-error hostile runtime query cycles must be rejected at the boundary.
        engine.snapshot("orders", cyclicQueryValue),
      );
      expect(cyclicQueryValueError).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status contains unsupported query value.",
      });

      type CyclicRecord = {
        self?: CyclicRecord;
      };
      const cyclicRecord: CyclicRecord = {};
      cyclicRecord.self = cyclicRecord;
      const cyclicRecordQueryValue: object = {
        select: ["id"],
        where: {
          status: cyclicRecord,
        },
      };
      const cyclicRecordQueryValueError = yield* Effect.flip(
        // @ts-expect-error hostile runtime query object cycles must be rejected at the boundary.
        engine.snapshot("orders", cyclicRecordQueryValue),
      );
      expect(cyclicRecordQueryValueError).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status contains unsupported query value.",
      });

      const invalidStartsWithQuery: object = {
        select: ["id"],
        where: {
          customerId: {
            startsWith: Symbol("customer"),
          },
        },
      };
      const invalidStartsWith = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not throw or broaden results.
        engine.snapshot("orders", invalidStartsWithQuery),
      );
      expect(invalidStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const invalidFunctionFilterQuery: object = {
        select: ["id"],
        where: {
          customerId: {
            eq: () => "customer",
          },
        },
      };
      const invalidFunctionFilter = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not throw or broaden results.
        engine.snapshot("orders", invalidFunctionFilterQuery),
      );
      expect(invalidFunctionFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      let getterReads = 0;
      const throwingWhere = {};
      Object.defineProperty(throwingWhere, "status", {
        enumerable: true,
        get() {
          getterReads += 1;
          if (getterReads === 1) {
            return { eq: "open" };
          }
          throw {
            _tag: "HostileGetterFailure",
            message: "clone failed",
          };
        },
      });
      const throwingWhereQuery: object = {
        select: ["id"],
        where: throwingWhere,
      };
      const throwingWhereResult = yield* Effect.flip(
        // @ts-expect-error hostile runtime query getters must be rejected at the boundary.
        engine.snapshot("orders", throwingWhereQuery),
      );
      expect(throwingWhereResult).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("where could not be cloned"),
      });

      const stringRangeQuery: object = {
        select: ["id"],
        where: {
          status: { gte: "open" },
        },
      };
      const stringRange = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported string range operators.
        engine.snapshot("orders", stringRangeQuery),
      );
      expect(stringRange).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field status does not support range operators.",
      });

      const numericStartsWithQuery: object = {
        select: ["id"],
        where: {
          price: { startsWith: "1" },
        },
      };
      const numericStartsWith = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported numeric startsWith operators.
        engine.snapshot("orders", numericStartsWithQuery),
      );
      expect(numericStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field price does not support startsWith.",
      });

      const arrayStartsWithQuery: object = {
        select: ["id"],
        where: {
          tags: { startsWith: "equity" },
        },
      };
      const arrayStartsWith = yield* Effect.flip(
        // @ts-expect-error runtime query validation rejects unsupported array startsWith operators.
        engine.snapshot("instruments", arrayStartsWithQuery),
      );
      expect(arrayStartsWith).toMatchObject({
        _tag: "InvalidQueryError",
        message: "Raw query where field tags does not support startsWith.",
      });

      const invalidNeqQuery: object = {
        select: ["id"],
        where: {
          price: { neq: "10" },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const invalidNeq = yield* engine.snapshot("orders", invalidNeqQuery);
      expect(invalidNeq.rows).toStrictEqual([]);

      const invalidNeqNaN = yield* engine.snapshot("orders", {
        select: ["id"],
        where: {
          price: {
            neq: Number.NaN,
          },
        },
      });
      expect(invalidNeqNaN.rows).toStrictEqual([]);

      const undefinedEqualsQuery: object = {
        select: ["id"],
        where: {
          status: {
            eq: undefined,
          },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const undefinedEquals = yield* engine.snapshot("orders", undefinedEqualsQuery);
      expect(undefinedEquals.rows).toStrictEqual([]);

      const undefinedDirectRuntimeQuery: object = {
        select: ["id"],
        where: Object.fromEntries([["status", undefined]]),
      };
      // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
      const undefinedDirectFilter = yield* engine.snapshot("orders", undefinedDirectRuntimeQuery);
      expect(undefinedDirectFilter.rows).toStrictEqual([]);

      const undefinedInFilterQuery: object = {
        select: ["id"],
        where: {
          status: { in: [undefined] },
        },
      };
      // @ts-expect-error malformed runtime queries must not broaden results.
      const undefinedInFilter = yield* engine.snapshot("orders", undefinedInFilterQuery);
      expect(undefinedInFilter.rows).toStrictEqual([]);

      const sparseValues = Array<string>();
      sparseValues[1] = "open";
      const sparseRuntimeQuery: object = {
        select: ["id"],
        where: {
          status: { in: sparseValues },
        },
      };
      const sparseInFilter = yield* Effect.flip(
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        engine.snapshot("orders", sparseRuntimeQuery),
      );
      expect(sparseInFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported query value"),
      });

      const emptyFilter = yield* Effect.flip(
        engine.snapshot("orders", {
          select: ["id"],
          where: {
            status: {},
          },
        }),
      );
      expect(emptyFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const unknownOperatorQuery: object = {
        select: ["id"],
        where: {
          status: {
            equals: "open",
          },
        },
      };
      const unknownOperator = yield* Effect.flip(
        // @ts-expect-error malformed runtime queries must not broaden results.
        engine.snapshot("orders", unknownOperatorQuery),
      );
      expect(unknownOperator).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unsupported filter operator"),
      });

      const typoFieldEmptyFilterQuery: object = {
        select: ["id"],
        where: {
          statuz: {},
        },
      };
      const typoFieldEmptyFilter = yield* Effect.flip(
        // @ts-expect-error malformed runtime query where field must be rejected.
        engine.snapshot("orders", typoFieldEmptyFilterQuery),
      );
      expect(typoFieldEmptyFilter).toMatchObject({
        _tag: "InvalidQueryError",
        message: expect.stringContaining("unknown field: statuz"),
      });
    }),
  );
});
