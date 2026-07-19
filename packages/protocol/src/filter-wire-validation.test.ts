import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaGetter } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { decodeWhere, encodeWhere } from "./protocol-query-common";
import { encodeQueryGraph } from "./protocol-query-schema";

const FilterRow = Schema.Struct({
  id: Schema.String,
  count: Schema.Number,
  sequence: Schema.BigInt,
  amount: Schema.BigDecimal,
  active: Schema.Boolean,
  mixed: Schema.Union([Schema.Number, Schema.BigInt]),
  profile: Schema.Struct({ country: Schema.String }),
});

const filterConfig = {
  topics: { values: { schema: FilterRow, key: "id" } },
} as const;

const decodeHostileWhere = (where: unknown) =>
  // @ts-expect-error hostile wire callers can provide values outside the declared boundary.
  decodeWhere("values", FilterRow, where);

const invalidMessage = (field: string, suffix: string): string =>
  `Filter condition ${field} ${suffix}`;

const isUnknownRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const unaryGroupDepth = (value: unknown): number => {
  let current = value;
  let depth = 0;
  while (isUnknownRecord(current)) {
    const type = current["type"];
    const conditions = current["conditions"];
    if ((type !== "AND" && type !== "OR") || !Array.isArray(conditions)) {
      return depth;
    }
    if (conditions.length !== 1) {
      return depth;
    }
    depth += 1;
    current = conditions[0];
  }
  return depth;
};

describe("filter wire validation", () => {
  it.effect("round-trips recursive expressions and every filter operand shape", () =>
    Effect.gen(function* () {
      const shared = {
        field: "id",
        type: "equals",
        filter: "Résumé",
        caseSensitive: false,
        accentSensitive: false,
      };
      const amount = BigDecimal.make(123n, 2);
      const where = [
        {
          type: "AND",
          conditions: [
            shared,
            shared,
            { type: "OR", conditions: [] },
            {
              type: "NOT",
              condition: { field: "profile.country", type: "endsWith", filter: "gal" },
            },
          ],
        },
        { field: "active", type: "blank" },
        { field: "active", type: "notBlank" },
        { field: "id", type: "notContains", filter: "blocked" },
        { field: "id", type: "startsWith", filter: "R" },
        { field: "count", type: "greaterThanOrEqual", filter: 1 },
        { field: "count", type: "lessThan", filter: 5 },
        { field: "count", type: "lessThanOrEqual", filter: 4 },
        { field: "mixed", type: "greaterThan", filter: 1 },
        { field: "mixed", type: "lessThan", filter: 3n },
        { field: "sequence", type: "in", filter: [1n, 2n] },
        {
          field: "amount",
          type: "inRange",
          filter: amount,
          filterTo: BigDecimal.make(124n, 2),
        },
      ];

      const encoded = yield* encodeWhere(filterConfig, "values", where);
      const decoded = yield* decodeWhere("values", FilterRow, encoded);
      const expected = [
        {
          type: "AND",
          conditions: [
            shared,
            { type: "OR", conditions: [] },
            {
              type: "NOT",
              condition: { field: "profile.country", type: "endsWith", filter: "gal" },
            },
          ],
        },
        ...where.slice(1, -1),
        {
          field: "amount",
          type: "inRange",
          filter: BigDecimal.make(123n, 2),
          filterTo: BigDecimal.make(124n, 2),
        },
      ];

      expect(decoded).toStrictEqual(expected);
      expect(Object.isFrozen(decoded)).toBe(true);
      expect(Object.isFrozen(decoded?.[0])).toBe(true);
    }),
  );

  it.effect("round-trips every BigDecimal filter operand position", () =>
    Effect.gen(function* () {
      const makeWhere = () => [
        { field: "amount", type: "equals", filter: BigDecimal.make(121n, 2) },
        { field: "amount", type: "notEqual", filter: BigDecimal.make(131n, 2) },
        {
          field: "amount",
          type: "in",
          filter: [BigDecimal.make(141n, 2), BigDecimal.make(151n, 2)],
        },
        { field: "amount", type: "greaterThan", filter: BigDecimal.make(161n, 2) },
        {
          field: "amount",
          type: "greaterThanOrEqual",
          filter: BigDecimal.make(171n, 2),
        },
        { field: "amount", type: "lessThan", filter: BigDecimal.make(181n, 2) },
        {
          field: "amount",
          type: "lessThanOrEqual",
          filter: BigDecimal.make(191n, 2),
        },
        {
          field: "amount",
          type: "inRange",
          filter: BigDecimal.make(201n, 2),
          filterTo: BigDecimal.make(211n, 2),
        },
      ];
      const where = makeWhere();

      const encoded = yield* encodeWhere(filterConfig, "values", where);
      const decoded = yield* decodeWhere("values", FilterRow, encoded);

      expect(decoded).toStrictEqual(makeWhere());
    }),
  );

  it.effect("rejects every unsafe BigDecimal operand before schema encoding", () =>
    Effect.gen(function* () {
      const invalidBigDecimals = [
        BigDecimal.make(1n, Number.NaN),
        BigDecimal.make(1n, Number.POSITIVE_INFINITY),
        BigDecimal.make(1n, 1.5),
      ];
      const valid = BigDecimal.make(1n, 0);

      for (const invalid of invalidBigDecimals) {
        const conditions: ReadonlyArray<ReadonlyArray<unknown>> = [
          [{ field: "amount", type: "equals", filter: invalid }],
          [{ field: "amount", type: "notEqual", filter: invalid }],
          [{ field: "amount", type: "in", filter: [invalid, valid, valid] }],
          [{ field: "amount", type: "in", filter: [valid, invalid, valid] }],
          [{ field: "amount", type: "in", filter: [valid, valid, invalid] }],
          [{ field: "amount", type: "greaterThan", filter: invalid }],
          [{ field: "amount", type: "greaterThanOrEqual", filter: invalid }],
          [{ field: "amount", type: "lessThan", filter: invalid }],
          [{ field: "amount", type: "lessThanOrEqual", filter: invalid }],
          [
            {
              field: "amount",
              type: "inRange",
              filter: invalid,
              filterTo: valid,
            },
          ],
          [
            {
              field: "amount",
              type: "inRange",
              filter: valid,
              filterTo: invalid,
            },
          ],
        ];

        for (const where of conditions) {
          const error = yield* Effect.flip(encodeWhere(filterConfig, "values", where));
          expect(error).toStrictEqual({
            _tag: "ViewServerRuntimeError",
            code: "InvalidQuery",
            message: "Filter condition amount BigDecimal operand is not wire-safe",
            topic: "values",
          });
        }
      }
    }),
  );

  it.effect("does not invoke BigDecimal formatting before rejecting an unsafe operand", () =>
    Effect.gen(function* () {
      const invalid = BigDecimal.make(1n, Number.NaN);
      Object.defineProperty(invalid, "normalized", {
        configurable: true,
        get: () => {
          throw new Error("unsafe BigDecimal must not be formatted");
        },
      });

      const error = yield* Effect.flip(
        encodeWhere(filterConfig, "values", [{ field: "amount", type: "equals", filter: invalid }]),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Filter condition amount BigDecimal operand is not wire-safe",
        topic: "values",
      });
    }),
  );

  it.effect("rejects a wire BigDecimal that decodes to an unsafe scale", () =>
    Effect.gen(function* () {
      const UnsafeDecodedBigDecimal = Schema.String.pipe(
        Schema.decodeTo(Schema.BigDecimal, {
          decode: SchemaGetter.transform(() => BigDecimal.make(1n, Number.NaN)),
          encode: SchemaGetter.transform((value) => BigDecimal.format(value)),
        }),
      );
      const UnsafeRow = Schema.Struct({ id: Schema.String, amount: UnsafeDecodedBigDecimal });
      const error = yield* Effect.flip(
        decodeWhere("values", UnsafeRow, [{ field: "amount", type: "equals", filter: "1" }]),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Filter condition amount BigDecimal operand is not wire-safe",
        topic: "values",
      });
    }),
  );

  it.effect(
    "encodes deeply nested filters and shared filter DAGs without recursive expansion",
    () =>
      Effect.gen(function* () {
        let deep: unknown = { field: "id", type: "equals", filter: "deep" };
        for (let depth = 0; depth < 5_000; depth += 1) {
          deep = { type: depth % 2 === 0 ? "AND" : "OR", conditions: [deep] };
        }
        const deeplyEncoded = yield* encodeWhere(filterConfig, "values", [deep]);
        const deeplyDecoded = yield* decodeWhere("values", FilterRow, deeplyEncoded);

        let shared: unknown = { field: "id", type: "equals", filter: "shared" };
        for (let depth = 0; depth < 100; depth += 1) {
          shared = { type: "AND", conditions: [shared, shared] };
        }
        const sharedEncoded = yield* encodeWhere(filterConfig, "values", [shared]);

        const repeatedRoots = Array.from({ length: 200 }, () => shared);
        const repeatedRootsEncoded = yield* encodeWhere(filterConfig, "values", repeatedRoots);

        expect(unaryGroupDepth(deeplyDecoded?.[0])).toBe(5_000);
        expect(unaryGroupDepth(sharedEncoded?.[0])).toBe(100);
        expect(JSON.stringify(sharedEncoded).length).toBeLessThan(10_000);
        expect(repeatedRootsEncoded?.every((root) => root === repeatedRootsEncoded[0])).toBe(true);
        expect(encodeQueryGraph(repeatedRootsEncoded).length).toBeLessThan(20_000);
      }),
  );

  it.effect("rejects malformed recursive expressions and invalid operator domains", () =>
    Effect.gen(function* () {
      class ArraySubclass extends Array<unknown> {}
      const symbolicArray: Array<unknown> = [];
      Object.defineProperty(symbolicArray, Symbol("metadata"), { enumerable: true, value: true });
      const sparseArray: Array<unknown> = [];
      sparseArray.length = 1;
      const accessorArray: Array<unknown> = [];
      Object.defineProperty(accessorArray, "0", {
        enumerable: true,
        get: () => ({ field: "id", type: "blank" }),
      });
      accessorArray.length = 1;
      const extraPropertyArray: Array<unknown> = [];
      Object.defineProperty(extraPropertyArray, "extra", { enumerable: true, value: true });
      const cyclic = { type: "AND", conditions: [] as Array<unknown> };
      cyclic.conditions.push(cyclic);
      const symbolicCondition = { field: "id", type: "blank" };
      Object.defineProperty(symbolicCondition, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const hiddenTextOption = { field: "id", type: "equals", filter: "x" };
      Object.defineProperty(hiddenTextOption, "caseSensitive", {
        enumerable: false,
        value: true,
      });

      const cases: ReadonlyArray<readonly [unknown, string]> = [
        [{}, "Query where must be an array"],
        [new ArraySubclass(), "Query where must be an array"],
        [symbolicArray, "Query where must be an array"],
        [sparseArray, "Query where must be an array"],
        [accessorArray, "Query where must be an array"],
        [extraPropertyArray, "Query where must be an array"],
        [[null], "Every filter expression must be an object"],
        [[cyclic], "Filter expressions must not contain cycles"],
        [[{ type: "AND", conditions: [], extra: true }], "Filter group AND has invalid keys"],
        [[{ type: "OR", conditions: null }], "Filter group OR conditions must be an array"],
        [[{ type: "NOT", condition: {}, extra: true }], "Filter NOT has invalid keys"],
        [[{ type: "NOT" }], "Every filter expression must be an object"],
        [[{}], "Filter conditions require field and type"],
        [
          [{ field: "missing", type: "equals", filter: "x" }],
          "Query references an unknown or non-filterable field: missing",
        ],
        [
          [{ field: "id", type: "unknown", filter: "x" }],
          "Unsupported filter condition type: unknown",
        ],
        [[{ field: "id", type: "blank", filter: "x" }], "Filter condition id has invalid keys"],
        [[symbolicCondition], "Filter condition id has invalid keys"],
        [[hiddenTextOption], "Filter condition id has invalid keys"],
        [
          [{ field: "id", type: "equals", filter: "x", caseSensitive: "yes" }],
          invalidMessage("id", "caseSensitive must be a boolean"),
        ],
        [
          [{ field: "id", type: "equals", filter: "x", accentSensitive: 1 }],
          invalidMessage("id", "accentSensitive must be a boolean"),
        ],
        [[{ field: "id", type: "in", filter: "x" }], "Filter condition id in must be an array"],
        [
          [{ field: "count", type: "contains", filter: "1" }],
          "Filter count does not support contains",
        ],
        [
          [{ field: "id", type: "endsWith", filter: 1 }],
          "Filter condition id endsWith requires a string",
        ],
        [
          [{ field: "id", type: "greaterThan", filter: "a" }],
          "Filter id does not support range operators",
        ],
      ];

      for (const [where, message] of cases) {
        const error = yield* Effect.flip(decodeHostileWhere(where));
        expect(error.code).toBe("InvalidQuery");
        expect(error.message).toBe(message);
      }
    }),
  );
});
