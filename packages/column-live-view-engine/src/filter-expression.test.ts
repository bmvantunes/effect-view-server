import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { compileRawPredicate } from "./raw-predicate-compiler";
import {
  compareRuntimeFilterExpressionStructure,
  type RuntimeFilterExpression,
} from "./filter-expression";
import { decodeRawQuery } from "./raw-query-decoder";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";

const Row = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  age: Schema.Number,
  active: Schema.optionalKey(Schema.Boolean),
  amount: Schema.optionalKey(Schema.BigDecimal),
  mixed: Schema.optionalKey(Schema.Union([Schema.Number, Schema.BigInt])),
  scalar: Schema.optionalKey(
    Schema.Union([Schema.String, Schema.Number, Schema.BigInt, Schema.Boolean, Schema.Null]),
  ),
  profile: Schema.optionalKey(
    Schema.Struct({
      country: Schema.String,
    }),
  ),
});

const metadata = rawQueryCompilerMetadata(Row);

const decodeWhere = (where: unknown) =>
  decodeRawQuery("people", metadata, { select: ["id"], where });

describe("recursive filter expressions", () => {
  it.effect("normalizes text and evaluates recursive Boolean expressions", () =>
    Effect.gen(function* () {
      const query = yield* decodeWhere([
        {
          type: "OR",
          conditions: [
            { field: "name", type: "equals", filter: "resume" },
            {
              type: "NOT",
              condition: { field: "age", type: "greaterThan", filter: 23 },
            },
          ],
        },
      ]);
      const predicate = compileRawPredicate<typeof Row.Type>(metadata, query.where);

      expect(predicate.matches({ id: "a", name: "Résumé", age: 30 })).toBe(true);
      expect(predicate.matches({ id: "b", name: "other", age: 23 })).toBe(true);
      expect(predicate.matches({ id: "c", name: "other", age: 24 })).toBe(false);
    }),
  );

  it.effect("ignores empty generated filters and gives equivalent queries one key", () =>
    Effect.gen(function* () {
      const shared = { field: "age", type: "in", filter: [] };
      const first = yield* decodeWhere([
        { type: "OR", conditions: [] },
        shared,
        { field: "name", type: "contains", filter: "JOHN" },
      ]);
      const second = yield* decodeWhere([
        { field: "name", type: "contains", filter: "john" },
        shared,
      ]);

      expect(first.where?.key).toBe(second.where?.key);
      expect(
        compileRawPredicate(metadata, first.where).matches({ id: "a", name: "John", age: 1 }),
      ).toBe(true);
    }),
  );

  it.effect("uses half-open ranges and nested paths with blank missing branches", () =>
    Effect.gen(function* () {
      const range = yield* decodeWhere([{ field: "age", type: "inRange", filter: 3, filterTo: 5 }]);
      const nested = yield* decodeWhere([{ field: "profile.country", type: "blank" }]);

      const rangePredicate = compileRawPredicate<typeof Row.Type>(metadata, range.where);
      const nestedPredicate = compileRawPredicate<object>(metadata, nested.where);
      expect(rangePredicate.matches({ id: "a", name: "x", age: 3 })).toBe(true);
      expect(rangePredicate.matches({ id: "b", name: "x", age: 4.999 })).toBe(true);
      expect(rangePredicate.matches({ id: "c", name: "x", age: 5 })).toBe(false);
      expect(nestedPredicate.matches({ id: "d", name: "x", age: 1 })).toBe(true);
      expect(nestedPredicate.matches({ id: "primitive", name: "x", age: 1, profile: 1 })).toBe(
        true,
      );
      expect(
        nestedPredicate.matches({ id: "e", name: "x", age: 1, profile: { country: "PT" } }),
      ).toBe(false);
    }),
  );

  it.effect("reads nested paths only from own enumerable data properties", () =>
    Effect.gen(function* () {
      const equalsQuery = yield* decodeWhere([
        { field: "profile.country", type: "equals", filter: "polluted" },
      ]);
      const blankQuery = yield* decodeWhere([{ field: "profile.country", type: "blank" }]);
      const notBlankQuery = yield* decodeWhere([{ field: "profile.country", type: "notBlank" }]);
      const equals = compileRawPredicate<object>(metadata, equalsQuery.where);
      const blank = compileRawPredicate<object>(metadata, blankQuery.where);
      const notBlank = compileRawPredicate<object>(metadata, notBlankQuery.where);
      const inheritedProfile = Object.create({ country: "polluted" });
      const inheritedRow = { id: "inherited", name: "x", age: 1, profile: inheritedProfile };
      let accessorReads = 0;
      const accessorProfile = {};
      Object.defineProperty(accessorProfile, "country", {
        enumerable: true,
        get: () => {
          accessorReads += 1;
          return "polluted";
        },
      });
      const accessorRow = { id: "accessor", name: "x", age: 1, profile: accessorProfile };
      const ownRow = {
        id: "own",
        name: "x",
        age: 1,
        profile: { country: "polluted" },
      };

      expect(equals.matches(inheritedRow)).toBe(false);
      expect(blank.matches(inheritedRow)).toBe(true);
      expect(notBlank.matches(inheritedRow)).toBe(false);
      expect(equals.matches(accessorRow)).toBe(false);
      expect(blank.matches(accessorRow)).toBe(true);
      expect(notBlank.matches(accessorRow)).toBe(false);
      expect(accessorReads).toBe(0);
      expect(equals.matches(ownRow)).toBe(true);
      expect(blank.matches(ownRow)).toBe(false);
      expect(notBlank.matches(ownRow)).toBe(true);
    }),
  );

  it.effect("supports own data properties with dangerous nested field names", () =>
    Effect.gen(function* () {
      const DangerousRow = Schema.Struct({
        id: Schema.String,
        nested: Schema.Struct({
          constructor: Schema.String,
          prototype: Schema.String,
          ["__proto__"]: Schema.String,
        }),
      });
      const dangerousMetadata = rawQueryCompilerMetadata(DangerousRow);
      const query = yield* decodeRawQuery("dangerous", dangerousMetadata, {
        select: ["id"],
        where: [
          { field: "nested.constructor", type: "equals", filter: "constructor" },
          { field: "nested.prototype", type: "equals", filter: "prototype" },
          { field: "nested.__proto__", type: "equals", filter: "__proto__" },
        ],
      });
      const predicate = compileRawPredicate<object>(dangerousMetadata, query.where);
      const nested: Record<string, unknown> = {};
      for (const field of ["constructor", "prototype", "__proto__"]) {
        Object.defineProperty(nested, field, {
          configurable: true,
          enumerable: true,
          value: field,
          writable: true,
        });
      }

      expect(predicate.matches({ id: "dangerous", nested })).toBe(true);
      expect(predicate.matches({ id: "inherited-only", nested: {} })).toBe(false);
    }),
  );

  it.effect("owns the submitted expression snapshot", () =>
    Effect.gen(function* () {
      const condition = { field: "name", type: "equals", filter: "Alice" };
      const where = [condition];
      const query = yield* decodeWhere(where);
      condition.filter = "Bob";
      where.push({ field: "name", type: "equals", filter: "Carol" });
      const predicate = compileRawPredicate<typeof Row.Type>(metadata, query.where);

      expect(predicate.matches({ id: "a", name: "Alice", age: 1 })).toBe(true);
      expect(predicate.matches({ id: "b", name: "Bob", age: 1 })).toBe(false);
    }),
  );

  it.effect("rejects the former field-keyed where shape and cycles", () =>
    Effect.gen(function* () {
      const oldShape = yield* Effect.flip(
        decodeWhere({ name: { type: "equals", filter: "Alice" } }),
      );
      const cyclic: Record<string, unknown> = { type: "NOT" };
      cyclic["condition"] = cyclic;
      const cycle = yield* Effect.flip(decodeWhere([cyclic]));

      expect(oldShape.message).toBe("Query where must be an array.");
      expect(cycle.message).toBe("Filter expressions must not contain cycles.");
    }),
  );

  it.effect("rejects unsupported canonical condition types and properties", () =>
    Effect.gen(function* () {
      const unsupportedType = yield* Effect.flip(
        decodeWhere([{ field: "name", type: "typo", filter: "Alice" }]),
      );
      const unsupportedProperty = yield* Effect.flip(
        decodeWhere([{ field: "name", type: "equals", filter: "Alice", typo: true }]),
      );

      expect(unsupportedType.message).toBe("Filter condition for name has an unsupported type.");
      expect(unsupportedProperty.message).toBe(
        "Filter expression contains unsupported property: typo.",
      );
    }),
  );

  it.effect("evaluates every canonical operator with explicit text sensitivity", () =>
    Effect.gen(function* () {
      const cases = [
        [{ field: "name", type: "notEqual", filter: "alice" }, "Alice", false],
        [{ field: "name", type: "in", filter: ["bob", "alice", "ALICE"] }, "Alice", true],
        [{ field: "name", type: "notContains", filter: "lic" }, "Alice", false],
        [{ field: "name", type: "startsWith", filter: "ali" }, "Alice", true],
        [{ field: "name", type: "endsWith", filter: "ICE" }, "Alice", true],
        [{ field: "age", type: "greaterThanOrEqual", filter: 3 }, 3, true],
        [{ field: "age", type: "lessThan", filter: 3 }, 3, false],
        [{ field: "age", type: "lessThanOrEqual", filter: 3 }, 3, true],
        [{ field: "active", type: "blank" }, undefined, true],
        [{ field: "active", type: "notBlank" }, false, true],
      ] as const;

      for (const [condition, value, expected] of cases) {
        const query = yield* decodeWhere([condition]);
        const predicate = compileRawPredicate(metadata, query.where);
        const row = {
          id: "row",
          name: typeof value === "string" ? value : "Alice",
          age: typeof value === "number" ? value : 3,
          active: typeof value === "boolean" ? value : undefined,
          amount: BigDecimal.make(1n, 0),
          mixed: 1,
        };
        expect(predicate.matches(row)).toBe(expected);
      }

      const caseSensitive = yield* decodeWhere([
        { field: "name", type: "equals", filter: "Résumé", caseSensitive: true },
      ]);
      const accentSensitive = yield* decodeWhere([
        { field: "name", type: "equals", filter: "resume", accentSensitive: true },
      ]);
      expect(
        compileRawPredicate(metadata, caseSensitive.where).matches({
          id: "case",
          name: "résumé",
          age: 1,
          active: true,
          amount: BigDecimal.make(1n, 0),
          mixed: 1,
        }),
      ).toBe(false);
      expect(
        compileRawPredicate(metadata, accentSensitive.where).matches({
          id: "accent",
          name: "résumé",
          age: 1,
          active: true,
          amount: BigDecimal.make(1n, 0),
          mixed: 1,
        }),
      ).toBe(false);
    }),
  );

  it.effect("treats only empty strings, null, and undefined as blank", () =>
    Effect.gen(function* () {
      const blankQuery = yield* decodeWhere([{ field: "scalar", type: "blank" }]);
      const notBlankQuery = yield* decodeWhere([{ field: "scalar", type: "notBlank" }]);
      const notEqualQuery = yield* decodeWhere([
        { field: "scalar", type: "notEqual", filter: "blocked" },
      ]);
      const notContainsQuery = yield* decodeWhere([
        { field: "scalar", type: "notContains", filter: "blocked" },
      ]);
      const blankPredicate = compileRawPredicate<typeof Row.Type>(metadata, blankQuery.where);
      const notBlankPredicate = compileRawPredicate<typeof Row.Type>(metadata, notBlankQuery.where);
      const notEqualPredicate = compileRawPredicate<typeof Row.Type>(metadata, notEqualQuery.where);
      const notContainsPredicate = compileRawPredicate<typeof Row.Type>(
        metadata,
        notContainsQuery.where,
      );
      const cases = [
        [undefined, true],
        [null, true],
        ["", true],
        [0, false],
        [0n, false],
        [false, false],
        [" ", false],
      ] as const;

      for (const [scalar, isBlank] of cases) {
        const row =
          scalar === undefined
            ? { id: String(scalar), name: "Alice", age: 1 }
            : { id: String(scalar), name: "Alice", age: 1, scalar };
        expect(blankPredicate.matches(row)).toBe(isBlank);
        expect(notBlankPredicate.matches(row)).toBe(!isBlank);
        expect(notEqualPredicate.matches(row)).toBe(true);
        expect(notContainsPredicate.matches(row)).toBe(true);
      }
    }),
  );

  it.effect("uses only effective text sensitivity in semantic identity", () =>
    Effect.gen(function* () {
      const numericEquals = yield* decodeWhere([{ field: "scalar", type: "equals", filter: 1 }]);
      const sensitiveNumericEquals = yield* decodeWhere([
        {
          field: "scalar",
          type: "equals",
          filter: 1,
          caseSensitive: true,
          accentSensitive: true,
        },
      ]);
      const numericIn = yield* decodeWhere([{ field: "scalar", type: "in", filter: [1, 2] }]);
      const sensitiveNumericIn = yield* decodeWhere([
        {
          field: "scalar",
          type: "in",
          filter: [1, 2],
          caseSensitive: true,
          accentSensitive: true,
        },
      ]);
      const stringEquals = yield* decodeWhere([
        { field: "scalar", type: "equals", filter: "Résumé" },
      ]);
      const sensitiveStringEquals = yield* decodeWhere([
        {
          field: "scalar",
          type: "equals",
          filter: "Résumé",
          caseSensitive: true,
          accentSensitive: true,
        },
      ]);

      expect(numericEquals.where?.key).toBe(sensitiveNumericEquals.where?.key);
      expect(numericIn.where?.key).toBe(sensitiveNumericIn.where?.key);
      expect(stringEquals.where?.key).not.toBe(sensitiveStringEquals.where?.key);
    }),
  );

  it.effect("orders bounded-key collisions by exact structural identity", () =>
    Effect.gen(function* () {
      const first = yield* decodeWhere([
        {
          type: "OR",
          conditions: [
            { field: "name", type: "equals", filter: "alice" },
            { field: "age", type: "equals", filter: 1 },
          ],
        },
        {
          type: "OR",
          conditions: [
            { field: "name", type: "equals", filter: "bob" },
            { field: "age", type: "equals", filter: 2 },
          ],
        },
      ]);
      const second = yield* decodeWhere([
        {
          type: "OR",
          conditions: [
            { field: "age", type: "equals", filter: 2 },
            { field: "name", type: "equals", filter: "bob" },
          ],
        },
        {
          type: "OR",
          conditions: [
            { field: "age", type: "equals", filter: 1 },
            { field: "name", type: "equals", filter: "alice" },
          ],
        },
      ]);
      const rootNot = yield* decodeWhere([
        {
          type: "NOT",
          condition: { field: "age", type: "greaterThan", filter: 10 },
        },
      ]);
      const secondRootNot = yield* decodeWhere([
        {
          type: "NOT",
          condition: { field: "age", type: "greaterThan", filter: 11 },
        },
      ]);

      expect(first.where?.key).toBe(second.where?.key);
      expect(first.where?._tag).toBe("group");
      expect(rootNot.where?._tag).toBe("NOT");
      expect(compareRuntimeFilterExpressionStructure(first.where!, first.where!)).toBe(0);
      expect(compareRuntimeFilterExpressionStructure(first.where!, second.where!)).toBe(0);
      expect(compareRuntimeFilterExpressionStructure(rootNot.where!, first.where!)).not.toBe(0);
      expect(
        compareRuntimeFilterExpressionStructure(rootNot.where!, secondRootNot.where!),
      ).not.toBe(0);
    }),
  );

  it.effect("orders canonical collisions by code units and interns wide groups linearly", () =>
    Effect.gen(function* () {
      const CollatingRow = Schema.Struct({
        id: Schema.String,
        Å: Schema.Number,
        Å: Schema.Number,
      });
      const collatingMetadata = rawQueryCompilerMetadata(CollatingRow);
      const angstromLetter = {
        type: "OR",
        conditions: [
          { field: "Å", type: "equals", filter: 1 },
          { field: "id", type: "equals", filter: "same" },
        ],
      };
      const angstromSign = {
        type: "OR",
        conditions: [
          { field: "Å", type: "equals", filter: 1 },
          { field: "id", type: "equals", filter: "same" },
        ],
      };
      const first = yield* decodeRawQuery("collating", collatingMetadata, {
        select: ["id"],
        where: [angstromLetter, angstromSign],
      });
      const second = yield* decodeRawQuery("collating", collatingMetadata, {
        select: ["id"],
        where: [angstromSign, angstromLetter],
      });
      const wideConditions = Array.from({ length: 2_000 }, (_, index) => ({
        type: "OR",
        conditions: [
          { field: "age", type: "equals", filter: index },
          { field: "name", type: "equals", filter: `name-${index}` },
        ],
      }));
      const wide = yield* decodeWhere(wideConditions);

      expect(first.where?.key).toBe(second.where?.key);
      expect(wide.where?._tag).toBe("group");
      expect(wide.where?._tag === "group" ? wide.where.conditions.length : 0).toBe(2_000);
    }),
  );

  it.effect("normalizes deeply generated Boolean trees without materializing every subtree", () =>
    Effect.gen(function* () {
      let expression: unknown = { field: "age", type: "equals", filter: 0 };
      for (let index = 1; index <= 5_000; index += 1) {
        expression = {
          type: index % 2 === 0 ? "AND" : "OR",
          conditions: [{ field: "age", type: "equals", filter: index }, expression],
        };
      }
      const query = yield* decodeWhere([expression]);
      const root = query.where;
      expect(root).toBeDefined();

      let retainedKeyBytes = 0;
      let expressionCount = 0;
      const pending = root === undefined ? [] : [root];
      while (pending.length > 0) {
        const current = pending.pop()!;
        retainedKeyBytes += current.key.length;
        expressionCount += 1;
        if (current._tag === "group") {
          for (const condition of current.conditions) {
            pending.push(condition);
          }
        } else if (current._tag === "NOT") {
          pending.push(current.condition);
        }
      }

      expect(expressionCount).toBe(10_001);
      expect(retainedKeyBytes).toBeLessThan(5_000_000);
    }),
  );

  it.effect("normalizes a deeply shared same-type filter DAG in linear space", () =>
    Effect.gen(function* () {
      let expression: unknown = { field: "age", type: "equals", filter: 42 };
      for (let depth = 0; depth < 5_000; depth += 1) {
        expression = { type: "AND", conditions: [expression, expression] };
      }

      const query = yield* decodeWhere([expression]);

      expect(query.where?._tag).toBe("condition");
      expect(query.where?.key.includes("42")).toBe(true);
    }),
  );

  it.effect("materializes right-deep distinct-leaf AND and OR components once", () =>
    Effect.gen(function* () {
      const conditionCount = 12_000;
      const rightDeep = (type: "AND" | "OR"): unknown => {
        let expression: unknown = { field: "age", type: "equals", filter: 0 };
        for (let filter = 1; filter < conditionCount; filter += 1) {
          expression = {
            type,
            conditions: [{ field: "age", type: "equals", filter }, expression],
          };
        }
        return expression;
      };

      const conjunction = yield* decodeWhere([rightDeep("AND")]);
      const disjunction = yield* decodeWhere([rightDeep("OR")]);

      expect(conjunction.where?._tag).toBe("group");
      expect(conjunction.where?._tag === "group" ? conjunction.where.conditions.length : 0).toBe(
        conditionCount,
      );
      expect(disjunction.where?._tag).toBe("group");
      expect(disjunction.where?._tag === "group" ? disjunction.where.conditions.length : 0).toBe(
        conditionCount,
      );
    }),
  );

  it.effect("flattens a shared same-operator group restored through double negation", () =>
    Effect.gen(function* () {
      const sharedGroup = {
        type: "AND",
        conditions: [
          { field: "age", type: "equals", filter: 1 },
          { field: "name", type: "equals", filter: "alice" },
        ],
      };
      const restoredGroup = {
        type: "NOT",
        condition: { type: "NOT", condition: sharedGroup },
      };

      const query = yield* decodeWhere([restoredGroup, restoredGroup]);

      expect(query.where?._tag).toBe("group");
      expect(query.where?._tag === "group" ? query.where.conditions.length : 0).toBe(2);
      expect(
        query.where?._tag === "group"
          ? query.where.conditions.every((condition) => condition._tag === "condition")
          : false,
      ).toBe(true);
    }),
  );

  it.effect("keys and compiles non-collapsible shared filter DAGs in linear space", () =>
    Effect.gen(function* () {
      const makeDiamond = (): unknown => {
        let shared: unknown = { field: "age", type: "equals", filter: 0 };
        for (let depth = 1; depth <= 100; depth += 1) {
          shared = {
            type: "AND",
            conditions: [
              {
                type: "OR",
                conditions: [shared, { field: "age", type: "equals", filter: depth }],
              },
              {
                type: "OR",
                conditions: [shared, { field: "age", type: "equals", filter: -depth }],
              },
            ],
          };
        }
        return shared;
      };
      const first = yield* decodeWhere([makeDiamond()]);
      const second = yield* decodeWhere([makeDiamond()]);
      const predicate = compileRawPredicate<typeof Row.Type>(metadata, first.where);

      expect(first.where?.key).toBe(second.where?.key);
      expect(compareRuntimeFilterExpressionStructure(first.where!, second.where!)).toBe(0);
      expect(first.where?.key.length).toBeLessThan(100_000);
      expect(predicate.matches({ id: "matching", name: "x", age: 0 })).toBe(true);
      expect(predicate.matches({ id: "missing", name: "x", age: 1_000 })).toBe(false);
    }),
  );

  it.effect("normalizes and compiles groups wider than VM argument limits", () =>
    Effect.gen(function* () {
      const conditionCount = 125_000;
      const wideConditions = Array.from({ length: conditionCount }, (_, filter) => ({
        field: "age",
        type: "equals",
        filter,
      }));
      const normalized = yield* decodeWhere([{ type: "AND", conditions: wideConditions }]);
      const sharedCondition: RuntimeFilterExpression = {
        _tag: "condition",
        key: "shared-age-equals-one",
        field: "age",
        type: "equals",
        caseSensitive: false,
        accentSensitive: false,
        filter: 1,
      };
      const wideSharedGroup: RuntimeFilterExpression = {
        _tag: "group",
        key: "wide-shared-group",
        type: "OR",
        conditions: Array.from({ length: conditionCount }, () => sharedCondition),
      };
      const predicate = compileRawPredicate<typeof Row.Type>(metadata, wideSharedGroup);

      expect(normalized.where?._tag).toBe("group");
      expect(normalized.where?._tag === "group" ? normalized.where.conditions.length : 0).toBe(
        conditionCount,
      );
      expect(predicate.matches({ id: "matching", name: "x", age: 1 })).toBe(true);
      expect(predicate.matches({ id: "missing", name: "x", age: 2 })).toBe(false);
    }),
  );

  it.effect("compares shared expression pairs without revisiting them", () =>
    Effect.gen(function* () {
      const leftCondition = (yield* decodeWhere([{ field: "age", type: "equals", filter: 1 }]))
        .where!;
      const rightCondition = (yield* decodeWhere([{ field: "age", type: "equals", filter: 1 }]))
        .where!;
      const otherRightCondition = (yield* decodeWhere([
        { field: "age", type: "equals", filter: 1 },
      ])).where!;
      const group = (
        conditions: ReadonlyArray<RuntimeFilterExpression>,
      ): RuntimeFilterExpression => ({
        _tag: "group",
        type: "OR",
        conditions,
        key: "comparison-only",
      });

      expect(
        compareRuntimeFilterExpressionStructure(
          group([leftCondition, leftCondition]),
          group([rightCondition, rightCondition]),
        ),
      ).toBe(0);
      expect(
        compareRuntimeFilterExpressionStructure(
          group([leftCondition, leftCondition]),
          group([rightCondition, otherRightCondition]),
        ),
      ).toBe(0);
    }),
  );

  it.effect("evaluates shared groups through AND, OR, and NOT DAG instructions", () =>
    Effect.gen(function* () {
      const shared = {
        type: "OR",
        conditions: [
          { field: "age", type: "equals", filter: 1 },
          { field: "age", type: "equals", filter: 2 },
        ],
      };
      const query = yield* decodeWhere([
        {
          type: "AND",
          conditions: [shared, { type: "NOT", condition: shared }],
        },
      ]);
      const predicate = compileRawPredicate<typeof Row.Type>(metadata, query.where);

      expect(predicate.matches({ id: "one", name: "x", age: 1 })).toBe(false);
      expect(predicate.matches({ id: "three", name: "x", age: 3 })).toBe(false);
    }),
  );

  it("short-circuits shared DAGs and memoizes visited nodes with reusable state", () => {
    const matchingId: RuntimeFilterExpression = {
      _tag: "condition",
      key: "id-equals-match",
      field: "id",
      type: "equals",
      caseSensitive: false,
      accentSensitive: false,
      filter: "match",
    };
    const sharedAge: RuntimeFilterExpression = {
      _tag: "condition",
      key: "age-equals-one",
      field: "age",
      type: "equals",
      caseSensitive: false,
      accentSensitive: false,
      filter: 1,
    };
    const sharedBranch: RuntimeFilterExpression = {
      _tag: "group",
      key: "shared-age-branch",
      type: "AND",
      conditions: [sharedAge, sharedAge],
    };
    const expression: RuntimeFilterExpression = {
      _tag: "group",
      key: "short-circuit-root",
      type: "OR",
      conditions: [matchingId, sharedBranch],
    };
    const predicate = compileRawPredicate<typeof Row.Type>(metadata, expression);
    let ageReads = 0;
    const matchingRow = new Proxy(
      { id: "match", name: "x", age: 1 },
      {
        getOwnPropertyDescriptor: (target, property) => {
          if (property === "age") {
            ageReads += 1;
          }
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    );

    expect(predicate.matches(matchingRow)).toBe(true);
    expect(ageReads).toBe(0);

    const sharedMatchingRow = new Proxy(
      { id: "missing", name: "x", age: 1 },
      {
        getOwnPropertyDescriptor: (target, property) => {
          if (property === "age") {
            ageReads += 1;
          }
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    );
    expect(predicate.matches(sharedMatchingRow)).toBe(true);
    expect(ageReads).toBe(1);
  });

  it.effect("canonicalizes shared, flattened, negated, and BigDecimal expressions", () =>
    Effect.gen(function* () {
      const shared = { field: "name", type: "contains", filter: "alice" };
      const query = yield* decodeWhere([
        {
          type: "AND",
          conditions: [
            shared,
            shared,
            { type: "AND", conditions: [{ field: "age", type: "greaterThan", filter: 1 }] },
            {
              type: "NOT",
              condition: {
                type: "NOT",
                condition: { field: "name", type: "startsWith", filter: "a" },
              },
            },
            { type: "NOT", condition: { field: "name", type: "equals", filter: "blocked" } },
            { type: "NOT", condition: { field: "name", type: "notEqual", filter: "allowed" } },
            { type: "NOT", condition: { field: "name", type: "contains", filter: "blocked" } },
            { type: "NOT", condition: { field: "name", type: "notContains", filter: "allowed" } },
            { type: "NOT", condition: { field: "active", type: "blank" } },
            { type: "NOT", condition: { field: "active", type: "notBlank" } },
            { type: "NOT", condition: { type: "AND", conditions: [] } },
            {
              type: "NOT",
              condition: {
                type: "OR",
                conditions: [
                  { field: "name", type: "endsWith", filter: "x" },
                  { field: "active", type: "blank" },
                ],
              },
            },
          ],
        },
        { field: "amount", type: "equals", filter: BigDecimal.make(10n, 1) },
        { field: "amount", type: "notEqual", filter: BigDecimal.make(1n, 0) },
        { field: "age", type: "equals", filter: -0 },
        { field: "age", type: "greaterThan", filter: -0 },
      ]);

      expect(query.where?._tag).toBe("group");
      expect(query.where?.key.includes("notEqual")).toBe(true);
      expect(Object.isFrozen(query.where)).toBe(true);
    }),
  );

  it.effect("rejects hostile expression containers and invalid operands", () =>
    Effect.gen(function* () {
      class ArraySubclass extends Array<unknown> {}
      const symbolicWhere: Array<unknown> = [];
      Object.defineProperty(symbolicWhere, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const sparseWhere: Array<unknown> = [];
      sparseWhere.length = 1;
      const accessorWhere: Array<unknown> = [];
      Object.defineProperty(accessorWhere, "0", {
        enumerable: true,
        get: () => ({ field: "name", type: "blank" }),
      });
      accessorWhere.length = 1;
      const extraWhere: Array<unknown> = [];
      Object.defineProperty(extraWhere, "extra", { enumerable: true, value: true });
      const symbolicCondition = { field: "name", type: "blank" };
      Object.defineProperty(symbolicCondition, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const accessorCondition = { type: "equals", filter: "alice" };
      Object.defineProperty(accessorCondition, "field", {
        enumerable: true,
        get: () => "name",
      });
      const cyclicAnd = { type: "AND", conditions: [] as Array<unknown> };
      cyclicAnd.conditions.push(cyclicAnd);

      const cases: ReadonlyArray<readonly [unknown, string]> = [
        [new ArraySubclass(), "Query where must be an array."],
        [symbolicWhere, "Query where must not contain symbol properties."],
        [sparseWhere, "Query where must be a dense array of data values."],
        [accessorWhere, "Query where must be a dense array of data values."],
        [extraWhere, "Query where contains unsupported property: extra."],
        [[null], "Every filter expression must be a plain object."],
        [[{ type: "AND", conditions: [null] }], "Every filter expression must be a plain object."],
        [[cyclicAnd], "Filter expressions must not contain cycles."],
        [[symbolicCondition], "Filter expressions must not contain symbol properties."],
        [
          [accessorCondition],
          "Filter expression property field must be an own enumerable data property.",
        ],
        [
          [{ field: 1, type: "equals", filter: "alice" }],
          "Filter condition field must be a string.",
        ],
        [
          [{ field: "missing", type: "equals", filter: "alice" }],
          "Filter condition references unknown or non-filterable field: missing.",
        ],
        [
          [{ field: "name", type: "equals", filter: "alice", caseSensitive: "yes" }],
          "Text Matching options must be booleans when present.",
        ],
        [
          [{ field: "age", type: "equals", filter: Number.POSITIVE_INFINITY }],
          "Filter numbers must be finite.",
        ],
        [
          [{ field: "age", type: "equals", filter: { value: 1 } }],
          "Filter operand for age does not satisfy its configured schema.",
        ],
        [
          [{ field: "name", type: "equals", filter: undefined }],
          "Filter operands must not be undefined.",
        ],
        [
          [{ field: "name", type: "equals", filter: 1 }],
          "Filter operand for name does not satisfy its configured schema.",
        ],
        [
          [{ field: "name", type: "in", filter: "alice" }],
          "Filter condition name in.filter must be an array.",
        ],
        [
          [{ field: "name", type: "contains", filter: "" }],
          "Filter condition name contains requires a non-empty search value.",
        ],
        [
          [{ field: "age", type: "contains", filter: "1" }],
          "Filter field age does not support contains.",
        ],
        [
          [{ field: "age", type: "greaterThan", filter: "1" }],
          "Filter field age does not support this numeric operand domain.",
        ],
        [
          [{ field: "age", type: "greaterThan", filter: { value: 1 } }],
          "Filter operands must be supported scalar values.",
        ],
        [
          [
            {
              field: "amount",
              type: "equals",
              filter: BigDecimal.make(1n, Number.NaN),
            },
          ],
          "Filter operands must be supported scalar values.",
        ],
        [
          [
            {
              field: "amount",
              type: "greaterThan",
              filter: BigDecimal.make(1n, Number.POSITIVE_INFINITY),
            },
          ],
          "Filter operands must be supported scalar values.",
        ],
        [
          [
            {
              field: "amount",
              type: "inRange",
              filter: BigDecimal.make(1n, 0),
              filterTo: BigDecimal.make(2n, 1.5),
            },
          ],
          "Filter operands must be supported scalar values.",
        ],
        [
          [{ field: "age", type: "inRange", filter: 3, filterTo: 3 }],
          "Filter condition age inRange requires filter < filterTo.",
        ],
        [
          [{ field: "mixed", type: "inRange", filter: 1, filterTo: 2n }],
          "Filter condition mixed inRange requires filter < filterTo.",
        ],
        [
          [{ type: "AND", conditions: [], extra: true }],
          "Filter expression contains unsupported property: extra.",
        ],
        [[{ type: "OR", conditions: null }], "Filter group OR.conditions must be an array."],
        [
          [{ type: "NOT", condition: {}, extra: true }],
          "Filter expression contains unsupported property: extra.",
        ],
      ];

      for (const [where, message] of cases) {
        const error = yield* Effect.flip(decodeWhere(where));
        expect(error.message).toBe(message);
      }
    }),
  );
});
