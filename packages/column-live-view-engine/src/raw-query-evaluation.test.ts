import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { format, fromStringUnsafe, make as makeBigDecimal } from "effect/BigDecimal";
import { evaluateRawQuery } from "./active-query";
import { compareQueryValue, prepareRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { compileRawPredicate } from "./raw-predicate-compiler";
import {
  normalizeWhere,
  type RuntimeFilterCondition,
  type RuntimeFilterConditionType,
  type RuntimeFilterScalar,
} from "./filter-expression";
import { predicateFilterPlans } from "./raw-predicate-plan";
import { order, Order, position, Position } from "../test-harness/public-engine";

const runtimeCondition = (
  field: string,
  type: RuntimeFilterConditionType,
  filter?: RuntimeFilterScalar | ReadonlyArray<RuntimeFilterScalar>,
  filterTo?: RuntimeFilterScalar,
  caseSensitive = false,
  accentSensitive = false,
): RuntimeFilterCondition =>
  Object.freeze({
    _tag: "condition",
    key: `${field}:${type}`,
    field,
    type,
    caseSensitive,
    accentSensitive,
    ...(filter === undefined ? {} : { filter }),
    ...(filterTo === undefined ? {} : { filterTo }),
  });

describe("Raw query evaluation", () => {
  it.effect("preserves compiled predicate miss behavior for custom storage scanners", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id"],
        where: [
          { field: "status", type: "equals", filter: "open" },
          { field: "customerId", type: "startsWith", filter: "customer-" },
        ],
      });
      const rows = [
        order("1", "closed", 10, 1),
        { ...order("2", "open", 20, 2), customerId: "account-2" },
      ];

      const evaluation = evaluateRawQuery(
        {
          scanRawWindow: (plan) => {
            const window = rows
              .filter((row) => plan.matches(row))
              .map((row) => ({
                key: row.id,
                row,
              }));
            return {
              keys: window.map((entry) => entry.key),
              window,
              totalRows: window.length,
            };
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation.rows).toStrictEqual([]);
      expect(evaluation.totalRows).toBe(0);
    }),
  );

  it("owns and freezes the full compiled predicate proof graph", () => {
    const prices = [10, 20];
    const metadata = rawQueryCompilerMetadata(Order);
    const predicate = compileRawPredicate<typeof Order.Type>(
      metadata,
      normalizeWhere(
        [
          { field: "price", type: "in", filter: prices },
          { field: "updatedAt", type: "greaterThanOrEqual", filter: 1 },
        ],
        metadata.filterFields,
      ),
    );
    const inFilter = Reflect.get(predicate.plan.filters, "0");
    const rangeFilter = Reflect.get(predicate.plan.filters, "1");
    const values = Reflect.get(inFilter, "values");
    const valueKeys = Reflect.get(inFilter, "valueKeys");

    expect(Object.isFrozen(predicate)).toBe(true);
    expect(Object.isFrozen(predicate.plan)).toBe(true);
    expect(Object.isFrozen(predicate.plan.filters)).toBe(true);
    expect(Object.isFrozen(inFilter)).toBe(true);
    expect(Object.isFrozen(rangeFilter)).toBe(true);
    expect(Object.isFrozen(values)).toBe(true);
    expect(Object.isFrozen(valueKeys)).toBe(true);
    expect(values).toStrictEqual([10, 20]);
    expect(valueKeys.has("number:10")).toBe(true);
    expect(valueKeys.has("number:20")).toBe(true);

    expect(() => Object.assign(predicate, { matches: () => false })).toThrowError(TypeError);
    expect(() => Object.assign(predicate.plan, { callbackRequired: true })).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(predicate.plan.filters, rangeFilter)).toThrowError(
      TypeError,
    );
    expect(() => Object.assign(inFilter, { field: "price" })).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(values, 30)).toThrowError(TypeError);
    expect(() => Set.prototype.add.call(valueKeys, "number:30")).toThrowError(TypeError);

    prices.push(30);
    expect(values).toStrictEqual([10, 20]);
    expect(predicate.matches(order("open", "open", 10, 1))).toBe(true);
    expect(predicate.matches(order("closed", "closed", 30, 1))).toBe(false);
  });

  it("owns immutable BigDecimal values exposed by predicate plans", () => {
    const source = fromStringUnsafe("1.00");
    const metadata = rawQueryCompilerMetadata(Position);
    const predicate = compileRawPredicate<typeof Position.Type>(
      metadata,
      normalizeWhere(
        [{ field: "price", type: "greaterThanOrEqual", filter: source }],
        metadata.filterFields,
      ),
    );
    const filter = Reflect.get(predicate.plan.filters, "0");
    const planValue = Reflect.get(filter, "value");
    const normalized = Reflect.get(planValue, "normalized");

    expect(Object.isFrozen(planValue)).toBe(true);
    expect(Object.isFrozen(normalized)).toBe(true);
    expect(format(planValue)).toBe("1");
    expect(() => Object.assign(planValue, { value: 999n })).toThrowError(TypeError);

    Object.assign(source, { value: 999n });
    expect(format(planValue)).toBe("1");
    expect(predicate.matches(position("low", "LOW", 1n, "0.5"))).toBe(false);
    expect(predicate.matches(position("high", "HIGH", 1n, "2"))).toBe(true);
  });

  it.effect("keeps BigDecimal row ordering total for safe and forged unsafe scales", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRawQuery("positions", rawQueryCompilerMetadata(Position), {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const tiny = {
        key: "tiny",
        row: {
          ...position("tiny", "TINY", 1n, "1"),
          price: makeBigDecimal(1n, Number.MAX_SAFE_INTEGER),
        },
      };
      const huge = {
        key: "huge",
        row: {
          ...position("huge", "HUGE", 1n, "1"),
          price: makeBigDecimal(1n, Number.MIN_SAFE_INTEGER),
        },
      };
      const malformed = {
        key: "malformed",
        row: {
          ...position("malformed", "MALFORMED", 1n, "1"),
          price: makeBigDecimal(1n, Number.NaN),
        },
      };

      expect(compiled.plan.compare(tiny, huge)).toBe(-1);
      expect(Number.isFinite(compiled.plan.compare(malformed, tiny))).toBe(true);
    }),
  );

  it("orders array and object fallback values by their stable query ranks", () => {
    expect({
      arrayBeforeObject: compareQueryValue([], {}),
      objectAfterArray: compareQueryValue({}, []),
    }).toStrictEqual({
      arrayBeforeObject: -1,
      objectAfterArray: 1,
    });
  });

  it("keeps forged runtime predicate states conservative", () => {
    const metadata = rawQueryCompilerMetadata(Order);
    const row = order("row", "open", 10, 1);
    const cases = [
      [runtimeCondition("missing", "equals", "x"), false],
      [runtimeCondition("status", "notBlank"), true],
      [runtimeCondition("status", "equals"), false],
      [runtimeCondition("status", "notEqual"), true],
      [runtimeCondition("status", "in"), false],
      [runtimeCondition("status", "contains"), false],
      [runtimeCondition("price", "greaterThan"), false],
      [runtimeCondition("price", "greaterThan", 1), true],
      [runtimeCondition("price", "inRange"), false],
    ] as const;

    for (const [condition, expected] of cases) {
      expect(compileRawPredicate(metadata, condition).matches(row)).toBe(expected);
    }
    expect(
      compileRawPredicate(metadata, runtimeCondition("price", "greaterThan", 1)).matches({
        ...row,
        price: "not-a-number",
      }),
    ).toBe(false);
    expect(
      compileRawPredicate(metadata, runtimeCondition("price", "inRange", 1, 2)).matches({
        ...row,
        price: "not-a-number",
      }),
    ).toBe(false);
    expect(
      compileRawPredicate(metadata, runtimeCondition("status", "contains", "open")).matches({
        ...row,
        status: 1,
      }),
    ).toBe(false);
  });

  it("marks forged non-indexable predicate operands for callback evaluation", () => {
    const metadata = rawQueryCompilerMetadata(Order);

    expect(predicateFilterPlans(runtimeCondition("price", "equals", [1]), metadata)).toStrictEqual({
      filters: [],
      callbackRequired: true,
    });
    expect(
      predicateFilterPlans(runtimeCondition("price", "greaterThan", "1"), metadata),
    ).toStrictEqual({ filters: [], callbackRequired: true });
    expect(predicateFilterPlans(runtimeCondition("price", "inRange", 1), metadata)).toStrictEqual({
      filters: [],
      callbackRequired: true,
    });
  });

  it("plans normalized text equality without weakening sensitivity semantics", () => {
    const metadata = rawQueryCompilerMetadata(Order);

    expect(
      predicateFilterPlans(runtimeCondition("status", "equals", "resume"), metadata),
    ).toStrictEqual({
      filters: [
        {
          field: "status",
          operator: "textEq",
          value: "resume",
          caseSensitive: false,
          accentSensitive: false,
        },
      ],
      callbackRequired: false,
    });
    const textInPlan = predicateFilterPlans(
      runtimeCondition("status", "in", ["resume", "open"], undefined, true, true),
      metadata,
    );
    const textInValueSet = Reflect.get(textInPlan.filters[0]!, "valueSet");
    expect(textInPlan).toStrictEqual({
      filters: [
        {
          field: "status",
          operator: "textIn",
          values: ["resume", "open"],
          valueSet: textInValueSet,
          caseSensitive: true,
          accentSensitive: true,
        },
      ],
      callbackRequired: false,
    });
    expect(Reflect.get(textInValueSet, "size")).toBe(2);
  });
});
