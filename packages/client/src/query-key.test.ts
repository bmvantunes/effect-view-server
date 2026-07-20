import { describe, expect, it } from "@effect/vitest";
import { fromStringUnsafe, make } from "effect/BigDecimal";
import { stableQueryKey } from "./query-key";
import { canonicalWhereKey, compareCanonicalWhereExpressions } from "./query-where-key";

type FilterNode =
  | {
      readonly field: "name";
      readonly type: "equals";
      readonly filter: string;
    }
  | {
      readonly type: "AND";
      readonly conditions: ReadonlyArray<FilterNode>;
    };

const leaf = (): FilterNode => ({ field: "name", type: "equals", filter: "Ada" });

const deeplyNestedQuery = (depth: number): object => {
  let condition = leaf();
  for (let index = 0; index < depth; index += 1) {
    condition = { type: "AND", conditions: [condition] };
  }
  return { select: ["name"], where: [condition] };
};

const sharedDagQuery = (depth: number): object => {
  let condition = leaf();
  for (let index = 0; index < depth; index += 1) {
    condition = { type: "AND", conditions: [condition, condition] };
  }
  return { select: ["name"], where: [condition] };
};

const nonCollapsibleDiamondQuery = (depth: number, share: boolean): object => {
  let condition: unknown = { field: "name", type: "equals", filter: "root" };
  for (let index = 0; index < depth; index += 1) {
    const rightCondition = share ? condition : structuredClone(condition);
    condition = {
      type: "AND",
      conditions: [
        {
          type: "OR",
          conditions: [condition, { field: "name", type: "equals", filter: `left-${index}` }],
        },
        {
          type: "OR",
          conditions: [rightCondition, { field: "name", type: "equals", filter: `right-${index}` }],
        },
      ],
    };
  }
  return { select: ["name"], where: [condition] };
};

const repeatedSharedGroupQuery = (size: number): object => {
  const shared = {
    type: "AND",
    conditions: Array.from({ length: size }, (_, index) => ({
      field: "name",
      type: "equals",
      filter: `name-${index}`,
    })),
  };
  return {
    where: Array.from({ length: size }, () => shared),
  };
};

const rightDeepDistinctQuery = (size: number): object => {
  let condition: unknown = {
    field: "name",
    type: "equals",
    filter: `name-${size - 1}`,
  };
  for (let index = size - 2; index >= 0; index -= 1) {
    condition = {
      type: "AND",
      conditions: [{ field: "name", type: "equals", filter: `name-${index}` }, condition],
    };
  }
  return { select: ["name"], where: [condition] };
};

describe("stableQueryKey", () => {
  it("uses a deterministic invalid identity without invoking getters or property reads", () => {
    let getterReads = 0;
    let proxyReads = 0;
    let arrayProxyReads = 0;
    const accessorQuery = { select: ["name"] };
    Object.defineProperty(accessorQuery, "where", {
      enumerable: true,
      get: () => {
        getterReads += 1;
        throw new Error("query getter must not run");
      },
    });
    const sparseWhere: Array<unknown> = [];
    sparseWhere.length = 1;
    const symbolicQuery = { select: ["name"] };
    Object.defineProperty(symbolicQuery, Symbol("query"), {
      enumerable: true,
      value: true,
    });
    const throwingProxy = new Proxy(
      { select: ["name"] },
      {
        ownKeys: () => {
          throw new Error("query proxy must not escape");
        },
      },
    );
    const readableProxy = new Proxy(
      { select: ["name"] },
      {
        get: () => {
          proxyReads += 1;
          throw new Error("query property reads must not run");
        },
      },
    );
    const readableArrayProxy = new Proxy([{ field: "name", type: "equals", filter: "Ada" }], {
      get: () => {
        arrayProxyReads += 1;
        throw new Error("query array property reads must not run");
      },
    });

    const accessorKey = stableQueryKey(accessorQuery);
    const sparseKey = stableQueryKey({ select: ["name"], where: sparseWhere });
    const symbolicKey = stableQueryKey(symbolicQuery);
    const proxyKey = stableQueryKey(throwingProxy);

    expect(getterReads).toBe(0);
    expect(proxyReads).toBe(0);
    expect(stableQueryKey(readableProxy)).toBe(stableQueryKey({ select: ["name"] }));
    expect(stableQueryKey({ select: ["name"], where: readableArrayProxy })).toBe(
      stableQueryKey({
        select: ["name"],
        where: [{ field: "name", type: "equals", filter: "Ada" }],
      }),
    );
    expect(arrayProxyReads).toBe(0);
    expect([accessorKey, sparseKey, symbolicKey, proxyKey]).toStrictEqual([
      accessorKey,
      accessorKey,
      accessorKey,
      accessorKey,
    ]);
    expect(accessorKey).not.toBe(stableQueryKey({ select: ["name"] }));
  });

  it("canonicalizes where using the engine's recursive filter semantics", () => {
    const expanded = {
      select: ["name"],
      where: [
        {
          field: "name",
          type: "contains",
          filter: "Résumé",
          caseSensitive: false,
          accentSensitive: false,
        },
        {
          type: "OR",
          conditions: [
            { type: "OR", conditions: [] },
            { field: "amount", type: "equals", filter: -0 },
            { field: "amount", type: "equals", filter: 0 },
            {
              type: "OR",
              conditions: [
                { field: "status", type: "equals", filter: "OPEN" },
                { field: "status", type: "equals", filter: "open" },
              ],
            },
          ],
        },
        {
          type: "NOT",
          condition: {
            type: "NOT",
            condition: { field: "region", type: "equals", filter: "USÁ" },
          },
        },
        { field: "name", type: "in", filter: [] },
      ],
    };
    const canonical = {
      select: ["name"],
      where: [
        {
          type: "AND",
          conditions: [
            { field: "region", type: "equals", filter: "usa" },
            {
              type: "OR",
              conditions: [
                { field: "status", type: "equals", filter: "open" },
                { field: "amount", type: "equals", filter: 0 },
              ],
            },
            { field: "name", type: "contains", filter: "resume" },
          ],
        },
      ],
    };
    const reorderedIn = {
      select: ["name"],
      where: [
        {
          field: "name",
          type: "in",
          filter: ["RÉSUMÉ", "other", "résumé", "other"],
        },
      ],
    };
    const canonicalIn = {
      select: ["name"],
      where: [{ field: "name", type: "in", filter: ["resume", "other"] }],
    };
    const doubleNot = {
      select: ["name"],
      where: [
        {
          type: "NOT",
          condition: {
            type: "NOT",
            condition: { field: "name", type: "startsWith", filter: "A" },
          },
        },
      ],
    };

    expect(stableQueryKey(expanded)).toBe(stableQueryKey(canonical));
    expect(stableQueryKey(reorderedIn)).toBe(stableQueryKey(canonicalIn));
    expect(stableQueryKey(doubleNot)).toBe(
      stableQueryKey({
        select: ["name"],
        where: [{ field: "name", type: "startsWith", filter: "a" }],
      }),
    );
    expect(stableQueryKey({ select: ["name"] })).toBe(
      stableQueryKey({
        select: ["name"],
        where: [{ type: "OR", conditions: [{ type: "AND", conditions: [] }] }],
      }),
    );
    expect(
      stableQueryKey({
        select: ["name"],
        where: [{ field: "mixed", type: "equals", filter: 1, caseSensitive: true }],
      }),
    ).toBe(
      stableQueryKey({
        select: ["name"],
        where: [{ field: "mixed", type: "equals", filter: 1 }],
      }),
    );
  });

  it("keeps meaningful where differences distinct after canonicalization", () => {
    const base = {
      select: ["name"],
      where: [{ field: "name", type: "contains", filter: "resume" }],
    };

    expect(stableQueryKey(base)).not.toBe(
      stableQueryKey({
        select: ["name"],
        where: [
          {
            field: "name",
            type: "contains",
            filter: "resume",
            accentSensitive: true,
          },
        ],
      }),
    );
    expect(stableQueryKey(base)).not.toBe(
      stableQueryKey({
        select: ["name"],
        where: [{ field: "name", type: "contains", filter: "different" }],
      }),
    );
  });

  it("canonicalizes every supported operator and scalar domain", () => {
    const where = [
      { field: "text", type: "equals", filter: "RÉSUMÉ", caseSensitive: true },
      { field: "nullable", type: "equals", filter: null },
      { field: "enabled", type: "equals", filter: true },
      { field: "quantity", type: "equals", filter: 10n },
      { field: "amount", type: "equals", filter: fromStringUnsafe("1.50") },
      { field: "text", type: "notEqual", filter: "other", accentSensitive: true },
      {
        field: "mixed",
        type: "in",
        filter: [null, false, 1, 1n, fromStringUnsafe("2.00"), "Á"],
        caseSensitive: true,
        accentSensitive: true,
      },
      { field: "quantity", type: "in", filter: [2n, 1n, 2n], caseSensitive: true },
      { field: "number", type: "greaterThan", filter: 1 },
      { field: "quantity", type: "greaterThanOrEqual", filter: 1n },
      { field: "amount", type: "lessThan", filter: fromStringUnsafe("2") },
      { field: "number", type: "lessThanOrEqual", filter: 3 },
      { field: "number", type: "inRange", filter: 1, filterTo: 2 },
      { field: "quantity", type: "inRange", filter: 1n, filterTo: 2n },
      {
        field: "amount",
        type: "inRange",
        filter: fromStringUnsafe("1"),
        filterTo: fromStringUnsafe("2"),
      },
      { field: "text", type: "contains", filter: "A", caseSensitive: true },
      { field: "text", type: "notContains", filter: "B" },
      { field: "text", type: "startsWith", filter: "C" },
      { field: "text", type: "endsWith", filter: "D" },
      { field: "text", type: "blank" },
      { field: "text", type: "notBlank" },
    ];

    expect(typeof canonicalWhereKey(where)).toBe("string");
    expect(canonicalWhereKey(where)).toBe(canonicalWhereKey([...where].reverse()));
    expect(
      typeof canonicalWhereKey([
        { type: "NOT", condition: { field: "a", type: "contains", filter: "a" } },
        { type: "NOT", condition: { field: "b", type: "notContains", filter: "b" } },
        { type: "NOT", condition: { field: "c", type: "blank" } },
        { type: "NOT", condition: { field: "d", type: "notBlank" } },
        { type: "NOT", condition: { field: "e", type: "startsWith", filter: "e" } },
        {
          type: "NOT",
          condition: {
            type: "OR",
            conditions: [
              { field: "f", type: "equals", filter: "f" },
              { field: "g", type: "equals", filter: "g" },
            ],
          },
        },
      ]),
    ).toBe("string");
    expect(canonicalWhereKey([{ type: "NOT", condition: { type: "OR", conditions: [] } }])).toBe(
      undefined,
    );
  });

  it("rejects every malformed where ownership and condition shape without invoking accessors", () => {
    const invalidKey = stableQueryKey({ where: undefined });
    let expressionReads = 0;
    const accessorExpression = { field: "name", type: "equals" };
    Object.defineProperty(accessorExpression, "filter", {
      enumerable: true,
      get: () => {
        expressionReads += 1;
        return "Ada";
      },
    });
    const missingDescriptorExpression = new Proxy(
      {},
      {
        ownKeys: () => ["ghost"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    const symbolicExpression = { field: "name", type: "equals", filter: "Ada" };
    Object.defineProperty(symbolicExpression, Symbol("expression"), {
      enumerable: true,
      value: true,
    });
    const symbolicWhere: Array<unknown> = [];
    Object.defineProperty(symbolicWhere, Symbol("where"), { enumerable: true, value: true });
    const extendedWhere: Array<unknown> = [];
    Reflect.set(extendedWhere, "extra", true);
    class CustomWhere extends Array<unknown> {}
    const cyclicConditions: Array<unknown> = [];
    const cyclicGroup = { type: "OR", conditions: cyclicConditions };
    cyclicGroup.conditions.push(cyclicGroup);

    expect(stableQueryKey({ where: null })).toBe(invalidKey);
    expect(stableQueryKey({ where: {} })).toBe(invalidKey);
    expect(stableQueryKey({ where: new CustomWhere() })).toBe(invalidKey);
    expect(stableQueryKey({ where: symbolicWhere })).toBe(invalidKey);
    expect(stableQueryKey({ where: extendedWhere })).toBe(invalidKey);
    expect(stableQueryKey({ where: [null] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [[]] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [Object.create(null)] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [symbolicExpression] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [accessorExpression] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [missingDescriptorExpression] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [cyclicGroup] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ field: "name", filter: "Ada" }] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ type: "unknown", field: "name", filter: "Ada" }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "equals", field: 1, filter: "Ada" }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "AND", unexpected: [], conditions: [] }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "AND", unexpected: [] }] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ type: "blank", field: "name", filter: "Ada" }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "equals", field: "name" }] })).toBe(invalidKey);
    expect(
      stableQueryKey({
        where: [{ type: "equals", field: "name", filter: "Ada", unexpected: true }],
      }),
    ).toBe(invalidKey);
    expect(
      stableQueryKey({
        where: [{ type: "equals", field: "name", filter: "Ada", caseSensitive: "yes" }],
      }),
    ).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ type: "in", field: "name", filter: {} }] })).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ type: "contains", field: "name", filter: 1 }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "contains", field: "name", filter: "" }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "equals", field: "name", filter: undefined }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "equals", field: "name", filter: () => true }] })).toBe(
      invalidKey,
    );
    expect(
      stableQueryKey({ where: [{ type: "equals", field: "name", filter: Symbol("filter") }] }),
    ).toBe(invalidKey);
    expect(stableQueryKey({ where: [{ type: "equals", field: "name", filter: Infinity }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "equals", field: "name", filter: {} }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "greaterThan", field: "name", filter: "1" }] })).toBe(
      invalidKey,
    );
    expect(stableQueryKey({ where: [{ type: "inRange", field: "number", filter: 1 }] })).toBe(
      invalidKey,
    );
    expect(
      stableQueryKey({
        where: [{ type: "inRange", field: "number", filter: 1, filterTo: 2n }],
      }),
    ).toBe(invalidKey);
    expect(
      stableQueryKey({
        where: [{ type: "inRange", field: "number", filter: "1", filterTo: 2 }],
      }),
    ).toBe(invalidKey);
    expect(expressionReads).toBe(0);
  });

  it("rejects hostile generic arrays and reuses completed generic graph nodes", () => {
    const invalidKey = stableQueryKey(Object.create(null));
    const symbolicArray: Array<unknown> = [];
    Object.defineProperty(symbolicArray, Symbol("array"), { enumerable: true, value: true });
    const accessorArray: Array<unknown> = [undefined];
    Object.defineProperty(accessorArray, "0", { enumerable: true, get: () => "computed" });
    const extendedArray: Array<unknown> = [];
    Reflect.set(extendedArray, "extra", true);
    class CustomArray extends Array<unknown> {}
    const shared = { nested: true };

    expect(stableQueryKey({ value: symbolicArray })).toBe(invalidKey);
    expect(stableQueryKey({ value: accessorArray })).toBe(invalidKey);
    expect(stableQueryKey({ value: extendedArray })).toBe(invalidKey);
    expect(stableQueryKey({ value: new CustomArray() })).toBe(invalidKey);
    expect(stableQueryKey({ first: shared, second: shared })).toBe(
      stableQueryKey({ first: { nested: true }, second: { nested: true } }),
    );
  });

  it("orders canonical expression DAGs without repeating structural comparisons", () => {
    const leftLeaf = Object.freeze({
      _tag: "condition",
      field: "name",
      type: "equals",
      caseSensitive: false,
      accentSensitive: false,
      filterKey: "same",
      filterToKey: undefined,
      key: "same",
    });
    const firstRightLeaf = Object.freeze({ ...leftLeaf });
    const secondRightLeaf = Object.freeze({ ...leftLeaf });
    const repeatedLeft = Object.freeze({
      _tag: "group",
      type: "OR",
      conditions: Object.freeze([leftLeaf, leftLeaf]),
    });
    const repeatedPairRight = Object.freeze({
      _tag: "group",
      type: "OR",
      conditions: Object.freeze([firstRightLeaf, firstRightLeaf]),
    });
    const repeatedLeftOnlyRight = Object.freeze({
      _tag: "group",
      type: "OR",
      conditions: Object.freeze([firstRightLeaf, secondRightLeaf]),
    });

    expect(compareCanonicalWhereExpressions(leftLeaf, leftLeaf)).toBe(0);
    expect(compareCanonicalWhereExpressions(repeatedLeft, repeatedPairRight)).toBe(0);
    expect(compareCanonicalWhereExpressions(repeatedLeft, repeatedLeftOnlyRight)).toBe(0);
  });

  it("serializes deeply nested valid filters without recursive stack growth", () => {
    const query = deeplyNestedQuery(12_000);

    const key = stableQueryKey(query);

    expect(key.length).toBeGreaterThan(0);
    expect(stableQueryKey(query)).toBe(key);
  });

  it("serializes shared filter DAGs without expanding every logical path", () => {
    const first = stableQueryKey(sharedDagQuery(30));
    const second = stableQueryKey(sharedDagQuery(30));

    expect(first).toBe(second);
    expect(first.length).toBeLessThan(20_000);
  });

  it("expands wide shared same-type groups only once", () => {
    const query = repeatedSharedGroupQuery(12_000);
    const key = stableQueryKey(query);

    expect(key).toBe(stableQueryKey(query));
    expect(key.length).toBeLessThan(5_000_000);
  });

  it("flattens right-deep distinct same-type groups without quadratic rebuilding", () => {
    const size = 8_192;
    const flat = {
      select: ["name"],
      where: Array.from({ length: size }, (_, index) => ({
        field: "name",
        type: "equals",
        filter: `name-${index}`,
      })),
    };

    expect(stableQueryKey(rightDeepDistinctQuery(size))).toBe(stableQueryKey(flat));
  });

  it("serializes non-collapsible filter diamonds as bounded canonical graphs", () => {
    const deepShared = stableQueryKey(nonCollapsibleDiamondQuery(100, true));

    expect(deepShared.length).toBeLessThan(100_000);
    expect(stableQueryKey(nonCollapsibleDiamondQuery(100, true))).toBe(deepShared);
    expect(stableQueryKey(nonCollapsibleDiamondQuery(8, true))).toBe(
      stableQueryKey(nonCollapsibleDiamondQuery(8, false)),
    );
  });

  it("canonicalizes structurally equal shared and duplicated children", () => {
    const shared = leaf();
    const sharedQuery = {
      where: [{ type: "AND", conditions: [shared, shared] }],
    };
    const duplicatedQuery = {
      where: [{ type: "AND", conditions: [leaf(), leaf()] }],
    };

    expect(stableQueryKey(sharedQuery)).toBe(stableQueryKey(duplicatedQuery));
  });

  it("flattens a same-type group recovered through double negation", () => {
    const nested = {
      where: [
        {
          type: "OR",
          conditions: [
            {
              type: "NOT",
              condition: {
                type: "NOT",
                condition: {
                  type: "OR",
                  conditions: [
                    { field: "name", type: "equals", filter: "Ada" },
                    { field: "name", type: "equals", filter: "Grace" },
                  ],
                },
              },
            },
            { field: "name", type: "equals", filter: "Linus" },
          ],
        },
      ],
    };
    const flat = {
      where: [
        {
          type: "OR",
          conditions: [
            { field: "name", type: "equals", filter: "Ada" },
            { field: "name", type: "equals", filter: "Grace" },
            { field: "name", type: "equals", filter: "Linus" },
          ],
        },
      ],
    };

    expect(stableQueryKey(nested)).toBe(stableQueryKey(flat));
  });

  it("preserves exact BigDecimal route identity while normalizing filter operands", () => {
    const exactScaleRoute = {
      select: ["name"],
      routeBy: { amount: fromStringUnsafe("1.50") },
    };
    const normalizedScaleRoute = {
      select: ["name"],
      routeBy: { amount: fromStringUnsafe("1.5") },
    };
    const exactScaleFilter = {
      select: ["name"],
      where: [{ field: "amount", type: "equals", filter: fromStringUnsafe("1.50") }],
    };
    const normalizedScaleFilter = {
      select: ["name"],
      where: [{ field: "amount", type: "equals", filter: fromStringUnsafe("1.5") }],
    };

    expect(stableQueryKey(exactScaleRoute)).not.toBe(stableQueryKey(normalizedScaleRoute));
    expect(stableQueryKey(exactScaleFilter)).toBe(stableQueryKey(normalizedScaleFilter));
  });

  it("uses a deterministic unsupported token for malformed BigDecimal values", () => {
    const invalidScale = make(150n, Number.POSITIVE_INFINITY);
    const semanticCollection = new Map([["amount", invalidScale]]);

    expect(() => stableQueryKey({ routeBy: { amount: invalidScale } })).not.toThrow();
    expect(stableQueryKey({ routeBy: { amount: invalidScale } })).toBe(
      stableQueryKey({ routeBy: { amount: make(150n, Number.NaN) } }),
    );
    expect(stableQueryKey({ value: semanticCollection })).toBe(
      stableQueryKey({ value: new Map([["amount", make(150n, Number.NaN)]]) }),
    );
  });

  it("preserves canonical nested collection values and their cycle markers", () => {
    class UnsupportedValue {}

    const recursiveArray: Array<unknown> = [];
    recursiveArray.push(recursiveArray);
    const recursiveObject: { self?: unknown; z: number; a: number } = { z: 2, a: 1 };
    recursiveObject.self = recursiveObject;
    const values = [
      null,
      undefined,
      true,
      Number.NaN,
      "value",
      10n,
      fromStringUnsafe("1.50"),
      Symbol("value"),
      () => undefined,
      new UnsupportedValue(),
      recursiveArray,
      recursiveObject,
    ];

    const first = stableQueryKey({
      value: new Map<unknown, unknown>([
        ["values", values],
        ["set", new Set(["b", "a"])],
      ]),
    });
    const second = stableQueryKey({
      value: new Map<unknown, unknown>([
        ["set", new Set(["a", "b"])],
        ["values", values],
      ]),
    });

    expect(first).toBe(second);
    expect(stableQueryKey({ routeBy: { nested: new Set([fromStringUnsafe("1.50")]) } })).not.toBe(
      stableQueryKey({ routeBy: { nested: new Set([fromStringUnsafe("1.5")]) } }),
    );
  });
});
