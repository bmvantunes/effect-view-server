import { describe, expect, it } from "@effect/vitest";
import { canonicalWhereKey, compareCanonicalWhereExpressions } from "./query-where-key";
import { stableQueryKey } from "./query-key";

describe("explicit match-none query keys", () => {
  it("gives FALSE one stable semantic key independent of object identity", () => {
    const first = [{ type: "FALSE" }];
    const second = [{ type: "FALSE" }];

    expect(canonicalWhereKey(first)).toBe(canonicalWhereKey(second));
    expect(canonicalWhereKey(first)).not.toBe(
      canonicalWhereKey([{ type: "NOT", condition: { type: "FALSE" } }]),
    );
    expect(stableQueryKey({ select: ["id"], where: first })).toBe(
      stableQueryKey({ select: ["id"], where: second }),
    );
    expect(canonicalWhereKey([{ type: "AND", conditions: [{ type: "FALSE" }, first[0]!] }])).toBe(
      falseKeyForTest(),
    );
    const canonicalFalse = { _tag: "false" } as const;
    const canonicalTrue = { _tag: "true" } as const;
    expect(compareCanonicalWhereExpressions(canonicalFalse, canonicalTrue)).not.toBe(0);
  });

  it("keeps Boolean composition and empty in no-op semantics distinct", () => {
    const falseKey = canonicalWhereKey([{ type: "FALSE" }]);
    const andKey = canonicalWhereKey([
      {
        type: "AND",
        conditions: [{ type: "FALSE" }, { field: "status", type: "equals", filter: "open" }],
      },
    ]);
    const orKey = canonicalWhereKey([
      {
        type: "OR",
        conditions: [{ type: "FALSE" }, { field: "status", type: "equals", filter: "open" }],
      },
    ]);
    const emptyInKey = canonicalWhereKey(
      [{ field: "status", type: "in", filter: [] }],
      new Map([["status", { supportsText: true, materialize: <Value>(value: Value) => value }]]),
    );

    expect(falseKey).toBeDefined();
    expect(andKey).not.toBe(orKey);
    expect(falseKey).not.toBe(emptyInKey);
    expect(emptyInKey).toBeUndefined();
    expect(
      canonicalWhereKey([{ type: "OR", conditions: [{ type: "FALSE" }, { type: "FALSE" }] }]),
    ).toBe(falseKey);
    expect(
      canonicalWhereKey([
        { type: "AND", conditions: [{ type: "NOT", condition: { type: "FALSE" } }] },
      ]),
    ).toBeUndefined();
    expect(
      canonicalWhereKey([
        {
          type: "AND",
          conditions: [
            { type: "NOT", condition: { type: "FALSE" } },
            { type: "NOT", condition: { type: "FALSE" } },
          ],
        },
      ]),
    ).toBeUndefined();
    expect(
      canonicalWhereKey([
        {
          type: "OR",
          conditions: [
            { type: "NOT", condition: { type: "FALSE" } },
            { type: "NOT", condition: { type: "FALSE" } },
          ],
        },
      ]),
    ).toBeUndefined();
    expect(
      canonicalWhereKey([
        {
          type: "NOT",
          condition: { type: "NOT", condition: { type: "FALSE" } },
        },
      ]),
    ).toBe(falseKey);
    expect(
      canonicalWhereKey(
        [
          {
            type: "OR",
            conditions: [{ type: "FALSE" }, { field: "status", type: "in", filter: [] }],
          },
        ],
        new Map([["status", { supportsText: true, materialize: <Value>(value: Value) => value }]]),
      ),
    ).toBe(falseKey);
  });
});

const falseKeyForTest = (): string | undefined => canonicalWhereKey([{ type: "FALSE" }]);
