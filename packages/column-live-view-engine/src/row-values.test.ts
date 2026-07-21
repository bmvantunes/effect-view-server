import { describe, expect, it } from "@effect/vitest";
import { fromStringUnsafe, make } from "effect/BigDecimal";
import { stableQueryValueString } from "./raw-query-compiler";
import {
  cloneUnknown,
  cloneRecord,
  fieldValue,
  scalarEqualityKey,
  trustedFieldValue,
  valuesEqual,
} from "./row-values";

describe("Row value semantics", () => {
  it("encodes scalar equality keys deterministically", () => {
    expect(scalarEqualityKey(null)).toBe("null");
    expect(scalarEqualityKey("open")).toBe("string:4:open");
    expect(scalarEqualityKey(false)).toBe("boolean:false");
    expect(scalarEqualityKey(true)).toBe("boolean:true");
    expect(scalarEqualityKey(1n)).toBe("bigint:1");
    expect(scalarEqualityKey(-0)).toBe("number:0");
    expect(scalarEqualityKey(Number.NaN)).toBe("number:NaN");
    expect(scalarEqualityKey(Number.POSITIVE_INFINITY)).toBe("number:Infinity");
    expect(scalarEqualityKey(fromStringUnsafe("1.0"))).toBe('bigDecimal:["1","0"]');
    expect(scalarEqualityKey(make(111n, Number.MIN_SAFE_INTEGER))).toBeUndefined();
    expect(scalarEqualityKey(make(111n, Number.MIN_SAFE_INTEGER + 1))).toBeUndefined();
    expect(scalarEqualityKey({ value: "open" })).toBeUndefined();
  });

  it("encodes primitive and unsupported query values deterministically", () => {
    function namedFilter() {
      return "ignored";
    }
    const anonymousFilter = () => "ignored";

    expect(JSON.parse(stableQueryValueString(null))).toStrictEqual(["null"]);
    expect(JSON.parse(stableQueryValueString(1n))).toStrictEqual(["bigint", "1"]);
    expect(JSON.parse(stableQueryValueString("x"))).toStrictEqual(["string", "x"]);
    expect(JSON.parse(stableQueryValueString(-0))).toStrictEqual(["number", "-0"]);
    expect(JSON.parse(stableQueryValueString(false))).toStrictEqual(["boolean", false]);
    expect(JSON.parse(stableQueryValueString(Symbol("filter")))).toStrictEqual([
      "unsupported",
      "symbol:filter",
    ]);
    expect(JSON.parse(stableQueryValueString(Symbol()))).toStrictEqual(["unsupported", "symbol:"]);
    expect(JSON.parse(stableQueryValueString(namedFilter))).toStrictEqual([
      "unsupported",
      "function:namedFilter",
    ]);
    expect(JSON.parse(stableQueryValueString(anonymousFilter))).toStrictEqual([
      "unsupported",
      "function:anonymousFilter",
    ]);
    expect(JSON.parse(stableQueryValueString(new Map()))).toStrictEqual(["map", []]);
    expect(
      JSON.parse(
        stableQueryValueString(
          new Map<unknown, unknown>([
            [{ id: "same" }, "b"],
            [{ id: "same" }, "a"],
          ]),
        ),
      ),
    ).toStrictEqual([
      "map",
      [
        [
          ["object", [["id", ["string", "same"]]]],
          ["string", "a"],
        ],
        [
          ["object", [["id", ["string", "same"]]]],
          ["string", "b"],
        ],
      ],
    ]);
    expect(
      JSON.parse(
        stableQueryValueString(
          new Map<unknown, unknown>([
            [{ id: "b" }, "same"],
            [{ id: "a" }, "same"],
          ]),
        ),
      ),
    ).toStrictEqual([
      "map",
      [
        [
          ["object", [["id", ["string", "a"]]]],
          ["string", "same"],
        ],
        [
          ["object", [["id", ["string", "b"]]]],
          ["string", "same"],
        ],
      ],
    ]);
    expect(JSON.parse(stableQueryValueString(new Set(["b", "a"])))).toStrictEqual([
      "set",
      [
        ["string", "a"],
        ["string", "b"],
      ],
    ]);
    class CustomQueryValue {}
    expect(JSON.parse(stableQueryValueString(new CustomQueryValue()))).toStrictEqual([
      "nonPlainObject",
      "[object Object]",
    ]);
    expect(JSON.parse(stableQueryValueString(undefined))).toStrictEqual(["undefined"]);

    const cyclicArray: Array<unknown> = [];
    cyclicArray.push(cyclicArray);
    expect(JSON.parse(stableQueryValueString(cyclicArray))).toStrictEqual(["array", [["cycle"]]]);

    const cyclicMap = new Map<unknown, unknown>();
    cyclicMap.set("self", cyclicMap);
    expect(JSON.parse(stableQueryValueString(cyclicMap))).toStrictEqual([
      "map",
      [[["string", "self"], ["cycle"]]],
    ]);

    const cyclicSet = new Set<unknown>();
    cyclicSet.add(cyclicSet);
    expect(JSON.parse(stableQueryValueString(cyclicSet))).toStrictEqual(["set", [["cycle"]]]);

    type CyclicObject = {
      self?: CyclicObject;
    };
    const cyclicObject: CyclicObject = {};
    cyclicObject.self = cyclicObject;
    expect(JSON.parse(stableQueryValueString(cyclicObject))).toStrictEqual([
      "object",
      [["self", ["cycle"]]],
    ]);
  });

  it("uses injective stable keys for structured query values", () => {
    const left = { a: "b", c: "d" };
    const right = { 'a:string:"b",c': "d" };

    expect(stableQueryValueString(left)).not.toBe(stableQueryValueString(right));
  });

  it("does not structurally compare map and set values", () => {
    expect(valuesEqual(new Map([["venue", "xnys"]]), new Map([["venue", "xnys"]]))).toBe(false);
    expect(valuesEqual(new Set(["xnys"]), new Set(["xnys"]))).toBe(false);
    expect(cloneRecord({ payload: new Map([["venue", "xnys"]]) })).toStrictEqual({
      payload: new Map([["venue", "xnys"]]),
    });
  });

  it("clones arrays and plain records while preserving BigDecimal identity", () => {
    const amount = fromStringUnsafe("1.25");
    const source = [{ nested: { amount } }];
    const cloned = cloneUnknown(source);

    expect(cloned).toStrictEqual(source);
    expect(cloned === source).toBe(false);
    expect(cloneUnknown(amount)).toBe(amount);
  });

  it("compares array and plain-record values structurally", () => {
    expect(valuesEqual(make(1n, Number.MAX_SAFE_INTEGER), make(1n, 0))).toBe(false);
    expect(valuesEqual([1, { nested: "same" }], [1, { nested: "same" }])).toBe(true);
    expect(valuesEqual([1], [1, 2])).toBe(false);
    expect(valuesEqual([1], [2])).toBe(false);

    expect(valuesEqual({ left: 1, right: 2 }, { right: 2, left: 1 })).toBe(true);
    expect(valuesEqual({ missing: true }, {})).toBe(false);
    expect(valuesEqual({ changed: 1 }, { changed: 2 })).toBe(false);
    expect(valuesEqual({}, { extra: true })).toBe(false);
  });

  it("ignores inherited row properties", () => {
    const inheritedRecord = Object.create({ inherited: "hidden" });
    inheritedRecord["id"] = "1";
    inheritedRecord["status"] = "open";

    expect(cloneRecord(inheritedRecord)).toStrictEqual({ id: "1", status: "open" });
    expect(fieldValue(inheritedRecord, "inherited")).toBeUndefined();
    expect(trustedFieldValue(inheritedRecord, "inherited")).toBe("hidden");
  });

  it("preserves own __proto__ data fields while cloning records", () => {
    const dangerousRecord = { id: "1" };
    Object.defineProperty(dangerousRecord, "__proto__", {
      configurable: true,
      enumerable: true,
      value: "visible-record-data",
      writable: true,
    });

    const clonedRecord = cloneRecord(dangerousRecord);

    expect(clonedRecord).toStrictEqual(dangerousRecord);
    expect(Object.hasOwn(clonedRecord, "__proto__")).toBe(true);
  });
});
