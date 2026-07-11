import { describe, expect, it } from "@effect/vitest";
import { fromStringUnsafe } from "effect/BigDecimal";
import { stableQueryValueString } from "./raw-query-compiler";
import {
  cloneRecord,
  cloneRow,
  fieldValue,
  rowsEqual,
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
    expect(scalarEqualityKey(-0)).toBe("number:-0");
    expect(scalarEqualityKey(Number.NaN)).toBe("number:NaN");
    expect(scalarEqualityKey(Number.POSITIVE_INFINITY)).toBe("number:Infinity");
    expect(scalarEqualityKey(fromStringUnsafe("1.0"))).toBe("bigDecimal:1");
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

  it("treats rows with different selected column counts as different", () => {
    expect(rowsEqual({ id: "1" }, { id: "1", note: "new" })).toBe(false);
  });

  it("treats identical row references as equal without structural comparison", () => {
    let getterReads = 0;
    const row = {
      id: "1",
      get status() {
        getterReads += 1;
        return "open";
      },
    };

    expect(rowsEqual(row, row)).toBe(true);
    expect(getterReads).toBe(0);
  });

  it("does not structurally compare map and set values on row hot paths", () => {
    expect(valuesEqual(new Map([["venue", "xnys"]]), new Map([["venue", "xnys"]]))).toBe(false);
    expect(valuesEqual(new Set(["xnys"]), new Set(["xnys"]))).toBe(false);
    expect(
      rowsEqual(
        { id: "1", payload: new Map([["venue", "xnys"]]) },
        { id: "1", payload: new Map([["venue", "xnys"]]) },
      ),
    ).toBe(false);
  });

  it("ignores inherited row properties", () => {
    const inheritedRecord = Object.create({ inherited: "hidden" });
    inheritedRecord["id"] = "1";
    inheritedRecord["status"] = "open";

    const inheritedRow = Object.create({ inherited: "hidden" });
    inheritedRow["id"] = "1";
    inheritedRow["status"] = "open";

    expect(cloneRecord(inheritedRecord)).toStrictEqual({ id: "1", status: "open" });
    expect(cloneRow(inheritedRow)).toStrictEqual({ id: "1", status: "open" });
    expect(fieldValue(inheritedRow, "inherited")).toBeUndefined();
    expect(trustedFieldValue(inheritedRow, "inherited")).toBe("hidden");
    expect(rowsEqual(inheritedRow, { id: "1", status: "open" })).toBe(true);
  });

  it("preserves own __proto__ data fields while cloning rows and records", () => {
    const dangerousRecord = { id: "1" };
    Object.defineProperty(dangerousRecord, "__proto__", {
      configurable: true,
      enumerable: true,
      value: "visible-record-data",
      writable: true,
    });

    const dangerousRow = { id: "1" };
    Object.defineProperty(dangerousRow, "__proto__", {
      configurable: true,
      enumerable: true,
      value: "visible-row-data",
      writable: true,
    });

    const clonedRecord = cloneRecord(dangerousRecord);
    const clonedRow = cloneRow(dangerousRow);

    expect(clonedRecord).toStrictEqual(dangerousRecord);
    expect(clonedRow).toStrictEqual(dangerousRow);
    expect(Object.hasOwn(clonedRecord, "__proto__")).toBe(true);
    expect(Object.hasOwn(clonedRow, "__proto__")).toBe(true);
  });
});
