import { describe, expect, it } from "@effect/vitest";
import { immutableReadonlyMap, immutableReadonlySet } from "./immutable-readonly-collection";

describe("immutable read-only collections", () => {
  it("provides the read interfaces without exposing mutable collection internals", () => {
    const set = immutableReadonlySet(["a", "b"]);
    expect(Object.isFrozen(set)).toBe(true);
    expect(set.size).toBe(2);
    expect(set.has("a")).toBe(true);
    expect([...set]).toStrictEqual(["a", "b"]);
    expect([...set.entries()]).toStrictEqual([
      ["a", "a"],
      ["b", "b"],
    ]);
    expect([...set.keys()]).toStrictEqual(["a", "b"]);
    expect([...set.values()]).toStrictEqual(["a", "b"]);
    expect([...set.union(new Set(["b", "c"]))]).toStrictEqual(["a", "b", "c"]);
    expect([...set.intersection(new Set(["b", "c"]))]).toStrictEqual(["b"]);
    expect([...set.difference(new Set(["b", "c"]))]).toStrictEqual(["a"]);
    expect([...set.symmetricDifference(new Set(["b", "c"]))]).toStrictEqual(["a", "c"]);
    expect(set.isSubsetOf(new Set(["a", "b", "c"]))).toBe(true);
    expect(set.isSupersetOf(new Set(["a"]))).toBe(true);
    expect(set.isDisjointFrom(new Set(["c"]))).toBe(true);
    const setVisits: Array<readonly [string, string, boolean]> = [];
    set.forEach((value, key, owner) => setVisits.push([value, key, owner === set]));
    expect(setVisits).toStrictEqual([
      ["a", "a", true],
      ["b", "b", true],
    ]);
    expect(() => Set.prototype.add.call(set, "c")).toThrowError(TypeError);

    const map = immutableReadonlyMap<string, number>([
      ["a", 1],
      ["b", 2],
    ]);
    expect(Object.isFrozen(map)).toBe(true);
    expect(map.size).toBe(2);
    expect(map.get("a")).toBe(1);
    expect(map.has("b")).toBe(true);
    expect([...map]).toStrictEqual([
      ["a", 1],
      ["b", 2],
    ]);
    expect([...map.entries()]).toStrictEqual([
      ["a", 1],
      ["b", 2],
    ]);
    expect([...map.keys()]).toStrictEqual(["a", "b"]);
    expect([...map.values()]).toStrictEqual([1, 2]);
    const mapVisits: Array<readonly [number, string, boolean]> = [];
    map.forEach((value, key, owner) => mapVisits.push([value, key, owner === map]));
    expect(mapVisits).toStrictEqual([
      [1, "a", true],
      [2, "b", true],
    ]);
    expect(() => Map.prototype.set.call(map, "c", 3)).toThrowError(TypeError);
  });
});
