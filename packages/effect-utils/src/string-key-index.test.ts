import { describe, expect, it } from "@effect/vitest";
import { StringKeyIndex } from "./string-key-index";

describe("StringKeyIndex", () => {
  it("preserves exact string identity, missing values, replacement, deletion and reuse", () => {
    const index = new StringKeyIndex<number | undefined>();
    expect(index.size).toBe(0);
    expect(index.get("missing")).toBeUndefined();
    expect(index.has("missing")).toBe(false);
    expect(index.delete("missing")).toBe(false);
    index.set("", 0);
    index.set("__proto__", 1);
    index.set("undefined", undefined);
    index.set("", 2);
    expect(index.size).toBe(3);
    expect(index.get("")).toBe(2);
    expect(index.get("__proto__")).toBe(1);
    expect(index.has("undefined")).toBe(true);
    expect(index.delete("undefined")).toBe(true);
    expect(index.size).toBe(2);
    index.clear();
    expect(index.size).toBe(0);
    expect(index.has("")).toBe(false);
    index.set("again", 3);
    expect(index.get("again")).toBe(3);
    expect(index.delete("again")).toBe(true);
    expect(index.size).toBe(0);
  });

  it("retains lookup and mutation behavior across repeated splits with a common prefix", () => {
    const index = new StringKeyIndex<number>();
    const prefix = "source/topic/local:0:";
    // Unique suffixes cross real leaf capacity; the second population forces a
    // nested split. No test-only capacity setting changes production behavior.
    for (let value = 0; value < 140_000; value += 1) index.set(`${prefix}${value}`, value);
    for (let value = 0; value < 140_000; value += 1)
      expect(index.get(`${prefix}${value}`)).toBe(value);
    expect(index.size).toBe(140_000);
    expect(index.get(`${prefix}missing`)).toBeUndefined();
    expect(index.has(`${prefix}missing`)).toBe(false);
    expect(index.delete(`${prefix}missing`)).toBe(false);
    index.set(`${prefix}0`, -1);
    expect(index.size).toBe(140_000);
    expect(index.get(`${prefix}0`)).toBe(-1);
    expect(index.delete(`${prefix}100000`)).toBe(true);
    expect(index.has(`${prefix}100000`)).toBe(false);
    expect(index.get(`${prefix}100000`)).toBeUndefined();
    expect(index.size).toBe(139_999);
    index.set(`${prefix}100000`, 7);
    index.set("", 8);
    index.set("different", 9);
    expect(index.get("")).toBe(8);
    expect(index.get("different")).toBe(9);
    expect(index.size).toBe(140_002);
    index.clear();
    expect(index.size).toBe(0);
    expect(index.get(`${prefix}0`)).toBeUndefined();
  });

  it("separates UTF-16 code units, prefix keys and late-diverging keys", () => {
    const index = new StringKeyIndex<number>();
    index.set("", -1);
    for (let value = 0; value < 65_536; value += 1) index.set(String.fromCharCode(value), value);
    for (let value = 0; value < 65_536; value += 1)
      expect(index.get(String.fromCharCode(value))).toBe(value);
    expect(index.get("")).toBe(-1);
    index.set("a-long-key", 1);
    expect(index.get("a-long-key")).toBe(1);
    const late = new StringKeyIndex<number>();
    for (let value = 0; value < 65_537; value += 1)
      late.set(`same-prefix-${value.toString().padStart(8, "0")}`, value);
    expect(late.get("same-prefix-00065536")).toBe(65_536);
    late.set("same-prefix-", -1);
    expect(late.get("same-prefix-")).toBe(-1);
  });

  it("reuses emptied branches while another key remains live", () => {
    const index = new StringKeyIndex<number>();
    index.set("sentinel", -1);
    for (let value = 0; value < 140_000; value += 1) index.set(`row-${value}`, value);
    for (let value = 0; value < 140_000; value += 1)
      expect(index.delete(`row-${value}`)).toBe(true);
    expect(index.size).toBe(1);
    expect(index.get("sentinel")).toBe(-1);
    expect(index.get("row-0")).toBeUndefined();
    index.set("row-0", 3);
    expect(index.get("row-0")).toBe(3);
    expect(index.delete("sentinel")).toBe(true);
    expect(index.get("row-0")).toBe(3);
    expect(index.delete("row-0")).toBe(true);
    expect(index.size).toBe(0);
  });
});
