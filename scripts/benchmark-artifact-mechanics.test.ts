import { describe, expect, it } from "@effect/vitest";

import {
  arrayValue,
  compareExact,
  compareExactJson,
  compareLatency,
  compareThroughput,
  exactObjectValue,
  finiteNumber,
  mapByUniqueKey,
  nonEmptyArrayValue,
  nonNegativeFiniteNumber,
  nonNegativeInteger,
  objectValue,
  positiveFiniteNumber,
  positiveInteger,
  pushRegression,
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";

describe("benchmark artifact mechanics", () => {
  it("owns canonical artifact decoding and unique identity diagnostics", () => {
    expect(finiteNumber(1, "artifact.number")).toBe(1);
    expect(() => finiteNumber("1", "artifact.number")).toThrow(
      "Benchmark artifact field artifact.number must be a finite number.",
    );
    expect(() => finiteNumber(Number.POSITIVE_INFINITY, "artifact.number")).toThrow(
      "Benchmark artifact field artifact.number must be a finite number.",
    );

    expect(stringValue("value", "artifact.string")).toBe("value");
    expect(() => stringValue(1, "artifact.string")).toThrow(
      "Benchmark artifact field artifact.string must be a non-empty string.",
    );
    expect(() => stringValue("", "artifact.string")).toThrow(
      "Benchmark artifact field artifact.string must be a non-empty string.",
    );

    expect(objectValue({ value: 1 }, "artifact.object")).toStrictEqual({ value: 1 });
    expect(() => objectValue(null, "artifact.object")).toThrow(
      "Benchmark artifact field artifact.object must be an object.",
    );
    expect(() => objectValue([], "artifact.object")).toThrow(
      "Benchmark artifact field artifact.object must be an object.",
    );
    expect(exactObjectValue({ value: 1 }, "artifact.exact", ["value"])).toStrictEqual({
      value: 1,
    });
    expect(() => exactObjectValue({ extra: 2, value: 1 }, "artifact.exact", ["value"])).toThrow(
      "Benchmark artifact field artifact.exact must contain exactly these keys: value.",
    );

    expect(arrayValue([1], "artifact.array")).toStrictEqual([1]);
    expect(() => arrayValue({}, "artifact.array")).toThrow(
      "Benchmark artifact field artifact.array must be an array.",
    );
    expect(nonEmptyArrayValue([1], "artifact.nonEmptyArray")).toStrictEqual([1]);
    expect(() => nonEmptyArrayValue([], "artifact.nonEmptyArray")).toThrow(
      "Benchmark artifact field artifact.nonEmptyArray must be a non-empty array.",
    );

    expect(positiveFiniteNumber(1, "artifact.positive")).toBe(1);
    expect(() => positiveFiniteNumber(0, "artifact.positive")).toThrow(
      "Benchmark artifact field artifact.positive must be a positive finite number.",
    );
    expect(nonNegativeFiniteNumber(0, "artifact.nonNegative")).toBe(0);
    expect(() => nonNegativeFiniteNumber(-1, "artifact.nonNegative")).toThrow(
      "Benchmark artifact field artifact.nonNegative must be a non-negative finite number.",
    );
    expect(positiveInteger(1, "artifact.positiveInteger")).toBe(1);
    expect(() => positiveInteger(0, "artifact.positiveInteger")).toThrow(
      "Benchmark artifact field artifact.positiveInteger must be a positive integer.",
    );
    expect(() => positiveInteger(1.5, "artifact.positiveInteger")).toThrow(
      "Benchmark artifact field artifact.positiveInteger must be a positive integer.",
    );
    expect(nonNegativeInteger(0, "artifact.nonNegativeInteger")).toBe(0);
    expect(() => nonNegativeInteger(-1, "artifact.nonNegativeInteger")).toThrow(
      "Benchmark artifact field artifact.nonNegativeInteger must be a non-negative integer.",
    );
    expect(() => nonNegativeInteger(1.5, "artifact.nonNegativeInteger")).toThrow(
      "Benchmark artifact field artifact.nonNegativeInteger must be a non-negative integer.",
    );

    expect(
      Array.from(
        mapByUniqueKey(
          [
            { name: "a", value: 1 },
            { name: "b", value: 2 },
          ],
          (value) => value.name,
          "artifact.cases",
          "case",
        ).entries(),
      ),
    ).toStrictEqual([
      ["a", { name: "a", value: 1 }],
      ["b", { name: "b", value: 2 }],
    ]);
    expect(() =>
      mapByUniqueKey(
        [{ name: "a" }, { name: "a" }],
        (value) => value.name,
        "artifact.cases",
        "case",
      ),
    ).toThrow("Benchmark artifact field artifact.cases contains duplicate case: a.");
  });

  it("owns canonical exact, latency, and throughput regression mechanics", () => {
    const regressions: Array<string> = [];
    pushRegression(regressions, "manual regression");
    compareExact(regressions, "task a", "count", 1, 1);
    compareExact(regressions, "task a", "count", 1, 2);
    compareExactJson(regressions, "task a", "state", { value: 1 }, { value: 1 });
    compareExactJson(regressions, "task a", "state", { value: 1 }, { value: 2 });
    compareLatency(
      regressions,
      "task a",
      "case a",
      "meanMs",
      { maxAbsoluteDeltaMs: 3, maxRatio: 2 },
      2,
      5,
    );
    compareLatency(
      regressions,
      "task a",
      "case a",
      "meanMs",
      { maxAbsoluteDeltaMs: 3, maxRatio: 2 },
      2,
      6,
    );
    compareThroughput(
      regressions,
      "task a",
      "case a",
      "rowsPerSecond",
      { minRatio: 0.5 },
      0,
      0,
    );
    compareThroughput(
      regressions,
      "task a",
      "case a",
      "rowsPerSecond",
      { minRatio: 0.5 },
      0,
      1,
    );
    compareThroughput(
      regressions,
      "task a",
      "case a",
      "rowsPerSecond",
      { minRatio: 0.5 },
      100,
      50,
    );
    compareThroughput(
      regressions,
      "task a",
      "case a",
      "rowsPerSecond",
      { minRatio: 0.5 },
      100,
      49,
    );

    expect(regressions).toStrictEqual([
      "manual regression",
      "task a: count changed from 1 to 2.",
      'task a: state changed from {"value":1} to {"value":2}.',
      "task a / case a: meanMs regressed from 2.000ms to 6.000ms; allowed <= 5.000ms.",
      "task a: case a rowsPerSecond changed from 0 to 1.",
      "task a / case a: rowsPerSecond throughput regressed from 100.000 rows/sec to 49.000 rows/sec; allowed >= 50.000 rows/sec.",
    ]);
  });
});
