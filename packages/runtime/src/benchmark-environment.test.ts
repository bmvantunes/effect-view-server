import { describe, expect, it } from "@effect/vitest";

import { booleanFromBenchmarkEnvironment } from "./benchmark-environment";

describe("benchmark environment", () => {
  it("decodes boolean values, blanks, and defaults", () => {
    expect(booleanFromBenchmarkEnvironment(undefined, "BENCHMARK_FLAG", true)).toBe(true);
    expect(booleanFromBenchmarkEnvironment("  ", "BENCHMARK_FLAG", false)).toBe(false);
    expect(booleanFromBenchmarkEnvironment(" true ", "BENCHMARK_FLAG", false)).toBe(true);
    expect(booleanFromBenchmarkEnvironment("false", "BENCHMARK_FLAG", true)).toBe(false);
  });

  it("rejects malformed boolean values", () => {
    expect(() => booleanFromBenchmarkEnvironment("yes", "BENCHMARK_FLAG", false)).toThrow(
      "BENCHMARK_FLAG must be true or false.",
    );
  });
});
