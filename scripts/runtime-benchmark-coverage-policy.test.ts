import { describe, expect, it } from "@effect/vitest";
import runtimeConfig from "../packages/runtime/vite.config";

describe("runtime benchmark coverage policy", () => {
  it("does not retain transport-specific benchmark harnesses", () => {
    expect(runtimeConfig.test?.coverage?.include ?? []).not.toContain(
      "test-harness/grpc-benchmark-memory.ts",
    );
  });
});
