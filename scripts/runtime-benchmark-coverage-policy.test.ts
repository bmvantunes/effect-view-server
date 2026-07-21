import { describe, expect, it } from "@effect/vitest";
import runtimeConfig from "../packages/runtime/vite.config";

describe("runtime benchmark coverage policy", () => {
  it("covers the shared gRPC benchmark memory lifecycle", () => {
    expect(runtimeConfig.test?.coverage?.include).toContain(
      "test-harness/grpc-benchmark-memory.ts",
    );
  });
});
