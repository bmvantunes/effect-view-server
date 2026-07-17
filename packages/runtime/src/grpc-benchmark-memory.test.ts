import { describe, expect, it } from "@effect/vitest";

import {
  grpcBenchmarkExplicitGcFromEnv,
  makeGrpcBenchmarkMemoryLifecycle,
} from "../test-harness/grpc-benchmark-memory";

describe("gRPC benchmark memory lifecycle", () => {
  it("settles and collects before both lifecycle checkpoints", async () => {
    const events: Array<string> = [];
    let snapshot = 0;
    const lifecycle = makeGrpcBenchmarkMemoryLifecycle({
      capture: () => {
        events.push("capture");
        snapshot += 1;
        return snapshot;
      },
      collectGarbage: () => events.push("gc"),
      explicitGc: true,
      settle: async () => {
        events.push("settle");
      },
    });

    await lifecycle.captureBefore();
    expect(await lifecycle.captureAfterCleanup()).toStrictEqual({
      afterCleanup: 2,
      before: 1,
    });
    expect(events).toStrictEqual(["settle", "gc", "capture", "settle", "gc", "capture"]);
  });

  it("preserves lifecycle ordering without explicit collection", async () => {
    const events: Array<string> = [];
    const lifecycle = makeGrpcBenchmarkMemoryLifecycle({
      capture: () => {
        events.push("capture");
        return events.length;
      },
      collectGarbage: undefined,
      explicitGc: false,
      settle: async () => {
        events.push("settle");
      },
    });

    await lifecycle.captureBefore();
    await lifecycle.captureAfterCleanup();
    expect(events).toStrictEqual(["settle", "capture", "settle", "capture"]);
  });

  it("requires exposed GC when explicit collection is enabled", () => {
    expect(() =>
      makeGrpcBenchmarkMemoryLifecycle({
        capture: () => 1,
        collectGarbage: undefined,
        explicitGc: true,
        settle: async () => undefined,
      }),
    ).toThrow("gRPC benchmark explicit GC requires Node to start with NODE_OPTIONS=--expose-gc.");
  });

  it("rejects duplicate and out-of-order checkpoints", async () => {
    const unfinished = makeGrpcBenchmarkMemoryLifecycle({
      capture: () => 1,
      collectGarbage: undefined,
      explicitGc: false,
      settle: async () => undefined,
    });
    await expect(unfinished.captureAfterCleanup()).rejects.toThrow(
      "gRPC benchmark memory cannot finish before its initial checkpoint.",
    );

    const finished = makeGrpcBenchmarkMemoryLifecycle({
      capture: () => 1,
      collectGarbage: undefined,
      explicitGc: false,
      settle: async () => undefined,
    });
    await finished.captureBefore();
    await expect(finished.captureBefore()).rejects.toThrow(
      "gRPC benchmark initial memory was already recorded.",
    );
    await finished.captureAfterCleanup();
    await expect(finished.captureAfterCleanup()).rejects.toThrow(
      "gRPC benchmark memory recording already finished.",
    );
  });

  it("accepts only the canonical explicit-GC environment values", () => {
    expect(grpcBenchmarkExplicitGcFromEnv(undefined)).toBe(false);
    expect(grpcBenchmarkExplicitGcFromEnv("0")).toBe(false);
    expect(grpcBenchmarkExplicitGcFromEnv("1")).toBe(true);
    expect(() => grpcBenchmarkExplicitGcFromEnv("true")).toThrow(
      "VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC must be 0 or 1.",
    );
  });
});
