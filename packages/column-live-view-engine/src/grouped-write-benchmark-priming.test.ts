import { describe, expect, it } from "@effect/vitest";
import {
  groupedWriteBenchmarkGarbageCollector,
  groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv,
  groupedWritePrimingAppendCase,
  groupedWritePrimingDeleteCase,
  primeGroupedWriteBenchmark,
  settleAndCollectGroupedWriteBenchmarkMemoryCheckpoint,
} from "./grouped-write-benchmark-priming";

describe("grouped write benchmark priming", () => {
  it("requires an exposed collector only when explicit collection is enabled", () => {
    const collectGarbage = () => undefined;

    expect(groupedWriteBenchmarkGarbageCollector({ collectGarbage, explicitGc: true })).toBe(
      collectGarbage,
    );
    expect(
      groupedWriteBenchmarkGarbageCollector({ collectGarbage: undefined, explicitGc: false }),
    ).toBeUndefined();
    expect(() =>
      groupedWriteBenchmarkGarbageCollector({ collectGarbage: undefined, explicitGc: true }),
    ).toThrow(
      /^Grouped write explicit GC requires Node to start with NODE_OPTIONS=--expose-gc\.$/u,
    );
  });

  it("records a fixed endpoint after ordered post-GC event-loop turns", async () => {
    const calls: Array<string> = [];
    const cleanupLedger = {
      activeSubscriptions: 0,
      activeViews: 0,
      pendingMutationBatches: 0,
      queuedEvents: 0,
    };

    const checkpoint = await settleAndCollectGroupedWriteBenchmarkMemoryCheckpoint({
      capture: () => {
        const sample = `sample-${calls.length}`;
        calls.push(`capture:${sample}`);
        return sample;
      },
      cleanupLedger,
      collectGarbage: () => {
        calls.push("collect");
      },
      postGcEventLoopTurns: 8,
      settle: async () => {
        calls.push("settle");
      },
    });

    expect({ calls, checkpoint }).toStrictEqual({
      calls: [
        "settle",
        "collect",
        "capture:sample-2",
        "settle",
        "capture:sample-4",
        "settle",
        "capture:sample-6",
        "settle",
        "capture:sample-8",
        "settle",
        "capture:sample-10",
        "settle",
        "capture:sample-12",
        "settle",
        "capture:sample-14",
        "settle",
        "capture:sample-16",
        "settle",
        "capture:sample-18",
      ],
      checkpoint: {
        endpoint: "sample-18",
        samples: [
          { cleanupLedger, eventLoopTurn: 0, memory: "sample-2" },
          { cleanupLedger, eventLoopTurn: 1, memory: "sample-4" },
          { cleanupLedger, eventLoopTurn: 2, memory: "sample-6" },
          { cleanupLedger, eventLoopTurn: 3, memory: "sample-8" },
          { cleanupLedger, eventLoopTurn: 4, memory: "sample-10" },
          { cleanupLedger, eventLoopTurn: 5, memory: "sample-12" },
          { cleanupLedger, eventLoopTurn: 6, memory: "sample-14" },
          { cleanupLedger, eventLoopTurn: 7, memory: "sample-16" },
          { cleanupLedger, eventLoopTurn: 8, memory: "sample-18" },
        ],
      },
    });
  });

  it("rejects a non-zero cleanup ledger before settling or sampling", async () => {
    const calls: Array<string> = [];

    await expect(
      settleAndCollectGroupedWriteBenchmarkMemoryCheckpoint({
        capture: () => {
          calls.push("capture");
          return "memory";
        },
        cleanupLedger: {
          activeSubscriptions: 1,
          activeViews: 0,
          pendingMutationBatches: 0,
          queuedEvents: 0,
        },
        collectGarbage: () => {
          calls.push("collect");
        },
        postGcEventLoopTurns: 8,
        settle: async () => {
          calls.push("settle");
        },
      }),
    ).rejects.toThrow(
      /^Grouped write benchmark cleanup ledger must be zero before memory sampling\.$/u,
    );
    expect(calls).toStrictEqual([]);
  });

  it("requires a bounded positive post-GC event-loop turn count with explicit GC", () => {
    expect(groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv(undefined, false)).toBe(0);
    expect(groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv("8", true)).toBe(8);
    expect(() => groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv(undefined, true)).toThrow(
      /^Grouped write explicit GC requires VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS\.$/u,
    );
    expect(() => groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv("8", false)).toThrow(
      /^VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS requires explicit GC\.$/u,
    );
    expect(() => groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv("7", true)).toThrow(
      /^VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS must be 8\.$/u,
    );
    expect(() => groupedWriteBenchmarkPostGcEventLoopTurnsFromEnv("1.5", true)).toThrow(
      /^VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS must be 8\.$/u,
    );
  });

  it("runs append, delta drain, exact-row deletion, delta drain, and cardinality proof in order", async () => {
    const calls: Array<string> = [];
    let rowCount = 5_000_000;

    const result = await primeGroupedWriteBenchmark({
      expectedRowCount: 5_000_000,
      operations: {
        appendBatch: async () => {
          calls.push("append:prime-1,prime-2");
          rowCount += 2;
          return ["prime-1", "prime-2"];
        },
        deleteRow: async (key) => {
          calls.push(`delete:${key}`);
          rowCount -= 1;
        },
        drainDelta: async (caseName) => {
          calls.push(`drain:${caseName}`);
        },
        readRowCount: async () => {
          calls.push("read-row-count");
          return rowCount;
        },
      },
    });

    expect({ calls, result, rowCount }).toStrictEqual({
      calls: [
        "append:prime-1,prime-2",
        `drain:${groupedWritePrimingAppendCase}`,
        "delete:prime-1",
        `drain:${groupedWritePrimingDeleteCase}`,
        "delete:prime-2",
        `drain:${groupedWritePrimingDeleteCase}`,
        "read-row-count",
      ],
      result: {
        appendedRowCount: 2,
        deltaVersionCount: 3,
        restoredRowCount: 5_000_000,
      },
      rowCount: 5_000_000,
    });
  });

  it("rejects an empty priming append before draining or reading", async () => {
    const calls: Array<string> = [];

    await expect(
      primeGroupedWriteBenchmark({
        expectedRowCount: 5_000_000,
        operations: {
          appendBatch: async () => {
            calls.push("append");
            return [];
          },
          deleteRow: async (key) => {
            calls.push(`delete:${key}`);
          },
          drainDelta: async (caseName) => {
            calls.push(`drain:${caseName}`);
          },
          readRowCount: async () => {
            calls.push("read-row-count");
            return 5_000_000;
          },
        },
      }),
    ).rejects.toThrow(/^Grouped write benchmark priming append must add at least one row\.$/u);
    expect(calls).toStrictEqual(["append"]);
  });

  it("rejects priming that does not restore the original row cardinality", async () => {
    const calls: Array<string> = [];

    await expect(
      primeGroupedWriteBenchmark({
        expectedRowCount: 5_000_000,
        operations: {
          appendBatch: async () => {
            calls.push("append:prime-1");
            return ["prime-1"];
          },
          deleteRow: async (key) => {
            calls.push(`delete:${key}`);
          },
          drainDelta: async (caseName) => {
            calls.push(`drain:${caseName}`);
          },
          readRowCount: async () => {
            calls.push("read-row-count");
            return 5_000_001;
          },
        },
      }),
    ).rejects.toThrow(
      /^Grouped write benchmark priming must restore 5000000 rows but found 5000001\.$/u,
    );
    expect(calls).toStrictEqual([
      "append:prime-1",
      `drain:${groupedWritePrimingAppendCase}`,
      "delete:prime-1",
      `drain:${groupedWritePrimingDeleteCase}`,
      "read-row-count",
    ]);
  });
});
