import { describe, expect, it } from "@effect/vitest";
import {
  captureGroupedWriteBenchmarkAfterCleanup,
  groupedWritePrimingAppendCase,
  groupedWritePrimingDeleteCase,
  prepareGroupedWriteBenchmarkSetup,
  primeGroupedWriteBenchmark,
} from "./grouped-write-benchmark-priming";

describe("grouped write benchmark priming", () => {
  it("settles after mutation key/index preparation, then primes before capturing memory", async () => {
    const calls: Array<string> = [];

    await prepareGroupedWriteBenchmarkSetup({
      captureMemoryBaseline: () => {
        calls.push("capture-memory-after-setup");
      },
      prepareMutationKeyIndexes: () => {
        calls.push("prepare-large-mutation-key-indexes");
      },
      prime: async () => {
        calls.push("prime-grouped-write");
      },
      settleMeasurementRuntime: () => {
        calls.push("settle-measurement-runtime");
      },
    });

    expect(calls).toStrictEqual([
      "prepare-large-mutation-key-indexes",
      "settle-measurement-runtime",
      "prime-grouped-write",
      "capture-memory-after-setup",
    ]);
  });

  it("prepares measured state and memory without priming when priming is disabled", async () => {
    const calls: Array<string> = [];

    await prepareGroupedWriteBenchmarkSetup({
      captureMemoryBaseline: () => {
        calls.push("capture-memory-after-setup");
      },
      prepareMutationKeyIndexes: () => {
        calls.push("prepare-large-mutation-key-indexes");
      },
      prime: undefined,
      settleMeasurementRuntime: undefined,
    });

    expect(calls).toStrictEqual([
      "prepare-large-mutation-key-indexes",
      "capture-memory-after-setup",
    ]);
  });

  it("captures cleanup memory only after releasing references and settling", async () => {
    const calls: Array<string> = [];

    const memory = await captureGroupedWriteBenchmarkAfterCleanup({
      captureMemoryAfterBenchmark: () => {
        calls.push("capture-memory-after-cleanup");
        return { rssBytes: 128 };
      },
      releaseBenchmarkReferences: async () => {
        calls.push("release-benchmark-references");
      },
      settleCleanupRuntime: () => {
        calls.push("settle-cleanup-runtime");
      },
    });

    expect({ calls, memory }).toStrictEqual({
      calls: [
        "release-benchmark-references",
        "settle-cleanup-runtime",
        "capture-memory-after-cleanup",
      ],
      memory: { rssBytes: 128 },
    });
  });

  it("captures cleanup memory after release when cleanup settling is disabled", async () => {
    const calls: Array<string> = [];

    const memory = await captureGroupedWriteBenchmarkAfterCleanup({
      captureMemoryAfterBenchmark: () => {
        calls.push("capture-memory-after-cleanup");
        return { rssBytes: 256 };
      },
      releaseBenchmarkReferences: async () => {
        calls.push("release-benchmark-references");
      },
      settleCleanupRuntime: undefined,
    });

    expect({ calls, memory }).toStrictEqual({
      calls: ["release-benchmark-references", "capture-memory-after-cleanup"],
      memory: { rssBytes: 256 },
    });
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
    ).rejects.toThrow("Grouped write benchmark priming append must add at least one row.");
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
      "Grouped write benchmark priming must restore 5000000 rows but found 5000001.",
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
