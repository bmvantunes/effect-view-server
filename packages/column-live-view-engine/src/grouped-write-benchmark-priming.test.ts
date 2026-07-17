import { describe, expect, it } from "@effect/vitest";
import {
  groupedWriteBenchmarkGarbageCollector,
  groupedWritePrimingAppendCase,
  groupedWritePrimingDeleteCase,
  primeGroupedWriteBenchmark,
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
    ).toThrow("Grouped write explicit GC requires Node to start with NODE_OPTIONS=--expose-gc.");
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
