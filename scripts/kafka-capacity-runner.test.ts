import { publishCapacityReport } from "../packages/kafka/benchmarks/capacity-report.ts";
import { describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import { mkdtempSync, existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync, readlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseCapacityArguments, runCapacityBenchmark } from "./kafka-capacity-runner.mjs";

function fixture(failure?: string) {
  const output = mkdtempSync(join(tmpdir(), "kafka-capacity-test-"));
  const lockPath = join(output, "run.lock");
  const processEvents = new EventEmitter();
  const calls: string[] = [];
  const workerArguments: string[][] = [];
  const kills: string[] = [];
  const options = parseCapacityArguments(["--rows=20", `--output=${output}`]);
  const spawnProcess = (command: string, args: string[]) => {
    const mode = command === "vp" ? "build" : command === "docker" ? "broker" : args[2]!;
    calls.push(mode);
    if (command === process.execPath) workerArguments.push(args);
    const child = Object.assign(new EventEmitter(), {
      kill(signal: string) { kills.push(signal); queueMicrotask(() => child.emit("close", null, signal)); },
    });
    queueMicrotask(() => {
      if (failure === "signal") { processEvents.emit("SIGINT"); return; }
      if (failure === "timeout") return;
      if (mode === failure) { child.emit("close", 1, null); return; }
      if (failure === "error") { child.emit("error", new Error("spawn failed")); return; }
      if (["1", "3", "10"].includes(mode) && failure !== "missing") writeFileSync(join(output, `scenario-${mode}.json`), "{}");
      child.emit("close", 0, null);
      if (failure === "between") processEvents.emit("SIGTERM");
    });
    return child;
  };
  return { output, options, calls, kills, workerArguments, dependencies: { availableMemory: () => 2 ** 41, spawnProcess, processEvents, lockPath, schedule: setTimeout, cancel: clearTimeout } };
}

describe("Kafka capacity orchestration", () => {
  it("validates arguments and defaults", () => {
    expect(parseCapacityArguments([]).rows).toBe(50_000_000);
    expect(parseCapacityArguments([]).heapMiB).toBe(978611);
    expect(parseCapacityArguments(["--heap-mib=1048576"]).heapMiB).toBe(1048576);
    expect(() => parseCapacityArguments(["--heap-mib=2048"])).toThrow("heap-mib");
    expect(() => parseCapacityArguments(["--heap-mib=no"])).toThrow("heap-mib");
    expect(parseCapacityArguments(["--help"]).help).toBe(true);
    expect(() => parseCapacityArguments(["--rows=0"])).toThrow();
    expect(() => parseCapacityArguments(["--rows=50000001"])).toThrow();
    expect(() => parseCapacityArguments(["--rows=no"])).toThrow();
    expect(() => parseCapacityArguments(["--timeout-seconds=0"])).toThrow();
    expect(() => parseCapacityArguments(["--timeout-seconds=2000001"])).toThrow();
    expect(() => parseCapacityArguments(["--timeout-seconds=no"])).toThrow();
  });

  it("runs serial scenarios and releases its lock", async () => {
    const run = fixture();
    await runCapacityBenchmark(run.options, run.dependencies);
    expect(run.calls).toStrictEqual(["build", "broker", "seed", "1", "3", "10", "report"]);
    expect(run.workerArguments.map((args) => args[0])).toStrictEqual(Array(5).fill("--max-old-space-size=2049"));
    expect(existsSync(run.dependencies.lockPath)).toBe(false);
    expect(run.dependencies.processEvents.listenerCount("SIGINT")).toBe(0);
  });

  it("compares without replacing the baseline and can skip build", async () => {
    const run = fixture();
    const baseline = JSON.stringify({
      corpus: { version: 1, seed: 494, topics: 10, rowsPerTopic: 20, minBytes: 200, maxBytes: 1024, protobuf: "google.protobuf.StringValue", partitionsPerTopic: 1 },
      node: "v26", platform: "linux", arch: "x64", cpu: "test", logicalCpus: 8, heapLimitBytes: 2048 * 1024 ** 2, totalMemoryBytes: 1024, revision: "test",
      scenarios: [1, 3, 10].map((topics) => ({ topics, rows: topics * 20, seconds: 1, messagesPerSecond: topics * 20, cpuSeconds: 1, averageCpuCores: 1, peakRssBytes: 1024 })),
    });
    mkdirSync(join(run.output, "current"));
    writeFileSync(join(run.output, "current", "benchmark-baseline.json"), baseline);
    await runCapacityBenchmark({ ...run.options, compare: true, "skip-build": true }, run.dependencies);
    expect(run.calls).toStrictEqual(["broker", "seed", "1", "3", "10", "compare"]);
    expect(readFileSync(join(run.output, "current", "benchmark-baseline.json"), "utf8")).toBe(baseline);
    await expect(runCapacityBenchmark({ ...run.options, rows: 21, compare: true }, run.dependencies)).rejects.toThrow("same corpus");
  });

  it.each(["{}", '{"corpus":{"version":2,"rowsPerTopic":20}}', '{"corpus":{"version":1,"rowsPerTopic":21}}', "not-json"])("rejects invalid comparison before spawning: %s", async (baseline) => {
    const run = fixture();
    mkdirSync(join(run.output, "current"));
    writeFileSync(join(run.output, "current", "benchmark-baseline.json"), baseline);
    await expect(runCapacityBenchmark({ ...run.options, compare: true }, run.dependencies)).rejects.toThrow();
    expect(run.calls).toStrictEqual([]);
  });

  it("rejects insufficient memory before building or seeding", async () => {
    const run = fixture();
    await expect(runCapacityBenchmark(run.options, { ...run.dependencies, availableMemory: () => 1024 })).rejects.toThrow("available memory");
    expect(run.calls).toStrictEqual([]);
  });

  it("does not remove somebody else's lock", async () => {
    const run = fixture();
    writeFileSync(run.dependencies.lockPath, "owner");
    await expect(runCapacityBenchmark(run.options, run.dependencies)).rejects.toThrow();
    expect(run.calls).toStrictEqual([]);
    expect(readFileSync(run.dependencies.lockPath, "utf8")).toBe("owner");
  });

  it.each(["build", "broker", "seed", "1", "3", "10", "missing", "error", "between"])("does not publish after %s failure", async (failure) => {
    const run = fixture(failure);
    mkdirSync(join(run.output, "current"));
    writeFileSync(join(run.output, "current", "benchmark-baseline.json"), "prior-baseline");
    await expect(runCapacityBenchmark(run.options, run.dependencies)).rejects.toThrow();
    expect(run.calls.includes("report")).toBe(false);
    expect(existsSync(run.dependencies.lockPath)).toBe(false);
    expect(readFileSync(join(run.output, "current", "benchmark-baseline.json"), "utf8")).toBe("prior-baseline");
  });

  it("interrupts the active child and releases the lock", async () => {
    const run = fixture("signal");
    await expect(runCapacityBenchmark(run.options, run.dependencies)).rejects.toThrow("signal=SIGTERM");
    expect(run.kills).toStrictEqual(["SIGTERM"]);
    expect(existsSync(run.dependencies.lockPath)).toBe(false);
  });

  it("kills a timed-out child without publishing", async () => {
    const run = fixture("timeout");
    await expect(runCapacityBenchmark(run.options, { ...run.dependencies,
      schedule: (callback: () => void) => setTimeout(callback, 1),
    })).rejects.toThrow("timedOut=true");
    expect(run.kills).toStrictEqual(["SIGKILL"]);
    expect(run.calls).toStrictEqual(["build"]);
    expect(existsSync(run.dependencies.lockPath)).toBe(false);
  });
});


describe("Kafka capacity report publication", () => {
  it("atomically switches complete report generations and preserves comparison baseline bytes", () => {
    const { output } = fixture();
    publishCapacityReport(output, "baseline\n", "first markdown", false);
    const oldGeneration = readlinkSync(join(output, "current"));
    publishCapacityReport(output, "comparison", "second markdown", true);
    expect(readFileSync(join(output, "current", "benchmark-baseline.json"), "utf8")).toBe("baseline\n");
    expect(readFileSync(join(output, "current", "benchmark.json"), "utf8")).toBe("comparison");
    expect(readFileSync(join(output, "current", "benchmark.md"), "utf8")).toBe("second markdown");
    expect(readFileSync(join(output, oldGeneration, "benchmark.json"), "utf8")).toBe("baseline\n");
    publishCapacityReport(output, "replacement", "third markdown", false);
    expect(readFileSync(join(output, "current", "benchmark-baseline.json"), "utf8")).toBe("replacement");
  });

  it("keeps the previous complete set if staging a later file fails", () => {
    const { output } = fixture();
    publishCapacityReport(output, "old", "old markdown", false);
    const before = readdirSync(output);
    // The filesystem rejects this invalid write payload after staging benchmark.json.
    // @ts-expect-error Deliberately pass an invalid filesystem payload to exercise failed staging.
    expect(() => publishCapacityReport(output, "new", null, false)).toThrow();
    expect(readdirSync(output)).toStrictEqual(before);
    expect(readFileSync(join(output, "current", "benchmark.json"), "utf8")).toBe("old");
    expect(readFileSync(join(output, "current", "benchmark.md"), "utf8")).toBe("old markdown");
    expect(readFileSync(join(output, "current", "benchmark-baseline.json"), "utf8")).toBe("old");
  });

  it("cleans staged artifacts when pointer promotion fails", () => {
    const { output } = fixture();
    mkdirSync(join(output, "current"));
    writeFileSync(join(output, "current", "benchmark.json"), "old");
    expect(() => publishCapacityReport(output, "new", "markdown", false)).toThrow();
    expect(readdirSync(output)).toStrictEqual(["current"]);
    expect(readFileSync(join(output, "current", "benchmark.json"), "utf8")).toBe("old");
  });
});
