import { Schema } from "effect";
import { Baseline, report } from "../packages/kafka/benchmarks/capacity-model.ts";
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, rmSync } from "node:fs";
import { resolve } from "node:path";
import { parseArgs } from "node:util";

export function parseCapacityArguments(args) {
const { values } = parseArgs({ args, options: {
  compare: { type: "boolean", default: false },
  rows: { type: "string", default: "50000000" },
  output: { type: "string", default: "packages/kafka/.artifacts/capacity" },
  "timeout-seconds": { type: "string", default: "86400" },
  "skip-build": { type: "boolean", default: false },
  help: { type: "boolean", default: false },
} });

  const rows = Number(values.rows);
  const timeout = Number(values["timeout-seconds"]);
  if (!Number.isSafeInteger(rows) || rows < 1 || rows > 50_000_000 || !Number.isSafeInteger(timeout) || timeout < 1 || timeout > 2_000_000) {
    throw new Error("Rows must be 1..50000000; timeout-seconds must be 1..2000000.");
  }
  return { ...values, rows, timeout, output: resolve(values.output) };
}

export async function runCapacityBenchmark(options, {
  spawnProcess,
  processEvents,
  schedule,
  cancel,
  lockPath,
}) {
  const { rows, timeout, output } = options;
  if (options.compare) {
    const baseline = Schema.decodeUnknownSync(Baseline)(JSON.parse(readFileSync(resolve(output, "benchmark-baseline.json"), "utf8")));
    report(baseline);
    if (baseline.corpus.rowsPerTopic !== rows) {
      throw new Error("Comparison requires a completed baseline with the same corpus and row count.");
    }
  }
  mkdirSync(output, { recursive: true });
  // A fixed lock also protects the shared dedicated broker across output paths.
  const lock = lockPath;
  const handle = openSync(lock, "wx");
  let child;
  let interrupted = false;
  const signal = () => {
    interrupted = true;
    child?.kill("SIGTERM");
  };
  processEvents.on("SIGINT", signal);
  processEvents.on("SIGTERM", signal);
  const run = (command, args) => new Promise((resolveRun, reject) => {
    if (interrupted) return reject(new Error("Benchmark interrupted"));
    const active = spawnProcess(command, args, { stdio: "inherit" });
    child = active;
    let timedOut = false;
    const timer = schedule(() => { timedOut = true; active.kill("SIGKILL"); }, timeout * 1000);
    child.once("error", (error) => { cancel(timer); reject(error); });
    child.once("close", (code, exitSignal) => {
      cancel(timer);
      child = undefined;
      if (code === 0 && !interrupted && !timedOut) resolveRun();
      else reject(new Error(`${command} failed: code=${code}, signal=${exitSignal}, timedOut=${timedOut}`));
    });
  });
  const worker = (mode) => run(process.execPath, ["packages/kafka/benchmarks/capacity.ts", mode, String(rows), output]);
  try {
    if (!options["skip-build"]) await run("vp", ["run", "-t", "effect-view-server#build"]);
    await run("docker", ["compose", "-f", "benchmarks/kafka-capacity/compose.yaml", "up", "-d", "--wait"]);
    await worker("seed");
    for (const count of [1, 3, 10]) {
      const result = resolve(output, `scenario-${count}.json`);
      rmSync(result, { force: true });
      await worker(String(count));
      if (!existsSync(result)) throw new Error(`Scenario ${count} produced no result`);
    }
    await worker(options.compare ? "compare" : "report");
    console.log(`Benchmark report: ${resolve(output, "benchmark.md")}`);
  } finally {
    closeSync(handle);
    rmSync(lock);
    processEvents.off("SIGINT", signal);
    processEvents.off("SIGTERM", signal);
  }
}
