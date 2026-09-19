import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { parseCapacityArguments, runCapacityBenchmark } from "./kafka-capacity-runner.mjs";

const options = parseCapacityArguments(process.argv.slice(2));
if (options.help) {
  console.log("vp run -w benchmark:kafka [--compare] [--rows=50000000] [--output=directory] [--timeout-seconds=86400] [--skip-build]\nRuns 1, 3 and 10 topics serially. Keeps the dedicated disk-backed Kafka corpus for replay.");
} else {
  await runCapacityBenchmark(options, { spawnProcess: spawn, processEvents: process, schedule: setTimeout, cancel: clearTimeout, lockPath: resolve("benchmarks/kafka-capacity/.run.lock") });
}
