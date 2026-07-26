import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createKafkaAdapterTestRunner } from "./test-kafka-adapter-runner.mjs";

const runner = createKafkaAdapterTestRunner({
  env: process.env,
  packageDirectory: fileURLToPath(new URL("../packages/kafka/", import.meta.url)),
  processId: process.pid,
  rootDirectory: fileURLToPath(new URL("..", import.meta.url)),
  spawnProcess: spawn,
  stdio: "inherit",
  testArguments: process.argv.slice(2),
});

const exitForSignal = (signal) => {
  void runner.handleSignal(signal).then((exitCode) => {
    process.exit(exitCode);
  });
};

process.once("SIGINT", () => {
  exitForSignal("SIGINT");
});
process.once("SIGTERM", () => {
  exitForSignal("SIGTERM");
});

process.exit(await runner.runMain());
