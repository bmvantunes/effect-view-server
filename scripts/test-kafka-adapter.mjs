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
  void runner.handleSignal(signal).then(exitWithCode, exitWithFailure);
};

const exitWithCode = (exitCode) => {
  process.exit(exitCode);
};

const exitWithFailure = (error) => {
  console.error("Kafka adapter test runner failed.", error);
  process.exit(1);
};

process.once("SIGINT", () => {
  exitForSignal("SIGINT");
});
process.once("SIGTERM", () => {
  exitForSignal("SIGTERM");
});
process.once("SIGHUP", () => {
  exitForSignal("SIGHUP");
});

void runner.runMain().then(exitWithCode, exitWithFailure);
