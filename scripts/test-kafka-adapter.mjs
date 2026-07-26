import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const packageDirectory = fileURLToPath(
  new URL("../packages/kafka/", import.meta.url),
);
const testArguments = process.argv.slice(2);
const vitestArguments = testArguments.filter(
  (argument) => argument !== "--",
);
const testFilters = vitestArguments.filter(
  (argument) =>
    !argument.startsWith("-") &&
    argument.includes(".test"),
);
const hasCoverageFlag = testArguments.some(
  (argument) =>
    argument === "--coverage" ||
    argument.startsWith("--coverage=") ||
    argument === "--no-coverage",
);
const shouldCollectCoverage =
  vitestArguments.length === 0 && !hasCoverageFlag;
const shouldStartKafka =
  testFilters.length === 0 ||
  testFilters.some((argument) =>
    argument.includes("kafka.integration.test"),
  );

const run = (command, arguments_, options = {}) => {
  const result = spawnSync(command, arguments_, {
    cwd: options.cwd ?? root,
    env: options.env ?? process.env,
    stdio: "inherit",
    shell: false,
  });
  return result.status ?? 1;
};

let didCleanup = false;
const cleanup = () => {
  if (!shouldStartKafka || didCleanup) {
    return 0;
  }
  didCleanup = true;
  return run("docker", [
    "compose",
    "-f",
    "compose.yaml",
    "down",
  ]);
};

process.once("SIGINT", () => {
  cleanup();
  process.exit(130);
});
process.once("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

let exitCode = shouldStartKafka
  ? run("docker", [
      "compose",
      "-f",
      "compose.yaml",
      "up",
      "-d",
      "--wait",
      "kafka",
      "kafka-london",
    ])
  : 0;

if (exitCode === 0) {
  exitCode = run(
    "vp",
    [
      "test",
      "run",
      ...(shouldCollectCoverage ? ["--coverage"] : []),
      "--typecheck",
      ...vitestArguments,
    ],
    {
      cwd: packageDirectory,
      env: {
        ...process.env,
        VIEW_SERVER_KAFKA_INTEGRATION:
          shouldStartKafka ? "1" : "0",
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS:
          process.env.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS ??
          "localhost:9092",
        VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS:
          process.env
            .VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS ??
          "localhost:9094",
      },
    },
  );
}

const cleanupExitCode = cleanup();
process.exit(
  exitCode === 0 ? cleanupExitCode : exitCode,
);
