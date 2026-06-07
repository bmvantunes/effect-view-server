import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const runtimePackage = new URL("../packages/runtime/", import.meta.url);
const runtimeDirectory = fileURLToPath(runtimePackage);
const kafkaBootstrapServers =
  process.env.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092";

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? root,
    env: options.env ?? process.env,
    stdio: "inherit",
    shell: false,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
};

run("docker", ["compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"]);
run(
  "sh",
  [
    "-c",
    "vp run -t @view-server/runtime-core#build && vp run -t @view-server/server#build && vp test run --coverage --typecheck",
  ],
  {
    cwd: runtimeDirectory,
    env: {
      ...process.env,
      VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: kafkaBootstrapServers,
    },
  },
);
