import { spawn } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const grpcDirectory = fileURLToPath(new URL("../packages/grpc/", import.meta.url));
const batchSize = process.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_BATCH_SIZE"] ?? "32";
const routeCount = process.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_ROUTE_COUNT"] ?? "32";
const retainedRows =
  process.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_RETAINED_ROWS"] ?? "50000";
const configuredOutput =
  process.env["VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON"] ??
  `.artifacts/grpc-source-adapter-${batchSize}batch-${routeCount}routes-${retainedRows}retained.json`;
const output = resolve(grpcDirectory, configuredOutput);

mkdirSync(dirname(output), { recursive: true });
rmSync(output, { force: true });

const child = spawn(
  "vp",
  [
    "test",
    "bench",
    "src/grpc.bench.ts",
    "--run",
    "--testTimeout",
    "0",
    "--outputJson",
    configuredOutput,
  ],
  {
    cwd: grpcDirectory,
    env: {
      ...process.env,
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: configuredOutput,
    },
    stdio: "inherit",
  },
);

child.once("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});

child.once("close", (code, signal) => {
  if (code !== 0) {
    console.error(
      signal === null
        ? `gRPC Source Adapter benchmark exited with code ${String(code)}.`
        : `gRPC Source Adapter benchmark terminated by ${signal}.`,
    );
    process.exitCode = code ?? 1;
    return;
  }
  if (!existsSync(output)) {
    console.error(`gRPC Source Adapter benchmark did not write ${output}.`);
    process.exitCode = 1;
  }
});
