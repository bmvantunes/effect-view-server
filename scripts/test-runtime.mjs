import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const runtimePackage = new URL("../packages/runtime/", import.meta.url);
const runtimeDirectory = fileURLToPath(runtimePackage);
const testArguments = process.argv.slice(2);
const vitestArguments = testArguments.filter((argument) => argument !== "--");
const hasCoverageFlag = testArguments.some(
  (argument) =>
    argument === "--coverage" ||
    argument.startsWith("--coverage=") ||
    argument === "--no-coverage",
);
const shouldCollectCoverage = vitestArguments.length === 0 && !hasCoverageFlag;
const buildDirectories = [
  new URL("../packages/effect-utils/", import.meta.url),
  new URL("../packages/source-adapter/", import.meta.url),
  new URL("../packages/config/", import.meta.url),
  new URL("../packages/source-adapter-testing/", import.meta.url),
  new URL("../packages/protocol/", import.meta.url),
  new URL("../packages/column-live-view-engine/", import.meta.url),
  new URL("../packages/client/", import.meta.url),
  new URL("../packages/runtime-core/", import.meta.url),
  new URL("../packages/server/", import.meta.url),
];

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? root,
    env: options.env ?? process.env,
    stdio: "inherit",
    shell: false,
  });
  return result.status ?? 1;
};

let exitCode = 0;

for (const directory of buildDirectories) {
  exitCode = run("vp", ["pack"], { cwd: fileURLToPath(directory) });
  if (exitCode !== 0) {
    break;
  }
}

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
      cwd: runtimeDirectory,
    },
  );
}

process.exit(exitCode);
