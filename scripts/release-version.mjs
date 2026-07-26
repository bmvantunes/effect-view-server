import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  ReleaseVersionCommandError,
  runReleaseVersion,
} from "./release-version-orchestration.mjs";

const rootDirectory = fileURLToPath(new URL("../", import.meta.url));
let exitCode = 0;

if (process.argv.length > 2) {
  process.stderr.write("release-version.mjs does not accept arguments.\n");
  exitCode = 1;
} else {
  try {
    runReleaseVersion({
      command: (command, args, options) => spawnSync(command, args, options),
      rootDirectory,
    });
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    exitCode = error instanceof ReleaseVersionCommandError ? error.exitCode : 1;
  }
}

process.exit(exitCode);
