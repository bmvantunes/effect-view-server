import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import {
  ReleasePublishCommandError,
  runReleasePublish,
} from "./release-publish-orchestration.mjs";

const rootDirectory = fileURLToPath(new URL("../", import.meta.url));
let exitCode = 0;

if (process.argv.length > 2) {
  process.stderr.write(
    "release-publish.mjs does not accept arguments; approve staged versions with npm stage approve.\n",
  );
  exitCode = 1;
} else {
  try {
    runReleasePublish({
      command: (command, args, options) => spawnSync(command, args, options),
      env: process.env,
      rootDirectory,
      stderr: (message) => process.stderr.write(message),
      stdout: (message) => process.stdout.write(message),
      temporaryDirectory: tmpdir(),
    });
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    exitCode = error instanceof ReleasePublishCommandError ? error.exitCode : 1;
  }
}

process.exit(exitCode);
