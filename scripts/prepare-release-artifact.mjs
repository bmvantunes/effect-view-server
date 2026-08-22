import {
  existsSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join, relative } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  createValidatedReleaseArtifactManifest,
  validatedReleaseArtifactManifestName,
} from "./release-publish-policy.mjs";
import {
  stripSourceMapReference,
  validatedPublishedFileViolations,
} from "./release-validated-artifact.mjs";

const rootDirectory = fileURLToPath(new URL("../", import.meta.url));
const distDirectory = fileURLToPath(
  new URL("../packages/effect-view-server/dist/", import.meta.url),
);

const collectFiles = (directory, baseDirectory = directory) => {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectFiles(path, baseDirectory));
    } else {
      files.push({
        relativePath: relative(baseDirectory, path).replaceAll("\\", "/"),
        contents: readFileSync(path, "utf8"),
      });
    }
  }
  return files;
};

if (!existsSync(distDirectory)) {
  throw new Error("Cannot prepare a validated release artifact before the public package build.");
}

for (const file of collectFiles(distDirectory)) {
  const path = join(distDirectory, file.relativePath);
  if (file.relativePath.endsWith(".map")) {
    rmSync(path);
  } else if (file.relativePath.endsWith(".js") || file.relativePath.endsWith(".d.ts")) {
    writeFileSync(path, stripSourceMapReference(file.contents));
  }
}

const files = collectFiles(distDirectory);
const violations = validatedPublishedFileViolations(files);
if (violations.length > 0) {
  throw new Error(
    [
      "Refusing to hand off an unsafe public package artifact.",
      ...violations.map((violation) => `- ${violation}`),
    ].join("\n"),
  );
}

const commitResult = spawnSync("git", ["rev-parse", "HEAD"], {
  cwd: rootDirectory,
  encoding: "utf8",
});
if (commitResult.status !== 0 || commitResult.stdout.trim().length === 0) {
  throw new Error("Cannot bind the validated release artifact to the tested commit.");
}
writeFileSync(
  join(distDirectory, validatedReleaseArtifactManifestName),
  `${JSON.stringify(createValidatedReleaseArtifactManifest(files, commitResult.stdout.trim()))}\n`,
);
