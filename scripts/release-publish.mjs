import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import {
  classifyStagePublishDuplicateOutput,
  packageTagName,
  oidcPublishEnvironmentViolations,
  publishedFileViolations,
  publicPackageName,
  publishDecision,
  sanitizePublicPackageJson,
  stagedPackageTagName,
  stagePublishCommandArguments,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const packageUrl = new URL("../packages/effect-view-server/package.json", import.meta.url);
const packageJson = JSON.parse(readFileSync(packageUrl, "utf8"));
const version = packageJson.version;
const workspacePackages = [];
const workspacePackageDirectories = ["apps", "examples", "packages", "tools"];

for (const directory of workspacePackageDirectories) {
  const directoryUrl = new URL(`../${directory}/`, import.meta.url);

  if (!existsSync(directoryUrl)) {
    continue;
  }

  for (const entry of readdirSync(directoryUrl, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const workspacePackageUrl = new URL(`${entry.name}/package.json`, directoryUrl);

    if (!existsSync(workspacePackageUrl)) {
      continue;
    }

    workspacePackages.push(JSON.parse(readFileSync(workspacePackageUrl, "utf8")));
  }
}

const decision = publishDecision({
  env: process.env,
  version,
  workspacePackages,
});

if (decision._tag === "Skip") {
  process.stdout.write(`${decision.message}\n`);
  process.exit(0);
}

if (decision._tag === "Refuse") {
  process.stderr.write(`${decision.message}\n`);
  process.exit(1);
}

const commandResult = (command, args, options = {}) => {
  const result = spawnSync(command, args, options);

  if (result.error !== undefined) {
    throw result.error;
  }

  return result;
};

const run = (command, args, options = {}) => {
  const result = commandResult(command, args, {
    stdio: "inherit",
    ...options,
  });

  if (result.status !== 0) {
    throw Object.assign(new Error(`${command} ${args.join(" ")} failed.`), {
      exitCode: result.status ?? 1,
    });
  }
};

const collectPublishedFiles = (directory, baseDirectory = directory) => {
  const files = [];

  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      files.push(...collectPublishedFiles(path, baseDirectory));
      continue;
    }

    files.push({
      relativePath: relative(baseDirectory, path).replaceAll("\\", "/"),
      contents: readFileSync(path, "utf8"),
    });
  }

  return files;
};

const stripPublishedSourceMapReferences = (directory) => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      stripPublishedSourceMapReferences(path);
      continue;
    }

    if (!path.endsWith(".js") && !path.endsWith(".d.ts")) {
      continue;
    }

    writeFileSync(path, stripSourceMapReference(readFileSync(path, "utf8")));
  }
};

const assertCleanPublishedFiles = (stageDirectory) => {
  const violations = publishedFileViolations(collectPublishedFiles(stageDirectory));

  if (violations.length > 0) {
    throw new Error(
      [
        "Refusing npm stage publish because the staged package contains private workspace artifacts.",
        ...violations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
};

const isPackageAlreadyCreated = () => {
  const result = commandResult("npm", ["view", publicPackageName, "name", "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return result.status === 0 && JSON.parse(result.stdout) === publicPackageName;
};

const isVersionAlreadyPublished = () => {
  const result = commandResult(
    "npm",
    ["view", `${publicPackageName}@${version}`, "version", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 && JSON.parse(result.stdout) === version;
};

const hasPendingStageForVersion = () => {
  const result = commandResult("npm", ["stage", "list", `${publicPackageName}@${version}`, "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  if (result.status !== 0) {
    throw Object.assign(
      new Error(`npm stage list ${publicPackageName}@${version} --json failed.\n${result.stderr}`),
      {
        exitCode: result.status ?? 1,
      },
    );
  }

  const stages = JSON.parse(result.stdout);

  return Array.isArray(stages)
    ? stages.length > 0
    : stages !== null && typeof stages === "object" && Object.keys(stages).length > 0;
};

const runStagePublish = (stageDirectory) => {
  const result = commandResult("npm", stagePublishCommandArguments(stageDirectory), {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);

  if (result.status === 0) {
    return {
      _tag: "Staged",
    };
  }

  const duplicate = classifyStagePublishDuplicateOutput({
    stderr: result.stderr,
    stdout: result.stdout,
    version,
  });

  if (duplicate._tag !== "Unknown") {
    return duplicate;
  }

  throw Object.assign(new Error(`npm ${stagePublishCommandArguments(stageDirectory).join(" ")} failed.`), {
    exitCode: result.status ?? 1,
  });
};

const gitRefTarget = (ref) => {
  const target = commandResult("git", ["rev-parse", "--quiet", "--verify", `${ref}^{}`], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return target.status === 0 ? target.stdout.trim() : undefined;
};

const gitTagExists = (tagName) => gitRefTarget(`refs/tags/${tagName}`) !== undefined;

const pushGitTag = (tagName, moved) => {
  run(
    "git",
    moved
      ? ["push", `--force-with-lease=refs/tags/${tagName}`, "origin", `refs/tags/${tagName}`]
      : ["push", "origin", `refs/tags/${tagName}`],
  );
};

const ensureGitTag = (tagName, targetRef = "HEAD", options = {}) => {
  const expectedTarget = gitRefTarget(targetRef);

  if (expectedTarget === undefined) {
    throw new Error(`Cannot create ${tagName} because ${targetRef} does not resolve to a git object.`);
  }

  const existingTarget = gitRefTarget(`refs/tags/${tagName}`);

  if (existingTarget !== undefined) {
    if (existingTarget !== expectedTarget) {
      if (options.allowMove !== true) {
        throw new Error(`${tagName} already points at ${existingTarget}, expected ${expectedTarget}.`);
      }

      run("git", ["tag", "-f", "-a", tagName, targetRef, "-m", tagName]);
      pushGitTag(tagName, true);
      return;
    }

    return;
  }

  run("git", ["tag", "-a", tagName, targetRef, "-m", tagName]);
  pushGitTag(tagName, false);
};

const ensurePublishedVersionTag = () => {
  const stagedTagName = stagedPackageTagName(version);

  if (!gitTagExists(stagedTagName)) {
    throw new Error(
      `Cannot create ${packageTagName(version)} because ${stagedTagName} does not exist. Stage the package before approving it.`,
    );
  }

  ensureGitTag(packageTagName(version), `refs/tags/${stagedTagName}`);
};

let exitCode = 0;
const stageDirectory = mkdtempSync(join(tmpdir(), "effect-view-server-publish-"));

try {
  const distUrl = new URL("../packages/effect-view-server/dist/", import.meta.url);
  const distDirectory = join(stageDirectory, "dist");

  cpSync(distUrl, distDirectory, {
    recursive: true,
    filter: (source) => !source.endsWith(".map"),
  });
  stripPublishedSourceMapReferences(distDirectory);
  cpSync(new URL("../README.md", import.meta.url), join(stageDirectory, "README.md"));
  writeFileSync(
    join(stageDirectory, "package.json"),
    `${JSON.stringify(sanitizePublicPackageJson(packageJson), null, 2)}\n`,
  );

  assertCleanPublishedFiles(stageDirectory);

  if (isVersionAlreadyPublished()) {
    process.stdout.write(`${publicPackageName}@${version} is already published; ensuring git tag.\n`);
    ensurePublishedVersionTag();
  } else {
    if (!isPackageAlreadyCreated()) {
      throw new Error(
        `${publicPackageName} must exist on npm before staged publishing can be used. Publish the first version manually, then rerun this workflow.`,
      );
    }

    const stagedTagName = stagedPackageTagName(version);

    const pendingStageExists = hasPendingStageForVersion();

    if (pendingStageExists) {
      if (!gitTagExists(stagedTagName)) {
        throw new Error(
          `${publicPackageName}@${version} is pending npm approval, but ${stagedTagName} is missing. Refusing to create an untraceable staged marker.`,
        );
      }

      process.stdout.write(
        `${publicPackageName}@${version} is already pending npm approval; keeping ${stagedTagName}.\n`,
      );
    } else {
      const oidcViolations = oidcPublishEnvironmentViolations(process.env);
      if (oidcViolations.length > 0) {
        throw new Error(
          [
            "Refusing npm stage publish because GitHub Actions OIDC is unavailable.",
            ...oidcViolations.map((violation) => `- ${violation}`),
          ].join("\n"),
        );
      }

      ensureGitTag(stagedTagName, "HEAD", {
        allowMove: true,
      });

      const stageResult = runStagePublish(stageDirectory);

      const duplicateVersionIsPublished =
        (stageResult._tag === "AlreadyPublished" || stageResult._tag === "DuplicateVersion") &&
        isVersionAlreadyPublished();

      if (duplicateVersionIsPublished) {
        process.stdout.write(
          `${publicPackageName}@${version} is already published; ensuring public git tag.\n`,
        );
        ensurePublishedVersionTag();
      } else {
        if (stageResult._tag === "DuplicateVersion") {
          if (!hasPendingStageForVersion()) {
            throw new Error(
              `npm reported a duplicate ${publicPackageName}@${version}, but npm stage list does not show a pending stage and npm view does not report it as published.`,
            );
          }

          process.stdout.write(
            `${publicPackageName}@${version} has a pending npm stage after a duplicate response; keeping staged marker tag.\n`,
          );
        }

        if (stageResult._tag === "AlreadyStaged" || stageResult._tag === "AlreadyPublished") {
          if (!hasPendingStageForVersion()) {
            throw new Error(
              `npm reported ${publicPackageName}@${version} as staged, but npm stage list does not show a pending stage.`,
            );
          }

          process.stdout.write(
            `${publicPackageName}@${version} is already staged in npm; ensuring staged marker tag.\n`,
          );
        }

        ensureGitTag(stagedTagName);
      }
    }
  }
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  exitCode =
    typeof error === "object" &&
    error !== null &&
    "exitCode" in error &&
    typeof error.exitCode === "number"
      ? error.exitCode
      : 1;
} finally {
  rmSync(stageDirectory, {
    force: true,
    recursive: true,
  });
}

process.exit(exitCode);
