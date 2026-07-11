import {
  cpSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join, relative } from "node:path";
import {
  classifyStagePublishDuplicateOutput,
  oidcPublishEnvironmentViolations,
  publishedFileViolations,
  publicPackageName,
  publishDecision,
  sanitizePublicPackageJson,
  stagedPackageTagName,
  stagePublishCommandArguments,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const publicPackageRelativeDirectory = join("packages", "effect-view-server");
const workspacePackageDirectories = ["apps", "examples", "packages", "tools"];

export class ReleasePublishCommandError extends Error {
  constructor(message, exitCode) {
    super(message);
    this.name = "ReleasePublishCommandError";
    this.exitCode = exitCode;
  }
}

const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));

const collectWorkspacePackages = (rootDirectory) => {
  const workspacePackages = [];

  for (const directory of workspacePackageDirectories) {
    const directoryPath = join(rootDirectory, directory);

    if (!existsSync(directoryPath)) {
      continue;
    }

    for (const entry of readdirSync(directoryPath, { withFileTypes: true })) {
      if (!entry.isDirectory()) {
        continue;
      }

      const packageJsonPath = join(directoryPath, entry.name, "package.json");

      if (!existsSync(packageJsonPath)) {
        continue;
      }

      workspacePackages.push(readJson(packageJsonPath));
    }
  }

  return workspacePackages;
};

const commandResult = (execution, executable, args, options) => {
  const result = execution.command(executable, args, {
    ...options,
    cwd: execution.cwd,
    env: execution.env,
  });

  if (result.error !== undefined) {
    throw result.error;
  }

  return result;
};

const runCommand = (execution, executable, args) => {
  const result = commandResult(execution, executable, args, {
    stdio: "inherit",
  });

  if (result.status !== 0) {
    throw new ReleasePublishCommandError(
      `${executable} ${args.join(" ")} failed.`,
      result.status ?? 1,
    );
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

const isPackageAlreadyCreated = (execution) => {
  const result = commandResult(execution, "npm", ["view", publicPackageName, "name", "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return result.status === 0 && JSON.parse(result.stdout) === publicPackageName;
};

const isVersionAlreadyPublished = (execution, version) => {
  const result = commandResult(
    execution,
    "npm",
    ["view", `${publicPackageName}@${version}`, "version", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 && JSON.parse(result.stdout) === version;
};

const runStagePublish = ({ execution, stageDirectory, stderr, stdout, version }) => {
  const args = stagePublishCommandArguments(stageDirectory);
  const result = commandResult(execution, "npm", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  stdout(result.stdout);
  stderr(result.stderr);

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

  throw new ReleasePublishCommandError(
    `npm ${args.join(" ")} failed.`,
    result.status ?? 1,
  );
};

const gitRefTarget = (execution, ref) => {
  const result = commandResult(
    execution,
    "git",
    ["rev-parse", "--quiet", "--verify", `${ref}^{}`],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 ? result.stdout.trim() : undefined;
};

const gitRefObject = (execution, ref) => {
  const result = commandResult(execution, "git", ["rev-parse", "--quiet", "--verify", ref], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return result.status === 0 ? result.stdout.trim() : undefined;
};

const gitTagExists = (execution, tagName) =>
  gitRefTarget(execution, `refs/tags/${tagName}`) !== undefined;

const pushGitTag = (execution, tagName, expectedRemoteObject) => {
  runCommand(
    execution,
    "git",
    expectedRemoteObject === undefined
      ? ["push", "origin", `refs/tags/${tagName}`]
      : [
          "push",
          `--force-with-lease=refs/tags/${tagName}:${expectedRemoteObject}`,
          "origin",
          `refs/tags/${tagName}`,
        ],
  );
};

const ensureStagedGitTag = (execution, tagName) => {
  const expectedTarget = gitRefTarget(execution, "HEAD");

  if (expectedTarget === undefined) {
    throw new Error(`Cannot create ${tagName} because HEAD does not resolve to a git object.`);
  }

  const ref = `refs/tags/${tagName}`;
  const existingTarget = gitRefTarget(execution, ref);
  const existingObject = gitRefObject(execution, ref);

  if (existingTarget !== undefined) {
    if (existingTarget !== expectedTarget) {
      runCommand(execution, "git", ["tag", "-f", "-a", tagName, expectedTarget, "-m", tagName]);
      pushGitTag(execution, tagName, existingObject);
    }
    return;
  }

  runCommand(execution, "git", ["tag", "-a", tagName, expectedTarget, "-m", tagName]);
  pushGitTag(execution, tagName, undefined);
};

const stagePublicPackage = ({ publicPackageDirectory, packageJson, stageDirectory }) => {
  const distDirectory = join(stageDirectory, "dist");

  cpSync(join(publicPackageDirectory, "dist"), distDirectory, {
    recursive: true,
    filter: (source) => !source.endsWith(".map"),
  });
  stripPublishedSourceMapReferences(distDirectory);
  cpSync(join(publicPackageDirectory, "README.md"), join(stageDirectory, "README.md"));
  writeFileSync(
    join(stageDirectory, "package.json"),
    `${JSON.stringify(sanitizePublicPackageJson(packageJson), null, 2)}\n`,
  );
  assertCleanPublishedFiles(stageDirectory);
};

export const runReleasePublish = ({
  command,
  env,
  rootDirectory,
  stderr,
  stdout,
  temporaryDirectory,
}) => {
  const publicPackageDirectory = join(rootDirectory, publicPackageRelativeDirectory);
  const packageJson = readJson(join(publicPackageDirectory, "package.json"));
  const version = packageJson.version;
  const execution = {
    command,
    cwd: rootDirectory,
    env,
  };
  const decision = publishDecision({
    env,
    version,
    workspacePackages: collectWorkspacePackages(rootDirectory),
  });

  if (decision._tag === "Skip") {
    stdout(`${decision.message}\n`);
    return {
      _tag: "Skipped",
      version,
    };
  }

  if (decision._tag === "Refuse") {
    throw new Error(decision.message);
  }

  const stageDirectory = mkdtempSync(join(temporaryDirectory, "effect-view-server-publish-"));

  try {
    stagePublicPackage({
      packageJson,
      publicPackageDirectory,
      stageDirectory,
    });

    if (isVersionAlreadyPublished(execution, version)) {
      stdout(`${publicPackageName}@${version} is already published.\n`);
      return {
        _tag: "AlreadyPublished",
        version,
      };
    }

    if (!isPackageAlreadyCreated(execution)) {
      throw new Error(
        `${publicPackageName} must exist on npm before staged publishing can be used. Publish the first version manually, then rerun this workflow.`,
      );
    }

    const oidcViolations = oidcPublishEnvironmentViolations(env);
    if (oidcViolations.length > 0) {
      throw new Error(
        [
          "Refusing npm stage publish because GitHub Actions OIDC is unavailable.",
          ...oidcViolations.map((violation) => `- ${violation}`),
        ].join("\n"),
      );
    }

    const tagName = stagedPackageTagName(version);
    const stageResult = runStagePublish({
      execution,
      stageDirectory,
      stderr,
      stdout,
      version,
    });

    if (stageResult._tag === "Staged") {
      ensureStagedGitTag(execution, tagName);
      return {
        _tag: "Staged",
        version,
      };
    }

    if (stageResult._tag === "AlreadyPublished") {
      if (isVersionAlreadyPublished(execution, version)) {
        stdout(`${publicPackageName}@${version} is already published.\n`);
        return {
          _tag: "AlreadyPublished",
          version,
        };
      }
      throw new Error(
        `npm reported ${publicPackageName}@${version} as already published, but npm view does not confirm it. Refusing to treat the release as complete.`,
      );
    }

    if (stageResult._tag === "AlreadyStaged") {
      if (!gitTagExists(execution, tagName)) {
        throw new Error(
          `npm reported ${publicPackageName}@${version} as already staged, but ${tagName} is missing. Refusing to recreate it from an unverified workflow HEAD; rerun the original failed staging workflow attempt or reject the npm stage and restage.`,
        );
      }
      stdout(`${publicPackageName}@${version} is already staged; keeping ${tagName}.\n`);
      return {
        _tag: "AlreadyStaged",
        version,
      };
    }

    throw new Error(
      `npm reported a duplicate ${publicPackageName}@${version}, but npm view does not report it as published. Refusing to guess whether a stage exists.`,
    );
  } finally {
    rmSync(stageDirectory, {
      force: true,
      recursive: true,
    });
  }
};
