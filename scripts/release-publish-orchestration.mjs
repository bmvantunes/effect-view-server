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
  compareReleaseTags,
  incrementReleaseVersion,
  isTrustedPublishEnvironment,
  oidcPublishEnvironmentViolations,
  packageTagName,
  parseReleaseTag,
  pendingPackageTagName,
  publishedFileViolations,
  publicPackageName,
  publishCommandArguments,
  publishDecision,
  releaseTypeFromChangesets,
  sanitizePublicPackageJson,
  validatedReleaseArtifactManifestName,
  validatedReleaseArtifactViolations,
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

const assertCleanPublishedFiles = (publishDirectory) => {
  const violations = publishedFileViolations(collectPublishedFiles(publishDirectory));

  if (violations.length > 0) {
    throw new Error(
      [
        "Refusing npm publish because the publish artifact contains private workspace artifacts.",
        ...violations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
};

const assertValidatedReleaseArtifact = (distDirectory, expectedCommit) => {
  if (typeof expectedCommit !== "string" || expectedCommit.length === 0) {
    throw new Error("Refusing npm publish without GITHUB_SHA identifying the tested commit.");
  }
  const manifestPath = join(distDirectory, validatedReleaseArtifactManifestName);
  if (!existsSync(manifestPath)) {
    throw new Error("Refusing npm publish without the validated release artifact manifest.");
  }
  const violations = validatedReleaseArtifactViolations({
    expectedCommit,
    files: collectPublishedFiles(distDirectory),
    manifestContents: readFileSync(manifestPath, "utf8"),
  });
  if (violations.length > 0) {
    throw new Error(
      [
        "Refusing npm publish because the validated release artifact failed integrity checks.",
        ...violations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
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

const readPublishedVersion = (execution) => {
  const result = commandResult(execution, "npm", ["view", publicPackageName, "version", "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  if (result.status !== 0) {
    throw new Error(`${publicPackageName} must already exist on npm before continuous publishing.`);
  }

  const version = JSON.parse(result.stdout);
  if (typeof version !== "string") {
    throw new Error(`npm view returned an invalid version for ${publicPackageName}.`);
  }
  return version;
};

const releaseTags = (execution) => {
  runCommand(execution, "git", ["fetch", "--tags", "origin"]);
  const result = commandResult(execution, "git", ["tag", "--list", `${publicPackageName}@*`], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  return result.stdout
    .split("\n")
    .map((tag) => tag.trim())
    .filter((tag) => tag.length > 0)
    .map(parseReleaseTag)
    .filter((tag) => tag !== undefined);
};

const latestReleaseTag = (tags, publishedVersion) => {
  const matchingTags = tags.filter((tag) => tag.version === publishedVersion);

  return matchingTags.reduce(
    (latest, tag) => (latest === undefined || compareReleaseTags(tag, latest) > 0 ? tag : latest),
    undefined,
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
  const result = commandResult(
    execution,
    "git",
    ["rev-parse", "--quiet", "--verify", ref],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 ? result.stdout.trim() : undefined;
};

const rootCommit = (execution) => {
  const result = commandResult(execution, "git", ["rev-list", "--max-parents=0", "HEAD"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  const commit = result.stdout.trim().split("\n")[0];
  if (commit.length === 0) {
    throw new Error("Cannot determine the repository root commit for release versioning.");
  }
  return commit;
};

const changedChangesetContents = (execution, rootDirectory, baseline) => {
  const result = commandResult(
    execution,
    "git",
    ["diff", "--name-only", `${baseline}..HEAD`, "--", ".changeset"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.stdout
    .split("\n")
    .map((path) => path.trim())
    .filter(
      (path) =>
        path.startsWith(".changeset/") &&
        path.endsWith(".md") &&
        path !== ".changeset/README.md",
    )
    .filter((path) => existsSync(join(rootDirectory, path)))
    .map((path) => readFileSync(join(rootDirectory, path), "utf8"));
};

const resolveReleaseVersion = ({ execution, rootDirectory, packageVersion }) => {
  const publishedVersion = readPublishedVersion(execution);
  const tags = releaseTags(execution);
  const tag = latestReleaseTag(tags, publishedVersion);

  if (tag?.pending === true && tag.version === publishedVersion) {
    const pendingTarget = gitRefTarget(execution, `refs/tags/${tag.tag}`);
    const headTarget = gitRefTarget(execution, "HEAD");
    if (pendingTarget !== undefined && pendingTarget === headTarget) {
      return {
        alreadyPublished: true,
        releaseType: "patch",
        version: publishedVersion,
      };
    }
  }

  if (tag === undefined && publishedVersion !== packageVersion) {
    throw new Error(
      `Cannot determine the release baseline for ${publicPackageName}@${publishedVersion}; its public tag is missing.`,
    );
  }

  const baseline = tag?.tag ?? rootCommit(execution);
  const releaseType = releaseTypeFromChangesets(
    changedChangesetContents(execution, rootDirectory, baseline),
  );

  return {
    releaseType,
    version: incrementReleaseVersion(publishedVersion, releaseType),
  };
};

const runPublish = ({ execution, publishDirectory, stderr, stdout, version }) => {
  const args = publishCommandArguments(publishDirectory);
  const result = commandResult(execution, "npm", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  stdout(result.stdout);
  stderr(result.stderr);

  if (result.status === 0) {
    return {
      _tag: "Published",
    };
  }

  if (isVersionAlreadyPublished(execution, version)) {
    if (!pendingTagMatchesHead(execution, version)) {
      throw new Error(
        `Refusing to adopt ${publicPackageName}@${version} without a pending tag at the tested commit.`,
      );
    }
    return {
      _tag: "AlreadyPublished",
    };
  }

  throw new ReleasePublishCommandError(
    `npm ${args.join(" ")} failed.`,
    result.status ?? 1,
  );
};

const ensurePublishedGitTag = (execution, tagName) => {
  const expectedTarget = gitRefTarget(execution, "HEAD");
  if (expectedTarget === undefined) {
    throw new Error(`Cannot create ${tagName} because HEAD does not resolve to a git object.`);
  }

  const ref = `refs/tags/${tagName}`;
  const existingTarget = gitRefTarget(execution, ref);

  if (existingTarget !== undefined) {
    if (existingTarget !== expectedTarget) {
      throw new Error(`Refusing to move published tag ${tagName} away from its existing commit.`);
    }
    return;
  }

  runCommand(execution, "git", ["tag", "-a", tagName, expectedTarget, "-m", tagName]);
  runCommand(execution, "git", ["push", "origin", `refs/tags/${tagName}`]);
};

const ensurePendingGitTag = (execution, tagName) => {
  const expectedTarget = gitRefTarget(execution, "HEAD");
  if (expectedTarget === undefined) {
    throw new Error(`Cannot reserve ${tagName} because HEAD does not resolve to a git object.`);
  }

  const ref = `refs/tags/${tagName}`;
  const existingTarget = gitRefTarget(execution, ref);
  if (existingTarget === expectedTarget) {
    return;
  }

  if (existingTarget === undefined) {
    runCommand(execution, "git", ["tag", "-a", tagName, expectedTarget, "-m", tagName]);
    runCommand(execution, "git", ["push", "origin", `refs/tags/${tagName}`]);
    return;
  }

  const existingObject = gitRefObject(execution, ref);
  if (existingObject === undefined) {
    throw new Error(`Cannot update pending tag ${tagName} because its git object is unavailable.`);
  }

  runCommand(execution, "git", ["tag", "-f", "-a", tagName, expectedTarget, "-m", tagName]);
  runCommand(
    execution,
    "git",
    [
      "push",
      `--force-with-lease=${ref}:${existingObject}`,
      "origin",
      `refs/tags/${tagName}`,
    ],
  );
};

const pendingTagMatchesHead = (execution, version) => {
  const headTarget = gitRefTarget(execution, "HEAD");
  const pendingTarget = gitRefTarget(
    execution,
    `refs/tags/${pendingPackageTagName(version)}`,
  );
  return headTarget !== undefined && pendingTarget === headTarget;
};

const preparePublicPackage = ({
  publicPackageDirectory,
  packageJson,
  publishDirectory,
  releaseVersion,
  testedCommit,
}) => {
  const sourceDistDirectory = join(publicPackageDirectory, "dist");
  const distDirectory = join(publishDirectory, "dist");
  const manifestPath = join(sourceDistDirectory, validatedReleaseArtifactManifestName);

  assertValidatedReleaseArtifact(sourceDistDirectory, testedCommit);
  cpSync(sourceDistDirectory, distDirectory, {
    recursive: true,
    filter: (source) => source !== manifestPath,
  });
  cpSync(join(publicPackageDirectory, "README.md"), join(publishDirectory, "README.md"));
  writeFileSync(
    join(publishDirectory, "package.json"),
    `${JSON.stringify(
      sanitizePublicPackageJson({ ...packageJson, version: releaseVersion }),
      null,
      2,
    )}\n`,
  );
  assertCleanPublishedFiles(publishDirectory);
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
  const execution = {
    command,
    cwd: rootDirectory,
    env,
  };
  if (!isTrustedPublishEnvironment(env)) {
    throw new Error("Refusing npm publish outside the trusted main-branch GitHub Actions context.");
  }
  const oidcViolations = oidcPublishEnvironmentViolations(env);
  if (oidcViolations.length > 0) {
    throw new Error(
      [
        "Refusing npm publish because GitHub Actions OIDC is unavailable.",
        ...oidcViolations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
  const release = resolveReleaseVersion({
    execution,
    packageVersion: packageJson.version,
    rootDirectory,
  });
  const decision = publishDecision({
    env,
    version: release.version,
    workspacePackages: collectWorkspacePackages(rootDirectory),
  });

  if (decision._tag === "Refuse") {
    throw new Error(decision.message);
  }

  const publishDirectory = mkdtempSync(join(temporaryDirectory, "effect-view-server-publish-"));

  try {
    preparePublicPackage({
      packageJson,
      publicPackageDirectory,
      publishDirectory,
      releaseVersion: release.version,
      testedCommit: env.GITHUB_SHA,
    });

    const tagName = packageTagName(release.version);
    if (release.alreadyPublished) {
      ensurePublishedGitTag(execution, tagName);
      stdout(`${publicPackageName}@${release.version} is already published; repaired its tag.\n`);
      return {
        _tag: "AlreadyPublished",
        releaseType: release.releaseType,
        version: release.version,
      };
    }

    if (isVersionAlreadyPublished(execution, release.version)) {
      if (!pendingTagMatchesHead(execution, release.version)) {
        throw new Error(
          `Refusing to adopt ${publicPackageName}@${release.version} without a pending tag at the tested commit.`,
        );
      }
      ensurePublishedGitTag(execution, tagName);
      stdout(`${publicPackageName}@${release.version} is already published.\n`);
      return {
        _tag: "AlreadyPublished",
        releaseType: release.releaseType,
        version: release.version,
      };
    }

    ensurePendingGitTag(execution, pendingPackageTagName(release.version));
    const publishResult = runPublish({
      execution,
      publishDirectory,
      stderr,
      stdout,
      version: release.version,
    });

    ensurePublishedGitTag(execution, tagName);
    stdout(`${publicPackageName}@${release.version} published as ${release.releaseType}.\n`);
    return {
      _tag: publishResult._tag,
      releaseType: release.releaseType,
      version: release.version,
    };
  } finally {
    rmSync(publishDirectory, {
      force: true,
      recursive: true,
    });
  }
};
