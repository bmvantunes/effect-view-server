import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const publicPackageName = "effect-view-server";
const kafkaPackageName = "@effect-view-server/kafka";
const publicPackageRelativePath = join("packages", "effect-view-server", "package.json");
const kafkaPackageRelativePath = join("packages", "kafka", "package.json");
const kafkaPeerMatrixRelativePath = join(
  "packages",
  "kafka",
  "source-adapter-peer-matrix.json",
);
const workspaceRelativePath = "pnpm-workspace.yaml";
const kafkaOverrideKey = '"@effect-view-server/kafka>effect-view-server"';
const semverPattern =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export class ReleaseVersionCommandError extends Error {
  constructor(message, exitCode) {
    super(message);
    this.name = "ReleaseVersionCommandError";
    this.exitCode = exitCode;
  }
}

const isRecord = (value) => typeof value === "object" && value !== null && !Array.isArray(value);

const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));

const validateManifest = (manifest, expectedName, path) => {
  if (!isRecord(manifest) || manifest.name !== expectedName) {
    throw new Error(`${path} must describe ${expectedName}.`);
  }

  if (typeof manifest.version !== "string" || !semverPattern.test(manifest.version)) {
    throw new Error(`${path} must contain a valid semantic version.`);
  }

  return manifest;
};

const validateKafkaManifest = (manifest, path) => {
  validateManifest(manifest, kafkaPackageName, path);

  if (manifest.private !== true) {
    throw new Error(`${path} must remain private.`);
  }

  for (const dependencyField of ["devDependencies", "peerDependencies"]) {
    const dependencies = manifest[dependencyField];
    if (
      !isRecord(dependencies) ||
      typeof dependencies[publicPackageName] !== "string"
    ) {
      throw new Error(
        `${path} must contain an exact ${dependencyField}.${publicPackageName} dependency.`,
      );
    }
  }

  return manifest;
};

const validatePeerMatrix = (matrix, path) => {
  if (!Array.isArray(matrix) || matrix.length === 0) {
    throw new Error(`${path} must contain at least one tested peer combination.`);
  }

  for (const entry of matrix) {
    if (
      !isRecord(entry) ||
      typeof entry.effect !== "string" ||
      typeof entry[publicPackageName] !== "string"
    ) {
      throw new Error(
        `${path} entries must contain exact effect and ${publicPackageName} versions.`,
      );
    }
  }

  return matrix;
};

const synchronizeWorkspaceOverride = (workspace, version, path) => {
  const matchingLines = workspace
    .split("\n")
    .filter((line) => line.includes(`${kafkaPackageName}>${publicPackageName}`));

  if (matchingLines.length !== 1) {
    throw new Error(
      `${path} must contain exactly one ${kafkaPackageName}>${publicPackageName} override.`,
    );
  }

  const overridePattern = new RegExp(
    `^(\\s*${kafkaOverrideKey}:\\s*)"workspace:[^"\\s]+"(\\s*)$`,
    "m",
  );

  if (!overridePattern.test(workspace)) {
    throw new Error(
      `${path} must express the ${kafkaPackageName}>${publicPackageName} override as an exact quoted workspace version.`,
    );
  }

  return workspace.replace(overridePattern, `$1"workspace:${version}"$2`);
};

const readReleaseMetadata = (rootDirectory) => {
  const publicPackagePath = join(rootDirectory, publicPackageRelativePath);
  const kafkaPackagePath = join(rootDirectory, kafkaPackageRelativePath);
  const kafkaPeerMatrixPath = join(rootDirectory, kafkaPeerMatrixRelativePath);
  const workspacePath = join(rootDirectory, workspaceRelativePath);
  const publicPackage = validateManifest(
    readJson(publicPackagePath),
    publicPackageName,
    publicPackageRelativePath,
  );
  const workspace = readFileSync(workspacePath, "utf8");

  synchronizeWorkspaceOverride(workspace, publicPackage.version, workspaceRelativePath);

  return {
    kafkaPackage: validateKafkaManifest(readJson(kafkaPackagePath), kafkaPackageRelativePath),
    kafkaPackagePath,
    kafkaPeerMatrix: validatePeerMatrix(
      readJson(kafkaPeerMatrixPath),
      kafkaPeerMatrixRelativePath,
    ),
    kafkaPeerMatrixPath,
    publicPackage,
    workspace,
    workspacePath,
  };
};

const prepareSynchronizedMetadata = (metadata) => {
  const version = metadata.publicPackage.version;
  const kafkaPackage = structuredClone(metadata.kafkaPackage);

  kafkaPackage.devDependencies[publicPackageName] = version;
  kafkaPackage.peerDependencies[publicPackageName] = version;

  return {
    kafkaPackage,
    kafkaPeerMatrix: metadata.kafkaPeerMatrix.map((entry) => ({
      ...entry,
      [publicPackageName]: version,
    })),
    version,
    workspace: synchronizeWorkspaceOverride(
      metadata.workspace,
      version,
      workspaceRelativePath,
    ),
  };
};

const commandResult = (command, executable, args, rootDirectory) => {
  const result = command(executable, args, {
    cwd: rootDirectory,
    stdio: "inherit",
  });

  if (result.error !== undefined) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new ReleaseVersionCommandError(
      `${executable} ${args.join(" ")} failed.`,
      result.status ?? 1,
    );
  }
};

export const runReleaseVersion = ({ command, rootDirectory }) => {
  readReleaseMetadata(rootDirectory);
  commandResult(command, "vp", ["exec", "changeset", "version"], rootDirectory);

  const metadata = readReleaseMetadata(rootDirectory);
  const synchronized = prepareSynchronizedMetadata(metadata);

  writeFileSync(
    metadata.kafkaPackagePath,
    `${JSON.stringify(synchronized.kafkaPackage, undefined, 2)}\n`,
  );
  writeFileSync(
    metadata.kafkaPeerMatrixPath,
    `${JSON.stringify(synchronized.kafkaPeerMatrix, undefined, 2)}\n`,
  );
  writeFileSync(metadata.workspacePath, synchronized.workspace);

  commandResult(command, "vp", ["install"], rootDirectory);

  return {
    version: synchronized.version,
  };
};
