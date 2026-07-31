export const expectedPublishRepository = "bmvantunes/effect-view-server";
export const internalPackageScope = "@effect-view-server/";
export const publicPackageName = "effect-view-server";

const cloneJson = (value) => structuredClone(value);

const omitInternalDependencies = (dependencies) =>
  dependencies === undefined
    ? undefined
    : Object.fromEntries(
        Object.entries(dependencies).filter(([name]) => !name.startsWith(internalPackageScope)),
      );

const definedEntries = (entries) =>
  Object.fromEntries(entries.filter(([, value]) => value !== undefined));

export const packageTagName = (version) => `${publicPackageName}@${version}`;
export const pendingPackageTagName = (version) => `${packageTagName(version)}-pending`;

export const publishCommandArguments = (publishDirectory) => [
  "publish",
  publishDirectory,
  "--provenance",
  "--access",
  "public",
];

const releaseTypeRank = {
  patch: 0,
  minor: 1,
  major: 2,
};

const semverPattern = /^(\d+)\.(\d+)\.(\d+)$/;

export const releaseTypeFromChangesets = (contents) => {
  let releaseType = "patch";

  for (const content of contents) {
    for (const match of content.matchAll(/["']effect-view-server["']\s*:\s*(major|minor|patch)/g)) {
      const candidate = match[1];
      if (releaseTypeRank[candidate] > releaseTypeRank[releaseType]) {
        releaseType = candidate;
      }
    }
  }

  return releaseType;
};

export const incrementReleaseVersion = (version, releaseType) => {
  const match = semverPattern.exec(version);
  if (match === null) {
    throw new Error(`Cannot increment invalid stable release version ${version}.`);
  }

  const major = Number(match[1]);
  const minor = Number(match[2]);
  const patch = Number(match[3]);

  if (releaseType === "major") {
    return `${major + 1}.0.0`;
  }
  if (releaseType === "minor") {
    return `${major}.${minor + 1}.0`;
  }
  return `${major}.${minor}.${patch + 1}`;
};

export const parseReleaseTag = (tag) => {
  const prefix = `${publicPackageName}@`;
  if (!tag.startsWith(prefix)) {
    return undefined;
  }

  const pending = tag.endsWith("-pending");
  const staged = tag.endsWith("-staged");
  const version = tag.slice(prefix.length).replace(/-(?:staged|pending)$/, "");
  const match = semverPattern.exec(version);
  if (match === null) {
    return undefined;
  }

  return {
    pending,
    staged,
    tag,
    version,
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
  };
};

export const compareReleaseTags = (left, right) => {
  if (left.major !== right.major) {
    return left.major - right.major;
  }
  if (left.minor !== right.minor) {
    return left.minor - right.minor;
  }
  if (left.patch !== right.patch) {
    return left.patch - right.patch;
  }
  const markerRank = (tag) => (tag.pending ? 1 : tag.staged ? 0 : 2);
  if (markerRank(left) !== markerRank(right)) {
    return markerRank(left) - markerRank(right);
  }
  return left.tag.localeCompare(right.tag);
};

export const stripSourceMapReference = (contents) =>
  contents.replace(/(?:\n)?\/\/# sourceMappingURL=.*(?:\n|$)/g, "\n");

const hasInternalWorkspaceReference = (file) =>
  file.relativePath === "package.json"
    ? file.contents.includes(internalPackageScope)
    : /(?:from\s+["']|import\s*(?:\(\s*)?["']|require\s*\(\s*["'])@effect-view-server\//.test(
        file.contents,
      );

export const sanitizePublicPackageJson = (packageJson) =>
  definedEntries([
    ["name", publicPackageName],
    ["version", packageJson.version],
    ["description", packageJson.description],
    ["keywords", cloneJson(packageJson.keywords)],
    ["homepage", packageJson.homepage],
    ["bugs", cloneJson(packageJson.bugs)],
    ["license", packageJson.license],
    ["repository", cloneJson(packageJson.repository)],
    ["type", packageJson.type],
    ["sideEffects", packageJson.sideEffects],
    ["exports", cloneJson(packageJson.exports)],
    ["engines", cloneJson(packageJson.engines)],
    ["files", ["dist", "README.md"]],
    [
      "publishConfig",
      {
        ...cloneJson(packageJson.publishConfig ?? {}),
        access: "public",
        provenance: true,
      },
    ],
    ["dependencies", omitInternalDependencies(packageJson.dependencies)],
    ["peerDependencies", omitInternalDependencies(packageJson.peerDependencies)],
    ["peerDependenciesMeta", cloneJson(packageJson.peerDependenciesMeta)],
  ]);

export const publishedFileViolations = (files) =>
  files.flatMap((file) => [
    ...(file.relativePath.endsWith(".map") ? [`${file.relativePath} is a source map`] : []),
    ...(file.contents.includes("sourceMappingURL")
      ? [`${file.relativePath} references a source map`]
      : []),
    ...(hasInternalWorkspaceReference(file)
      ? [`${file.relativePath} references ${internalPackageScope}`]
      : []),
  ]);

export const internalPublishViolations = (workspacePackages) =>
  workspacePackages
    .filter((workspacePackage) => workspacePackage.name !== publicPackageName)
    .filter((workspacePackage) => workspacePackage.private !== true)
    .map((workspacePackage) => workspacePackage.name);

export const isTrustedPublishEnvironment = (env) =>
  env.GITHUB_ACTIONS === "true" &&
  env.GITHUB_EVENT_NAME === "push" &&
  env.GITHUB_REF === "refs/heads/main" &&
  env.GITHUB_REPOSITORY === expectedPublishRepository;

export const oidcPublishEnvironmentViolations = (env) =>
  ["ACTIONS_ID_TOKEN_REQUEST_URL", "ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    .filter((name) => env[name] === undefined || env[name] === "")
    .map((name) => `${name} is required for npm trusted publishing.`);

export const publishDecision = ({ env, version, workspacePackages }) => {
  if (version === "0.0.0") {
    return {
      _tag: "Refuse",
      message: `Refusing to publish placeholder version ${publicPackageName}@0.0.0.`,
    };
  }

  const violations = internalPublishViolations(workspacePackages);
  if (violations.length > 0) {
    return {
      _tag: "Refuse",
      message: `Refusing to publish because ${violations.join(", ")} ${
        violations.length === 1 ? "is" : "are"
      } not private.`,
    };
  }

  if (!isTrustedPublishEnvironment(env)) {
    return {
      _tag: "Refuse",
      message: "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    };
  }

  return {
    _tag: "Publish",
  };
};
