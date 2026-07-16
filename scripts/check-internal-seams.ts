import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import MarkdownIt from "markdown-it";
import type Token from "markdown-it/lib/token.mjs";
import {
  consumerPackageSpecifiers,
  expectedPackageSurfaces,
  facadeProjections,
  packageSurfacePolicy,
  sourceModuleExtensions,
  sourceForbiddenExportPolicies,
  workspacePackageSpecifiers,
  type ExpectedPackageSurface,
  type FacadeProjection,
  type PrivatePackageSurface,
  type SourceForbiddenExportPolicy,
} from "./package-surface-policy.ts";
import {
  identifierNamesFromTypeScript,
  inspectLibraryPack,
  inspectReexportModule,
  inspectTypeScriptModule,
  namespaceImportsFromTypeScript,
  type UnsupportedModuleLoad,
} from "./typescript-module-inspection.ts";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const noIgnoredDirectories: ReadonlySet<string> = new Set();

const toPosixPath = (path: string): string => path.replaceAll("\\", "/");

export const sourceFiles = (
  directory: string,
  ignoredDirectoryNames: ReadonlySet<string> = noIgnoredDirectories,
): ReadonlyArray<string> => {
  if (!existsSync(directory)) {
    return [];
  }
  const files: Array<string> = [];
  const entries = readdirSync(directory, { withFileTypes: true }).sort((left, right) =>
    left.name.localeCompare(right.name),
  );
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (!ignoredDirectoryNames.has(entry.name)) {
        files.push(...sourceFiles(path, ignoredDirectoryNames));
      }
    } else if (
      entry.isFile() &&
      sourceModuleExtensions.some((extension) => entry.name.endsWith(extension))
    ) {
      files.push(path);
    }
  }
  return files;
};

export const isTestFile = (path: string): boolean =>
  /\.(?:test|test-d|bench)\.(?:ts|tsx|mts|cts)$/.test(path) ||
  toPosixPath(path).includes("/test-support/");

const currentScope = "@effect-view-server";
const staleScope = "@view" + "-server";
const facadePackage = packageSurfacePolicy.facade.packageName;
const workspaceSpecifierSet = new Set(workspacePackageSpecifiers);
const consumerSpecifierSet = new Set(consumerPackageSpecifiers);

const isScopeSpecifier = (specifier: string, scope: string): boolean =>
  specifier === scope || specifier.startsWith(`${scope}/`);

const isFacadeSpecifier = (specifier: string): boolean =>
  specifier === facadePackage || specifier.startsWith(`${facadePackage}/`);

const inspectionViolationMessage = (
  relativePath: string,
  violation: UnsupportedModuleLoad,
): string =>
  `${relativePath}:${violation.line}:${violation.column} uses unsupported ${violation.kind} module loading through ${violation.loader}.`;

export const consumerImportViolationsFor = ({
  contents,
  fileName,
  relativePath,
}: {
  readonly contents: string;
  readonly fileName?: string;
  readonly relativePath: string;
}): ReadonlyArray<string> => {
  const inspection = inspectTypeScriptModule({ fileName: fileName ?? relativePath, source: contents });
  const violations = inspection.moduleSpecifiers.flatMap((specifier) => {
    if (isScopeSpecifier(specifier, currentScope)) {
      return [
        `${relativePath} imports ${specifier}: consumers must import the publishable effect-view-server/* facade.`,
      ];
    }
    if (isScopeSpecifier(specifier, staleScope)) {
      return [
        `${relativePath} imports ${specifier}: stale View Server package scope; consumers must use approved effect-view-server/* subpaths.`,
      ];
    }
    if (specifier === facadePackage) {
      return [
        `${relativePath} imports ${specifier}: the package root is not exported; consumers must use an approved effect-view-server/* subpath.`,
      ];
    }
    if (isFacadeSpecifier(specifier) && !consumerSpecifierSet.has(specifier)) {
      return [
        `${relativePath} imports ${specifier}: consumers must use approved effect-view-server/* package exports.`,
      ];
    }
    return [];
  });
  violations.push(
    ...inspection.violations.map((violation) =>
      inspectionViolationMessage(relativePath, violation),
    ),
  );
  return violations;
};

type MarkdownFenceToken = Token & {
  readonly map: [number, number];
  readonly type: "fence";
};

const markdown = new MarkdownIt("commonmark");
const isFenceToken = (token: Token): token is MarkdownFenceToken => token.type === "fence";

const virtualFenceExtension = (info: string): string => {
  const language = info.trim().split(/\s+/, 1)[0]?.toLowerCase();
  if (language === "ts" || language === "typescript") {
    return ".ts";
  }
  if (language === "mts" || language === "cts") {
    return `.${language}`;
  }
  if (language === "tsx" || language === "jsx") {
    return `.${language}`;
  }
  if (language === "js" || language === "mjs" || language === "cjs") {
    return `.${language}`;
  }
  if (language === "javascript") {
    return ".js";
  }
  return ".tsx";
};

export const consumerMarkdownImportViolationsFor = ({
  contents,
  relativePath,
}: {
  readonly contents: string;
  readonly relativePath: string;
}): ReadonlyArray<string> =>
  markdown
    .parse(contents, {})
    .filter(isFenceToken)
    .flatMap((token) => {
      const openingLine = token.map[0] + 1;
      return consumerImportViolationsFor({
        contents: token.content,
        fileName: `${relativePath}:${openingLine}${virtualFenceExtension(token.info)}`,
        relativePath: `${relativePath}:${openingLine}`,
      });
    });

const consumerIgnoredDirectories = new Set([
  ".next",
  ".output",
  ".vite",
  "build",
  "coverage",
  "dist",
  "node_modules",
]);

const markdownFiles = (directory: string): ReadonlyArray<string> => {
  if (!existsSync(directory)) {
    return [];
  }
  return readdirSync(directory, { withFileTypes: true })
    .sort((left, right) => left.name.localeCompare(right.name))
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory() && !consumerIgnoredDirectories.has(entry.name)) {
        return markdownFiles(path);
      }
      return entry.isFile() && entry.name.endsWith(".md") ? [path] : [];
    });
};

const consumerMarkdownFiles = (repositoryRoot: string): ReadonlyArray<string> => {
  const files = existsSync(join(repositoryRoot, "README.md"))
    ? [join(repositoryRoot, "README.md")]
    : [];
  for (const directory of ["apps", "docs", "examples", "plans"]) {
    files.push(...markdownFiles(join(repositoryRoot, directory)));
  }
  const packagesRoot = join(repositoryRoot, "packages");
  if (existsSync(packagesRoot)) {
    for (const entry of readdirSync(packagesRoot, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        const readme = join(packagesRoot, entry.name, "README.md");
        if (existsSync(readme)) {
          files.push(readme);
        }
      }
    }
  }
  return files.sort();
};

export const collectConsumerImportViolations = (
  repositoryRoot = repoRoot,
): ReadonlyArray<string> => {
  const violations: Array<string> = [];
  for (const directory of ["apps", "examples"]) {
    for (const path of sourceFiles(
      join(repositoryRoot, directory),
      consumerIgnoredDirectories,
    )) {
      violations.push(
        ...consumerImportViolationsFor({
          contents: readFileSync(path, "utf8"),
          fileName: path,
          relativePath: toPosixPath(relative(repositoryRoot, path)),
        }),
      );
    }
  }
  for (const path of consumerMarkdownFiles(repositoryRoot)) {
    violations.push(
      ...consumerMarkdownImportViolationsFor({
        contents: readFileSync(path, "utf8"),
        relativePath: toPosixPath(relative(repositoryRoot, path)),
      }),
    );
  }
  return violations.sort();
};

const isInsideDirectory = (parent: string, child: string): boolean => {
  const relativePath = relative(parent, child);
  return (
    relativePath === "" ||
    (!relativePath.startsWith("..") && !resolve(parent, relativePath).startsWith(".."))
  );
};

const allowedSpecifiersFor = (
  packagePolicy: PrivatePackageSurface,
  relativePath: string,
): ReadonlySet<string> => {
  const allowed = new Set(packagePolicy.architecture.allowedWorkspaceSpecifiers);
  for (const override of packagePolicy.architecture.relativeOverrides) {
    if (toPosixPath(relativePath) === override.relativePath) {
      for (const specifier of override.allowedWorkspaceSpecifiers) {
        allowed.add(specifier);
      }
    }
  }
  return allowed;
};

export const packageImportViolationsForSource = ({
  contents,
  fileName,
  packagePolicy,
  packageRoot,
  path,
}: {
  readonly contents: string;
  readonly fileName: string;
  readonly packagePolicy: PrivatePackageSurface;
  readonly packageRoot: string;
  readonly path: string;
}): ReadonlyArray<string> => {
  const relativePath = toPosixPath(relative(packageRoot, path));
  const displayPath = `packages/${packagePolicy.directory}/${relativePath}`;
  const inspection = inspectTypeScriptModule({ fileName, source: contents });
  const allowed = allowedSpecifiersFor(packagePolicy, relativePath);
  const violations = inspection.moduleSpecifiers.flatMap((specifier) => {
    if (isScopeSpecifier(specifier, staleScope)) {
      return [
        `${displayPath} imports ${specifier}: stale View Server package scope; use @effect-view-server/* workspace packages.`,
      ];
    }
    if (isFacadeSpecifier(specifier)) {
      return [
        `${displayPath} imports ${specifier}: public effect-view-server facade is for consumers; internal packages must import @effect-view-server/* workspace packages.`,
      ];
    }
    if (isScopeSpecifier(specifier, currentScope)) {
      if (!workspaceSpecifierSet.has(specifier)) {
        return [
          `${displayPath} imports ${specifier}: View Server imports must use approved package exports.`,
        ];
      }
      if (!allowed.has(specifier)) {
        return [
          `${displayPath} imports ${specifier}: ${packagePolicy.architecture.message}`,
        ];
      }
    }
    return [];
  });
  for (const specifier of inspection.moduleSpecifiers.filter((candidate) =>
    candidate.startsWith("."),
  )) {
    if (!isInsideDirectory(packageRoot, resolve(dirname(path), specifier))) {
      violations.push(
        `${displayPath} imports ${specifier}: relative imports must not cross package seams.`,
      );
    }
  }
  violations.push(
    ...inspection.violations.map((violation) =>
      inspectionViolationMessage(displayPath, violation),
    ),
  );
  return violations;
};

export const collectPackageImportViolations = (
  repositoryRoot = repoRoot,
): ReadonlyArray<string> => {
  const violations: Array<string> = [];
  for (const packagePolicy of packageSurfacePolicy.packages) {
    const packageRoot = join(repositoryRoot, "packages", packagePolicy.directory);
    for (const path of sourceFiles(join(packageRoot, "src"))) {
      if (!isTestFile(path)) {
        violations.push(
          ...packageImportViolationsForSource({
            contents: readFileSync(path, "utf8"),
            fileName: path,
            packagePolicy,
            packageRoot,
            path,
          }),
        );
      }
    }
  }
  return violations.sort();
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const packageSurfaceViolationsForManifest = ({
  manifestContents,
  surface,
}: {
  readonly manifestContents: string;
  readonly surface: ExpectedPackageSurface;
}): ReadonlyArray<string> => {
  const path = `packages/${surface.directory}/package.json`;
  const parsed: unknown = JSON.parse(manifestContents);
  if (!isRecord(parsed)) {
    return [`${path} must contain an object.`];
  }
  const violations: Array<string> = [];
  if (parsed.name !== surface.packageName) {
    violations.push(`${path} name must be ${surface.packageName}, received ${String(parsed.name)}.`);
  }
  if (!isRecord(parsed.exports)) {
    violations.push(`${path} exports must be an exact object.`);
    return violations;
  }
  const expectedByKey = new Map(
    surface.manifestExports.map((manifestExport) => [manifestExport.exportKey, manifestExport]),
  );
  const actualKeys = Object.keys(parsed.exports);
  for (const exportKey of actualKeys.filter((key) => !expectedByKey.has(key)).sort()) {
    violations.push(`${path} has unapproved export ${exportKey}.`);
  }
  for (const expected of surface.manifestExports) {
    if (!(expected.exportKey in parsed.exports)) {
      violations.push(`${path} is missing export ${expected.exportKey}.`);
      continue;
    }
    const target = parsed.exports[expected.exportKey];
    if (!isRecord(target)) {
      violations.push(`${path} export ${expected.exportKey} must have exact types and import targets.`);
      continue;
    }
    for (const condition of Object.keys(target).filter(
      (condition) => condition !== "types" && condition !== "import",
    )) {
      violations.push(`${path} export ${expected.exportKey} has unapproved condition ${condition}.`);
    }
    if (target.types !== expected.typesTarget) {
      violations.push(
        `${path} export ${expected.exportKey} types target must be ${expected.typesTarget}, received ${String(target.types)}.`,
      );
    }
    if (target.import !== expected.importTarget) {
      violations.push(
        `${path} export ${expected.exportKey} import target must be ${expected.importTarget}, received ${String(target.import)}.`,
      );
    }
  }
  return violations;
};

export const packageSurfaceViolationsForViteConfig = ({
  surface,
  viteConfigContents,
}: {
  readonly surface: ExpectedPackageSurface;
  readonly viteConfigContents: string;
}): ReadonlyArray<string> => {
  const path = `packages/${surface.directory}/vite.config.ts`;
  const inspection = inspectLibraryPack({ fileName: path, source: viteConfigContents });
  const violations = inspection.violations.map(
    (violation) => {
      if (violation.kind === "duplicate-library-pack") {
        return `${path}:${violation.line}:${violation.column} declares package pack more than once.`;
      }
      if (violation.kind === "missing-library-pack") {
        return `${path}:${violation.line}:${violation.column} does not declare package pack in its default config object.`;
      }
      return violation.kind === "ambiguous-library-pack-override"
        ? `${path}:${violation.line}:${violation.column} may override package pack through a spread or computed property.`
        : `${path}:${violation.line}:${violation.column} uses a non-literal libraryPack declaration.`;
    },
  );
  const expected = new Set(surface.packEntrypoints);
  const actual = new Set(inspection.entrypoints);
  for (const duplicate of inspection.entrypoints.filter(
    (entrypoint, index) => inspection.entrypoints.indexOf(entrypoint) !== index,
  )) {
    violations.push(`${path} repeats pack entry ${duplicate}.`);
  }
  for (const entrypoint of surface.packEntrypoints.filter((entrypoint) => !actual.has(entrypoint))) {
    violations.push(`${path} is missing pack entry ${entrypoint}.`);
  }
  for (const entrypoint of Array.from(actual).filter((entrypoint) => !expected.has(entrypoint)).sort()) {
    violations.push(`${path} has unapproved pack entry ${entrypoint}.`);
  }
  return violations;
};

export const packageSourceViolationsFor = ({
  repositoryRoot,
  surface,
}: {
  readonly repositoryRoot: string;
  readonly surface: ExpectedPackageSurface;
}): ReadonlyArray<string> =>
  surface.packEntrypoints.flatMap((sourceEntrypoint) =>
    existsSync(join(repositoryRoot, "packages", surface.directory, sourceEntrypoint))
      ? []
      : [`packages/${surface.directory}/${sourceEntrypoint} is missing.`],
  );

export const facadeProjectionViolationsForSource = ({
  contents,
  fileName,
  projection,
  relativePath,
}: {
  readonly contents: string;
  readonly fileName: string;
  readonly projection: FacadeProjection;
  readonly relativePath: string;
}): ReadonlyArray<string> => {
  const inspection = inspectReexportModule({ fileName, source: contents });
  const violations = inspection.violations.map((violation) =>
    inspectionViolationMessage(relativePath, violation),
  );
  if (inspection.nonReexportStatements.length > 0) {
    violations.push(`${relativePath} must contain only package re-export declarations.`);
  }
  if (projection.reexport.kind === "all") {
    if (
      inspection.reexports.length !== 1 ||
      inspection.reexports[0]?.kind !== "all" ||
      inspection.reexports[0].moduleSpecifier !== projection.workspaceSpecifier ||
      inspection.reexports[0].typeOnly
    ) {
      violations.push(
        `${relativePath} must exclusively re-export all of ${projection.workspaceSpecifier}.`,
      );
    }
    return violations;
  }
  const namedReexports = inspection.reexports.flatMap((reexport) =>
    reexport.kind === "named" && reexport.moduleSpecifier === projection.workspaceSpecifier
      ? [reexport]
      : [],
  );
  const namedExports = namedReexports.flatMap((reexport) => reexport.exports);
  const runtimeNames = namedExports
    .filter((namedExport) => !namedExport.typeOnly)
    .map((namedExport) => namedExport.exportedName);
  const typeNames = namedExports
    .filter((namedExport) => namedExport.typeOnly)
    .map((namedExport) => namedExport.exportedName);
  if (
    inspection.reexports.length !== namedReexports.length ||
    namedExports.some((namedExport) => namedExport.sourceName !== namedExport.exportedName) ||
    runtimeNames.join("\0") !== projection.reexport.runtime.join("\0") ||
    typeNames.join("\0") !== projection.reexport.types.join("\0")
  ) {
    violations.push(
      `${relativePath} must exactly re-export the curated runtime and type symbols from ${projection.workspaceSpecifier}.`,
    );
  }
  return violations;
};

export const sourceForbiddenExportViolationsForSource = ({
  contents,
  fileName,
  policy,
  relativePath,
}: {
  readonly contents: string;
  readonly fileName: string;
  readonly policy: SourceForbiddenExportPolicy;
  readonly relativePath: string;
}): ReadonlyArray<string> => {
  const inspection = inspectTypeScriptModule({ fileName, source: contents });
  const violations = inspection.reexports.flatMap((reexport) => {
    if (reexport.kind === "all") {
      return [
        `${relativePath} wildcard re-export cannot prove the forbidden source export policy for ${policy.specifier}.`,
      ];
    }
    return reexport.kind === "namespace"
      ? [
          `${relativePath} namespace re-export cannot prove the forbidden source export policy for ${policy.specifier}.`,
        ]
      : [];
  });
  const identifiers = new Set([
    ...identifierNamesFromTypeScript({ fileName, source: contents }),
    ...inspection.reexports.flatMap((reexport) =>
      reexport.kind === "named"
        ? reexport.exports.flatMap((namedExport) => [
            namedExport.sourceName,
            namedExport.exportedName,
          ])
        : [],
    ),
  ]);
  violations.push(
    ...policy.forbidden
      .filter((forbiddenName) => identifiers.has(forbiddenName))
      .map(
        (forbiddenName) =>
          `${relativePath} exports forbidden package-surface symbol ${forbiddenName}.`,
      ),
  );
  return violations;
};

const facadeProjectionViolations = (repositoryRoot: string): ReadonlyArray<string> =>
  facadeProjections.flatMap((projection) => {
    const path = join(
      repositoryRoot,
      "packages",
      packageSurfacePolicy.facade.directory,
      projection.consumerSourceEntrypoint,
    );
    const relativePath = toPosixPath(relative(repositoryRoot, path));
    return existsSync(path)
      ? facadeProjectionViolationsForSource({
          contents: readFileSync(path, "utf8"),
          fileName: path,
          projection,
          relativePath,
        })
      : [`${relativePath} is missing.`];
  });

export const collectPackageSurfaceViolations = (
  repositoryRoot = repoRoot,
): ReadonlyArray<string> => {
  const violations: Array<string> = [];
  const packagesRoot = join(repositoryRoot, "packages");
  const expectedDirectories = new Set(expectedPackageSurfaces.map((surface) => surface.directory));
  if (existsSync(packagesRoot)) {
    for (const entry of readdirSync(packagesRoot, { withFileTypes: true })) {
      if (entry.isDirectory() && !expectedDirectories.has(entry.name)) {
        violations.push(`packages/${entry.name} is not declared by the Package Surface Policy.`);
      }
    }
  }
  for (const surface of expectedPackageSurfaces) {
    const directory = join(repositoryRoot, "packages", surface.directory);
    const manifestPath = join(directory, "package.json");
    const viteConfigPath = join(directory, "vite.config.ts");
    if (!existsSync(manifestPath)) {
      violations.push(`packages/${surface.directory}/package.json is missing.`);
    } else {
      violations.push(
        ...packageSurfaceViolationsForManifest({
          manifestContents: readFileSync(manifestPath, "utf8"),
          surface,
        }),
      );
    }
    if (!existsSync(viteConfigPath)) {
      violations.push(`packages/${surface.directory}/vite.config.ts is missing.`);
    } else {
      violations.push(
        ...packageSurfaceViolationsForViteConfig({
          surface,
          viteConfigContents: readFileSync(viteConfigPath, "utf8"),
        }),
      );
    }
    if (surface.directory !== packageSurfacePolicy.facade.directory) {
      violations.push(...packageSourceViolationsFor({ repositoryRoot, surface }));
    }
  }
  for (const policy of sourceForbiddenExportPolicies) {
    const path = join(repositoryRoot, "packages", policy.directory, policy.sourceEntrypoint);
    const relativePath = toPosixPath(relative(repositoryRoot, path));
    if (existsSync(path)) {
      violations.push(
        ...sourceForbiddenExportViolationsForSource({
          contents: readFileSync(path, "utf8"),
          fileName: path,
          policy,
          relativePath,
        }),
      );
    }
  }
  violations.push(...facadeProjectionViolations(repositoryRoot));
  return violations.sort();
};

const restrictedTopicStoreHelpers = [
  {
    name: "makeTopicStoreSubscriptionPermit",
    boundary: "Topic Store Module",
    allowed: new Set([
      "packages/column-live-view-engine/src/topic-store-state.ts",
      "packages/column-live-view-engine/src/topic-store-subscription.ts",
    ]),
  },
  {
    name: "topicStoreRawQueryMetadata",
    boundary: "Topic Store Interface",
    allowed: new Set<string>(),
  },
  {
    name: "topicStoreReadModel",
    boundary: "Topic Store Interface",
    allowed: new Set<string>(),
  },
  {
    name: "topicStoreState",
    boundary: "Topic Store Module",
    allowed: new Set([
      "packages/column-live-view-engine/src/topic-store-mutation.ts",
      "packages/column-live-view-engine/src/topic-store-state.ts",
    ]),
  },
  {
    name: "topicStoreQueryResources",
    boundary: "Topic Store Module",
    allowed: new Set([
      "packages/column-live-view-engine/src/topic-store-query.ts",
      "packages/column-live-view-engine/src/topic-store-state.ts",
    ]),
  },
] as const;

export const topicStoreHelperViolationsForFile = ({
  contents,
  path,
  repositoryRoot = repoRoot,
}: {
  readonly contents: string;
  readonly path: string;
  readonly repositoryRoot?: string;
}): ReadonlyArray<string> => {
  const relativePath = toPosixPath(relative(repositoryRoot, path));
  const identifiers = new Set(identifierNamesFromTypeScript({ fileName: path, source: contents }));
  return restrictedTopicStoreHelpers.flatMap((helper) =>
    identifiers.has(helper.name) && !helper.allowed.has(relativePath)
      ? [
          `${relativePath} references ${helper.name} outside the ${helper.boundary}.`,
        ]
      : [],
  );
};

const activeQueryLeafModules = new Set([
  "packages/column-live-view-engine/src/active-materialized-query.ts",
  "packages/column-live-view-engine/src/active-raw-query.ts",
]);

const activeQueryContractModule =
  "packages/column-live-view-engine/src/active-query-contract.ts";

const allowedActiveQueryContractImports = new Set([
  "./query-result",
  "./raw-query-plan",
  "./row-scan",
]);

const lowerTopicStoreModules = new Set([
  "packages/column-live-view-engine/src/topic-row-storage.ts",
  "packages/column-live-view-engine/src/topic-store-query-interface.ts",
]);

export const engineTypeDependencyViolationsForFile = ({
  contents,
  path,
  repositoryRoot = repoRoot,
}: {
  readonly contents: string;
  readonly path: string;
  readonly repositoryRoot?: string;
}): ReadonlyArray<string> => {
  const relativePath = toPosixPath(relative(repositoryRoot, path));
  const moduleSpecifiers = inspectTypeScriptModule({ fileName: path, source: contents }).moduleSpecifiers;
  const violations: Array<string> = [];
  if (activeQueryLeafModules.has(relativePath) && moduleSpecifiers.includes("./active-query")) {
    violations.push(
      `${relativePath} imports the Active Query facade from an Active Query leaf module.`,
    );
  }
  if (relativePath === activeQueryContractModule) {
    for (const specifier of moduleSpecifiers) {
      if (specifier.startsWith("./") && !allowedActiveQueryContractImports.has(specifier)) {
        violations.push(
          `${relativePath} imports ${specifier} above the allowed lower Active Query contracts.`,
        );
      }
    }
  }
  if (
    lowerTopicStoreModules.has(relativePath) &&
    moduleSpecifiers.some((specifier) => specifier.startsWith("./active-"))
  ) {
    violations.push(
      `${relativePath} imports Active Query state below the Topic Store Interface.`,
    );
  }
  return violations;
};

const restrictedStateExportNames: ReadonlySet<string> = new Set(
  restrictedTopicStoreHelpers.map((helper) => helper.name),
);

export const topicStoreStateExportViolationsForFile = ({
  contents,
  path,
  repositoryRoot = repoRoot,
}: {
  readonly contents: string;
  readonly path: string;
  readonly repositoryRoot?: string;
}): ReadonlyArray<string> => {
  const relativePath = toPosixPath(relative(repositoryRoot, path));
  if (relativePath.endsWith("/topic-store-state.ts")) {
    return [];
  }
  const inspection = inspectTypeScriptModule({ fileName: path, source: contents });
  const namespaceImportViolations = namespaceImportsFromTypeScript({
    fileName: path,
    source: contents,
  }).flatMap((namespaceImport) =>
    namespaceImport.moduleSpecifier === "./topic-store-state"
      ? [
          `${relativePath} namespace imports restricted Topic Store state internals.`,
        ]
      : [],
  );
  return [
    ...namespaceImportViolations,
    ...inspection.reexports.flatMap((reexport) => {
    if (reexport.kind === "all" && reexport.moduleSpecifier === "./topic-store-state") {
      return [`${relativePath} wildcard re-exports restricted Topic Store state internals.`];
    }
    if (reexport.kind === "namespace" && reexport.moduleSpecifier === "./topic-store-state") {
      return [`${relativePath} namespace re-exports restricted Topic Store state internals.`];
    }
    if (
      reexport.kind === "named" &&
      reexport.exports.some(
        (namedExport) =>
          restrictedStateExportNames.has(namedExport.sourceName) ||
          restrictedStateExportNames.has(namedExport.exportedName),
      )
    ) {
      return [`${relativePath} named re-exports restricted Topic Store state internals.`];
    }
      return [];
    }),
  ];
};

export const collectEngineSeamViolations = (
  repositoryRoot = repoRoot,
): {
  readonly helperViolations: ReadonlyArray<string>;
  readonly stateExportViolations: ReadonlyArray<string>;
} => {
  const helperViolations: Array<string> = [];
  const stateExportViolations: Array<string> = [];
  const sourceRoot = join(repositoryRoot, "packages", "column-live-view-engine", "src");
  for (const path of sourceFiles(sourceRoot)) {
    if (!isTestFile(path)) {
      const contents = readFileSync(path, "utf8");
      helperViolations.push(
        ...topicStoreHelperViolationsForFile({ contents, path, repositoryRoot }),
        ...engineTypeDependencyViolationsForFile({ contents, path, repositoryRoot }),
      );
      stateExportViolations.push(
        ...topicStoreStateExportViolationsForFile({ contents, path, repositoryRoot }),
      );
    }
  }
  return { helperViolations, stateExportViolations };
};

const neutralRuntimeCompositionModules = new Set([
  "packages/runtime/src/internal.ts",
  "packages/runtime/src/runtime-dependencies.ts",
  "packages/runtime/src/runtime-options.ts",
  "packages/runtime/src/runtime-source.ts",
]);

const runtimeAdapterPolicySpecifier =
  /^\.\/(?!runtime-)(?!tcp-publish-)(?!transport-)[^/]+-(?:health(?:-observation)?|ingress(?:-error)?|lease-manager|runtime-(?:option-contract|options|source)|source-lifecycle)$/;

const runtimeAdapterOptionModule =
  /^packages\/runtime\/src\/(.+)-runtime-(?:option-contract|options)\.ts$/;

const runtimeAdapterLeafModule =
  /^packages\/runtime\/src\/(?!tcp-publish-)(.+)-(?:ingress|lease-manager)\.ts$/;

const runtimeAdapterImplementationSpecifier = (adapter: string, specifier: string): boolean =>
  specifier === `./${adapter}-ingress` ||
  specifier === `./${adapter}-lease-manager` ||
  specifier === `./${adapter}-runtime-source`;

export const runtimeSourceSeamViolationsForFile = ({
  contents,
  path,
  repositoryRoot = repoRoot,
}: {
  readonly contents: string;
  readonly path: string;
  readonly repositoryRoot?: string;
}): ReadonlyArray<string> => {
  const relativePath = toPosixPath(relative(repositoryRoot, path));
  const moduleSpecifiers = inspectTypeScriptModule({ fileName: path, source: contents }).moduleSpecifiers;
  const violations: Array<string> = [];
  if (neutralRuntimeCompositionModules.has(relativePath)) {
    for (const specifier of moduleSpecifiers) {
      if (runtimeAdapterPolicySpecifier.test(specifier)) {
        violations.push(
          `${relativePath} imports source Adapter policy through ${specifier}.`,
        );
      }
    }
  }
  if (relativePath === "packages/runtime/src/runtime-types.ts") {
    for (const specifier of moduleSpecifiers) {
      if (specifier.startsWith("./") && !specifier.endsWith("-runtime-option-contract")) {
        violations.push(
          `${relativePath} imports non-contract runtime code through ${specifier}.`,
        );
      }
    }
  }
  const adapterOptionMatch = runtimeAdapterOptionModule.exec(relativePath);
  if (adapterOptionMatch !== null) {
    const adapter = adapterOptionMatch[1];
    for (const specifier of moduleSpecifiers) {
      if (adapter !== undefined && runtimeAdapterImplementationSpecifier(adapter, specifier)) {
        violations.push(
          `${relativePath} imports its source Adapter Implementation through ${specifier}.`,
        );
      }
    }
  }
  if (
    runtimeAdapterLeafModule.test(relativePath) &&
    moduleSpecifiers.includes("./runtime-options")
  ) {
    violations.push(
      `${relativePath} imports central runtime options instead of its Adapter-owned option module.`,
    );
  }
  return violations;
};

export const collectRuntimeSourceSeamViolations = (
  repositoryRoot = repoRoot,
): ReadonlyArray<string> => {
  const violations: Array<string> = [];
  const sourceRoot = join(repositoryRoot, "packages", "runtime", "src");
  for (const path of sourceFiles(sourceRoot)) {
    if (!isTestFile(path)) {
      violations.push(
        ...runtimeSourceSeamViolationsForFile({
          contents: readFileSync(path, "utf8"),
          path,
          repositoryRoot,
        }),
      );
    }
  }
  return violations.sort();
};

const localTypeScriptDependency = (
  path: string,
  specifier: string,
  sourcePaths: ReadonlySet<string>,
): string | undefined => {
  if (!specifier.startsWith(".")) {
    return undefined;
  }
  const base = resolve(dirname(path), specifier);
  const candidates = [
    base,
    ...sourceModuleExtensions.flatMap((extension) => [
      `${base}${extension}`,
      join(base, `index${extension}`),
    ]),
  ];
  for (const candidate of candidates) {
    if (sourcePaths.has(candidate)) {
      return candidate;
    }
  }
  return undefined;
};

export const collectRuntimeCoreDependencyCycleViolations = (
  repositoryRoot = repoRoot,
): ReadonlyArray<string> => {
  const sourceRoot = join(repositoryRoot, "packages", "runtime-core", "src");
  const paths = sourceFiles(sourceRoot).filter((path) => !isTestFile(path)).sort();
  const sourcePaths = new Set(paths);
  const graph = new Map(
    paths.map((path) => {
      const dependencies = inspectTypeScriptModule({
        fileName: path,
        source: readFileSync(path, "utf8"),
      }).moduleSpecifiers.flatMap((specifier) => {
        const dependency = localTypeScriptDependency(path, specifier, sourcePaths);
        return dependency === undefined ? [] : [dependency];
      });
      return [path, Array.from(new Set(dependencies)).sort()] as const;
    }),
  );
  const completed = new Set<string>();
  const activeIndexes = new Map<string, number>();
  const stack: Array<string> = [];
  const violations: Array<string> = [];

  const visit = (path: string): void => {
    if (completed.has(path)) {
      return;
    }
    activeIndexes.set(path, stack.length);
    stack.push(path);
    for (const dependency of graph.get(path)!) {
      const activeIndex = activeIndexes.get(dependency);
      if (activeIndex !== undefined) {
        const cycle = [...stack.slice(activeIndex), dependency].map((cyclePath) =>
          toPosixPath(relative(repositoryRoot, cyclePath)),
        );
        violations.push(`${cycle.join(" -> ")} forms a local Runtime Core dependency cycle.`);
      } else {
        visit(dependency);
      }
    }
    stack.pop();
    activeIndexes.delete(path);
    completed.add(path);
  };

  for (const path of paths) {
    visit(path);
  }
  return Array.from(new Set(violations)).sort();
};

const assertNoViolations = (heading: string, violations: ReadonlyArray<string>): void => {
  if (violations.length > 0) {
    throw new Error([heading, ...violations.map((violation) => `- ${violation}`)].join("\n"));
  }
};

export const assertNoPackageSurfaceViolations = (violations: ReadonlyArray<string>): void =>
  assertNoViolations("Package Surface Policy violations found.", violations);

export const assertNoPackageImportViolations = (violations: ReadonlyArray<string>): void =>
  assertNoViolations("Package architecture seam violations found.", violations);

export const assertNoConsumerImportViolations = (violations: ReadonlyArray<string>): void =>
  assertNoViolations("Consumer facade import violations found.", violations);

export const assertNoEngineSeamViolations = ({
  helperViolations,
  stateExportViolations,
}: {
  readonly helperViolations: ReadonlyArray<string>;
  readonly stateExportViolations: ReadonlyArray<string>;
}): void =>
  assertNoViolations("Topic Store Module seam violations found.", [
    ...helperViolations,
    ...stateExportViolations,
  ]);

export const assertNoRuntimeSourceSeamViolations = (
  violations: ReadonlyArray<string>,
): void => assertNoViolations("Runtime Source Module seam violations found.", violations);

export const assertNoRuntimeCoreDependencyCycleViolations = (
  violations: ReadonlyArray<string>,
): void => assertNoViolations("Runtime Core dependency cycles found.", violations);

assertNoPackageSurfaceViolations(collectPackageSurfaceViolations());
assertNoPackageImportViolations(collectPackageImportViolations());
assertNoConsumerImportViolations(collectConsumerImportViolations());
assertNoEngineSeamViolations(collectEngineSeamViolations());
assertNoRuntimeSourceSeamViolations(collectRuntimeSourceSeamViolations());
assertNoRuntimeCoreDependencyCycleViolations(collectRuntimeCoreDependencyCycleViolations());
