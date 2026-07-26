import {
  isSourceAdapterHandle,
  isSourceDefinition,
} from "@effect-view-server/source-adapter/internal";
import { readFile } from "node:fs/promises";
import { builtinModules } from "node:module";
import { isAbsolute, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { gzipSync } from "node:zlib";
import { Context, Data, Effect, Exit, Layer, Option, Schema } from "effect";
import ts from "typescript";
import { build, type Plugin } from "vite";
import { browserBuildChunks } from "./browser-build-output";
import { sourceAdapterConformanceDefinitionIsLinked } from "./conformance";
import { importPackageExportModule } from "./package-export-loader";

export type SourceAdapterPackageSchemaProbe = {
  readonly valid: Exit.Exit<unknown, unknown>;
  readonly invalid: Exit.Exit<unknown, unknown>;
};

export type SourceAdapterPackageTypeTestEvidence = {
  readonly compilerExitCode: number;
  readonly contractFiles: number;
  readonly positiveCases: number;
  readonly negativeCases: number;
};

export type SourceAdapterPackageLifecycleEvidence = {
  readonly lifecycle: "materialized" | "leased";
  readonly definition: unknown;
  readonly structuralLookalike: unknown;
  readonly metricsSchema: SourceAdapterPackageSchemaProbe;
  readonly rejectionLocationSchema: SourceAdapterPackageSchemaProbe;
};

export type SourceAdapterPackageContractEvidence = {
  readonly adapter: unknown;
  readonly runtimeServiceAdapter: unknown;
  readonly failureSchema: SourceAdapterPackageSchemaProbe;
  readonly lifecycles: Readonly<
    Partial<Record<"materialized" | "leased", SourceAdapterPackageLifecycleEvidence>>
  >;
  readonly typeTests: SourceAdapterPackageTypeTestEvidence;
};

export type SourceAdapterPackagePlatformEvidence = {
  readonly module: object;
  readonly emptyResources: Exit.Exit<unknown, unknown>;
  readonly missingResources: Exit.Exit<unknown, unknown>;
  readonly extraResources: Exit.Exit<unknown, unknown>;
  readonly duplicateResources: Exit.Exit<unknown, unknown>;
  readonly exactRuntimeService: Exit.Exit<unknown, unknown>;
  readonly exactConfigRuntimeService: Exit.Exit<unknown, unknown>;
};

export type SourceAdapterPackageConformanceSnapshot = {
  readonly exports: ReadonlyArray<string>;
  readonly dependencies: Readonly<Record<string, string>>;
  readonly peerDependencies: Readonly<Record<string, string>>;
  readonly devDependencies: Readonly<Record<string, string>>;
  readonly testedPeerMatrix: ReadonlyArray<Readonly<Record<string, string>>>;
  readonly contract: SourceAdapterPackageContractEvidence;
  readonly contractBrowserBundleGzipBytes: number;
  readonly contractBrowserBundleBudgetBytes: number;
  readonly forbiddenContractModules: ReadonlyArray<string>;
  readonly bundledPeerRuntimeModules: ReadonlyArray<string>;
  readonly platforms: Readonly<Record<string, SourceAdapterPackagePlatformEvidence>>;
};

export type SourceAdapterPackageValueProbe = {
  readonly valid: unknown;
  readonly invalid: unknown;
};

export type SourceAdapterPackageLifecycleProbe = {
  readonly lifecycle: "materialized" | "leased";
  readonly definitionExport: string | readonly [string, ...ReadonlyArray<string>];
  readonly definitionArguments:
    | ReadonlyArray<unknown>
    | ((contractModule: object) => ReadonlyArray<unknown>);
  readonly metrics: SourceAdapterPackageValueProbe;
  readonly rejectionLocation: SourceAdapterPackageValueProbe;
};

export type SourceAdapterPackageContractProbe = {
  readonly adapterExport: string;
  readonly serverAdapterExport: string;
  readonly failure: SourceAdapterPackageValueProbe;
  readonly lifecycles: readonly [
    SourceAdapterPackageLifecycleProbe,
    ...ReadonlyArray<SourceAdapterPackageLifecycleProbe>,
  ];
};

export type SourceAdapterPackageBrowserBundleProbe = {
  readonly budgetBytes: number;
  readonly additionalForbiddenModulePatterns?: ReadonlyArray<string>;
  readonly additionalPeerRuntimeModulePatterns?: ReadonlyArray<string>;
};

export type SourceAdapterPackagePlatformProbe = {
  readonly export: string;
  readonly viewServer: unknown;
  readonly exactResources: unknown;
  readonly emptyResources: unknown;
  readonly missingResources: unknown;
  readonly extraResources: unknown;
  readonly duplicateResources: unknown;
  readonly exactConfigResources: unknown;
};

export type SourceAdapterPackageInspectionOptions = {
  readonly name: string;
  readonly packageRoot: string;
  readonly contract: SourceAdapterPackageContractProbe;
  readonly typeTestProject: string;
  readonly browser: SourceAdapterPackageBrowserBundleProbe;
  readonly platforms: readonly [
    SourceAdapterPackagePlatformProbe,
    ...ReadonlyArray<SourceAdapterPackagePlatformProbe>,
  ];
  readonly effectPeerDependencies?: ReadonlyArray<string>;
  readonly testedPeerMatrixFile?: string;
};

export type SourceAdapterPackageConformanceIssue = {
  readonly code:
    | "MissingExport"
    | "MissingPeer"
    | "NonExactPeer"
    | "MissingMatchingDevDependency"
    | "PeerBundledAsDependency"
    | "UntestedPeerCombination"
    | "InvalidBrowserBundleBudget"
    | "BrowserBundleBudgetExceeded"
    | "ForbiddenContractModule"
    | "BundledPeerRuntime"
    | "ContractCheckFailed"
    | "MissingPlatformCheck"
    | "PlatformCheckFailed";
  readonly detail: string;
};

export type SourceAdapterContractBrowserDependency = {
  readonly importer: string;
  readonly specifier: string;
  readonly resolvedId: string;
};

export class SourceAdapterPackageInspectionError extends Data.TaggedError(
  "SourceAdapterPackageInspectionError",
)<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const inspectionError = (message: string, cause?: unknown): SourceAdapterPackageInspectionError =>
  new SourceAdapterPackageInspectionError({
    message,
    ...(cause === undefined ? {} : { cause }),
  });

type JsonObject = Readonly<Record<string, unknown>>;

const jsonObject = (value: unknown, label: string): JsonObject => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be a JSON object.`);
  }
  return Object.fromEntries(Object.keys(value).map((key) => [key, Reflect.get(value, key)]));
};

const stringRecord = (value: unknown): Readonly<Record<string, string>> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }
  const entries = Object.keys(value).flatMap((key) => {
    const entry = Reflect.get(value, key);
    return typeof entry === "string" ? [[key, entry] as const] : [];
  });
  return Object.fromEntries(entries);
};

const parseJsonFile = (path: string): Promise<unknown> =>
  readFile(path, "utf8").then((contents) => JSON.parse(contents));

const resolveWithinPackage = (packageRoot: string, path: string, label: string): string => {
  const root = resolve(packageRoot);
  const target = resolve(root, path);
  if (target === root || !target.startsWith(`${root}${sep}`)) {
    throw new TypeError(`${label} must resolve to a file inside the package root.`);
  }
  return target;
};

const exportTarget = (manifestExports: unknown, exportName: string): string => {
  const exportsObject = jsonObject(manifestExports, "Package exports");
  const candidate = exportsObject[exportName];
  if (typeof candidate === "string") {
    return candidate;
  }
  const conditions = jsonObject(candidate, `Package export ${exportName}`);
  const target = conditions["import"] ?? conditions["default"];
  if (typeof target !== "string") {
    throw new TypeError(`Package export ${exportName} requires an import or default target.`);
  }
  return target;
};

const contractExport = (
  module: object,
  path: string | readonly [string, ...ReadonlyArray<string>],
  label: string,
): Effect.Effect<unknown, SourceAdapterPackageInspectionError> =>
  Effect.try({
    try: () => {
      const segments = typeof path === "string" ? [path] : path;
      let current: unknown = module;
      for (const segment of segments) {
        if ((typeof current !== "object" || current === null) && typeof current !== "function") {
          return undefined;
        }
        current = Reflect.get(current, segment);
      }
      return current;
    },
    catch: (cause) => inspectionError(`${label} could not be inspected.`, cause),
  });

const contractProbeValue = <Value>(
  probe: Value | ((contractModule: object) => Value),
  contractModule: object,
  label: string,
): Effect.Effect<Value, SourceAdapterPackageInspectionError> =>
  Effect.try({
    try: () =>
      typeof probe === "function" ? Reflect.apply(probe, undefined, [contractModule]) : probe,
    catch: (cause) => inspectionError(`${label} failed.`, cause),
  });

const moduleStem = (path: string): string =>
  path.replace(/(?:\.d)?\.(?:[cm]?ts|tsx|[cm]?js)$/u, "");

const typeTestContractBindings = (
  file: string,
  sourceFile: ts.SourceFile,
  options: ts.CompilerOptions,
  contractTarget: string,
  checker: ts.TypeChecker,
): ReadonlySet<ts.Symbol> => {
  const bindings = new Set<ts.Symbol>();
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier)) {
      continue;
    }
    const resolved = ts.resolveModuleName(
      statement.moduleSpecifier.text,
      file,
      options,
      ts.sys,
    ).resolvedModule;
    const isContractImport =
      resolved !== undefined &&
      moduleStem(resolve(resolved.resolvedFileName)) === moduleStem(contractTarget);
    if (!isContractImport) {
      continue;
    }
    const namedBindings = statement.importClause?.namedBindings;
    if (namedBindings !== undefined) {
      if (ts.isNamespaceImport(namedBindings)) {
        bindings.add(
          Option.getOrThrow(
            Option.fromUndefinedOr(checker.getSymbolAtLocation(namedBindings.name)),
          ),
        );
      } else {
        for (const element of namedBindings.elements) {
          bindings.add(
            Option.getOrThrow(Option.fromUndefinedOr(checker.getSymbolAtLocation(element.name))),
          );
        }
      }
    }
  }
  return bindings;
};

const nodeUsesContractBinding = (
  node: ts.Node,
  bindings: ReadonlySet<ts.Symbol>,
  checker: ts.TypeChecker,
): boolean => {
  let usesBinding = false;
  const visit = (candidate: ts.Node): void => {
    if (ts.isIdentifier(candidate)) {
      const symbol = checker.getSymbolAtLocation(candidate);
      if (symbol !== undefined && bindings.has(symbol)) {
        usesBinding = true;
        return;
      }
    }
    if (!usesBinding) {
      ts.forEachChild(candidate, visit);
    }
  };
  visit(node);
  return usesBinding;
};

const countExpectTypeOfCalls = (
  sourceFile: ts.SourceFile,
  bindings: ReadonlySet<ts.Symbol>,
  checker: ts.TypeChecker,
): number => {
  let count = 0;
  const visit = (node: ts.Node): void => {
    if (
      ts.isCallExpression(node) &&
      ts.isIdentifier(node.expression) &&
      node.expression.text === "expectTypeOf" &&
      (node.arguments.some((argument) => nodeUsesContractBinding(argument, bindings, checker)) ||
        (node.typeArguments?.some((argument) =>
          nodeUsesContractBinding(argument, bindings, checker),
        ) ??
          false))
    ) {
      count += 1;
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);
  return count;
};

const countExpectedContractErrors = (
  sourceFile: ts.SourceFile,
  bindings: ReadonlySet<ts.Symbol>,
  checker: ts.TypeChecker,
): number => {
  const commentDirectives = Reflect.get(sourceFile, "commentDirectives");
  const expectedErrorPositions = Array.isArray(commentDirectives)
    ? commentDirectives.flatMap((directive) => {
        const directiveObject = jsonObject(directive, "TypeScript comment directive");
        if (directiveObject["type"] !== 0) {
          return [];
        }
        const range = jsonObject(directiveObject["range"], "TypeScript comment directive range");
        return [Number(range["pos"])];
      })
    : [];
  return expectedErrorPositions.filter((position) => {
    let enclosingCall: ts.CallExpression | undefined;
    const visit = (node: ts.Node): void => {
      if (
        ts.isCallExpression(node) &&
        node.getFullStart() <= position &&
        position < node.end &&
        (enclosingCall === undefined ||
          node.end - node.getFullStart() < enclosingCall.end - enclosingCall.getFullStart())
      ) {
        enclosingCall = node;
      }
      ts.forEachChild(node, visit);
    };
    visit(sourceFile);
    return enclosingCall !== undefined && nodeUsesContractBinding(enclosingCall, bindings, checker);
  }).length;
};

const executeSchemaProbe = (
  schema: Schema.Codec<unknown, unknown, never, never>,
  probe: SourceAdapterPackageValueProbe,
): Effect.Effect<SourceAdapterPackageSchemaProbe> =>
  Effect.all({
    valid: Effect.exit(Schema.decodeUnknownEffect(schema)(probe.valid)),
    invalid: Effect.exit(Schema.decodeUnknownEffect(schema)(probe.invalid)),
  });

const inspectTypeTests = (
  packageRoot: string,
  project: string,
  contractTarget: string,
): Effect.Effect<SourceAdapterPackageTypeTestEvidence, SourceAdapterPackageInspectionError> =>
  Effect.tryPromise({
    try: async () => {
      const projectPath = resolveWithinPackage(
        packageRoot,
        project,
        "Source Adapter type-test project",
      );
      const config = ts.readConfigFile(projectPath, (path) => ts.sys.readFile(path));
      if (config.error !== undefined) {
        throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"));
      }
      const parsed = ts.parseJsonConfigFileContent(
        config.config,
        ts.sys,
        resolve(projectPath, ".."),
        {
          noEmit: true,
        },
        projectPath,
      );
      const files = parsed.fileNames.filter((file) => file.endsWith(".test-d.ts"));
      const program = ts.createProgram(parsed.fileNames, parsed.options);
      const checker = program.getTypeChecker();
      const contractSources = files.flatMap((file) => {
        const sourceFile = Option.getOrThrow(Option.fromUndefinedOr(program.getSourceFile(file)));
        const bindings = typeTestContractBindings(
          file,
          sourceFile,
          parsed.options,
          contractTarget,
          checker,
        );
        return bindings.size === 0 ? [] : [{ sourceFile, bindings }];
      });
      return {
        compilerExitCode: ts.getPreEmitDiagnostics(program).length === 0 ? 0 : 1,
        contractFiles: contractSources.length,
        positiveCases: contractSources.reduce(
          (count, contractSource) =>
            count +
            countExpectTypeOfCalls(contractSource.sourceFile, contractSource.bindings, checker),
          0,
        ),
        negativeCases: contractSources.reduce(
          (count, contractSource) =>
            count +
            countExpectedContractErrors(
              contractSource.sourceFile,
              contractSource.bindings,
              checker,
            ),
          0,
        ),
      };
    },
    catch: (cause) => inspectionError("Source Adapter package type tests could not run.", cause),
  });

const packageSpecifierMatches = (specifier: string, packageName: string): boolean =>
  specifier === packageName || specifier.startsWith(`${packageName}/`);

export const inspectSourceAdapterContractBrowserBundle = (
  contractTarget: string,
  peerPackageNames: ReadonlyArray<string> = ["effect-view-server", "effect"],
): Effect.Effect<
  {
    readonly gzipBytes: number;
    readonly modules: ReadonlyArray<string>;
    readonly renderedModules: ReadonlyArray<string>;
    readonly dependencies: ReadonlyArray<SourceAdapterContractBrowserDependency>;
  },
  SourceAdapterPackageInspectionError
> =>
  Effect.tryPromise({
    try: async () => {
      const modules = new Set<string>();
      const dependencies = new Map<string, SourceAdapterContractBrowserDependency>();
      const captureResolvedContractGraph: Plugin = {
        name: "source-adapter-contract-resolved-graph",
        enforce: "pre",
        resolveId: async function (source, importer) {
          if (importer === undefined) {
            return null;
          }
          const resolved = await this.resolve(source, importer, { skipSelf: true });
          const resolvedId = resolved?.id ?? source;
          modules.add(source);
          modules.add(resolvedId);
          dependencies.set(`${importer}\u0000${source}`, {
            importer,
            specifier: source,
            resolvedId,
          });
          return resolved;
        },
        moduleParsed: (module) => {
          modules.add(module.id);
          for (const importedId of [...module.importedIds, ...module.dynamicallyImportedIds]) {
            modules.add(importedId);
            dependencies.set(`${module.id}\u0000${importedId}`, {
              importer: module.id,
              specifier: importedId,
              resolvedId: importedId,
            });
          }
        },
      };
      const result = await build({
        configFile: false,
        logLevel: "silent",
        plugins: [captureResolvedContractGraph],
        build: {
          minify: "esbuild",
          rollupOptions: {
            external: (source) =>
              peerPackageNames.some((packageName) => packageSpecifierMatches(source, packageName)),
            input: contractTarget,
          },
          target: "es2022",
          write: false,
        },
      });
      const chunks = browserBuildChunks(result);
      const code = chunks.map((chunk) => chunk.code).join("\n");
      return {
        gzipBytes: gzipSync(code).byteLength,
        modules: [...modules],
        renderedModules: chunks.flatMap((chunk) => Object.keys(chunk.modules)),
        dependencies: [...dependencies.values()],
      };
    },
    catch: (cause) =>
      inspectionError("Source Adapter contract browser bundle could not be built.", cause),
  });

export const classifySourceAdapterContractBrowserModules = (
  modules: ReadonlyArray<string>,
  probe: SourceAdapterPackageBrowserBundleProbe,
  policy: {
    readonly packageName?: string;
    readonly platformExports?: ReadonlyArray<string>;
    readonly forbiddenResolvedTargets?: ReadonlyArray<string>;
    readonly peerPackageNames?: ReadonlyArray<string>;
  } = {},
): {
  readonly forbiddenContractModules: ReadonlyArray<string>;
  readonly bundledPeerRuntimeModules: ReadonlyArray<string>;
} => {
  const cleanModuleId = (module: string): string => module.replace(/\?.*$/u, "");
  const normalizedBuiltins = builtinModules.map((module) =>
    module.startsWith("node:") ? module.slice("node:".length) : module,
  );
  const isNodeBuiltin = (module: string): boolean => {
    const normalized = module.startsWith("node:") ? module.slice("node:".length) : module;
    return normalizedBuiltins.some(
      (builtin) => normalized === builtin || normalized.startsWith(`${builtin}/`),
    );
  };
  const isForbiddenSdkSpecifier = (module: string): boolean =>
    module.startsWith("@effect/") ||
    module === "effect-view-server" ||
    (module.startsWith("effect-view-server/") && module !== "effect-view-server/source-adapter") ||
    module.startsWith("@effect-view-server/");
  const packageName = policy.packageName;
  const forbiddenPackageSpecifiers =
    packageName === undefined
      ? []
      : [
          `${packageName}/server`,
          ...(policy.platformExports ?? []).map(
            (platformExport) => `${packageName}${platformExport.slice(1)}`,
          ),
        ];
  const forbiddenResolvedTargets = new Set(
    (policy.forbiddenResolvedTargets ?? []).map(cleanModuleId),
  );
  const additionalForbiddenPatterns = probe.additionalForbiddenModulePatterns ?? [];
  const additionalPeerRuntimePatterns = probe.additionalPeerRuntimeModulePatterns ?? [];
  const peerPackageNames = policy.peerPackageNames ?? ["effect-view-server", "effect"];
  const belongsToBundledPeer = (module: string): boolean => {
    const clean = cleanModuleId(module);
    if (!isAbsolute(clean)) {
      return false;
    }
    const normalized = clean.replaceAll("\\", "/");
    return peerPackageNames.some((peer) => {
      const segment = `/node_modules/${peer}`;
      return normalized.includes(`${segment}/`) || normalized.endsWith(segment);
    });
  };
  return {
    forbiddenContractModules: Array.from(
      new Set(
        modules.filter((module) => {
          const clean = cleanModuleId(module);
          return (
            isNodeBuiltin(module) ||
            isForbiddenSdkSpecifier(module) ||
            forbiddenPackageSpecifiers.some((specifier) =>
              packageSpecifierMatches(module, specifier),
            ) ||
            forbiddenResolvedTargets.has(clean) ||
            additionalForbiddenPatterns.some((pattern) => module.includes(pattern))
          );
        }),
      ),
    ),
    bundledPeerRuntimeModules: Array.from(
      new Set(
        modules.filter(
          (module) =>
            belongsToBundledPeer(module) ||
            additionalPeerRuntimePatterns.some((pattern) => module.includes(pattern)),
        ),
      ),
    ),
  };
};

const invokeLayerConstructor = (
  module: object,
  constructor: "layer" | "layerConfig",
  viewServer: unknown,
  resources: unknown,
): Effect.Effect<Context.Context<unknown>, SourceAdapterPackageInspectionError> =>
  Effect.try({
    try: () => {
      const makeLayer = Reflect.get(module, constructor);
      if (typeof makeLayer !== "function") {
        throw new TypeError(`Platform export requires ${constructor}(...).`);
      }
      const candidate = Reflect.apply(makeLayer, undefined, [viewServer, resources]);
      if (!Layer.isLayer(candidate)) {
        throw new TypeError(`Platform ${constructor}(...) must return an Effect Layer.`);
      }
      return candidate;
    },
    catch: (cause) =>
      inspectionError(`Platform ${constructor}(...) could not be constructed.`, cause),
  }).pipe(
    Effect.flatMap((layer) =>
      Effect.scoped(Layer.build(layer)).pipe(
        Effect.provideContext(Context.makeUnsafe<unknown>(Context.empty().mapUnsafe)),
        Effect.mapError((cause) =>
          inspectionError(`Platform ${constructor}(...) could not be acquired.`, cause),
        ),
      ),
    ),
  );

const runtimeLifecycleMatches = (declaration: unknown, implementation: unknown): boolean => {
  if (declaration === undefined) {
    return implementation === undefined;
  }
  return (
    typeof implementation === "object" &&
    implementation !== null &&
    typeof Reflect.get(implementation, "acquire") === "function" &&
    typeof Reflect.get(implementation, "metrics") === "function" &&
    typeof Reflect.get(implementation, "retryDefault") === "function"
  );
};

const runtimeServiceMatchesAdapter = (
  service: unknown,
  adapter: {
    readonly materialized: unknown;
    readonly leased: unknown;
  },
): boolean =>
  typeof service === "object" &&
  service !== null &&
  Reflect.get(service, "adapter") === adapter &&
  runtimeLifecycleMatches(adapter.materialized, Reflect.get(service, "materialized")) &&
  runtimeLifecycleMatches(adapter.leased, Reflect.get(service, "leased"));

const inspectPlatform = (
  module: object,
  probe: SourceAdapterPackagePlatformProbe,
  adapter: import("@effect-view-server/source-adapter").SourceAdapterHandle<
    string,
    string | undefined,
    unknown,
    | import("@effect-view-server/source-adapter").SourceLifecycleDeclaration<
        unknown,
        unknown,
        unknown,
        import("@effect-view-server/source-adapter").SourceDefinitionOptionsFamily
      >
    | undefined,
    | import("@effect-view-server/source-adapter").SourceLifecycleDeclaration<
        unknown,
        unknown,
        unknown,
        import("@effect-view-server/source-adapter").SourceDefinitionOptionsFamily
      >
    | undefined
  >,
): Effect.Effect<SourceAdapterPackagePlatformEvidence, SourceAdapterPackageInspectionError> => {
  const exactRuntimeService = invokeLayerConstructor(
    module,
    "layer",
    probe.viewServer,
    probe.exactResources,
  ).pipe(
    Effect.flatMap((context) => {
      const service = Context.getOption(context, adapter.runtimeService);
      return Option.isSome(service) && runtimeServiceMatchesAdapter(service.value, adapter)
        ? Effect.succeed(service.value)
        : Effect.fail(inspectionError("Platform Layer did not provide the exact adapter service."));
    }),
  );
  const exactConfigRuntimeService = invokeLayerConstructor(
    module,
    "layerConfig",
    probe.viewServer,
    probe.exactConfigResources,
  ).pipe(
    Effect.flatMap((context) => {
      const service = Context.getOption(context, adapter.runtimeService);
      return Option.isSome(service) && runtimeServiceMatchesAdapter(service.value, adapter)
        ? Effect.succeed(service.value)
        : Effect.fail(
            inspectionError("Platform Config Layer did not provide the exact adapter service."),
          );
    }),
  );
  return Effect.all({
    module: Effect.succeed(module),
    emptyResources: Effect.exit(
      invokeLayerConstructor(module, "layer", probe.viewServer, probe.emptyResources),
    ),
    missingResources: Effect.exit(
      invokeLayerConstructor(module, "layer", probe.viewServer, probe.missingResources),
    ),
    extraResources: Effect.exit(
      invokeLayerConstructor(module, "layer", probe.viewServer, probe.extraResources),
    ),
    duplicateResources: Effect.exit(
      invokeLayerConstructor(module, "layer", probe.viewServer, probe.duplicateResources),
    ),
    exactRuntimeService: Effect.exit(exactRuntimeService),
    exactConfigRuntimeService: Effect.exit(exactConfigRuntimeService),
  });
};

export const inspectSourceAdapterPackageConformance = (
  options: SourceAdapterPackageInspectionOptions,
): Effect.Effect<SourceAdapterPackageConformanceSnapshot, SourceAdapterPackageInspectionError> =>
  Effect.gen(function* () {
    const packageRoot = resolve(options.packageRoot);
    const manifestValue = yield* Effect.tryPromise({
      try: () => parseJsonFile(resolve(packageRoot, "package.json")),
      catch: (cause) => inspectionError("Package manifest could not be read.", cause),
    });
    const manifest = yield* Effect.try({
      try: () => jsonObject(manifestValue, "Package manifest"),
      catch: (cause) => inspectionError("Package manifest is invalid.", cause),
    });
    const peerDependencies = stringRecord(manifest["peerDependencies"]);
    const browserPeerPackageNames = Array.from(
      new Set([
        "effect-view-server",
        "effect",
        ...Object.keys(peerDependencies).filter((peer) => peer.startsWith("@effect/")),
        ...(options.effectPeerDependencies ?? []),
      ]),
    );
    const manifestExports = manifest["exports"];
    const contractTarget = yield* Effect.try({
      try: () =>
        resolveWithinPackage(
          packageRoot,
          exportTarget(manifestExports, "./contract"),
          "Package export ./contract",
        ),
      catch: (cause) => inspectionError("Contract export target is invalid.", cause),
    });
    const contractModule = yield* Effect.tryPromise({
      try: () => importPackageExportModule(pathToFileURL(contractTarget)),
      catch: (cause) => inspectionError("Contract export could not be imported.", cause),
    });
    const serverTarget = yield* Effect.try({
      try: () =>
        resolveWithinPackage(
          packageRoot,
          exportTarget(manifestExports, "./server"),
          "Package export ./server",
        ),
      catch: (cause) => inspectionError("Server export could not be imported.", cause),
    });
    const serverModule = yield* Effect.tryPromise({
      try: () => importPackageExportModule(pathToFileURL(serverTarget)),
      catch: (cause) => inspectionError("Server export could not be imported.", cause),
    });
    const adapter = Reflect.get(contractModule, options.contract.adapterExport);
    if (!isSourceAdapterHandle(adapter)) {
      return yield* inspectionError("Contract adapter export is not nominal.");
    }
    const declaredLifecycles = (["materialized", "leased"] as const).filter(
      (lifecycle) => adapter[lifecycle] !== undefined,
    );
    const probedLifecycles = options.contract.lifecycles.map((probe) => probe.lifecycle);
    if (
      new Set(probedLifecycles).size !== probedLifecycles.length ||
      declaredLifecycles.length !== probedLifecycles.length ||
      declaredLifecycles.some((lifecycle) => !probedLifecycles.includes(lifecycle))
    ) {
      return yield* inspectionError(
        "Contract lifecycle probes must exactly match the adapter declarations.",
      );
    }
    const lifecycleEntries: Array<
      readonly ["materialized" | "leased", SourceAdapterPackageLifecycleEvidence]
    > = [];
    for (const lifecycleProbe of options.contract.lifecycles) {
      const makeDefinition = yield* contractExport(
        contractModule,
        lifecycleProbe.definitionExport,
        `Contract ${lifecycleProbe.lifecycle} definition export`,
      );
      if (typeof makeDefinition !== "function") {
        return yield* inspectionError(
          `Contract ${lifecycleProbe.lifecycle} definition export is not callable.`,
        );
      }
      const definition = yield* Effect.try({
        try: () =>
          Reflect.apply(
            makeDefinition,
            undefined,
            typeof lifecycleProbe.definitionArguments === "function"
              ? Reflect.apply(lifecycleProbe.definitionArguments, undefined, [contractModule])
              : lifecycleProbe.definitionArguments,
          ),
        catch: (cause) =>
          inspectionError(
            `Contract ${lifecycleProbe.lifecycle} definition construction failed.`,
            cause,
          ),
      });
      const declaration = Option.getOrThrow(
        Option.fromUndefinedOr(
          lifecycleProbe.lifecycle === "materialized" ? adapter.materialized : adapter.leased,
        ),
      );
      lifecycleEntries.push([
        lifecycleProbe.lifecycle,
        {
          lifecycle: lifecycleProbe.lifecycle,
          definition,
          structuralLookalike:
            typeof definition === "object" && definition !== null ? { ...definition } : definition,
          metricsSchema: yield* executeSchemaProbe(declaration.metrics, lifecycleProbe.metrics),
          rejectionLocationSchema: yield* executeSchemaProbe(
            declaration.rejectionLocation,
            lifecycleProbe.rejectionLocation,
          ),
        },
      ]);
    }
    const typeTests = yield* inspectTypeTests(packageRoot, options.typeTestProject, contractTarget);
    const packageName = manifest["name"];
    if (typeof packageName !== "string" || packageName.length === 0) {
      return yield* inspectionError("Package manifest name must be a non-empty string.");
    }
    const platformEntries: Array<readonly [string, SourceAdapterPackagePlatformEvidence]> = [];
    const platformTargets: Array<string> = [];
    for (const platform of options.platforms) {
      const platformTarget = yield* Effect.try({
        try: () =>
          resolveWithinPackage(
            packageRoot,
            exportTarget(manifestExports, platform.export),
            `Package export ${platform.export}`,
          ),
        catch: (cause) =>
          inspectionError(`Platform export ${platform.export} could not be imported.`, cause),
      });
      const module = yield* Effect.tryPromise({
        try: () => importPackageExportModule(pathToFileURL(platformTarget)),
        catch: (cause) =>
          inspectionError(`Platform export ${platform.export} could not be imported.`, cause),
      });
      platformTargets.push(platformTarget);
      const viewServer = yield* contractProbeValue(
        platform.viewServer,
        contractModule,
        `Platform export ${platform.export} View Server probe`,
      );
      platformEntries.push([
        platform.export,
        yield* inspectPlatform(
          module,
          {
            ...platform,
            viewServer,
          },
          adapter,
        ),
      ]);
    }
    const forbiddenBrowserTargets = [serverTarget, ...platformTargets];
    const browserBundle = yield* inspectSourceAdapterContractBrowserBundle(
      contractTarget,
      browserPeerPackageNames,
    );
    const testedPeerMatrixPath = options.testedPeerMatrixFile ?? "source-adapter-peer-matrix.json";
    const testedPeerMatrixValue = yield* Effect.tryPromise({
      try: () => parseJsonFile(resolve(packageRoot, testedPeerMatrixPath)),
      catch: (cause) => inspectionError("Tested peer matrix could not be read.", cause),
    });
    if (!Array.isArray(testedPeerMatrixValue)) {
      return yield* inspectionError("Tested peer matrix must be a JSON array.");
    }
    const testedPeerMatrix = testedPeerMatrixValue.map((entry) => stringRecord(entry));
    const browserModuleEvidence = classifySourceAdapterContractBrowserModules(
      browserBundle.modules,
      options.browser,
      {
        packageName,
        platformExports: options.platforms.map((platform) => platform.export),
        forbiddenResolvedTargets: forbiddenBrowserTargets,
        peerPackageNames: browserPeerPackageNames,
      },
    );
    return {
      exports: Object.keys(jsonObject(manifestExports, "Package exports")),
      dependencies: stringRecord(manifest["dependencies"]),
      peerDependencies,
      devDependencies: stringRecord(manifest["devDependencies"]),
      testedPeerMatrix,
      contract: {
        adapter,
        runtimeServiceAdapter: Reflect.get(serverModule, options.contract.serverAdapterExport),
        failureSchema: yield* executeSchemaProbe(adapter.failureSchema, options.contract.failure),
        lifecycles: Object.fromEntries(lifecycleEntries),
        typeTests,
      },
      contractBrowserBundleGzipBytes: browserBundle.gzipBytes,
      contractBrowserBundleBudgetBytes: options.browser.budgetBytes,
      forbiddenContractModules: browserModuleEvidence.forbiddenContractModules,
      bundledPeerRuntimeModules: browserModuleEvidence.bundledPeerRuntimeModules,
      platforms: Object.fromEntries(platformEntries),
    };
  });

const isExactVersion = (version: string): boolean =>
  version.length > 0 && !version.startsWith("workspace:") && !/[<>=~^*|\s]/u.test(version);

const allContractChecks = (
  evidence: SourceAdapterPackageContractEvidence,
): ReadonlyArray<readonly [string, boolean]> => {
  const adapter = evidence.adapter;
  const declaredLifecycles = isSourceAdapterHandle(adapter)
    ? (["materialized", "leased"] as const).filter((lifecycle) => adapter[lifecycle] !== undefined)
    : [];
  const evidencedLifecycles = (["materialized", "leased"] as const).filter(
    (lifecycle) => evidence.lifecycles[lifecycle] !== undefined,
  );
  const checks: Array<readonly [string, boolean]> = [
    [
      "runtimeServiceAdapterLinked",
      isSourceAdapterHandle(adapter) && evidence.runtimeServiceAdapter === adapter,
    ],
    [
      "lifecycleEvidenceExact",
      declaredLifecycles.length > 0 &&
        declaredLifecycles.length === evidencedLifecycles.length &&
        declaredLifecycles.every((lifecycle) => evidencedLifecycles.includes(lifecycle)),
    ],
    [
      "failureSchemaExact",
      Exit.isSuccess(evidence.failureSchema.valid) &&
        Exit.isFailure(evidence.failureSchema.invalid),
    ],
    [
      "positiveTypeInference",
      evidence.typeTests.compilerExitCode === 0 &&
        evidence.typeTests.contractFiles > 0 &&
        evidence.typeTests.positiveCases > 0,
    ],
    [
      "negativeTypeInference",
      evidence.typeTests.compilerExitCode === 0 &&
        evidence.typeTests.contractFiles > 0 &&
        evidence.typeTests.negativeCases > 0,
    ],
  ];
  for (const lifecycle of evidencedLifecycles) {
    const lifecycleEvidence = Option.getOrThrow(
      Option.fromUndefinedOr(evidence.lifecycles[lifecycle]),
    );
    checks.push(
      [
        `${lifecycle}:nominalDefinitionLinked`,
        isSourceAdapterHandle(adapter) &&
          sourceAdapterConformanceDefinitionIsLinked(
            lifecycleEvidence.definition,
            adapter,
            lifecycle,
          ),
      ],
      [
        `${lifecycle}:structuralLookalikeRejected`,
        !isSourceDefinition(lifecycleEvidence.structuralLookalike),
      ],
      [
        `${lifecycle}:metricsSchemaExact`,
        Exit.isSuccess(lifecycleEvidence.metricsSchema.valid) &&
          Exit.isFailure(lifecycleEvidence.metricsSchema.invalid),
      ],
      [
        `${lifecycle}:rejectionLocationSchemaExact`,
        Exit.isSuccess(lifecycleEvidence.rejectionLocationSchema.valid) &&
          Exit.isFailure(lifecycleEvidence.rejectionLocationSchema.invalid),
      ],
    );
  }
  return checks;
};

const allPlatformChecks = (
  evidence: SourceAdapterPackagePlatformEvidence,
): ReadonlyArray<readonly [string, boolean]> => [
  ["hasLayer", typeof Reflect.get(evidence.module, "layer") === "function"],
  ["hasLayerConfig", typeof Reflect.get(evidence.module, "layerConfig") === "function"],
  ["rejectsEmptyResources", Exit.isFailure(evidence.emptyResources)],
  ["rejectsMissingResources", Exit.isFailure(evidence.missingResources)],
  ["rejectsExtraResources", Exit.isFailure(evidence.extraResources)],
  ["rejectsDuplicateResources", Exit.isFailure(evidence.duplicateResources)],
  ["providesExactRuntimeService", Exit.isSuccess(evidence.exactRuntimeService)],
  ["providesExactConfigRuntimeService", Exit.isSuccess(evidence.exactConfigRuntimeService)],
];

export const validateSourceAdapterPackageConformance = (
  snapshot: SourceAdapterPackageConformanceSnapshot,
  options: Pick<SourceAdapterPackageInspectionOptions, "effectPeerDependencies" | "platforms">,
): ReadonlyArray<SourceAdapterPackageConformanceIssue> => {
  const issues: Array<SourceAdapterPackageConformanceIssue> = [];
  const platformExports = options.platforms.map((platform) => platform.export);
  const requiredExports = ["./contract", "./server", ...platformExports];
  for (const requiredExport of requiredExports) {
    if (!snapshot.exports.includes(requiredExport)) {
      issues.push({
        code: "MissingExport",
        detail: requiredExport,
      });
    }
  }

  const requiredPeers = ["effect-view-server", "effect", ...(options.effectPeerDependencies ?? [])];
  for (const peer of requiredPeers) {
    const peerVersion = snapshot.peerDependencies[peer];
    if (peerVersion === undefined) {
      issues.push({
        code: "MissingPeer",
        detail: peer,
      });
      continue;
    }
    if (!isExactVersion(peerVersion)) {
      issues.push({
        code: "NonExactPeer",
        detail: peer,
      });
    }
    if (snapshot.devDependencies[peer] !== peerVersion) {
      issues.push({
        code: "MissingMatchingDevDependency",
        detail: peer,
      });
    }
    if (Object.hasOwn(snapshot.dependencies, peer)) {
      issues.push({
        code: "PeerBundledAsDependency",
        detail: peer,
      });
    }
  }

  const hasTestedCombination = snapshot.testedPeerMatrix.some((combination) =>
    requiredPeers.every(
      (peer) =>
        snapshot.peerDependencies[peer] !== undefined &&
        combination[peer] === snapshot.peerDependencies[peer],
    ),
  );
  if (!hasTestedCombination) {
    issues.push({
      code: "UntestedPeerCombination",
      detail: requiredPeers.join(","),
    });
  }

  if (
    !Number.isSafeInteger(snapshot.contractBrowserBundleBudgetBytes) ||
    snapshot.contractBrowserBundleBudgetBytes <= 0 ||
    !Number.isSafeInteger(snapshot.contractBrowserBundleGzipBytes) ||
    snapshot.contractBrowserBundleGzipBytes < 0
  ) {
    issues.push({
      code: "InvalidBrowserBundleBudget",
      detail: `${snapshot.contractBrowserBundleGzipBytes}/${snapshot.contractBrowserBundleBudgetBytes}`,
    });
  } else if (snapshot.contractBrowserBundleGzipBytes > snapshot.contractBrowserBundleBudgetBytes) {
    issues.push({
      code: "BrowserBundleBudgetExceeded",
      detail: `${snapshot.contractBrowserBundleGzipBytes}/${snapshot.contractBrowserBundleBudgetBytes}`,
    });
  }

  for (const module of snapshot.forbiddenContractModules) {
    issues.push({
      code: "ForbiddenContractModule",
      detail: module,
    });
  }
  for (const module of snapshot.bundledPeerRuntimeModules) {
    issues.push({
      code: "BundledPeerRuntime",
      detail: module,
    });
  }
  for (const [check, passed] of allContractChecks(snapshot.contract)) {
    if (!passed) {
      issues.push({
        code: "ContractCheckFailed",
        detail: check,
      });
    }
  }
  for (const platformExport of platformExports) {
    const checks = snapshot.platforms[platformExport];
    if (checks === undefined) {
      issues.push({
        code: "MissingPlatformCheck",
        detail: platformExport,
      });
      continue;
    }
    for (const [check, passed] of allPlatformChecks(checks)) {
      if (!passed) {
        issues.push({
          code: "PlatformCheckFailed",
          detail: `${platformExport}:${check}`,
        });
      }
    }
  }
  return issues;
};

export const SourceAdapterPackageConformance = {
  inspect: inspectSourceAdapterPackageConformance,
  validate: validateSourceAdapterPackageConformance,
} as const;
