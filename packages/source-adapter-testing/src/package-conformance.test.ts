import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import { Config, Effect, Exit } from "effect";
import { fileURLToPath } from "node:url";
import {
  classifySourceAdapterContractBrowserModules,
  inspectSourceAdapterContractBrowserBundle,
  inspectSourceAdapterPackageConformance,
  type SourceAdapterPackageInspectionOptions,
  type SourceAdapterPackageConformanceSnapshot,
  validateSourceAdapterPackageConformance,
} from "./package-conformance";

Reflect.set(
  globalThis,
  Symbol.for("@effect-view-server/source-adapter-testing/PublicSdkFixtureRuntime"),
  {
    SourceAdapter,
    SourceAdapterServer,
  },
);

const packageRoot = fileURLToPath(new URL("../test-fixtures/package-adapter", import.meta.url));
const invalidManifestRoot = fileURLToPath(
  new URL("../test-fixtures/package-invalid-manifest", import.meta.url),
);
const invalidContractRoot = fileURLToPath(
  new URL("../test-fixtures/package-invalid-contract", import.meta.url),
);
const missingNameRoot = fileURLToPath(
  new URL("../test-fixtures/package-missing-name", import.meta.url),
);
const missingServerRoot = fileURLToPath(
  new URL("../test-fixtures/package-missing-server", import.meta.url),
);
const invalidServerRoot = fileURLToPath(
  new URL("../test-fixtures/package-invalid-server", import.meta.url),
);
const contractImportFailureRoot = fileURLToPath(
  new URL("../test-fixtures/package-contract-import-failure", import.meta.url),
);
const materializedOnlyRoot = fileURLToPath(
  new URL("../test-fixtures/package-materialized-only", import.meta.url),
);
const browserLeaksRoot = fileURLToPath(
  new URL("../test-fixtures/package-browser-leaks", import.meta.url),
);
const missingPackageRoot = fileURLToPath(
  new URL("../test-fixtures/package-does-not-exist", import.meta.url),
);
const browserContractLeak = fileURLToPath(
  new URL("../test-fixtures/package-adapter/browser-contract-leak.js", import.meta.url),
);
const browserContractFailure = fileURLToPath(
  new URL("../test-fixtures/package-adapter/browser-contract-failure.js", import.meta.url),
);
const browserTreeShakenLeak = fileURLToPath(
  new URL("../test-fixtures/package-adapter/browser-contract-tree-shaken-leak.js", import.meta.url),
);
const browserTreeShakenForbidden = fileURLToPath(
  new URL("../test-fixtures/package-adapter/tree-shaken-forbidden.js", import.meta.url),
);
const browserTreeShakenNode = fileURLToPath(
  new URL("../test-fixtures/package-adapter/browser-contract-tree-shaken-node.js", import.meta.url),
);
const browserServerLeak = fileURLToPath(
  new URL("../test-fixtures/package-adapter/server.js", import.meta.url),
);
const browserLeaksContract = fileURLToPath(
  new URL("../test-fixtures/package-browser-leaks/contract.js", import.meta.url),
);
const privatePeerRuntime = fileURLToPath(
  new URL(
    "../test-fixtures/package-browser-leaks/private-peer-runtime/node_modules/effect/index.js",
    import.meta.url,
  ),
);
const forbiddenPublicServerSdk = "effect-view-server/source-adapter/server";
const forbiddenEffectPlatform = "@effect/platform-node";

const options = {
  name: "Source Adapter built-package conformance contract",
  packageRoot,
  contract: {
    adapterExport: "adapter",
    serverAdapterExport: "serverAdapter",
    failure: {
      valid: {
        _tag: "PackageFixtureFailure",
        message: "offline",
      },
      invalid: {
        _tag: "PackageFixtureFailure",
        message: 1,
      },
    },
    lifecycles: [
      {
        lifecycle: "materialized",
        definitionExport: "source",
        definitionArguments: [{ stream: "orders" }],
        metrics: {
          valid: {
            observed: 1n,
          },
          invalid: {
            observed: "1",
          },
        },
        rejectionLocation: {
          valid: {
            offset: 1n,
          },
          invalid: {
            offset: 1,
          },
        },
      },
      {
        lifecycle: "leased",
        definitionExport: "leasedSource",
        definitionArguments: [["region"], { stream: "orders" }],
        metrics: {
          valid: {
            observed: 1n,
          },
          invalid: {
            observed: "1",
          },
        },
        rejectionLocation: {
          valid: {
            offset: 1n,
          },
          invalid: {
            offset: 1,
          },
        },
      },
    ],
  },
  typeTestProject: "tsconfig.json",
  browser: {
    budgetBytes: 96 * 1024,
  },
  platforms: [
    {
      export: "./node",
      viewServer: { topics: {} },
      exactResources: {
        resources: ["client"],
      },
      emptyResources: {
        resources: [],
      },
      missingResources: {},
      extraResources: {
        resources: ["client"],
        extra: true,
      },
      duplicateResources: {
        resources: ["client", "client"],
      },
      exactConfigResources: {
        resources: Config.succeed(["client"]),
      },
    },
  ],
  effectPeerDependencies: ["@effect/platform-node"],
} satisfies SourceAdapterPackageInspectionOptions;

const materializedOnlyOptions = {
  ...options,
  name: "Materialized-only built-package conformance contract",
  packageRoot: materializedOnlyRoot,
  contract: {
    ...options.contract,
    failure: {
      valid: {
        _tag: "PackageMaterializedOnlyFailure",
        message: "offline",
      },
      invalid: {
        _tag: "PackageMaterializedOnlyFailure",
        message: 1,
      },
    },
    lifecycles: [options.contract.lifecycles[0]],
  },
} satisfies SourceAdapterPackageInspectionOptions;

it.effect(options.name, () =>
  Effect.gen(function* () {
    const snapshot = yield* inspectSourceAdapterPackageConformance(options);
    expect(validateSourceAdapterPackageConformance(snapshot, options)).toStrictEqual([]);
  }),
);

it.effect(materializedOnlyOptions.name, () =>
  Effect.gen(function* () {
    const snapshot = yield* inspectSourceAdapterPackageConformance(materializedOnlyOptions);
    expect(
      validateSourceAdapterPackageConformance(snapshot, materializedOnlyOptions),
    ).toStrictEqual([]);
    const extraLifecycleService = yield* inspectSourceAdapterPackageConformance({
      ...materializedOnlyOptions,
      platforms: [
        {
          ...materializedOnlyOptions.platforms[0],
          export: "./extra-node",
        },
      ],
    });
    const extraLifecyclePlatform = extraLifecycleService.platforms["./extra-node"];
    expect(extraLifecyclePlatform?.exactRuntimeService._tag).toBe("Failure");
    expect(extraLifecyclePlatform?.exactConfigRuntimeService._tag).toBe("Failure");
  }),
);

const failed = Effect.runSync(Effect.fail("invalid").pipe(Effect.exit));
const succeeded = Exit.succeed(undefined);
const invalidSnapshot: SourceAdapterPackageConformanceSnapshot = {
  exports: [],
  dependencies: {
    effect: "bundled",
  },
  peerDependencies: {
    effect: "^4.0.0-beta.100",
  },
  devDependencies: {
    effect: "wrong",
  },
  testedPeerMatrix: [],
  contract: {
    adapter: {},
    runtimeServiceAdapter: {},
    failureSchema: {
      valid: failed,
      invalid: succeeded,
    },
    lifecycles: {},
    typeTests: {
      compilerExitCode: 1,
      contractFiles: 0,
      positiveCases: 0,
      negativeCases: 0,
    },
  },
  contractBrowserBundleGzipBytes: -1,
  contractBrowserBundleBudgetBytes: 0,
  forbiddenContractModules: ["node:net"],
  bundledPeerRuntimeModules: ["effect"],
  platforms: {},
};

describe("Source Adapter package conformance validation", () => {
  it.effect("returns typed inspection errors for malformed package evidence", () =>
    Effect.gen(function* () {
      const cases = [
        {
          options: { ...options, packageRoot: missingPackageRoot },
          message: "Package manifest could not be read.",
        },
        {
          options: { ...options, packageRoot: invalidManifestRoot },
          message: "Package manifest is invalid.",
        },
        {
          options: { ...options, packageRoot: invalidContractRoot },
          message: "Contract export target is invalid.",
        },
        {
          options: { ...options, packageRoot: missingServerRoot },
          message: "Server export could not be imported.",
        },
        {
          options: { ...options, packageRoot: invalidServerRoot },
          message: "Server export could not be imported.",
        },
        {
          options: { ...options, packageRoot: contractImportFailureRoot },
          message: "Contract export could not be imported.",
        },
        {
          options: { ...options, packageRoot: missingNameRoot },
          message: "Package manifest name must be a non-empty string.",
        },
        {
          options: {
            ...options,
            contract: {
              ...options.contract,
              adapterExport: "missingAdapter",
            },
          },
          message: "Contract adapter export is not nominal.",
        },
        {
          options: {
            ...options,
            contract: {
              ...options.contract,
              lifecycles: [
                {
                  ...options.contract.lifecycles[0],
                  definitionExport: "missingDefinition",
                },
                options.contract.lifecycles[1],
              ],
            },
          },
          message: "Contract materialized definition export is not callable.",
        },
        {
          options: {
            ...options,
            contract: {
              ...options.contract,
              lifecycles: [
                {
                  ...options.contract.lifecycles[0],
                  definitionExport: "throwingSource",
                },
                options.contract.lifecycles[1],
              ],
            },
          },
          message: "Contract materialized definition construction failed.",
        },
        {
          options: {
            ...options,
            contract: {
              ...options.contract,
              lifecycles: [
                {
                  ...options.contract.lifecycles[0],
                  lifecycle: "leased",
                },
              ],
            },
          },
          message: "Contract lifecycle probes must exactly match the adapter declarations.",
        },
        {
          options: {
            ...options,
            typeTestProject: "invalid-types/broken.txt",
          },
          message: "Source Adapter package type tests could not run.",
        },
        {
          options: {
            ...options,
            typeTestProject: "../outside-package.json",
          },
          message: "Source Adapter package type tests could not run.",
        },
        {
          options: {
            ...options,
            typeTestProject: ".",
          },
          message: "Source Adapter package type tests could not run.",
        },
        {
          options: {
            ...options,
            platforms: [
              {
                ...options.platforms[0],
                export: "./missing-platform",
              },
            ],
          },
          message: "Platform export ./missing-platform could not be imported.",
        },
        {
          options: {
            ...options,
            platforms: [
              {
                ...options.platforms[0],
                export: "./import-failure-node",
              },
            ],
          },
          message: "Platform export ./import-failure-node could not be imported.",
        },
        {
          options: {
            ...options,
            testedPeerMatrixFile: "missing-peer-matrix.json",
          },
          message: "Tested peer matrix could not be read.",
        },
        {
          options: {
            ...options,
            testedPeerMatrixFile: "invalid-peer-matrix.json",
          },
          message: "Tested peer matrix must be a JSON array.",
        },
      ] as const;

      for (const fixtureCase of cases) {
        const failure = yield* Effect.flip(
          inspectSourceAdapterPackageConformance(fixtureCase.options),
        );
        expect(failure._tag).toBe("SourceAdapterPackageInspectionError");
        expect(failure.message).toBe(fixtureCase.message);
      }
    }),
  );

  it.effect("records compiler failures and primitive definition evidence", () =>
    Effect.gen(function* () {
      const compilerFailure = yield* inspectSourceAdapterPackageConformance({
        ...options,
        typeTestProject: "invalid-types/tsconfig.json",
      });
      expect(compilerFailure.contract.typeTests).toStrictEqual({
        compilerExitCode: 1,
        contractFiles: 0,
        positiveCases: 0,
        negativeCases: 0,
      });

      const compilerRootEvidence = yield* inspectSourceAdapterPackageConformance(options);
      expect(compilerRootEvidence.contract.typeTests).toStrictEqual({
        compilerExitCode: 0,
        contractFiles: 1,
        positiveCases: 3,
        negativeCases: 3,
      });

      const unrelatedEvidence = yield* inspectSourceAdapterPackageConformance({
        ...options,
        typeTestProject: "unrelated-types/tsconfig.json",
      });
      expect(unrelatedEvidence.contract.typeTests).toStrictEqual({
        compilerExitCode: 0,
        contractFiles: 0,
        positiveCases: 0,
        negativeCases: 0,
      });
      expect(
        validateSourceAdapterPackageConformance(unrelatedEvidence, options)
          .filter((issue) => issue.code === "ContractCheckFailed")
          .map((issue) => issue.detail),
      ).toStrictEqual(["positiveTypeInference", "negativeTypeInference"]);

      const unlinkedEvidence = yield* inspectSourceAdapterPackageConformance({
        ...options,
        typeTestProject: "unlinked-types/tsconfig.json",
      });
      expect(unlinkedEvidence.contract.typeTests).toStrictEqual({
        compilerExitCode: 0,
        contractFiles: 1,
        positiveCases: 0,
        negativeCases: 0,
      });
      expect(
        validateSourceAdapterPackageConformance(unlinkedEvidence, options)
          .filter((issue) => issue.code === "ContractCheckFailed")
          .map((issue) => issue.detail),
      ).toStrictEqual(["positiveTypeInference", "negativeTypeInference"]);

      const shadowedEvidence = yield* inspectSourceAdapterPackageConformance({
        ...options,
        typeTestProject: "shadowed-types/tsconfig.json",
      });
      expect(shadowedEvidence.contract.typeTests).toStrictEqual({
        compilerExitCode: 0,
        contractFiles: 2,
        positiveCases: 0,
        negativeCases: 0,
      });
      expect(
        validateSourceAdapterPackageConformance(shadowedEvidence, options)
          .filter((issue) => issue.code === "ContractCheckFailed")
          .map((issue) => issue.detail),
      ).toStrictEqual(["positiveTypeInference", "negativeTypeInference"]);

      const primitiveDefinition = yield* inspectSourceAdapterPackageConformance({
        ...options,
        contract: {
          ...options.contract,
          lifecycles: [
            {
              ...options.contract.lifecycles[0],
              definitionExport: "primitiveSource",
            },
            options.contract.lifecycles[1],
          ],
        },
      });
      expect(primitiveDefinition.contract.lifecycles.materialized?.definition).toBe(
        "not-a-definition",
      );
      expect(primitiveDefinition.contract.lifecycles.materialized?.structuralLookalike).toBe(
        "not-a-definition",
      );

      const {
        effectPeerDependencies: configuredEffectPeerDependencies,
        ...optionsWithoutConfiguredEffectPeers
      } = options;
      expect(configuredEffectPeerDependencies).toStrictEqual(["@effect/platform-node"]);
      const defaultPeerEvidence = yield* inspectSourceAdapterPackageConformance(
        optionsWithoutConfiguredEffectPeers,
      );
      expect(defaultPeerEvidence.bundledPeerRuntimeModules).toStrictEqual([]);
    }),
  );

  it.effect("detects server modules imported by the exact browser contract entry", () =>
    Effect.gen(function* () {
      const bundle = yield* inspectSourceAdapterContractBrowserBundle(browserContractLeak);
      const bundledPeerRuntime = "/private-peer-runtime/effect.js";
      expect(
        classifySourceAdapterContractBrowserModules([...bundle.modules, bundledPeerRuntime], {
          ...options.browser,
          additionalForbiddenModulePatterns: [browserServerLeak],
          additionalPeerRuntimeModulePatterns: ["/private-peer-runtime/"],
        }),
      ).toStrictEqual({
        forbiddenContractModules: [browserServerLeak],
        bundledPeerRuntimeModules: [bundledPeerRuntime],
      });
    }),
  );

  it.effect("classifies forbidden modules from the resolved graph before tree shaking", () =>
    Effect.gen(function* () {
      const bundle = yield* inspectSourceAdapterContractBrowserBundle(browserTreeShakenLeak);
      expect(bundle.modules).toContain(browserTreeShakenForbidden);
      expect(bundle.renderedModules).not.toContain(browserTreeShakenForbidden);
      expect(
        classifySourceAdapterContractBrowserModules(bundle.modules, {
          ...options.browser,
          additionalForbiddenModulePatterns: [browserTreeShakenForbidden],
        }).forbiddenContractModules,
      ).toStrictEqual([browserTreeShakenForbidden]);
    }),
  );

  it.effect("retains original built-in specifiers before browser externalization", () =>
    Effect.gen(function* () {
      const bundle = yield* inspectSourceAdapterContractBrowserBundle(browserTreeShakenNode);
      expect(bundle.modules).toContain("node:net");
      expect(
        classifySourceAdapterContractBrowserModules(bundle.modules, options.browser)
          .forbiddenContractModules,
      ).toContain("node:net");
    }),
  );

  it("enforces the built-in SDK policy while preserving the portable contract seam", () => {
    expect(
      classifySourceAdapterContractBrowserModules(
        [
          "effect-view-server",
          "effect-view-server/source-adapter",
          "@effect-view-server/source-adapter",
          "@effect-view-server/server",
          "@fixture/adapter/server",
        ],
        options.browser,
        {
          packageName: "@fixture/adapter",
        },
      ).forbiddenContractModules,
    ).toStrictEqual([
      "effect-view-server",
      "@effect-view-server/source-adapter",
      "@effect-view-server/server",
      "@fixture/adapter/server",
    ]);
  });

  it.effect("enforces kit-owned browser purity through full package inspection", () =>
    Effect.gen(function* () {
      const bundle = yield* inspectSourceAdapterContractBrowserBundle(browserLeaksContract, [
        "effect-view-server",
        "effect",
        "@effect/platform-node",
      ]);
      expect(bundle.modules).toContain("node:net");
      expect(bundle.modules).toContain(privatePeerRuntime);
      expect(bundle.modules).toContain(forbiddenPublicServerSdk);
      expect(bundle.modules).toContain(forbiddenEffectPlatform);
      expect(bundle.renderedModules).not.toContain("node:net");
      expect(bundle.renderedModules).not.toContain(privatePeerRuntime);
      expect(bundle.renderedModules).not.toContain(forbiddenPublicServerSdk);
      expect(bundle.renderedModules).not.toContain(forbiddenEffectPlatform);

      const leakOptions = {
        ...options,
        name: "Source Adapter mandatory browser leak fixture",
        packageRoot: browserLeaksRoot,
        browser: {
          budgetBytes: options.browser.budgetBytes,
          additionalForbiddenModulePatterns: ["never-match-forbidden"],
          additionalPeerRuntimeModulePatterns: ["never-match-peer"],
        },
      } satisfies SourceAdapterPackageInspectionOptions;
      const snapshot = yield* inspectSourceAdapterPackageConformance(leakOptions);
      expect(snapshot.forbiddenContractModules).toStrictEqual([
        "node:net",
        forbiddenPublicServerSdk,
        forbiddenEffectPlatform,
      ]);
      expect(snapshot.bundledPeerRuntimeModules).toStrictEqual([privatePeerRuntime]);
      expect(
        validateSourceAdapterPackageConformance(snapshot, leakOptions)
          .filter(
            (issue) =>
              issue.code === "ForbiddenContractModule" || issue.code === "BundledPeerRuntime",
          )
          .map((issue) => ({ code: issue.code, detail: issue.detail })),
      ).toStrictEqual([
        {
          code: "ForbiddenContractModule",
          detail: "node:net",
        },
        {
          code: "ForbiddenContractModule",
          detail: forbiddenPublicServerSdk,
        },
        {
          code: "ForbiddenContractModule",
          detail: forbiddenEffectPlatform,
        },
        {
          code: "BundledPeerRuntime",
          detail: privatePeerRuntime,
        },
      ]);
    }),
  );

  it.effect("reports browser contract build failures as typed inspection errors", () =>
    Effect.gen(function* () {
      const failure = yield* Effect.flip(
        inspectSourceAdapterContractBrowserBundle(browserContractFailure),
      );
      expect(failure._tag).toBe("SourceAdapterPackageInspectionError");
      expect(failure.message).toBe("Source Adapter contract browser bundle could not be built.");
    }),
  );

  it.effect("captures constructor, acquisition, and exact-service platform failures", () =>
    Effect.gen(function* () {
      const inspectPlatform = (platformExport: string) =>
        inspectSourceAdapterPackageConformance({
          ...options,
          platforms: [
            {
              ...options.platforms[0],
              export: platformExport,
            },
          ],
        });

      const invalidConstructor = yield* inspectPlatform("./bad-node");
      const invalidConstructorPlatform = invalidConstructor.platforms["./bad-node"];
      expect(invalidConstructorPlatform?.emptyResources._tag).toBe("Failure");
      expect(invalidConstructorPlatform?.missingResources._tag).toBe("Failure");
      expect(invalidConstructorPlatform?.extraResources._tag).toBe("Failure");
      expect(invalidConstructorPlatform?.duplicateResources._tag).toBe("Failure");
      expect(invalidConstructorPlatform?.exactRuntimeService._tag).toBe("Failure");
      expect(invalidConstructorPlatform?.exactConfigRuntimeService._tag).toBe("Failure");

      const acquisitionFailure = yield* inspectPlatform("./failing-node");
      const acquisitionFailurePlatform = acquisitionFailure.platforms["./failing-node"];
      expect(acquisitionFailurePlatform?.exactRuntimeService._tag).toBe("Failure");
      expect(acquisitionFailurePlatform?.exactConfigRuntimeService._tag).toBe("Failure");

      const wrongService = yield* inspectPlatform("./wrong-node");
      const wrongServicePlatform = wrongService.platforms["./wrong-node"];
      expect(wrongServicePlatform?.exactRuntimeService._tag).toBe("Failure");
      expect(wrongServicePlatform?.exactConfigRuntimeService._tag).toBe("Failure");
    }),
  );

  it("reports every failed package contract without throwing", () => {
    const issues = validateSourceAdapterPackageConformance(invalidSnapshot, options);

    expect(issues.map((issue) => issue.code)).toStrictEqual([
      "MissingExport",
      "MissingExport",
      "MissingExport",
      "MissingPeer",
      "NonExactPeer",
      "MissingMatchingDevDependency",
      "PeerBundledAsDependency",
      "MissingPeer",
      "UntestedPeerCombination",
      "InvalidBrowserBundleBudget",
      "ForbiddenContractModule",
      "BundledPeerRuntime",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "MissingPlatformCheck",
    ]);
  });

  it("reports an over-budget bundle and each failed real platform outcome", () => {
    const issues = validateSourceAdapterPackageConformance(
      {
        ...invalidSnapshot,
        exports: [".", "./contract", "./server", "./node"],
        dependencies: {},
        peerDependencies: {
          "effect-view-server": "0.0.6",
          effect: "4.0.0-beta.100",
          "@effect/platform-node": "4.0.0-beta.100",
        },
        devDependencies: {
          "effect-view-server": "0.0.6",
          effect: "4.0.0-beta.100",
          "@effect/platform-node": "4.0.0-beta.100",
        },
        testedPeerMatrix: [
          {
            "effect-view-server": "0.0.6",
            effect: "4.0.0-beta.100",
            "@effect/platform-node": "4.0.0-beta.100",
          },
        ],
        contractBrowserBundleGzipBytes: 2,
        contractBrowserBundleBudgetBytes: 1,
        forbiddenContractModules: [],
        bundledPeerRuntimeModules: [],
        platforms: {
          "./node": {
            module: {},
            emptyResources: succeeded,
            missingResources: succeeded,
            extraResources: succeeded,
            duplicateResources: succeeded,
            exactRuntimeService: failed,
            exactConfigRuntimeService: failed,
          },
        },
      },
      options,
    );

    expect(issues.map((issue) => issue.code)).toStrictEqual([
      "BrowserBundleBudgetExceeded",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "ContractCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
      "PlatformCheckFailed",
    ]);
  });

  it("uses only the mandatory peers when no platform peers are configured", () => {
    const issues = validateSourceAdapterPackageConformance(invalidSnapshot, {
      platforms: options.platforms,
    });

    expect(issues.some((issue) => issue.detail === "@effect/platform-node")).toBe(false);
  });
});
