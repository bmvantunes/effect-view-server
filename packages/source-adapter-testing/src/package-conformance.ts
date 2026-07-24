import { expect, layer as vitestLayer } from "@effect/vitest";
import {
  isSourceAdapterHandle,
  isSourceDefinition,
} from "@effect-view-server/source-adapter/internal";
import { Context, Effect, Exit, Layer, Scope } from "effect";

export type SourceAdapterPackageSchemaProbe = {
  readonly valid: Exit.Exit<unknown, unknown>;
  readonly invalid: Exit.Exit<unknown, unknown>;
};

export type SourceAdapterPackageContractEvidence = {
  readonly adapter: unknown;
  readonly definition: unknown;
  readonly runtimeServiceAdapter: unknown;
  readonly structuralLookalike: unknown;
  readonly failureSchema: SourceAdapterPackageSchemaProbe;
  readonly metricsSchema: SourceAdapterPackageSchemaProbe;
  readonly rejectionLocationSchema: SourceAdapterPackageSchemaProbe;
  readonly typeTests: {
    readonly compilerExitCode: number;
    readonly positiveCases: number;
    readonly negativeCases: number;
  };
};

export type SourceAdapterPackagePlatformEvidence = {
  readonly module: object;
  readonly emptyResources: Exit.Exit<unknown, unknown>;
  readonly missingResources: Exit.Exit<unknown, unknown>;
  readonly extraResources: Exit.Exit<unknown, unknown>;
  readonly duplicateResources: Exit.Exit<unknown, unknown>;
  readonly exactRuntimeService: Exit.Exit<unknown, unknown>;
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

export type SourceAdapterPackageConformanceSubjectValue = {
  readonly inspect: Effect.Effect<SourceAdapterPackageConformanceSnapshot, unknown, Scope.Scope>;
};

export class SourceAdapterPackageConformanceSubject extends Context.Service<
  SourceAdapterPackageConformanceSubject,
  SourceAdapterPackageConformanceSubjectValue
>()("@effect-view-server/source-adapter-testing/PackageConformanceSubject") {}

export type SourceAdapterPackageConformanceOptions = {
  readonly name: string;
  readonly layer: Layer.Layer<SourceAdapterPackageConformanceSubject, unknown>;
  readonly platformExports: readonly [string, ...ReadonlyArray<string>];
  readonly effectPeerDependencies?: ReadonlyArray<string>;
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

const isExactVersion = (version: string): boolean =>
  version.length > 0 && !version.startsWith("workspace:") && !/[<>=~^*|\s]/u.test(version);

const allContractChecks = (
  evidence: SourceAdapterPackageContractEvidence,
): ReadonlyArray<readonly [string, boolean]> => [
  [
    "nominalDefinitionLinked",
    isSourceAdapterHandle(evidence.adapter) &&
      isSourceDefinition(evidence.definition) &&
      evidence.definition.adapter === evidence.adapter &&
      evidence.runtimeServiceAdapter === evidence.adapter,
  ],
  ["structuralLookalikeRejected", !isSourceDefinition(evidence.structuralLookalike)],
  [
    "failureSchemaExact",
    Exit.isSuccess(evidence.failureSchema.valid) && Exit.isFailure(evidence.failureSchema.invalid),
  ],
  [
    "metricsSchemaExact",
    Exit.isSuccess(evidence.metricsSchema.valid) && Exit.isFailure(evidence.metricsSchema.invalid),
  ],
  [
    "rejectionLocationSchemaExact",
    Exit.isSuccess(evidence.rejectionLocationSchema.valid) &&
      Exit.isFailure(evidence.rejectionLocationSchema.invalid),
  ],
  [
    "positiveTypeInference",
    evidence.typeTests.compilerExitCode === 0 && evidence.typeTests.positiveCases > 0,
  ],
  [
    "negativeTypeInference",
    evidence.typeTests.compilerExitCode === 0 && evidence.typeTests.negativeCases > 0,
  ],
];

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
];

export const validateSourceAdapterPackageConformance = (
  snapshot: SourceAdapterPackageConformanceSnapshot,
  options: Pick<
    SourceAdapterPackageConformanceOptions,
    "effectPeerDependencies" | "platformExports"
  >,
): ReadonlyArray<SourceAdapterPackageConformanceIssue> => {
  const issues: Array<SourceAdapterPackageConformanceIssue> = [];
  const requiredExports = ["./contract", "./server", ...options.platformExports];
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
  for (const platformExport of options.platformExports) {
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

export const registerSourceAdapterPackageConformance = (
  options: SourceAdapterPackageConformanceOptions,
): void => {
  vitestLayer(options.layer)(options.name, (it) => {
    it.effect("passes package, browser, peer, nominal, type, and platform checks", () =>
      Effect.gen(function* () {
        const subject = yield* SourceAdapterPackageConformanceSubject;
        const snapshot = yield* subject.inspect;
        expect(validateSourceAdapterPackageConformance(snapshot, options)).toStrictEqual([]);
      }),
    );
  });
};

export const SourceAdapterPackageConformance = {
  register: registerSourceAdapterPackageConformance,
  Subject: SourceAdapterPackageConformanceSubject,
  validate: validateSourceAdapterPackageConformance,
} as const;
