import { describe, expect, it } from "@effect/vitest";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Effect, Exit, Layer, Schema } from "effect";
import {
  SourceAdapterPackageConformanceSubject,
  registerSourceAdapterPackageConformance,
  type SourceAdapterPackageConformanceSnapshot,
  validateSourceAdapterPackageConformance,
} from "./package-conformance";

const Failure = Schema.TaggedStruct("PackageFixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  observed: Schema.BigInt,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});
const adapter = SourceAdapter.make({
  identity: { name: "package-fixture", version: "1" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly stream: string }>(),
  },
  leased: undefined,
});
const definition = adapter.materializedSource({ stream: "orders" });

const schemaProbe = <Value>(
  schema: Schema.Codec<Value, unknown, never, never>,
  valid: unknown,
  invalid: unknown,
) => ({
  valid: Effect.runSync(Schema.decodeUnknownEffect(schema)(valid).pipe(Effect.exit)),
  invalid: Effect.runSync(Schema.decodeUnknownEffect(schema)(invalid).pipe(Effect.exit)),
});

const platformEvidence = {
  module: {
    layer: () => Layer.empty,
    layerConfig: () => Layer.empty,
  },
  emptyResources: Effect.runSync(Effect.fail("empty").pipe(Effect.exit)),
  missingResources: Effect.runSync(Effect.fail("missing").pipe(Effect.exit)),
  extraResources: Effect.runSync(Effect.fail("extra").pipe(Effect.exit)),
  duplicateResources: Effect.runSync(Effect.fail("duplicate").pipe(Effect.exit)),
  exactRuntimeService: Exit.succeed(adapter),
} as const;

const contractEvidence = {
  adapter,
  definition,
  runtimeServiceAdapter: adapter,
  structuralLookalike: { ...definition },
  failureSchema: schemaProbe(
    Failure,
    { _tag: "PackageFixtureFailure", message: "offline" },
    { _tag: "PackageFixtureFailure", message: 1 },
  ),
  metricsSchema: schemaProbe(Metrics, { observed: 1n }, { observed: "1" }),
  rejectionLocationSchema: schemaProbe(Location, { offset: 1n }, { offset: 1 }),
  typeTests: {
    compilerExitCode: 0,
    positiveCases: 1,
    negativeCases: 1,
  },
} as const;

const validSnapshot: SourceAdapterPackageConformanceSnapshot = {
  exports: [".", "./contract", "./server", "./node"],
  dependencies: {},
  peerDependencies: {
    "effect-view-server": "0.1.0",
    effect: "4.0.0-beta.100",
    "@effect/platform-node": "4.0.0-beta.100",
  },
  devDependencies: {
    "effect-view-server": "0.1.0",
    effect: "4.0.0-beta.100",
    "@effect/platform-node": "4.0.0-beta.100",
  },
  testedPeerMatrix: [
    {
      "effect-view-server": "0.1.0",
      effect: "4.0.0-beta.100",
      "@effect/platform-node": "4.0.0-beta.100",
    },
  ],
  contract: contractEvidence,
  contractBrowserBundleGzipBytes: 10_000,
  contractBrowserBundleBudgetBytes: 20_000,
  forbiddenContractModules: [],
  bundledPeerRuntimeModules: [],
  platforms: {
    "./node": platformEvidence,
  },
};

const options = {
  platformExports: ["./node"],
  effectPeerDependencies: ["@effect/platform-node"],
} as const;

registerSourceAdapterPackageConformance({
  name: "Source Adapter package conformance contract",
  layer: Layer.succeed(SourceAdapterPackageConformanceSubject, {
    inspect: Effect.succeed(validSnapshot),
  }),
  ...options,
});

describe("Source Adapter package conformance validation", () => {
  it("reports every failed package contract without throwing", () => {
    const issues = validateSourceAdapterPackageConformance(
      {
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
          definition: {},
          runtimeServiceAdapter: {},
          structuralLookalike: definition,
          failureSchema: {
            valid: Effect.runSync(Effect.fail("invalid").pipe(Effect.exit)),
            invalid: Exit.succeed(undefined),
          },
          metricsSchema: {
            valid: Effect.runSync(Effect.fail("invalid").pipe(Effect.exit)),
            invalid: Exit.succeed(undefined),
          },
          rejectionLocationSchema: {
            valid: Effect.runSync(Effect.fail("invalid").pipe(Effect.exit)),
            invalid: Exit.succeed(undefined),
          },
          typeTests: {
            compilerExitCode: 1,
            positiveCases: 0,
            negativeCases: 0,
          },
        },
        contractBrowserBundleGzipBytes: -1,
        contractBrowserBundleBudgetBytes: 0,
        forbiddenContractModules: ["node:net"],
        bundledPeerRuntimeModules: ["effect"],
        platforms: {},
      },
      options,
    );

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
      "ContractCheckFailed",
      "ContractCheckFailed",
      "MissingPlatformCheck",
    ]);
  });

  it("reports a validly shaped browser bundle that exceeds its budget", () => {
    expect(
      validateSourceAdapterPackageConformance(
        {
          ...validSnapshot,
          contractBrowserBundleGzipBytes: 20_001,
        },
        options,
      ),
    ).toStrictEqual([
      {
        code: "BrowserBundleBudgetExceeded",
        detail: "20001/20000",
      },
    ]);
  });

  it("reports individual failed platform constructors and resource checks", () => {
    expect(
      validateSourceAdapterPackageConformance(
        {
          ...validSnapshot,
          platforms: {
            "./node": {
              ...platformEvidence,
              module: {
                layer: () => Layer.empty,
              },
              extraResources: Exit.succeed(undefined),
            },
          },
        },
        options,
      ),
    ).toStrictEqual([
      {
        code: "PlatformCheckFailed",
        detail: "./node:hasLayerConfig",
      },
      {
        code: "PlatformCheckFailed",
        detail: "./node:rejectsExtraResources",
      },
    ]);
  });

  it("uses the mandatory core peer set when no additional Effect peers are declared", () => {
    expect(
      validateSourceAdapterPackageConformance(validSnapshot, {
        platformExports: ["./node"],
      }),
    ).toStrictEqual([]);
  });
});
