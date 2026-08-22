import { describe, expect, it } from "@effect/vitest";
import {
  approvedPackageSpecifiers,
  consumerPackageSpecifiers,
  expectedPackageSurfaces,
  forbiddenDeepImportSpecifiers,
  facadeProjectionFor,
  packageDistStemForSourceEntrypoint,
  packageSurfacePolicy,
  runtimeSymbolPolicies,
  sourceForbiddenExportPolicies,
  sourceForbiddenExportPolicyFor,
  sourceModuleExtensions,
  workspacePackageSpecifiers,
} from "./package-surface-policy";

describe("Package Surface Policy", () => {
  it("owns one unique inventory of private and consumer package specifiers", () => {
    expect(packageSurfacePolicy.packages).toHaveLength(15);
    expect(workspacePackageSpecifiers).toHaveLength(33);
    expect(new Set(workspacePackageSpecifiers).size).toBe(33);
    expect(consumerPackageSpecifiers).toHaveLength(25);
    expect(new Set(consumerPackageSpecifiers).size).toBe(25);
    expect(consumerPackageSpecifiers).not.toContain("effect-view-server");
    expect(
      expectedPackageSurfaces
        .filter((surface) => surface.directory !== "effect-view-server")
        .flatMap((surface) => surface.packEntrypoints),
    ).toHaveLength(33);
    expect(packageSurfacePolicy.runtimeSymbols.map((policy) => policy.workspaceSpecifier).sort()).toStrictEqual(
      [...workspacePackageSpecifiers].sort(),
    );
    expect(runtimeSymbolPolicies.map((policy) => policy.specifier).sort()).toStrictEqual(
      [...approvedPackageSpecifiers].sort(),
    );
    expect(forbiddenDeepImportSpecifiers).not.toContain(
      "@effect-view-server/column-live-view-engine/internal",
    );
    expect(forbiddenDeepImportSpecifiers).toContain(
      "@effect-view-server/column-live-view-engine/internal/src/index",
    );
    expect(sourceModuleExtensions).toStrictEqual([".ts", ".tsx", ".mts", ".cts"]);
    expect(sourceForbiddenExportPolicies).toStrictEqual([
      {
        directory: "config",
        forbidden: [
          "decodeKafkaTopicMessage",
          "KafkaDecodedTopicMessage",
          "KafkaDecodedTopicSourceMessage",
          "KafkaResolvedSourceTopicDefinition",
        ],
        sourceEntrypoint: "src/index.ts",
        specifier: "@effect-view-server/config",
      },
    ]);
  });

  it("projects exact manifests, packs, and facade mappings from the inventory", () => {
    const configSurface = expectedPackageSurfaces.find(
      (surface) => surface.directory === "config",
    );
    const facadeSurface = expectedPackageSurfaces.find(
      (surface) => surface.directory === "effect-view-server",
    );
    const kafkaContractProjection = facadeProjectionFor(
      "effect-view-server/kafka/contract",
    );
    const sourceAdapterTestingProjection = facadeProjectionFor(
      "effect-view-server/source-adapter/testing",
    );
    const valueSemanticsProjection = facadeProjectionFor(
      "effect-view-server/value-semantics",
    );

    expect(configSurface).toStrictEqual({
      directory: "config",
      packageName: "@effect-view-server/config",
      manifestExports: [
        { exportKey: ".", importTarget: "./dist/index.js", typesTarget: "./dist/index.d.ts" },
        {
          exportKey: "./runtime",
          importTarget: "./dist/runtime.js",
          typesTarget: "./dist/runtime.d.ts",
        },
        {
          exportKey: "./query",
          importTarget: "./dist/topic-contract.js",
          typesTarget: "./dist/topic-contract.d.ts",
        },
        {
          exportKey: "./health",
          importTarget: "./dist/health-contract.js",
          typesTarget: "./dist/health-contract.d.ts",
        },
        {
          exportKey: "./live-protocol",
          importTarget: "./dist/live-protocol.js",
          typesTarget: "./dist/live-protocol.d.ts",
        },
        {
          exportKey: "./internal",
          importTarget: "./dist/internal.js",
          typesTarget: "./dist/internal.d.ts",
        },
      ],
      packEntrypoints: [
        "src/index.ts",
        "src/runtime.ts",
        "src/topic-contract.ts",
        "src/health-contract.ts",
        "src/live-protocol.ts",
        "src/internal.ts",
      ],
    });
    expect(facadeSurface?.manifestExports).toHaveLength(25);
    expect(facadeSurface?.packEntrypoints).toHaveLength(25);
    expect(facadeSurface?.manifestExports).toContainEqual({
      exportKey: "./react/viewport-base-row",
      importTarget: "./dist/react-viewport-base-row.js",
      typesTarget: "./dist/react-viewport-base-row.d.ts",
    });
    expect(kafkaContractProjection.workspaceSpecifier).toBe(
      "@effect-view-server/kafka/contract",
    );
    expect(sourceAdapterTestingProjection.workspaceSpecifiers).toStrictEqual([
      "@effect-view-server/source-adapter-conformance-host",
      "@effect-view-server/source-adapter-testing",
    ]);
    expect(valueSemanticsProjection).toMatchObject({
      consumerExportKey: "./value-semantics",
      consumerSourceEntrypoint: "src/value-semantics.ts",
      workspaceSpecifier: "@effect-view-server/effect-utils/value-semantics",
    });
    expect(() => packageDistStemForSourceEntrypoint("index.js")).toThrowError(
      "Unsupported package source entrypoint: index.js",
    );
    expect(() => packageDistStemForSourceEntrypoint("src/../escape.ts")).toThrowError(
      "Unsafe package source entrypoint: src/../escape.ts",
    );
    expect(
      sourceForbiddenExportPolicyFor("@effect-view-server/config").sourceEntrypoint,
    ).toBe("src/index.ts");
    expect(() => sourceForbiddenExportPolicyFor("missing")).toThrowError(
      "Unknown source forbidden export policy directory: missing",
    );
  });
});
