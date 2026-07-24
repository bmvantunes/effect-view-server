import { describe, expect, it } from "@effect/vitest";
import {
  approvedPackageSpecifiers,
  consumerPackageSpecifiers,
  expectedPackageSurfaces,
  forbiddenDeepImportSpecifiers,
  namedFacadeProjectionFor,
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
    expect(packageSurfacePolicy.packages).toHaveLength(12);
    expect(workspacePackageSpecifiers).toHaveLength(26);
    expect(new Set(workspacePackageSpecifiers).size).toBe(26);
    expect(consumerPackageSpecifiers).toHaveLength(19);
    expect(new Set(consumerPackageSpecifiers).size).toBe(19);
    expect(consumerPackageSpecifiers).not.toContain("effect-view-server");
    expect(
      expectedPackageSurfaces
        .filter((surface) => surface.directory !== "effect-view-server")
        .flatMap((surface) => surface.packEntrypoints),
    ).toHaveLength(27);
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
    const kafkaProjection = namedFacadeProjectionFor("effect-view-server/config/kafka");

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
        { exportKey: "./kafka", importTarget: "./dist/kafka.js", typesTarget: "./dist/kafka.d.ts" },
        { exportKey: "./grpc", importTarget: "./dist/grpc.js", typesTarget: "./dist/grpc.d.ts" },
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
        "src/kafka.ts",
        "src/grpc.ts",
        "src/internal.ts",
        "src/grpc-contract.ts",
      ],
    });
    expect(facadeSurface?.manifestExports).toHaveLength(19);
    expect(facadeSurface?.packEntrypoints).toHaveLength(19);
    expect(kafkaProjection.workspaceSpecifier).toBe("@effect-view-server/config/kafka");
    expect(kafkaProjection.consumerSourceEntrypoint).toBe("src/config-kafka.ts");
    expect(kafkaProjection.reexport).toMatchObject({
      kind: "named",
      runtime: ["decodeKafkaCodec", "kafka", "kafkaErrorIsMapping"],
    });
    expect(kafkaProjection.reexport.types).toHaveLength(27);
    expect(() => namedFacadeProjectionFor("effect-view-server/client")).toThrowError(
      "Unknown named facade projection policy directory: effect-view-server/client",
    );
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
