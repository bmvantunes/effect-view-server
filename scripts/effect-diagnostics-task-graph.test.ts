import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import config from "../vite.config";

describe("strict Effect diagnostics task graph", () => {
  it("builds the server declaration before runtime diagnostics", () => {
    const facadePackage = JSON.parse(
      readFileSync("packages/effect-view-server/package.json", "utf8"),
    );

    expect({
      declarationBuild: config.run?.tasks?.["build:effect-declarations"],
      runtimeDiagnostics: config.run?.tasks?.["check:effect:runtime"],
      serverDependency: facadePackage.devDependencies["@effect-view-server/server"],
    }).toStrictEqual({
      declarationBuild: {
        command: "vp run -t effect-view-server#build",
      },
      runtimeDiagnostics: {
        command:
          "effect-language-service diagnostics --project packages/runtime/tsconfig.json --format text --strict",
        dependsOn: ["build:effect-declarations"],
      },
      serverDependency: "workspace:*",
    });
  });

  it("fans independent diagnostics out through Vite tasks", () => {
    const rootPackage = JSON.parse(readFileSync("package.json", "utf8"));

    expect({
      legacyCheckScript: rootPackage.scripts["check:effect"],
      legacyExamplesScript: rootPackage.scripts["examples:check:effect"],
      rootTask: config.run?.tasks?.["check:effect"],
      examplesTask: config.run?.tasks?.["examples:check:effect"],
    }).toStrictEqual({
      legacyCheckScript: undefined,
      legacyExamplesScript: undefined,
      rootTask: {
        command:
          "effect-language-service diagnostics --project apps/example/tsconfig.json --format text --strict",
        dependsOn: [
          "build:effect-declarations",
          "check:effect:config",
          "check:effect:protocol",
          "check:effect:effect-utils",
          "check:effect:client",
          "check:effect:column-live-view-engine",
          "check:effect:runtime-core",
          "check:effect:in-memory",
          "check:effect:server",
          "check:effect:runtime",
          "check:effect:react",
          "check:effect:facade",
          "examples:check:effect",
        ],
      },
      examplesTask: {
        command:
          "effect-language-service diagnostics --project examples/in-memory-react/tsconfig.json --format text --strict",
        dependsOn: [
          "build:effect-declarations",
          "check:effect:example:kafka-react",
          "check:effect:example:grpc-leased-react",
          "check:effect:example:grpc-materialized-react",
          "check:effect:example:combined-sources-react",
          "check:effect:example:ssr-react",
          "check:effect:example:tcp-publisher-react",
        ],
      },
    });
  });
});
