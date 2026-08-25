import { describe, expect, it } from "@effect/vitest";
import {
  antipattern,
  correctness,
  effectNative,
  style,
} from "@effect/tsgo/oxlint-presets";
import { readFileSync } from "node:fs";
import facadeConfig from "../packages/effect-view-server/vite.config";
import kafkaConfig from "../packages/kafka/vite.config";
import config from "../vite.config";

const effectPresets = [correctness, antipattern, effectNative, style];
const repositoryRoot = new URL("../", import.meta.url);
const readRepositoryJson = (relativePath: string) =>
  JSON.parse(readFileSync(new URL(relativePath, repositoryRoot), "utf8"));
const tasks = config.run?.tasks ?? {};
const taskEntriesWithPrefix = (prefix: string) =>
  Object.entries(tasks).filter(([name]) => name.startsWith(prefix));
const taskCommand = (task: (typeof tasks)[string]) =>
  typeof task === "object" && !Array.isArray(task) ? task.command : task;
const taskCwd = (task: (typeof tasks)[string]) =>
  typeof task === "object" && !Array.isArray(task) ? task.cwd : undefined;
const effectRuleNames = effectPresets
  .flatMap((preset) => Object.keys(preset.rules ?? {}))
  .sort();
const configuredEffectRules = Object.fromEntries(
  Object.entries(config.lint?.rules ?? {})
    .filter(([ruleName]) => ruleName.startsWith("effecttsgo/"))
    .sort(([left], [right]) => left.localeCompare(right)),
);

describe("aggressive Effect Oxlint integration", () => {
  it("enables every Effect category with an explicit zero-debt severity", () => {
    const configuredSeverities = Object.values(configuredEffectRules).reduce(
      (counts: Record<string, number>, severity) => ({
        ...counts,
        [String(severity)]: (counts[String(severity)] ?? 0) + 1,
      }),
      {},
    );

    expect({
      configuredRuleNames: Object.keys(configuredEffectRules),
      configuredPresets: config.lint?.extends,
      configuredSeverities,
      lintOptions: config.lint?.options,
    }).toStrictEqual({
      configuredRuleNames: effectRuleNames,
      configuredPresets: effectPresets,
      configuredSeverities: { error: 65, off: 30 },
      lintOptions: { denyWarnings: true, typeAware: true, typeCheck: true },
    });
  });

  it("uses Oxlint as the only command-line Effect diagnostics path", () => {
    const rootPackage = readRepositoryJson("package.json");
    const rootTsconfig = readRepositoryJson("tsconfig.json");
    const effectPlugin = rootTsconfig.compilerOptions.plugins.find(
      (plugin: { name?: string }) => plugin.name === "@effect/language-service",
    );
    const auditTask = tasks["audit:effect"];
    const auditCommand =
      typeof auditTask === "object" && !Array.isArray(auditTask) ? auditTask.command : auditTask;

    expect({
      auditCommandPrefix: typeof auditCommand === "string" ? auditCommand.split(" ").slice(0, 2) : [],
      auditedDeferredRules:
        typeof auditCommand === "string" ? auditCommand.split(" ").slice(2).sort() : [],
      commandLineDiagnostics: Object.keys(tasks).filter((name) => name.startsWith("check:effect")),
      editorDiagnostics: effectPlugin?.diagnostics,
      examplesUseStandaloneDiagnostics: rootPackage.scripts["examples:test"].includes("check:effect"),
      explicitTsgolintDependency: rootPackage.devDependencies["oxlint-tsgolint"],
      installPatch: rootPackage.scripts.prepare,
      readyUsesOxlintGate: rootPackage.scripts.ready.includes("vp check"),
      readyUsesReactCompilerGate: rootPackage.scripts.ready.includes(
        "vp run -w check:react-compiler",
      ),
      readyUsesStandaloneDiagnostics: rootPackage.scripts.ready.includes("check:effect"),
    }).toStrictEqual({
      auditCommandPrefix: ["vp", "lint"],
      auditedDeferredRules: Object.entries(configuredEffectRules)
        .filter(([, severity]) => severity === "off")
        .map(([ruleName]) => `--warn=${ruleName}`)
        .sort(),
      commandLineDiagnostics: [],
      editorDiagnostics: false,
      examplesUseStandaloneDiagnostics: false,
      explicitTsgolintDependency: "catalog:",
      installPatch: "vp config && effect-tsgo patch --typescript --oxlint",
      readyUsesOxlintGate: true,
      readyUsesReactCompilerGate: true,
      readyUsesStandaloneDiagnostics: false,
    });
  });

  it("preserves the build graph used by repository checks", () => {
    const facadePackage = readRepositoryJson("packages/effect-view-server/package.json");
    const declarationBuild = tasks["build:effect-declarations"];
    const declarationBuildTasks = taskEntriesWithPrefix("build:effect-declarations:");
    const allDeclarationBuilds = [
      ...declarationBuildTasks,
      ["build:effect-declarations", declarationBuild],
    ];
    const buildDirectories = allDeclarationBuilds.map(([, task]) => taskCwd(task));

    expect({
      buildCommands: allDeclarationBuilds.map(([, task]) => taskCommand(task)),
      buildDirectories,
      declarationBuild,
      declarationBuildTaskNames: declarationBuildTasks.map(([name]) => name),
      facadeBuild: facadeConfig.run?.tasks?.build,
      kafkaBuild: kafkaConfig.run?.tasks?.build,
      kafkaTest: kafkaConfig.run?.tasks?.test,
      nestedBuildRuns: allDeclarationBuilds
        .map(([, task]) => taskCommand(task))
        .filter((command) => typeof command === "string" && command.includes("vp run")),
      runtimeDeclarationBuild: tasks["build:effect-declarations:runtime"],
      serverDeclarationBuild: tasks["build:effect-declarations:server"],
      serverDependency: facadePackage.devDependencies["@effect-view-server/server"],
      uniqueBuildDirectories: [...new Set(buildDirectories)],
    }).toStrictEqual({
      buildCommands: Array.from({ length: 16 }, () => "vp pack"),
      buildDirectories: [
        "packages/effect-utils",
        "packages/source-adapter",
        "packages/grpc",
        "packages/source-adapter-testing",
        "packages/kafka",
        "packages/config",
        "packages/column-live-view-engine",
        "packages/protocol",
        "packages/client",
        "packages/runtime-core",
        "packages/source-adapter-conformance-host",
        "packages/server",
        "packages/in-memory",
        "packages/runtime",
        "packages/react",
        "packages/effect-view-server",
      ],
      declarationBuild: {
        command: "vp pack",
        cwd: "packages/effect-view-server",
        dependsOn: [
          "build:effect-declarations:effect-utils",
          "build:effect-declarations:source-adapter",
          "build:effect-declarations:grpc",
          "build:effect-declarations:source-adapter-testing",
          "build:effect-declarations:kafka",
          "build:effect-declarations:config",
          "build:effect-declarations:column-live-view-engine",
          "build:effect-declarations:protocol",
          "build:effect-declarations:client",
          "build:effect-declarations:runtime-core",
          "build:effect-declarations:source-adapter-conformance-host",
          "build:effect-declarations:server",
          "build:effect-declarations:in-memory",
          "build:effect-declarations:runtime",
          "build:effect-declarations:react",
        ],
      },
      declarationBuildTaskNames: [
        "build:effect-declarations:effect-utils",
        "build:effect-declarations:source-adapter",
        "build:effect-declarations:grpc",
        "build:effect-declarations:source-adapter-testing",
        "build:effect-declarations:kafka",
        "build:effect-declarations:config",
        "build:effect-declarations:column-live-view-engine",
        "build:effect-declarations:protocol",
        "build:effect-declarations:client",
        "build:effect-declarations:runtime-core",
        "build:effect-declarations:source-adapter-conformance-host",
        "build:effect-declarations:server",
        "build:effect-declarations:in-memory",
        "build:effect-declarations:runtime",
        "build:effect-declarations:react",
      ],
      facadeBuild: {
        command: "vp pack",
        dependsOn: [
          "@effect-view-server/client#build",
          "@effect-view-server/column-live-view-engine#build",
          "@effect-view-server/config#build",
          "@effect-view-server/effect-utils#build",
          "@effect-view-server/grpc#build",
          "@effect-view-server/in-memory#build",
          "@effect-view-server/kafka#build",
          "@effect-view-server/react#build",
          "@effect-view-server/runtime#build",
          "@effect-view-server/server#build",
          "@effect-view-server/source-adapter#build",
          "@effect-view-server/source-adapter-conformance-host#build",
          "@effect-view-server/source-adapter-testing#build",
        ],
      },
      kafkaBuild: {
        command: "vp pack",
        dependsOn: ["@effect-view-server/source-adapter#build"],
      },
      kafkaTest: {
        command: "node ../../scripts/test-kafka-adapter.mjs",
        dependsOn: [
          "@effect-view-server/kafka#build",
          "@effect-view-server/source-adapter-conformance-host#build",
        ],
      },
      nestedBuildRuns: [],
      runtimeDeclarationBuild: {
        command: "vp pack",
        cwd: "packages/runtime",
        dependsOn: [
          "build:effect-declarations:client",
          "build:effect-declarations:config",
          "build:effect-declarations:effect-utils",
          "build:effect-declarations:runtime-core",
          "build:effect-declarations:server",
          "build:effect-declarations:source-adapter",
        ],
      },
      serverDeclarationBuild: {
        command: "vp pack",
        cwd: "packages/server",
        dependsOn: [
          "build:effect-declarations:client",
          "build:effect-declarations:config",
          "build:effect-declarations:effect-utils",
          "build:effect-declarations:protocol",
          "build:effect-declarations:runtime-core",
          "build:effect-declarations:source-adapter",
        ],
      },
      serverDependency: "workspace:*",
      uniqueBuildDirectories: buildDirectories,
    });
  });
});
