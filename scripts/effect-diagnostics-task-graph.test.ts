import { describe, expect, it } from "@effect/vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import config from "../vite.config";

const tasks = config.run?.tasks ?? {};

const taskEntriesWithPrefix = (prefix: string) =>
  Object.entries(tasks).filter(([name]) => name.startsWith(prefix));

const taskCommand = (task: (typeof tasks)[string]) =>
  typeof task === "object" && !Array.isArray(task) ? task.command : task;

const taskCwd = (task: (typeof tasks)[string]) =>
  typeof task === "object" && !Array.isArray(task) ? task.cwd : undefined;

const taskDependencies = (task: (typeof tasks)[string]) =>
  typeof task === "object" && !Array.isArray(task) ? (task.dependsOn ?? []) : [];

describe("strict Effect diagnostics task graph", () => {
  it("builds each declaration package once before runtime diagnostics", () => {
    const facadePackage = JSON.parse(
      readFileSync("packages/effect-view-server/package.json", "utf8"),
    );
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
      nestedBuildRuns: allDeclarationBuilds
        .map(([, task]) => taskCommand(task))
        .filter((command) => typeof command === "string" && command.includes("vp run")),
      runtimeDeclarationBuild: tasks["build:effect-declarations:runtime"],
      runtimeDiagnostics: tasks["check:effect:runtime"],
      serverDependency: facadePackage.devDependencies["@effect-view-server/server"],
      uniqueBuildDirectories: [...new Set(buildDirectories)],
    }).toStrictEqual({
      buildCommands: Array.from({ length: 11 }, () => "vp pack"),
      buildDirectories: [
        "packages/config",
        "packages/effect-utils",
        "packages/column-live-view-engine",
        "packages/protocol",
        "packages/client",
        "packages/runtime-core",
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
          "build:effect-declarations:config",
          "build:effect-declarations:effect-utils",
          "build:effect-declarations:column-live-view-engine",
          "build:effect-declarations:protocol",
          "build:effect-declarations:client",
          "build:effect-declarations:runtime-core",
          "build:effect-declarations:server",
          "build:effect-declarations:in-memory",
          "build:effect-declarations:runtime",
          "build:effect-declarations:react",
        ],
      },
      declarationBuildTaskNames: [
        "build:effect-declarations:config",
        "build:effect-declarations:effect-utils",
        "build:effect-declarations:column-live-view-engine",
        "build:effect-declarations:protocol",
        "build:effect-declarations:client",
        "build:effect-declarations:runtime-core",
        "build:effect-declarations:server",
        "build:effect-declarations:in-memory",
        "build:effect-declarations:runtime",
        "build:effect-declarations:react",
      ],
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
        ],
      },
      runtimeDiagnostics: {
        command:
          "effect-language-service diagnostics --project packages/runtime/tsconfig.json --format text --strict",
        dependsOn: ["build:effect-declarations"],
      },
      serverDependency: "workspace:*",
      uniqueBuildDirectories: buildDirectories,
    });
  });

  it("fans every independent diagnostic out through sibling Vite tasks", () => {
    const rootPackage = JSON.parse(readFileSync("package.json", "utf8"));
    const diagnosticTasks = taskEntriesWithPrefix("check:effect:");
    const diagnosticTaskNames = diagnosticTasks.map(([name]) => name);
    const exampleDiagnosticTaskNames = diagnosticTaskNames.filter((name) =>
      name.startsWith("check:effect:example:"),
    );
    const discoveredProjects = ["packages", "examples"]
      .flatMap((directory) =>
        readdirSync(directory, { withFileTypes: true })
          .filter((entry) => entry.isDirectory())
          .map((entry) => `${directory}/${entry.name}`),
      )
      .concat("apps/example")
      .filter((directory) => existsSync(`${directory}/tsconfig.json`))
      .sort();
    const diagnosticProjects = diagnosticTasks
      .flatMap(([, task]) => {
        const command = taskCommand(task);
        if (typeof command !== "string") return [];
        const match = command.match(/--project (.+)\/tsconfig\.json/);
        return match === null ? [] : [match[1]];
      })
      .sort();
    const serializedDiagnostics = diagnosticTasks.flatMap(([name, task]) =>
      taskDependencies(task)
        .filter((dependency) => dependency.startsWith("check:effect:"))
        .map((dependency) => ({ dependency, name })),
    );

    expect({
      appTask: tasks["check:effect:app"],
      diagnosticProjects,
      examplesTask: tasks["examples:check:effect"],
      inMemoryExampleTask: tasks["check:effect:example:in-memory-react"],
      legacyCheckScript: rootPackage.scripts["check:effect"],
      legacyExamplesScript: rootPackage.scripts["examples:check:effect"],
      rootTask: tasks["check:effect"],
      serializedDiagnostics,
    }).toStrictEqual({
      appTask: {
        command:
          "effect-language-service diagnostics --project apps/example/tsconfig.json --format text --strict",
        dependsOn: ["build:effect-declarations"],
      },
      diagnosticProjects: discoveredProjects,
      examplesTask: {
        command: 'node --eval ""',
        dependsOn: exampleDiagnosticTaskNames,
      },
      inMemoryExampleTask: {
        command:
          "effect-language-service diagnostics --project examples/in-memory-react/tsconfig.json --format text --strict",
        dependsOn: ["build:effect-declarations"],
      },
      legacyCheckScript: undefined,
      legacyExamplesScript: undefined,
      rootTask: {
        command: 'node --eval ""',
        dependsOn: diagnosticTaskNames,
      },
      serializedDiagnostics: [],
    });
  });
});
