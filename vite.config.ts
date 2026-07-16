import { defineConfig } from "vite-plus";

const declarationBuildTask = "build:effect-declarations";

const declarationProjects = [
  { name: "config", directory: "packages/config", dependsOn: [] },
  { name: "effect-utils", directory: "packages/effect-utils", dependsOn: [] },
  {
    name: "column-live-view-engine",
    directory: "packages/column-live-view-engine",
    dependsOn: ["config", "effect-utils"],
  },
  {
    name: "protocol",
    directory: "packages/protocol",
    dependsOn: ["config", "effect-utils"],
  },
  {
    name: "client",
    directory: "packages/client",
    dependsOn: ["config", "effect-utils", "protocol"],
  },
  {
    name: "runtime-core",
    directory: "packages/runtime-core",
    dependsOn: ["client", "column-live-view-engine", "config", "effect-utils"],
  },
  {
    name: "server",
    directory: "packages/server",
    dependsOn: ["client", "config", "effect-utils", "protocol"],
  },
  {
    name: "in-memory",
    directory: "packages/in-memory",
    dependsOn: ["client", "config", "runtime-core"],
  },
  {
    name: "runtime",
    directory: "packages/runtime",
    dependsOn: ["client", "config", "effect-utils", "runtime-core", "server"],
  },
  {
    name: "react",
    directory: "packages/react",
    dependsOn: ["client", "config", "effect-utils", "in-memory"],
  },
] as const;

const declarationTaskName = (name: string) => `${declarationBuildTask}:${name}`;

const declarationTasks = Object.fromEntries(
  declarationProjects.map(({ name, directory, dependsOn }) => [
    declarationTaskName(name),
    {
      command: "vp pack",
      cwd: directory,
      dependsOn: dependsOn.map(declarationTaskName),
    },
  ]),
);

const diagnosticsProjects = [
  { name: "config", project: "packages/config", declarationTask: undefined },
  { name: "effect-utils", project: "packages/effect-utils", declarationTask: undefined },
  {
    name: "protocol",
    project: "packages/protocol",
    declarationTask: declarationTaskName("protocol"),
  },
  {
    name: "client",
    project: "packages/client",
    declarationTask: declarationTaskName("client"),
  },
  {
    name: "column-live-view-engine",
    project: "packages/column-live-view-engine",
    declarationTask: declarationTaskName("column-live-view-engine"),
  },
  {
    name: "runtime-core",
    project: "packages/runtime-core",
    declarationTask: declarationTaskName("runtime-core"),
  },
  {
    name: "in-memory",
    project: "packages/in-memory",
    declarationTask: declarationTaskName("in-memory"),
  },
  {
    name: "server",
    project: "packages/server",
    declarationTask: declarationTaskName("server"),
  },
  {
    name: "runtime",
    project: "packages/runtime",
    declarationTask: declarationTaskName("runtime"),
  },
  {
    name: "react",
    project: "packages/react",
    declarationTask: declarationTaskName("react"),
  },
  {
    name: "facade",
    project: "packages/effect-view-server",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:kafka-react",
    project: "examples/kafka-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:grpc-leased-react",
    project: "examples/grpc-leased-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:grpc-materialized-react",
    project: "examples/grpc-materialized-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:combined-sources-react",
    project: "examples/combined-sources-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:ssr-react",
    project: "examples/ssr-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:tcp-publisher-react",
    project: "examples/tcp-publisher-react",
    declarationTask: declarationBuildTask,
  },
  {
    name: "example:in-memory-react",
    project: "examples/in-memory-react",
    declarationTask: declarationBuildTask,
  },
  { name: "app", project: "apps/example", declarationTask: declarationBuildTask },
] as const;

const diagnosticsTaskName = (name: string) => `check:effect:${name}`;

const effectDiagnosticsTask = (project: string, declarationTask: string | undefined) => ({
  command: `effect-language-service diagnostics --project ${project}/tsconfig.json --format text --strict`,
  dependsOn: declarationTask === undefined ? [] : [declarationTask],
});

const diagnosticsTasks = Object.fromEntries(
  diagnosticsProjects.map(({ name, project, declarationTask }) => [
    diagnosticsTaskName(name),
    effectDiagnosticsTask(project, declarationTask),
  ]),
);

const exampleDiagnosticsTasks = diagnosticsProjects
  .filter(({ name }) => name.startsWith("example:"))
  .map(({ name }) => diagnosticsTaskName(name));

const allDiagnosticsTasks = diagnosticsProjects.map(({ name }) => diagnosticsTaskName(name));

export default defineConfig({
  test: {
    include: ["scripts/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      include: [
        "scripts/benchmark-artifact-mechanics.mjs",
        "scripts/benchmark-artifact-io.mjs",
        "scripts/benchmark-baseline.mjs",
        "scripts/benchmark-comparison-policy.mjs",
        "scripts/benchmark-baseline-cli.mjs",
        "scripts/benchmark-baseline-profiles.mjs",
        "scripts/benchmark-baseline-workflow.mjs",
        "scripts/benchmark-baseline-task-catalog.mjs",
        "scripts/benchmark-profile-artifact.mjs",
        "scripts/benchmark-profile-runner.mjs",
        "scripts/benchmark-profile.mjs",
        "scripts/benchmark-sampling-policy.mjs",
        "scripts/bench-runtime-kafka-ingest.mjs",
        "scripts/check-internal-seams.ts",
        "scripts/grpc-leased-benchmark-policy.mjs",
        "scripts/grpc-materialized-benchmark-policy.mjs",
        "scripts/package-surface-policy.ts",
        "scripts/release-publish-orchestration.mjs",
        "scripts/release-publish-policy.mjs",
        "scripts/typescript-module-inspection.ts",
      ],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    ignorePatterns: [".repos/**", "scripts/**"],
  },
  lint: {
    ignorePatterns: [".repos/**", "scripts/**"],
    options: { typeAware: true, typeCheck: true },
  },
  run: {
    cache: true,
    tasks: {
      ...declarationTasks,
      ...diagnosticsTasks,
      "build:effect-declarations": {
        command: "vp pack",
        cwd: "packages/effect-view-server",
        dependsOn: declarationProjects.map(({ name }) => declarationTaskName(name)),
      },
      "examples:check:effect": {
        command: 'node --eval ""',
        dependsOn: exampleDiagnosticsTasks,
      },
      "check:effect": {
        command: 'node --eval ""',
        dependsOn: allDiagnosticsTasks,
      },
    },
  },
});
