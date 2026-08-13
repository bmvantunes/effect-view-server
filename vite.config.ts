import { defineConfig } from "vite-plus";

const declarationBuildTask = "build:effect-declarations";

const declarationProjects = [
  { name: "effect-utils", directory: "packages/effect-utils", dependsOn: [] },
  { name: "source-adapter", directory: "packages/source-adapter", dependsOn: [] },
  { name: "grpc", directory: "packages/grpc", dependsOn: ["source-adapter"] },
  {
    name: "source-adapter-testing",
    directory: "packages/source-adapter-testing",
    dependsOn: ["source-adapter"],
  },
  {
    name: "kafka",
    directory: "packages/kafka",
    dependsOn: ["source-adapter"],
  },
  { name: "config", directory: "packages/config", dependsOn: ["source-adapter"] },
  {
    name: "column-live-view-engine",
    directory: "packages/column-live-view-engine",
    dependsOn: ["config", "effect-utils", "source-adapter"],
  },
  {
    name: "protocol",
    directory: "packages/protocol",
    dependsOn: ["config", "effect-utils"],
  },
  {
    name: "client",
    directory: "packages/client",
    dependsOn: ["config", "effect-utils", "protocol", "source-adapter"],
  },
  {
    name: "runtime-core",
    directory: "packages/runtime-core",
    dependsOn: ["client", "column-live-view-engine", "config", "effect-utils", "source-adapter"],
  },
  {
    name: "source-adapter-conformance-host",
    directory: "packages/source-adapter-conformance-host",
    dependsOn: ["config", "runtime-core", "source-adapter", "source-adapter-testing"],
  },
  {
    name: "server",
    directory: "packages/server",
    dependsOn: ["client", "config", "effect-utils", "protocol", "runtime-core", "source-adapter"],
  },
  {
    name: "in-memory",
    directory: "packages/in-memory",
    dependsOn: ["client", "config", "runtime-core"],
  },
  {
    name: "runtime",
    directory: "packages/runtime",
    dependsOn: ["client", "config", "effect-utils", "runtime-core", "server", "source-adapter"],
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
  {
    name: "source-adapter",
    project: "packages/source-adapter",
    declarationTask: declarationTaskName("source-adapter"),
  },
  {
    name: "grpc",
    project: "packages/grpc",
    declarationTask: declarationTaskName("grpc"),
  },
  {
    name: "source-adapter-testing",
    project: "packages/source-adapter-testing",
    declarationTask: declarationTaskName("source-adapter-testing"),
  },
  {
    name: "kafka",
    project: "packages/kafka",
    declarationTask: declarationTaskName("kafka"),
  },
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
    name: "source-adapter-conformance-host",
    project: "packages/source-adapter-conformance-host",
    declarationTask: declarationTaskName("source-adapter-conformance-host"),
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

const diagnosticsAdditionalDependencies: Readonly<Record<string, ReadonlyArray<string>>> = {
  kafka: [declarationTaskName("source-adapter-conformance-host")],
};

const effectDiagnosticsTask = (
  project: string,
  declarationTask: string | undefined,
  additionalDependencies: ReadonlyArray<string>,
) => ({
  command: `effect-tsgo diagnostics --project ${project}/tsconfig.json --format text --strict`,
  dependsOn: [
    ...(declarationTask === undefined ? [] : [declarationTask]),
    ...additionalDependencies,
  ],
});

const diagnosticsTasks = Object.fromEntries(
  diagnosticsProjects.map(({ name, project, declarationTask }) => [
    diagnosticsTaskName(name),
    effectDiagnosticsTask(project, declarationTask, diagnosticsAdditionalDependencies[name] ?? []),
  ]),
);

const exampleDiagnosticsTasks = diagnosticsProjects
  .filter(({ name }) => name.startsWith("example:"))
  .map(({ name }) => diagnosticsTaskName(name));

const allDiagnosticsTasks = diagnosticsProjects.map(({ name }) => diagnosticsTaskName(name));

export default defineConfig({
  test: {
    globalSetup: ["./scripts/repository-test-global-setup.ts"],
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
        "scripts/test-kafka-adapter-runner.mjs",
        "scripts/check-internal-seams.ts",
        "scripts/grpc-leased-benchmark-policy.mjs",
        "scripts/grpc-materialized-benchmark-policy.mjs",
        "scripts/package-surface-policy.ts",
        "scripts/release-publish-orchestration.mjs",
        "scripts/release-publish-policy.mjs",
        "scripts/release-version-orchestration.mjs",
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
    ignorePatterns: [
      ".agents/**",
      ".pnpm-store/**",
      ".repos/**",
      "scripts/**",
      "tools/oxlint/anti-slop/**",
    ],
  },
  lint: {
    jsPlugins: [
      {
        name: "anti-slop",
        specifier: "./tools/oxlint/anti-slop/index.ts",
      },
    ],
    ignorePatterns: [
      ".pnpm-store/**",
      ".repos/**",
      "scripts/**",
      ".agents/skills/install-anti-slop/**",
      "packages/source-adapter-testing/test-fixtures/package-adapter/invalid-types/**",
      "tools/oxlint/anti-slop/**",
    ],
    options: { typeAware: true, typeCheck: true },
    rules: {
      "anti-slop/no-unsafe-dictionary-type": "error",
    },
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
      "bench:kafka-source-broker": {
        command: "node scripts/run-kafka-source-broker-bench.mjs",
        dependsOn: [declarationTaskName("runtime-core")],
      },
      "bench:grpc-source-adapter": {
        command: "node scripts/run-grpc-source-adapter-bench.mjs",
        dependsOn: [declarationTaskName("runtime-core")],
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
