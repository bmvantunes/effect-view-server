import { defineConfig } from "vite-plus";

const effectDiagnosticsTask = (project: string, dependsOn: Array<string> = []) => ({
  command: `effect-language-service diagnostics --project ${project}/tsconfig.json --format text --strict`,
  dependsOn,
});

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
      "build:effect-declarations": {
        command: "vp run -t effect-view-server#build",
      },
      "check:effect:config": effectDiagnosticsTask("packages/config"),
      "check:effect:protocol": effectDiagnosticsTask("packages/protocol", [
        "build:effect-declarations",
      ]),
      "check:effect:effect-utils": effectDiagnosticsTask("packages/effect-utils"),
      "check:effect:client": effectDiagnosticsTask("packages/client", [
        "build:effect-declarations",
      ]),
      "check:effect:column-live-view-engine": effectDiagnosticsTask(
        "packages/column-live-view-engine",
        ["build:effect-declarations"],
      ),
      "check:effect:runtime-core": effectDiagnosticsTask("packages/runtime-core", [
        "build:effect-declarations",
      ]),
      "check:effect:in-memory": effectDiagnosticsTask("packages/in-memory", [
        "build:effect-declarations",
      ]),
      "check:effect:server": effectDiagnosticsTask("packages/server", [
        "build:effect-declarations",
      ]),
      "check:effect:runtime": effectDiagnosticsTask("packages/runtime", [
        "build:effect-declarations",
      ]),
      "check:effect:react": effectDiagnosticsTask("packages/react", ["build:effect-declarations"]),
      "check:effect:facade": effectDiagnosticsTask("packages/effect-view-server", [
        "build:effect-declarations",
      ]),
      "check:effect:example:kafka-react": effectDiagnosticsTask("examples/kafka-react", [
        "build:effect-declarations",
      ]),
      "check:effect:example:grpc-leased-react": effectDiagnosticsTask(
        "examples/grpc-leased-react",
        ["build:effect-declarations"],
      ),
      "check:effect:example:grpc-materialized-react": effectDiagnosticsTask(
        "examples/grpc-materialized-react",
        ["build:effect-declarations"],
      ),
      "check:effect:example:combined-sources-react": effectDiagnosticsTask(
        "examples/combined-sources-react",
        ["build:effect-declarations"],
      ),
      "check:effect:example:ssr-react": effectDiagnosticsTask("examples/ssr-react", [
        "build:effect-declarations",
      ]),
      "check:effect:example:tcp-publisher-react": effectDiagnosticsTask(
        "examples/tcp-publisher-react",
        ["build:effect-declarations"],
      ),
      "examples:check:effect": effectDiagnosticsTask("examples/in-memory-react", [
        "build:effect-declarations",
        "check:effect:example:kafka-react",
        "check:effect:example:grpc-leased-react",
        "check:effect:example:grpc-materialized-react",
        "check:effect:example:combined-sources-react",
        "check:effect:example:ssr-react",
        "check:effect:example:tcp-publisher-react",
      ]),
      "check:effect": effectDiagnosticsTask("apps/example", [
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
      ]),
    },
  },
});
