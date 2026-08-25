import { antipattern, correctness, effectNative, style } from "@effect/tsgo/oxlint-presets";
import { defineConfig } from "vite-plus";
import { strictLintOptions } from "./tools/vite/lint-policy";

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

const effectTsgoPresets = [correctness, antipattern, effectNative, style];

// The Oxlint integration is zero-debt: every clean rule is an error. Rules that already expose
// repository-wide migration work stay explicitly disabled until that work can land atomically.
const deferredEffectTsgoRules = new Set([
  "effecttsgo/abort-controller-in-effect",
  "effecttsgo/any-unknown-in-error-context",
  "effecttsgo/async-function",
  "effecttsgo/crypto-random-uuid",
  "effecttsgo/deterministic-keys",
  "effecttsgo/extends-native-error",
  "effecttsgo/global-date",
  "effecttsgo/global-fetch-in-effect",
  "effecttsgo/global-random",
  "effecttsgo/global-timers",
  "effecttsgo/instance-of-schema",
  "effecttsgo/lazy-effect",
  "effecttsgo/missed-pipeable-opportunity",
  "effecttsgo/missing-pipeable-signature",
  "effecttsgo/nested-effect-gen-yield",
  "effecttsgo/new-promise",
  "effecttsgo/new-schema-class",
  "effecttsgo/node-builtin-import",
  "effecttsgo/prefer-schema-over-json",
  "effecttsgo/prefer-typed-schema-decoder",
  "effecttsgo/prefer-unsafe-constructor",
  "effecttsgo/process-env",
  "effecttsgo/redundant-map-error",
  "effecttsgo/schema-number",
  "effecttsgo/schema-sync-in-effect",
  "effecttsgo/service-not-as-class",
  "effecttsgo/strict-boolean-expressions",
  "effecttsgo/strict-effect-provide",
  "effecttsgo/unnecessary-arrow-block",
  "effecttsgo/unnecessary-typeof-type",
]);

const aggressiveEffectTsgoRules = Object.fromEntries(
  effectTsgoPresets.flatMap((preset) =>
    Object.keys(preset.rules ?? {}).map((ruleName) => [
      ruleName,
      deferredEffectTsgoRules.has(ruleName) ? "off" : "error",
    ]),
  ),
);

const deferredEffectTsgoAuditFlags = [...deferredEffectTsgoRules]
  .map((ruleName) => `--warn=${ruleName}`)
  .join(" ");

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
        "scripts/benchmark-environment.mjs",
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
      // Standalone repository scripts use their own runtime/test formatting conventions.
      "scripts/**",
      // The plugin source is mirrored into the skill asset and must stay byte-for-byte identical.
      "tools/oxlint/anti-slop/**",
    ],
  },
  lint: {
    extends: effectTsgoPresets,
    jsPlugins: [
      {
        name: "anti-slop",
        specifier: "./tools/oxlint/anti-slop/index.ts",
      },
    ],
    ignorePatterns: [
      ".pnpm-store/**",
      ".repos/**",
      // Most repository scripts are outside the application type-aware lint project; keep the integration test covered.
      "scripts/**",
      "!scripts/anti-slop-rule.test.ts",
      // The bundled copy is an installation asset; lint the live plugin source instead.
      ".agents/skills/install-anti-slop/**",
      // Package fixtures intentionally contain invalid, duplicated, and non-Effect consumer code.
      "packages/source-adapter-testing/test-fixtures/**",
      "tools/oxlint/anti-slop/**",
    ],
    options: strictLintOptions,
    rules: {
      ...aggressiveEffectTsgoRules,
      "anti-slop/no-unsafe-dictionary-type": "error",
    },
  },
  run: {
    cache: true,
    tasks: {
      ...declarationTasks,
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
      "audit:effect": {
        command: `vp lint ${deferredEffectTsgoAuditFlags}`,
      },
    },
  },
});
