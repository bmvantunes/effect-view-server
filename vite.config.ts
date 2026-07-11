import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    include: ["scripts/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      include: [
        "scripts/benchmark-baseline.mjs",
        "scripts/benchmark-baseline-cli.mjs",
        "scripts/benchmark-baseline-runner.mjs",
        "scripts/bench-runtime-kafka-ingest.mjs",
        "scripts/check-internal-seams.ts",
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
  },
});
