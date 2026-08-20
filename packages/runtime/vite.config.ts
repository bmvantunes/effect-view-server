import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";
import { strictLintOptions } from "../../tools/vite/lint-policy";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    testTimeout: 30_000,
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["**/*.bench.ts", "src/**/*.test.ts", "src/**/*.test-d.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack("src/index.ts"),
  lint: {
    options: strictLintOptions,
  },
  fmt: {},
});
