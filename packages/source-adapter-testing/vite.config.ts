import { fileURLToPath } from "node:url";
import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";
import { strictLintOptions } from "../../tools/vite/lint-policy";

export default defineConfig({
  resolve: {
    alias: {
      "@effect-view-server/source-adapter/internal": fileURLToPath(
        new URL("../source-adapter/src/internal.ts", import.meta.url),
      ),
      "@effect-view-server/source-adapter/server": fileURLToPath(
        new URL("../source-adapter/src/server.ts", import.meta.url),
      ),
      "@effect-view-server/source-adapter": fileURLToPath(
        new URL("../source-adapter/src/index.ts", import.meta.url),
      ),
      "@effect-view-server/source-adapter-testing": fileURLToPath(
        new URL("./src/index.ts", import.meta.url),
      ),
    },
  },
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
      exclude: ["src/**/*.test.ts", "src/**/*.test-d.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack(["src/index.ts"]),
  lint: {
    options: strictLintOptions,
  },
  fmt: {},
});
