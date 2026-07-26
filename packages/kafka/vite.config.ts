import { fileURLToPath } from "node:url";
import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "vp pack",
        dependsOn: ["@effect-view-server/source-adapter#build"],
      },
      test: {
        command: "node ../../scripts/test-kafka-adapter.mjs",
        dependsOn: [
          "@effect-view-server/kafka#build",
          "@effect-view-server/source-adapter-conformance-host#build",
        ],
      },
    },
  },
  resolve: {
    alias: {
      "effect-view-server/source-adapter/server": fileURLToPath(
        new URL("../source-adapter/dist/server.js", import.meta.url),
      ),
      "effect-view-server/source-adapter": fileURLToPath(
        new URL("../source-adapter/dist/index.js", import.meta.url),
      ),
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
    testTimeout: 30_000,
    benchmark: {
      include: ["src/**/*.bench.ts"],
    },
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: [
        "src/**/*.bench.ts",
        "src/**/*.test.ts",
        "src/**/*.test-d.ts",
        "src/test-fixtures/**",
      ],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack(["src/contract.ts", "src/server.ts", "src/node.ts"]),
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
