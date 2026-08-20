import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";
import { strictLintOptions } from "../../tools/vite/lint-policy";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    benchmark: {
      include: ["src/**/*.bench.ts"],
    },
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.bench.ts", "src/**/*.test.ts", "src/**/*.test-d.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack(["src/index.ts", "src/internal.ts"]),
  lint: {
    options: strictLintOptions,
  },
  fmt: {},
});
