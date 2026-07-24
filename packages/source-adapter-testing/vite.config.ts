import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";

export default defineConfig({
  resolve: {
    alias: {
      "@effect-view-server/source-adapter-testing": new URL("./src/index.ts", import.meta.url)
        .pathname,
    },
  },
  test: {
    include: [
      "src/**/*.test.ts",
      "../runtime-core/src/source-runtime.test.ts",
      "../runtime-core/src/source-runtime-conformance.test.ts",
    ],
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
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
