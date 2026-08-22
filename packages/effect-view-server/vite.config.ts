import { fileURLToPath } from "node:url";
import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";
import { strictLintOptions } from "../../tools/vite/lint-policy";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "vp pack",
        dependsOn: [
          "@effect-view-server/client#build",
          "@effect-view-server/column-live-view-engine#build",
          "@effect-view-server/config#build",
          "@effect-view-server/effect-utils#build",
          "@effect-view-server/grpc#build",
          "@effect-view-server/in-memory#build",
          "@effect-view-server/kafka#build",
          "@effect-view-server/react#build",
          "@effect-view-server/runtime#build",
          "@effect-view-server/server#build",
          "@effect-view-server/source-adapter#build",
          "@effect-view-server/source-adapter-conformance-host#build",
          "@effect-view-server/source-adapter-testing#build",
        ],
      },
    },
  },
  test: {
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
  },
  pack: libraryPack(
    [
      "src/client.ts",
      "src/client-remote.ts",
      "src/column-live-view-engine.ts",
      "src/config.ts",
      "src/config-health.ts",
      "src/config-live-protocol.ts",
      "src/config-query.ts",
      "src/config-runtime.ts",
      "src/grpc-contract.ts",
      "src/grpc-server.ts",
      "src/grpc-node.ts",
      "src/in-memory.ts",
      "src/in-memory-testing.ts",
      "src/kafka-contract.ts",
      "src/kafka-server.ts",
      "src/kafka-node.ts",
      "src/react.ts",
      "src/react-testing.ts",
      "src/runtime.ts",
      "src/server.ts",
      "src/source-adapter.ts",
      "src/source-adapter-server.ts",
      "src/source-adapter-testing.ts",
      "src/value-semantics.ts",
    ],
    {
      // The testing subpath bundles TypeScript, whose Node runtime reads these CommonJS globals.
      shims: true,
      alias: {
        "effect-view-server/source-adapter/server": fileURLToPath(
          new URL("../source-adapter/dist/server.js", import.meta.url),
        ),
        "effect-view-server/source-adapter": fileURLToPath(
          new URL("../source-adapter/dist/index.js", import.meta.url),
        ),
      },
      tsconfig: "./tsconfig.build.json",
    },
  ),
  lint: {
    options: strictLintOptions,
  },
  fmt: {},
});
