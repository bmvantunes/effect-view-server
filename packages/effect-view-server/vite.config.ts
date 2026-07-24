import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";

export default defineConfig({
  test: {
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
  },
  pack: libraryPack([
    "src/client.ts",
    "src/client-remote.ts",
    "src/column-live-view-engine.ts",
    "src/config.ts",
    "src/config-grpc.ts",
    "src/config-health.ts",
    "src/config-kafka.ts",
    "src/config-live-protocol.ts",
    "src/config-query.ts",
    "src/config-runtime.ts",
    "src/in-memory.ts",
    "src/in-memory-testing.ts",
    "src/react.ts",
    "src/react-testing.ts",
    "src/runtime.ts",
    "src/server.ts",
    "src/source-adapter.ts",
    "src/source-adapter-server.ts",
    "src/source-adapter-testing.ts",
  ]),
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
