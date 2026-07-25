import { describe, expect, it } from "@effect/vitest";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { build } from "vite";

// Includes Effect Schema, Source Adapter contract, generated protobuf
// descriptors, and the first-party gRPC descriptor helper.
const grpcBrowserContractBudgetBytes = 64 * 1024;
const fixture = fileURLToPath(
  new URL("./fixtures/grpc-source-adapter-browser.ts", import.meta.url),
);

describe("gRPC Source Adapter browser contract", () => {
  it("bundles generated descriptors through the public facade without server/platform leaks", async () => {
    const result = await build({
      configFile: false,
      logLevel: "silent",
      build: {
        minify: "esbuild",
        rollupOptions: {
          input: fixture,
        },
        target: "es2022",
        write: false,
      },
    });
    const outputs = Array.isArray(result) ? result : [result];
    const chunks = outputs.flatMap((output) =>
      "output" in output
        ? output.output.filter((entry) => entry.type === "chunk")
        : [],
    );
    const code = chunks[0]?.code;
    if (code === undefined) {
      throw new Error("The gRPC browser fixture emitted no JavaScript chunk.");
    }
    const moduleIds = chunks.flatMap((chunk) => Object.keys(chunk.modules));

    expect(code).toContain("GrpcConfigurationFailure");
    expect(
      moduleIds.filter(
        (id) =>
          id.includes("/packages/grpc/dist/server") ||
          id.includes("/packages/grpc/dist/node") ||
          id.includes("/packages/source-adapter-conformance-host/") ||
          id.includes("/packages/source-adapter-testing/") ||
          id.includes("/packages/runtime-core/") ||
          id.includes("/packages/server/") ||
          id.includes("/@connectrpc/connect-node/") ||
          id.includes("/@effect/platform-node/") ||
          id.startsWith("node:"),
      ),
    ).toStrictEqual([]);
    expect(gzipSync(code).byteLength).toBeLessThanOrEqual(
      grpcBrowserContractBudgetBytes,
    );
  });
});
