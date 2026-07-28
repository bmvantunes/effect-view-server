import { describe, expect, it } from "@effect/vitest";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { build } from "vite";

// One real authored config plus both first-party browser contracts, including
// generated protobuf descriptors, must fit inside this documented gzip budget.
const firstPartyContractsBudgetBytes = 128 * 1024;
const fixture = fileURLToPath(
  new URL("./fixtures/first-party-contracts-browser.ts", import.meta.url),
);

describe("first-party browser contracts", () => {
  it("bundles config plus Kafka and gRPC contracts without server or Node implementations", async () => {
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
    const code = chunks.map((chunk) => chunk.code).join("\n");
    const moduleIds = chunks.flatMap((chunk) => Object.keys(chunk.modules));

    expect(chunks.length).toBeGreaterThan(0);
    expect(
      moduleIds.filter((id) => id.endsWith("/fixtures/first-party-contracts-browser.ts")),
    ).toHaveLength(1);
    expect(
      moduleIds.filter(
        (id) =>
          id.includes("/packages/config/dist/grpc") ||
          id.includes("/packages/config/dist/kafka") ||
          id.includes("/packages/grpc/dist/node") ||
          id.includes("/packages/grpc/dist/server") ||
          id.includes("/packages/kafka/dist/node") ||
          id.includes("/packages/kafka/dist/server") ||
          id.includes("/packages/runtime-core/") ||
          id.includes("/packages/server/") ||
          id.includes("/@connectrpc/connect-node/") ||
          id.includes("/@effect/platform-node/") ||
          id.includes("/@platformatic/kafka/") ||
          id.startsWith("node:"),
      ),
    ).toStrictEqual([]);
    expect(gzipSync(code).byteLength).toBeLessThanOrEqual(firstPartyContractsBudgetBytes);
  });
});
