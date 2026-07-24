import { describe, expect, it } from "@effect/vitest";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import { build } from "vite";

const browserContractBudgetBytes = 32 * 1024;
const fixture = fileURLToPath(
  new URL("./fixtures/source-adapter-browser.ts", import.meta.url),
);

describe("Source Adapter browser contract", () => {
  it("bundles the portable facade without server/platform leaks and stays inside its budget", async () => {
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
      throw new Error("The Source Adapter browser fixture emitted no JavaScript chunk.");
    }
    const moduleIds = chunks.flatMap((chunk) => Object.keys(chunk.modules));

    expect(
      moduleIds.some(
        (id) =>
          id.includes("/packages/effect-view-server/dist/model-") &&
          id.endsWith(".js"),
      ),
    ).toBe(true);
    expect(
      moduleIds.filter(
        (id) =>
          id.includes("/packages/source-adapter/src/server.ts") ||
          id.includes("/packages/source-adapter-testing/") ||
          id.includes("/packages/server/") ||
          id.includes("/@effect/platform-node/") ||
          id.startsWith("node:"),
      ),
    ).toStrictEqual([]);
    expect(gzipSync(code).byteLength).toBeLessThanOrEqual(
      browserContractBudgetBytes,
    );
  });
});
