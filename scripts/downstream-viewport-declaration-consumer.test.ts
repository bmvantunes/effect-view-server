import { execFileSync } from "node:child_process";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "@effect/vitest";
import { inspectTypeScriptModule } from "./typescript-module-inspection";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));

const writeJson = (path: string, value: unknown): void => {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
};

const installStrictly = (directory: string): void => {
  writeFileSync(
    join(directory, "pnpm-workspace.yaml"),
    [
      "packages:",
      "  - .",
      "",
      "autoInstallPeers: false",
      "strictPeerDependencies: true",
      "",
      "onlyBuiltDependencies:",
      "  - msgpackr-extract",
      "",
      "allowBuilds:",
      "  msgpackr-extract: true",
      "",
    ].join("\n"),
  );
  execFileSync("vp", ["install"], {
    cwd: directory,
    killSignal: "SIGTERM",
    stdio: "inherit",
    timeout: 55_000,
  });
};

const pack = (directory: string, destination: string): string => {
  const output = execFileSync(
    "npm",
    [
      "pack",
      directory,
      "--json",
      "--pack-destination",
      destination,
      "--cache",
      join(destination, ".npm-cache"),
    ],
    { cwd: repositoryRoot, encoding: "utf8" },
  );
  const result: unknown = JSON.parse(output);
  if (
    !Array.isArray(result) ||
    result.length !== 1 ||
    typeof result[0] !== "object" ||
    result[0] === null ||
    !("filename" in result[0]) ||
    typeof result[0].filename !== "string"
  ) {
    throw new Error("npm pack returned an unexpected result.");
  }
  return join(destination, result[0].filename);
};

describe("downstream viewport declaration bundle", () => {
  it(
    "preserves the source-owned base row without Effect in the downstream root",
    ({ onTestFinished }) => {
      const temporaryRoot = mkdtempSync(join(tmpdir(), "view-server-viewport-bundle-"));
      onTestFinished(() => rmSync(temporaryRoot, { force: true, recursive: true }));

      const requestedViewServerTarball =
        process.env.VIEW_SERVER_VIEWPORT_CONSUMER_TARBALL;
      const viewServerTarball =
        requestedViewServerTarball === undefined
          ? (() => {
              if (process.env.VIEW_SERVER_REPOSITORY_TEST_ARTIFACTS_READY !== "1") {
                execFileSync("vp", ["run", "effect-view-server#build"], {
                  cwd: repositoryRoot,
                  stdio: "inherit",
                });
              }
              return pack(
                join(repositoryRoot, "packages", "effect-view-server"),
                temporaryRoot,
              );
            })()
          : resolve(requestedViewServerTarball);

      const downstreamDirectory = join(temporaryRoot, "downstream");
      mkdirSync(join(downstreamDirectory, "src"), { recursive: true });
      writeJson(join(downstreamDirectory, "package.json"), {
        name: "downstream-viewport-adapter",
        version: "1.0.0",
        type: "module",
        packageManager: "pnpm@11.9.0",
        files: ["dist"],
        exports: {
          ".": {
            types: "./dist/index.d.mts",
            import: "./dist/index.mjs",
          },
        },
        dependencies: {},
        devDependencies: {
          "@emnapi/core": "1.7.1",
          "@emnapi/runtime": "1.7.1",
          "@effect/atom-react": "4.0.0-rc.111",
          "@types/node": "26.2.0",
          "@types/react": "19.2.18",
          "@types/react-dom": "19.2.4",
          effect: "4.0.0-rc.111",
          "effect-view-server": `file:${viewServerTarball}`,
          react: "19.2.8",
          "react-dom": "19.2.8",
          redis: "6.2.1",
          typescript: "7.0.2",
          vite: "8.0.0",
          "vite-plus": "0.2.8",
        },
      });
      writeFileSync(
        join(downstreamDirectory, "vite.config.ts"),
        [
          'import { defineConfig } from "vite-plus";',
          "",
          "export default defineConfig({",
          "  pack: {",
          '    entry: { index: "src/index.ts" },',
          "    dts: { tsgo: true },",
          "  },",
          "});",
          "",
        ].join("\n"),
      );
      writeJson(join(downstreamDirectory, "tsconfig.json"), {
        compilerOptions: {
          declaration: true,
          exactOptionalPropertyTypes: true,
          module: "preserve",
          moduleResolution: "bundler",
          strict: true,
        },
        include: ["src"],
      });
      writeFileSync(
        join(downstreamDirectory, "src", "index.ts"),
        [
          'import type { LiveQueryViewportBaseRow } from "effect-view-server/react";',
          "",
          "export type BundledViewportBaseRow<Viewport> = LiveQueryViewportBaseRow<Viewport>;",
          "export const clientReady = true;",
          "",
        ].join("\n"),
      );
      installStrictly(downstreamDirectory);
      execFileSync("vp", ["pack"], { cwd: downstreamDirectory, stdio: "inherit" });

      const downstreamDeclaration = readFileSync(
        join(downstreamDirectory, "dist", "index.d.mts"),
        "utf8",
      );
      const downstreamRuntime = readFileSync(
        join(downstreamDirectory, "dist", "index.mjs"),
        "utf8",
      );
      expect(downstreamDeclaration).toContain("BundledViewportBaseRow");
      expect(
        inspectTypeScriptModule({
          fileName: "index.d.mts",
          source: downstreamDeclaration,
        }).moduleSpecifiers.filter(
          (specifier) =>
            specifier === "effect" ||
            specifier.startsWith("effect/") ||
            specifier === "effect-view-server" ||
            specifier.startsWith("effect-view-server/"),
        ),
      ).toStrictEqual([]);
      expect(downstreamRuntime).not.toContain("effect-view-server");
      expect(downstreamRuntime).not.toMatch(/(?:from|import\()[^\n]*["']effect(?:\/|["'])/);

      const downstreamTarball = pack(downstreamDirectory, temporaryRoot);
      const integrationDirectory = join(temporaryRoot, "integration-consumer");
      mkdirSync(integrationDirectory, { recursive: true });
      writeJson(join(integrationDirectory, "package.json"), {
        name: "viewport-integration-consumer",
        private: true,
        type: "module",
        packageManager: "pnpm@11.9.0",
        dependencies: {
          "@emnapi/core": "1.7.1",
          "@emnapi/runtime": "1.7.1",
          "@effect/atom-react": "4.0.0-rc.111",
          "@types/node": "26.2.0",
          "@types/react": "19.2.18",
          "@types/react-dom": "19.2.4",
          "downstream-viewport-adapter": `file:${downstreamTarball}`,
          effect: "4.0.0-rc.111",
          "effect-view-server": `file:${viewServerTarball}`,
          react: "19.2.8",
          "react-dom": "19.2.8",
          redis: "6.2.1",
          typescript: "7.0.2",
          vite: "8.0.0",
        },
      });
      writeJson(join(integrationDirectory, "tsconfig.json"), {
        compilerOptions: {
          exactOptionalPropertyTypes: true,
          lib: ["ESNext", "DOM"],
          module: "preserve",
          moduleResolution: "bundler",
          noEmit: true,
          skipLibCheck: false,
          strict: true,
          types: ["node"],
        },
        include: ["consumer.ts"],
      });
      writeFileSync(
        join(integrationDirectory, "consumer.ts"),
        [
          'import type { BundledViewportBaseRow } from "downstream-viewport-adapter";',
          'import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";',
          'import type { LiveQueryViewport } from "effect-view-server/react";',
          'import { Schema } from "effect";',
          "",
          "const orderConfig = defineViewServerConfig({",
          "  topics: { orders: { schema: Schema.Struct({ id: ViewServerId, total: Schema.Number }) } },",
          "});",
          "const positionConfig = defineViewServerConfig({",
          "  topics: { positions: { schema: Schema.Struct({ id: ViewServerId, quantity: Schema.Number }) } },",
          "});",
          'declare const orders: LiveQueryViewport<typeof orderConfig.topics, "orders">;',
          'declare const positions: LiveQueryViewport<typeof positionConfig.topics, "positions">;',
          "type Order = typeof orderConfig.topics.orders.schema.Type;",
          "type RequireNever<Value extends never> = Value;",
          "type ExactSource<Row, Viewport> =",
          "  [BundledViewportBaseRow<Viewport>] extends [Row]",
          "    ? [Row] extends [BundledViewportBaseRow<Viewport>] ? Viewport : never",
          "    : never;",
          "declare const extractedOrder: BundledViewportBaseRow<typeof orders>;",
          "declare const expectedOrder: Order;",
          "const exactForward: Order = extractedOrder;",
          "const exactBackward: BundledViewportBaseRow<typeof orders> = expectedOrder;",
          "const matching: ExactSource<Order, typeof orders> = orders;",
          "// @ts-expect-error a viewport for another Topic Row is rejected invariantly.",
          "const wrong: ExactSource<Order, typeof positions> = positions;",
          "type RejectAny = RequireNever<BundledViewportBaseRow<any>>;",
          "type RejectUnknown = RequireNever<BundledViewportBaseRow<unknown>>;",
          "type RejectErasedViewport = RequireNever<BundledViewportBaseRow<LiveQueryViewport<any, string>>>;",
          "type RejectUnwitnessed = RequireNever<BundledViewportBaseRow<{ readonly destroy: () => void }>>;",
          "void exactForward;",
          "void exactBackward;",
          "void matching;",
          "void wrong;",
          "type Rejected = RejectAny | RejectUnknown | RejectErasedViewport | RejectUnwitnessed;",
          "",
        ].join("\n"),
      );
      installStrictly(integrationDirectory);
      execFileSync("vp", ["exec", "tsc"], { cwd: integrationDirectory, stdio: "inherit" });
      execFileSync(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          [
            'const facade = await import("effect-view-server/react");',
            "const keys = Reflect.ownKeys(facade);",
            'if (keys.length !== 2 || keys[0] !== "createViewServerReact" || keys[1] !== Symbol.toStringTag) {',
            '  throw new Error(`Unexpected React facade keys: ${keys.map(String).join(", ")}`);',
            "}",
          ].join("\n"),
        ],
        { cwd: integrationDirectory, stdio: "inherit" },
      );

      const clientDirectory = join(temporaryRoot, "client-only-consumer");
      mkdirSync(clientDirectory, { recursive: true });
      writeJson(join(clientDirectory, "package.json"), {
        name: "viewport-client-only-consumer",
        private: true,
        type: "module",
        packageManager: "pnpm@11.9.0",
        dependencies: {
          "downstream-viewport-adapter": `file:${downstreamTarball}`,
          typescript: "7.0.2",
        },
      });
      writeJson(join(clientDirectory, "tsconfig.json"), {
        compilerOptions: {
          module: "preserve",
          moduleResolution: "bundler",
          noEmit: true,
          skipLibCheck: false,
          strict: true,
        },
        include: ["consumer.ts"],
      });
      writeFileSync(
        join(clientDirectory, "consumer.ts"),
        [
          'import { clientReady, type BundledViewportBaseRow } from "downstream-viewport-adapter";',
          "type RequireNever<Value extends never> = Value;",
          "type Unwitnessed = RequireNever<BundledViewportBaseRow<{ readonly ready: true }>>;",
          "if (!clientReady) throw new Error(\"downstream runtime was not ready\");",
          "type ClientRejection = Unwitnessed;",
          "",
        ].join("\n"),
      );
      installStrictly(clientDirectory);
      const installedPackageEntries = readdirSync(
        join(clientDirectory, "node_modules", ".pnpm"),
      );
      expect(
        installedPackageEntries.filter(
          (entry) => entry.startsWith("effect@") || entry.startsWith("effect-view-server@"),
        ),
      ).toStrictEqual([]);
      const clientLockfile = readFileSync(join(clientDirectory, "pnpm-lock.yaml"), "utf8");
      expect(clientLockfile).not.toMatch(/^\s{2}(?:effect|effect-view-server)@/m);
      execFileSync("vp", ["exec", "tsc"], { cwd: clientDirectory, stdio: "inherit" });
      execFileSync(
        process.execPath,
        ["--input-type=module", "--eval", 'await import("downstream-viewport-adapter")'],
        { cwd: clientDirectory, stdio: "inherit" },
      );
    },
    120_000,
  );
});
