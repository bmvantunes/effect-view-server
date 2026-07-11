import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  expectedPackageSurfaceFor,
  facadeProjectionFor,
  privatePackageSurfaceFor,
  sourceForbiddenExportPolicyFor,
} from "./package-surface-policy";
import {
  assertNoConsumerImportViolations,
  assertNoEngineSeamViolations,
  assertNoPackageImportViolations,
  assertNoPackageSurfaceViolations,
  collectConsumerImportViolations,
  collectEngineSeamViolations,
  collectPackageImportViolations,
  collectPackageSurfaceViolations,
  consumerImportViolationsFor,
  consumerMarkdownImportViolationsFor,
  facadeProjectionViolationsForSource,
  isTestFile,
  packageImportViolationsForSource,
  packageSourceViolationsFor,
  packageSurfaceViolationsForManifest,
  packageSurfaceViolationsForViteConfig,
  sourceFiles,
  sourceForbiddenExportViolationsForSource,
  topicStoreHelperViolationsForFile,
  topicStoreStateExportViolationsForFile,
} from "./check-internal-seams";

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-seams-"));

describe("internal Seam checker", () => {
  it("keeps the repository aligned with the Package Surface Policy", () => {
    expect(collectPackageSurfaceViolations()).toStrictEqual([]);
    expect(collectPackageImportViolations()).toStrictEqual([]);
    expect(collectConsumerImportViolations()).toStrictEqual([]);
    expect(collectEngineSeamViolations()).toStrictEqual({
      helperViolations: [],
      stateExportViolations: [],
    });
  });

  it("rejects private, stale, bare-root, deep, and evasive consumer imports", () => {
    const staleScope = "@view" + "-server";
    expect(
      consumerImportViolationsFor({
        contents: [
          'import "@effect-view-server/config";',
          `export * from "${staleScope}/runtime";`,
          'require("effect-view-server");',
          'import("effect-view-server/config/internal");',
          'import("effect-view-server/config");',
          "import(packageName);",
        ].join("\n"),
        fileName: "apps/example/src/main.mts",
        relativePath: "apps/example/src/main.mts",
      }),
    ).toStrictEqual([
      "apps/example/src/main.mts imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      `apps/example/src/main.mts imports ${staleScope}/runtime: stale View Server package scope; consumers must use approved effect-view-server/* subpaths.`,
      "apps/example/src/main.mts imports effect-view-server: the package root is not exported; consumers must use an approved effect-view-server/* subpath.",
      "apps/example/src/main.mts imports effect-view-server/config/internal: consumers must use approved effect-view-server/* package exports.",
      "apps/example/src/main.mts:6:1 uses unsupported non-literal-specifier module loading through dynamic-import.",
    ]);
  });

  it("preserves CommonMark fences while choosing safe virtual TypeScript filenames", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "# Examples",
          "```tsx",
          '<Panel>import "@effect-view-server/runtime"</Panel>',
          '{require("@effect-view-server/runtime")}',
          "```",
          "> ~~~mts",
          '> export * from "@effect-view-server/config";',
          "> ~~~",
          "```custom",
          '<Panel>import "@effect-view-server/server"</Panel>',
          "```",
        ].join("\n"),
        relativePath: "README.md",
      }),
    ).toStrictEqual([
      "README.md:2 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "README.md:6 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
    ]);
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "```ts",
          'import "@effect-view-server/config";',
          "```",
          "```typescript",
          'import "@effect-view-server/config";',
          "```",
          "```cts",
          'import "@effect-view-server/config";',
          "```",
          "```jsx",
          'import "@effect-view-server/config";',
          "```",
          "```js",
          'import "@effect-view-server/config";',
          "```",
          "```mjs",
          'import "@effect-view-server/config";',
          "```",
          "```cjs",
          'import "@effect-view-server/config";',
          "```",
        ].join("\n"),
        relativePath: "docs/languages.md",
      }),
    ).toStrictEqual(
      [1, 4, 7, 10, 13, 16, 19].map(
        (line) =>
          `docs/languages.md:${line} imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.`,
      ),
    );
    expect(
      consumerImportViolationsFor({
        contents: 'import "effect";',
        relativePath: "apps/example/src/external.ts",
      }),
    ).toStrictEqual([]);
  });

  it("preserves CommonMark list containers, boundaries, and unclosed fences", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "- Example:",
          "    ```ts",
          '    import "@effect-view-server/config";',
          "    ```",
          "",
          "- Parent",
          "",
          "    - ~~~mts",
          '      export * from "@effect-view-server/server";',
          "      ~~~",
          "",
          "-\t```cts",
          '\tconst client = require("@effect-view-server/client");',
          "\t```",
        ].join("\n"),
        relativePath: "docs/list-containers.md",
      }),
    ).toStrictEqual([
      "docs/list-containers.md:2 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "docs/list-containers.md:8 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
      "docs/list-containers.md:12 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
    ]);
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          'Prose and `import "@effect-view-server/config"` are not executable.',
          "```sh",
          "vp run @effect-view-server/example#test",
          "```",
          "```json",
          '{ "task": "@effect-view-server/runtime" }',
          "```",
          "> ```ts",
          '> import "effect-view-server/config";',
          'import "@effect-view-server/server";',
          "```ts",
          'import "@effect-view-server/client";',
          "```",
          "~~~javascript",
          'import("@effect-view-server/runtime");',
          "~~",
        ].join("\n"),
        relativePath: "plans/fence-boundaries.md",
      }),
    ).toStrictEqual([
      "plans/fence-boundaries.md:11 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
      "plans/fence-boundaries.md:14 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
    ]);
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "10.",
          "    ```ts",
          '    import "@effect-view-server/runtime";',
          "    ```",
        ].join("\n"),
        relativePath: "docs/empty-ordered-item.md",
      }),
    ).toStrictEqual([
      "docs/empty-ordered-item.md:2 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
    ]);
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "Paragraph",
          "2. continuation",
          "    ```ts",
          '    import "@effect-view-server/server";',
          "    ```",
        ].join("\n"),
        relativePath: "docs/ordered-paragraph.md",
      }),
    ).toStrictEqual([]);
  });

  it("collects Markdown only from the approved consumer documentation scope", () => {
    const root = makeDirectory();
    for (const directory of [
      "apps/example",
      "docs/node_modules",
      "examples/example",
      "packages/facade",
      "plans",
      ".agents",
    ]) {
      mkdirSync(join(root, directory), { recursive: true });
    }
    const markdownWithPrivateImport = (specifier: string) =>
      ["```ts", `import "${specifier}";`, "```"].join("\n");
    for (const [path, specifier] of [
      ["README.md", "@effect-view-server/config"],
      ["apps/example/guide.md", "@effect-view-server/react"],
      ["docs/guide.md", "@effect-view-server/client"],
      ["examples/example/README.md", "@effect-view-server/runtime"],
      ["packages/facade/README.md", "@effect-view-server/server"],
      ["plans/active.md", "@effect-view-server/protocol"],
      ["packages/facade/internal.md", "@effect-view-server/config"],
      ["docs/node_modules/ignored.md", "@effect-view-server/config"],
      [".agents/README.md", "@effect-view-server/config"],
    ]) {
      writeFileSync(join(root, path), markdownWithPrivateImport(specifier));
    }
    writeFileSync(join(root, "packages", "not-a-package.txt"), "");

    expect(collectConsumerImportViolations(root)).toStrictEqual([
      "README.md:1 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "apps/example/guide.md:1 imports @effect-view-server/react: consumers must import the publishable effect-view-server/* facade.",
      "docs/guide.md:1 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
      "examples/example/README.md:1 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "packages/facade/README.md:1 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
      "plans/active.md:1 imports @effect-view-server/protocol: consumers must import the publishable effect-view-server/* facade.",
    ]);
    rmSync(root, { recursive: true });
  });

  it("scans TS, TSX, MTS, and CTS recursively and classifies their tests", () => {
    const root = makeDirectory();
    mkdirSync(join(root, "nested"));
    for (const fileName of ["a.ts", "b.tsx", "c.mts", "d.cts", "ignored.js"]) {
      writeFileSync(join(root, fileName), "");
    }
    writeFileSync(join(root, "nested", "e.test.mts"), "");

    expect(sourceFiles(root).map((path) => path.slice(root.length + 1))).toStrictEqual([
      "a.ts",
      "b.tsx",
      "c.mts",
      "d.cts",
      "nested/e.test.mts",
    ]);
    expect(sourceFiles(join(root, "missing"))).toStrictEqual([]);
    expect(
      ["test", "test-d", "bench"].flatMap((kind) =>
        ["ts", "tsx", "mts", "cts"].map((extension) =>
          isTestFile(`src/a.${kind}.${extension}`),
        ),
      ),
    ).toStrictEqual(Array.from({ length: 12 }, () => true));
    expect(
      ["src/a.ts", "src/a.mts", "src/a.test.js", "src/a.integration.ts"].map(isTestFile),
    ).toStrictEqual([false, false, false, false]);
    rmSync(root, { recursive: true });
  });

  it("collects forbidden imports across every consumer source extension and ignores generated trees", () => {
    const root = makeDirectory();
    for (const directory of [
      "apps/z-app/src",
      "apps/z-app/node_modules",
      "apps/z-app/dist",
      "examples/a-example/src",
      "examples/a-example/coverage",
      "examples/a-example/.vite",
    ]) {
      mkdirSync(join(root, directory), { recursive: true });
    }
    for (const [path, contents] of [
      ["apps/z-app/vite.config.ts", 'import "@effect-view-server/config";'],
      ["apps/z-app/src/component.test.tsx", 'import "@effect-view-server/react";'],
      ["examples/a-example/src/module.test.mts", 'export * from "@effect-view-server/server";'],
      ["examples/a-example/runtime.cts", 'require("@effect-view-server/runtime");'],
      ["apps/z-app/node_modules/private.ts", 'import "@effect-view-server/config";'],
      ["apps/z-app/dist/generated.tsx", 'import "@effect-view-server/config";'],
      ["examples/a-example/coverage/ignored.mts", 'import "@effect-view-server/config";'],
      ["examples/a-example/.vite/ignored.cts", 'import "@effect-view-server/config";'],
    ]) {
      writeFileSync(join(root, path), contents);
    }

    expect(collectConsumerImportViolations(root)).toStrictEqual([
      "apps/z-app/src/component.test.tsx imports @effect-view-server/react: consumers must import the publishable effect-view-server/* facade.",
      "apps/z-app/vite.config.ts imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "examples/a-example/runtime.cts imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "examples/a-example/src/module.test.mts imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
    ]);
    rmSync(root, { recursive: true });
  });

  it("enforces policy-owned package direction, deep paths, relative escapes, and file overrides", () => {
    const reactPolicy = privatePackageSurfaceFor("react");

    expect(
      packageImportViolationsForSource({
        contents: [
          'import "@effect-view-server/client/remote";',
          'import "@effect-view-server/runtime";',
          'import "@effect-view-server/config/private";',
          'import "effect-view-server/react";',
          'import "@view-server/config";',
          'import ".";',
          'import "./local";',
          'import "../../runtime/src/index";',
          "import(packageName);",
        ].join("\n"),
        fileName: "/repo/packages/react/src/index.tsx",
        packagePolicy: reactPolicy,
        packageRoot: "/repo/packages/react",
        path: "/repo/packages/react/src/index.tsx",
      }),
    ).toStrictEqual([
      "packages/react/src/index.tsx imports @effect-view-server/runtime: React bindings may use client transports but must not import runtime, server, engine, or in-memory outside the testing entrypoint.",
      "packages/react/src/index.tsx imports @effect-view-server/config/private: View Server imports must use approved package exports.",
      "packages/react/src/index.tsx imports effect-view-server/react: public effect-view-server facade is for consumers; internal packages must import @effect-view-server/* workspace packages.",
      "packages/react/src/index.tsx imports @view-server/config: stale View Server package scope; use @effect-view-server/* workspace packages.",
      "packages/react/src/index.tsx imports ../../runtime/src/index: relative imports must not cross package seams.",
      "packages/react/src/index.tsx:9:1 uses unsupported non-literal-specifier module loading through dynamic-import.",
    ]);

    expect(
      packageImportViolationsForSource({
        contents: [
          'import "@effect-view-server/in-memory";',
          'import "@effect-view-server/in-memory/testing";',
        ].join("\n"),
        fileName: "/repo/packages/react/src/testing.tsx",
        packagePolicy: reactPolicy,
        packageRoot: "/repo/packages/react",
        path: "/repo/packages/react/src/testing.tsx",
      }),
    ).toStrictEqual([]);
  });

  it("rejects exact manifest drift and exact Vite+ pack drift", () => {
    const clientSurface = expectedPackageSurfaceFor("client");
    const manifest = JSON.stringify({
      name: "@effect-view-server/client",
      exports: {
        ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
        "./remote": { types: "./dist/remote.d.ts", import: "../escape.js" },
        "./extra": { types: "./dist/extra.d.ts", import: "./dist/extra.js" },
      },
    });

    expect(
      packageSurfaceViolationsForManifest({ manifestContents: manifest, surface: clientSurface }),
    ).toStrictEqual([
      "packages/client/package.json has unapproved export ./extra.",
      "packages/client/package.json export ./remote import target must be ./dist/remote.js, received ../escape.js.",
    ]);
    expect(
      packageSurfaceViolationsForManifest({ manifestContents: "[]", surface: clientSurface }),
    ).toStrictEqual(["packages/client/package.json must contain an object."]);
    expect(
      packageSurfaceViolationsForManifest({
        manifestContents: JSON.stringify({ name: "wrong", exports: [] }),
        surface: clientSurface,
      }),
    ).toStrictEqual([
      "packages/client/package.json name must be @effect-view-server/client, received wrong.",
      "packages/client/package.json exports must be an exact object.",
    ]);
    expect(
      packageSurfaceViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            ".": "./dist/index.js",
            "./remote": {
              types: "./dist/wrong.d.ts",
              import: "./dist/remote.js",
              default: "./dist/remote.js",
            },
          },
        }),
        surface: clientSurface,
      }),
    ).toStrictEqual([
      "packages/client/package.json export . must have exact types and import targets.",
      "packages/client/package.json export ./remote has unapproved condition default.",
      "packages/client/package.json export ./remote types target must be ./dist/remote.d.ts, received ./dist/wrong.d.ts.",
    ]);
    expect(
      packageSurfaceViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
          },
        }),
        surface: clientSurface,
      }),
    ).toStrictEqual(["packages/client/package.json is missing export ./remote."]);
    expect(
      packageSurfaceViolationsForViteConfig({
        surface: clientSurface,
        viteConfigContents:
          'export default { pack: libraryPack(["src/index.ts", "src/index.ts", "src/remote.ts"]) };',
      }),
    ).toStrictEqual(["packages/client/vite.config.ts repeats pack entry src/index.ts."]);
    expect(
      packageSurfaceViolationsForViteConfig({
        surface: clientSurface,
        viteConfigContents: "export default { pack: libraryPack(entries) };",
      }),
    ).toContain(
      "packages/client/vite.config.ts:1:36 uses a non-literal libraryPack declaration.",
    );
    expect(
      packageSurfaceViolationsForViteConfig({
        surface: clientSurface,
        viteConfigContents: "export default {};",
      }),
    ).toStrictEqual([
      "packages/client/vite.config.ts:1:16 does not declare package pack in its default config object.",
      "packages/client/vite.config.ts is missing pack entry src/index.ts.",
      "packages/client/vite.config.ts is missing pack entry src/remote.ts.",
    ]);
    expect(
      packageSurfaceViolationsForViteConfig({
        surface: clientSurface,
        viteConfigContents:
          'export default { pack: libraryPack(["src/index.ts", "src/remote.mts", "../escape.cts"]) };',
      }),
    ).toStrictEqual([
      "packages/client/vite.config.ts is missing pack entry src/remote.ts.",
      "packages/client/vite.config.ts has unapproved pack entry ../escape.cts.",
      "packages/client/vite.config.ts has unapproved pack entry src/remote.mts.",
    ]);
    expect(
      packageSurfaceViolationsForViteConfig({
        surface: clientSurface,
        viteConfigContents: [
          "export default {",
          '  pack: libraryPack(["src/index.ts"]),',
          '  pack: libraryPack(["src/remote.ts"]),',
          "  ...override,",
          "};",
        ].join("\n"),
      }),
    ).toStrictEqual([
      "packages/client/vite.config.ts:4:3 may override package pack through a spread or computed property.",
      "packages/client/vite.config.ts:3:3 declares package pack more than once.",
      "packages/client/vite.config.ts is missing pack entry src/remote.ts.",
    ]);
  });

  it("requires every private policy source and pack-only entrypoint to exist", () => {
    const root = makeDirectory();
    const clientSurface = expectedPackageSurfaceFor("client");
    const configSurface = expectedPackageSurfaceFor("config");
    mkdirSync(join(root, "packages", "client", "src"), { recursive: true });
    mkdirSync(join(root, "packages", "config", "src"), { recursive: true });
    writeFileSync(join(root, "packages", "client", "src", "index.ts"), "");
    for (const sourceEntrypoint of configSurface.packEntrypoints.filter(
      (entrypoint) => entrypoint !== "src/grpc-contract.ts",
    )) {
      writeFileSync(join(root, "packages", "config", sourceEntrypoint), "");
    }

    expect(packageSourceViolationsFor({ repositoryRoot: root, surface: clientSurface })).toStrictEqual([
      "packages/client/src/remote.ts is missing.",
    ]);
    expect(packageSourceViolationsFor({ repositoryRoot: root, surface: configSurface })).toStrictEqual([
      "packages/config/src/grpc-contract.ts is missing.",
    ]);
    rmSync(root, { recursive: true });
  });

  it("rejects package directories and missing surfaces outside the declared inventory", () => {
    const root = makeDirectory();
    const emptyRoot = makeDirectory();
    mkdirSync(join(root, "packages", "backdoor"), { recursive: true });

    expect(collectPackageSurfaceViolations(root)).toContain(
      "packages/backdoor is not declared by the Package Surface Policy.",
    );
    expect(collectPackageSurfaceViolations(root)).toContain(
      "packages/client/package.json is missing.",
    );
    expect(collectPackageSurfaceViolations(root)).toContain(
      "packages/client/vite.config.ts is missing.",
    );
    expect(collectPackageSurfaceViolations(root)).toContain(
      "packages/effect-view-server/src/client.ts is missing.",
    );
    expect(collectPackageSurfaceViolations(emptyRoot)).toContain(
      "packages/client/package.json is missing.",
    );
    expect(collectPackageSurfaceViolations(emptyRoot)).toContain(
      "packages/client/src/remote.ts is missing.",
    );
    expect(collectPackageSurfaceViolations(emptyRoot)).toContain(
      "packages/config/src/grpc-contract.ts is missing.",
    );
    rmSync(root, { recursive: true });
    rmSync(emptyRoot, { recursive: true });
  });

  it("requires facade files to be exact reexport-only projections", () => {
    const clientProjection = facadeProjectionFor("effect-view-server/client");
    const kafkaProjection = facadeProjectionFor("effect-view-server/config/kafka");

    expect(
      facadeProjectionViolationsForSource({
        contents: [
          'export * from "@effect-view-server/client";',
          'import "@effect-view-server/runtime";',
          "export const leak = true;",
          "import(packageName);",
        ].join("\n"),
        fileName: "packages/effect-view-server/src/client.ts",
        projection: clientProjection,
        relativePath: "packages/effect-view-server/src/client.ts",
      }),
    ).toStrictEqual([
      "packages/effect-view-server/src/client.ts:4:1 uses unsupported non-literal-specifier module loading through dynamic-import.",
      "packages/effect-view-server/src/client.ts must contain only package re-export declarations.",
    ]);
    expect(
      facadeProjectionViolationsForSource({
        contents: [
          'export { decodeKafkaCodec as decode } from "@effect-view-server/config/kafka";',
          'export * from "@effect-view-server/config";',
        ].join("\n"),
        fileName: "packages/effect-view-server/src/config-kafka.ts",
        projection: kafkaProjection,
        relativePath: "packages/effect-view-server/src/config-kafka.ts",
      }),
    ).toStrictEqual([
      "packages/effect-view-server/src/config-kafka.ts must exactly re-export the curated runtime and type symbols from @effect-view-server/config/kafka.",
    ]);
    expect(
      facadeProjectionViolationsForSource({
        contents: 'export type * from "@effect-view-server/client";',
        fileName: "packages/effect-view-server/src/client.ts",
        projection: clientProjection,
        relativePath: "packages/effect-view-server/src/client.ts",
      }),
    ).toStrictEqual([
      "packages/effect-view-server/src/client.ts must exclusively re-export all of @effect-view-server/client.",
    ]);
  });

  it("keeps runtime-only Kafka symbols out of the public config source", () => {
    const policy = sourceForbiddenExportPolicyFor("@effect-view-server/config");
    const input = {
      fileName: "packages/config/src/index.ts",
      policy,
      relativePath: "packages/config/src/index.ts",
    };

    expect(
      sourceForbiddenExportViolationsForSource({
        ...input,
        contents: [
          "export const decodeKafkaTopicMessage = safe;",
          "export interface KafkaDecodedTopicMessage {}",
          "export type KafkaDecodedTopicSourceMessage = Safe;",
          "export function KafkaResolvedSourceTopicDefinition() {}",
          "export const publicValue = true;",
        ].join("\n"),
      }),
    ).toStrictEqual([
      "packages/config/src/index.ts exports forbidden package-surface symbol decodeKafkaTopicMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaDecodedTopicMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaDecodedTopicSourceMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaResolvedSourceTopicDefinition.",
    ]);
    expect(
      sourceForbiddenExportViolationsForSource({
        ...input,
        contents: [
          "const safe = true;",
          "export { decodeKafkaTopicMessage as decode };",
          "export { safe as KafkaDecodedTopicMessage };",
          'export * as kafkaInternal from "./kafka-contract";',
          'export * from "./other";',
          "export default KafkaDecodedTopicSourceMessage;",
        ].join("\n"),
      }),
    ).toStrictEqual([
      "packages/config/src/index.ts namespace re-export cannot prove the forbidden source export policy for @effect-view-server/config.",
      "packages/config/src/index.ts wildcard re-export cannot prove the forbidden source export policy for @effect-view-server/config.",
      "packages/config/src/index.ts exports forbidden package-surface symbol decodeKafkaTopicMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaDecodedTopicMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaDecodedTopicSourceMessage.",
    ]);
    expect(
      sourceForbiddenExportViolationsForSource({
        ...input,
        contents: [
          "// decodeKafkaTopicMessage",
          'const documentation = "KafkaDecodedTopicMessage";',
          "export { documentation };",
        ].join("\n"),
      }),
    ).toStrictEqual([]);
    expect(
      sourceForbiddenExportViolationsForSource({
        ...input,
        contents: [
          'export { "decodeKafkaTopicMessage" as decode } from "./kafka-contract";',
          'export { safe as "KafkaDecodedTopicMessage" } from "./other";',
        ].join("\n"),
      }),
    ).toStrictEqual([
      "packages/config/src/index.ts exports forbidden package-surface symbol decodeKafkaTopicMessage.",
      "packages/config/src/index.ts exports forbidden package-surface symbol KafkaDecodedTopicMessage.",
    ]);
  });

  it("keeps Topic Store helpers local and rejects state reexports", () => {
    expect(
      topicStoreHelperViolationsForFile({
        contents: "const rows = topicStoreReadModel(state);",
        path: "/repo/packages/column-live-view-engine/src/active-query.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/active-query.ts references topicStoreReadModel outside the Topic Store Module.",
    ]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: 'export * from "./topic-store-state";',
        path: "/repo/packages/column-live-view-engine/src/topic-store.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/topic-store.ts wildcard re-exports restricted Topic Store state internals.",
    ]);
    expect(
      topicStoreHelperViolationsForFile({
        contents: [
          "// topicStoreState",
          'const text = "topicStoreReadModel";',
          "const value = safeState;",
        ].join("\n"),
        path: "/repo/packages/column-live-view-engine/src/active-query.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: "export { topicStoreState as state };",
        path: "/repo/packages/column-live-view-engine/src/topic-store.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/topic-store.ts named re-exports restricted Topic Store state internals.",
    ]);
    expect(
      topicStoreHelperViolationsForFile({
        contents: 'const state = State["topicStoreState"];',
        path: "/repo/packages/column-live-view-engine/src/active-query.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/active-query.ts references topicStoreState outside the Topic Store Module.",
    ]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: [
          'import * as State from "./topic-store-state";',
          "export { State };",
        ].join("\n"),
        path: "/repo/packages/column-live-view-engine/src/topic-store.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/topic-store.ts namespace imports restricted Topic Store state internals.",
    ]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: [
          'import * as Other from "./other";',
          'export * as State from "./topic-store-state";',
          "export { safe as topicStoreState };",
        ].join("\n"),
        path: "/repo/packages/column-live-view-engine/src/topic-store.ts",
        repositoryRoot: "/repo",
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/topic-store.ts namespace re-exports restricted Topic Store state internals.",
      "packages/column-live-view-engine/src/topic-store.ts named re-exports restricted Topic Store state internals.",
    ]);
  });

  it("formats each checker failure through its public assertion Interface", () => {
    expect(() => assertNoPackageSurfaceViolations(["surface"])).toThrowError(
      "Package Surface Policy violations found.\n- surface",
    );
    expect(() => assertNoPackageImportViolations(["package"])).toThrowError(
      "Package architecture seam violations found.\n- package",
    );
    expect(() => assertNoConsumerImportViolations(["consumer"])).toThrowError(
      "Consumer facade import violations found.\n- consumer",
    );
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations: ["helper"],
        stateExportViolations: [],
      }),
    ).toThrowError("Topic Store Module seam violations found.\n- helper");
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations: ["state"],
      }),
    ).toThrowError("Topic Store Module seam violations found.\n- state");

    assertNoPackageSurfaceViolations([]);
    assertNoPackageImportViolations([]);
    assertNoConsumerImportViolations([]);
    assertNoEngineSeamViolations({ helperViolations: [], stateExportViolations: [] });
    expect(() => privatePackageSurfaceFor("missing")).toThrowError(
      "Unknown private package policy directory: missing",
    );
    expect(() => expectedPackageSurfaceFor("missing")).toThrowError(
      "Unknown package surface policy directory: missing",
    );
    expect(() => facadeProjectionFor("missing")).toThrowError(
      "Unknown facade projection policy directory: missing",
    );
    expect(
      topicStoreHelperViolationsForFile({
        contents: "const state = topicStoreState;",
        path: join(
          process.cwd(),
          "packages/column-live-view-engine/src/topic-store-mutation.ts",
        ),
      }),
    ).toStrictEqual([]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: "export const topicStoreState = true;",
        path: join(process.cwd(), "packages/column-live-view-engine/src/topic-store-state.ts"),
      }),
    ).toStrictEqual([]);
  });
});
