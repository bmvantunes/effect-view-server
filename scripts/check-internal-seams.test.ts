import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  assertNoConsumerImportViolations,
  assertNoPackageImportViolations,
  assertNoPackageExportViolations,
  assertNoEngineSeamViolations,
  collectConsumerImportViolations,
  collectEngineSeamViolations,
  collectPackageExportViolations,
  collectPackageImportViolations,
  consumerImportViolationsFor,
  consumerImportViolationMessage,
  consumerMarkdownImportViolationsFor,
  importSpecifiersFromSource,
  isTestFile,
  libraryPackEntrypointPaths,
  packageExportSpecifiersForManifest,
  packageExportViolationMessage,
  packageExportViolationsForManifest,
  packedEntrypointsFromViteConfigContents,
  packedPackageEntrypointsForPackage,
  packageImportViolationsFor,
  packageImportViolationsForFile,
  packageImportViolationMessage,
  packageRelativeImportViolationsFor,
  sourceFiles,
  sourceEntrypointForPackEntry,
  sourceEntrypointForRelativeDistEntrypoint,
  sourceWithoutComments,
  staleApprovedPackageExportViolations,
  topicStoreHelperViolationMessage,
  topicStoreHelperViolationsForFile,
  topicStoreStateExportViolationMessage,
  topicStoreStateExportViolationsForFile,
  toPosixRelativePath,
} from "./check-internal-seams";

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-internal-seams-"));

describe("internal seam checker", () => {
  it("rejects private workspace imports from consumer source", () => {
    expect(
      consumerImportViolationsFor({
        contents: 'import { defineViewServerConfig } from "@effect-view-server/config";',
        relativePath: "apps/example/src/view-server.config.ts",
      }),
    ).toStrictEqual([
      "apps/example/src/view-server.config.ts imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("allows only approved publishable consumer subpaths", () => {
    const staleScope = "@view" + "-server";

    expect(
      consumerImportViolationsFor({
        contents: [
          'import "@effect-view-server/config";',
          'import "@effect-view-server/runtime/internal";',
          `import "${staleScope}/react";`,
          'import "effect-view-server";',
          'import "effect-view-server/config/internal";',
          'import "effect-view-server/config";',
          'import "effect-view-server/react/testing";',
          'import "effect-view-server/runtime";',
          'import "effect";',
        ].join("\n"),
        relativePath: "examples/example/src/index.ts",
      }),
    ).toStrictEqual([
      "examples/example/src/index.ts imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "examples/example/src/index.ts imports @effect-view-server/runtime/internal: consumers must import the publishable effect-view-server/* facade.",
      `examples/example/src/index.ts imports ${staleScope}/react: stale View Server package scope; consumers must use approved effect-view-server/* subpaths.`,
      "examples/example/src/index.ts imports effect-view-server: the package root is not exported; consumers must use an approved effect-view-server/* subpath.",
      "examples/example/src/index.ts imports effect-view-server/config/internal: consumers must use approved effect-view-server/* package exports.",
    ]);
  });

  it("rejects private workspace imports from consumer TypeScript code fences", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "# Consumer setup",
          "```ts",
          'import { defineViewServerConfig } from "@effect-view-server/config";',
          "```",
        ].join("\n"),
        relativePath: "README.md",
      }),
    ).toStrictEqual([
      "README.md:2 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks TSX, MTS, and CTS fences with Markdown fence semantics", () => {
    const staleScope = "@view" + "-server";

    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "# More examples",
          "~~~tsx",
          'import "@effect-view-server/react";',
          "```",
          "~~~~",
          "```mts",
          `export * from "${staleScope}/runtime";`,
          "``",
          "````",
          "````cts",
          'const facade = require("effect-view-server");',
          "`````",
          "```typescript",
          'import "effect-view-server/config";',
          "```",
        ].join("\r\n"),
        relativePath: "docs/examples.md",
      }),
    ).toStrictEqual([
      "docs/examples.md:2 imports @effect-view-server/react: consumers must import the publishable effect-view-server/* facade.",
      `docs/examples.md:6 imports ${staleScope}/runtime: stale View Server package scope; consumers must use approved effect-view-server/* subpaths.`,
      "docs/examples.md:10 imports effect-view-server: the package root is not exported; consumers must use an approved effect-view-server/* subpath.",
    ]);
  });

  it("checks fenced source inside blockquote containers", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "> ```ts",
          '> import "@effect-view-server/config";',
          "> ````",
          ">",
          "> > ~~~~custom-language",
          '> > export * from "@effect-view-server/runtime";',
          "> > ~~~~~",
        ].join("\n"),
        relativePath: "docs/blockquote-examples.md",
      }),
    ).toStrictEqual([
      "docs/blockquote-examples.md:1 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "docs/blockquote-examples.md:5 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks fenced source inside sufficiently indented list containers", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "- ```ts",
          '  import "@effect-view-server/client";',
          "  ````",
          "",
          "- Outer item",
          "  1. ~~~~custom-language",
          '       export * from "@effect-view-server/server";',
          "       ~~~~~",
          "",
          "-     ```ts",
          '      import "@effect-view-server/config";',
          "      ```",
          "",
          ">     ```ts",
          '>     import "@effect-view-server/react";',
          ">     ```",
          "",
          "    ```ts",
          '    import "@effect-view-server/runtime";',
          "    ```",
        ].join("\n"),
        relativePath: "examples/list-examples.md",
      }),
    ).toStrictEqual([
      "examples/list-examples.md:1 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
      "examples/list-examples.md:6 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks fences continued beneath list item content indentation", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "- Example:",
          "    ```ts",
          '    import "@effect-view-server/config";',
          "    ````",
          "",
          "10. Ordered example:",
          "    ~~~~custom-language",
          '    export * from "@effect-view-server/runtime";',
          "    ~~~~~",
          "",
          "- Parent",
          "",
          "    - ```ts",
          '      import "@effect-view-server/server";',
          "      ````",
          "",
          "- Tab-indented parent",
          "",
          "\t- ~~~~ts",
          '\t  import "@effect-view-server/client";',
          "\t  ~~~~~",
        ].join("\n"),
        relativePath: "docs/list-continuations.md",
      }),
    ).toStrictEqual([
      "docs/list-continuations.md:2 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "docs/list-continuations.md:7 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "docs/list-continuations.md:13 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
      "docs/list-continuations.md:19 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks a fence in a tab-padded list item", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "-\t```ts",
          '\timport "@effect-view-server/config";',
          "\t````",
        ].join("\n"),
        relativePath: "docs/tab-padded-list.md",
      }),
    ).toStrictEqual([
      "docs/tab-padded-list.md:1 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks a fence beneath an empty ordered list item", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "10.",
          "    ```ts",
          '    import "@effect-view-server/runtime";',
          "    ````",
        ].join("\n"),
        relativePath: "docs/empty-list-item.md",
      }),
    ).toStrictEqual([
      "docs/empty-list-item.md:2 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("does not treat a non-one ordered paragraph continuation as a list", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "Paragraph",
          "2. continuation",
          "    ```ts",
          '    import "@effect-view-server/server";',
          "    ```",
        ].join("\n"),
        relativePath: "docs/paragraph-continuation.md",
      }),
    ).toStrictEqual([]);
  });

  it("stops a contained fence when its Markdown container ends", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "> ```ts",
          '> import "effect-view-server/config";',
          'import "@effect-view-server/server";',
          "```ts",
          'import "@effect-view-server/client";',
          "```",
        ].join("\n"),
        relativePath: "docs/blockquote-boundary.md",
      }),
    ).toStrictEqual([
      "docs/blockquote-boundary.md:4 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
    ]);

    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "- Example:",
          "    ```ts",
          "",
          '    import "effect-view-server/config";',
          'import "@effect-view-server/runtime";',
        ].join("\n"),
        relativePath: "docs/list-boundary.md",
      }),
    ).toStrictEqual([]);
  });

  it("checks every fence while ignoring prose, inline code, task names, and data values", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          'Prose may explain `import "@effect-view-server/config"` without being executable.',
          "```sh",
          "vp run @effect-view-server/example#test",
          "```",
          "```json",
          '{ "task": "@effect-view-server/runtime" }',
          "```",
          "```ts",
          'const taskName = "@effect-view-server/runtime";',
          "```",
          "```text",
          'import "@effect-view-server/server";',
          "```",
          "```",
          'export * from "@effect-view-server/client";',
          "```",
          "```custom-language",
          'const protocol = require("@effect-view-server/protocol");',
          "```",
        ].join("\n"),
        relativePath: "plans/example.md",
      }),
    ).toStrictEqual([
      "plans/example.md:11 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
      "plans/example.md:14 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
      "plans/example.md:17 imports @effect-view-server/protocol: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("checks an unclosed source fence through end of file", () => {
    expect(
      consumerMarkdownImportViolationsFor({
        contents: [
          "Consumer example:",
          "~~~javascript",
          'const runtime = import("@effect-view-server/runtime");',
          "~~",
        ].join("\n"),
        relativePath: "examples/example/README.md",
      }),
    ).toStrictEqual([
      "examples/example/README.md:2 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("scans every supported TypeScript module extension recursively", () => {
    const directory = makeDirectory();
    const nested = join(directory, "nested");
    mkdirSync(nested);
    writeFileSync(join(directory, "index.ts"), "");
    writeFileSync(join(directory, "component.tsx"), "");
    writeFileSync(join(directory, "module.mts"), "");
    writeFileSync(join(directory, "common.cts"), "");
    writeFileSync(join(directory, "ignore.js"), "");
    writeFileSync(join(directory, "ignore.jsx"), "");
    writeFileSync(join(nested, "testing.tsx"), "");
    writeFileSync(join(nested, "testing.mts"), "");
    writeFileSync(join(nested, "testing.cts"), "");

    expect(sourceFiles(directory).map((path) => basename(path))).toStrictEqual([
      "common.cts",
      "component.tsx",
      "index.ts",
      "module.mts",
      "testing.cts",
      "testing.mts",
      "testing.tsx",
    ]);
  });

  it("collects consumer source violations across apps, examples, tests, and module extensions", () => {
    const directory = makeDirectory();
    const appRoot = join(directory, "apps", "z-app");
    const exampleRoot = join(directory, "examples", "a-example");
    mkdirSync(join(appRoot, "src"), { recursive: true });
    mkdirSync(join(appRoot, "node_modules"), { recursive: true });
    mkdirSync(join(appRoot, "dist"), { recursive: true });
    mkdirSync(join(exampleRoot, "src"), { recursive: true });
    mkdirSync(join(exampleRoot, "coverage"), { recursive: true });
    mkdirSync(join(exampleRoot, ".vite"), { recursive: true });
    writeFileSync(
      join(appRoot, "src", "component.test.tsx"),
      'import "@effect-view-server/react";',
    );
    writeFileSync(join(appRoot, "vite.config.ts"), 'import "@effect-view-server/config";');
    writeFileSync(
      join(exampleRoot, "runtime.cts"),
      'const runtime = require("@effect-view-server/runtime");',
    );
    writeFileSync(
      join(exampleRoot, "src", "module.test.mts"),
      'export * from "@effect-view-server/server";',
    );
    writeFileSync(
      join(appRoot, "node_modules", "private.ts"),
      'import "@effect-view-server/config";',
    );
    writeFileSync(join(appRoot, "dist", "generated.ts"), 'import "@effect-view-server/config";');
    writeFileSync(
      join(exampleRoot, "coverage", "ignored.mts"),
      'import "@effect-view-server/config";',
    );
    writeFileSync(
      join(exampleRoot, ".vite", "ignored.cts"),
      'import "@effect-view-server/config";',
    );

    expect(collectConsumerImportViolations(directory)).toStrictEqual([
      "apps/z-app/src/component.test.tsx imports @effect-view-server/react: consumers must import the publishable effect-view-server/* facade.",
      "apps/z-app/vite.config.ts imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "examples/a-example/runtime.cts imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "examples/a-example/src/module.test.mts imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("collects consumer Markdown violations from the approved documentation scope", () => {
    const directory = makeDirectory();
    mkdirSync(join(directory, "apps", "example"), { recursive: true });
    mkdirSync(join(directory, "docs"), { recursive: true });
    mkdirSync(join(directory, "examples", "example"), { recursive: true });
    mkdirSync(join(directory, "packages", "facade"), { recursive: true });
    mkdirSync(join(directory, "plans"), { recursive: true });
    mkdirSync(join(directory, ".agents"), { recursive: true });
    const markdownWithPrivateImport = (specifier: string) =>
      ["```ts", `import "${specifier}";`, "```"].join("\n");
    writeFileSync(
      join(directory, "README.md"),
      markdownWithPrivateImport("@effect-view-server/config"),
    );
    writeFileSync(
      join(directory, "apps", "example", "guide.md"),
      markdownWithPrivateImport("@effect-view-server/react"),
    );
    writeFileSync(
      join(directory, "docs", "guide.md"),
      markdownWithPrivateImport("@effect-view-server/client"),
    );
    writeFileSync(
      join(directory, "examples", "example", "README.md"),
      markdownWithPrivateImport("@effect-view-server/runtime"),
    );
    writeFileSync(
      join(directory, "packages", "facade", "README.md"),
      markdownWithPrivateImport("@effect-view-server/server"),
    );
    writeFileSync(
      join(directory, "plans", "active.md"),
      markdownWithPrivateImport("@effect-view-server/protocol"),
    );
    writeFileSync(
      join(directory, "packages", "facade", "internal.md"),
      markdownWithPrivateImport("@effect-view-server/config"),
    );
    writeFileSync(join(directory, "packages", "not-a-package.txt"), "");
    writeFileSync(
      join(directory, ".agents", "README.md"),
      markdownWithPrivateImport("@effect-view-server/config"),
    );

    expect(collectConsumerImportViolations(directory)).toStrictEqual([
      "README.md:1 imports @effect-view-server/config: consumers must import the publishable effect-view-server/* facade.",
      "apps/example/guide.md:1 imports @effect-view-server/react: consumers must import the publishable effect-view-server/* facade.",
      "docs/guide.md:1 imports @effect-view-server/client: consumers must import the publishable effect-view-server/* facade.",
      "examples/example/README.md:1 imports @effect-view-server/runtime: consumers must import the publishable effect-view-server/* facade.",
      "packages/facade/README.md:1 imports @effect-view-server/server: consumers must import the publishable effect-view-server/* facade.",
      "plans/active.md:1 imports @effect-view-server/protocol: consumers must import the publishable effect-view-server/* facade.",
    ]);
  });

  it("formats and throws consumer import violation summaries", () => {
    const violations = [
      "apps/example/src/index.ts imports @effect-view-server/config: consumers must use the facade.",
    ];

    expect(consumerImportViolationMessage(violations)).toStrictEqual(
      [
        "Consumer facade import violations found.",
        "- apps/example/src/index.ts imports @effect-view-server/config: consumers must use the facade.",
      ].join("\n"),
    );
    expect(() => assertNoConsumerImportViolations(violations)).toThrowError(
      "Consumer facade import violations found.",
    );
    expect(assertNoConsumerImportViolations([])).toStrictEqual(undefined);
  });

  it("keeps current consumer source and documentation imports facade-only", () => {
    expect(collectConsumerImportViolations()).toStrictEqual([]);
  });

  it("classifies tests and benchmarks for every supported TypeScript module extension", () => {
    const paths = [
      "src/index.test.ts",
      "src/component.test.tsx",
      "src/module.test.mts",
      "src/common.test.cts",
      "src/index.test-d.ts",
      "src/module.test-d.mts",
      "src/index.bench.ts",
      "src/component.bench.tsx",
      "src/module.bench.mts",
      "src/common.bench.cts",
      "src/index.ts",
      "src/test.ts",
    ];

    expect(paths.filter(isTestFile)).toStrictEqual(paths.slice(0, 10));
  });

  it("collects engine seam violations for restricted helpers and re-exports", () => {
    const engineFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "engine.ts",
    );
    const indexFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "index.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: engineFile,
      }),
    ).toStrictEqual(["packages/column-live-view-engine/src/engine.ts uses topicStoreState"]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: 'export { topicStoreState } from "./topic-store-state";',
        path: indexFile,
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      "packages/column-live-view-engine/src/index.ts has a restricted local state helper re-export",
    ]);
  });

  it("allows restricted engine helpers in their owning files", () => {
    const topicStoreStateFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "topic-store-state.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: topicStoreStateFile,
      }),
    ).toStrictEqual([]);
  });

  it("formats and throws engine seam violation summaries", () => {
    const helperViolations = ["packages/column-live-view-engine/src/engine.ts uses topicStoreState"];
    const stateExportViolations = [
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
    ];

    expect(topicStoreHelperViolationMessage(helperViolations)).toStrictEqual(
      [
        "Production engine modules must not use restricted TopicStore state helpers.",
        "Route query/read-model behavior through TopicStore helper operations instead.",
        "- packages/column-live-view-engine/src/engine.ts uses topicStoreState",
      ].join("\n"),
    );
    expect(topicStoreStateExportViolationMessage(stateExportViolations)).toStrictEqual(
      [
        "Production engine modules must not re-export restricted TopicStore state internals.",
        "- packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      ].join("\n"),
    );
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations,
        stateExportViolations: [],
      }),
    ).toThrowError("Production engine modules must not use restricted TopicStore state helpers.");
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations,
      }),
    ).toThrowError("Production engine modules must not re-export restricted TopicStore state internals.");
    expect(
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations: [],
      }),
    ).toStrictEqual(undefined);
  });

  it("keeps the current engine source free of internal seam violations", () => {
    expect(collectEngineSeamViolations()).toStrictEqual({
      helperViolations: [],
      stateExportViolations: [],
    });
  });

  it("reports restricted package imports including subexports and dynamic imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set([
        "@effect-view-server/in-memory",
        "@effect-view-server/runtime",
        "@effect-view-server/server",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "@effect-view-server/runtime";',
          'import { createInMemoryViewServer } from "@effect-view-server/in-memory";',
          'const runtime = import("@effect-view-server/runtime/internal");',
          "const server = import(`@effect-view-server/server`);",
          'import type { ViewServerLiveClient } from "@effect-view-server/client";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
      "src/index.tsx imports @effect-view-server/in-memory: React production must stay transport-neutral.",
      "src/index.tsx imports @effect-view-server/runtime/internal: View Server imports must use approved package exports.",
      "src/index.tsx imports @effect-view-server/server: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted package imports with escaped quoted specifiers", () => {
    const restriction = {
      forbiddenSpecifiers: new Set([
        "@effect-view-server/runtime",
        "@effect-view-server/server",
        "@effect-view-server/protocol",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "\\u0040effect-view-server/runtime";',
          'const server = require("\\x40effect-view-server/server");',
          'const protocol = import.meta.resolve("\\u{40}effect-view-server/protocol");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @effect-view-server/runtime: React production must stay transport-neutral.",
      "src/index.ts imports @effect-view-server/server: React production must stay transport-neutral.",
      "src/index.ts imports @effect-view-server/protocol: React production must stay transport-neutral.",
    ]);
  });

  it("reports stale View Server package scope imports", () => {
    const staleScope = "@view" + "-server";
    const restriction = {
      forbiddenSpecifiers: new Set<string>(),
      message: "unused",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          `import "${staleScope}/runtime";`,
          `const protocol = import("${staleScope}/protocol");`,
          `const client = require("${staleScope}/client");`,
          'const server = import("\\u0040view-server/server");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      `src/index.ts imports ${staleScope}/runtime: stale View Server package scope; use @effect-view-server/* workspace packages.`,
      `src/index.ts imports ${staleScope}/protocol: stale View Server package scope; use @effect-view-server/* workspace packages.`,
      `src/index.ts imports ${staleScope}/client: stale View Server package scope; use @effect-view-server/* workspace packages.`,
      `src/index.ts imports ${staleScope}/server: stale View Server package scope; use @effect-view-server/* workspace packages.`,
    ]);
  });

  it("reports public facade package imports from internal package source", () => {
    const restriction = {
      forbiddenSpecifiers: new Set<string>(),
      message: "unused",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import { createViewServerReact } from "effect-view-server/react";',
          'const runtime = import("effect-view-server/runtime");',
          'const server = require("effect-view-server/server");',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports effect-view-server/react: public effect-view-server facade is for consumers; internal packages must import @effect-view-server/* workspace packages.",
      "src/index.tsx imports effect-view-server/runtime: public effect-view-server facade is for consumers; internal packages must import @effect-view-server/* workspace packages.",
      "src/index.tsx imports effect-view-server/server: public effect-view-server facade is for consumers; internal packages must import @effect-view-server/* workspace packages.",
    ]);
  });

  it("does not report member APIs named import", () => {
    expect(
      importSpecifiersFromSource(
        [
          'registry.import("@effect-view-server/runtime");',
          'registry?.import("@effect-view-server/server");',
          'this.import("@effect-view-server/protocol");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores malformed escaped quoted specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          'import "\\u{}view-server/runtime";',
          'import "\\u{40view-server/missing-brace";',
          'import "\\u{110000}view-server/out-of-range";',
          'import "\\u{zz}view-server/server";',
          'const protocol = require("\\u12zzview-server/protocol");',
          'const client = import.meta.resolve("\\xzzview-server/client");',
          'const unfinished = require("\\',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "u{}view-server/runtime",
      "u{40view-server/missing-brace",
      "u{110000}view-server/out-of-range",
      "u{zz}view-server/server",
      "u12zzview-server/protocol",
      "xzzview-server/client",
    ]);
  });

  it("reports restricted CommonJS package imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require("@effect-view-server/runtime");',
          'const client = require("@effect-view-server/client");',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted createRequire package imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = createRequire(import.meta.url)("@effect-view-server/runtime");',
          'const server = createRequire(import.meta.url).resolve("@effect-view-server/server");',
          'const protocol = createRequire(import.meta.url).resolve.call(require, "@effect-view-server/protocol");',
          'const client = (createRequire(import.meta.url)).resolve("@effect-view-server/client");',
          'const config = (createRequire(import.meta.url))["resolve"]("@effect-view-server/config");',
          'const inMemory = (createRequire(import.meta.url)).resolve.call(require, "@effect-view-server/in-memory");',
          'function resolveRuntime() { return (createRequire(import.meta.url)).resolve("@effect-view-server/runtime/return"); }',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/protocol",
      "@effect-view-server/client",
      "@effect-view-server/config",
      "@effect-view-server/in-memory",
      "@effect-view-server/runtime/return",
    ]);
  });

  it("does not report member APIs named createRequire", () => {
    expect(
      importSpecifiersFromSource(
        [
          'factory.createRequire(import.meta.url)("@effect-view-server/runtime");',
          'this.#createRequire(import.meta.url)("@effect-view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores malformed createRequire package imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const factory = createRequire;",
          "const inertFactory = createRequire(import.meta.url);",
          "const inertResolve = createRequire(import.meta.url).resolve;",
          "const dynamicResolve = createRequire(import.meta.url).resolve(packageName);",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("reports restricted CommonJS package resolution", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require.resolve("@effect-view-server/runtime");',
          'const client = require.resolve("@effect-view-server/client");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports inside generic calls", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const runtime = loader<Runtime>(require("@effect-view-server/runtime"));',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after TypeScript angle-bracket assertions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const cast = <Runtime>value; const runtime = require("@effect-view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after less-than expressions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const ok = a < b; const runtime = require("@effect-view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("ignores require identifiers that are not literal calls", () => {
    expect(importSpecifiersFromSource("const label = require;")).toStrictEqual([]);
  });

  it("ignores require calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = require(packageName);")).toStrictEqual([]);
  });

  it("ignores require resolve calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = require.resolve(packageName);")).toStrictEqual(
      [],
    );
  });

  it("ignores require resolve property reads", () => {
    expect(importSpecifiersFromSource("const runtime = require.resolve;")).toStrictEqual([]);
  });

  it("detects optional CommonJS literal calls", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = require?.("@effect-view-server/runtime");',
          'const server = require.resolve?.("@effect-view-server/server");',
          'const client = require?.resolve?.("@effect-view-server/client");',
          'const spacedRuntime = require ?. ("@effect-view-server/runtime");',
          'const spacedServer = require . resolve ?. ("@effect-view-server/server");',
          'const spacedClient = require ?. resolve("@effect-view-server/client");',
          'const bracketRuntime = require?.["resolve"]("@effect-view-server/runtime");',
          'const bracketServer = require["resolve"]?.("@effect-view-server/server");',
          'const parenthesizedResolveRuntime = (require).resolve("@effect-view-server/runtime/resolve");',
          'const parenthesizedBracketResolveRuntime = (require)["resolve"]("@effect-view-server/runtime/bracket-resolve");',
          'const parenthesizedRuntime = (require)("@effect-view-server/runtime");',
          'function loadRuntime() { return (require)("@effect-view-server/runtime/return"); }',
          'const voidRuntime = void (require)("@effect-view-server/runtime/void");',
          'for (;;) (require)("@effect-view-server/runtime/for");',
          'if (ok) noop(); else (require)("@effect-view-server/runtime/else");',
          'if (path.endsWith(")")) (require)("@effect-view-server/runtime/if-string");',
          'if (path.match(/[)]/)) (require)("@effect-view-server/runtime/if-regex");',
          'if (ok /* ) */) (require)("@effect-view-server/runtime/if-block-comment");',
          'if (ok // )\n) (require)("@effect-view-server/runtime/if-line-comment");',
          'do (require)("@effect-view-server/runtime/do"); while (ok);',
          'const sequenceRuntime = (0, require)("@effect-view-server/runtime/sequence");',
          'const logicalRuntime = (false || require)("@effect-view-server/runtime/logical");',
          'const fallbackLogicalRuntime = (require || fallback)("@effect-view-server/runtime/logical-left");',
          'const parenthesizedFallbackLogicalRuntime = ((require) || fallback)("@effect-view-server/runtime/parenthesized-logical-left");',
          'const nullishRuntime = (require ?? fallback)("@effect-view-server/runtime/nullish-left");',
          'const nullishFallbackWithParenRuntime = (require ?? fallback(")"))("@effect-view-server/runtime/nullish-fallback-paren");',
          'const nullishFallbackRegexRuntime = (require ?? /[)]/)("@effect-view-server/runtime/nullish-fallback-regex");',
          'const fallbackNullishRuntime = (fallback ?? require)("@effect-view-server/runtime/nullish-right");',
          'const parenthesizedFallbackNullishRuntime = (fallback ?? (require))("@effect-view-server/runtime/parenthesized-nullish-right");',
          'const ternaryRuntime = (condition ? require : fallback)("@effect-view-server/runtime/ternary-then");',
          'const ternaryFallbackWithParenRuntime = (condition ? require : fallback({ text: ")" }))("@effect-view-server/runtime/ternary-fallback-paren");',
          'const fallbackTernaryRuntime = (condition ? fallback : require)("@effect-view-server/runtime/ternary-else");',
          'const nestedSequenceRuntime = ((0, require))("@effect-view-server/runtime/nested-sequence");',
          'const sequenceCalledRuntime = (0, require).call(undefined, "@effect-view-server/runtime/sequence-call");',
          'const sequenceBoundRuntime = (0, require).bind(undefined)("@effect-view-server/runtime/sequence-bind");',
          'const nestedRuntime = ((require))("@effect-view-server/runtime/nested");',
          'const calledRuntime = require.call(undefined, "@effect-view-server/runtime/call");',
          'const calledServer = (require).call(undefined, "@effect-view-server/server/call");',
          'const boundRuntime = require.bind(undefined)("@effect-view-server/runtime/bind");',
          'const boundArgumentRuntime = require.bind(undefined, "@effect-view-server/runtime/bind-argument")();',
          'const parenthesizedBoundRuntime = (require).bind(undefined)("@effect-view-server/runtime/parenthesized-bind");',
          'const regexBoundRuntime = require.bind(/,/)("@effect-view-server/runtime/regex-bind");',
          'const appliedRuntime = require.apply(undefined, ["@effect-view-server/runtime/apply"]);',
          'const extraAppliedRuntime = require.apply(undefined, ["@effect-view-server/runtime/extra-apply", extra]);',
          'const regexAppliedRuntime = require.apply(/,/, ["@effect-view-server/runtime/regex-apply"]);',
          'const nestedApplyRuntime = require.apply(fn("ignored", value), ["@effect-view-server/runtime/nested-apply"]);',
          'const regexRuntime = require.call(/,/, "@effect-view-server/runtime/regex");',
          'const quoteRegexRuntime = require.call(/"/, "@effect-view-server/runtime/quote-regex");',
          'const escapedRuntime = requ\\u0069re("@effect-view-server/runtime/escaped");',
          'const escapedBraceRuntime = requ\\u{69}re("@effect-view-server/runtime/escaped-brace");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/client",
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/client",
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/runtime/resolve",
      "@effect-view-server/runtime/bracket-resolve",
      "@effect-view-server/runtime",
      "@effect-view-server/runtime/return",
      "@effect-view-server/runtime/void",
      "@effect-view-server/runtime/for",
      "@effect-view-server/runtime/else",
      "@effect-view-server/runtime/if-string",
      "@effect-view-server/runtime/if-regex",
      "@effect-view-server/runtime/if-block-comment",
      "@effect-view-server/runtime/if-line-comment",
      "@effect-view-server/runtime/do",
      "@effect-view-server/runtime/sequence",
      "@effect-view-server/runtime/logical",
      "@effect-view-server/runtime/logical-left",
      "@effect-view-server/runtime/parenthesized-logical-left",
      "@effect-view-server/runtime/nullish-left",
      "@effect-view-server/runtime/nullish-fallback-paren",
      "@effect-view-server/runtime/nullish-fallback-regex",
      "@effect-view-server/runtime/nullish-right",
      "@effect-view-server/runtime/parenthesized-nullish-right",
      "@effect-view-server/runtime/ternary-then",
      "@effect-view-server/runtime/ternary-fallback-paren",
      "@effect-view-server/runtime/ternary-else",
      "@effect-view-server/runtime/nested-sequence",
      "@effect-view-server/runtime/sequence-call",
      "@effect-view-server/runtime/sequence-bind",
      "@effect-view-server/runtime/nested",
      "@effect-view-server/runtime/call",
      "@effect-view-server/server/call",
      "@effect-view-server/runtime/bind",
      "@effect-view-server/runtime/bind-argument",
      "@effect-view-server/runtime/parenthesized-bind",
      "@effect-view-server/runtime/regex-bind",
      "@effect-view-server/runtime/apply",
      "@effect-view-server/runtime/extra-apply",
      "@effect-view-server/runtime/regex-apply",
      "@effect-view-server/runtime/nested-apply",
      "@effect-view-server/runtime/regex",
      "@effect-view-server/runtime/quote-regex",
      "@effect-view-server/runtime/escaped",
      "@effect-view-server/runtime/escaped-brace",
    ]);
  });

  it("ignores malformed optional CommonJS accessors", () => {
    expect(
      importSpecifiersFromSource(
        [
          "require;",
          "const inertSequence = (0, require);",
          "const inertSequenceCall = (0, require).call(undefined);",
          'const localLoader = (require && localLoader)("@effect-view-server/runtime");',
          "const inertParenthesizedNullish = (fallback ?? (require));",
          "const inertNestedParenthesizedNullish = ((fallback ?? (require)));",
          'const unfinishedNullishWrapper = (require ?? fallback("@effect-view-server/runtime");',
          'const unfinishedTernaryWrapper = (condition ? require : fallback("@effect-view-server/runtime");',
          'const runtime = require ? ("@effect-view-server/runtime") : undefined;',
          'const server = require ? resolveCandidate("@effect-view-server/server") : undefined;',
          'const client = module ? requireCandidate("@effect-view-server/client") : undefined;',
          'const runtimeCandidate = module.load("@effect-view-server/runtime");',
          'const serverCandidate = module?.load("@effect-view-server/server");',
          'const parenthesizedRuntimeCandidate = (require).load("@effect-view-server/runtime");',
          'const parenthesizedServerCandidate = (module).load("@effect-view-server/server");',
          'const protocolCandidate = require["load"]("@effect-view-server/protocol");',
          'const missingBracket = require["resolve"("@effect-view-server/runtime");',
          'const malformedEscapedRequire = requ\\u{zz}re("@effect-view-server/runtime");',
          'const malformedCodePointRequire = requ\\u{110000}re("@effect-view-server/runtime");',
          'const malformedFixedEscapedRequire = requ\\u00zzre("@effect-view-server/server");',
          'const indirectLoader = makeLoader(require)("@effect-view-server/runtime");',
          'const indirectResolver = makeResolver(require.resolve)("@effect-view-server/runtime");',
          'const indirectParenthesizedResolver = makeResolver((require).resolve)("@effect-view-server/runtime");',
          'const indirectParenthesizedModuleRequire = makeResolver((module).require)("@effect-view-server/runtime");',
          'const indirectParenthesizedImportMetaResolve = makeResolver((import.meta).resolve).call(undefined, "@effect-view-server/runtime");',
          'const indirectImportMetaResolver = makeResolver(import.meta.resolve).call(undefined, "@effect-view-server/runtime");',
          'const malformedControl = if ok) (require)("@effect-view-server/runtime");',
          'const malformedOpenControl = if (ok (require)("@effect-view-server/runtime");',
          "const bindProperty = require.bind;",
          "const inertBind = require.bind(undefined);",
          "const unfinishedBind = require.bind(undefined",
          "const applyProperty = require.apply;",
          "const parenthesizedApplyProperty = (require).apply;",
          "const unfinishedNoCommaApply = require.apply(undefined",
          'const stringApply = require.apply(undefined, "@effect-view-server/runtime");',
          'const missingApplyArgument = require.apply(undefined);',
          "const dynamicApply = require.apply(undefined, packageName);",
          "const emptyApply = require.apply(undefined, []);",
          "const unfinishedEmptyApply = require.apply(undefined, [",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = (require ?? fallback")).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = (condition ? require : fallback")).toStrictEqual(
      [],
    );
  });

  it("detects bracketed CommonJS package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = require["resolve"]("@effect-view-server/runtime");',
          "const server = require['resolve']('@effect-view-server/server');",
          'const parenthesizedRuntime = (require.resolve)("@effect-view-server/runtime/parenthesized");',
          'const parenthesizedBaseRuntime = ((require).resolve)("@effect-view-server/runtime/parenthesized-base");',
          'for (const item of items) (require.resolve)("@effect-view-server/server/for-of");',
          'for await (const item of items) (require.resolve)("@effect-view-server/server/for-await");',
          'while (path.endsWith(")")) (require.resolve)("@effect-view-server/runtime/while-string");',
          'const ternaryParenthesizedResolve = (condition ? (require.resolve) : fallback)("@effect-view-server/runtime/ternary-parenthesized-resolve");',
          'const sequenceServer = (0, require.resolve)("@effect-view-server/server/sequence");',
          'const sequenceCalledServer = (0, require.resolve).call(require, "@effect-view-server/server/sequence-call");',
          'const calledRuntime = require.resolve.call(require, "@effect-view-server/runtime/call");',
          'const regexRuntime = require.resolve.call(/,/, "@effect-view-server/runtime/regex");',
          'const appliedServer = require.resolve.apply(require, ["@effect-view-server/server/apply", { paths: [] }]);',
          'const escapedServer = require.res\\u006flve("@effect-view-server/server/escaped");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/runtime/parenthesized",
      "@effect-view-server/runtime/parenthesized-base",
      "@effect-view-server/server/for-of",
      "@effect-view-server/server/for-await",
      "@effect-view-server/runtime/while-string",
      "@effect-view-server/runtime/ternary-parenthesized-resolve",
      "@effect-view-server/server/sequence",
      "@effect-view-server/server/sequence-call",
      "@effect-view-server/runtime/call",
      "@effect-view-server/runtime/regex",
      "@effect-view-server/server/apply",
      "@effect-view-server/server/escaped",
    ]);
  });

  it("detects import meta package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = import.meta.resolve("@effect-view-server/runtime");',
          'const server = import.meta.resolve?.("@effect-view-server/server");',
          'const protocol = import.meta["resolve"]("@effect-view-server/protocol");',
          'const client = (import.meta.resolve)("@effect-view-server/client");',
          'async function resolveRuntime() { return await (import.meta.resolve)("@effect-view-server/runtime/await"); }',
          'const sequenceClient = (0, import.meta.resolve)("@effect-view-server/client/sequence");',
          'const nestedSequenceClient = ((0, import.meta.resolve))("@effect-view-server/client/nested-sequence");',
          'const sequenceCalledClient = (0, import.meta.resolve).call(import.meta, "@effect-view-server/client/sequence-call");',
          'const ternaryParenthesizedCalledClient = (condition ? fallback : (import.meta.resolve)).call(import.meta, "@effect-view-server/client/ternary-parenthesized-call");',
          'const config = (import.meta["resolve"])?.("@effect-view-server/config");',
          'const parenthesizedBaseRuntime = (import.meta).resolve("@effect-view-server/runtime/parenthesized-base");',
          'const parenthesizedBaseServer = (import.meta)["resolve"]("@effect-view-server/server/parenthesized-base");',
          'const rpc = import.meta.resolve.call(import.meta, "@effect-view-server/protocol/rpc");',
          'const health = (import.meta.resolve).call(import.meta, "@effect-view-server/protocol/health");',
          'const runtimeAgain = import.meta.resolve.call(getMeta("ignored", import.meta), "@effect-view-server/runtime/internal");',
          'const regexClient = import.meta.resolve.call(/,/, "@effect-view-server/client/regex");',
          'const appliedClient = import.meta.resolve.apply(/,/, ["@effect-view-server/client/apply"]);',
          'const boundArgumentClient = import.meta.resolve.bind(import.meta, "@effect-view-server/client/bind-argument")();',
          'const nestedRuntime = ((import.meta.resolve))("@effect-view-server/runtime/nested");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/protocol",
      "@effect-view-server/client",
      "@effect-view-server/runtime/await",
      "@effect-view-server/client/sequence",
      "@effect-view-server/client/nested-sequence",
      "@effect-view-server/client/sequence-call",
      "@effect-view-server/client/ternary-parenthesized-call",
      "@effect-view-server/config",
      "@effect-view-server/runtime/parenthesized-base",
      "@effect-view-server/server/parenthesized-base",
      "@effect-view-server/protocol/rpc",
      "@effect-view-server/protocol/health",
      "@effect-view-server/runtime/internal",
      "@effect-view-server/client/regex",
      "@effect-view-server/client/apply",
      "@effect-view-server/client/bind-argument",
      "@effect-view-server/runtime/nested",
    ]);
  });

  it("ignores malformed import meta package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = import.meta.load("@effect-view-server/runtime");',
          'const server = import.metadata.resolve("@effect-view-server/server");',
          'const protocol = import ? meta.resolve("@effect-view-server/protocol") : undefined;',
          "const client = import.meta.resolve(packageName);",
          "const clientResolver = (import.meta.resolve);",
          "const config = import.meta.resolve.call;",
          'const effectUtils = import.meta.resolve.call("ignored");',
          'const serverPackage = import.meta.resolve.call("ignored", packageName);',
          'const runtimePackage = import.meta.resolve.call("quoted, comma");',
          "const unterminatedCall = import.meta.resolve.call(import.meta",
          'const falseRuntime = registry.import.meta.resolve("@effect-view-server/runtime");',
          'const falseServer = registry.import.meta.resolve.call(registry, "@effect-view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not report import meta resolution specifiers from call context arguments", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const fs = import.meta.resolve.call(["ignored", "@effect-view-server/runtime"], "node:fs");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("detects Node module.require literal calls", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = module.require("@effect-view-server/runtime");',
          'const server = module?.require("@effect-view-server/server");',
          'const protocol = module?.require?.("@effect-view-server/protocol");',
          'const client = module.require("@effect-view-server/client");',
          'const spacedRuntime = module ?. require("@effect-view-server/runtime");',
          'const spacedServer = module?. require("@effect-view-server/server");',
          'const spacedProtocol = module ?. require ?. ("@effect-view-server/protocol");',
          'const bracketRuntime = module["require"]("@effect-view-server/runtime");',
          'const bracketServer = module?.["require"]("@effect-view-server/server");',
          'const bracketProtocol = module["require"]?.("@effect-view-server/protocol");',
          'const parenthesizedBaseRuntime = (module).require("@effect-view-server/runtime/base");',
          'const parenthesizedBaseServer = (module)["require"]("@effect-view-server/server/base");',
          'const parenthesizedRuntime = (module.require)("@effect-view-server/runtime/parenthesized");',
          'if (ok) (module.require)("@effect-view-server/runtime/if");',
          'if (isEnabled()) (module.require)("@effect-view-server/runtime/if-call");',
          'const calledRuntime = module.require.call(module, "@effect-view-server/runtime/call");',
          'const boundRuntime = module.require.bind(module)("@effect-view-server/runtime/bind");',
          'const boundArgumentRuntime = module.require.bind(module, "@effect-view-server/runtime/bind-argument")();',
          'const appliedRuntime = module.require.apply(module, ["@effect-view-server/runtime/apply"]);',
          'const regexRuntime = module.require.call(/,/, "@effect-view-server/runtime/regex");',
          'const escapedProtocol = module.requ\\u0069re("@effect-view-server/protocol/escaped");',
        ].join("\n"),
      ),
    ).toStrictEqual([
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/protocol",
      "@effect-view-server/client",
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/protocol",
      "@effect-view-server/runtime",
      "@effect-view-server/server",
      "@effect-view-server/protocol",
      "@effect-view-server/runtime/base",
      "@effect-view-server/server/base",
      "@effect-view-server/runtime/parenthesized",
      "@effect-view-server/runtime/if",
      "@effect-view-server/runtime/if-call",
      "@effect-view-server/runtime/call",
      "@effect-view-server/runtime/bind",
      "@effect-view-server/runtime/bind-argument",
      "@effect-view-server/runtime/apply",
      "@effect-view-server/runtime/regex",
      "@effect-view-server/protocol/escaped",
    ]);
  });

  it("ignores Node module.require calls without quoted specifiers", () => {
    expect(importSpecifiersFromSource("const runtime = module.require(packageName);")).toStrictEqual(
      [],
    );
  });

  it("ignores member APIs named module.require", () => {
    expect(
      importSpecifiersFromSource(
        [
          'loader.module.require("@effect-view-server/runtime");',
          'this.#module.require("@effect-view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores private member APIs named require", () => {
    expect(
      importSpecifiersFromSource('class Loader { load() { return this.#require("@effect-view-server/runtime"); } }'),
    ).toStrictEqual([]);
  });

  it("ignores interpolated CommonJS template specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const packageName = 'runtime';",
          "const runtime = require(`@external/${packageName}`);",
          "const resolved = require.resolve(`@external/${packageName}`);",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("reports interpolated View Server CommonJS template specifiers conservatively", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const packageName = 'runtime';",
          "const runtime = require(`@effect-view-server/${packageName}`);",
          "const resolved = require.resolve(`@effect-view-server/${packageName}`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/${packageName}", "@effect-view-server/${packageName}"]);
  });

  it("ignores comments while scanning template expressions for CommonJS imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const value = `${// }",
          'require("@effect-view-server/runtime")',
          "}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
    expect(importSpecifiersFromSource("const unfinished = `${// }")).toStrictEqual([]);
    expect(importSpecifiersFromSource("const unfinished = `${/* }")).toStrictEqual([]);
  });

  it("does not treat regex literal slash pairs as comments in template expressions", () => {
    expect(
      importSpecifiersFromSource(
        'const value = `${/\\\\//.test(path) && require("@effect-view-server/runtime")}`;',
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        'const value = `${"source" in /\\\\// && require("@effect-view-server/server")}`;',
      ),
    ).toStrictEqual(["@effect-view-server/server"]);
  });

  it("does not treat regex literal slash pairs as comments before CommonJS imports", () => {
    expect(
      importSpecifiersFromSource(
        'const value = /\\\\//.test(path) && require("@effect-view-server/runtime");',
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        'const value = /[/]/gi.test(path) && require("@effect-view-server/server");',
      ),
    ).toStrictEqual(["@effect-view-server/server"]);
    expect(
      importSpecifiersFromSource(
        'if ("source" in /\\\\// && require("@effect-view-server/protocol")) {}',
      ),
    ).toStrictEqual(["@effect-view-server/protocol"]);
    expect(
      importSpecifiersFromSource(
        'if (path) /\\\\//.test(path) && require("@effect-view-server/client");',
      ),
    ).toStrictEqual(["@effect-view-server/client"]);
    expect(
      importSpecifiersFromSource(
        'if (fn({ value: true })) /[//]/.test(path) && require("@effect-view-server/runtime");',
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
    expect(
      importSpecifiersFromSource(
        [
          "if (",
          "  path",
          ') /[//]/.test(path) && require("@effect-view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/client"]);
    expect(
      importSpecifiersFromSource(
        'for (;;) /[//]/.test(path) && require("@effect-view-server/server");',
      ),
    ).toStrictEqual(["@effect-view-server/server"]);
    expect(
      importSpecifiersFromSource(
        'for (const value of /\\\\//) require("@effect-view-server/protocol");',
      ),
    ).toStrictEqual(["@effect-view-server/protocol"]);
  });

  it("detects no-substitution CommonJS template specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const runtime = require(`@effect-view-server/runtime`);",
          "const server = require.resolve(`@effect-view-server/server`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime", "@effect-view-server/server"]);
  });

  it("ignores member APIs named require", () => {
    expect(
      importSpecifiersFromSource(
        [
          'validator.require("@effect-view-server/runtime");',
          'this.require("@effect-view-server/server");',
          'loader?.require("@effect-view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not hide CommonJS imports after self-closing JSX", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const node = <Panel />; const runtime = require("@effect-view-server/runtime");',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("ignores comments while scanning JSX expressions for CommonJS imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: '<Panel value={/* } */ require("@effect-view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(
      packageImportViolationsFor({
        contents: ['<Panel value={// }', 'require("@effect-view-server/runtime")} />'].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(packageImportViolationsFor({
      contents: "<Panel value={// }",
      relativePath: "src/index.tsx",
      restriction,
    })).toStrictEqual([]);
  });

  it("does not treat regex literal slash pairs as comments in JSX expressions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: '<Panel value={/\\\\//.test(path) && require("@effect-view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
    expect(
      packageImportViolationsFor({
        contents: '<Panel value={"source" in /\\\\// && require("@effect-view-server/runtime")} />',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects deep imports even when the package root is allowed", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@effect-view-server/client"]),
      forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@effect-view-server/client";',
          'import { makeViewServerClient } from "@effect-view-server/client/remote/internal";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @effect-view-server/client/remote/internal: View Server imports must use approved package exports.",
    ]);
  });

  it("rejects approved subexports that are not explicitly allowed for a package", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@effect-view-server/client"]),
      forbiddenSpecifiers: new Set(["@effect-view-server/client"]),
      message: "Server code may depend on client contracts only.",
      packageName: "server",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@effect-view-server/client";',
          'import { makeViewServerClient } from "@effect-view-server/client/remote";',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @effect-view-server/client/remote: Server code may depend on client contracts only.",
    ]);
  });

  it("allows intentionally carved testing entrypoints", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@effect-view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@effect-view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@effect-view-server/in-memory";',
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("matches relative path carveouts across path separators", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@effect-view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@effect-view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@effect-view-server/in-memory";',
        relativePath: toPosixRelativePath("src\\testing.tsx"),
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("does not allow testing entrypoint carveouts to hide unrelated forbidden packages", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@effect-view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@effect-view-server/in-memory", "@effect-view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import { createInMemoryViewServer } from "@effect-view-server/in-memory";',
          'import { createViewServerRuntime } from "@effect-view-server/runtime";',
        ].join("\n"),
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/testing.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects relative imports that escape a package root", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageRelativeImportViolationsFor({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "../../runtime/src/index";',
        ].join("\n"),
        packageRoot,
        path,
      }),
    ).toStrictEqual([
      "src/index.tsx imports ../../runtime/src/index: relative imports must not cross package seams.",
    ]);
  });

  it("collects relative and package import violations for a package file", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageImportViolationsForFile({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "@effect-view-server/runtime";',
          'import { server } from "../../server/src/index";',
        ].join("\n"),
        packageRoot,
        path,
        restriction: {
          allowedSpecifiers: new Set(["@effect-view-server/client"]),
          forbiddenSpecifiers: new Set(["@effect-view-server/runtime"]),
          message: "React production must stay transport-neutral.",
          packageName: "react",
        },
      }),
    ).toStrictEqual([
      "packages/react/src/index.tsx imports ../../server/src/index: relative imports must not cross package seams.",
      "packages/react/src/index.tsx imports @effect-view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("formats and throws package import violation summaries", () => {
    const violations = ["packages/react/src/index.tsx imports @effect-view-server/runtime: no"];

    expect(packageImportViolationMessage(violations)).toStrictEqual(
      [
        "Package architecture seam violations found.",
        "- packages/react/src/index.tsx imports @effect-view-server/runtime: no",
      ].join("\n"),
    );
    expect(() => assertNoPackageImportViolations(violations)).toThrowError(
      "Package architecture seam violations found.",
    );
    expect(assertNoPackageImportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package import violations", () => {
    expect(collectPackageImportViolations()).toStrictEqual([]);
  }, 30000);

  it("collects package export specifiers from package manifests", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/example",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/example", "@effect-view-server/example/testing"]);
  });

  it("parses Vite+ libraryPack entrypoint declarations", () => {
    expect(libraryPackEntrypointPaths('export default { pack: libraryPack("src/index.ts") };')).toStrictEqual([
      "src/index.ts",
    ]);
    expect(
      libraryPackEntrypointPaths(
        [
          "export default {",
          "  pack: libraryPack([",
          '    "src/index.ts",',
          '    // "src/internal.ts",',
          '    "src/testing.tsx",',
          "  ]),",
          "};",
        ].join("\n"),
      ),
    ).toStrictEqual(["src/index.ts", "src/testing.tsx"]);
    expect(libraryPackEntrypointPaths("export default { fmt: {} };")).toStrictEqual([]);
  });

  it("normalizes safe libraryPack source entrypoints only", () => {
    expect(sourceEntrypointForPackEntry("src/index.ts")).toStrictEqual("index");
    expect(sourceEntrypointForPackEntry("src/testing.tsx")).toStrictEqual("testing");
    expect(sourceEntrypointForPackEntry("src/module.mts")).toStrictEqual("module");
    expect(sourceEntrypointForPackEntry("src/common.cts")).toStrictEqual("common");
    expect(sourceEntrypointForPackEntry("generated/index.ts")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/index.js")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/index.mjs")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/index.cjs")).toStrictEqual(undefined);
    expect(sourceEntrypointForPackEntry("src/../index.ts")).toStrictEqual(undefined);
  });

  it("collects packed entrypoints from Vite+ config contents", () => {
    expect(
      Array.from(
        packedEntrypointsFromViteConfigContents(
          [
            "export default {",
            "  pack: libraryPack([",
            '    "generated/index.ts",',
            '    "src/index.ts",',
            '    "src/feature.tsx",',
            '    "src/../escape.ts",',
            "  ]),",
            "};",
          ].join("\n"),
        ),
      ).sort(),
    ).toStrictEqual(["feature", "index"]);
  });

  it("collects packed entrypoints from package Vite+ config files", () => {
    expect(Array.from(packedPackageEntrypointsForPackage("react")).sort()).toStrictEqual(["index", "testing"]);
    expect(Array.from(packedPackageEntrypointsForPackage("missing-package"))).toStrictEqual([]);
  });

  it("resolves package source entrypoint files without normalizing missing files into existence", () => {
    expect(sourceEntrypointForRelativeDistEntrypoint("react", "testing")?.endsWith("src/testing.tsx")).toStrictEqual(
      true,
    );
    expect(sourceEntrypointForRelativeDistEntrypoint("react", "missing")).toStrictEqual(undefined);
  });

  it("resolves MTS and CTS package source entrypoint files", () => {
    const directory = makeDirectory();
    const sourceRoot = join(directory, "packages", "example", "src");
    mkdirSync(sourceRoot, { recursive: true });
    writeFileSync(join(sourceRoot, "module.mts"), "");
    writeFileSync(join(sourceRoot, "common.cts"), "");

    expect(
      sourceEntrypointForRelativeDistEntrypoint("example", "module", directory),
    ).toStrictEqual(join(sourceRoot, "module.mts"));
    expect(
      sourceEntrypointForRelativeDistEntrypoint("example", "common", directory),
    ).toStrictEqual(join(sourceRoot, "common.cts"));
  });

  it("collects root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            import: "./dist/index.js",
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/client"]);
  });

  it("collects types-only root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/client"]);
  });

  it("collects default-only root conditional package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            default: "./dist/index.js",
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/client"]);
  });

  it("keeps non-subpath keys readable in mixed package export maps", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/example",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
            types: "./dist/index.d.ts",
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/example", "@effect-view-server/example/types"]);
  });

  it("accepts root conditional package export maps for packed entries", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            import: "./dist/index.js",
            types: "./dist/index.d.ts",
          },
        }),
        packageDirectoryName: "client",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed TSX package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              browser: ["./dist/testing.js", null],
              default: "./dist/testing.js",
              import: "./dist/testing.js",
              node: null,
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts public effect-view-server facade package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "effect-view-server",
          exports: {
            "./client": {
              import: "./dist/client.js",
              types: "./dist/client.d.ts",
            },
            "./config": {
              import: "./dist/config.js",
              types: "./dist/config.d.ts",
            },
            "./react/testing": {
              import: "./dist/react-testing.js",
              types: "./dist/react-testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "effect-view-server",
      }),
    ).toStrictEqual([]);
  });

  it("rejects public effect-view-server root package export", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "effect-view-server",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "effect-view-server",
      }),
    ).toStrictEqual([
      "packages/effect-view-server/package.json exports effect-view-server: add intentional public specifier approval or remove the export.",
      "packages/effect-view-server/package.json export . points at ./dist/index.js without a matching packed src entrypoint.",
      "packages/effect-view-server/package.json export . points at ./dist/index.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("rejects unapproved public effect-view-server facade deep exports", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "effect-view-server",
          exports: {
            "./react/internal": {
              import: "./dist/react-internal.js",
              types: "./dist/react-internal.d.ts",
            },
          },
        }),
        packageDirectoryName: "effect-view-server",
      }),
    ).toStrictEqual([
      "packages/effect-view-server/package.json exports effect-view-server/react/internal: add intentional public specifier approval or remove the export.",
      "packages/effect-view-server/package.json export ./react/internal points at ./dist/react-internal.js without a matching packed src entrypoint.",
      "packages/effect-view-server/package.json export ./react/internal points at ./dist/react-internal.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("keeps runtime-only Kafka decoder symbols out of public config facades", () => {
    const forbiddenSymbols = [
      "decodeKafkaTopicMessage",
      "KafkaDecodedTopicMessage",
      "KafkaDecodedTopicSourceMessage",
      "KafkaResolvedSourceTopicDefinition",
    ];
    const publicEntrypoints = [
      {
        path: join(process.cwd(), "packages", "config", "src", "index.ts"),
        relativePath: "packages/config/src/index.ts",
      },
      {
        path: join(process.cwd(), "packages", "effect-view-server", "src", "config-kafka.ts"),
        relativePath: "packages/effect-view-server/src/config-kafka.ts",
      },
    ];

    expect(
      publicEntrypoints.map((entrypoint) => ({
        path: entrypoint.relativePath,
        leakedSymbols: forbiddenSymbols.filter((symbol) =>
          readFileSync(entrypoint.path, "utf8").includes(symbol),
        ),
      })),
    ).toStrictEqual([
      {
        path: "packages/config/src/index.ts",
        leakedSymbols: [],
      },
      {
        path: "packages/effect-view-server/src/config-kafka.ts",
        leakedSymbols: [],
      },
    ]);
  });

  it("accepts packed package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: [null, "./dist/testing.js"],
              types: [null, "./dist/testing.d.ts"],
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed conditional objects inside package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": [
              {
                import: "./dist/testing.js",
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed default conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              default: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed nested types conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              default: "./dist/testing.js",
              types: {
                default: "./dist/testing.d.ts",
              },
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed versioned TypeScript package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              "types@>=5.2": "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed nested runtime conditional package export entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              node: {
                import: "./dist/testing.js",
              },
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("accepts packed default conditional objects inside package export fallback arrays", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": [
              {
                browser: null,
                default: "./dist/testing.js",
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("collects root string package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: "./dist/index.js",
          name: "@effect-view-server/example",
        }),
      ),
    ).toStrictEqual(["@effect-view-server/example"]);
  });

  it("collects root fallback array package export maps as the root specifier", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: ["./dist/index.js"],
          name: "@effect-view-server/example",
        }),
      ),
    ).toStrictEqual(["@effect-view-server/example"]);
  });

  it("ignores package export violations from unsupported top-level export shapes", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([]);
  });

  it("ignores package export specifiers when the manifest has no package name", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
      ),
    ).toStrictEqual([]);
  });

  it("collects package export specifiers from string targets", () => {
    expect(
      packageExportSpecifiersForManifest(
        JSON.stringify({
          name: "@effect-view-server/example",
          exports: {
            ".": "./dist/index.js",
            "./array": ["./dist/array.js"],
            "./null": null,
          },
        }),
      ),
    ).toStrictEqual(["@effect-view-server/example", "@effect-view-server/example/array", "@effect-view-server/example/null"]);
  });

  it("reports unsupported package export map targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./array": ["./dist/array.js"],
            "./null": null,
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @effect-view-server/react/array: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./array points at ./dist/array.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./array has no types target.",
      "packages/react/package.json exports @effect-view-server/react/null: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./null has no import target.",
    ]);
  });

  it("reports package string exports through the same approval and source checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./internal": "./dist/internal.js",
            "./missing": "./dist/missing.js",
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @effect-view-server/react/internal: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./internal points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./internal has no types target.",
      "packages/react/package.json exports @effect-view-server/react/missing: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./missing points at ./dist/missing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./missing has no types target.",
    ]);
  });

  it("reports root string package exports through the same target checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: "./dist/index.js",
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export . has no types target."]);
  });

  it("reports root fallback array package exports through the same target checks", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: ["./dist/live-query-state.js"],
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export . points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export . has no types target.",
    ]);
  });

  it("reports types-only fallback array exports without runtime import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": [
              {
                types: "./dist/testing.d.ts",
              },
            ],
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("does not allow nested types import conditions to satisfy runtime import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              types: {
                import: "./dist/testing.d.ts",
              },
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("reports package exports that are not approved public specifiers", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./internal": {
              import: "./dist/internal.js",
              types: "./dist/internal.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports @effect-view-server/react/internal: add intentional public specifier approval or remove the export.",
      "packages/react/package.json export ./internal points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./internal points at ./dist/internal.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports from nameless manifests using the package directory label", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json exports packages/react: add intentional public specifier approval or remove the export.",
    ]);
  });

  it("reports package exports without import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no import target."]);
  });

  it("reports package exports without types targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual(["packages/react/package.json export ./testing has no types target."]);
  });

  it("reports package exports without matching source entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/missing.js",
              types: "./dist/missing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/missing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/missing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports unpacked import and types fallback array targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: ["./dist/live-query-state.js"],
              types: ["./dist/live-query-state.d.ts"],
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/live-query-state.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports unpacked conditional package export targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              browser: {
                default: "./dist/live-query-state.js",
              },
              default: "./dist/internal.js",
              import: "./dist/testing.js",
              node: ["./dist/internal.js"],
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition browser.default points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing condition default points at ./dist/internal.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing condition node[0] points at ./dist/internal.js without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports without a Vite+ pack config", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/client",
          exports: {
            ".": {
              import: "./dist/index.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "missing-package",
      }),
    ).toStrictEqual([
      "packages/missing-package/package.json export . points at ./dist/index.js without a matching packed src entrypoint.",
      "packages/missing-package/package.json export . points at ./dist/index.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with import targets outside dist entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./generated/testing.js",
              types: "./generated/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./generated/testing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./generated/testing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with traversal in dist entrypoint targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/../src/testing.js",
              types: "./dist/../src/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/../src/testing.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/../src/testing.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports that target unpacked source entrypoints", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/live-query-state.js",
              types: "./dist/live-query-state.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
      "packages/react/package.json export ./testing points at ./dist/live-query-state.d.ts without a matching packed src entrypoint.",
    ]);
  });

  it("does not report mismatch noise when the import target is unpacked", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/live-query-state.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing points at ./dist/live-query-state.js without a matching packed src entrypoint.",
    ]);
  });

  it("reports package exports with declaration targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing types target ./dist/index.d.ts does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports package exports with runtime condition targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              browser: "./dist/index.js",
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition browser target ./dist/index.js does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports package exports with versioned declaration targets that do not match import targets", () => {
    expect(
      packageExportViolationsForManifest({
        manifestContents: JSON.stringify({
          name: "@effect-view-server/react",
          exports: {
            "./testing": {
              import: "./dist/testing.js",
              types: "./dist/testing.d.ts",
              "types@>=5.2": "./dist/index.d.ts",
            },
          },
        }),
        packageDirectoryName: "react",
      }),
    ).toStrictEqual([
      "packages/react/package.json export ./testing condition types@>=5.2 target ./dist/index.d.ts does not match import target ./dist/testing.js.",
    ]);
  });

  it("reports stale approved public package export specifiers", () => {
    expect(
      staleApprovedPackageExportViolations({
        approvedSpecifiers: new Set(["@effect-view-server/client", "@effect-view-server/client/missing"]),
        exportedSpecifiers: new Set(["@effect-view-server/client"]),
      }),
    ).toStrictEqual([
      "@effect-view-server/client/missing is approved as public but is not exported by any package.json.",
    ]);
  });

  it("formats and throws package export violation summaries", () => {
    const violations = [
      "packages/react/package.json exports @effect-view-server/react/internal: remove it.",
    ];

    expect(packageExportViolationMessage(violations)).toStrictEqual(
      [
        "Package public export violations found.",
        "- packages/react/package.json exports @effect-view-server/react/internal: remove it.",
      ].join("\n"),
    );
    expect(() => assertNoPackageExportViolations(violations)).toThrowError(
      "Package public export violations found.",
    );
    expect(assertNoPackageExportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package export violations", () => {
    expect(collectPackageExportViolations()).toStrictEqual([]);
  });

  it("ignores import-like text in comments", () => {
    expect(
      sourceWithoutComments(
        [
          'import { client } from "@effect-view-server/client";',
          '// import { runtime } from "@effect-view-server/runtime";',
          '/* import { server } from "@effect-view-server/server"; */',
          'const example = "import from comment-like string";',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'import { client } from "@effect-view-server/client";',
        "",
        "",
        'const example = "import from comment-like string";',
      ].join("\n"),
    );
  });

  it("keeps regex literals while stripping comments", () => {
    expect(
      sourceWithoutComments(
        [
          'const value = /\\\\//.test(path) && require("@effect-view-server/runtime");',
          'const unfinished = /unterminated',
          '// require("@effect-view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'const value = /\\\\//.test(path) && require("@effect-view-server/runtime");',
        "const unfinished = /unterminated",
        "",
      ].join("\n"),
    );
    expect(sourceWithoutComments("const unfinished = /unterminated")).toStrictEqual(
      "const unfinished = /unterminated",
    );
  });

  it("does not treat import-like text inside strings as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const message = "Do not import from \\"@effect-view-server/runtime\\"";',
          "const docs = `import { server } from \"@effect-view-server/server\"`;",
          'import { client } from "@effect-view-server/client";',
          "const runtime = import(`@effect-view-server/runtime`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/client", "@effect-view-server/runtime"]);
  });

  it("does not treat import-like JSX text as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>Install from \"@effect-view-server/runtime\" and import from \"@effect-view-server/server\".</p>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX fragment text as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <>Install from \"@effect-view-server/runtime\" and import from \"@effect-view-server/server\".</>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text after logical operators as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return condition && <p>Install from \"@effect-view-server/runtime\".</p>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return fallback || <>Install from \"@effect-view-server/server\".</>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text inside underscore or dollar components as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <_Panel>Install from \"@effect-view-server/runtime\".</_Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <$Panel>Install from \"@effect-view-server/server\".</$Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not treat import-like JSX text after nested self-closing children as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <Panel><Icon />Install from \"@effect-view-server/runtime\".</Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not strip code after JSX text that looks like a block comment", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>/*</p>;",
          "}",
          'const runtime = require("@effect-view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
  });

  it("does not strip code after JSX text that looks like a line comment", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>//</p>;",
          "}",
          'const runtime = require("@effect-view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
  });

  it("detects imports inside JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel>{import(\"@effect-view-server/runtime\")}</Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
  });

  it("detects imports inside self-closing JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel value={import(\"@effect-view-server/runtime\")} />;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
  });

  it("handles unfinished JSX tag expressions conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel value={import(")).toStrictEqual(
      [],
    );
  });

  it("handles unfinished JSX tags conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel")).toStrictEqual([]);
  });

  it("handles unfinished JSX child expressions conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel>{import(")).toStrictEqual([]);
  });

  it("handles unfinished JSX roots conservatively", () => {
    expect(importSpecifiersFromSource("export const node = <Panel>")).toStrictEqual([]);
  });

  it("detects imports inside template literal expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const text = `plain import { server } from \"@effect-view-server/server\"`;",
          "const runtime = `${await import(\"@effect-view-server/runtime\")}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@effect-view-server/runtime"]);
  });

  it("handles unfinished template literal expressions conservatively", () => {
    expect(importSpecifiersFromSource("const text = `${await import(")).toStrictEqual([]);
  });

  it("handles unfinished plain template literals conservatively", () => {
    expect(importSpecifiersFromSource("const text = `unterminated")).toStrictEqual([]);
  });

  it("ignores unterminated quoted import specifiers", () => {
    expect(importSpecifiersFromSource('const broken = import("@effect-view-server/runtime')).toStrictEqual(
      [],
    );
  });

  it("ignores from keywords that are not followed by quoted specifiers", () => {
    expect(importSpecifiersFromSource("from notAString")).toStrictEqual([]);
  });
});
