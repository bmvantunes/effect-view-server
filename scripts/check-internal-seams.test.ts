import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  assertNoPackageImportViolations,
  assertNoEngineSeamViolations,
  collectEngineSeamViolations,
  collectPackageImportViolations,
  importSpecifiersFromSource,
  packageImportViolationsFor,
  packageImportViolationsForFile,
  packageImportViolationMessage,
  packageRelativeImportViolationsFor,
  sourceFiles,
  sourceWithoutComments,
  topicStoreHelperViolationMessage,
  topicStoreHelperViolationsForFile,
  topicStoreStateExportViolationMessage,
  topicStoreStateExportViolationsForFile,
  toPosixRelativePath,
} from "./check-internal-seams";

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-internal-seams-"));

describe("internal seam checker", () => {
  it("scans TypeScript and TSX source files recursively", () => {
    const directory = makeDirectory();
    const nested = join(directory, "nested");
    mkdirSync(nested);
    writeFileSync(join(directory, "index.ts"), "");
    writeFileSync(join(directory, "component.tsx"), "");
    writeFileSync(join(directory, "ignore.js"), "");
    writeFileSync(join(nested, "testing.tsx"), "");

    expect(sourceFiles(directory).map((path) => basename(path)).sort()).toStrictEqual([
      "component.tsx",
      "index.ts",
      "testing.tsx",
    ]);
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
        "@view-server/in-memory",
        "@view-server/runtime",
        "@view-server/server",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "@view-server/runtime";',
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'const runtime = import("@view-server/runtime/internal");',
          "const server = import(`@view-server/server`);",
          'import type { ViewServerLiveClient } from "@view-server/client";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/in-memory: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/runtime/internal: View Server imports must use approved package exports.",
      "src/index.tsx imports @view-server/server: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted CommonJS package imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require("@view-server/runtime");',
          'const client = require("@view-server/client");',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("reports restricted CommonJS package resolution", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'const runtime = require.resolve("@view-server/runtime");',
          'const client = require.resolve("@view-server/client");',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports inside generic calls", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const runtime = loader<Runtime>(require("@view-server/runtime"));',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after TypeScript angle-bracket assertions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const cast = <Runtime>value; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("does not hide CommonJS imports after less-than expressions", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const ok = a < b; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/runtime: React production must stay transport-neutral.",
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
          'const runtime = require?.("@view-server/runtime");',
          'const server = require.resolve?.("@view-server/server");',
          'const client = require?.resolve?.("@view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime", "@view-server/server", "@view-server/client"]);
  });

  it("detects bracketed CommonJS package resolution", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = require["resolve"]("@view-server/runtime");',
          "const server = require['resolve']('@view-server/server');",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime", "@view-server/server"]);
  });

  it("detects Node module.require literal calls", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const runtime = module.require("@view-server/runtime");',
          'const client = module.require("@view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime", "@view-server/client"]);
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
          'loader.module.require("@view-server/runtime");',
          'this.#module.require("@view-server/server");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("ignores private member APIs named require", () => {
    expect(
      importSpecifiersFromSource('class Loader { load() { return this.#require("@view-server/runtime"); } }'),
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
          "const runtime = require(`@view-server/${packageName}`);",
          "const resolved = require.resolve(`@view-server/${packageName}`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/${packageName}", "@view-server/${packageName}"]);
  });

  it("detects no-substitution CommonJS template specifiers", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const runtime = require(`@view-server/runtime`);",
          "const server = require.resolve(`@view-server/server`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime", "@view-server/server"]);
  });

  it("ignores member APIs named require", () => {
    expect(
      importSpecifiersFromSource(
        [
          'validator.require("@view-server/runtime");',
          'this.require("@view-server/server");',
          'loader?.require("@view-server/client");',
        ].join("\n"),
      ),
    ).toStrictEqual([]);
  });

  it("does not hide CommonJS imports after self-closing JSX", () => {
    const restriction = {
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'const node = <Panel />; const runtime = require("@view-server/runtime");',
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects deep imports even when the package root is allowed", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote/internal";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/client/remote/internal: View Server imports must use approved package exports.",
    ]);
  });

  it("rejects approved subexports that are not explicitly allowed for a package", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/client"]),
      message: "Server code may depend on client contracts only.",
      packageName: "server",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote";',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/client/remote: Server code may depend on client contracts only.",
    ]);
  });

  it("allows intentionally carved testing entrypoints", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("matches relative path carveouts across path separators", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: toPosixRelativePath("src\\testing.tsx"),
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("does not allow testing entrypoint carveouts to hide unrelated forbidden packages", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory", "@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'import { createViewServerRuntime } from "@view-server/runtime";',
        ].join("\n"),
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/testing.tsx imports @view-server/runtime: React production must stay transport-neutral.",
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
          'import { createViewServerRuntime } from "@view-server/runtime";',
          'import { server } from "../../server/src/index";',
        ].join("\n"),
        packageRoot,
        path,
        restriction: {
          allowedSpecifiers: new Set(["@view-server/client"]),
          forbiddenSpecifiers: new Set(["@view-server/runtime"]),
          message: "React production must stay transport-neutral.",
          packageName: "react",
        },
      }),
    ).toStrictEqual([
      "packages/react/src/index.tsx imports ../../server/src/index: relative imports must not cross package seams.",
      "packages/react/src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("formats and throws package import violation summaries", () => {
    const violations = ["packages/react/src/index.tsx imports @view-server/runtime: no"];

    expect(packageImportViolationMessage(violations)).toStrictEqual(
      [
        "Package architecture seam violations found.",
        "- packages/react/src/index.tsx imports @view-server/runtime: no",
      ].join("\n"),
    );
    expect(() => assertNoPackageImportViolations(violations)).toThrowError(
      "Package architecture seam violations found.",
    );
    expect(assertNoPackageImportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package import violations", () => {
    expect(collectPackageImportViolations()).toStrictEqual([]);
  });

  it("ignores import-like text in comments", () => {
    expect(
      sourceWithoutComments(
        [
          'import { client } from "@view-server/client";',
          '// import { runtime } from "@view-server/runtime";',
          '/* import { server } from "@view-server/server"; */',
          'const example = "import from comment-like string";',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'import { client } from "@view-server/client";',
        "",
        "",
        'const example = "import from comment-like string";',
      ].join("\n"),
    );
  });

  it("does not treat import-like text inside strings as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const message = "Do not import from \\"@view-server/runtime\\"";',
          "const docs = `import { server } from \"@view-server/server\"`;",
          'import { client } from "@view-server/client";',
          "const runtime = import(`@view-server/runtime`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/client", "@view-server/runtime"]);
  });

  it("does not treat import-like JSX text as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>Install from \"@view-server/runtime\" and import from \"@view-server/server\".</p>;",
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
          "  return <>Install from \"@view-server/runtime\" and import from \"@view-server/server\".</>;",
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
          "  return condition && <p>Install from \"@view-server/runtime\".</p>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return fallback || <>Install from \"@view-server/server\".</>;",
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
          "  return <_Panel>Install from \"@view-server/runtime\".</_Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual([]);
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <$Panel>Install from \"@view-server/server\".</$Panel>;",
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
          "  return <Panel><Icon />Install from \"@view-server/runtime\".</Panel>;",
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
          'const runtime = require("@view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("does not strip code after JSX text that looks like a line comment", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function HelpText() {",
          "  return <p>//</p>;",
          "}",
          'const runtime = require("@view-server/runtime");',
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("detects imports inside JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel>{import(\"@view-server/runtime\")}</Panel>;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("detects imports inside self-closing JSX expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "export function Loader() {",
          "  return <Panel value={import(\"@view-server/runtime\")} />;",
          "}",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
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
          "const text = `plain import { server } from \"@view-server/server\"`;",
          "const runtime = `${await import(\"@view-server/runtime\")}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("handles unfinished template literal expressions conservatively", () => {
    expect(importSpecifiersFromSource("const text = `${await import(")).toStrictEqual([]);
  });

  it("handles unfinished plain template literals conservatively", () => {
    expect(importSpecifiersFromSource("const text = `unterminated")).toStrictEqual([]);
  });

  it("ignores unterminated quoted import specifiers", () => {
    expect(importSpecifiersFromSource('const broken = import("@view-server/runtime')).toStrictEqual(
      [],
    );
  });

  it("ignores from keywords that are not followed by quoted specifiers", () => {
    expect(importSpecifiersFromSource("from notAString")).toStrictEqual([]);
  });
});
