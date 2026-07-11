import { describe, expect, it } from "@effect/vitest";
import {
  identifierNamesFromTypeScript,
  inspectLibraryPack,
  inspectPrivateWorkspaceLeaks,
  inspectReexportModule,
  inspectTypeScriptModule,
  namespaceImportsFromTypeScript,
} from "./typescript-module-inspection";

describe("TypeScript Module Inspection", () => {
  it("collects supported TypeScript module references with the real filename syntax kind", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/example.tsx",
      source: [
        'import value from "static-import";',
        'import type { Type } from "static-type-import";',
        'export * from "static-export";',
        'export type { Other } from "static-type-export";',
        'const dynamic = import("dynamic-import");',
        "const templateDynamic = import(`dynamic-template`);",
        'type Imported = import("import-type").Imported;',
        'import legacy = require("import-equals");',
        'const required = require("require");',
        'const resolved = require.resolve("require-resolve");',
        'const moduleRequired = module.require("module-require");',
        'const created = createRequire(import.meta.url)("create-require");',
        'const createdResolved = createRequire(import.meta.url).resolve("create-require-resolve");',
        'const metadataResolved = import.meta.resolve("import-meta-resolve");',
        "const element = <section>{value}</section>;",
        "void dynamic; void templateDynamic; void required; void resolved; void moduleRequired;",
        "void created; void createdResolved; void metadataResolved; void element;",
      ].join("\n"),
    });

    expect(inspection).toStrictEqual({
      moduleSpecifiers: [
        "static-import",
        "static-type-import",
        "static-export",
        "static-type-export",
        "dynamic-import",
        "dynamic-template",
        "import-type",
        "import-equals",
        "require",
        "require-resolve",
        "module-require",
        "create-require",
        "create-require-resolve",
        "import-meta-resolve",
      ],
      reexports: [
        { kind: "all", moduleSpecifier: "static-export", typeOnly: false },
        {
          exports: [
            {
              exportedName: "Other",
              sourceName: "Other",
              typeOnly: true,
            },
          ],
          kind: "named",
          moduleSpecifier: "static-type-export",
        },
      ],
      violations: [],
    });
  });

  it("uses filename-aware TypeScript, declaration, JavaScript, and JSX syntax kinds", () => {
    for (const fileName of [
      "source.ts",
      "source.mts",
      "source.cts",
      "source.d.ts",
      "source.d.mts",
      "source.d.cts",
      "source.js",
      "source.mjs",
      "source.cjs",
      "source.tsx",
      "source.jsx",
    ]) {
      expect(
        inspectTypeScriptModule({ fileName, source: 'import "literal";' }).moduleSpecifiers,
      ).toStrictEqual(["literal"]);
    }
    expect(
      inspectTypeScriptModule({
        fileName: "source.ts",
        source: 'const value = <Runtime>input; import "typed";',
      }).moduleSpecifiers,
    ).toStrictEqual(["typed"]);
    expect(
      inspectTypeScriptModule({
        fileName: "source.tsx",
        source: [
          '<Panel>import "jsx-text"</Panel>;',
          '<Panel>{require("jsx-expression")}</Panel>;',
        ].join("\n"),
      }).moduleSpecifiers,
    ).toStrictEqual(["jsx-expression"]);
  });

  it("accepts optional and literal-bracket forms without losing source order", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/example.mts",
      source: [
        'require?.("optional-require");',
        'require["resolve"]?.("bracket-require-resolve");',
        'module?.["require"]("bracket-module-require");',
        'import.meta["resolve"]?.("bracket-import-meta-resolve");',
        'createRequire(import.meta.url)["resolve"]("bracket-create-require-resolve");',
        'require.resolve("resolve-options", { paths: [] });',
        'createRequire(import.meta.url).resolve("created-resolve-options", { paths: [] });',
        'require("duplicate");',
        'require("duplicate");',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([
      "optional-require",
      "bracket-require-resolve",
      "bracket-module-require",
      "bracket-import-meta-resolve",
      "bracket-create-require-resolve",
      "resolve-options",
      "created-resolve-options",
      "duplicate",
      "duplicate",
    ]);
    expect(inspection.violations).toStrictEqual([]);
  });

  it("normalizes escaped syntax while ignoring unrelated member APIs", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/escaped.ts",
      source: [
        'import "\\u0040effect-view-server/config";',
        'requ\\u0069re("@effect-view-server/runtime");',
        'registry.require("@effect-view-server/server");',
        'registry.import("@effect-view-server/protocol");',
        'factory.createRequire(import.meta.url)("@effect-view-server/client");',
        'loader.module.require("@effect-view-server/config");',
        "html`not-a-loader`;",
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([
      "@effect-view-server/config",
      "@effect-view-server/runtime",
    ]);
    expect(inspection.violations).toStrictEqual([]);
  });

  it("fails closed on computed, interpolated, aliased, wrapped, and controlled module loads", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/evasive.cts",
      source: [
        "import(moduleName);",
        "require(`runtime/${name}`);",
        "require[method](\"computed\");",
        "const load = require;",
        "const { resolve } = require;",
        'require.call(undefined, "called");',
        'require.apply(undefined, ["applied"]);',
        'require.bind(undefined, "bound")();',
        '(0, require)("sequence");',
        '(enabled && require)("logical");',
        '(enabled ? require : fallback)("conditional");',
        'new require("constructed");',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([]);
    expect(inspection.violations.map((violation) => violation.kind)).toStrictEqual([
      "non-literal-specifier",
      "interpolated-specifier",
      "computed-loader",
      "loader-alias",
      "loader-alias",
      "loader-wrapper",
      "loader-wrapper",
      "loader-wrapper",
      "controlled-loader",
      "controlled-loader",
      "controlled-loader",
      "constructed-loader",
    ]);
    expect(inspection.violations[0]).toStrictEqual({
      column: 1,
      kind: "non-literal-specifier",
      line: 1,
      loader: "dynamic-import",
    });
  });

  it("extracts only literal Vite+ libraryPack entrypoints from the TypeScript AST", () => {
    expect(
      inspectLibraryPack({
        fileName: "packages/config/vite.config.ts",
        source: [
          'import { defineConfig } from "vite-plus";',
          'import { libraryPack } from "../../vite.pack";',
          "export default defineConfig({",
          "  1: true,",
          '  pack: libraryPack(["src/index.ts", `src/runtime.mts`, "src/internal.cts"]),',
          "});",
        ].join("\n"),
      }),
    ).toStrictEqual({
      entrypoints: ["src/index.ts", "src/runtime.mts", "src/internal.cts"],
      violations: [],
    });

    expect(
      inspectLibraryPack({
        fileName: "packages/config/vite.config.ts",
        source: "export default { pack: libraryPack(entries) };",
      }).violations.map((violation) => violation.kind),
    ).toStrictEqual(["non-literal-library-pack"]);
    expect(
      [
        "export default { pack: entries };",
        "export default { pack: libraryPack() };",
        'export default { pack: libraryPack(["src/index.ts", entry]) };',
        "export default { pack: libraryPack([...entries]) };",
        'export default { pack: libraryPack([, "src/index.ts"]) };',
        'export default { [packName]: libraryPack("src/index.ts") };',
        'export default { pack: libraryPack("src/index.ts", options) };',
      ].map((source) =>
        inspectLibraryPack({ fileName: "vite.config.ts", source }).violations.map(
          (violation) => violation.kind,
        ),
      ),
    ).toStrictEqual([
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["ambiguous-library-pack-override", "missing-library-pack"],
      ["non-literal-library-pack"],
    ]);
    expect(
      inspectLibraryPack({
        fileName: "vite.config.ts",
        source: [
          "export default {",
          '  pack: libraryPack("src/index.ts"),',
          '  pack: libraryPack("src/remote.ts"),',
          "  ...override,",
          "  [computed]: override,",
          "};",
        ].join("\n"),
      }).violations.map((violation) => violation.kind),
    ).toStrictEqual([
      "ambiguous-library-pack-override",
      "ambiguous-library-pack-override",
      "duplicate-library-pack",
    ]);
    expect(
      [
        'const unused = { pack: libraryPack("src/index.ts") }; export default {};',
        'export default Object.assign({ pack: libraryPack("src/index.ts") }, override);',
        "const base = {}; export default { ...base, ...override };",
        "const config = {}; export default defineConfig(config);",
        "export default defineConfig({}, override);",
        "const config = {};",
        "export default { pack };",
        "export default { pack() {} };",
        "export default { get pack() { return value; } };",
        "export default { set pack(value) {} };",
      ].map((source) =>
        inspectLibraryPack({ fileName: "vite.config.ts", source }).violations.map(
          (violation) => violation.kind,
        ),
      ),
    ).toStrictEqual([
      ["missing-library-pack"],
      ["non-literal-library-pack"],
      [
        "ambiguous-library-pack-override",
        "ambiguous-library-pack-override",
        "missing-library-pack",
      ],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["missing-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
      ["non-literal-library-pack"],
    ]);
    expect(
      inspectLibraryPack({
        fileName: "vite.config.ts",
        source: [
          'export default { "pack": libraryPack("src/index.ts") };',
          "export default {};",
        ].join("\n"),
      }),
    ).toStrictEqual({
      entrypoints: ["src/index.ts"],
      violations: [
        { column: 1, kind: "non-literal-library-pack", line: 2 },
      ],
    });
  });

  it("distinguishes wildcard, namespace, aliased, type-only, and local exports", () => {
    const inspection = inspectReexportModule({
      fileName: "src/facade.ts",
      source: [
        'export * from "all";',
        'export type * from "types";',
        'export * as namespace from "namespace";',
        'export { internal as public, type InternalType as PublicType } from "named";',
        "export { local };",
        "export const executable = true;",
      ].join("\n"),
    });

    expect(inspection.reexports).toStrictEqual([
      { kind: "all", moduleSpecifier: "all", typeOnly: false },
      { kind: "all", moduleSpecifier: "types", typeOnly: true },
      {
        exportedName: "namespace",
        kind: "namespace",
        moduleSpecifier: "namespace",
        typeOnly: false,
      },
      {
        exports: [
          { exportedName: "public", sourceName: "internal", typeOnly: false },
          { exportedName: "PublicType", sourceName: "InternalType", typeOnly: true },
        ],
        kind: "named",
        moduleSpecifier: "named",
      },
      {
        exports: [{ exportedName: "local", sourceName: "local", typeOnly: false }],
        kind: "named",
        moduleSpecifier: undefined,
      },
    ]);
    expect(inspection.nonReexportStatements.map((statement) => statement.line)).toStrictEqual([
      5, 6,
    ]);
    expect(
      identifierNamesFromTypeScript({
        fileName: "src/topic.ts",
        source: [
          "// topicStoreState",
          'const text = "topicStoreReadModel";',
          "const value = topicStoreState;",
          'import { "topicStoreRawQueryMetadata" as imported } from "module";',
        ].join("\n"),
      }),
    ).toStrictEqual([
      "text",
      "value",
      "topicStoreState",
      "topicStoreRawQueryMetadata",
      "imported",
    ]);
    expect(
      namespaceImportsFromTypeScript({
        fileName: "src/malformed.ts",
        source: "import * as Namespace from ModuleName;",
      }),
    ).toStrictEqual([]);
  });

  it("rejects a non-literal specifier at every supported loader root", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/non-literal.d.cts",
      source: [
        "import(name);",
        "require(name);",
        "require.resolve(name);",
        "module.require(name);",
        "import.meta.resolve(name);",
        "createRequire(import.meta.url)(name);",
        "createRequire(import.meta.url).resolve(name);",
      ].join("\n"),
    });

    expect(inspection.violations).toStrictEqual([
      { column: 1, kind: "non-literal-specifier", line: 1, loader: "dynamic-import" },
      { column: 1, kind: "non-literal-specifier", line: 2, loader: "require" },
      { column: 1, kind: "non-literal-specifier", line: 3, loader: "require-resolve" },
      { column: 1, kind: "non-literal-specifier", line: 4, loader: "module-require" },
      { column: 1, kind: "non-literal-specifier", line: 5, loader: "import-meta-resolve" },
      { column: 1, kind: "non-literal-specifier", line: 6, loader: "create-require" },
      {
        column: 1,
        kind: "non-literal-specifier",
        line: 7,
        loader: "create-require-resolve",
      },
    ]);
  });

  it("fails closed on malformed static, import-type, and import-equals specifiers", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/malformed.ts",
      source: [
        'import "unterminated',
        "export * from ExportName;",
        "type Imported = import(ImportName).Type;",
        "import Legacy = require(ImportEqualsName);",
        "import Missing = require();",
        "export * as MissingNamespace from ExportNamespace;",
        'import Broken = require("unterminated',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([]);
    expect(inspection.violations).toStrictEqual([
      { column: 1, kind: "malformed-literal", line: 1, loader: "static-import" },
      { column: 1, kind: "non-literal-specifier", line: 2, loader: "static-export" },
      { column: 17, kind: "non-literal-specifier", line: 3, loader: "import-type" },
      { column: 1, kind: "non-literal-specifier", line: 4, loader: "import-equals" },
      { column: 1, kind: "non-literal-specifier", line: 5, loader: "import-equals" },
      { column: 1, kind: "non-literal-specifier", line: 6, loader: "static-export" },
      { column: 1, kind: "malformed-literal", line: 7, loader: "import-equals" },
    ]);
    expect(
      inspectTypeScriptModule({
        fileName: "src/qualified.ts",
        source: "import Alias = Namespace.Member;",
      }),
    ).toStrictEqual({ moduleSpecifiers: [], reexports: [], violations: [] });
  });

  it("rejects zero-argument and concatenated loads plus aliased createRequire capabilities", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/aliases.ts",
      source: [
        "require();",
        'require("@effect/" + packageName);',
        'import { createRequire as cr } from "node:module";',
        'import * as Module from "node:module";',
        'import ModuleDefault from "node:module";',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([
      "node:module",
      "node:module",
      "node:module",
    ]);
    expect(inspection.violations).toStrictEqual([
      { column: 1, kind: "non-literal-specifier", line: 1, loader: "require" },
      { column: 1, kind: "non-literal-specifier", line: 2, loader: "require" },
      { column: 1, kind: "loader-alias", line: 3, loader: "create-require-factory" },
      { column: 1, kind: "loader-alias", line: 4, loader: "create-require-factory" },
      { column: 1, kind: "loader-alias", line: 5, loader: "create-require-factory" },
    ]);
    expect(
      inspectTypeScriptModule({
        fileName: "src/direct-create-require.ts",
        source: [
          'import { createRequire } from "node:module";',
          'import type * as ModuleTypes from "node:module";',
        ].join("\n"),
      }).violations,
    ).toStrictEqual([]);
  });

  it("rejects direct CommonJS and dynamic acquisition of node:module capabilities", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/node-module-acquisition.cts",
      source: [
        'const CommonJsModule = require("node:module");',
        'const { createRequire: commonJsAlias } = require("module");',
        'const DynamicModule = import("node:module");',
        'const { createRequire: dynamicAlias } = await import("module");',
        'assigned = require("node:module");',
        'const holder = { Module: import("module") };',
        'function acquire() { return require("node:module"); }',
        'register(import("module"));',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([
      "node:module",
      "module",
      "node:module",
      "module",
      "node:module",
      "module",
      "node:module",
      "module",
    ]);
    expect(inspection.violations).toStrictEqual(
      [7, 7, 7, 7, 1, 18, 22, 1].map((column, index) => ({
        column,
        kind: "loader-alias",
        line: index + 1,
        loader: "create-require-factory",
      })),
    );
  });

  it("pins generic loader data-flow aliases outside the syntactic inspection boundary", () => {
    expect(
      inspectTypeScriptModule({
        fileName: "src/data-flow-limit.ts",
        source: [
          'const load = () => require; load()("package");',
          '[require][0]("package");',
        ].join("\n"),
      }),
    ).toStrictEqual({ moduleSpecifiers: [], reexports: [], violations: [] });
  });

  it("rejects inline node:module createRequire acquisition", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/inline-node-module.mts",
      source: [
        'require("node:module").createRequire(import.meta.url)("private-one");',
        '(await import("module"))["createRequire"](import.meta.url)("private-two");',
        'require("node:module")[method](import.meta.url)("private-three");',
        'require("node:module").builtinModules;',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual([
      "node:module",
      "module",
      "node:module",
      "node:module",
    ]);
    expect(
      inspection.violations.map(({ kind, line, loader }) => ({ kind, line, loader })),
    ).toStrictEqual([
      { kind: "loader-alias", line: 1, loader: "create-require-factory" },
      { kind: "loader-alias", line: 2, loader: "create-require-factory" },
      { kind: "computed-loader", line: 3, loader: "create-require-factory" },
    ]);
  });

  it("finds private workspace leaks in emitted JavaScript and declarations", () => {
    expect(
      inspectPrivateWorkspaceLeaks({
        fileName: "dist/index.js",
        privateScope: "@effect-view-server",
        source: [
          'import "@effect-view-server/config";',
          'export * from "@effect-view-server/client";',
          'require("@effect-view-server/runtime");',
          'import("@effect-view-server/server");',
          "require(packageName);",
        ].join("\n"),
      }),
    ).toStrictEqual({
      privateSpecifiers: [
        "@effect-view-server/config",
        "@effect-view-server/client",
        "@effect-view-server/runtime",
        "@effect-view-server/server",
      ],
      violations: [
        { column: 1, kind: "non-literal-specifier", line: 5, loader: "require" },
      ],
    });
    expect(
      inspectPrivateWorkspaceLeaks({
        fileName: "dist/index.d.ts",
        privateScope: "@effect-view-server",
        source: [
          'import type { A } from "@effect-view-server/config/query";',
          'export type { B } from "@effect-view-server/config/health";',
          'type C = import("@effect-view-server/protocol").C;',
        ].join("\n"),
      }).privateSpecifiers,
    ).toStrictEqual([
      "@effect-view-server/config/query",
      "@effect-view-server/config/health",
      "@effect-view-server/protocol",
    ]);
  });

  it("rejects loader capabilities escaping through assignments, calls, objects, and returns", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/capabilities.ts",
      source: [
        "load = require;",
        "register(require.resolve);",
        "const capabilities = { load: module.require };",
        "function leak() { return import.meta.resolve; }",
        "const factory = createRequire;",
        "const created = createRequire(import.meta.url);",
      ].join("\n"),
    });

    expect(inspection.violations.map((violation) => violation.loader)).toStrictEqual([
      "require",
      "require-resolve",
      "module-require",
      "import-meta-resolve",
      "create-require-factory",
      "create-require",
    ]);
    expect(inspection.violations.every((violation) => violation.kind === "loader-alias")).toBe(
      true,
    );
  });

  it("rejects wrappers, computed access, control flow, construction, tags, and malformed calls", () => {
    const inspection = inspectTypeScriptModule({
      fileName: "src/more-evasions.d.mts",
      source: [
        'require.resolve.call(require, "called");',
        'module.require.apply(module, ["applied"]);',
        'import.meta.resolve.bind(import.meta, "bound")();',
        'createRequire(import.meta.url).call(undefined, "created-called");',
        'createRequire(import.meta.url).resolve.apply(undefined, ["created-applied"]);',
        'require[method]("computed-require");',
        'module[method]("computed-module");',
        'import.meta[method]("computed-meta");',
        'createRequire(import.meta.url)[method]("computed-created");',
        '(enabled ?? require.resolve)("nullish");',
        'require`tagged`;',
        'new module.require("constructed");',
        'require("extra", options);',
        'import("allowed-options", options);',
        'require("unterminated);',
      ].join("\n"),
    });

    expect(inspection.moduleSpecifiers).toStrictEqual(["allowed-options"]);
    expect(inspection.violations.map((violation) => violation.kind)).toStrictEqual([
      "loader-wrapper",
      "loader-wrapper",
      "loader-wrapper",
      "loader-wrapper",
      "loader-wrapper",
      "computed-loader",
      "computed-loader",
      "computed-loader",
      "computed-loader",
      "controlled-loader",
      "tagged-loader",
      "constructed-loader",
      "ambiguous-loader-call",
      "malformed-literal",
    ]);
  });
});
