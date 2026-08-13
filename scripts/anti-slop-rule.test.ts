import { describe, expect, it } from "@effect/vitest";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

type RuleSeverity = "error" | "warn";
type LintMode = "isolated" | "repository";

type LintFixtureResult = {
  readonly error: Error | undefined;
  readonly output: string;
  readonly status: number | null;
};

const lintFixture = (
  source: string,
  additionalFiles: Readonly<Record<string, string>> = {},
  rules: Readonly<Record<string, RuleSeverity>> = {
    "anti-slop/no-unsafe-dictionary-type": "error",
  },
  mode: LintMode = "isolated",
): LintFixtureResult => {
  const directory = mkdtempSync(join(tmpdir(), "effect-view-server-anti-slop-"));
  const file = join(directory, "fixture.ts");
  const config = join(directory, "oxlint.config.json");
  writeFileSync(file, source);
  writeFileSync(
    config,
    JSON.stringify({
      jsPlugins: [
        {
          name: "anti-slop",
          specifier: join(process.cwd(), "tools/oxlint/anti-slop/index.ts"),
        },
      ],
      rules,
    }),
  );
  for (const [name, contents] of Object.entries(additionalFiles)) {
    writeFileSync(join(directory, name), contents);
  }
  const args =
    mode === "repository"
      ? ["lint", "--format", "json", file]
      : ["exec", "oxlint", "--config", config, "--format", "json", file];
  const result = spawnSync("vp", args, {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout: 120_000,
  });
  rmSync(directory, { force: true, recursive: true });
  return {
    error: result.error,
    output: `${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    status: result.status,
  };
};

const relativeFiles = (directory: string, prefix = ""): readonly string[] =>
  readdirSync(join(directory, prefix), { withFileTypes: true })
    .flatMap((entry) => {
      const relativePath = join(prefix, entry.name);
      return entry.isDirectory() ? relativeFiles(directory, relativePath) : [relativePath];
    })
    .sort();

describe("anti-slop Oxlint integration", () => {
  it("rejects direct exported unsafe dictionary contracts", () => {
    const result = lintFixture(
      [
        "export type UnsafeRecord = Record<string, unknown>;",
        "export type GlobalThisRecord = globalThis.Record<string, unknown>;",
        "export interface UnsafeInterface {",
        "  [key: string]: object;",
        "}",
        "export interface DefaultedInterface<T = unknown> {",
        "  [key: string]: T;",
        "}",
        "export type UnsafeMapped = { [key: string]: any };",
        "type Local = Record<string, unknown>;",
        "export type PublicAlias = Local;",
        "type ExportedByList = Record<string, unknown>;",
        "export type { ExportedByList };",
        "type WrappedUnsafe = Record<string, unknown>;",
        "export type ParenthesizedPublic = (WrappedUnsafe);",
        "export type UnionPublic = WrappedUnsafe | never;",
        "type UnsafeUnion = Record<string, unknown> | string;",
        "export type PublicAliasUnion = UnsafeUnion;",
        "interface HiddenUnsafe {",
        "  [key: string]: unknown;",
        "}",
        "export type InterfaceAlias = HiddenUnsafe;",
        "interface BaseUnsafe {",
        "  [key: string]: unknown;",
        "}",
        "export interface DerivedUnsafe extends BaseUnsafe {}",
        "interface HeritageBase {",
        "  [key: string]: unknown;",
        "}",
        "type HeritageAlias = HeritageBase;",
        "export interface HeritagePublic extends HeritageAlias {}",
        "interface TransitiveHeritage extends HeritageAlias {}",
        "export type TransitiveHeritagePublic = TransitiveHeritage;",
        "interface MergedEmpty {}",
        "interface MergedEmpty {}",
        "export type MergedEmptyPublic = Record<string, MergedEmpty>;",
        "function Record() {}",
        "export type FunctionShadowedRecord = Record<string, unknown>;",
        "export type Nested = { readonly values: Record<string, unknown> };",
      ].join("\n"),
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.output.match(/anti-slop\(no-unsafe-dictionary-type\)/g)?.length).toBe(16);
    expect(
      Array.from(
        result.output.matchAll(/exported object dictionary contract (?:\\"|")([^"\\]+)(?:\\"|")/g),
        (match) => match[1] ?? "",
      ),
    ).toStrictEqual([
      "UnsafeRecord",
      "GlobalThisRecord",
      "UnsafeInterface",
      "DefaultedInterface",
      "UnsafeMapped",
      "PublicAlias",
      "ExportedByList",
      "ParenthesizedPublic",
      "UnionPublic",
      "PublicAliasUnion",
      "InterfaceAlias",
      "DerivedUnsafe",
      "HeritagePublic",
      "TransitiveHeritagePublic",
      "MergedEmptyPublic",
      "FunctionShadowedRecord",
    ]);
  }, 120_000);

  it("recognizes export-assignment type contracts", () => {
    const result = lintFixture([
      "type Public = Record<string, unknown>;",
      "declare const Public: Public;",
      "export = Public;",
    ].join("\n"));

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.output).toContain("anti-slop(no-unsafe-dictionary-type)");
  }, 120_000);

  it("keeps reviewed deferred rules precise when enabled", () => {
    const result = lintFixture(
      [
        "const shape = 1;",
        "const shapeValue = 2;",
        "type ObjectAlias = object;",
        "function acceptsObject(value: ObjectAlias = {}) {}",
        "function acceptsUnknown(value: unknown = undefined) {}",
        "function acceptsCause(cause: unknown) {}",
      ].join("\n"),
      {},
      {
        "anti-slop/no-object-parameters": "error",
        "anti-slop/no-shape-in-symbol-names": "error",
        "anti-slop/no-unknown-parameters": "error",
      },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.output.match(/anti-slop\(no-object-parameters\)/g)?.length).toBe(1);
    expect(result.output.match(/anti-slop\(no-shape-in-symbol-names\)/g)?.length).toBe(1);
    expect(result.output.match(/anti-slop\(no-unknown-parameters\)/g)?.length).toBe(1);
  }, 120_000);

  it("registers the plugin through the repository Vite+ lint configuration", () => {
    const result = lintFixture(
      "export type RepositoryConfigUnsafe = Record<string, unknown>;",
      {},
      { "anti-slop/no-unsafe-dictionary-type": "error" },
      "repository",
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.output).toContain("anti-slop(no-unsafe-dictionary-type)");
  }, 120_000);

  it("does not treat source-backed re-exports as local exports", () => {
    const result = lintFixture(
      [
        "type Payload = Record<string, unknown>;",
        'export type { Payload } from "./generated";',
      ].join("\n"),
      { "generated.ts": "export type Payload = string;" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.output).not.toContain("anti-slop(no-unsafe-dictionary-type)");
  }, 120_000);

  it("allows boundary guards and internal dictionary plumbing", () => {
    const result = lintFixture(`
type Row = { readonly fields?: Readonly<Record<string, unknown>> };
export type Validated = Row & { readonly tag: true };
const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null;
`);

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.output).not.toContain("anti-slop(no-unsafe-dictionary-type)");
  }, 120_000);

  it("installs cleanly and safely overwrites managed files when forced", () => {
    const directory = mkdtempSync(join(tmpdir(), "effect-view-server-anti-slop-install-"));
    const target = join(directory, "plugin");
    const script = join(process.cwd(), ".agents/skills/install-anti-slop/scripts/install.mjs");

    const first = spawnSync("node", [script, target], {
      cwd: process.cwd(),
      encoding: "utf8",
      timeout: 120_000,
    });
    expect(first.error).toBeUndefined();
    expect(first.status).toBe(0);
    expect(existsSync(join(target, "index.ts"))).toBe(true);

    writeFileSync(join(target, "stale.ts"), "export const stale = true;\n");
    writeFileSync(join(target, "index.ts"), "export const stale = true;\n");
    const refused = spawnSync("node", [script, target], {
      cwd: process.cwd(),
      encoding: "utf8",
      timeout: 120_000,
    });
    expect(refused.error).toBeUndefined();
    expect(refused.status).toBe(1);

    const forced = spawnSync("node", [script, target, "--force"], {
      cwd: process.cwd(),
      encoding: "utf8",
      timeout: 120_000,
    });
    expect(forced.error).toBeUndefined();
    expect(forced.status).toBe(0);
    expect(existsSync(join(target, "stale.ts"))).toBe(true);
    expect(readFileSync(join(target, "index.ts"), "utf8")).toBe(
      readFileSync(join(".agents/skills/install-anti-slop/assets/anti-slop", "index.ts"), "utf8"),
    );
    rmSync(directory, { force: true, recursive: true });
  }, 120_000);

  it("keeps the bundled asset and live plugin copies identical", () => {
    const liveFiles = relativeFiles("tools/oxlint/anti-slop");
    const bundledFiles = relativeFiles(".agents/skills/install-anti-slop/assets/anti-slop");

    expect(liveFiles).toStrictEqual(bundledFiles);
    expect(liveFiles.map((file) => readFileSync(join("tools/oxlint/anti-slop", file), "utf8"))).toStrictEqual(
      bundledFiles.map((file) =>
        readFileSync(join(".agents/skills/install-anti-slop/assets/anti-slop", file), "utf8"),
      ),
    );
  }, 120_000);
});
