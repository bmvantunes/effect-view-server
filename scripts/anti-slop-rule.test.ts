import { describe, expect, it } from "@effect/vitest";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const lintFixture = (source: string, additionalFiles: Readonly<Record<string, string>> = {}) => {
  const directory = mkdtempSync(join(tmpdir(), "effect-view-server-anti-slop-"));
  const file = join(directory, "fixture.ts");
  writeFileSync(file, source);
  for (const [name, contents] of Object.entries(additionalFiles)) {
    writeFileSync(join(directory, name), contents);
  }
  const result = spawnSync("vp", ["lint", "--format", "json", file], {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  rmSync(directory, { force: true, recursive: true });
  return {
    output: `${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    status: result.status,
  };
};

describe("anti-slop Oxlint integration", () => {
  it("rejects direct exported unsafe dictionary contracts", () => {
    const result = lintFixture(
      [
        "export type UnsafeRecord = Record<string, unknown>;",
        "export interface UnsafeInterface {",
        "  [key: string]: object;",
        "}",
        "export type UnsafeMapped = { [key: string]: any };",
        "type Local = Record<string, unknown>;",
        "export type PublicAlias = Local;",
        "type ExportedByList = Record<string, unknown>;",
        "export type { ExportedByList };",
        "type WrappedUnsafe = Record<string, unknown>;",
        "export type ParenthesizedPublic = (WrappedUnsafe);",
        "export type UnionPublic = WrappedUnsafe | never;",
        "interface HiddenUnsafe {",
        "  [key: string]: unknown;",
        "}",
        "export type InterfaceAlias = HiddenUnsafe;",
        "interface BaseUnsafe {",
        "  [key: string]: unknown;",
        "}",
        "export interface DerivedUnsafe extends BaseUnsafe {}",
        "function Record() {}",
        "export type FunctionShadowedRecord = Record<string, unknown>;",
        "export type Nested = { readonly values: Record<string, unknown> };",
      ].join("\n"),
    );

    expect(result.status).toBe(1);
    expect(result.output.match(/anti-slop\(no-unsafe-dictionary-type\)/g)).toHaveLength(10);
  });

  it("does not treat source-backed re-exports as local exports", () => {
    const result = lintFixture(
      [
        "type Payload = Record<string, unknown>;",
        'export type { Payload } from "./generated";',
      ].join("\n"),
      { "generated.ts": "export type Payload = string;" },
    );

    expect(result.status).toBe(0);
    expect(result.output).not.toContain("anti-slop(no-unsafe-dictionary-type)");
  });

  it("allows boundary guards and internal dictionary plumbing", () => {
    const result = lintFixture(`
type Row = { readonly fields?: Readonly<Record<string, unknown>> };
export type Validated = Row & { readonly tag: true };
const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null;
`);

    expect(result.status).toBe(0);
    expect(result.output).not.toContain("anti-slop(no-unsafe-dictionary-type)");
  });

  it("keeps the bundled asset and live plugin copies identical", () => {
    const result = spawnSync(
      "diff",
      ["-ru", "tools/oxlint/anti-slop", ".agents/skills/install-anti-slop/assets/anti-slop"],
      { cwd: process.cwd(), encoding: "utf8" },
    );

    expect(result.status).toBe(0);
    expect(result.stdout).toBe("");
    expect(result.stderr).toBe("");
  });
});
