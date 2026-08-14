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

type LintDiagnostic = {
  readonly code: string;
  readonly message: string;
  readonly severity: string;
};

type LintReport = {
  readonly diagnostics: readonly LintDiagnostic[];
};

type LintFixtureResult = {
  readonly error: Error | undefined;
  readonly report: LintReport | undefined;
  readonly status: number | null;
};

const isLintReport = (value: unknown): value is LintReport => {
  if (typeof value !== "object" || value === null || !("diagnostics" in value)) return false;
  const diagnostics = value.diagnostics;
  if (!Array.isArray(diagnostics)) return false;
  return diagnostics.every((diagnostic) => {
    if (typeof diagnostic !== "object" || diagnostic === null) return false;
    return (
      "code" in diagnostic &&
      typeof diagnostic.code === "string" &&
      "message" in diagnostic &&
      typeof diagnostic.message === "string" &&
      "severity" in diagnostic &&
      typeof diagnostic.severity === "string"
    );
  });
};

const diagnosticsFor = (result: LintFixtureResult, rule: string): readonly LintDiagnostic[] =>
  result.report?.diagnostics.filter((diagnostic) => diagnostic.code === `anti-slop(${rule})`) ?? [];

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
  const stdout = result.stdout ?? "";
  const parsed: unknown = stdout.trim().length === 0 ? undefined : JSON.parse(stdout);
  return {
    error: result.error,
    report: isLintReport(parsed) ? parsed : undefined,
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
    expect(diagnosticsFor(result, "no-unsafe-dictionary-type")).toHaveLength(16);
    expect(
      diagnosticsFor(result, "no-unsafe-dictionary-type").map((diagnostic) =>
        diagnostic.message.match(/contract (?:\\"|")([^"\\]+)(?:\\"|")/)?.[1] ?? "",
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
    expect(diagnosticsFor(result, "no-unsafe-dictionary-type")).toHaveLength(1);
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
        "function decodeValue(value: unknown): string { return String(value); }",
        "const parseValue = (value: unknown): number => Number(value);",
        "function parseWithoutEvidence(value: unknown): string { return value as string; }",
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
    expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(1);
    expect(diagnosticsFor(result, "no-shape-in-symbol-names")).toHaveLength(1);
    expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(4);
  }, 120_000);

  it("checks object and unknown parameters in type-only signatures", () => {
    const result = lintFixture(
      [
        "type UnknownCallback = (value: unknown) => void;",
        "interface UnknownConsumer { consume(value: unknown): void; }",
        "type ObjectCallback = (value: object) => void;",
        "interface ObjectConsumer { consume(value: object): void; }",
      ].join("\n"),
      {},
      {
        "anti-slop/no-object-parameters": "error",
        "anti-slop/no-unknown-parameters": "error",
      },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(2);
    expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(2);
  }, 120_000);

	it("keeps validation exemptions correlated with the validated parameter", () => {
    const result = lintFixture(
      [
        "function unrelatedObject(value: object, other: unknown) { JSON.stringify(other); return value; }",
        "function unrelatedUnknown(value: unknown, other: unknown) { return typeof other === 'string' ? value : value; }",
      ].join("\n"),
      {},
      {
        "anti-slop/no-object-parameters": "error",
        "anti-slop/no-unknown-parameters": "error",
      },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(1);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(2);
	}, 120_000);

	it("does not treat coercion, literal comparisons, or property names as validation", () => {
		const result = lintFixture(
			[
				"function coerced(value: unknown) { return String(value); }",
				"function compared(value: unknown) { return value === null; }",
				"function instance(value: unknown) { return value instanceof Error; }",
				"function objectKey(value: unknown) { return ({ value: 1 }).value; }",
				"function memberKey(value: unknown) { const record = { value: 1 }; return record.value; }",
			].join("\n"),
			{},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(5);
	}, 120_000);

	it("requires type predicates to have runtime validation evidence", () => {
		const result = lintFixture(
			[
				"function dishonest(value: unknown): value is string { return true; }",
				"function dishonestAssertion(value: unknown): value is string { return value as string; }",
				"function dishonestCoercion(value: unknown): value is string { return String(value) === value; }",
				"function dishonestUnconditional(value: unknown): value is string { return value !== undefined; }",
				"function inverted(value: unknown): value is string { return typeof value !== 'string'; }",
				"function invertedBranch(value: unknown): value is string { if (typeof value === 'string') return false; return true; }",
				"function wrongTypeofTag(value: unknown): value is string { return typeof value === 'number'; }",
				"function dishonestSwitch(value: unknown): value is string { switch (value) { case 1: return false; default: return true; } }",
				"const honest = (value: unknown): value is string => typeof value === 'string';",
			].join("\n"),
			{},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(8);
	}, 120_000);

	it("does not trust nested validators, shadowed validation names, or overload implementations", () => {
		const result = lintFixture(
			[
				"import { Schema } from 'effect';",
				"function nested(value: unknown) { return (() => typeof value === 'string')(); }",
				"function shadowed(value: unknown) { const String = (input: string) => input; return String(value); }",
				"function noOpTypeof(value: unknown) { typeof value; return value; }",
				"function noOpComparison(value: unknown) { typeof value === 'string'; return value; }",
				"function discardedSchemaCheck(value: unknown) { Schema.is(Schema.String)(value); return value; }",
				"function assignedSchemaCheck(value: unknown) { const checked = Schema.is(Schema.String)(value); return true; }",
				"function usedSchemaCheck(value: unknown) { const checked = Schema.is(Schema.String)(value); return checked; }",
				"function spoofedValidator(value: unknown) { const isPlainRecord = () => true; return isPlainRecord(value); }",
				"function overloaded(value: unknown): void;",
				"function overloaded(value: string) {}",
			].join("\n"),
			{},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(8);
	}, 120_000);

		it("does not trust validator names imported from unrelated modules", () => {
		const result = lintFixture(
			[
				'import { unrelatedFunction as inspect } from "./not-a-validator";',
				'import { unrelatedFunction as isPlainRecord } from "./not-a-validator";',
				"function importedInspect(value: unknown) { return inspect(value); }",
				"function importedRecord(value: unknown) { return isPlainRecord(value); }",
			].join("\n"),
			{
				"not-a-validator.ts":
					"export const unrelatedFunction = (value: unknown) => value;",
			},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
			expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(2);
		}, 120_000);

		it("only trusts predicate and decoder operations from validation namespaces", () => {
			const result = lintFixture(
				[
					'import { Layer, Option, Result } from "effect";',
					"function optionConstructor(value: unknown) { return Option.some(value); }",
					"function resultConstructor(value: unknown) { return Result.succeed(value); }",
					"function layerConstructor(value: unknown) { return Layer.succeed(value); }",
					"function optionPredicate(value: unknown) { return Option.isSome(value); }",
					"function resultPredicate(value: unknown) { return Result.isSuccess(value); }",
				].join("\n"),
				{},
				{ "anti-slop/no-unknown-parameters": "error" },
			);

			expect(result.error).toBeUndefined();
			expect(result.status).toBe(1);
			expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(3);
		}, 120_000);

	it("does not treat self-comparison as object validation", () => {
		const result = lintFixture(
			"function noOpCompare(value: object) { return value === value; }",
			{},
			{ "anti-slop/no-object-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(1);
	}, 120_000);

	it("matches validation evidence to the parameter binding", () => {
		const result = lintFixture(
			[
				"function shadowedUnknown(value: unknown) { { const value = 'text'; return isPlainRecord(value); } }",
				"function shadowedObject(value: object) { { const value = {}; return isPlainRecord(value); } }",
			].join("\n"),
			{},
			{
				"anti-slop/no-object-parameters": "error",
				"anti-slop/no-unknown-parameters": "error",
			},
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(1);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toHaveLength(1);
	}, 120_000);

	it("resolves generic object and unknown parameter aliases, including defaults", () => {
		const objectResult = lintFixture(
			"type Box<T = object> = T; function accepts(value: Box) {} function acceptsExplicit(value: Box<object>) {}",
			{},
			{ "anti-slop/no-object-parameters": "error" },
		);
		const unknownResult = lintFixture(
			"type Box<T = unknown> = T; function accepts(value: Box) {} function acceptsExplicit(value: Box<unknown>) {}",
			{},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(objectResult.error).toBeUndefined();
		expect(objectResult.status).toBe(1);
		expect(diagnosticsFor(objectResult, "no-object-parameters")).toHaveLength(2);
		expect(unknownResult.error).toBeUndefined();
		expect(unknownResult.status).toBe(1);
		expect(diagnosticsFor(unknownResult, "no-unknown-parameters")).toHaveLength(2);
	}, 120_000);

	it("resolves type aliases in their lexical scope", () => {
		const result = lintFixture(
			[
				"type Contract = unknown;",
				"type Row = object;",
				"function nested() {",
				"  type Contract = string;",
				"  type Row = { readonly value: string };",
				"  function acceptsUnknown(value: Contract) {}",
				"  function acceptsObject(value: Row) {}",
				"}",
			].join("\n"),
			{},
			{
				"anti-slop/no-object-parameters": "error",
				"anti-slop/no-unknown-parameters": "error",
			},
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-object-parameters")).toStrictEqual([]);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toStrictEqual([]);
	}, 120_000);

	it("resolves broad object intersections instead of allowing a phantom brand bypass", () => {
		const result = lintFixture(
			"type BrandedObject = object & { readonly brand?: never }; function accepts(value: BrandedObject) { return value; }",
			{},
			{ "anti-slop/no-object-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-object-parameters")).toHaveLength(1);
	}, 120_000);

  it("reports runtime typeof comparisons while allowing explicit type guards", () => {
    const result = lintFixture(
      [
        "declare const external: unknown;",
        "const compared = typeof external;",
        "const optionalGlobal = typeof globalThis.gc === 'function';",
        "const unresolvedGlobal = typeof optionalGlobalFeature;",
        "function isString(value: unknown): value is string { return typeof value === 'string'; }",
        "function ordinary(value: unknown) { return typeof value === 'string'; }",
        "function tautology(value: unknown) { return typeof value === typeof value; }",
        "function unrelatedPredicate(value: unknown, other: unknown): value is string { return typeof other === 'string'; }",
        "function format(value: unknown): string { return typeof value === 'string' ? value : 'fallback'; }",
        "const typed: string = 'text'; const typedKind = typeof typed;",
      ].join("\n"),
      {},
      { "anti-slop/no-runtime-typeof": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-runtime-typeof")).toHaveLength(6);
  }, 120_000);

	it("reports typeof checks on local and aliased broad values", () => {
		const result = lintFixture(
			[
				"type HiddenUnknown = unknown;",
				"const local: HiddenUnknown = 'text';",
				"const alias = local;",
        "const first = typeof local;",
        "const second = typeof alias;",
        "const object = { value: typeof local };",
        "const array = [typeof alias];",
        "function nested(value: HiddenUnknown) { const localAlias = value; return typeof localAlias; }",
			].join("\n"),
			{},
			{ "anti-slop/no-runtime-typeof": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-runtime-typeof")).toHaveLength(5);
	}, 120_000);

	it("detects asserted and named empty-object conditional spreads", () => {
    const result = lintFixture(
      [
        "const empty = {};",
        "const asserted = { ...(true ? ({} as const) : { value: 1 }) };",
        "const aliased = { ...(true ? empty : { value: 1 }) };",
      ].join("\n"),
      {},
      { "anti-slop/no-conditional-empty-object-spread": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toHaveLength(2);
	}, 120_000);

	it("keeps empty-object aliases scoped to their declarations", () => {
		const result = lintFixture(
			[
				"const empty = {};",
				"const alias = empty;",
				"const valid = { ...(true ? alias : { value: 1 }) };",
				"function shadowed(empty: object) { return { ...(true ? empty : { value: 1 }) }; }",
			].join("\n"),
			{},
			{ "anti-slop/no-conditional-empty-object-spread": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toHaveLength(1);
	}, 120_000);

	it("does not classify mutated empty-object aliases as empty", () => {
		const result = lintFixture(
			[
				"const empty = {};",
				"empty.value = 1;",
				"const result = { ...(true ? empty : { value: 2 }) };",
			].join("\n"),
			{},
			{ "anti-slop/no-conditional-empty-object-spread": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toStrictEqual([]);
	}, 120_000);

	it("invalidates empty-object aliases across calls", () => {
		const result = lintFixture(
			[
				"const assigned = {};",
				"const assignedAlias = assigned;",
				"Object.assign(assignedAlias, { value: 1 });",
				"const assignedResult = { ...(true ? assigned : { value: 2 }) };",
				"const defined = {};",
				"const definedAlias = defined;",
				"Object.defineProperty(definedAlias, 'value', { value: 1 });",
				"const definedResult = { ...(true ? defined : { value: 2 }) };",
				"const called = {};",
				"const calledAlias = called;",
				"mutate(calledAlias);",
				"const calledResult = { ...(true ? called : { value: 2 }) };",
				"function Mutate(target: object) { target.value = 1; }",
				"const constructed = {};",
				"const constructedAlias = constructed;",
				"new Mutate(constructedAlias);",
				"const constructedResult = { ...(true ? constructed : { value: 2 }) };",
			].join("\n"),
			{},
			{ "anti-slop/no-conditional-empty-object-spread": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toStrictEqual([]);
	}, 120_000);

	it("keeps unrelated empty-object aliases after a mutation", () => {
		const result = lintFixture(
			[
				"const first = {};",
				"const second = {};",
				"first.value = 1;",
				"const result = { ...(true ? second : { value: 2 }) };",
			].join("\n"),
			{},
			{ "anti-slop/no-conditional-empty-object-spread": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toHaveLength(1);
	}, 120_000);

	it("invalidates every variable in an empty-object alias class", () => {
		const result = lintFixture(
			[
				"const source = {};",
				"const sourceAlias = source;",
				"source.value = 1;",
				"const sourceResult = { ...(true ? sourceAlias : { value: 2 }) };",
				"const aliasSource = {};",
				"const alias = aliasSource;",
				"alias.value = 1;",
				"const aliasResult = { ...(true ? aliasSource : { value: 2 }) };",
			].join("\n"),
			{},
			{ "anti-slop/no-conditional-empty-object-spread": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-conditional-empty-object-spread")).toStrictEqual([]);
	}, 120_000);

  it("checks method and interface names for forbidden terms", () => {
    const result = lintFixture(
      [
        "class Container { shape() {} }",
        "const object = { shape() {} };",
        "type Data = { shape: string };",
        "interface Contract { shape(): void; }",
      ].join("\n"),
      {},
      { "anti-slop/no-shape-in-symbol-names": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-shape-in-symbol-names")).toHaveLength(3);
  }, 120_000);

	it("resolves union and generic unknown aliases without rejecting schema boundaries", () => {
    const result = lintFixture(
      [
        "import { Schema } from 'effect';",
        "type HiddenUnknown = unknown;",
        "type HiddenUnion = HiddenUnknown | never;",
        "type Generic<T> = T;",
        "type GenericUnknown = Generic<unknown>;",
        "type Defaulted<T = unknown> = T;",
        "type DefaultedUnknown = Defaulted;",
        "type SchemaBoundary = Schema.Schema.Type<typeof Schema.Unknown>;",
        "type ConcreteSchema = Schema.Schema.Type<typeof Schema.String>;",
        "type GenericSchema<S> = Schema.Schema.Type<S>;",
      ].join("\n"),
      {},
      { "anti-slop/no-unknown-type-aliases": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-unknown-type-aliases")).toHaveLength(4);
	}, 120_000);

	it("allows explicit Schema.Unknown parameters at decoder boundaries", () => {
		const result = lintFixture(
			[
				"import { Schema } from 'effect';",
				"function decode(value: Schema.Schema.Type<typeof Schema.Unknown>) { return value; }",
			].join("\n"),
			{},
			{ "anti-slop/no-unknown-parameters": "error" },
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-unknown-parameters")).toStrictEqual([]);
	}, 120_000);

  it("covers every enabled assertion, spread, runtime, alias, and widening rule", () => {
    const result = lintFixture(
      [
        "declare const external: unknown;",
        "const chained = ({ value: 1 } as { readonly value: number }) as { readonly value: number };",
        "const conditional = { ...(true ? {} : { value: 1 }) };",
        "const observed = typeof external;",
        "type HiddenUnknown = unknown;",
        "const widened: object = { value: 1 };",
        "const narrowed = widened as { readonly value: number };",
      ].join("\n"),
      {},
      {
        "anti-slop/no-chained-type-assertions": "error",
        "anti-slop/no-conditional-empty-object-spread": "error",
        "anti-slop/no-runtime-typeof": "error",
        "anti-slop/no-unknown-type-aliases": "error",
        "anti-slop/no-widen-then-assert": "error",
      },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    for (const rule of [
      "no-chained-type-assertions",
      "no-conditional-empty-object-spread",
      "no-runtime-typeof",
      "no-unknown-type-aliases",
      "no-widen-then-assert",
    ]) {
      expect(diagnosticsFor(result, rule)).toHaveLength(1);
    }
  }, 120_000);

  it("reports immutable broadening but allows mutable boundary state", () => {
    const result = lintFixture(
      [
        "const widened: object = { value: 1 };",
        "let mutable: unknown = { value: 1 };",
        "mutable = { value: 2 };",
      ].join("\n"),
      {},
      { "anti-slop/no-known-value-widening": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-known-value-widening")).toHaveLength(1);
  }, 120_000);

  it("rejects named and anonymous broad targets", () => {
    const result = lintFixture(
      [
        "type OwnerContract = Record<string, unknown>;",
        "const named: OwnerContract = { value: 1 };",
        "const anonymous: Record<string, unknown> = { value: 1 };",
      ].join("\n"),
      {},
      { "anti-slop/no-known-value-widening": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(diagnosticsFor(result, "no-known-value-widening")).toHaveLength(2);
  }, 120_000);

	it("does not crash on incomplete generic dictionary syntax", () => {
    const result = lintFixture(
      "const incomplete: Record<string> = {};",
      {},
      { "anti-slop/no-known-value-widening": "error" },
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
		expect(diagnosticsFor(result, "no-known-value-widening")).toStrictEqual([]);
	}, 120_000);

	it("unwraps non-null and satisfies wrappers around assertion chains", () => {
		const result = lintFixture(
			[
				"declare const external: unknown;",
				"const chained = ((external as object)! as { readonly id: string });",
				"const chainedSatisfies = ((external as object) satisfies object) as { readonly id: string };",
				"const widened: object = { id: 1 };",
				"const narrowed = (widened!) as { readonly id: number };",
				"const narrowedWithSatisfies = (widened satisfies object) as { readonly id: number };",
			].join("\n"),
			{},
			{
				"anti-slop/no-chained-type-assertions": "error",
				"anti-slop/no-widen-then-assert": "error",
			},
		);

		expect(result.error).toBeUndefined();
		expect(result.status).toBe(1);
		expect(diagnosticsFor(result, "no-chained-type-assertions")).toHaveLength(2);
		expect(diagnosticsFor(result, "no-widen-then-assert")).toHaveLength(2);
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
    expect(diagnosticsFor(result, "no-unsafe-dictionary-type")).toHaveLength(1);
  }, 120_000);

  it("enables every configured anti-slop rule through the repository lint configuration", () => {
    const result = lintFixture(
      [
        "declare const external: unknown;",
        "const chained = ({ value: 1 } as { readonly value: number }) as { readonly value: number };",
        "const conditional = { ...(true ? {} : { value: 1 }) };",
        "const widened: object = { value: 1 };",
        "function acceptsObject(value: object) { return value; }",
        "const observed = typeof external;",
        "const shape = 1;",
        "function acceptsUnknown(value: unknown) { return value; }",
        "type HiddenUnknown = unknown;",
        "const narrowed = widened as { readonly value: number };",
        "export type RepositoryUnsafe = Record<string, unknown>;",
      ].join("\n"),
      {},
      {},
      "repository",
    );

    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    for (const rule of [
      "no-chained-type-assertions",
      "no-conditional-empty-object-spread",
      "no-known-value-widening",
      "no-object-parameters",
      "no-runtime-typeof",
      "no-shape-in-symbol-names",
      "no-unknown-parameters",
      "no-unknown-type-aliases",
      "no-unsafe-dictionary-type",
      "no-widen-then-assert",
    ]) {
      expect(diagnosticsFor(result, rule)).not.toStrictEqual([]);
    }
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
    expect(diagnosticsFor(result, "no-unsafe-dictionary-type")).toStrictEqual([]);
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
    expect(diagnosticsFor(result, "no-unsafe-dictionary-type")).toStrictEqual([]);
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
