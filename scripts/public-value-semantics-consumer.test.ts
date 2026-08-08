import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "@effect/vitest";
import { preparePublicPackage } from "./release-publish-orchestration.mjs";

type PackedFile = {
  readonly path: string;
};

type PackResult = {
  readonly filename: string;
  readonly files: ReadonlyArray<PackedFile>;
};

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const publicPackageDirectory = join(repositoryRoot, "packages", "effect-view-server");

const readJson = (path: string): unknown => JSON.parse(readFileSync(path, "utf8"));

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const objectProperty = (value: unknown, key: string): Record<string, unknown> => {
  if (!isRecord(value)) {
    throw new Error(`${key} must be an object.`);
  }
  return value;
};

const stringProperty = (value: unknown, key: string): string => {
  if (typeof value !== "string") {
    throw new Error(`${key} must be a string.`);
  }
  return value;
};

const parsePackResult = (output: string): PackResult => {
  const parsed: unknown = JSON.parse(output);
  if (!Array.isArray(parsed) || parsed.length !== 1) {
    throw new Error("npm pack must return exactly one package result.");
  }
  const result = objectProperty(parsed[0], "npm pack result");
  if (!Array.isArray(result.files)) {
    throw new Error("npm pack result files must be an array.");
  }
  return {
    filename: stringProperty(result.filename, "npm pack result filename"),
    files: result.files.map((file, index) => ({
      path: stringProperty(
        objectProperty(file, `npm pack result files[${index}]`).path,
        `npm pack result files[${index}].path`,
      ),
    })),
  };
};

const collectStaticModuleGraph = (
  entryPath: string,
): {
  readonly bareSpecifiers: ReadonlyArray<string>;
  readonly files: ReadonlyArray<string>;
  readonly source: string;
} => {
  const pending = [entryPath];
  const visited = new Set<string>();
  const bareSpecifiers = new Set<string>();
  const sources: Array<string> = [];
  const importPattern = /(?:\bfrom\s+|\bimport\s+(?:\(\s*)?)(["'])([^"']+)\1/g;

  while (pending.length > 0) {
    const path = pending.pop();
    if (path === undefined || visited.has(path)) {
      continue;
    }
    visited.add(path);
    const source = readFileSync(path, "utf8");
    sources.push(source);

    for (const match of source.matchAll(importPattern)) {
      const specifier = match[2];
      if (specifier === undefined) {
        continue;
      }
      if (!specifier.startsWith(".")) {
        bareSpecifiers.add(specifier);
        continue;
      }
      const importedPath = resolve(dirname(path), specifier);
      const importedRelativePath = relative(dirname(entryPath), importedPath);
      if (isAbsolute(importedRelativePath) || importedRelativePath.startsWith("..")) {
        throw new Error(`Focused module graph escaped its package directory: ${specifier}`);
      }
      pending.push(importedPath);
    }
  }

  return {
    bareSpecifiers: [...bareSpecifiers].sort(),
    files: [...visited].sort(),
    source: sources.join("\n"),
  };
};

describe("published value semantics consumer", () => {
  it("packs, installs, type-checks, and imports the sanitized public subpath", ({
    onTestFinished,
  }) => {
    const temporaryRoot = mkdtempSync(join(tmpdir(), "view-server-value-semantics-"));
    onTestFinished(() => rmSync(temporaryRoot, { force: true, recursive: true }));

    {
      const publishDirectory = join(temporaryRoot, "publish");
      const consumerDirectory = join(temporaryRoot, "consumer");
      const installedPackageDirectory = join(
        consumerDirectory,
        "node_modules",
        "effect-view-server",
      );
      mkdirSync(publishDirectory, { recursive: true });
      mkdirSync(installedPackageDirectory, { recursive: true });

      const packageJson = objectProperty(
        readJson(join(publicPackageDirectory, "package.json")),
        "public package manifest",
      );
      preparePublicPackage({
        packageJson,
        publicPackageDirectory,
        publishDirectory,
        releaseVersion: "9.9.9",
      });

      const packOutput = execFileSync(
        "npm",
        ["pack", publishDirectory, "--json", "--pack-destination", temporaryRoot],
        { cwd: repositoryRoot, encoding: "utf8" },
      );
      const packResult = parsePackResult(packOutput);
      const packedPaths = packResult.files.map((file) => file.path);
      expect(packedPaths).toContain("dist/value-semantics.js");
      expect(packedPaths).toContain("dist/value-semantics.d.ts");

      execFileSync(
        "tar",
        [
          "-xzf",
          join(temporaryRoot, packResult.filename),
          "-C",
          installedPackageDirectory,
          "--strip-components=1",
        ],
        { cwd: repositoryRoot },
      );
      symlinkSync(
        realpathSync(join(repositoryRoot, "node_modules", "effect")),
        join(consumerDirectory, "node_modules", "effect"),
        "junction",
      );

      const installedManifest = objectProperty(
        readJson(join(installedPackageDirectory, "package.json")),
        "installed package manifest",
      );
      const exportsMap = objectProperty(installedManifest.exports, "exports");
      const valueSemanticsExport = objectProperty(
        exportsMap["./value-semantics"],
        "exports['./value-semantics']",
      );
      const entryTarget = stringProperty(
        valueSemanticsExport.import,
        "exports['./value-semantics'].import",
      );
      const declarationTarget = stringProperty(
        valueSemanticsExport.types,
        "exports['./value-semantics'].types",
      );
      const peers = objectProperty(installedManifest.peerDependencies, "peerDependencies");
      expect(peers.effect).toBe("4.0.0-beta.100");

      const graph = collectStaticModuleGraph(
        join(installedPackageDirectory, entryTarget.replace(/^\.\//, "")),
      );
      const graphRelativeFiles = graph.files.map((path) =>
        path.slice(installedPackageDirectory.length + 1),
      );
      expect(graph.bareSpecifiers).toStrictEqual(["effect/BigDecimal"]);
      expect(graphRelativeFiles).toContain("dist/value-semantics.js");
      expect(graphRelativeFiles.every((path) => packedPaths.includes(path))).toBe(true);
      expect(graph.source).not.toContain("@effect-view-server/");
      expect(graph.source).not.toContain('from "effect"');
      expect(graph.source).not.toContain("effect/Schema");
      const declarationSource = readFileSync(
        join(installedPackageDirectory, declarationTarget.replace(/^\.\//, "")),
        "utf8",
      );
      expect(declarationSource).not.toContain("@effect-view-server/");

      writeFileSync(
        join(consumerDirectory, "runtime.mjs"),
        [
          'import * as BigDecimal from "effect/BigDecimal";',
          'import { compareTrustedWireSafeBigDecimal, inspectWireSafeBigDecimal } from "effect-view-server/value-semantics";',
          "const scaled = BigDecimal.make(150n, 2);",
          "const canonical = BigDecimal.make(15n, 1);",
          'if (compareTrustedWireSafeBigDecimal(scaled, canonical) !== 0) throw new Error("scaled equality drifted");',
          'if (inspectWireSafeBigDecimal(scaled)._tag !== "Success") throw new Error("wire admission drifted");',
        ].join("\n"),
      );
      execFileSync(process.execPath, [join(consumerDirectory, "runtime.mjs")], {
        cwd: consumerDirectory,
      });

      writeFileSync(
        join(consumerDirectory, "consumer.ts"),
        [
          'import type { BigDecimal } from "effect/BigDecimal";',
          'import { compareTrustedWireSafeBigDecimal, inspectWireSafeBigDecimal, type WireSafeBigDecimalInspection } from "effect-view-server/value-semantics";',
          '// @ts-expect-error package-internal deep paths are not public.',
          'import "effect-view-server/value-semantics/internal";',
          "declare const decimal: BigDecimal;",
          "const comparison: number | undefined = compareTrustedWireSafeBigDecimal(decimal, decimal);",
          "const inspection: WireSafeBigDecimalInspection = inspectWireSafeBigDecimal(decimal);",
          "void comparison;",
          "void inspection;",
          "// @ts-expect-error trusted comparison rejects non-BigDecimal values.",
          "compareTrustedWireSafeBigDecimal({}, decimal);",
        ].join("\n"),
      );
      writeFileSync(
        join(consumerDirectory, "tsconfig.json"),
        `${JSON.stringify(
          {
            compilerOptions: {
              exactOptionalPropertyTypes: true,
              lib: ["DOM", "ES2022", "ESNext.Disposable"],
              module: "NodeNext",
              moduleResolution: "NodeNext",
              noEmit: true,
              noUncheckedIndexedAccess: true,
              strict: true,
              target: "ES2022",
            },
            include: ["consumer.ts"],
          },
          null,
          2,
        )}\n`,
      );
      execFileSync(
        process.execPath,
        [
          join(repositoryRoot, "node_modules", "typescript", "bin", "tsc"),
          "-p",
          join(consumerDirectory, "tsconfig.json"),
        ],
        { cwd: consumerDirectory, stdio: "inherit" },
      );
    }
  }, 30_000);
});
