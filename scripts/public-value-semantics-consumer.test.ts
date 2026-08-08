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
import { Schema } from "effect";
import { extract } from "tar";
import { runReleasePublish } from "./release-publish-orchestration.mjs";
import { inspectTypeScriptModule } from "./typescript-module-inspection";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));

type CommandResult = {
  readonly status: number;
  readonly stderr: string;
  readonly stdout: string;
};

const trustedEnvironment = {
  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "token",
  ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.actions.githubusercontent.com",
  GITHUB_ACTIONS: "true",
  GITHUB_EVENT_NAME: "push",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "bmvantunes/effect-view-server",
};

const commandResult = ({
  status = 0,
  stderr = "",
  stdout = "",
}: Partial<CommandResult> = {}): CommandResult => ({ status, stderr, stdout });

const PackageExport = Schema.Struct({
  import: Schema.String,
  types: Schema.String,
});
const PackageExports = Schema.StructWithRest(
  Schema.Struct({ "./value-semantics": PackageExport }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
const PackagePeerDependencies = Schema.StructWithRest(
  Schema.Struct({ effect: Schema.String }),
  [Schema.Record(Schema.String, Schema.String)],
);
const PackageManifest = Schema.StructWithRest(
  Schema.Struct({
    exports: PackageExports,
    peerDependencies: PackagePeerDependencies,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
const NpmPackResult = Schema.Struct({
  filename: Schema.String,
  files: Schema.Array(Schema.Struct({ path: Schema.String })),
});
const decodePackageManifest = Schema.decodeUnknownSync(Schema.fromJsonString(PackageManifest));
const decodeNpmPackResult = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Tuple([NpmPackResult])),
);

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

  while (pending.length > 0) {
    const path = pending.pop();
    if (path === undefined || visited.has(path)) {
      continue;
    }
    visited.add(path);
    const source = readFileSync(path, "utf8");
    sources.push(source);
    const inspection = inspectTypeScriptModule({ fileName: path, source });
    if (inspection.violations.length > 0) {
      throw new Error(
        `Focused module graph contains unsupported module loading: ${JSON.stringify(inspection.violations)}`,
      );
    }

    for (const specifier of inspection.moduleSpecifiers) {
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
      const consumerDirectory = join(temporaryRoot, "consumer");
      const installedPackageDirectory = join(
        consumerDirectory,
        "node_modules",
        "effect-view-server",
      );
      mkdirSync(installedPackageDirectory, { recursive: true });

      let packOutput: string | undefined;
      const command = (executable: string, args: ReadonlyArray<string>): CommandResult => {
        if (executable === "npm" && args[0] === "view" && args[1] === "effect-view-server") {
          return commandResult({ stdout: '"0.0.6"' });
        }
        if (executable === "git" && args[0] === "fetch") {
          return commandResult();
        }
        if (executable === "git" && args[0] === "tag" && args[1] === "--list") {
          return commandResult({ stdout: "effect-view-server@0.0.6-staged\n" });
        }
        if (executable === "git" && args[0] === "diff") {
          return commandResult({ stdout: ".changeset/public-value-semantics.md\n" });
        }
        if (
          executable === "npm" &&
          args[0] === "view" &&
          args[1]?.startsWith("effect-view-server@") === true
        ) {
          return commandResult({ status: 1 });
        }
        if (executable === "git" && args[0] === "rev-parse" && args.at(-1) === "HEAD^{}") {
          return commandResult({ stdout: "head-object\n" });
        }
        if (executable === "git" && args[0] === "rev-parse") {
          return commandResult({ status: 1 });
        }
        if (executable === "git" && (args[0] === "tag" || args[0] === "push")) {
          return commandResult();
        }
        if (executable === "npm" && args[0] === "publish") {
          const publishDirectory = args[1];
          if (publishDirectory === undefined) {
            throw new Error("Release Publish Orchestration omitted its staged package directory.");
          }
          packOutput = execFileSync(
            "npm",
            ["pack", publishDirectory, "--json", "--pack-destination", temporaryRoot],
            { cwd: repositoryRoot, encoding: "utf8" },
          );
          return commandResult({ stdout: "published\n" });
        }
        throw new Error(`Unexpected release command: ${executable} ${args.join(" ")}`);
      };

      expect(
        runReleasePublish({
          command,
          env: trustedEnvironment,
          rootDirectory: repositoryRoot,
          stderr: () => undefined,
          stdout: () => undefined,
          temporaryDirectory: temporaryRoot,
        }),
      ).toMatchObject({ _tag: "Published" });

      if (packOutput === undefined) {
        throw new Error("Release Publish Orchestration did not publish its staged artifact.");
      }

      const [packResult] = decodeNpmPackResult(packOutput);
      const packedPaths = packResult.files.map((file) => file.path);
      expect(packedPaths).toContain("dist/value-semantics.js");
      expect(packedPaths).toContain("dist/value-semantics.d.ts");

      extract({
        cwd: installedPackageDirectory,
        file: join(temporaryRoot, packResult.filename),
        strip: 1,
        sync: true,
      });
      symlinkSync(
        realpathSync(join(repositoryRoot, "node_modules", "effect")),
        join(consumerDirectory, "node_modules", "effect"),
        "junction",
      );

      const installedManifest = decodePackageManifest(
        readFileSync(join(installedPackageDirectory, "package.json"), "utf8"),
      );
      const valueSemanticsExport = installedManifest.exports["./value-semantics"];
      const entryTarget = valueSemanticsExport.import;
      const declarationTarget = valueSemanticsExport.types;
      expect(installedManifest.peerDependencies.effect).toBe("4.0.0-beta.100");

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
