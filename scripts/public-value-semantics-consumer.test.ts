import { execFileSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
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
const testedCommit = execFileSync("git", ["rev-parse", "HEAD"], {
  cwd: repositoryRoot,
  encoding: "utf8",
}).trim();

const copyConsumerPackage = (sourceDirectory: string, targetDirectory: string): void => {
  mkdirSync(targetDirectory, { recursive: true });
  cpSync(
    join(sourceDirectory, "package.json"),
    join(targetDirectory, "package.json"),
    { dereference: true },
  );
  const packageJson = JSON.parse(readFileSync(join(sourceDirectory, "package.json"), "utf8"));
  const targets = new Set<string>();
  const collectTargets = (value: unknown): void => {
    if (typeof value === "string") {
      targets.add(value);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach(collectTargets);
      return;
    }
    if (value !== null && typeof value === "object") {
      Object.values(value).forEach(collectTargets);
    }
  };
  collectTargets(packageJson.exports);
  collectTargets(packageJson.main);
  collectTargets(packageJson.module);
  collectTargets(packageJson.types);
  const copiedTopLevelEntries = new Set<string>();
  for (const target of targets) {
    if (!target.startsWith("./")) {
      continue;
    }
    const relativeTarget = target.slice(2).split("*")[0];
    const topLevelEntry = relativeTarget.split("/")[0];
    if (topLevelEntry === "" || copiedTopLevelEntries.has(topLevelEntry)) {
      continue;
    }
    const sourcePath = join(sourceDirectory, topLevelEntry);
    if (!existsSync(sourcePath)) {
      continue;
    }
    cpSync(sourcePath, join(targetDirectory, topLevelEntry), {
      dereference: true,
      recursive: true,
    });
    copiedTopLevelEntries.add(topLevelEntry);
  }
};

const copyConsumerDependencyTree = (sourceDirectory: string, targetDirectory: string): void => {
  for (const entry of readdirSync(sourceDirectory, { withFileTypes: true })) {
    if (entry.name === "effect") {
      continue;
    }
    const sourcePath = join(sourceDirectory, entry.name);
    if ((entry.isDirectory() || entry.isSymbolicLink()) && entry.name.startsWith("@")) {
      for (const scopedEntry of readdirSync(sourcePath, { withFileTypes: true })) {
        if (!scopedEntry.isDirectory() && !scopedEntry.isSymbolicLink()) {
          continue;
        }
        copyConsumerPackage(
          join(sourcePath, scopedEntry.name),
          join(targetDirectory, entry.name, scopedEntry.name),
        );
      }
      continue;
    }
    if (entry.isDirectory() || entry.isSymbolicLink()) {
      copyConsumerPackage(sourcePath, join(targetDirectory, entry.name));
    }
  }
};

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
  GITHUB_SHA: testedCommit,
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
  Schema.Struct({ effect: Schema.String, typescript: Schema.String }),
  [Schema.Record(Schema.String, Schema.String)],
);
const PackageManifest = Schema.StructWithRest(
  Schema.Struct({
    dependencies: Schema.Record(Schema.String, Schema.String),
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

      execFileSync(process.execPath, ["scripts/prepare-release-artifact.mjs"], {
        cwd: repositoryRoot,
        stdio: "inherit",
      });

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
      expect(packedPaths).not.toContain("dist/effect-schemaast-compat.d.ts");

      const strictConsumerDirectory = join(temporaryRoot, "strict-consumer");
      mkdirSync(strictConsumerDirectory, { recursive: true });
      writeFileSync(
        join(strictConsumerDirectory, "package.json"),
        `${JSON.stringify(
          {
            name: "effect-view-server-strict-peer-consumer",
            private: true,
            packageManager: "pnpm@11.9.0",
            dependencies: {
              "@emnapi/core": "1.7.1",
              "@emnapi/runtime": "1.7.1",
              "@effect/vitest": "4.0.0-rc.111",
              effect: "4.0.0-rc.111",
              "effect-view-server": `file:${join(temporaryRoot, packResult.filename)}`,
              redis: "6.2.1",
              typescript: "7.0.2",
              vite: "8.0.0",
              vitest: "4.1.10",
            },
          },
          null,
          2,
        )}\n`,
      );
      writeFileSync(
        join(strictConsumerDirectory, "pnpm-workspace.yaml"),
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
        cwd: strictConsumerDirectory,
        killSignal: "SIGTERM",
        stdio: "inherit",
        timeout: 55_000,
      });

      const strictInstalledPackageDirectory = join(
        strictConsumerDirectory,
        "node_modules",
        "effect-view-server",
      );
      const strictInstalledManifest = decodePackageManifest(
        readFileSync(join(strictInstalledPackageDirectory, "package.json"), "utf8"),
      );
      expect(strictInstalledManifest.dependencies).toStrictEqual({
        "@bufbuild/protobuf": "2.13.0",
        "@connectrpc/connect": "2.1.2",
        "@connectrpc/connect-node": "2.1.2",
        "@effect/platform-browser": "4.0.0-rc.111",
        "@effect/platform-node": "4.0.0-rc.111",
        "@effect/platform-node-shared": "4.0.0-rc.111",
        "@platformatic/kafka": "2.9.0",
      });
      for (const path of packedPaths.filter(
        (path) => path.endsWith(".js") || path.endsWith(".d.ts"),
      )) {
        expect(readFileSync(join(strictInstalledPackageDirectory, path), "utf8")).not.toContain(
          "typescript-compiler-api",
        );
      }
      expect(strictInstalledManifest.peerDependencies).toStrictEqual({
        "@effect/atom-react": "4.0.0-rc.111",
        "@effect/vitest": "4.0.0-rc.111",
        effect: "4.0.0-rc.111",
        react: "19.2.8",
        "react-dom": "19.2.8",
        typescript: ">=7.0.0 <8.0.0",
        vite: "*",
      });
      const strictLockfile = readFileSync(
        join(strictConsumerDirectory, "pnpm-lock.yaml"),
        "utf8",
      );
      expect(strictLockfile).toContain(
        "@effect/platform-node-shared@4.0.0-rc.111(effect@4.0.0-rc.111)",
      );
      expect(strictLockfile).not.toContain("@effect/platform-node-shared@4.0.0-rc.112");
      expect(strictLockfile).toMatch(
        /effect-view-server@file:[^\n]+\(effect@4\.0\.0-rc\.111\)[^\n]*\(typescript@7\.0\.2\)/,
      );
      expect(strictLockfile).not.toContain("typescript@6.0.3");
      expect(
        readFileSync(join(strictInstalledPackageDirectory, "dist", "react.d.ts"), "utf8"),
      ).toContain("LiveQueryViewportBaseRow");
      execFileSync(
        process.execPath,
        ["--input-type=module", "--eval", 'await import("effect-view-server/source-adapter/testing")'],
        {
          cwd: strictConsumerDirectory,
          stdio: "inherit",
        },
      );

      extract({
        cwd: installedPackageDirectory,
        file: join(temporaryRoot, packResult.filename),
        strip: 1,
        sync: true,
      });
      const effectDirectory = realpathSync(join(repositoryRoot, "node_modules", "effect"));
      const consumerEffectDirectory = join(consumerDirectory, "node_modules", "effect");
      copyConsumerPackage(effectDirectory, consumerEffectDirectory);
      mkdirSync(join(consumerEffectDirectory, "node_modules"), { recursive: true });
      copyConsumerDependencyTree(
        dirname(effectDirectory),
        join(consumerEffectDirectory, "node_modules"),
      );
      const fastCheckDirectory = realpathSync(join(dirname(effectDirectory), "fast-check"));
      copyConsumerPackage(
        realpathSync(join(dirname(fastCheckDirectory), "pure-rand")),
        join(consumerEffectDirectory, "node_modules", "pure-rand"),
      );
      const installedManifest = decodePackageManifest(
        readFileSync(join(installedPackageDirectory, "package.json"), "utf8"),
      );
      const valueSemanticsExport = installedManifest.exports["./value-semantics"];
      const entryTarget = valueSemanticsExport.import;
      const declarationTarget = valueSemanticsExport.types;
      expect(installedManifest.peerDependencies.effect).toBe("4.0.0-rc.111");

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
      expect(declarationSource).not.toContain("effect-schemaast-compat.d.ts");
      expect(declarationSource).not.toContain("@effect-view-server/");

      writeFileSync(
        join(consumerDirectory, "runtime.mjs"),
        [
          'import * as BigDecimal from "effect/BigDecimal";',
          'import { compareTrustedWireSafeBigDecimal, compareWireSafeBigDecimalComparisonMetadata, inspectWireSafeBigDecimal, trustedWireSafeBigDecimalComparisonMetadata } from "effect-view-server/value-semantics";',
          "const scaled = BigDecimal.make(150n, 2);",
          "const canonical = BigDecimal.make(15n, 1);",
          'if (compareTrustedWireSafeBigDecimal(scaled, canonical) !== 0) throw new Error("scaled equality drifted");',
          'if (inspectWireSafeBigDecimal(scaled)._tag !== "Success") throw new Error("wire admission drifted");',
          'const scaledMetadata = trustedWireSafeBigDecimalComparisonMetadata(scaled);',
          'const canonicalMetadata = trustedWireSafeBigDecimalComparisonMetadata(canonical);',
          'if (scaledMetadata === undefined || canonicalMetadata === undefined || compareWireSafeBigDecimalComparisonMetadata(scaledMetadata, canonicalMetadata) !== 0) throw new Error("metadata comparison drifted");',
        ].join("\n"),
      );
      execFileSync(process.execPath, [join(consumerDirectory, "runtime.mjs")], {
        cwd: consumerDirectory,
      });

      writeFileSync(
        join(consumerDirectory, "consumer.ts"),
        [
          'import type { BigDecimal } from "effect/BigDecimal";',
          'import { compareTrustedWireSafeBigDecimal, compareWireSafeBigDecimalComparisonMetadata, inspectWireSafeBigDecimal, trustedWireSafeBigDecimalComparisonMetadata, type WireSafeBigDecimal, type WireSafeBigDecimalComparisonMetadata, type WireSafeBigDecimalInspection } from "effect-view-server/value-semantics";',
          '// @ts-expect-error package-internal deep paths are not public.',
          'import "effect-view-server/value-semantics/internal";',
          "declare const decimal: BigDecimal;",
          "declare const definiteMetadata: WireSafeBigDecimalComparisonMetadata;",
          "const comparison: number | undefined = compareTrustedWireSafeBigDecimal(decimal, decimal);",
          "const inspection: WireSafeBigDecimalInspection = inspectWireSafeBigDecimal(decimal);",
          "const metadata: WireSafeBigDecimalComparisonMetadata | undefined = trustedWireSafeBigDecimalComparisonMetadata(decimal);",
          "if (metadata !== undefined) compareWireSafeBigDecimalComparisonMetadata(metadata, metadata);",
          "const wireDecimal: WireSafeBigDecimal = decimal;",
          "void comparison;",
          "void inspection;",
          "void wireDecimal;",
          "// @ts-expect-error trusted comparison rejects non-BigDecimal values.",
          "compareTrustedWireSafeBigDecimal({}, decimal);",
          "// @ts-expect-error metadata comparison rejects raw BigDecimal values.",
          "compareWireSafeBigDecimalComparisonMetadata(decimal, definiteMetadata);",
          "// @ts-expect-error opaque metadata cannot be constructed structurally.",
          "compareWireSafeBigDecimalComparisonMetadata({}, definiteMetadata);",
          "// @ts-expect-error trusted metadata construction rejects non-BigDecimal values.",
          "trustedWireSafeBigDecimalComparisonMetadata({});",
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
  }, 60_000);
});
