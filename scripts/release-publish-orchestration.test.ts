import { describe, expect, it } from "@effect/vitest";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ReleasePublishCommandError, runReleasePublish } from "./release-publish-orchestration.mjs";

type CommandOptions = {
  cwd?: string;
  encoding?: string;
  env?: NodeJS.ProcessEnv;
  stdio?: string | ReadonlyArray<string>;
};

type CommandCall = {
  args: ReadonlyArray<string>;
  command: string;
  options: CommandOptions;
};

type CommandResult = {
  error?: Error;
  status: number | null;
  stderr: string;
  stdout: string;
};

const trustedEnvironment = {
  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "token",
  ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.actions.githubusercontent.com",
  GITHUB_ACTIONS: "true",
  GITHUB_EVENT_NAME: "push",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "bmvantunes/effect-view-server",
};

const result = ({
  status = 0,
  stderr = "",
  stdout = "",
}: Partial<CommandResult> = {}): CommandResult => ({ status, stderr, stdout });

const writeJson = (path: string, value: unknown) => {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
};

const makeReleaseTree = (version = "0.0.6") => {
  const rootDirectory = mkdtempSync(join(tmpdir(), "view-server-release-root-"));
  const temporaryDirectory = join(rootDirectory, "temporary");
  const publicPackageDirectory = join(rootDirectory, "packages", "effect-view-server");
  const internalPackageDirectory = join(rootDirectory, "packages", "client");

  mkdirSync(join(publicPackageDirectory, "dist", "nested"), { recursive: true });
  mkdirSync(internalPackageDirectory, { recursive: true });
  mkdirSync(join(rootDirectory, ".changeset"));
  mkdirSync(join(rootDirectory, "apps", "missing-package"), { recursive: true });
  writeFileSync(join(rootDirectory, "apps", "README.md"), "workspace directory marker\n");
  mkdirSync(temporaryDirectory);
  writeJson(join(publicPackageDirectory, "package.json"), {
    name: "effect-view-server",
    version,
    description: "Typed Effect View Server.",
    type: "module",
    exports: {
      ".": {
        import: "./dist/index.js",
        types: "./dist/index.d.ts",
      },
    },
    publishConfig: { provenance: true },
    dependencies: {
      "@effect-view-server/client": "workspace:*",
      effect: "4.0.0-beta.106",
    },
    scripts: { build: "vp pack" },
  });
  writeJson(join(internalPackageDirectory, "package.json"), {
    name: "@effect-view-server/client",
    private: true,
  });
  writeFileSync(join(publicPackageDirectory, "README.md"), "# Public package\n");
  writeFileSync(
    join(publicPackageDirectory, "dist", "index.js"),
    "export const ready = true;\n//# sourceMappingURL=index.js.map\n",
  );
  writeFileSync(
    join(publicPackageDirectory, "dist", "index.d.ts"),
    "export declare const ready: true;\n//# sourceMappingURL=index.d.ts.map\n",
  );
  writeFileSync(join(publicPackageDirectory, "dist", "nested", "data.txt"), "ready\n");
  writeFileSync(join(publicPackageDirectory, "dist", "index.js.map"), "{}\n");

  return { publicPackageDirectory, rootDirectory, temporaryDirectory };
};

type ReleaseTree = ReturnType<typeof makeReleaseTree>;

const makeScenario = ({
  env = trustedEnvironment,
  releaseTree = makeReleaseTree(),
  changeset = '---\n"effect-view-server": patch\n---\n\nFix.\n',
  publishedVersion = "0.0.6",
  matchingTag = "effect-view-server@0.0.6-staged",
  releaseVersionExists = false,
  publishVersionExists = false,
  publishedVersionResult,
  publishResult = result({ stdout: "published\n" }),
  existingTagTarget,
  existingPendingTagTarget,
  existingPendingTagObject,
  gitTagResult,
  headTarget = "head-object",
  headTargetSequence,
  rootCommitOutput = "root-object\n",
  commandError,
  unsafePublishedFile = false,
}: {
  env?: NodeJS.ProcessEnv;
  releaseTree?: ReleaseTree;
  changeset?: string;
  publishedVersion?: string | number;
  matchingTag?: string;
  releaseVersionExists?: boolean;
  publishVersionExists?: boolean;
  publishedVersionResult?: CommandResult;
  publishResult?: CommandResult;
  existingTagTarget?: string;
  existingPendingTagTarget?: string;
  existingPendingTagObject?: string | null;
  gitTagResult?: CommandResult;
  headTarget?: string | null;
  headTargetSequence?: ReadonlyArray<string | null>;
  rootCommitOutput?: string;
  commandError?: Error;
  unsafePublishedFile?: boolean;
} = {}) => {
  const calls: Array<CommandCall> = [];
  const stdout: Array<string> = [];
  const stderr: Array<string> = [];
  let releaseVersionViewCount = 0;
  let headTargetCallCount = 0;
  let publishedArtifact: {
    declaration: string;
    files: Array<string>;
    manifest: Record<string, unknown>;
    nestedFile: string;
    readme: string;
    runtime: string;
    sourceMapExists: boolean;
  } | undefined;

  writeFileSync(join(releaseTree.rootDirectory, ".changeset", "release.md"), changeset);
  if (unsafePublishedFile) {
    writeFileSync(
      join(releaseTree.publicPackageDirectory, "dist", "unsafe.js"),
      'import { client } from "@effect-view-server/client";\nexport { client };\n',
    );
  }

  const command = (nextCommand: string, args: ReadonlyArray<string>, options: CommandOptions) => {
    calls.push({ args, command: nextCommand, options });

    if (commandError !== undefined) {
      return { error: commandError, status: null, stderr: "", stdout: "" };
    }

    if (nextCommand === "npm" && args[0] === "view" && args[1] === "effect-view-server") {
      return publishedVersionResult ?? result({ stdout: JSON.stringify(publishedVersion) });
    }
    if (nextCommand === "git" && args[0] === "tag" && args[1] === "--list") {
      return result({ stdout: `${matchingTag}\n` });
    }
    if (nextCommand === "git" && args[0] === "fetch") {
      return result();
    }
    if (nextCommand === "git" && args[0] === "rev-list") {
      return result({ stdout: rootCommitOutput });
    }
    if (nextCommand === "git" && args[0] === "diff") {
      return result({ stdout: ".changeset/release.md\n" });
    }
    if (nextCommand === "npm" && args[0] === "view" && args[1]?.startsWith("effect-view-server@")) {
      releaseVersionViewCount += 1;
      const exists = releaseVersionExists || (publishVersionExists && releaseVersionViewCount > 1);
      return exists
        ? result({ stdout: JSON.stringify(args[1].slice("effect-view-server@".length)) })
        : result({ status: 1 });
    }
    if (nextCommand === "npm" && args[0] === "publish") {
      const publishDirectory = args[1];
      if (publishDirectory === undefined) {
        throw new Error("The direct npm publish command is missing its package directory.");
      }
      publishedArtifact = {
        declaration: readFileSync(join(publishDirectory, "dist", "index.d.ts"), "utf8"),
        files: readdirSync(publishDirectory).sort(),
        manifest: JSON.parse(readFileSync(join(publishDirectory, "package.json"), "utf8")),
        nestedFile: readFileSync(join(publishDirectory, "dist", "nested", "data.txt"), "utf8"),
        readme: readFileSync(join(publishDirectory, "README.md"), "utf8"),
        runtime: readFileSync(join(publishDirectory, "dist", "index.js"), "utf8"),
        sourceMapExists: existsSync(join(publishDirectory, "dist", "index.js.map")),
      };
      return publishResult;
    }
    if (nextCommand === "git" && args[0] === "rev-parse" && args.at(-1) === "HEAD^{}") {
      const sequenceTarget = headTargetSequence?.[headTargetCallCount];
      const target = sequenceTarget === undefined ? headTarget : sequenceTarget;
      headTargetCallCount += 1;
      return target === null ? result({ status: 1 }) : result({ stdout: `${target}\n` });
    }
    if (nextCommand === "git" && args[0] === "rev-parse") {
      const ref = args.at(-1);
      const isPending = ref?.includes("-pending") === true;
      const isRawPending = ref?.endsWith("-pending") === true;
      const existingTarget = isPending
        ? isRawPending
          ? (existingPendingTagObject === undefined
              ? existingPendingTagTarget
              : existingPendingTagObject)
          : existingPendingTagTarget
        : existingTagTarget;
      return existingTarget === undefined || existingTarget === null
        ? result({ status: 1 })
        : result({ stdout: `${existingTarget}\n` });
    }
    if (nextCommand === "git" && args[0] === "tag") {
      return gitTagResult ?? result();
    }
    if (nextCommand === "git" && args[0] === "push") {
      return result();
    }

    throw new Error(`Unexpected command: ${nextCommand} ${args.join(" ")}`);
  };

  return {
    ...releaseTree,
    calls,
    cleanup: () => rmSync(releaseTree.rootDirectory, { force: true, recursive: true }),
    publishedArtifact: () => publishedArtifact,
    run: () =>
      runReleasePublish({
        command,
        env,
        rootDirectory: releaseTree.rootDirectory,
        stderr: (message: string) => stderr.push(message),
        stdout: (message: string) => stdout.push(message),
        temporaryDirectory: releaseTree.temporaryDirectory,
      }),
    stderr,
    stdout,
  };
};

describe("release publish orchestration", () => {
  it("publishes one sanitized package directly and creates a public tag", () => {
    const scenario = makeScenario();

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(scenario.publishedArtifact()).toStrictEqual({
      declaration:
        '/// <reference path="./effect-schemaast-compat.d.ts" />\nexport declare const ready: true;\n',
      files: ["README.md", "dist", "package.json"],
      manifest: {
        name: "effect-view-server",
        version: "0.0.7",
        description: "Typed Effect View Server.",
        type: "module",
        exports: {
          ".": { import: "./dist/index.js", types: "./dist/index.d.ts" },
        },
        files: ["dist", "README.md"],
        publishConfig: { access: "public", provenance: true },
        dependencies: { effect: "4.0.0-beta.106" },
      },
      nestedFile: "ready\n",
      readme: "# Public package\n",
      runtime: "export const ready = true;\n",
      sourceMapExists: false,
    });
    expect(scenario.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["npm", "view", "effect-view-server", "version", "--json"],
      ["git", "fetch", "--tags", "origin"],
      ["git", "tag", "--list", "effect-view-server@*"],
      [
        "git",
        "diff",
        "--name-only",
        "effect-view-server@0.0.6-staged..HEAD",
        "--",
        ".changeset",
      ],
      ["npm", "view", "effect-view-server@0.0.7", "version", "--json"],
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@0.0.7-pending^{}",
      ],
      [
        "git",
        "tag",
        "-a",
        "effect-view-server@0.0.7-pending",
        "head-object",
        "-m",
        "effect-view-server@0.0.7-pending",
      ],
      ["git", "push", "origin", "refs/tags/effect-view-server@0.0.7-pending"],
      ["npm", "publish", scenario.calls[9]?.args[1], "--provenance", "--access", "public"],
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@0.0.7^{}",
      ],
      [
        "git",
        "tag",
        "-a",
        "effect-view-server@0.0.7",
        "head-object",
        "-m",
        "effect-view-server@0.0.7",
      ],
      ["git", "push", "origin", "refs/tags/effect-view-server@0.0.7"],
    ]);
    expect(scenario.stdout).toStrictEqual(["published\n", "effect-view-server@0.0.7 published as patch.\n"]);
    expect(scenario.stderr).toStrictEqual([""]);
    expect(readdirSync(scenario.temporaryDirectory)).toStrictEqual([]);

    scenario.cleanup();
  });

  it("uses the strongest changeset since the last published tag during migration", () => {
    const scenario = makeScenario({
      changeset: '---\n"effect-view-server": major\n---\n\nBreaking.\n',
    });

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "major",
      version: "1.0.0",
    });
    expect(scenario.calls.some(({ command, args }) => command === "npm" && args[0] === "publish")).toBe(
      true,
    );

    scenario.cleanup();
  });

  it("selects the newest matching release marker when multiple tags exist", () => {
    const scenario = makeScenario({
      matchingTag: "effect-view-server@0.0.6\neffect-view-server@0.0.6-staged",
    });

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });

    scenario.cleanup();
  });

  it("continues when a pending marker has no resolvable git target", () => {
    const scenario = makeScenario({
      existingPendingTagTarget: undefined,
      matchingTag: "effect-view-server@0.0.6-pending",
    });

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });

    scenario.cleanup();
  });

  it("uses the repository root when the bootstrap release has no matching tag", () => {
    const scenario = makeScenario({ matchingTag: "" });

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(scenario.calls.some(({ command, args }) => command === "git" && args[0] === "rev-list")).toBe(
      true,
    );

    scenario.cleanup();
  });

  it("refuses a missing public baseline once the source package version has moved", () => {
    const scenario = makeScenario({ matchingTag: "", releaseTree: makeReleaseTree("0.0.5") });

    expect(scenario.run).toThrowError(
      "Cannot determine the release baseline for effect-view-server@0.0.6; its public tag is missing.",
    );

    scenario.cleanup();
  });

  it("refuses to continue when npm has no existing package or returns an invalid version", () => {
    const missing = makeScenario({ publishedVersionResult: result({ status: 1 }) });
    const invalid = makeScenario({ publishedVersion: 123 });

    expect(missing.run).toThrowError(
      "effect-view-server must already exist on npm before continuous publishing.",
    );
    expect(invalid.run).toThrowError(
      "npm view returned an invalid version for effect-view-server.",
    );

    missing.cleanup();
    invalid.cleanup();
  });

  it("refuses to continue when the repository root cannot be determined", () => {
    const scenario = makeScenario({ matchingTag: "", rootCommitOutput: "\n" });

    expect(scenario.run).toThrowError(
      "Cannot determine the repository root commit for release versioning.",
    );

    scenario.cleanup();
  });

  it("propagates command adapter errors and rejects private references in the artifact", () => {
    const commandError = makeScenario({ commandError: new Error("spawn failure") });
    const unsafeArtifact = makeScenario({ unsafePublishedFile: true });

    expect(commandError.run).toThrowError("spawn failure");
    expect(unsafeArtifact.run).toThrowError(
      [
        "Refusing npm publish because the publish artifact contains private workspace artifacts.",
        "- dist/unsafe.js references @effect-view-server/",
      ].join("\n"),
    );

    commandError.cleanup();
    unsafeArtifact.cleanup();
  });

  it("repairs the tag when a retry finds the computed version already public", () => {
    const scenario = makeScenario({
      existingPendingTagTarget: "head-object",
      releaseVersionExists: true,
    });

    expect(scenario.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(scenario.calls.some(({ command, args }) => command === "npm" && args[0] === "publish")).toBe(
      false,
    );

    scenario.cleanup();
  });

  it("fails closed when npm already has a version without this commit's pending marker", () => {
    const scenario = makeScenario({ releaseVersionExists: true });

    expect(scenario.run).toThrowError(
      "Refusing to adopt effect-view-server@0.0.7 without a pending tag at the tested commit.",
    );

    scenario.cleanup();
  });

  it("requires the pending marker when npm reports a raced publish after an error", () => {
    const safe = makeScenario({
      existingPendingTagTarget: "head-object",
      publishResult: result({ status: 1, stderr: "already published\n" }),
      publishVersionExists: true,
    });
    const unsafe = makeScenario({
      publishResult: result({ status: 1, stderr: "already published\n" }),
      publishVersionExists: true,
    });

    expect(safe.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(unsafe.run).toThrowError(
      "Refusing to adopt effect-view-server@0.0.7 without a pending tag at the tested commit.",
    );

    safe.cleanup();
    unsafe.cleanup();
  });

  it("repairs a missing tag after npm succeeded before the previous run could tag it", () => {
    const scenario = makeScenario({
      matchingTag: "effect-view-server@0.0.7-pending",
      publishedVersion: "0.0.7",
      existingPendingTagTarget: "head-object",
    });

    expect(scenario.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(scenario.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["npm", "view", "effect-view-server", "version", "--json"],
      ["git", "fetch", "--tags", "origin"],
      ["git", "tag", "--list", "effect-view-server@*"],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@0.0.7-pending^{}",
      ],
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      ["git", "rev-parse", "--quiet", "--verify", "refs/tags/effect-view-server@0.0.7^{}"],
      [
        "git",
        "tag",
        "-a",
        "effect-view-server@0.0.7",
        "head-object",
        "-m",
        "effect-view-server@0.0.7",
      ],
      ["git", "push", "origin", "refs/tags/effect-view-server@0.0.7"],
    ]);

    scenario.cleanup();
  });

  it("keeps a matching public tag and rejects a tag pointing at another commit", () => {
    const matching = makeScenario({
      existingPendingTagTarget: "head-object",
      existingTagTarget: "head-object",
    });
    const mismatched = makeScenario({ existingTagTarget: "different-head" });

    expect(matching.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(matching.calls.some(({ command, args }) => command === "git" && args[0] === "push")).toBe(
      false,
    );
    expect(mismatched.run).toThrowError(
      "Refusing to move published tag effect-view-server@0.0.7 away from its existing commit.",
    );

    matching.cleanup();
    mismatched.cleanup();
  });

  it("moves a stale pending marker with a force-with-lease", () => {
    const scenario = makeScenario({ existingPendingTagTarget: "old-head" });

    expect(scenario.run()).toStrictEqual({
      _tag: "Published",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(
      scenario.calls.some(
        ({ command, args }) =>
          command === "git" &&
          args[0] === "push" &&
          args[1] ===
            "--force-with-lease=refs/tags/effect-view-server@0.0.7-pending:old-head",
      ),
    ).toBe(true);

    scenario.cleanup();
  });

  it("fails if HEAD disappears before the public tag can be created", () => {
    const scenario = makeScenario({ headTargetSequence: ["head-object", null] });

    expect(scenario.run).toThrowError(
      "Cannot create effect-view-server@0.0.7 because HEAD does not resolve to a git object.",
    );

    scenario.cleanup();
  });

  it("fails if a stale pending tag has no lease object", () => {
    const scenario = makeScenario({
      existingPendingTagObject: null,
      existingPendingTagTarget: "old-head",
    });

    expect(scenario.run).toThrowError(
      "Cannot update pending tag effect-view-server@0.0.7-pending because its git object is unavailable.",
    );

    scenario.cleanup();
  });

  it("preserves a git tag command failure status when it is null", () => {
    const scenario = makeScenario({ gitTagResult: result({ status: null }) });

    expect(scenario.run).toThrowError(
      expect.objectContaining({
        exitCode: 1,
        message: expect.stringContaining("git tag"),
        name: "ReleasePublishCommandError",
      }),
    );

    scenario.cleanup();
  });

  it("normalizes a null npm publish status to exit code one", () => {
    const scenario = makeScenario({ publishResult: result({ status: null }) });

    expect(scenario.run).toThrowError(
      expect.objectContaining({
        exitCode: 1,
        message: expect.stringContaining("npm publish"),
        name: "ReleasePublishCommandError",
      }),
    );

    scenario.cleanup();
  });

  it("refuses to create a release tag when HEAD is unavailable", () => {
    const scenario = makeScenario({ headTarget: null });

    expect(scenario.run).toThrowError(
      "Cannot reserve effect-view-server@0.0.7-pending because HEAD does not resolve to a git object.",
    );

    scenario.cleanup();
  });

  it("refuses to publish when an internal workspace package is public", () => {
    const scenario = makeScenario();
    writeJson(
      join(scenario.rootDirectory, "packages", "client", "package.json"),
      { name: "@effect-view-server/client", private: false },
    );

    expect(scenario.run).toThrowError(
      "Refusing to publish because @effect-view-server/client is not private.",
    );
    expect(scenario.calls.some(({ command, args }) => command === "npm" && args[0] === "publish")).toBe(
      false,
    );

    scenario.cleanup();
  });

  it("does not run npm for an untrusted branch or missing OIDC", () => {
    const untrusted = makeScenario({
      env: { ...trustedEnvironment, GITHUB_REF: "refs/heads/feature" },
    });
    const missingOidc = makeScenario({
      env: {
        ...trustedEnvironment,
        ACTIONS_ID_TOKEN_REQUEST_TOKEN: "",
        ACTIONS_ID_TOKEN_REQUEST_URL: "",
      },
    });

    expect(untrusted.run).toThrowError(
      "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    );
    expect(untrusted.calls).toStrictEqual([]);
    expect(missingOidc.run).toThrowError(
      [
        "Refusing npm publish because GitHub Actions OIDC is unavailable.",
        "- ACTIONS_ID_TOKEN_REQUEST_URL is required for npm trusted publishing.",
        "- ACTIONS_ID_TOKEN_REQUEST_TOKEN is required for npm trusted publishing.",
      ].join("\n"),
    );
    expect(missingOidc.calls).toStrictEqual([]);

    untrusted.cleanup();
    missingOidc.cleanup();
  });

  it("confirms a raced successful publish and preserves unknown npm failures", () => {
    const raced = makeScenario({
      existingPendingTagTarget: "head-object",
      publishResult: result({
        status: 1,
        stderr: "npm error cannot publish over previously published version 0.0.7\n",
      }),
      publishVersionExists: true,
    });
    const failed = makeScenario({
      publishResult: result({ status: 23, stderr: "npm error authentication failed\n" }),
    });

    expect(raced.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      releaseType: "patch",
      version: "0.0.7",
    });
    expect(failed.run).toThrowError(
      expect.objectContaining({
        exitCode: 23,
        message: expect.stringContaining("npm publish"),
        name: "ReleasePublishCommandError",
      }),
    );
    expect(failed.stderr).toStrictEqual(["npm error authentication failed\n"]);

    raced.cleanup();
    failed.cleanup();
  });
});
