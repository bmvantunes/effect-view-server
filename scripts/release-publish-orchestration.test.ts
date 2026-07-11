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
import {
  ReleasePublishCommandError,
  runReleasePublish,
} from "./release-publish-orchestration.mjs";

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
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "bmvantunes/effect-view-server",
};

const commandResult = ({
  error,
  status = 0,
  stderr = "",
  stdout = "",
}: {
  error?: Error;
  status?: number | null;
  stderr?: string;
  stdout?: string;
} = {}) => ({
  error,
  status,
  stderr,
  stdout,
});

const writeJson = (path: string, value: unknown) => {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
};

const makeReleaseTree = (version = "1.2.3") => {
  const rootDirectory = mkdtempSync(join(tmpdir(), "view-server-release-root-"));
  const temporaryDirectory = join(rootDirectory, "temporary");
  const publicPackageDirectory = join(rootDirectory, "packages", "effect-view-server");
  const internalPackageDirectory = join(rootDirectory, "packages", "client");

  mkdirSync(join(rootDirectory, "apps", "missing-package"), { recursive: true });
  mkdirSync(join(publicPackageDirectory, "dist", "nested"), { recursive: true });
  mkdirSync(internalPackageDirectory, { recursive: true });
  mkdirSync(temporaryDirectory);
  writeFileSync(join(rootDirectory, "apps", "README.md"), "not a package\n");
  writeJson(join(publicPackageDirectory, "package.json"), {
    name: "effect-view-server",
    version,
    type: "module",
    exports: {
      ".": {
        import: "./dist/index.js",
        types: "./dist/index.d.ts",
      },
    },
    publishConfig: {
      provenance: true,
    },
    dependencies: {
      "@effect-view-server/client": "workspace:*",
      effect: "4.0.0-beta.91",
    },
    scripts: {
      build: "vp pack",
    },
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

  return {
    publicPackageDirectory,
    rootDirectory,
    temporaryDirectory,
  };
};

const makeScenario = (
  responses: ReadonlyArray<CommandResult>,
  {
    env = trustedEnvironment,
    releaseTree = makeReleaseTree(),
  }: {
    env?: NodeJS.ProcessEnv;
    releaseTree?: ReturnType<typeof makeReleaseTree>;
  } = {},
) => {
  const calls: Array<CommandCall> = [];
  const stdout: Array<string> = [];
  const stderr: Array<string> = [];
  const command = (nextCommand: string, args: ReadonlyArray<string>, options: CommandOptions) => {
    const response = responses[calls.length];
    calls.push({ args, command: nextCommand, options });
    if (response === undefined) {
      throw new Error(`Unexpected command: ${nextCommand} ${args.join(" ")}`);
    }
    return response;
  };

  return {
    ...releaseTree,
    calls,
    cleanup: () => {
      rmSync(releaseTree.rootDirectory, { force: true, recursive: true });
    },
    run: () =>
      runReleasePublish({
        command,
        env,
        rootDirectory: releaseTree.rootDirectory,
        stderr: (message: string) => {
          stderr.push(message);
        },
        stdout: (message: string) => {
          stdout.push(message);
        },
        temporaryDirectory: releaseTree.temporaryDirectory,
      }),
    expectedCommandCount: responses.length,
    stderr,
    stdout,
  };
};

const expectCleanTemporaryDirectory = (scenario: ReturnType<typeof makeScenario>) => {
  expect(scenario.calls).toHaveLength(scenario.expectedCommandCount);
  expect(readdirSync(scenario.temporaryDirectory)).toStrictEqual([]);
};

describe("release publish orchestration", () => {
  it("stages a sanitized temporary package and creates its pending marker tag", () => {
    const { rootDirectory, temporaryDirectory } = makeReleaseTree();
    const calls: Array<CommandCall> = [];
    const stdout: Array<string> = [];
    const stderr: Array<string> = [];
    let stagedPackage: unknown = undefined;
    const responses = [
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stdout: "staged 1.2.3\n" }),
      commandResult({ stdout: "head-object\n" }),
      commandResult({ status: 1 }),
      commandResult({ status: 1 }),
      commandResult(),
      commandResult(),
    ];
    const command = (nextCommand: string, args: ReadonlyArray<string>, options: CommandOptions) => {
      calls.push({ args, command: nextCommand, options });
      if (nextCommand === "npm" && args[0] === "stage") {
        const stageDirectory = args[2];
        if (stageDirectory !== undefined) {
          stagedPackage = {
            declaration: readFileSync(join(stageDirectory, "dist", "index.d.ts"), "utf8"),
            files: readdirSync(stageDirectory).sort(),
            manifest: JSON.parse(readFileSync(join(stageDirectory, "package.json"), "utf8")),
            nestedFile: readFileSync(join(stageDirectory, "dist", "nested", "data.txt"), "utf8"),
            readme: readFileSync(join(stageDirectory, "README.md"), "utf8"),
            runtime: readFileSync(join(stageDirectory, "dist", "index.js"), "utf8"),
            sourceMapExists: existsSync(join(stageDirectory, "dist", "index.js.map")),
          };
        }
      }
      const response = responses[calls.length - 1];
      if (response === undefined) {
        throw new Error(`Unexpected command: ${nextCommand} ${args.join(" ")}`);
      }
      return response;
    };

    const outcome = runReleasePublish({
      command,
      env: trustedEnvironment,
      rootDirectory,
      stderr: (message: string) => {
        stderr.push(message);
      },
      stdout: (message: string) => {
        stdout.push(message);
      },
      temporaryDirectory,
    });

    expect(outcome).toStrictEqual({
      _tag: "Staged",
      version: "1.2.3",
    });
    expect(stagedPackage).toStrictEqual({
      declaration: "export declare const ready: true;\n",
      files: ["README.md", "dist", "package.json"],
      manifest: {
        name: "effect-view-server",
        version: "1.2.3",
        type: "module",
        exports: {
          ".": {
            import: "./dist/index.js",
            types: "./dist/index.d.ts",
          },
        },
        files: ["dist", "README.md"],
        publishConfig: {
          access: "public",
          provenance: true,
        },
        dependencies: {
          effect: "4.0.0-beta.91",
        },
      },
      nestedFile: "ready\n",
      readme: "# Public package\n",
      runtime: "export const ready = true;\n",
      sourceMapExists: false,
    });
    expect(calls.map(({ command: calledCommand, args }) => [calledCommand, ...args])).toStrictEqual([
      ["npm", "view", "effect-view-server@1.2.3", "version", "--json"],
      ["npm", "view", "effect-view-server", "name", "--json"],
      ["npm", "stage", "publish", calls[2]?.args[2], "--provenance", "--access", "public"],
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@1.2.3-staged^{}",
      ],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@1.2.3-staged",
      ],
      [
        "git",
        "tag",
        "-a",
        "effect-view-server@1.2.3-staged",
        "head-object",
        "-m",
        "effect-view-server@1.2.3-staged",
      ],
      ["git", "push", "origin", "refs/tags/effect-view-server@1.2.3-staged"],
    ]);
    expect(stdout).toStrictEqual(["staged 1.2.3\n"]);
    expect(stderr).toStrictEqual([""]);
    expect(calls.map(({ options }) => options)).toStrictEqual([
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "ignore"],
      },
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "ignore"],
      },
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "pipe"],
      },
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "ignore"],
      },
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "ignore"],
      },
      {
        cwd: rootDirectory,
        encoding: "utf8",
        env: trustedEnvironment,
        stdio: ["ignore", "pipe", "ignore"],
      },
      {
        cwd: rootDirectory,
        env: trustedEnvironment,
        stdio: "inherit",
      },
      {
        cwd: rootDirectory,
        env: trustedEnvironment,
        stdio: "inherit",
      },
    ]);
    expect(readdirSync(temporaryDirectory)).toStrictEqual([]);
    rmSync(rootDirectory, { force: true, recursive: true });
  });

  it("returns before staging for placeholder versions and untrusted release contexts", () => {
    const skipped = makeScenario([], {
      releaseTree: makeReleaseTree("0.0.0"),
    });
    const refused = makeScenario([], {
      env: {
        ...trustedEnvironment,
        GITHUB_REF: "refs/heads/not-main",
      },
    });

    expect(skipped.run()).toStrictEqual({
      _tag: "Skipped",
      version: "0.0.0",
    });
    expect(skipped.stdout).toStrictEqual([
      "Skipping npm publish for effect-view-server@0.0.0.\n",
    ]);
    expect(skipped.stderr).toStrictEqual([]);
    expect(skipped.calls).toStrictEqual([]);
    expectCleanTemporaryDirectory(skipped);
    expect(refused.run).toThrowError(
      "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    );
    expect(refused.stdout).toStrictEqual([]);
    expect(refused.stderr).toStrictEqual([]);
    expect(refused.calls).toStrictEqual([]);
    expectCleanTemporaryDirectory(refused);

    skipped.cleanup();
    refused.cleanup();
  });

  it("returns without npm staging when the requested version is already public", () => {
    const scenario = makeScenario([
      commandResult({ stdout: '"1.2.3"\n' }),
    ]);

    expect(scenario.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      version: "1.2.3",
    });
    expect(scenario.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["npm", "view", "effect-view-server@1.2.3", "version", "--json"],
    ]);
    expect(scenario.stdout).toStrictEqual([
      "effect-view-server@1.2.3 is already published.\n",
    ]);
    expect(scenario.stderr).toStrictEqual([]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses staging when npm has no package to stage or GitHub OIDC is unavailable", () => {
    const missingPackage = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ status: 1 }),
    ]);
    const missingOidc = makeScenario(
      [
        commandResult({ status: 1 }),
        commandResult({ stdout: '"effect-view-server"\n' }),
      ],
      {
        env: {
          ...trustedEnvironment,
          ACTIONS_ID_TOKEN_REQUEST_TOKEN: "",
          ACTIONS_ID_TOKEN_REQUEST_URL: "",
        },
      },
    );

    expect(missingPackage.run).toThrowError(
      "effect-view-server must exist on npm before staged publishing can be used.",
    );
    expect(missingOidc.run).toThrowError(
      [
        "Refusing npm stage publish because GitHub Actions OIDC is unavailable.",
        "- ACTIONS_ID_TOKEN_REQUEST_URL is required for npm trusted publishing.",
        "- ACTIONS_ID_TOKEN_REQUEST_TOKEN is required for npm trusted publishing.",
      ].join("\n"),
    );
    expectCleanTemporaryDirectory(missingPackage);
    expectCleanTemporaryDirectory(missingOidc);

    missingPackage.cleanup();
    missingOidc.cleanup();
  });

  it("confirms npm's stage-time already-published response before completing", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({
        status: 1,
        stderr: "npm error cannot publish over previously published version 1.2.3\n",
      }),
      commandResult({ stdout: '"1.2.3"\n' }),
    ]);

    expect(scenario.run()).toStrictEqual({
      _tag: "AlreadyPublished",
      version: "1.2.3",
    });
    expect(scenario.stdout).toStrictEqual([
      "",
      "effect-view-server@1.2.3 is already published.\n",
    ]);
    expect(scenario.stderr).toStrictEqual([
      "npm error cannot publish over previously published version 1.2.3\n",
    ]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses an unconfirmed stage-time already-published response", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({
        status: 1,
        stderr: "npm error cannot publish over previously published version 1.2.3\n",
      }),
      commandResult({ status: 1 }),
    ]);

    expect(scenario.run).toThrowError(
      "npm reported effect-view-server@1.2.3 as already published, but npm view does not confirm it.",
    );
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("keeps an existing marker when npm reports that the version is already staged", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stdout: "version 1.2.3 is already staged\n", status: 1 }),
      commandResult({ stdout: "staged-head\n" }),
    ]);

    expect(scenario.run()).toStrictEqual({
      _tag: "AlreadyStaged",
      version: "1.2.3",
    });
    expect(scenario.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["npm", "view", "effect-view-server@1.2.3", "version", "--json"],
      ["npm", "view", "effect-view-server", "name", "--json"],
      [
        "npm",
        "stage",
        "publish",
        scenario.calls[2]?.args[2],
        "--provenance",
        "--access",
        "public",
      ],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@1.2.3-staged^{}",
      ],
    ]);
    expect(scenario.stdout).toStrictEqual([
      "version 1.2.3 is already staged\n",
      "effect-view-server@1.2.3 is already staged; keeping effect-view-server@1.2.3-staged.\n",
    ]);
    expect(scenario.stderr).toStrictEqual([""]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses to invent a missing marker for an already-staged npm version", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stdout: "version 1.2.3 is already staged\n", status: 1 }),
      commandResult({ status: 1 }),
    ]);

    expect(scenario.run).toThrowError(
      "npm reported effect-view-server@1.2.3 as already staged, but effect-view-server@1.2.3-staged is missing.",
    );
    expect(scenario.calls).toHaveLength(4);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses ambiguous duplicate versions and preserves the npm failure code for unknown failures", () => {
    const duplicate = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stderr: "npm error version 1.2.3 already exists\n", status: 1 }),
    ]);
    const unknown = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stderr: "npm error authentication failed\n", status: 23 }),
    ]);

    expect(duplicate.run).toThrowError(
      "npm reported a duplicate effect-view-server@1.2.3, but npm view does not report it as published.",
    );
    expect(unknown.run).toThrowError(
      expect.objectContaining({
        exitCode: 23,
        message: expect.stringContaining("npm stage publish"),
        name: "ReleasePublishCommandError",
      }),
    );
    expectCleanTemporaryDirectory(duplicate);
    expectCleanTemporaryDirectory(unknown);

    duplicate.cleanup();
    unknown.cleanup();
  });

  it("restages a rejected version and moves its stale marker with a force-with-lease push", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ stdout: "restaged 1.2.3\n" }),
      commandResult({ stdout: "new-head\n" }),
      commandResult({ stdout: "old-head\n" }),
      commandResult({ stdout: "old-tag-object\n" }),
      commandResult(),
      commandResult(),
    ]);

    expect(scenario.run()).toStrictEqual({
      _tag: "Staged",
      version: "1.2.3",
    });
    expect(scenario.calls.slice(-2).map(({ command, args }) => [command, ...args])).toStrictEqual([
      [
        "git",
        "tag",
        "-f",
        "-a",
        "effect-view-server@1.2.3-staged",
        "new-head",
        "-m",
        "effect-view-server@1.2.3-staged",
      ],
      [
        "git",
        "push",
        "--force-with-lease=refs/tags/effect-view-server@1.2.3-staged:old-tag-object",
        "origin",
        "refs/tags/effect-view-server@1.2.3-staged",
      ],
    ]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("keeps a matching marker after a successful restage", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult(),
      commandResult({ stdout: "head-object\n" }),
      commandResult({ stdout: "head-object\n" }),
      commandResult({ stdout: "tag-object\n" }),
    ]);

    expect(scenario.run()).toStrictEqual({
      _tag: "Staged",
      version: "1.2.3",
    });
    expect(
      scenario.calls.map(({ command, args }) => [command, ...args]).slice(3),
    ).toStrictEqual([
      ["git", "rev-parse", "--quiet", "--verify", "HEAD^{}"],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@1.2.3-staged^{}",
      ],
      [
        "git",
        "rev-parse",
        "--quiet",
        "--verify",
        "refs/tags/effect-view-server@1.2.3-staged",
      ],
    ]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses marker creation when workflow HEAD cannot be resolved", () => {
    const scenario = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult(),
      commandResult({ status: 1 }),
    ]);

    expect(scenario.run).toThrowError(
      "Cannot create effect-view-server@1.2.3-staged because HEAD does not resolve to a git object.",
    );
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("preserves marker creation and push command failures after cleaning the staged package", () => {
    const beforeTag = [
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult(),
      commandResult({ stdout: "head-object\n" }),
      commandResult({ status: 1 }),
      commandResult({ status: 1 }),
    ];
    const createFailure = makeScenario([
      ...beforeTag,
      commandResult({ status: 12 }),
    ]);
    const pushFailure = makeScenario([
      ...beforeTag,
      commandResult(),
      commandResult({ status: 13 }),
    ]);
    const missingStatus = makeScenario([
      ...beforeTag,
      commandResult({ status: null }),
    ]);

    expect(createFailure.run).toThrowError(
      expect.objectContaining({
        exitCode: 12,
        message: expect.stringContaining("git tag -a"),
      }),
    );
    expect(pushFailure.run).toThrowError(
      expect.objectContaining({
        exitCode: 13,
        message: expect.stringContaining("git push origin"),
      }),
    );
    expect(missingStatus.run).toThrowError(
      expect.objectContaining({
        exitCode: 1,
        message: expect.stringContaining("git tag -a"),
      }),
    );
    expectCleanTemporaryDirectory(createFailure);
    expectCleanTemporaryDirectory(pushFailure);
    expectCleanTemporaryDirectory(missingStatus);

    createFailure.cleanup();
    pushFailure.cleanup();
    missingStatus.cleanup();
  });

  it("propagates command adapter failures after cleaning the staged package", () => {
    const scenario = makeScenario([
      commandResult({
        error: new Error("spawn failed"),
        status: null,
      }),
    ]);

    expect(scenario.run).toThrowError("spawn failed");
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("refuses staged private workspace imports and cleans without invoking commands", () => {
    const releaseTree = makeReleaseTree();
    writeFileSync(
      join(releaseTree.publicPackageDirectory, "dist", "leak.js"),
      'import "@effect-view-server/client";\n',
    );
    const scenario = makeScenario([], { releaseTree });

    expect(scenario.run).toThrowError(
      [
        "Refusing npm stage publish because the staged package contains private workspace artifacts.",
        "- dist/leak.js references @effect-view-server/",
      ].join("\n"),
    );
    expect(scenario.calls).toStrictEqual([]);
    expectCleanTemporaryDirectory(scenario);

    scenario.cleanup();
  });

  it("treats mismatched npm view values as absent and rejects null stage statuses with exit code one", () => {
    const mismatchedVersion = makeScenario([
      commandResult({ stdout: '"9.9.9"\n' }),
      commandResult({ status: 1 }),
    ]);
    const mismatchedPackage = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"another-package"\n' }),
    ]);
    const nullStageStatus = makeScenario([
      commandResult({ status: 1 }),
      commandResult({ stdout: '"effect-view-server"\n' }),
      commandResult({ status: null, stderr: "npm failed without a status\n" }),
    ]);

    expect(mismatchedVersion.run).toThrowError(
      "effect-view-server must exist on npm before staged publishing can be used.",
    );
    expect(mismatchedPackage.run).toThrowError(
      "effect-view-server must exist on npm before staged publishing can be used.",
    );
    expect(nullStageStatus.run).toThrowError(
      expect.objectContaining({
        exitCode: 1,
        message: expect.stringContaining("npm stage publish"),
      }),
    );
    expectCleanTemporaryDirectory(mismatchedVersion);
    expectCleanTemporaryDirectory(mismatchedPackage);
    expectCleanTemporaryDirectory(nullStageStatus);

    mismatchedVersion.cleanup();
    mismatchedPackage.cleanup();
    nullStageStatus.cleanup();
  });
});
