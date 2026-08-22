import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ReleaseVersionCommandError,
  runReleaseVersion,
} from "./release-version-orchestration.mjs";

type CommandResult = {
  readonly error?: Error;
  readonly status: number | null;
};

type CommandCall = {
  readonly args: ReadonlyArray<string>;
  readonly command: string;
  readonly options: {
    readonly cwd: string;
    readonly stdio: string;
  };
};

const publicPackagePath = join("packages", "effect-view-server", "package.json");
const grpcPackagePath = join("packages", "grpc", "package.json");
const kafkaPackagePath = join("packages", "kafka", "package.json");
const kafkaPeerMatrixPath = join("packages", "kafka", "source-adapter-peer-matrix.json");
const workspacePath = "pnpm-workspace.yaml";

const initialGrpcPackage = {
  name: "@effect-view-server/grpc",
  version: "0.0.0",
  private: true,
  devDependencies: {
    effect: "4.0.0-rc.111",
    "effect-view-server": "0.0.6",
  },
  peerDependencies: {
    effect: "4.0.0-rc.111",
    "effect-view-server": "0.0.6",
  },
};

const initialKafkaPackage = {
  name: "@effect-view-server/kafka",
  version: "0.0.0",
  private: true,
  devDependencies: {
    effect: "4.0.0-rc.111",
    "effect-view-server": "0.0.6",
  },
  peerDependencies: {
    effect: "4.0.0-rc.111",
    "effect-view-server": "0.0.6",
  },
};

const initialKafkaPeerMatrix = [
  {
    effect: "4.0.0-rc.111",
    "effect-view-server": "0.0.6",
  },
  {
    channel: "next",
    effect: "4.0.0-rc.101",
    "effect-view-server": "0.0.6",
  },
];

const initialWorkspace = [
  "packages:",
  "  - packages/*",
  "overrides:",
  '  "@effect-view-server/grpc>effect-view-server": "workspace:0.0.6"',
  '  "@effect-view-server/kafka>effect-view-server": "workspace:0.0.6"',
  "",
].join("\n");

const writeJson = (path: string, value: unknown) => {
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
};

const makeReleaseVersionTree = () => {
  const rootDirectory = mkdtempSync(join(tmpdir(), "effect-view-server-release-version-"));
  mkdirSync(join(rootDirectory, "packages", "effect-view-server"), { recursive: true });
  mkdirSync(join(rootDirectory, "packages", "grpc"), { recursive: true });
  mkdirSync(join(rootDirectory, "packages", "kafka"), { recursive: true });
  writeJson(join(rootDirectory, publicPackagePath), {
    name: "effect-view-server",
    version: "0.0.6",
  });
  writeJson(join(rootDirectory, grpcPackagePath), initialGrpcPackage);
  writeJson(join(rootDirectory, kafkaPackagePath), initialKafkaPackage);
  writeJson(join(rootDirectory, kafkaPeerMatrixPath), initialKafkaPeerMatrix);
  writeFileSync(join(rootDirectory, workspacePath), initialWorkspace);

  return {
    cleanup: () => rmSync(rootDirectory, { force: true, recursive: true }),
    rootDirectory,
  };
};

const makeCommand = (
  calls: Array<CommandCall>,
  responses: ReadonlyArray<() => CommandResult>,
) => {
  let responseIndex = 0;

  return (
    command: string,
    args: ReadonlyArray<string>,
    options: CommandCall["options"],
  ): CommandResult => {
    calls.push({ args, command, options });
    const response = responses[responseIndex];
    responseIndex += 1;
    if (response === undefined) {
      throw new Error(`Unexpected command: ${command} ${args.join(" ")}`);
    }
    return response();
  };
};

describe("release version orchestration", () => {
  it("keeps adapters private while synchronizing the complete facade compatibility plan", () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    let installSnapshot: unknown = undefined;
    const command = makeCommand(calls, [
      () => {
        writeJson(join(scenario.rootDirectory, publicPackagePath), {
          name: "effect-view-server",
          version: "0.1.0",
        });
        return { status: 0 };
      },
      () => {
        installSnapshot = {
          grpcPackage: JSON.parse(
            readFileSync(join(scenario.rootDirectory, grpcPackagePath), "utf8"),
          ),
          kafkaPackage: JSON.parse(
            readFileSync(join(scenario.rootDirectory, kafkaPackagePath), "utf8"),
          ),
          kafkaPeerMatrix: JSON.parse(
            readFileSync(join(scenario.rootDirectory, kafkaPeerMatrixPath), "utf8"),
          ),
          workspace: readFileSync(join(scenario.rootDirectory, workspacePath), "utf8"),
        };
        return { status: 0 };
      },
    ]);

    expect(
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    ).toStrictEqual({
      version: "0.1.0",
    });
    expect(calls).toStrictEqual([
      {
        args: ["exec", "changeset", "version"],
        command: "vp",
        options: {
          cwd: scenario.rootDirectory,
          stdio: "inherit",
        },
      },
      {
        args: ["install"],
        command: "vp",
        options: {
          cwd: scenario.rootDirectory,
          stdio: "inherit",
        },
      },
    ]);
    expect(installSnapshot).toStrictEqual({
      grpcPackage: {
        name: "@effect-view-server/grpc",
        version: "0.0.0",
        private: true,
        devDependencies: {
          effect: "4.0.0-rc.111",
          "effect-view-server": "0.1.0",
        },
        peerDependencies: {
          effect: "4.0.0-rc.111",
          "effect-view-server": "0.1.0",
        },
      },
      kafkaPackage: {
        name: "@effect-view-server/kafka",
        version: "0.0.0",
        private: true,
        devDependencies: {
          effect: "4.0.0-rc.111",
          "effect-view-server": "0.1.0",
        },
        peerDependencies: {
          effect: "4.0.0-rc.111",
          "effect-view-server": "0.1.0",
        },
      },
      kafkaPeerMatrix: [
        {
          effect: "4.0.0-rc.111",
          "effect-view-server": "0.1.0",
        },
        {
          channel: "next",
          effect: "4.0.0-rc.101",
          "effect-view-server": "0.1.0",
        },
      ],
      workspace: [
        "packages:",
        "  - packages/*",
        "overrides:",
        '  "@effect-view-server/grpc>effect-view-server": "workspace:0.1.0"',
        '  "@effect-view-server/kafka>effect-view-server": "workspace:0.1.0"',
        "",
      ].join("\n"),
    });

    scenario.cleanup();
  });

  it("stops before synchronization and install when Changesets fails", async () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    const command = makeCommand(calls, [() => ({ status: 9 })]);
    const release = Promise.resolve().then(() =>
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    );

    await expect(release).rejects.toThrowError("vp exec changeset version failed.");
    await expect(release).rejects.toHaveProperty("exitCode", 9);
    expect(calls).toHaveLength(1);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, grpcPackagePath), "utf8")),
    ).toStrictEqual(initialGrpcPackage);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, kafkaPackagePath), "utf8")),
    ).toStrictEqual(initialKafkaPackage);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, kafkaPeerMatrixPath), "utf8")),
    ).toStrictEqual(initialKafkaPeerMatrix);
    expect(readFileSync(join(scenario.rootDirectory, workspacePath), "utf8")).toBe(
      initialWorkspace,
    );

    scenario.cleanup();
  });

  it("preserves the install exit code after writing the synchronized plan", async () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    const command = makeCommand(calls, [
      () => {
        writeJson(join(scenario.rootDirectory, publicPackagePath), {
          name: "effect-view-server",
          version: "0.1.0",
        });
        return { status: 0 };
      },
      () => ({ status: 17 }),
    ]);
    const release = Promise.resolve().then(() =>
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    );

    await expect(release).rejects.toBeInstanceOf(ReleaseVersionCommandError);
    await expect(release).rejects.toHaveProperty("exitCode", 17);
    expect(calls).toHaveLength(2);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, grpcPackagePath), "utf8")),
    ).toStrictEqual({
      ...initialGrpcPackage,
      devDependencies: {
        ...initialGrpcPackage.devDependencies,
        "effect-view-server": "0.1.0",
      },
      peerDependencies: {
        ...initialGrpcPackage.peerDependencies,
        "effect-view-server": "0.1.0",
      },
    });
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, kafkaPeerMatrixPath), "utf8")),
    ).toStrictEqual([
      {
        effect: "4.0.0-rc.111",
        "effect-view-server": "0.1.0",
      },
      {
        channel: "next",
        effect: "4.0.0-rc.101",
        "effect-view-server": "0.1.0",
      },
    ]);

    scenario.cleanup();
  });

  it("propagates process launch failures", () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    const command = makeCommand(calls, [
      () => ({
        error: new Error("vp could not launch"),
        status: null,
      }),
    ]);

    expect(() =>
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    ).toThrowError("vp could not launch");
    expect(calls).toHaveLength(1);

    scenario.cleanup();
  });

  it("uses a failure exit code when a command terminates without a status", async () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    const command = makeCommand(calls, [() => ({ status: null })]);
    const release = Promise.resolve().then(() =>
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    );

    await expect(release).rejects.toThrowError("vp exec changeset version failed.");
    await expect(release).rejects.toHaveProperty("exitCode", 1);
    expect(calls).toHaveLength(1);

    scenario.cleanup();
  });

  it.each([
    {
      message: "packages/effect-view-server/package.json must describe effect-view-server.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, publicPackagePath), {
          name: "wrong-package",
          version: "0.0.6",
        }),
    },
    {
      message: "packages/effect-view-server/package.json must contain a valid semantic version.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, publicPackagePath), {
          name: "effect-view-server",
          version: "next",
        }),
    },
    {
      message: "packages/grpc/package.json must describe @effect-view-server/grpc.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, grpcPackagePath), {
          name: "wrong-package",
          version: "0.0.0",
        }),
    },
    {
      message: "packages/kafka/package.json must describe @effect-view-server/kafka.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPackagePath), {
          name: "wrong-package",
          version: "0.0.0",
        }),
    },
    {
      message: "packages/grpc/package.json must remain private.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, grpcPackagePath), {
          ...initialGrpcPackage,
          private: false,
        }),
    },
    {
      message:
        "packages/grpc/package.json must contain an exact devDependencies.effect-view-server dependency.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, grpcPackagePath), {
          ...initialGrpcPackage,
          devDependencies: {},
        }),
    },
    {
      message:
        "packages/grpc/package.json must contain an exact peerDependencies.effect-view-server dependency.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, grpcPackagePath), {
          ...initialGrpcPackage,
          peerDependencies: {},
        }),
    },
    {
      message: "packages/kafka/package.json must remain private.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPackagePath), {
          ...initialKafkaPackage,
          private: false,
        }),
    },
    {
      message:
        "packages/kafka/package.json must contain an exact devDependencies.effect-view-server dependency.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPackagePath), {
          name: "@effect-view-server/kafka",
          version: "0.0.0",
          private: true,
          devDependencies: {},
          peerDependencies: {
            "effect-view-server": "0.0.6",
          },
        }),
    },
    {
      message:
        "packages/kafka/package.json must contain an exact peerDependencies.effect-view-server dependency.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPackagePath), {
          name: "@effect-view-server/kafka",
          version: "0.0.0",
          private: true,
          devDependencies: {
            "effect-view-server": "0.0.6",
          },
          peerDependencies: {},
        }),
    },
    {
      message:
        "packages/kafka/source-adapter-peer-matrix.json must contain at least one tested peer combination.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPeerMatrixPath), []),
    },
    {
      message:
        "packages/kafka/source-adapter-peer-matrix.json entries must contain exact effect and effect-view-server versions.",
      mutate: (rootDirectory: string) =>
        writeJson(join(rootDirectory, kafkaPeerMatrixPath), [
          {
            effect: "4.0.0-rc.111",
          },
        ]),
    },
    {
      message:
        "pnpm-workspace.yaml must contain exactly one @effect-view-server/grpc>effect-view-server override.",
      mutate: (rootDirectory: string) =>
        writeFileSync(join(rootDirectory, workspacePath), "packages:\n  - packages/*\n"),
    },
    {
      message:
        "pnpm-workspace.yaml must contain exactly one @effect-view-server/kafka>effect-view-server override.",
      mutate: (rootDirectory: string) =>
        writeFileSync(
          join(rootDirectory, workspacePath),
          [
            "overrides:",
            '  "@effect-view-server/grpc>effect-view-server": "workspace:0.0.6"',
            '  "@effect-view-server/kafka>effect-view-server": "workspace:0.0.6"',
            '  "@effect-view-server/kafka>effect-view-server": "workspace:0.0.6"',
            "",
          ].join("\n"),
        ),
    },
    {
      message:
        "pnpm-workspace.yaml must express the @effect-view-server/kafka>effect-view-server override as an exact quoted workspace version.",
      mutate: (rootDirectory: string) =>
        writeFileSync(
          join(rootDirectory, workspacePath),
          [
            "overrides:",
            '  "@effect-view-server/grpc>effect-view-server": "workspace:0.0.6"',
            '  "@effect-view-server/kafka>effect-view-server": workspace:*',
            "",
          ].join("\n"),
        ),
    },
    {
      message:
        "pnpm-workspace.yaml must express the @effect-view-server/grpc>effect-view-server override as an exact quoted workspace version.",
      mutate: (rootDirectory: string) =>
        writeFileSync(
          join(rootDirectory, workspacePath),
          [
            "overrides:",
            '  "@effect-view-server/grpc>effect-view-server": workspace:*',
            '  "@effect-view-server/kafka>effect-view-server": "workspace:0.0.6"',
            "",
          ].join("\n"),
        ),
    },
  ])("rejects malformed release metadata before running Changesets", ({ message, mutate }) => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    mutate(scenario.rootDirectory);

    expect(() =>
      runReleaseVersion({
        command: makeCommand(calls, []),
        rootDirectory: scenario.rootDirectory,
      }),
    ).toThrowError(message);
    expect(calls).toStrictEqual([]);

    scenario.cleanup();
  });

  it("rejects post-Changesets metadata corruption before writing compatibility files", () => {
    const scenario = makeReleaseVersionTree();
    const calls: Array<CommandCall> = [];
    const command = makeCommand(calls, [
      () => {
        writeJson(join(scenario.rootDirectory, kafkaPeerMatrixPath), {
          combinations: [],
        });
        return { status: 0 };
      },
    ]);

    expect(() =>
      runReleaseVersion({
        command,
        rootDirectory: scenario.rootDirectory,
      }),
    ).toThrowError(
      "packages/kafka/source-adapter-peer-matrix.json must contain at least one tested peer combination.",
    );
    expect(calls).toHaveLength(1);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, kafkaPackagePath), "utf8")),
    ).toStrictEqual(initialKafkaPackage);
    expect(
      JSON.parse(readFileSync(join(scenario.rootDirectory, kafkaPeerMatrixPath), "utf8")),
    ).toStrictEqual({
      combinations: [],
    });
    expect(readFileSync(join(scenario.rootDirectory, workspacePath), "utf8")).toBe(
      initialWorkspace,
    );

    scenario.cleanup();
  });
});
