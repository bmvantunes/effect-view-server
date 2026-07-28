import { afterEach, describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createKafkaAdapterPortLock,
  createKafkaAdapterTestRunner,
  kafkaAdapterTestPlan,
} from "./test-kafka-adapter-runner.mjs";

const temporaryDirectories = new Set<string>();

afterEach(async () => {
  await Promise.all(
    Array.from(temporaryDirectories, (directory) => rm(directory, { force: true, recursive: true })),
  );
  temporaryDirectories.clear();
});

type SpawnCall = {
  readonly args: ReadonlyArray<string>;
  readonly command: string;
  readonly options: {
    readonly cwd: string;
    readonly env: NodeJS.ProcessEnv;
    readonly stdio: string;
  };
};

class FakeChildProcess extends EventEmitter {
  killed = false;
  readonly signals: Array<string> = [];

  kill(signal: string): boolean {
    this.killed = true;
    this.signals.push(signal);
    this.emit("close", null, signal);
    return true;
  }
}

const makeSpawn = () => {
  const calls: Array<SpawnCall> = [];
  const children: Array<FakeChildProcess> = [];
  const spawnProcess = (
    command: string,
    args: ReadonlyArray<string>,
    options: SpawnCall["options"],
  ) => {
    const child = new FakeChildProcess();
    calls.push({ args, command, options });
    children.push(child);
    return child;
  };
  return { calls, children, spawnProcess };
};

const makeImmediateLock = () => ({
  acquire: (shouldContinue: () => boolean = () => true) => Promise.resolve(shouldContinue()),
  lockDirectory: "/tmp/fake-kafka-adapter-ports.lock",
  release: () => Promise.resolve(),
});

const makeRunner = (
  fake: ReturnType<typeof makeSpawn>,
  testArguments: ReadonlyArray<string> = [],
  spawnProcess: typeof fake.spawnProcess = fake.spawnProcess,
  kafkaPortLock: ReturnType<typeof createKafkaAdapterPortLock> = makeImmediateLock(),
  processId = 42,
) =>
  createKafkaAdapterTestRunner({
    env: {
      VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "broker:19092",
      VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "london:19094",
    },
    packageDirectory: "/workspace/packages/kafka",
    processId,
    rootDirectory: "/workspace",
    kafkaPortLock,
    spawnProcess,
    stdio: "ignore",
    testArguments,
  });

const settle = async (
  children: ReadonlyArray<FakeChildProcess>,
  index: number,
  code: number | null,
  signal: string | null = null,
) => {
  await expect.poll(() => children.length).toBe(index + 1);
  children[index].emit("close", code, signal);
};

describe("Kafka adapter test runner", () => {
  it("plans integration, filtering, separators, and coverage flags", () => {
    expect({
      all: kafkaAdapterTestPlan([]),
      separatorOnly: kafkaAdapterTestPlan(["--"]),
      focused: kafkaAdapterTestPlan(["src/node.test.ts"]),
      integration: kafkaAdapterTestPlan(["src/kafka.integration.test.ts", "--coverage=false"]),
      noCoverage: kafkaAdapterTestPlan(["--no-coverage"]),
    }).toStrictEqual({
      all: {
        shouldCollectCoverage: true,
        shouldStartKafka: true,
        vitestArguments: [],
      },
      separatorOnly: {
        shouldCollectCoverage: true,
        shouldStartKafka: true,
        vitestArguments: [],
      },
      focused: {
        shouldCollectCoverage: false,
        shouldStartKafka: false,
        vitestArguments: ["src/node.test.ts"],
      },
      integration: {
        shouldCollectCoverage: false,
        shouldStartKafka: true,
        vitestArguments: ["src/kafka.integration.test.ts", "--coverage=false"],
      },
      noCoverage: {
        shouldCollectCoverage: false,
        shouldStartKafka: true,
        vitestArguments: ["--no-coverage"],
      },
    });
  });

  it("runs isolated Kafka services, the complete suite, and cleanup", async () => {
    const fake = makeSpawn();
    const runner = makeRunner(fake);
    const result = runner.runMain();

    await settle(fake.children, 0, 0);
    fake.children[0].emit("error", new Error("stale Docker error"));
    await settle(fake.children, 1, 0);
    await settle(fake.children, 2, 0);

    await expect(result).resolves.toBe(0);
    expect({
      calls: fake.calls,
      cleanupAgain: await runner.cleanup(),
      composeProjectName: runner.composeProjectName,
    }).toStrictEqual({
      calls: [
        {
          command: "docker",
          args: [
            "compose",
            "-f",
            "compose.yaml",
            "up",
            "-d",
            "--wait",
            "kafka",
            "kafka-london",
          ],
          options: {
            cwd: "/workspace",
            env: {
              COMPOSE_PROJECT_NAME: "view-server-kafka-adapter-42",
              VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "broker:19092",
              VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "london:19094",
            },
            stdio: "ignore",
          },
        },
        {
          command: "vp",
          args: ["test", "run", "--coverage", "--typecheck"],
          options: {
            cwd: "/workspace/packages/kafka",
            env: {
              COMPOSE_PROJECT_NAME: "view-server-kafka-adapter-42",
              VIEW_SERVER_KAFKA_INTEGRATION: "1",
              VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "broker:19092",
              VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "london:19094",
            },
            stdio: "ignore",
          },
        },
        {
          command: "docker",
          args: ["compose", "-f", "compose.yaml", "down"],
          options: {
            cwd: "/workspace",
            env: {
              COMPOSE_PROJECT_NAME: "view-server-kafka-adapter-42",
              VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "broker:19092",
              VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "london:19094",
            },
            stdio: "ignore",
          },
        },
      ],
      cleanupAgain: 0,
      composeProjectName: "view-server-kafka-adapter-42",
    });
  });

  it("runs focused tests without Docker and preserves explicit arguments", async () => {
    const fake = makeSpawn();
    const runner = makeRunner(fake, ["--", "src/node.test.ts", "--coverage=true"]);
    const result = runner.runMain();

    await settle(fake.children, 0, 0);

    await expect(result).resolves.toBe(0);
    expect(fake.calls).toStrictEqual([
      {
        command: "vp",
        args: ["test", "run", "--typecheck", "src/node.test.ts", "--coverage=true"],
        options: {
          cwd: "/workspace/packages/kafka",
          env: {
            COMPOSE_PROJECT_NAME: "view-server-kafka-adapter-42",
            VIEW_SERVER_KAFKA_INTEGRATION: "0",
            VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "broker:19092",
            VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "london:19094",
          },
          stdio: "ignore",
        },
      },
    ]);
    await expect(runner.cleanup()).resolves.toBe(0);
  });

  it("preserves startup and test failures while cleaning exactly once", async () => {
    const startupFake = makeSpawn();
    const startupRunner = makeRunner(startupFake);
    const startupResult = startupRunner.runMain();
    await settle(startupFake.children, 0, 7);
    await settle(startupFake.children, 1, 0);
    await expect(startupResult).resolves.toBe(7);
    expect(startupFake.calls.map(({ command }) => command)).toStrictEqual(["docker", "docker"]);

    const testFake = makeSpawn();
    const testRunner = makeRunner(testFake);
    const testResult = testRunner.runMain();
    await settle(testFake.children, 0, 0);
    await settle(testFake.children, 1, 8);
    await settle(testFake.children, 2, 9);
    await expect(testResult).resolves.toBe(8);
  });

  it("uses cleanup failures after successful tests", async () => {
    const cleanupFake = makeSpawn();
    const cleanupRunner = makeRunner(cleanupFake);
    const cleanupResult = cleanupRunner.runMain();
    await settle(cleanupFake.children, 0, 0);
    await settle(cleanupFake.children, 1, 0);
    await settle(cleanupFake.children, 2, 6);
    await expect(cleanupResult).resolves.toBe(6);
  });

  it.each([
    { expected: 130, signal: "SIGINT" },
    { expected: 143, signal: "SIGTERM" },
    { expected: 129, signal: "SIGHUP" },
    { expected: 137, signal: "SIGKILL" },
    { expected: 1, signal: "SIGUSR1" },
  ] as const)("maps a child $signal to exit code $expected", async ({ expected, signal }) => {
      const fake = makeSpawn();
      const runner = makeRunner(fake, ["src/node.test.ts"]);
      const result = runner.runMain();
      await settle(fake.children, 0, null, signal);
      await expect(result).resolves.toBe(expected);
  });

  it("cleans once on handled signals and preserves cleanup failure", async () => {
    const interruptFake = makeSpawn();
    const interruptRunner = makeRunner(interruptFake);
    const running = interruptRunner.runMain();
    await Promise.resolve();
    const interrupted = interruptRunner.handleSignal("SIGINT");
    await settle(interruptFake.children, 1, 0);
    await expect(interrupted).resolves.toBe(130);
    await expect(running).resolves.toBe(130);
    expect(interruptFake.children[0]?.signals).toStrictEqual(["SIGTERM"]);
    expect(interruptFake.calls).toHaveLength(2);

    const cleanupFake = makeSpawn();
    const cleanupRunner = makeRunner(cleanupFake);
    const cleanupRunning = cleanupRunner.runMain();
    await settle(cleanupFake.children, 0, 0);
    await settle(cleanupFake.children, 1, 0);
    await Promise.resolve();
    const cleanupSignal = cleanupRunner.handleSignal("SIGTERM");
    await settle(cleanupFake.children, 2, 5);
    await expect(cleanupSignal).resolves.toBe(5);
    await expect(cleanupRunning).resolves.toBe(5);
    expect(cleanupFake.children[2]?.signals).toStrictEqual([]);
  });

  it("cleans on a handled SIGHUP and returns its conventional exit code", async () => {
    const fake = makeSpawn();
    const runner = makeRunner(fake);
    const running = runner.runMain();
    await Promise.resolve();
    const interrupted = runner.handleSignal("SIGHUP");
    await settle(fake.children, 1, 0);
    await expect(interrupted).resolves.toBe(129);
    await expect(running).resolves.toBe(129);
    expect(fake.children[0]?.signals).toStrictEqual(["SIGTERM"]);
  });

  it("maps spawn failures to command failures and still attempts cleanup", async () => {
    const fake = makeSpawn();
    let attempts = 0;
    const runner = makeRunner(fake, [], (command, args, options) => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error("spawn failed");
      }
      return fake.spawnProcess(command, args, options);
    });
    const result = runner.runMain();
    await settle(fake.children, 0, 0);
    await expect(result).resolves.toBe(1);
    expect(fake.calls.map(({ command }) => command)).toStrictEqual(["docker"]);
  });

  it("maps child errors without letting a stale close replace the active state", async () => {
    const fake = makeSpawn();
    const runner = makeRunner(fake, ["src/node.test.ts"]);
    const result = runner.runMain();
    await Promise.resolve();
    expect(fake.children).toHaveLength(1);
    fake.children[0].emit("error", new Error("test process failed"));
    fake.children[0].emit("close", 0);
    await expect(result).resolves.toBe(1);
  });

  it("handles SIGTERM during a focused test and supplies default broker endpoints", async () => {
    const fake = makeSpawn();
    const runner = createKafkaAdapterTestRunner({
      env: {},
      packageDirectory: "/workspace/packages/kafka",
      processId: 42,
      rootDirectory: "/workspace",
      spawnProcess: fake.spawnProcess,
      stdio: "ignore",
      testArguments: ["src/node.test.ts"],
    });
    const running = runner.runMain();
    await Promise.resolve();
    const interrupted = runner.handleSignal("SIGTERM");

    await expect(interrupted).resolves.toBe(143);
    await expect(running).resolves.toBe(143);
    expect(fake.calls).toStrictEqual([
      {
        command: "vp",
        args: ["test", "run", "--typecheck", "src/node.test.ts"],
        options: {
          cwd: "/workspace/packages/kafka",
          env: {
            COMPOSE_PROJECT_NAME: "view-server-kafka-adapter-42",
            VIEW_SERVER_KAFKA_INTEGRATION: "0",
            VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "localhost:9092",
            VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: "localhost:9094",
          },
          stdio: "ignore",
        },
      },
    ]);
  });

  it("serializes concurrent fixed-port Kafka runner lifecycles across processes", async () => {
    const parentDirectory = await mkdtemp(
      join(tmpdir(), "effect-view-server-kafka-adapter-lock-test-"),
    );
    temporaryDirectories.add(parentDirectory);
    const lockDirectory = join(parentDirectory, "ports.lock");
    let allowRetry = () => {};
    let reportRetryStarted = () => {};
    const retryAllowed = new Promise<void>((resolve) => {
      allowRetry = resolve;
    });
    const retryStarted = new Promise<void>((resolve) => {
      reportRetryStarted = resolve;
    });
    const firstLock = createKafkaAdapterPortLock({ lockDirectory });
    const secondLock = createKafkaAdapterPortLock({
      lockDirectory,
      staleAfterMilliseconds: 0,
      waitForRetry: () => {
        reportRetryStarted();
        return retryAllowed;
      },
    });

    const firstFake = makeSpawn();
    const secondFake = makeSpawn();
    const firstRunner = makeRunner(firstFake, [], firstFake.spawnProcess, firstLock, 101);
    const secondRunner = makeRunner(secondFake, [], secondFake.spawnProcess, secondLock, 202);
    const firstRun = firstRunner.runMain();
    await settle(firstFake.children, 0, 0);

    const secondRun = secondRunner.runMain();
    await retryStarted;
    expect(secondFake.calls).toStrictEqual([]);

    await settle(firstFake.children, 1, 0);
    await settle(firstFake.children, 2, 0);
    await expect(firstRun).resolves.toBe(0);

    allowRetry();
    await settle(secondFake.children, 0, 0);
    await settle(secondFake.children, 1, 0);
    await settle(secondFake.children, 2, 0);
    await expect(secondRun).resolves.toBe(0);
    await secondLock.release();
    expect({
      firstProjects: firstFake.calls.map((call) => call.options.env.COMPOSE_PROJECT_NAME),
      secondProjects: secondFake.calls.map((call) => call.options.env.COMPOSE_PROJECT_NAME),
    }).toStrictEqual({
      firstProjects: [
        "view-server-kafka-adapter-101",
        "view-server-kafka-adapter-101",
        "view-server-kafka-adapter-101",
      ],
      secondProjects: [
        "view-server-kafka-adapter-202",
        "view-server-kafka-adapter-202",
        "view-server-kafka-adapter-202",
      ],
    });
  });

  it("cancels lock acquisition and preserves unexpected filesystem failures", async () => {
    const cancelled = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
      waitForRetry: () => Promise.resolve(),
    });
    await expect(cancelled.acquire(() => false)).resolves.toBe(false);
    await cancelled.release();

    const failure = new Error("lock unavailable");
    const unavailable = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.reject(failure),
    });
    await expect(unavailable.acquire()).rejects.toBe(failure);
  });

  it("retries the default fixed-port lock wait before acquiring", async () => {
    let attempts = 0;
    const lock = createKafkaAdapterPortLock({
      makeDirectory: () => {
        attempts += 1;
        return attempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "retry-owner" })),
      removeDirectory: () => Promise.resolve(),
      token: "retry-owner",
      writeOwnerFile: () => Promise.resolve(),
    });

    await expect(lock.acquire()).resolves.toBe(true);
    await lock.release();
    expect(attempts).toBe(2);
  });

  it("records ownership and atomically recovers an abandoned lock", async () => {
    const removed: Array<string> = [];
    const moved: Array<readonly [string, string]> = [];
    const written: Array<readonly [string, string]> = [];
    let makeAttempts = 0;
    let ownerReads = 0;
    const lock = createKafkaAdapterPortLock({
      currentTime: () => 5_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 4_999 }),
      isOwnerAlive: () => false,
      lockDirectory: "/tmp/recoverable-kafka.lock",
      makeDirectory: () => {
        makeAttempts += 1;
        return makeAttempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      moveDirectory: (source: string, destination: string) => {
        moved.push([source, destination]);
        return Promise.resolve();
      },
      ownerProcessId: 22,
      readOwnerFile: () => {
        ownerReads += 1;
        return Promise.resolve(
          JSON.stringify({
            acquiredAt: ownerReads === 1 ? 1_000 : 5_000,
            processId: ownerReads === 1 ? 11 : 22,
            token: ownerReads === 1 ? "abandoned" : "replacement",
          }),
        );
      },
      removeDirectory: (path: string) => {
        removed.push(path);
        return Promise.resolve();
      },
      token: "replacement",
      writeOwnerFile: (path: string, value: string) => {
        written.push([path, value]);
        return Promise.resolve();
      },
    });

    await expect(lock.acquire()).resolves.toBe(true);
    await lock.release();
    expect({
      makeAttempts,
      moved,
      removed,
      written: written.map(([path, value]) => [path, JSON.parse(value)]),
    }).toStrictEqual({
      makeAttempts: 2,
      moved: [
        [
          "/tmp/recoverable-kafka.lock",
          "/tmp/recoverable-kafka.lock.stale-22-replacement",
        ],
      ],
      removed: [
        "/tmp/recoverable-kafka.lock.stale-22-replacement",
        "/tmp/recoverable-kafka.lock",
      ],
      written: [
        [
          "/tmp/recoverable-kafka.lock/owner.json",
          {
            acquiredAt: 5_000,
            processId: 22,
            token: "replacement",
          },
        ],
      ],
    });
  });

  it("bounds waiting for a live lock owner", async () => {
    let waits = 0;
    const lock = createKafkaAdapterPortLock({
      currentTime: () => 5_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 4_999 }),
      isOwnerAlive: () => true,
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
      maximumAttempts: 2,
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 4_999, processId: 11, token: "active" })),
      waitForRetry: () => {
        waits += 1;
        return Promise.resolve();
      },
    });

    await expect(lock.acquire()).rejects.toThrow(
      "Timed out acquiring the Kafka adapter port lock after 2 attempts.",
    );
    expect(waits).toBe(1);
  });

  it("preserves stale live owners and recovers dead-owner, missing-status, and rename-race locks", async () => {
    let staleWaits = 0;
    let staleMoves = 0;
    const stale = createKafkaAdapterPortLock({
      currentTime: () => 10_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
      isOwnerAlive: () => true,
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
      maximumAttempts: 2,
      moveDirectory: () => {
        staleMoves += 1;
        return Promise.resolve();
      },
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "stale" })),
      removeDirectory: () => Promise.resolve(),
      staleAfterMilliseconds: 1,
      token: "stale",
      waitForRetry: () => {
        staleWaits += 1;
        return Promise.resolve();
      },
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(stale.acquire()).rejects.toThrow(
      "Timed out acquiring the Kafka adapter port lock after 2 attempts.",
    );
    await stale.release();

    let deadAttempts = 0;
    const dead = createKafkaAdapterPortLock({
      currentTime: () => 10_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 9_999 }),
      makeDirectory: () => {
        deadAttempts += 1;
        return deadAttempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      moveDirectory: () => Promise.resolve(),
      readOwnerFile: () =>
        Promise.resolve(
          JSON.stringify({
            acquiredAt: 9_999,
            processId: Number.MAX_SAFE_INTEGER,
            token: "dead",
          }),
        ),
      removeDirectory: () => Promise.resolve(),
      token: "dead",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(dead.acquire()).resolves.toBe(true);
    await dead.release();

    let missingAttempts = 0;
    const missing = createKafkaAdapterPortLock({
      getOwnerStatus: () => Promise.reject({ code: "ENOENT" }),
      makeDirectory: () => {
        missingAttempts += 1;
        return missingAttempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "missing" })),
      removeDirectory: () => Promise.resolve(),
      token: "missing",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(missing.acquire()).resolves.toBe(true);
    await missing.release();

    let renameAttempts = 0;
    const renameRace = createKafkaAdapterPortLock({
      currentTime: () => 10_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
      isOwnerAlive: () => false,
      makeDirectory: () => {
        renameAttempts += 1;
        return renameAttempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      moveDirectory: () => Promise.reject({ code: "ENOENT" }),
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "rename-race" })),
      removeDirectory: () => Promise.resolve(),
      staleAfterMilliseconds: 1,
      token: "rename-race",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(renameRace.acquire()).resolves.toBe(true);
    await renameRace.release();

    expect({
      deadAttempts,
      missingAttempts,
      renameAttempts,
      staleMoves,
      staleWaits,
    }).toStrictEqual({
      deadAttempts: 2,
      missingAttempts: 2,
      renameAttempts: 2,
      staleMoves: 0,
      staleWaits: 1,
    });
  });

  it("handles malformed owner metadata and preserves lock filesystem failures", async () => {
    const malformedOwners = [
      null,
      {},
      { processId: "1", token: "owner" },
      { processId: 1.5, token: "owner" },
      { processId: 0, token: "owner" },
      { processId: 1, token: 1 },
      { processId: 1, token: "" },
    ];
    for (const recordedOwner of malformedOwners) {
      let attempts = 0;
      let reads = 0;
      const lock = createKafkaAdapterPortLock({
        currentTime: () => 10_000,
        getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
        makeDirectory: () => {
          attempts += 1;
          return attempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
        },
        moveDirectory: () => Promise.resolve(),
        readOwnerFile: () => {
          reads += 1;
          return Promise.resolve(
            JSON.stringify(
              reads === 1
                ? recordedOwner
                : { acquiredAt: 10_000, processId: 1, token: "replacement" },
            ),
          );
        },
        removeDirectory: () => Promise.resolve(),
        token: "replacement",
        writeOwnerFile: () => Promise.resolve(),
      });
      await expect(lock.acquire()).resolves.toBe(true);
      await lock.release();
      expect(attempts).toBe(2);
    }

    let syntaxAttempts = 0;
    const syntax = createKafkaAdapterPortLock({
      currentTime: () => 10_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
      makeDirectory: () => {
        syntaxAttempts += 1;
        return syntaxAttempts === 1 ? Promise.reject({ code: "EEXIST" }) : Promise.resolve();
      },
      moveDirectory: () => Promise.resolve(),
      readOwnerFile: () => Promise.resolve("{"),
      removeDirectory: () => Promise.resolve(),
      token: "syntax",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(syntax.acquire()).resolves.toBe(true);

    const statusFailure = new Error("status failed");
    const statusLock = createKafkaAdapterPortLock({
      getOwnerStatus: () => Promise.reject(statusFailure),
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
    });
    await expect(statusLock.acquire()).rejects.toBe(statusFailure);

    const readFailure = new Error("read failed");
    const readLock = createKafkaAdapterPortLock({
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
      readOwnerFile: () => Promise.reject(readFailure),
    });
    await expect(readLock.acquire()).rejects.toBe(readFailure);

    const moveFailure = new Error("move failed");
    const moveLock = createKafkaAdapterPortLock({
      currentTime: () => 10_000,
      getOwnerStatus: () => Promise.resolve({ mtimeMs: 0 }),
      makeDirectory: () => Promise.reject({ code: "EEXIST" }),
      moveDirectory: () => Promise.reject(moveFailure),
      readOwnerFile: () => Promise.resolve("{}"),
      staleAfterMilliseconds: 1,
    });
    await expect(moveLock.acquire()).rejects.toBe(moveFailure);
  });

  it("cleans failed ownership writes and releases only its own recorded lock", async () => {
    const writeFailure = new Error("owner write failed");
    let failedWriteRemovals = 0;
    const failedWrite = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      removeDirectory: () => {
        failedWriteRemovals += 1;
        return Promise.resolve();
      },
      writeOwnerFile: () => Promise.reject(writeFailure),
    });
    await expect(failedWrite.acquire()).rejects.toBe(writeFailure);
    expect(failedWriteRemovals).toBe(1);

    let mismatchedRemovals = 0;
    const mismatched = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "replacement" })),
      removeDirectory: () => {
        mismatchedRemovals += 1;
        return Promise.resolve();
      },
      token: "original",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(mismatched.acquire()).resolves.toBe(true);
    await mismatched.release();
    expect(mismatchedRemovals).toBe(0);

    const missingOwner = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      readOwnerFile: () => Promise.reject({ code: "ENOENT" }),
      removeDirectory: () => Promise.resolve(),
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(missingOwner.acquire()).resolves.toBe(true);
    await expect(missingOwner.release()).resolves.toBeUndefined();

    const releaseFailure = new Error("release read failed");
    let failedReleaseReads = 0;
    let failedReleaseRemovals = 0;
    const failedRelease = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      readOwnerFile: () => {
        failedReleaseReads += 1;
        return failedReleaseReads === 1
          ? Promise.reject(releaseFailure)
          : Promise.resolve(
              JSON.stringify({ acquiredAt: 0, processId: 1, token: "retry-read" }),
            );
      },
      removeDirectory: () => {
        failedReleaseRemovals += 1;
        return Promise.resolve();
      },
      token: "retry-read",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(failedRelease.acquire()).resolves.toBe(true);
    await expect(failedRelease.release()).rejects.toBe(releaseFailure);
    await expect(failedRelease.release()).resolves.toBeUndefined();

    const removalFailure = new Error("release remove failed");
    let removalAttempts = 0;
    const failedRemoval = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "retry-remove" })),
      removeDirectory: () => {
        removalAttempts += 1;
        return removalAttempts === 1 ? Promise.reject(removalFailure) : Promise.resolve();
      },
      token: "retry-remove",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(failedRemoval.acquire()).resolves.toBe(true);
    await expect(failedRemoval.release()).rejects.toBe(removalFailure);
    await expect(failedRemoval.release()).resolves.toBeUndefined();

    let missingRemovalAttempts = 0;
    const missingRemoval = createKafkaAdapterPortLock({
      makeDirectory: () => Promise.resolve(),
      readOwnerFile: () =>
        Promise.resolve(JSON.stringify({ acquiredAt: 0, processId: 1, token: "missing-remove" })),
      removeDirectory: () => {
        missingRemovalAttempts += 1;
        return Promise.reject({ code: "ENOENT" });
      },
      token: "missing-remove",
      writeOwnerFile: () => Promise.resolve(),
    });
    await expect(missingRemoval.acquire()).resolves.toBe(true);
    await expect(missingRemoval.release()).resolves.toBeUndefined();
    await expect(missingRemoval.release()).resolves.toBeUndefined();
    expect({
      failedReleaseReads,
      failedReleaseRemovals,
      missingRemovalAttempts,
      removalAttempts,
    }).toStrictEqual({
      failedReleaseReads: 2,
      failedReleaseRemovals: 1,
      missingRemovalAttempts: 1,
      removalAttempts: 2,
    });
  });

  it("retries runner cleanup after a transient lock release failure", async () => {
    const fake = makeSpawn();
    const releaseFailure = new Error("transient lock release failure");
    let releases = 0;
    const runner = makeRunner(fake, [], fake.spawnProcess, {
      acquire: () => Promise.resolve(true),
      lockDirectory: "/tmp/retry-cleanup.lock",
      release: () => {
        releases += 1;
        return releases === 1 ? Promise.reject(releaseFailure) : Promise.resolve();
      },
    });
    const running = runner.runMain();
    await settle(fake.children, 0, 0);
    await settle(fake.children, 1, 0);
    await settle(fake.children, 2, 0);
    await expect(running).rejects.toBe(releaseFailure);

    const retry = runner.cleanup();
    await settle(fake.children, 3, 0);
    await expect(retry).resolves.toBe(0);
    expect({
      calls: fake.calls.map(({ command }) => command),
      releases,
    }).toStrictEqual({
      calls: ["docker", "vp", "docker", "docker"],
      releases: 2,
    });
  });

  it("uses one system-wide default lock path for the fixed Kafka ports", async () => {
    const lock = createKafkaAdapterPortLock();

    expect(lock.lockDirectory).toBe(
      join(tmpdir(), "effect-view-server-kafka-adapter-ports.lock"),
    );
    await lock.release();
  });

  it("maps lock failures and interruption while waiting without starting Docker", async () => {
    const failedFake = makeSpawn();
    const failedRunner = makeRunner(failedFake, [], failedFake.spawnProcess, {
      acquire: () => Promise.reject(new Error("lock failed")),
      lockDirectory: "/tmp/failed.lock",
      release: () => Promise.resolve(),
    });
    await expect(failedRunner.runMain()).resolves.toBe(1);
    expect(failedFake.calls).toStrictEqual([]);

    const interruptedFake = makeSpawn();
    let finishAcquisition = (_acquired: boolean) => {};
    const acquisition = new Promise<boolean>((resolve) => {
      finishAcquisition = resolve;
    });
    const interruptedRunner = makeRunner(interruptedFake, [], interruptedFake.spawnProcess, {
      acquire: () => acquisition,
      lockDirectory: "/tmp/interrupted.lock",
      release: () => Promise.resolve(),
    });
    const running = interruptedRunner.runMain();
    const interrupted = interruptedRunner.handleSignal("SIGINT");
    finishAcquisition(false);

    await expect(running).resolves.toBe(130);
    await expect(interrupted).resolves.toBe(130);
    expect(interruptedFake.calls).toStrictEqual([]);
  });

  it("releases an acquired port lock when interrupted before Docker starts", async () => {
    const fake = makeSpawn();
    let finishAcquisition = (_acquired: boolean) => {};
    let releases = 0;
    const acquisition = new Promise<boolean>((resolve) => {
      finishAcquisition = resolve;
    });
    const runner = makeRunner(fake, [], fake.spawnProcess, {
      acquire: () => acquisition,
      lockDirectory: "/tmp/interrupted-after-acquire.lock",
      release: () => {
        releases += 1;
        return Promise.resolve();
      },
    });
    const running = runner.runMain();
    const interrupted = runner.handleSignal("SIGTERM");
    finishAcquisition(true);

    await expect(running).resolves.toBe(143);
    await expect(interrupted).resolves.toBe(143);
    expect({ calls: fake.calls, releases }).toStrictEqual({
      calls: [],
      releases: 1,
    });
  });
});
