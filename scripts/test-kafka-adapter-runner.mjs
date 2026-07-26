import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as wait } from "node:timers/promises";

const exitCodeForSignal = (signal) => (signal === "SIGINT" ? 130 : 143);

const exitCodeForChildSignal = (signal) =>
  signal === "SIGINT"
    ? 130
    : signal === "SIGTERM"
      ? 143
      : signal === "SIGHUP"
        ? 129
        : signal === "SIGKILL"
          ? 137
          : 1;

const ownerIsAlive = (processId) => {
  try {
    process.kill(processId, 0);
    return true;
  } catch (error) {
    return error?.code !== "ESRCH";
  }
};

export const kafkaAdapterTestPlan = (testArguments) => {
  const vitestArguments = testArguments.filter((argument) => argument !== "--");
  const testFilters = vitestArguments.filter(
    (argument) => !argument.startsWith("-") && argument.includes(".test"),
  );
  const hasCoverageFlag = testArguments.some(
    (argument) =>
      argument === "--coverage" ||
      argument.startsWith("--coverage=") ||
      argument === "--no-coverage",
  );
  return {
    shouldCollectCoverage: vitestArguments.length === 0 && !hasCoverageFlag,
    shouldStartKafka:
      testFilters.length === 0 ||
      testFilters.some((argument) => argument.includes("kafka.integration.test")),
    vitestArguments,
  };
};

export const createKafkaAdapterPortLock = ({
  currentTime = () => performance.timeOrigin + performance.now(),
  getOwnerStatus = stat,
  isOwnerAlive = ownerIsAlive,
  lockDirectory = join(tmpdir(), "effect-view-server-kafka-adapter-ports.lock"),
  makeDirectory = mkdir,
  maximumAttempts = 18_000,
  moveDirectory = rename,
  ownerProcessId = process.pid,
  readOwnerFile = readFile,
  removeDirectory = rm,
  staleAfterMilliseconds = 30 * 60 * 1_000,
  token = randomUUID(),
  waitForRetry = () => wait(100),
  writeOwnerFile = writeFile,
} = {}) => {
  let ownsLock = false;
  const ownerFile = join(lockDirectory, "owner.json");
  const owner = Object.freeze({
    acquiredAt: currentTime(),
    processId: ownerProcessId,
    token,
  });

  const recoverAbandonedLock = async () => {
    let status;
    let recordedOwner;
    try {
      status = await getOwnerStatus(lockDirectory);
    } catch (error) {
      if (error?.code === "ENOENT") {
        return true;
      }
      throw error;
    }
    try {
      recordedOwner = JSON.parse(await readOwnerFile(ownerFile, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT" && !(error instanceof SyntaxError)) {
        throw error;
      }
    }
    const hasValidOwner =
      typeof recordedOwner === "object" &&
      recordedOwner !== null &&
      typeof recordedOwner.processId === "number" &&
      Number.isSafeInteger(recordedOwner.processId) &&
      recordedOwner.processId > 0 &&
      typeof recordedOwner.token === "string" &&
      recordedOwner.token.length > 0;
    const isStale = currentTime() - status.mtimeMs >= staleAfterMilliseconds;
    if (
      (hasValidOwner && isOwnerAlive(recordedOwner.processId)) ||
      (!hasValidOwner && !isStale)
    ) {
      return false;
    }
    const staleDirectory = `${lockDirectory}.stale-${ownerProcessId}-${token}`;
    try {
      await moveDirectory(lockDirectory, staleDirectory);
    } catch (error) {
      if (error?.code === "ENOENT") {
        return true;
      }
      throw error;
    }
    await removeDirectory(staleDirectory, { force: true, recursive: true });
    return true;
  };

  const acquire = async (shouldContinue = () => true) => {
    let attempts = 0;
    while (shouldContinue()) {
      try {
        await makeDirectory(lockDirectory);
        try {
          await writeOwnerFile(ownerFile, JSON.stringify(owner), {
            encoding: "utf8",
            flag: "wx",
          });
        } catch (error) {
          await removeDirectory(lockDirectory, { force: true, recursive: true });
          throw error;
        }
        ownsLock = true;
        return true;
      } catch (error) {
        if (error.code !== "EEXIST") {
          throw error;
        }
        attempts += 1;
        if (attempts >= maximumAttempts) {
          throw new Error(
            `Timed out acquiring the Kafka adapter port lock after ${maximumAttempts} attempts.`,
          );
        }
        if (await recoverAbandonedLock()) {
          continue;
        }
        await waitForRetry();
      }
    }
    return false;
  };

  const release = async () => {
    if (!ownsLock) {
      return;
    }
    let recordedOwner;
    try {
      recordedOwner = JSON.parse(await readOwnerFile(ownerFile, "utf8"));
    } catch (error) {
      if (error?.code === "ENOENT") {
        ownsLock = false;
        return;
      }
      throw error;
    }
    if (recordedOwner?.token !== token) {
      ownsLock = false;
      return;
    }
    try {
      await removeDirectory(lockDirectory, { force: true, recursive: true });
    } catch (error) {
      if (error?.code === "ENOENT") {
        ownsLock = false;
        return;
      }
      throw error;
    }
    ownsLock = false;
  };

  return {
    acquire,
    lockDirectory,
    release,
  };
};

export const createKafkaAdapterTestRunner = ({
  env,
  kafkaPortLock = createKafkaAdapterPortLock(),
  packageDirectory,
  processId,
  rootDirectory,
  spawnProcess,
  stdio,
  testArguments,
}) => {
  const plan = kafkaAdapterTestPlan(testArguments);
  const composeProjectName = `view-server-kafka-adapter-${processId}`;
  let activeChild = undefined;
  let activeRun = Promise.resolve(0);
  let cleanupPromise = undefined;
  let interruptedSignal = undefined;
  let isCleaning = false;
  let ownsKafkaPortLock = false;
  let shouldCleanupKafka = false;

  const run = (command, args, options) => {
    let child = undefined;
    try {
      child = spawnProcess(command, args, {
        cwd: options.cwd,
        env: options.env,
        stdio,
      });
    } catch {
      activeChild = undefined;
      activeRun = Promise.resolve(1);
      return activeRun;
    }
    activeChild = child;
    activeRun = new Promise((resolveExitCode) => {
      child.once("error", () => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        resolveExitCode(1);
      });
      child.once("close", (code, signal) => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        resolveExitCode(code ?? exitCodeForChildSignal(signal));
      });
    });
    return activeRun;
  };

  const dockerEnvironment = {
    ...env,
    COMPOSE_PROJECT_NAME: composeProjectName,
  };
  const docker = (args) =>
    run("docker", ["compose", "-f", "compose.yaml", ...args], {
      cwd: rootDirectory,
      env: dockerEnvironment,
    });

  const cleanup = () => {
    if (!shouldCleanupKafka && !ownsKafkaPortLock) {
      return Promise.resolve(0);
    }
    if (cleanupPromise !== undefined) {
      return cleanupPromise;
    }
    isCleaning = true;
    cleanupPromise = (shouldCleanupKafka ? docker(["down"]) : Promise.resolve(0))
      .then(async (exitCode) => {
        await kafkaPortLock.release();
        ownsKafkaPortLock = false;
        return exitCode;
      })
      .catch((error) => {
        cleanupPromise = undefined;
        throw error;
      })
      .finally(() => {
        isCleaning = false;
      });
    return cleanupPromise;
  };

  const interruptedExit = async () => {
    const cleanupExitCode = await cleanup();
    return cleanupExitCode === 0 ? exitCodeForSignal(interruptedSignal) : cleanupExitCode;
  };

  const handleSignal = async (signal) => {
    interruptedSignal = signal;
    if (!isCleaning && activeChild !== undefined && activeChild.killed !== true) {
      activeChild.kill("SIGTERM");
    }
    await activeRun;
    return interruptedExit();
  };

  const runMain = async () => {
    let exitCode = 0;
    if (plan.shouldStartKafka) {
      const lockAcquisition = kafkaPortLock.acquire(() => interruptedSignal === undefined);
      activeRun = lockAcquisition.then(
        () => 0,
        () => 1,
      );
      try {
        ownsKafkaPortLock = await lockAcquisition;
      } catch {
        return 1;
      }
      if (!ownsKafkaPortLock) {
        return interruptedExit();
      }
      if (interruptedSignal !== undefined) {
        return interruptedExit();
      }
      shouldCleanupKafka = true;
      exitCode = await docker(["up", "-d", "--wait", "kafka", "kafka-london"]);
      if (interruptedSignal !== undefined) {
        return interruptedExit();
      }
    }
    if (exitCode === 0) {
      exitCode = await run(
        "vp",
        [
          "test",
          "run",
          ...(plan.shouldCollectCoverage ? ["--coverage"] : []),
          "--typecheck",
          ...plan.vitestArguments,
        ],
        {
          cwd: packageDirectory,
          env: {
            ...env,
            COMPOSE_PROJECT_NAME: composeProjectName,
            VIEW_SERVER_KAFKA_INTEGRATION: plan.shouldStartKafka ? "1" : "0",
            VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS:
              env.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092",
            VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS:
              env.VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS ?? "localhost:9094",
          },
        },
      );
      if (interruptedSignal !== undefined) {
        return interruptedExit();
      }
    }
    const cleanupExitCode = await cleanup();
    return exitCode === 0 ? cleanupExitCode : exitCode;
  };

  return {
    cleanup,
    composeProjectName,
    handleSignal,
    plan,
    runMain,
  };
};
