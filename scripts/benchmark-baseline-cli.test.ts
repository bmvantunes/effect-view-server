import { describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import {
  childIsRunning,
  createBenchmarkTaskRunner,
  runBenchmarkBaselineCli,
} from "./benchmark-baseline-cli.mjs";
import {
  makeDirectory,
  makeTask,
  silentLogger,
} from "./benchmark-baseline-runner-test-support";

class FakeChildProcess extends EventEmitter {
  exitCode: number | null = null;
  killedSignals: Array<string> = [];
  signalCode: string | null = null;

  kill(signal: string) {
    this.killedSignals.push(signal);
    this.signalCode = signal;
    return true;
  }
}

describe("benchmark baseline runner", () => {
  it("runs benchmark child processes and maps process exit states", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const spawned: Array<{
      args: ReadonlyArray<string>;
      command: string;
      env: Record<string, string>;
      stdio: string;
    }> = [];
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: (command, args, options) => {
        spawned.push({
          args,
          command,
          env: options.env,
          stdio: options.stdio,
        });
        return child;
      },
    });
    const task = makeTask(makeDirectory());

    const exitCodePromise = taskRunner.runTask(task);
    child.exitCode = 7;
    child.emit("exit", 7, null);

    await expect(exitCodePromise).resolves.toBe(7);
    expect({
      childIsRunningAfterExit: childIsRunning(child),
      spawned,
    }).toStrictEqual({
      childIsRunningAfterExit: false,
      spawned: [
        {
          args: ["run", "--no-cache", "fake#bench"],
          command: "vp",
          env: {
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
          },
          stdio: "inherit",
        },
      ],
    });
  });

  it("reports child running state from exit and signal fields", () => {
    const running = new FakeChildProcess();
    const exited = new FakeChildProcess();
    const signalled = new FakeChildProcess();
    exited.exitCode = 0;
    signalled.signalCode = "SIGTERM";

    expect({
      exited: childIsRunning(exited),
      running: childIsRunning(running),
      signalled: childIsRunning(signalled),
    }).toStrictEqual({
      exited: false,
      running: true,
      signalled: false,
    });
  });

  it("ignores direct termination when no benchmark child is active", () => {
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => new FakeChildProcess(),
    });

    expect(taskRunner.terminateActiveChild("SIGTERM")).toBeUndefined();
  });

  it("returns the recorded parent termination code without spawning a future task", async () => {
    const processLike = new EventEmitter();
    const spawnedCommands: Array<string> = [];
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: (command) => {
        spawnedCommands.push(command);
        return new FakeChildProcess();
      },
    });

    processLike.emit("SIGTERM");

    await expect(taskRunner.runTask(makeTask(makeDirectory()))).resolves.toBe(143);
    expect(spawnedCommands).toStrictEqual([]);
  });

  it("leaves the active benchmark child intact when an older child exits", async () => {
    const firstChild = new FakeChildProcess();
    const secondChild = new FakeChildProcess();
    const children = [firstChild, secondChild];
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => children.shift(),
    });

    const firstExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    const secondExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    firstChild.exitCode = 0;
    firstChild.emit("exit", 0, null);
    taskRunner.terminateActiveChild("SIGTERM");
    secondChild.emit("exit", null, "SIGTERM");

    await expect(firstExitCode).resolves.toBe(0);
    await expect(secondExitCode).resolves.toBe(143);
    expect(secondChild.killedSignals).toStrictEqual(["SIGTERM"]);
  });

  it("keeps the active benchmark child when an older child reports an error", async () => {
    const firstChild = new FakeChildProcess();
    const secondChild = new FakeChildProcess();
    const children = [firstChild, secondChild];
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => children.shift(),
    });

    const firstExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    const secondExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    firstChild.emit("error", new Error("old child failed"));
    taskRunner.terminateActiveChild("SIGTERM");
    secondChild.emit("exit", null, "SIGTERM");

    await expect(firstExitCode).rejects.toThrow("old child failed");
    await expect(secondExitCode).resolves.toBe(143);
    expect(secondChild.killedSignals).toStrictEqual(["SIGTERM"]);
  });

  it("forwards parent termination signals to the active benchmark child", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    processLike.emit("SIGINT");
    child.emit("exit", null, "SIGTERM");

    await expect(exitCodePromise).resolves.toBe(130);
    expect(child.killedSignals).toStrictEqual(["SIGINT"]);
  });

  it("maps child signal exits when the parent did not initiate termination", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    child.signalCode = "SIGTERM";
    child.emit("exit", null, "SIGTERM");

    await expect(exitCodePromise).resolves.toBe(143);
  });

  it("maps null child exit codes to failure", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    child.emit("exit", null, null);

    await expect(exitCodePromise).resolves.toBe(1);
  });

  it("rejects benchmark child spawn errors", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });
    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    const error = new Error("spawn failed");

    child.emit("error", error);

    await expect(exitCodePromise).rejects.toThrow("spawn failed");
  });

  it("wires the CLI runner to the baseline core with a benchmark task runner", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const exitCode = await runBenchmarkBaselineCli({
      argv: ["node", "script"],
      environment: {},
      logger: silentLogger().logger,
      processLike,
      runBaseline: async ({ runTask }) => {
        const taskExitCode = runTask(makeTask(makeDirectory()));
        child.exitCode = 12;
        child.emit("exit", 12, null);
        return taskExitCode;
      },
      spawn: () => child,
    });

    expect(exitCode).toBe(12);
  });

  it("returns the parent termination code when termination happens outside child execution", async () => {
    const processLike = new EventEmitter();
    const exitCode = await runBenchmarkBaselineCli({
      argv: ["node", "script"],
      environment: {},
      logger: silentLogger().logger,
      processLike,
      runBaseline: async () => {
        processLike.emit("SIGHUP");
        return 0;
      },
      spawn: () => new FakeChildProcess(),
    });

    expect(exitCode).toBe(129);
  });
});
