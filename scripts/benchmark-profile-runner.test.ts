import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import { fileSystemBenchmarkArtifactIo } from "./benchmark-artifact-io.mjs";
import {
  benchmarkProfileArtifactKind,
  validateBenchmarkProfileArtifact,
} from "./benchmark-profile-artifact.mjs";
import { taskForRepeat } from "./benchmark-baseline-task-catalog.mjs";
import { resolveBenchmarkProfile } from "./benchmark-profile.mjs";
import { runBenchmarkProfile } from "./benchmark-profile-runner.mjs";
import {
  makeDirectory,
  makeTask,
  silentLogger,
  summary,
  vitestOutput,
  writeArtifacts,
} from "./benchmark-baseline-runner-test-support";

const makeMemoryArtifactIo = () => {
  const files = new Map<string, unknown>();
  return {
    files,
    io: {
      exists: (path: string) => files.has(path),
      readJson: (path: string) => files.get(path),
      remove: (path: string) => {
        files.delete(path);
      },
      writeJson: (path: string, value: unknown) => {
        files.set(path, value);
      },
    },
  };
};

const writeTaskArtifacts = (
  files: Map<string, unknown>,
  task: ReturnType<typeof makeTask>,
) => {
  files.set(task.outputJsonPath, vitestOutput);
  files.set(task.summaryPath, {
    ...summary,
    latency: {
      ...summary.latency,
      outputJsonPath: task.packageOutputJsonPath,
    },
  });
};

describe("benchmark profile execution", () => {
  it("executes a resolved profile and persists a validated artifact through injected I/O", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));
    const memory = makeMemoryArtifactIo();
    const { logger } = silentLogger();

    const result = await runBenchmarkProfile({
      artifactIo: memory.io,
      environment: { KEEP_ME: "yes" },
      logger,
      profile,
      repeatCount: 1,
      runTask: async (currentTask) => {
        writeTaskArtifacts(memory.files, currentTask);
        return 0;
      },
    });

    expect({
      artifact: validateBenchmarkProfileArtifact(memory.files.get(profile.artifact.path)),
      exitCode: result.exitCode,
      persistedPath: result.artifactPath,
      resultArtifact: result.artifact,
    }).toStrictEqual({
      artifact: result.artifact,
      exitCode: 0,
      persistedPath: profile.artifact.path,
      resultArtifact: result.artifact,
    });
    expect(result.artifact?.artifactKind).toBe(benchmarkProfileArtifactKind);
    expect(Object.hasOwn(result.artifact ?? {}, "thresholds")).toBe(false);
  });

  it("executes every task and repeat serially with isolated artifacts and a clean environment", async () => {
    const directory = makeDirectory();
    const firstTask = makeTask(directory);
    const secondTask = {
      ...makeTask(directory),
      env: {
        TASK_SPECIFIC: "second",
        VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "second.json",
      },
      label: "task b",
      outputJsonPath: `${directory}/second.json`,
      packageOutputJsonPath: "second.json",
      summaryPath: `${directory}/second.summary.json`,
    };
    const profile = resolveBenchmarkProfile(
      "tiny",
      new Map([["tiny", [firstTask, secondTask]]]),
    );
    const memory = makeMemoryArtifactIo();
    const { logger } = silentLogger();
    const capturedTasks: Array<{
      env: Record<string, string>;
      label: string;
      staleOutputRemoved: boolean;
      staleSummaryRemoved: boolean;
    }> = [];

    for (let repeatIndex = 0; repeatIndex < 2; repeatIndex += 1) {
      for (const task of profile.tasks) {
        writeTaskArtifacts(memory.files, taskForRepeat(task, repeatIndex, 2));
      }
    }
    memory.files.set(profile.artifact.path, { stale: true });

    const result = await runBenchmarkProfile({
      artifactIo: memory.io,
      environment: {
        KEEP_ME: "yes",
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "tiny",
        VIEW_SERVER_BENCH_REPEAT_INDEX: "stale",
        VIEW_SERVER_BENCH_REPEAT_TOTAL: "stale",
        VIEW_SERVER_ENGINE_BENCH_ROWS: "stale",
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "stale",
        VIEW_SERVER_REACT_BENCH_ROWS: "stale",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "stale",
        VITE_VIEW_SERVER_REACT_BENCH_ROWS: "stale",
      },
      logger,
      profile,
      repeatCount: 2,
      runTask: async (currentTask) => {
        capturedTasks.push({
          env: currentTask.env,
          label: currentTask.label,
          staleOutputRemoved: !memory.io.exists(currentTask.outputJsonPath),
          staleSummaryRemoved: !memory.io.exists(currentTask.summaryPath),
        });
        writeTaskArtifacts(memory.files, currentTask);
        return 0;
      },
    });

    expect({
      capturedTasks,
      exitCode: result.exitCode,
      observationLabels: result.artifact?.tasks.map((task) => task.taskLabel),
    }).toStrictEqual({
      capturedTasks: [
        {
          env: {
            KEEP_ME: "yes",
            VIEW_SERVER_BENCH_REPEAT_INDEX: "1",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-01-of-02.json",
          },
          label: "task a run 1/2",
          staleOutputRemoved: true,
          staleSummaryRemoved: true,
        },
        {
          env: {
            KEEP_ME: "yes",
            TASK_SPECIFIC: "second",
            VIEW_SERVER_BENCH_REPEAT_INDEX: "1",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "second.run-01-of-02.json",
          },
          label: "task b run 1/2",
          staleOutputRemoved: true,
          staleSummaryRemoved: true,
        },
        {
          env: {
            KEEP_ME: "yes",
            VIEW_SERVER_BENCH_REPEAT_INDEX: "2",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-02-of-02.json",
          },
          label: "task a run 2/2",
          staleOutputRemoved: true,
          staleSummaryRemoved: true,
        },
        {
          env: {
            KEEP_ME: "yes",
            TASK_SPECIFIC: "second",
            VIEW_SERVER_BENCH_REPEAT_INDEX: "2",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "second.run-02-of-02.json",
          },
          label: "task b run 2/2",
          staleOutputRemoved: true,
          staleSummaryRemoved: true,
        },
      ],
      exitCode: 0,
      observationLabels: [
        "task a run 1/2",
        "task b run 1/2",
        "task a run 2/2",
        "task b run 2/2",
      ],
    });
  });

  it("appends task Node options without replacing caller Node options", async () => {
    const directory = makeDirectory();
    const baseTask = makeTask(directory);
    const task = {
      ...baseTask,
      env: {
        ...baseTask.env,
        NODE_OPTIONS: "--expose-gc",
      },
    };
    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));
    const memory = makeMemoryArtifactIo();
    const { logger } = silentLogger();
    let childNodeOptions: string | undefined;

    const result = await runBenchmarkProfile({
      artifactIo: memory.io,
      environment: { NODE_OPTIONS: "--max-old-space-size=12288 --trace-warnings" },
      logger,
      profile,
      repeatCount: 1,
      runTask: async (currentTask) => {
        childNodeOptions = currentTask.env.NODE_OPTIONS;
        writeTaskArtifacts(memory.files, currentTask);
        return 0;
      },
    });

    expect(childNodeOptions).toBe(
      "--max-old-space-size=12288 --trace-warnings --expose-gc",
    );
    expect(result.exitCode).toBe(0);
  });

  it("uses the filesystem artifact Adapter by default", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));
    const { logger } = silentLogger();

    const result = await runBenchmarkProfile({
      environment: {},
      logger,
      profile,
      repeatCount: 1,
      runTask: async (currentTask) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect({
      artifactKind: result.artifact?.artifactKind,
      artifactPersisted: fileSystemBenchmarkArtifactIo.exists(profile.artifact.path),
      exitCode: result.exitCode,
    }).toStrictEqual({
      artifactKind: benchmarkProfileArtifactKind,
      artifactPersisted: true,
      exitCode: 0,
    });
    fileSystemBenchmarkArtifactIo.remove(profile.artifact.path);
  });

  it("removes stale task and profile artifacts before returning a child failure", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));
    const memory = makeMemoryArtifactIo();
    const { logger } = silentLogger();
    writeTaskArtifacts(memory.files, task);
    memory.files.set(profile.artifact.path, { stale: true });

    const result = await runBenchmarkProfile({
      artifactIo: memory.io,
      environment: {},
      logger,
      profile,
      repeatCount: 1,
      runTask: async () => 42,
    });

    expect({
      artifact: result.artifact,
      exitCode: result.exitCode,
      outputExists: memory.io.exists(task.outputJsonPath),
      profileArtifactExists: memory.io.exists(profile.artifact.path),
      summaryExists: memory.io.exists(task.summaryPath),
    }).toStrictEqual({
      artifact: undefined,
      exitCode: 42,
      outputExists: false,
      profileArtifactExists: false,
      summaryExists: false,
    });
  });

  it("rejects successful children with missing or malformed artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));
    const missing = makeMemoryArtifactIo();
    const missingSummary = makeMemoryArtifactIo();
    const malformed = makeMemoryArtifactIo();
    const { logger } = silentLogger();

    await expect(
      runBenchmarkProfile({
        artifactIo: missing.io,
        environment: {},
        logger,
        profile,
        repeatCount: 1,
        runTask: async () => 0,
      }),
    ).rejects.toThrow(`${task.label}: missing benchmark output ${task.outputJsonPath}.`);

    await expect(
      runBenchmarkProfile({
        artifactIo: missingSummary.io,
        environment: {},
        logger,
        profile,
        repeatCount: 1,
        runTask: async (currentTask) => {
          missingSummary.files.set(currentTask.outputJsonPath, vitestOutput);
          return 0;
        },
      }),
    ).rejects.toThrow(`${task.label}: missing benchmark summary ${task.summaryPath}.`);

    await expect(
      runBenchmarkProfile({
        artifactIo: malformed.io,
        environment: {},
        logger,
        profile,
        repeatCount: 1,
        runTask: async (currentTask) => {
          writeTaskArtifacts(malformed.files, currentTask);
          malformed.files.set(currentTask.outputJsonPath, { files: "wrong" });
          return 0;
        },
      }),
    ).rejects.toThrow("Benchmark artifact field vitestOutput.files must be an array.");
  });

  it("contains no baseline comparison or threshold policy", () => {
    const source = readFileSync("scripts/benchmark-profile-runner.mjs", "utf8");

    expect(source).not.toContain("benchmark-baseline.mjs");
    expect(source).not.toContain("compareBenchmarkBaseline");
    expect(source).not.toContain("thresholds");
  });
});
