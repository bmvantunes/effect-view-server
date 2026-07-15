import { describe, expect, it } from "@effect/vitest";
import { existsSync } from "node:fs";
import { join } from "node:path";
import {
  readBenchmarkBaseline,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";
import { defaultBenchmarkThresholds } from "./benchmark-comparison-policy.mjs";
import { runBenchmarkBaseline } from "./benchmark-baseline-workflow.mjs";
import {
  makeDirectory,
  makeTask,
  mixedSamplingPolicy,
  observation,
  silentLogger,
  summary,
  vitestOutput,
  writeArtifacts,
} from "./benchmark-baseline-runner-test-support";

describe("benchmark baseline runner", () => {
  it("updates and compares a tiny profile with fresh artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const baselineFile = join(directory, "baseline.json");
    const capturedEnvironments: Array<Record<string, string>> = [];
    const { logger } = silentLogger();
    const runTask = async (currentTask: typeof task) => {
      capturedEnvironments.push(currentTask.env);
      writeArtifacts(currentTask);
      return 0;
    };

    const updateExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline"],
      baselinePathForProfile: () => baselineFile,
      environment: {
        KEEP_ME: "yes",
        VIEW_SERVER_ENGINE_BENCH_ROWS: "stale",
      },
      logger,
      profileMap,
      runTask,
    });
    const compareExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => baselineFile,
      environment: {
        KEEP_ME: "yes",
      },
      logger,
      profileMap,
      runTask,
    });

    expect({
      baseline: readBenchmarkBaseline(baselineFile),
      capturedEnvironments,
      compareExitCode,
      updateExitCode,
    }).toStrictEqual({
      baseline: {
        artifactKind: "view-server-benchmark-baseline",
        profile: "tiny",
        tasks: [
          {
            ...observation,
            groupedKeyWidthParameters: undefined,
            outputJsonPath: task.outputJsonPath,
            summaryPath: task.summaryPath,
            throughputCases: undefined,
          },
        ],
        thresholds: defaultBenchmarkThresholds,
      },
      capturedEnvironments: [
        {
          KEEP_ME: "yes",
          VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
        },
        {
          KEEP_ME: "yes",
          VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
        },
      ],
      compareExitCode: 0,
      updateExitCode: 0,
    });
  });

  it("rejects wrong exact mutation totals before writing an updated baseline", async () => {
    const directory = makeDirectory();
    const task = {
      ...makeTask(directory),
      expectedMutationCount: 105,
      samplingPolicy: mixedSamplingPolicy,
    };
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny", "--update-baseline"],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(
            currentTask,
            {
              ...summary,
              benchmarkCases: ["read case", "live case"],
              mutationCount: 104,
              samplingPolicy: mixedSamplingPolicy,
            },
            {
              files: [
                {
                  groups: [
                    {
                      fullName: "src/runner-example.bench.ts > runner example benchmark group",
                      benchmarks: [
                        {
                          max: 3,
                          mean: 2,
                          min: 1,
                          name: "read case",
                          p99: 3,
                          sampleCount: 200,
                        },
                        {
                          max: 3,
                          mean: 2,
                          min: 1,
                          name: "live case",
                          p99: 3,
                          sampleCount: 5,
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          );
          return 0;
        },
      }),
    ).rejects.toThrow("task a: mutationCount must be exactly 105 but was 104.");
    expect(existsSync(baselineFile)).toBe(false);
  });

  it("updates only selected baseline tasks", async () => {
    const directory = makeDirectory();
    const firstTask = makeTask(directory);
    const secondTask = {
      ...makeTask(directory),
      env: {
        VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual-b.json",
      },
      label: "task b",
      outputJsonPath: join(directory, "actual-b.json"),
      packageOutputJsonPath: "actual-b.json",
      summaryPath: join(directory, "actual-b.summary.json"),
    };
    const profileMap = new Map([["tiny", [firstTask, secondTask]]]);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    let meanMs = 2;
    const runTask = async (currentTask: typeof firstTask) => {
      writeArtifacts(currentTask, summary, {
        files: vitestOutput.files.map((file) => ({
          ...file,
          groups: file.groups.map((group) => ({
            ...group,
            benchmarks: group.benchmarks.map((benchmark) => ({
              ...benchmark,
              mean: meanMs,
            })),
          })),
        })),
      });
      return 0;
    };

    const initialExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline"],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap,
      runTask,
    });
    const initialBaseline = readBenchmarkBaseline(baselineFile);
    meanMs = 2.5;
    const scopedTaskLabels: Array<string> = [];
    const scopedExitCode = await runBenchmarkBaseline({
      argv: [
        "node",
        "script",
        "--profile=tiny",
        "--update-baseline",
        "--update-baseline-task=task a",
      ],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof firstTask) => {
        scopedTaskLabels.push(currentTask.label);
        return runTask(currentTask);
      },
    });
    const scopedBaseline = readBenchmarkBaseline(baselineFile);

    expect({
      initialExitCode,
      preservedTask: scopedBaseline.tasks.find((task) => task.taskLabel === "task b"),
      scopedExitCode,
      scopedTaskLabels,
      updatedMeanMs: scopedBaseline.tasks.find((task) => task.taskLabel === "task a")?.benchmarks[0]
        ?.meanMs,
    }).toStrictEqual({
      initialExitCode: 0,
      preservedTask: initialBaseline.tasks.find((task) => task.taskLabel === "task b"),
      scopedExitCode: 0,
      scopedTaskLabels: ["task a"],
      updatedMeanMs: 2.5,
    });
  });

  it("rejects immutable selected-task topology drift without rewriting the baseline", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    const runTask = async (currentTask: typeof task) => {
      writeArtifacts(currentTask);
      return 0;
    };
    await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline"],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap: new Map([["tiny", [task]]]),
      runTask,
    });
    const originalBaseline = readBenchmarkBaseline(baselineFile);

    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(currentTask, {
            ...summary,
            benchmarkName: "drifted benchmark",
          });
          return 0;
        },
      }),
    ).rejects.toThrow(
      "Scoped benchmark update changed immutable task a fields: benchmarkName.",
    );
    expect(readBenchmarkBaseline(baselineFile)).toStrictEqual(originalBaseline);
  });

  it("rejects selected-task benchmark identity drift without rewriting the baseline", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    const runTask = async (currentTask: typeof task) => {
      writeArtifacts(currentTask);
      return 0;
    };
    await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline"],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap: new Map([["tiny", [task]]]),
      runTask,
    });
    const originalBaseline = readBenchmarkBaseline(baselineFile);

    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(currentTask, summary, {
            files: [
              {
                groups: [
                  {
                    fullName: "src/runner-example.bench.ts > runner example benchmark group",
                    benchmarks: [
                      {
                        max: 3,
                        mean: 2,
                        min: 1,
                        name: "renamed case",
                        p99: 3,
                        sampleCount: 7,
                      },
                    ],
                  },
                ],
              },
            ],
          });
          return 0;
        },
      }),
    ).rejects.toThrow("Scoped benchmark update changed immutable task a fields: benchmarks.");
    expect(readBenchmarkBaseline(baselineFile)).toStrictEqual(originalBaseline);
  });

  it("requires baseline update mode for scoped task updates", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const { logger, messages } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline-task=task a"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger,
      profileMap: new Map([["tiny", [task]]]),
      runTask: async () => 0,
    });

    expect({ exitCode, messages }).toStrictEqual({
      exitCode: 1,
      messages: ["--update-baseline-task requires --update-baseline."],
    });
  });

  it("rejects malformed scoped update arguments before running benchmarks", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const bareLogger = silentLogger();
    const emptyLogger = silentLogger();
    let runCount = 0;
    const runTask = async (currentTask: typeof task) => {
      runCount += 1;
      writeArtifacts(currentTask);
      return 0;
    };

    const bareExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline", "--update-baseline-task"],
      baselinePathForProfile: () => join(directory, "bare-baseline.json"),
      environment: {},
      logger: bareLogger.logger,
      profileMap,
      runTask,
    });
    const emptyExitCode = await runBenchmarkBaseline({
      argv: [
        "node",
        "script",
        "--profile=tiny",
        "--update-baseline",
        "--update-baseline-task=",
      ],
      baselinePathForProfile: () => join(directory, "empty-baseline.json"),
      environment: {},
      logger: emptyLogger.logger,
      profileMap,
      runTask,
    });

    expect({
      bareError: bareLogger.messages[0],
      bareExitCode,
      emptyError: emptyLogger.messages[0],
      emptyExitCode,
      runCount,
    }).toStrictEqual({
      bareError:
        "--update-baseline-task must use --update-baseline-task=<task label> with a non-empty label.",
      bareExitCode: 1,
      emptyError:
        "--update-baseline-task must use --update-baseline-task=<task label> with a non-empty label.",
      emptyExitCode: 1,
      runCount: 0,
    });
  });

  it("rejects unknown scoped baseline tasks before running benchmarks", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const { logger, messages } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: [
        "node",
        "script",
        "--profile=tiny",
        "--update-baseline",
        "--update-baseline-task=missing task",
      ],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger,
      profileMap: new Map([["tiny", [task]]]),
      runTask: async () => 0,
    });

    expect({ exitCode, messages }).toStrictEqual({
      exitCode: 1,
      messages: ["Unknown benchmark baseline task for tiny: missing task"],
    });
  });

  it("rejects scoped updates missing from the committed baseline", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "tiny",
      tasks: [
        {
          ...observation,
          outputJsonPath: task.outputJsonPath,
          summaryPath: task.summaryPath,
          taskLabel: "task b",
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    let runCount = 0;
    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async (currentTask: typeof task) => {
          runCount += 1;
          writeArtifacts(currentTask);
          return 0;
        },
      }),
    ).rejects.toThrow("Cannot update missing benchmark baseline task: task a");
    expect(runCount).toBe(0);
  });

  it("rejects scoped updates when the committed baseline omits an unselected current task", async () => {
    const directory = makeDirectory();
    const firstTask = makeTask(directory);
    const secondTask = {
      ...makeTask(directory),
      env: {
        VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual-b.json",
      },
      label: "task b",
      outputJsonPath: join(directory, "actual-b.json"),
      packageOutputJsonPath: "actual-b.json",
      summaryPath: join(directory, "actual-b.summary.json"),
    };
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    let runCount = 0;
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "tiny",
      tasks: [
        {
          ...observation,
          outputJsonPath: firstTask.outputJsonPath,
          summaryPath: firstTask.summaryPath,
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [firstTask, secondTask]]]),
        runTask: async () => {
          runCount += 1;
          return 0;
        },
      }),
    ).rejects.toThrow("Cannot update missing benchmark baseline task: task b");
    expect(runCount).toBe(0);
  });

  it("rejects scoped updates when the committed baseline has a stale task", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    let runCount = 0;
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "tiny",
      tasks: [
        {
          ...observation,
          outputJsonPath: task.outputJsonPath,
          summaryPath: task.summaryPath,
        },
        {
          ...observation,
          outputJsonPath: join(directory, "actual-b.json"),
          summaryPath: join(directory, "actual-b.summary.json"),
          taskLabel: "task b",
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async () => {
          runCount += 1;
          return 0;
        },
      }),
    ).rejects.toThrow(
      "Cannot update stale benchmark baseline task absent from current profile: task b",
    );
    expect(runCount).toBe(0);
  });

  it("rejects a scoped update against a different baseline profile before running", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const baselineFile = join(directory, "baseline.json");
    const { logger } = silentLogger();
    let runCount = 0;
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "other",
      tasks: [
        {
          ...observation,
          outputJsonPath: task.outputJsonPath,
          summaryPath: task.summaryPath,
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    await expect(
      runBenchmarkBaseline({
        argv: [
          "node",
          "script",
          "--profile=tiny",
          "--update-baseline",
          "--update-baseline-task=task a",
        ],
        baselinePathForProfile: () => baselineFile,
        environment: {},
        logger,
        profileMap: new Map([["tiny", [task]]]),
        runTask: async () => {
          runCount += 1;
          return 0;
        },
      }),
    ).rejects.toThrow("Cannot update benchmark profile tiny from committed profile other.");
    expect(runCount).toBe(0);
  });
});
