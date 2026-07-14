import { describe, expect, it } from "@effect/vitest";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { defaultBenchmarkThresholds, writeBenchmarkBaseline } from "./benchmark-baseline.mjs";
import {
  assertTaskArtifactsWritten,
  baselinePath,
  cleanBenchmarkEnvironment,
  exitCodeForSignal,
  removeTaskArtifacts,
  repeatArtifactPath,
  repeatCountFrom,
  runBenchmarkBaseline,
  summaryPath,
  taskForRepeat,
} from "./benchmark-baseline-runner.mjs";
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
  it("computes runner utility values", () => {
    expect({
      baseline: baselinePath("smoke"),
      cleanedEnvironment: cleanBenchmarkEnvironment({
        KEEP_ME: "yes",
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "smoke",
        VIEW_SERVER_BENCH_REPEAT_INDEX: "2",
        VIEW_SERVER_BENCH_REPEAT_TOTAL: "3",
        VIEW_SERVER_ENGINE_BENCH_ROWS: "100",
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "localhost:19092",
        VIEW_SERVER_REACT_BENCH_ROWS: "100",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "100",
        VITE_VIEW_SERVER_REACT_BENCH_ROWS: "100",
      }),
      knownSignalExitCode: exitCodeForSignal("SIGTERM"),
      nonJsonSummary: summaryPath(".artifacts/result"),
      summary: summaryPath(".artifacts/result.json"),
      unknownSignalExitCode: exitCodeForSignal("NOT_A_SIGNAL"),
    }).toStrictEqual({
      baseline: "benchmarks/baselines/smoke.json",
      cleanedEnvironment: {
        KEEP_ME: "yes",
      },
      knownSignalExitCode: 143,
      nonJsonSummary: ".artifacts/result.summary.json",
      summary: ".artifacts/result.summary.json",
      unknownSignalExitCode: 1,
    });
  });

  it("computes repeat artifact paths and repeat task environments", () => {
    const directory = makeDirectory();
    const currentTask = makeTask(directory);
    const repeatedTask = taskForRepeat(currentTask, 1, 3);
    const repeatedTaskWithExtraEnv = taskForRepeat(
      {
        ...currentTask,
        env: {
          ...currentTask.env,
          KEEP_ME: "yes",
        },
      },
      0,
      2,
    );

    expect({
      invalidRepeat: repeatCountFrom(["node", "script", "--repeat=0"]),
      invalidRepeatBare: repeatCountFrom(["node", "script", "--repeat", "3"]),
      invalidRepeatDecimal: repeatCountFrom(["node", "script", "--repeat=1.5"]),
      invalidRepeatDuplicate: repeatCountFrom(["node", "script", "--repeat=3", "--repeat=4"]),
      invalidRepeatJunk: repeatCountFrom(["node", "script", "--repeat=3abc"]),
      invalidRepeatTooLarge: repeatCountFrom(["node", "script", "--repeat=21"]),
      invalidRepeatUnsafe: repeatCountFrom(["node", "script", "--repeat=9007199254740992"]),
      missingRepeat: repeatCountFrom(["node", "script"]),
      repeatedEnv: repeatedTask.env,
      repeatedExtraEnv: repeatedTaskWithExtraEnv.env,
      repeatedLabel: repeatedTask.label,
      repeatedOutputJsonPath: repeatedTask.outputJsonPath,
      repeatedPackageOutputJsonPath: repeatedTask.packageOutputJsonPath,
      repeatedSummaryPath: repeatedTask.summaryPath,
      repeatPathNoExtension: repeatArtifactPath(".artifacts/result", 1, 2),
      repeatPath: repeatArtifactPath(".artifacts/result.json", 2, 12),
      singlePath: repeatArtifactPath(".artifacts/result.json", 0, 1),
      validRepeat: repeatCountFrom(["node", "script", "--repeat=3"]),
    }).toStrictEqual({
      invalidRepeat: undefined,
      invalidRepeatBare: undefined,
      invalidRepeatDecimal: undefined,
      invalidRepeatDuplicate: undefined,
      invalidRepeatJunk: undefined,
      invalidRepeatTooLarge: undefined,
      invalidRepeatUnsafe: undefined,
      missingRepeat: 1,
      repeatedEnv: {
        VIEW_SERVER_BENCH_REPEAT_INDEX: "2",
        VIEW_SERVER_BENCH_REPEAT_TOTAL: "3",
        VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-02-of-03.json",
      },
      repeatedExtraEnv: {
        KEEP_ME: "yes",
        VIEW_SERVER_BENCH_REPEAT_INDEX: "1",
        VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
        VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-01-of-02.json",
      },
      repeatedLabel: "task a run 2/3",
      repeatedOutputJsonPath: join(directory, "actual.run-02-of-03.json"),
      repeatedPackageOutputJsonPath: "actual.run-02-of-03.json",
      repeatedSummaryPath: join(directory, "actual.run-02-of-03.summary.json"),
      repeatPathNoExtension: ".artifacts/result.run-02-of-02",
      repeatPath: ".artifacts/result.run-03-of-12.json",
      singlePath: ".artifacts/result.json",
      validRepeat: 3,
    });
  });

  it("runs no-compare profiles without an existing baseline", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      repeatableProfiles: new Set(["tiny"]),
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });

  it("runs repeated report-only profiles with isolated artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const capturedTasks: Array<{
      env: Record<string, string>;
      label: string;
      outputJsonPath: string;
      summaryPath: string;
    }> = [];
    const { logger, messages } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--repeat=2", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      repeatableProfiles: new Set(["tiny"]),
      runTask: async (currentTask: typeof task) => {
        capturedTasks.push({
          env: currentTask.env,
          label: currentTask.label,
          outputJsonPath: currentTask.outputJsonPath,
          summaryPath: currentTask.summaryPath,
        });
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect({
      capturedTasks,
      exitCode,
      startMessage: messages[0],
    }).toStrictEqual({
      capturedTasks: [
        {
          env: {
            VIEW_SERVER_BENCH_REPEAT_INDEX: "1",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-01-of-02.json",
          },
          label: "task a run 1/2",
          outputJsonPath: join(directory, "actual.run-01-of-02.json"),
          summaryPath: join(directory, "actual.run-01-of-02.summary.json"),
        },
        {
          env: {
            VIEW_SERVER_BENCH_REPEAT_INDEX: "2",
            VIEW_SERVER_BENCH_REPEAT_TOTAL: "2",
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.run-02-of-02.json",
          },
          label: "task a run 2/2",
          outputJsonPath: join(directory, "actual.run-02-of-02.json"),
          summaryPath: join(directory, "actual.run-02-of-02.summary.json"),
        },
      ],
      exitCode: 0,
      startMessage: "Running tiny benchmark baseline serially (1 tasks x 2 runs).",
    });
  });

  it("rejects repeated baseline runs outside report-only mode", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const compareLogger = silentLogger();
    const updateLogger = silentLogger();
    const invalidLogger = silentLogger();
    const unsupportedLogger = silentLogger();
    const unknownProfileLogger = silentLogger();

    const compareExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--repeat=2"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger: compareLogger.logger,
      profileMap,
      runTask: async () => 0,
    });
    const updateExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--repeat=2", "--update-baseline", "--no-compare"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger: updateLogger.logger,
      profileMap,
      repeatableProfiles: new Set(["tiny"]),
      runTask: async () => 0,
    });
    const invalidExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--repeat=nope", "--no-compare"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger: invalidLogger.logger,
      profileMap,
      runTask: async () => 0,
    });
    const unsupportedExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--repeat=2", "--no-compare"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger: unsupportedLogger.logger,
      profileMap,
      runTask: async () => 0,
    });
    const unknownProfileExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=missing", "--repeat=2", "--no-compare"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger: unknownProfileLogger.logger,
      profileMap,
      runTask: async () => 0,
    });

    expect({
      compareError: compareLogger.messages[0],
      compareExitCode,
      invalidError: invalidLogger.messages[0],
      invalidExitCode,
      unknownProfileError: unknownProfileLogger.messages[0],
      unknownProfileExitCode,
      unsupportedError: unsupportedLogger.messages[0],
      unsupportedExitCode,
      updateError: updateLogger.messages[0],
      updateExitCode,
    }).toStrictEqual({
      compareError: "--repeat requires --no-compare because repeated artifacts are report-only.",
      compareExitCode: 1,
      invalidError: "--repeat must be a positive integer.",
      invalidExitCode: 1,
      unknownProfileError: "Unknown benchmark baseline profile: missing\nAvailable profiles: tiny",
      unknownProfileExitCode: 1,
      unsupportedError: "--repeat is not enabled for benchmark baseline profile: tiny",
      unsupportedExitCode: 1,
      updateError: "--repeat cannot be combined with --update-baseline.",
      updateExitCode: 1,
    });
  });

  it("uses the profile from the benchmark environment when no profile argument is provided", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "tiny",
      },
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });

  it("uses the smoke profile by default", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["smoke", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });

  it("accepts the timed-read floor and exact iteration-bound sample count", async () => {
    const directory = makeDirectory();
    const task = {
      ...makeTask(directory),
      samplingPolicy: mixedSamplingPolicy,
    };
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(
          currentTask,
          {
            ...summary,
            benchmarkCases: ["read case", "live case"],
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
    });

    expect(exitCode).toBe(0);
  });

  it("rejects an under-sampled timed read", async () => {
    const directory = makeDirectory();
    const task = {
      ...makeTask(directory),
      samplingPolicy: mixedSamplingPolicy,
    };
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny", "--no-compare"],
        baselinePathForProfile: () => join(directory, "missing-baseline.json"),
        environment: {},
        logger,
        profileMap,
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(
            currentTask,
            {
              ...summary,
              benchmarkCases: ["read case", "live case"],
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
                          sampleCount: 199,
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
    ).rejects.toThrow(
      "task a / src/runner-example.bench.ts > runner example benchmark group / read case: timed read sampleCount must be at least 200 but was 199.",
    );
  });

  it("rejects a non-exact iteration-bound sample count", async () => {
    const directory = makeDirectory();
    const task = {
      ...makeTask(directory),
      samplingPolicy: mixedSamplingPolicy,
    };
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny", "--no-compare"],
        baselinePathForProfile: () => join(directory, "missing-baseline.json"),
        environment: {},
        logger,
        profileMap,
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(
            currentTask,
            {
              ...summary,
              benchmarkCases: ["read case", "live case"],
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
                          sampleCount: 6,
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
    ).rejects.toThrow(
      "task a / src/runner-example.bench.ts > runner example benchmark group / live case: iteration-bound sampleCount must be exactly 5 but was 6.",
    );
  });

  it("rejects a missing declared iteration-bound benchmark", async () => {
    const directory = makeDirectory();
    const task = {
      ...makeTask(directory),
      samplingPolicy: mixedSamplingPolicy,
    };
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny", "--no-compare"],
        baselinePathForProfile: () => join(directory, "missing-baseline.json"),
        environment: {},
        logger,
        profileMap,
        runTask: async (currentTask: typeof task) => {
          writeArtifacts(currentTask, {
            ...summary,
            benchmarkCases: ["read case"],
            samplingPolicy: mixedSamplingPolicy,
          }, {
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
                    ],
                  },
                ],
              },
            ],
          });
          return 0;
        },
      }),
    ).rejects.toThrow("task a: missing iteration-bound benchmark case live case.");
  });


  it("returns child task failures without reading stale artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    writeArtifacts(task);
    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async () => 42,
    });

    expect({
      exitCode,
      outputStillExists: existsSync(task.outputJsonPath),
      summaryStillExists: existsSync(task.summaryPath),
    }).toStrictEqual({
      exitCode: 42,
      outputStillExists: false,
      summaryStillExists: false,
    });
  });

  it("rejects successful tasks that do not write expected artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny"],
        baselinePathForProfile: () => join(directory, "baseline.json"),
        environment: {},
        logger,
        profileMap,
        runTask: async () => 0,
      }),
    ).rejects.toThrow(`${task.label}: missing benchmark output ${task.outputJsonPath}.`);
  });

  it("rejects missing summaries after output was written", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    writeFileSync(task.outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() => assertTaskArtifactsWritten(task)).toThrow(
      `${task.label}: missing benchmark summary ${task.summaryPath}.`,
    );
  });

  it("returns comparison failures", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const baselineFile = join(directory, "baseline.json");
    const { logger, messages } = silentLogger();
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "tiny",
      tasks: [
        {
          ...observation,
          outputJsonPath: task.outputJsonPath,
          summaryPath: task.summaryPath,
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask, summary, {
          files: [
            {
              groups: [
                {
                  fullName: "src/runner-example.bench.ts > runner example benchmark group",
                  benchmarks: [
                    {
                      max: 100,
                      mean: 100,
                      min: 100,
                      name: "case a",
                      p99: 100,
                      sampleCount: 5,
                    },
                  ],
                },
              ],
            },
          ],
        });
        return 0;
      },
    });

    expect({
      exitCode,
      firstError: messages[3],
    }).toStrictEqual({
      exitCode: 1,
      firstError:
        "\ntiny benchmark baseline regressed:\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: mean regressed from 2.000ms to 100.000ms; allowed <= 16.000ms.\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 24.000ms.",
    });
  });

  it("returns unknown profile failures", async () => {
    const { logger, messages } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=missing"],
      environment: {},
      logger,
      runTask: async () => 0,
    });

    expect({
      exitCode,
      message: messages[0],
    }).toStrictEqual({
      exitCode: 1,
      message:
        "Unknown benchmark baseline profile: missing\nAvailable profiles: smoke, kafka-ingest, kafka-sustained-firehose, grpc-materialized, grpc-leased, grpc-leased-retained, websocket-firehose, active-query-sharing, raw-read-write, grouped-admission, grouped-order-neutral, release",
    });
  });

  it("removes expected artifacts before each run", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    writeArtifacts(task);

    removeTaskArtifacts(task);

    expect({
      outputStillExists: existsSync(task.outputJsonPath),
      summaryStillExists: existsSync(task.summaryPath),
    }).toStrictEqual({
      outputStillExists: false,
      summaryStillExists: false,
    });
  });
});
