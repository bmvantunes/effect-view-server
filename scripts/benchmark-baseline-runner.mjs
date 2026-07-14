import { existsSync, rmSync } from "node:fs";
import { constants as osConstants } from "node:os";
import {
  buildBenchmarkBaseline,
  compareBenchmarkBaseline,
  readBenchmarkBaseline,
  readBenchmarkObservation,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";
import {
  profiles,
  repeatableReportOnlyProfiles,
} from "./benchmark-baseline-profiles.mjs";
import {
  repeatArtifactPath,
  summaryPath,
  taskForRepeat,
} from "./benchmark-baseline-task-catalog.mjs";

export {
  profiles,
  repeatableReportOnlyProfiles,
  repeatArtifactPath,
  summaryPath,
  taskForRepeat,
};

export const baselinePath = (profile) => `benchmarks/baselines/${profile}.json`;

export const repeatCountFrom = (argv) => {
  if (argv.includes("--repeat")) {
    return undefined;
  }
  const repeatArguments = argv.filter((argument) => argument.startsWith("--repeat="));
  if (repeatArguments.length > 1) {
    return undefined;
  }
  const repeatArgument = repeatArguments[0];
  if (repeatArgument === undefined) {
    return 1;
  }
  const repeatValue = repeatArgument.slice("--repeat=".length);
  if (!/^[1-9]\d*$/.test(repeatValue)) {
    return undefined;
  }
  const repeatCount = Number.parseInt(repeatValue, 10);
  return Number.isSafeInteger(repeatCount) && repeatCount <= 20 ? repeatCount : undefined;
};

export const isBenchmarkEnvironmentKey = (key) =>
  key === "VIEW_SERVER_BENCH_BASELINE_PROFILE" ||
  key === "VIEW_SERVER_BENCH_REPEAT_INDEX" ||
  key === "VIEW_SERVER_BENCH_REPEAT_TOTAL" ||
  key === "VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS" ||
  key.startsWith("VIEW_SERVER_ENGINE_BENCH_") ||
  key.startsWith("VIEW_SERVER_REACT_BENCH_") ||
  key.startsWith("VIEW_SERVER_RUNTIME_BENCH_") ||
  key.startsWith("VITE_VIEW_SERVER_REACT_BENCH_");

export const cleanBenchmarkEnvironment = (environment) =>
  Object.fromEntries(
    Object.entries(environment).filter(([key]) => !isBenchmarkEnvironmentKey(key)),
  );

export const exitCodeForSignal = (signal) => {
  const signalNumber = osConstants.signals[signal];
  return typeof signalNumber === "number" ? 128 + signalNumber : 1;
};

export const removeTaskArtifacts = (currentTask) => {
  rmSync(currentTask.outputJsonPath, { force: true });
  rmSync(currentTask.summaryPath, { force: true });
};

export const assertTaskArtifactsWritten = (currentTask) => {
  if (!existsSync(currentTask.outputJsonPath)) {
    throw new Error(`${currentTask.label}: missing benchmark output ${currentTask.outputJsonPath}.`);
  }
  if (!existsSync(currentTask.summaryPath)) {
    throw new Error(`${currentTask.label}: missing benchmark summary ${currentTask.summaryPath}.`);
  }
};

const requestedProfileFrom = (argv, environment) => {
  const profileArgument = argv.find((argument) => argument.startsWith("--profile="));
  return (
    profileArgument?.slice("--profile=".length) ??
    environment["VIEW_SERVER_BENCH_BASELINE_PROFILE"] ??
    "smoke"
  );
};

const updateBaselineTaskArgument = "--update-baseline-task";
const updateBaselineTaskArgumentPrefix = `${updateBaselineTaskArgument}=`;
const scopedMutableTaskFields = new Set([
  "benchmarks",
  "memoryRssTotalDeltaBytes",
  "minimumSampleCount",
  "samplingPolicy",
]);

const updateBaselineTaskSelectionFrom = (argv) => {
  const scopedArguments = argv.filter((argument) =>
    argument.startsWith(updateBaselineTaskArgument),
  );
  const malformedArgument = scopedArguments.find(
    (argument) =>
      !argument.startsWith(updateBaselineTaskArgumentPrefix) ||
      argument.slice(updateBaselineTaskArgumentPrefix.length).trim() === "",
  );
  return {
    malformedArgument,
    taskLabels: scopedArguments.map((argument) =>
      argument.slice(updateBaselineTaskArgumentPrefix.length),
    ),
  };
};

const mergeSelectedBaselineTasks = (baseline, actualBaseline) => {
  const actualTaskByLabel = new Map(
    actualBaseline.tasks.map((task) => [task.taskLabel, task]),
  );
  for (const baselineTask of baseline.tasks) {
    const actualTask = actualTaskByLabel.get(baselineTask.taskLabel);
    if (actualTask === undefined) {
      continue;
    }
    const immutableDriftFields = Object.keys({ ...baselineTask, ...actualTask }).filter(
      (field) =>
        !scopedMutableTaskFields.has(field) &&
        JSON.stringify(baselineTask[field]) !== JSON.stringify(actualTask[field]),
    );
    const baselineBenchmarkIdentities = baselineTask.benchmarks.map(({ groupName, name }) => ({
      groupName,
      name,
    }));
    const actualBenchmarkIdentities = actualTask.benchmarks.map(({ groupName, name }) => ({
      groupName,
      name,
    }));
    if (
      JSON.stringify(baselineBenchmarkIdentities) !== JSON.stringify(actualBenchmarkIdentities)
    ) {
      immutableDriftFields.push("benchmarks");
    }
    if (immutableDriftFields.length > 0) {
      throw new Error(
        `Scoped benchmark update changed immutable ${baselineTask.taskLabel} fields: ${immutableDriftFields.join(", ")}.`,
      );
    }
  }
  return {
    ...baseline,
    tasks: baseline.tasks.map((task) =>
      actualTaskByLabel.get(task.taskLabel) ?? task,
    ),
  };
};

export const runBenchmarkBaseline = async ({
  argv,
  baselinePathForProfile = baselinePath,
  environment,
  logger,
  profileMap = profiles,
  repeatableProfiles = repeatableReportOnlyProfiles,
  runTask,
}) => {
  const compareBaseline = !argv.includes("--no-compare");
  const updateBaseline = argv.includes("--update-baseline");
  const updateBaselineTaskSelection = updateBaselineTaskSelectionFrom(argv);
  const updateBaselineTaskLabels = updateBaselineTaskSelection.taskLabels;
  const repeatCount = repeatCountFrom(argv);
  const requestedProfile = requestedProfileFrom(argv, environment);
  const tasks = profileMap.get(requestedProfile);
  const parentEnvironment = cleanBenchmarkEnvironment(environment);

  if (repeatCount === undefined) {
    logger.error("--repeat must be a positive integer.");
    return 1;
  }
  if (repeatCount > 1 && compareBaseline) {
    logger.error("--repeat requires --no-compare because repeated artifacts are report-only.");
    return 1;
  }
  if (repeatCount > 1 && updateBaseline) {
    logger.error("--repeat cannot be combined with --update-baseline.");
    return 1;
  }
  if (updateBaselineTaskSelection.malformedArgument !== undefined) {
    logger.error(
      "--update-baseline-task must use --update-baseline-task=<task label> with a non-empty label.",
    );
    return 1;
  }
  if (updateBaselineTaskLabels.length > 0 && !updateBaseline) {
    logger.error("--update-baseline-task requires --update-baseline.");
    return 1;
  }

  if (tasks === undefined) {
    logger.error(
      [
        `Unknown benchmark baseline profile: ${requestedProfile}`,
        `Available profiles: ${[...profileMap.keys()].join(", ")}`,
      ].join("\n"),
    );
    return 1;
  }
  if (repeatCount > 1 && !repeatableProfiles.has(requestedProfile)) {
    logger.error(`--repeat is not enabled for benchmark baseline profile: ${requestedProfile}`);
    return 1;
  }
  const unknownUpdateTaskLabel = updateBaselineTaskLabels.find(
    (taskLabel) => !tasks.some((task) => task.label === taskLabel),
  );
  if (unknownUpdateTaskLabel !== undefined) {
    logger.error(`Unknown benchmark baseline task for ${requestedProfile}: ${unknownUpdateTaskLabel}`);
    return 1;
  }

  const profileBaselinePath = baselinePathForProfile(requestedProfile);
  const scopedBaseline =
    updateBaselineTaskLabels.length === 0
      ? undefined
      : readBenchmarkBaseline(profileBaselinePath);
  if (scopedBaseline !== undefined && scopedBaseline.profile !== requestedProfile) {
    throw new Error(
      `Cannot update benchmark profile ${requestedProfile} from committed profile ${scopedBaseline.profile}.`,
    );
  }
  const missingBaselineTaskLabel =
    scopedBaseline === undefined
      ? undefined
      : tasks.find(
          (task) =>
            !scopedBaseline.tasks.some((baselineTask) => baselineTask.taskLabel === task.label),
        )?.label;
  if (missingBaselineTaskLabel !== undefined) {
    throw new Error(`Cannot update missing benchmark baseline task: ${missingBaselineTaskLabel}`);
  }
  const staleBaselineTaskLabel =
    scopedBaseline === undefined
      ? undefined
      : scopedBaseline.tasks.find(
          (baselineTask) => !tasks.some((task) => task.label === baselineTask.taskLabel),
        )?.taskLabel;
  if (staleBaselineTaskLabel !== undefined) {
    throw new Error(
      `Cannot update stale benchmark baseline task absent from current profile: ${staleBaselineTaskLabel}`,
    );
  }
  const selectedTaskLabelSet = new Set(updateBaselineTaskLabels);
  const tasksToRun =
    updateBaselineTaskLabels.length === 0
      ? tasks
      : tasks.filter((task) => selectedTaskLabelSet.has(task.label));

  const runCountMessage =
    repeatCount === 1
      ? `${tasksToRun.length} tasks`
      : `${tasksToRun.length} tasks x ${repeatCount} runs`;
  logger.log(`Running ${requestedProfile} benchmark baseline serially (${runCountMessage}).`);

  const completedTasks = [];
  const totalTaskRuns = tasksToRun.length * repeatCount;
  for (let repeatIndex = 0; repeatIndex < repeatCount; repeatIndex += 1) {
    for (const [index, baseTask] of tasksToRun.entries()) {
      const taskNumber = repeatIndex * tasksToRun.length + index + 1;
      const currentTask = taskForRepeat(baseTask, repeatIndex, repeatCount);
      const startedAt = process.hrtime.bigint();
      logger.log(`\n[${taskNumber}/${totalTaskRuns}] ${currentTask.label}`);
      removeTaskArtifacts(currentTask);
      const exitCode = await runTask({
        ...currentTask,
        env: {
          ...parentEnvironment,
          ...currentTask.env,
        },
      });
      if (exitCode !== 0) {
        return exitCode;
      }
      assertTaskArtifactsWritten(currentTask);
      completedTasks.push(currentTask);
      const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
      logger.log(`[${taskNumber}/${totalTaskRuns}] completed in ${elapsedMs.toFixed(0)}ms`);
    }
  }

  const observations = completedTasks.map(readBenchmarkObservation);
  const actualBaseline = buildBenchmarkBaseline(requestedProfile, observations);

  if (updateBaseline) {
    const baselineToWrite =
      scopedBaseline === undefined
        ? actualBaseline
        : mergeSelectedBaselineTasks(scopedBaseline, actualBaseline);
    writeBenchmarkBaseline(profileBaselinePath, baselineToWrite);
    logger.log(`\nUpdated benchmark baseline: ${profileBaselinePath}`);
  } else if (compareBaseline) {
    const baseline = readBenchmarkBaseline(profileBaselinePath);
    const comparison = compareBenchmarkBaseline(baseline, actualBaseline);
    if (!comparison.ok) {
      logger.error(
        [
          `\n${requestedProfile} benchmark baseline regressed:`,
          ...comparison.regressions.map((regression) => `- ${regression}`),
        ].join("\n"),
      );
      return 1;
    }
    logger.log(`\n${requestedProfile} benchmark baseline comparison passed.`);
  }

  logger.log(`\n${requestedProfile} benchmark baseline completed.`);
  return 0;
};
