import { fileSystemBenchmarkArtifactIo } from "./benchmark-artifact-io.mjs";
import {
  buildBenchmarkBaseline,
  readBenchmarkBaseline,
  validateBenchmarkBaseline,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";
import {
  benchmarkComparisonPolicyForProfile,
  compareBenchmarkArtifacts,
} from "./benchmark-comparison-policy.mjs";
import {
  profiles,
  repeatableReportOnlyProfiles,
} from "./benchmark-baseline-profiles.mjs";
import {
  requestedBenchmarkProfileName,
  resolveBenchmarkProfile,
} from "./benchmark-profile.mjs";
import { runBenchmarkProfile } from "./benchmark-profile-runner.mjs";

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

const updateBaselineTaskArgument = "--update-baseline-task";
const updateBaselineTaskArgumentPrefix = `${updateBaselineTaskArgument}=`;
const scopedMutableTaskFields = new Set([
  "benchmarks",
  "measurementProtocol",
  "memoryRssTotalDeltaBytes",
  "minimumSampleCount",
  "runtimeOperationCases",
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
  artifactIo = fileSystemBenchmarkArtifactIo,
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
  const requestedProfile = requestedBenchmarkProfileName(argv, environment);
  const tasks = profileMap.get(requestedProfile);

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
  const resolvedProfile = resolveBenchmarkProfile(requestedProfile, profileMap);
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
  const execution = await runBenchmarkProfile({
    artifactIo,
    environment,
    logger,
    profile: {
      ...resolvedProfile,
      tasks: tasksToRun,
    },
    repeatCount,
    runTask,
  });
  if (execution.exitCode !== 0) {
    return execution.exitCode;
  }
  const actualBaseline = buildBenchmarkBaseline(requestedProfile, execution.artifact.tasks);

  if (updateBaseline) {
    const baselineToWrite =
      scopedBaseline === undefined
        ? actualBaseline
        : mergeSelectedBaselineTasks(scopedBaseline, actualBaseline);
    writeBenchmarkBaseline(profileBaselinePath, baselineToWrite);
    logger.log(`\nUpdated benchmark baseline: ${profileBaselinePath}`);
  } else if (compareBaseline) {
    const baseline = readBenchmarkBaseline(profileBaselinePath);
    const comparison = compareBenchmarkArtifacts({
      actual: validateBenchmarkBaseline(actualBaseline, "actual"),
      baseline,
      policy: benchmarkComparisonPolicyForProfile(requestedProfile),
    });
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
