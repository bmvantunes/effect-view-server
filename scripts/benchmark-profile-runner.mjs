import { fileSystemBenchmarkArtifactIo } from "./benchmark-artifact-io.mjs";
import {
  decodeBenchmarkTaskArtifacts,
  makeBenchmarkProfileArtifact,
  validateBenchmarkProfileArtifact,
} from "./benchmark-profile-artifact.mjs";
import { taskForRepeat } from "./benchmark-baseline-task-catalog.mjs";

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

const mergeTaskEnvironment = (parentEnvironment, taskEnvironment) => {
  const environment = {
    ...parentEnvironment,
    ...taskEnvironment,
  };
  const parentNodeOptions = parentEnvironment.NODE_OPTIONS;
  const taskNodeOptions = taskEnvironment.NODE_OPTIONS;
  if (parentNodeOptions !== undefined && taskNodeOptions !== undefined) {
    environment.NODE_OPTIONS = `${parentNodeOptions} ${taskNodeOptions}`;
  }
  return environment;
};

export const removeTaskArtifacts = (artifactIo, currentTask) => {
  artifactIo.remove(currentTask.outputJsonPath);
  artifactIo.remove(currentTask.summaryPath);
};

export const assertTaskArtifactsWritten = (artifactIo, currentTask) => {
  if (!artifactIo.exists(currentTask.outputJsonPath)) {
    throw new Error(`${currentTask.label}: missing benchmark output ${currentTask.outputJsonPath}.`);
  }
  if (!artifactIo.exists(currentTask.summaryPath)) {
    throw new Error(`${currentTask.label}: missing benchmark summary ${currentTask.summaryPath}.`);
  }
};

export const runBenchmarkProfile = async ({
  artifactIo = fileSystemBenchmarkArtifactIo,
  environment,
  logger,
  profile,
  repeatCount,
  runTask,
}) => {
  artifactIo.remove(profile.artifact.path);
  const parentEnvironment = cleanBenchmarkEnvironment(environment);
  const observations = [];
  const totalTaskRuns = profile.tasks.length * repeatCount;

  for (let repeatIndex = 0; repeatIndex < repeatCount; repeatIndex += 1) {
    for (const [index, baseTask] of profile.tasks.entries()) {
      const taskNumber = repeatIndex * profile.tasks.length + index + 1;
      const currentTask = taskForRepeat(baseTask, repeatIndex, repeatCount);
      const startedAt = process.hrtime.bigint();
      logger.log(`\n[${taskNumber}/${totalTaskRuns}] ${currentTask.label}`);
      removeTaskArtifacts(artifactIo, currentTask);
      const exitCode = await runTask({
        ...currentTask,
        env: mergeTaskEnvironment(parentEnvironment, currentTask.env),
      });
      if (exitCode !== 0) {
        return {
          artifact: undefined,
          artifactPath: profile.artifact.path,
          exitCode,
        };
      }
      assertTaskArtifactsWritten(artifactIo, currentTask);
      observations.push(
        decodeBenchmarkTaskArtifacts(
          currentTask,
          artifactIo.readJson(currentTask.summaryPath),
          artifactIo.readJson(currentTask.outputJsonPath),
        ),
      );
      const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
      logger.log(`[${taskNumber}/${totalTaskRuns}] completed in ${elapsedMs.toFixed(0)}ms`);
    }
  }

  const artifact = makeBenchmarkProfileArtifact(profile.name, observations);
  artifactIo.writeJson(profile.artifact.path, artifact);
  const persistedArtifact = validateBenchmarkProfileArtifact(
    artifactIo.readJson(profile.artifact.path),
    profile.artifact.path,
  );
  return {
    artifact: persistedArtifact,
    artifactPath: profile.artifact.path,
    exitCode: 0,
  };
};
