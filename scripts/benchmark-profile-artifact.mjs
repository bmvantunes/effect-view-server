import {
  exactObjectValue,
  nonEmptyArrayValue,
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";
import {
  decodeBenchmarkObservation,
  validateBenchmarkObservation,
} from "./benchmark-baseline.mjs";

export const benchmarkProfileArtifactKind = "view-server-benchmark-profile-run";

export const validateBenchmarkProfileArtifact = (artifact, path = "artifact") => {
  const artifactObject = exactObjectValue(artifact, path, [
    "artifactKind",
    "profile",
    "tasks",
  ]);
  if (artifactObject.artifactKind !== benchmarkProfileArtifactKind) {
    throw new Error(
      `Benchmark artifact field ${path}.artifactKind must equal ${benchmarkProfileArtifactKind}.`,
    );
  }
  return {
    artifactKind: benchmarkProfileArtifactKind,
    profile: stringValue(artifactObject.profile, `${path}.profile`),
    tasks: nonEmptyArrayValue(artifactObject.tasks, `${path}.tasks`).map((task, index) =>
      validateBenchmarkObservation(task, `${path}.tasks[${index}]`),
    ),
  };
};

export const makeBenchmarkProfileArtifact = (profile, tasks) =>
  validateBenchmarkProfileArtifact({
    artifactKind: benchmarkProfileArtifactKind,
    profile,
    tasks,
  });

export const decodeBenchmarkTaskArtifacts = (task, summaryArtifact, vitestOutput) =>
  decodeBenchmarkObservation(task, summaryArtifact, vitestOutput);
