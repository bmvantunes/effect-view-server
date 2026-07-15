import { describe, expect, it } from "@effect/vitest";
import {
  benchmarkProfileArtifactKind,
  decodeBenchmarkTaskArtifacts,
  makeBenchmarkProfileArtifact,
  validateBenchmarkProfileArtifact,
} from "./benchmark-profile-artifact.mjs";
import {
  makeDirectory,
  makeTask,
  observation,
  summary,
  vitestOutput,
} from "./benchmark-baseline-runner-test-support";

describe("benchmark profile artifact codec", () => {
  it("builds a schema-valid, profile-identifiable artifact without comparison policy", () => {
    const artifact = makeBenchmarkProfileArtifact("tiny", [observation]);

    expect({
      artifactKind: artifact.artifactKind,
      keys: Object.keys(artifact),
      profile: artifact.profile,
      taskLabels: artifact.tasks.map((task) => task.taskLabel),
    }).toStrictEqual({
      artifactKind: benchmarkProfileArtifactKind,
      keys: ["artifactKind", "profile", "tasks"],
      profile: "tiny",
      taskLabels: ["task a"],
    });
  });

  it("decodes task output and summary values through the artifact codec", () => {
    const task = makeTask(makeDirectory());

    const decoded = decodeBenchmarkTaskArtifacts(
      task,
      {
        ...summary,
        latency: {
          ...summary.latency,
          outputJsonPath: task.packageOutputJsonPath,
        },
      },
      vitestOutput,
    );

    expect({
      benchmarkScope: decoded.benchmarkScope,
      outputJsonPath: decoded.outputJsonPath,
      taskLabel: decoded.taskLabel,
    }).toStrictEqual({
      benchmarkScope: "engine-runner",
      outputJsonPath: task.outputJsonPath,
      taskLabel: "task a",
    });
  });

  it("rejects malformed artifact identity, shape, and task observations", () => {
    expect(() =>
      validateBenchmarkProfileArtifact({
        artifactKind: "wrong-kind",
        profile: "tiny",
        tasks: [observation],
      }),
    ).toThrow(
      `Benchmark artifact field artifact.artifactKind must equal ${benchmarkProfileArtifactKind}.`,
    );
    expect(() =>
      validateBenchmarkProfileArtifact({
        artifactKind: benchmarkProfileArtifactKind,
        profile: "",
        tasks: [observation],
      }),
    ).toThrow("Benchmark artifact field artifact.profile must be a non-empty string.");
    expect(() =>
      validateBenchmarkProfileArtifact({
        artifactKind: benchmarkProfileArtifactKind,
        profile: "tiny",
        tasks: [],
      }),
    ).toThrow("Benchmark artifact field artifact.tasks must be a non-empty array.");
    expect(() =>
      validateBenchmarkProfileArtifact({
        artifactKind: benchmarkProfileArtifactKind,
        profile: "tiny",
        tasks: [{ ...observation, rowCount: "100" }],
      }),
    ).toThrow("Benchmark artifact field artifact.tasks[0].rowCount must be a finite number.");
    expect(() =>
      validateBenchmarkProfileArtifact({
        artifactKind: benchmarkProfileArtifactKind,
        profile: "tiny",
        tasks: [observation],
        thresholds: {},
      }),
    ).toThrow(
      "Benchmark artifact field artifact must contain exactly these keys: artifactKind, profile, tasks.",
    );
  });
});
