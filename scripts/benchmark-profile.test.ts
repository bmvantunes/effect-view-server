import { describe, expect, it } from "@effect/vitest";
import { join } from "node:path";
import { benchmarkProfileArtifactKind } from "./benchmark-profile-artifact.mjs";
import {
  requestedBenchmarkProfileName,
  resolveBenchmarkProfile,
} from "./benchmark-profile.mjs";
import { makeDirectory, makeTask } from "./benchmark-baseline-runner-test-support";

describe("benchmark profile resolution", () => {
  it("resolves a named profile to a deterministic workload and artifact contract", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);

    const profile = resolveBenchmarkProfile("tiny", new Map([["tiny", [task]]]));

    expect({
      artifact: profile.artifact,
      name: profile.name,
      taskLabels: profile.tasks.map((currentTask) => currentTask.label),
    }).toStrictEqual({
      artifact: {
        artifactKind: benchmarkProfileArtifactKind,
        path: join(directory, "profile-tiny.json"),
      },
      name: "tiny",
      taskLabels: ["task a"],
    });
  });

  it("selects the argument, environment, and default profile names deterministically", () => {
    expect({
      argument: requestedBenchmarkProfileName(["node", "script", "--profile=tiny"], {
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "ignored",
      }),
      environment: requestedBenchmarkProfileName(["node", "script"], {
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "tiny",
      }),
      fallback: requestedBenchmarkProfileName(["node", "script"], {}),
    }).toStrictEqual({
      argument: "tiny",
      environment: "tiny",
      fallback: "smoke",
    });
  });

  it("resolves the built-in smoke profile by default", () => {
    const profile = resolveBenchmarkProfile("smoke");

    expect({
      artifactKind: profile.artifact.artifactKind,
      name: profile.name,
      taskCount: profile.tasks.length,
    }).toStrictEqual({
      artifactKind: benchmarkProfileArtifactKind,
      name: "smoke",
      taskCount: 19,
    });
  });

  it("rejects unknown and empty profiles before execution", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([
      ["tiny", [task]],
      ["empty", []],
    ]);

    expect(() => resolveBenchmarkProfile("missing", profileMap)).toThrow(
      "Unknown benchmark profile: missing. Available profiles: tiny, empty.",
    );
    expect(() => resolveBenchmarkProfile("empty", profileMap)).toThrow(
      "Benchmark profile empty must contain at least one task.",
    );
  });
});
