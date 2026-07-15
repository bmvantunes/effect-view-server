import { dirname, join } from "node:path";
import { benchmarkProfileArtifactKind } from "./benchmark-profile-artifact.mjs";
import { profiles } from "./benchmark-baseline-profiles.mjs";

export const requestedBenchmarkProfileName = (argv, environment) => {
  const profileArgument = argv.find((argument) => argument.startsWith("--profile="));
  return (
    profileArgument?.slice("--profile=".length) ??
    environment["VIEW_SERVER_BENCH_BASELINE_PROFILE"] ??
    "smoke"
  );
};

export const resolveBenchmarkProfile = (name, profileMap = profiles) => {
  const tasks = profileMap.get(name);
  if (tasks === undefined) {
    throw new Error(
      `Unknown benchmark profile: ${name}. Available profiles: ${[...profileMap.keys()].join(", ")}.`,
    );
  }
  if (tasks.length === 0) {
    throw new Error(`Benchmark profile ${name} must contain at least one task.`);
  }
  return {
    artifact: {
      artifactKind: benchmarkProfileArtifactKind,
      path: join(dirname(tasks[0].outputJsonPath), `profile-${name}.json`),
    },
    name,
    tasks,
  };
};
