import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));

export default function buildRepositoryTestArtifacts(): void {
  if (process.env.VIEW_SERVER_REPOSITORY_TEST_ARTIFACTS_READY === "1") {
    return;
  }

  execFileSync("vp", ["run", "--concurrency-limit", "1", "-w", "build:effect-declarations"], {
    cwd: repositoryRoot,
    stdio: "inherit",
  });
}
