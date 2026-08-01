import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "@effect/vitest";

const gitEnvironment = {
  ...process.env,
  GIT_ALLOW_PROTOCOL: "file",
};

const git = (cwd: string, args: ReadonlyArray<string>) =>
  execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    env: gitEnvironment,
  }).trim();

const initializeRepository = (directory: string) => {
  git(directory, ["config", "commit.gpgsign", "false"]);
  git(directory, ["config", "user.email", "effect-view-server@example.com"]);
  git(directory, ["config", "user.name", "Effect View Server"]);
};

describe("Effect upgrade command", () => {
  it("initializes the Effect submodule in a fresh clone", () => {
    const rootDirectory = mkdtempSync(join(tmpdir(), "effect-view-server-upgrade-effect-"));
    const effectRemote = join(rootDirectory, "effect.git");
    const effectSeed = join(rootDirectory, "effect-seed");
    const projectRemote = join(rootDirectory, "project.git");
    const projectSeed = join(rootDirectory, "project-seed");
    const checkout = join(rootDirectory, "checkout");

    git(rootDirectory, ["init", "--bare", "--initial-branch=main", effectRemote]);
    git(rootDirectory, ["init", "--initial-branch=main", effectSeed]);
    initializeRepository(effectSeed);
    writeFileSync(join(effectSeed, "README.md"), "# Effect fixture\n");
    git(effectSeed, ["add", "README.md"]);
    git(effectSeed, ["commit", "-m", "Initial Effect fixture"]);
    git(effectSeed, ["remote", "add", "origin", effectRemote]);
    git(effectSeed, ["push", "-u", "origin", "main"]);

    git(rootDirectory, ["init", "--bare", "--initial-branch=main", projectRemote]);
    git(rootDirectory, ["init", "--initial-branch=main", projectSeed]);
    initializeRepository(projectSeed);
    git(projectSeed, ["submodule", "add", effectRemote, ".repos/effect"]);
    git(projectSeed, ["commit", "-m", "Add Effect submodule"]);
    git(projectSeed, ["remote", "add", "origin", projectRemote]);
    git(projectSeed, ["push", "-u", "origin", "main"]);

    git(rootDirectory, ["clone", "--no-recurse-submodules", projectRemote, checkout]);
    expect(git(checkout, ["submodule", "status", "--", ".repos/effect"]).startsWith("-"))
      .toBe(true);

    const packageJson = JSON.parse(readFileSync("package.json", "utf8"));
    execFileSync("sh", ["-c", packageJson.scripts["upgrade-effect"]], {
      cwd: checkout,
      env: gitEnvironment,
      stdio: "pipe",
    });

    expect(git(checkout, ["submodule", "status", "--", ".repos/effect"]).startsWith("-"))
      .toBe(false);
    expect(
      realpathSync(git(join(checkout, ".repos/effect"), ["rev-parse", "--show-toplevel"])),
    ).toBe(realpathSync(join(checkout, ".repos/effect")));
    expect(git(checkout, ["symbolic-ref", "--short", "HEAD"])).toBe("main");

    rmSync(rootDirectory, { force: true, recursive: true });
  });
});
