import { existsSync, readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "@effect/vitest";

const repositoryRoot = new URL("../", import.meta.url);
const readRepositoryFile = (relativePath: string) =>
  readFileSync(new URL(relativePath, repositoryRoot), "utf8");
const repositoryPathExists = (relativePath: string) =>
  existsSync(new URL(relativePath, repositoryRoot));
const repositoryDirectoryIsAbsentOrEmpty = (relativePath: string) => {
  const directory = new URL(relativePath, repositoryRoot);
  return !existsSync(directory) || readdirSync(directory).length === 0;
};

describe("installed tooling skill policy", () => {
  it("uses installed packages instead of copied manuals or an Effect submodule", () => {
    const agentInstructions = readRepositoryFile("AGENTS.md");
    const effectSkill = readRepositoryFile(".agents/skills/effect-ts/SKILL.md");
    const viteSkill = readRepositoryFile(".agents/skills/vite/SKILL.md");
    const vitestSkill = readRepositoryFile(".agents/skills/vitest/SKILL.md");
    const rootPackage = JSON.parse(readRepositoryFile("package.json"));
    const gitmodules = repositoryPathExists(".gitmodules")
      ? readRepositoryFile(".gitmodules")
      : "";

    expect({
      effectUsesInstalledGuide: effectSkill.includes("node_modules/effect/AGENTS.md"),
      effectUsesInstalledSource: effectSkill.includes("node_modules/effect/src"),
      effectAvoidsVendoredCheckout: !effectSkill.includes(".repos/effect"),
      installedEffectGuideExists: repositoryPathExists("node_modules/effect/AGENTS.md"),
      installedEffectSourceExists: repositoryPathExists("node_modules/effect/src"),
      viteUsesInstalledGuide: viteSkill.includes("node_modules/vite-plus/AGENTS.md"),
      viteUsesInstalledDocs: viteSkill.includes("node_modules/vite-plus/docs"),
      installedVitePlusGuideExists: repositoryPathExists("node_modules/vite-plus/AGENTS.md"),
      installedVitePlusTestGuideExists: repositoryPathExists(
        "node_modules/vite-plus/docs/guide/test.md",
      ),
      installedVitePlusTestConfigExists: repositoryPathExists(
        "node_modules/vite-plus/docs/config/test.md",
      ),
      vitestUsesInstalledPackage: vitestSkill.includes("node_modules/vitest"),
      installedVitestPackageExists: repositoryPathExists("node_modules/vitest"),
      installedEffectVitestPackageExists: repositoryPathExists("node_modules/@effect/vitest"),
      repositoryAvoidsVendoredCheckout: !agentInstructions.includes(".repos/effect"),
      repositoryHasNoEffectUpgradeCommand: rootPackage.scripts["upgrade-effect"] === undefined,
      repositoryHasNoEffectSubmodule: !gitmodules.includes(".repos/effect"),
      repositoryHasNoVendoredEffectCheckout: !repositoryPathExists(".repos/effect"),
      copiedEffectManualsStayAbsent: repositoryDirectoryIsAbsentOrEmpty(
        ".agents/skills/effect-ts/references",
      ),
      copiedViteManualsStayAbsent:
        repositoryDirectoryIsAbsentOrEmpty(".agents/skills/vite/references") &&
        !repositoryPathExists(".agents/skills/vite/GENERATION.md"),
      copiedVitestManualsStayAbsent:
        repositoryDirectoryIsAbsentOrEmpty(".agents/skills/vitest/references") &&
        !repositoryPathExists(".agents/skills/vitest/GENERATION.md"),
    }).toStrictEqual({
      effectUsesInstalledGuide: true,
      effectUsesInstalledSource: true,
      effectAvoidsVendoredCheckout: true,
      installedEffectGuideExists: true,
      installedEffectSourceExists: true,
      viteUsesInstalledGuide: true,
      viteUsesInstalledDocs: true,
      installedVitePlusGuideExists: true,
      installedVitePlusTestGuideExists: true,
      installedVitePlusTestConfigExists: true,
      vitestUsesInstalledPackage: true,
      installedVitestPackageExists: true,
      installedEffectVitestPackageExists: true,
      repositoryAvoidsVendoredCheckout: true,
      repositoryHasNoEffectUpgradeCommand: true,
      repositoryHasNoEffectSubmodule: true,
      repositoryHasNoVendoredEffectCheckout: true,
      copiedEffectManualsStayAbsent: true,
      copiedViteManualsStayAbsent: true,
      copiedVitestManualsStayAbsent: true,
    });
  });
});
