import { describe, expect, it } from "@effect/vitest";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { strictLintOptions } from "../tools/vite/lint-policy";

const lintConfigPaths = [
  "vite.config.ts",
  "apps/example/vite.config.ts",
  "examples/vite.config.shared.ts",
  ...readdirSync("packages")
    .map((directory) => `packages/${directory}/vite.config.ts`)
    .filter(existsSync),
].sort();

describe("repository lint policy", () => {
  it("makes every warning fatal through one shared Vite+ interface", () => {
    expect(strictLintOptions).toStrictEqual({
      denyWarnings: true,
      typeAware: true,
      typeCheck: true,
    });
    expect(
      lintConfigPaths.map((path) => ({
        path,
        usesStrictLintOptions: readFileSync(path, "utf8").includes("options: strictLintOptions"),
      })),
    ).toStrictEqual(
      lintConfigPaths.map((path) => ({
        path,
        usesStrictLintOptions: true,
      })),
    );
  });

  it("hard-fails native React compiler and Hooks diagnostics", () => {
    const rootPackage = JSON.parse(readFileSync("package.json", "utf8"));

    expect(rootPackage.scripts["check:react-compiler"]).toBe(
      "vp exec oxlint --deny react/react-compiler --deny react/rules-of-hooks --deny react/exhaustive-deps --react-plugin --deny-warnings examples",
    );
  });
});
