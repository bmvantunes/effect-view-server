import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";

describe("package export script", () => {
  it("builds package dist before resolving public exports", () => {
    const packageJson = JSON.parse(readFileSync("package.json", "utf8"));

    expect(packageJson.scripts["check:package-exports"]).toStrictEqual(
      [
        "vp run -r build",
        "tsc --ignoreConfig --noEmit --strict --skipLibCheck --module preserve --moduleResolution bundler scripts/node-ambient.d.ts scripts/check-package-exports.ts",
        "node --experimental-strip-types scripts/check-package-exports.ts",
      ].join(" && "),
    );
  });
});
