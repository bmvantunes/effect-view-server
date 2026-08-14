import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import {
  compareReleaseTags,
  incrementReleaseVersion,
  internalPublishViolations,
  oidcPublishEnvironmentViolations,
  packageTagName,
  pendingPackageTagName,
  parseReleaseTag,
  publishedFileViolations,
  publishCommandArguments,
  publishDecision,
  releaseTypeFromChangesets,
  sanitizePublicPackageJson,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const trustedEnvironment = {
  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "token",
  ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.actions.githubusercontent.com",
  GITHUB_ACTIONS: "true",
  GITHUB_EVENT_NAME: "push",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "bmvantunes/effect-view-server",
};

const workspacePackages = [
  { name: "effect-view-server", private: false },
  { name: "@effect-view-server/client", private: true },
  { name: "@effect-view-server/runtime", private: true },
];

const publicPackageJson = {
  name: "effect-view-server",
  version: "1.2.3",
  description: "Typed Effect View Server.",
  keywords: ["effect", "view-server"],
  homepage: "https://github.com/bmvantunes/effect-view-server#readme",
  bugs: { url: "https://github.com/bmvantunes/effect-view-server/issues" },
  license: "MIT",
  repository: {
    type: "git",
    url: "git+https://github.com/bmvantunes/effect-view-server.git",
    directory: "packages/effect-view-server",
  },
  type: "module",
  sideEffects: false,
  exports: {
    ".": {
      types: "./dist/index.d.ts",
      import: "./dist/index.js",
    },
  },
  engines: { node: ">=26.0.0" },
  files: ["dist", "README.md"],
  publishConfig: { provenance: true },
  dependencies: {
    "@effect-view-server/client": "workspace:*",
    effect: "4.0.0-rc.109",
  },
  devDependencies: { "@effect-view-server/runtime": "workspace:*" },
  peerDependencies: { react: "19.2.8" },
  peerDependenciesMeta: { react: { optional: true } },
};

describe("release publish policy", () => {
  it("refuses the placeholder version before trusted publishing", () => {
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "0.0.0",
        workspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message: "Refusing to publish placeholder version effect-view-server@0.0.0.",
    });
  });

  it("refuses untrusted branches and repositories", () => {
    expect(
      publishDecision({
        env: { ...trustedEnvironment, GITHUB_REF: "refs/heads/feature/release" },
        version: "1.2.3",
        workspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message: "Refusing npm publish outside the trusted main-branch GitHub Actions context.",
    });
  });

  it("refuses public internal workspace packages", () => {
    const unsafeWorkspacePackages = [
      ...workspacePackages,
      { name: "@effect-view-server/server", private: false },
    ];

    expect(internalPublishViolations(unsafeWorkspacePackages)).toStrictEqual([
      "@effect-view-server/server",
    ]);
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages: unsafeWorkspacePackages,
      }),
    ).toStrictEqual({
      _tag: "Refuse",
        message: "Refusing to publish because @effect-view-server/server is not private.",
      });

    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages: [
          ...unsafeWorkspacePackages,
          { name: "@effect-view-server/other", private: false },
        ],
      }),
    ).toStrictEqual({
      _tag: "Refuse",
      message:
        "Refusing to publish because @effect-view-server/server, @effect-view-server/other are not private.",
    });
  });

  it("allows direct publishing from the trusted main push", () => {
    expect(
      publishDecision({
        env: trustedEnvironment,
        version: "1.2.3",
        workspacePackages,
      }),
    ).toStrictEqual({ _tag: "Publish" });
  });

  it("requires the GitHub Actions OIDC variables", () => {
    expect(oidcPublishEnvironmentViolations({})).toStrictEqual([
      "ACTIONS_ID_TOKEN_REQUEST_URL is required for npm trusted publishing.",
      "ACTIONS_ID_TOKEN_REQUEST_TOKEN is required for npm trusted publishing.",
    ]);
    expect(oidcPublishEnvironmentViolations(trustedEnvironment)).toStrictEqual([]);
  });

  it("derives the strongest release type from changeset contents", () => {
    expect(
      releaseTypeFromChangesets([
        '---\n"effect-view-server": patch\n---',
        '---\n"effect-view-server": minor\n---',
        '---\n"effect-view-server": major\n---',
      ]),
    ).toStrictEqual("major");
    expect(releaseTypeFromChangesets(["docs only"])).toStrictEqual("patch");
  });

  it("increments stable versions according to the release type", () => {
    expect(incrementReleaseVersion("0.0.6", "patch")).toStrictEqual("0.0.7");
    expect(incrementReleaseVersion("0.0.6", "minor")).toStrictEqual("0.1.0");
    expect(incrementReleaseVersion("0.0.6", "major")).toStrictEqual("1.0.0");
    expect(() => incrementReleaseVersion("0.0.6-beta.1", "patch")).toThrowError(
      "Cannot increment invalid stable release version 0.0.6-beta.1.",
    );
  });

  it("parses release tags and prefers a public tag over a staged marker", () => {
    const staged = {
      pending: false,
      staged: true,
      tag: "effect-view-server@0.0.6-staged",
      version: "0.0.6",
      major: 0,
      minor: 0,
      patch: 6,
    };
    const published = {
      pending: false,
      staged: false,
      tag: "effect-view-server@0.0.6",
      version: "0.0.6",
      major: 0,
      minor: 0,
      patch: 6,
    };
    expect(staged).toStrictEqual({
      pending: false,
      staged: true,
      tag: "effect-view-server@0.0.6-staged",
      version: "0.0.6",
      major: 0,
      minor: 0,
      patch: 6,
    });
    expect(compareReleaseTags(published, staged)).toBeGreaterThan(0);
    expect(compareReleaseTags(staged, published)).toBeLessThan(0);
    expect(parseReleaseTag("effect-view-server@0.0.6-staged")).toStrictEqual(staged);
    expect(parseReleaseTag("effect-view-server@0.0.6")).toStrictEqual(published);
    expect(parseReleaseTag("effect-view-server@not-semver")).toBeUndefined();
    expect(parseReleaseTag("effect-view-server@0.0.7-pending")).toStrictEqual({
      pending: true,
      staged: false,
      tag: "effect-view-server@0.0.7-pending",
      version: "0.0.7",
      major: 0,
      minor: 0,
      patch: 7,
    });
    expect(
      compareReleaseTags(
        published,
        {
          pending: true,
          staged: false,
          tag: "effect-view-server@0.0.6-pending",
          version: "0.0.6",
          major: 0,
          minor: 0,
          patch: 6,
        },
      ),
    ).toBeGreaterThan(0);
    expect(parseReleaseTag("unrelated")).toBeUndefined();
  });

  it("orders release tags by semver before marker kind and tag name", () => {
    const tag = (major: number, minor: number, patch: number, staged = false, name = "tag") => ({
      major,
      minor,
      patch,
      staged,
      tag: name,
    });

    expect(compareReleaseTags(tag(1, 0, 0), tag(0, 9, 9))).toBeGreaterThan(0);
    expect(compareReleaseTags(tag(1, 1, 0), tag(1, 0, 9))).toBeGreaterThan(0);
    expect(compareReleaseTags(tag(1, 1, 1), tag(1, 1, 0))).toBeGreaterThan(0);
    expect(compareReleaseTags(tag(1, 1, 1, false), tag(1, 1, 1, true))).toBeGreaterThan(0);
    expect(compareReleaseTags(tag(1, 1, 1, false, "z"), tag(1, 1, 1, false, "a"))).toBeGreaterThan(0);
  });

  it("constructs a direct npm publish command", () => {
    expect(publishCommandArguments("/tmp/effect-view-server")).toStrictEqual([
      "publish",
      "/tmp/effect-view-server",
      "--provenance",
      "--access",
      "public",
    ]);
    expect(packageTagName("1.2.3")).toStrictEqual("effect-view-server@1.2.3");
    expect(pendingPackageTagName("1.2.3")).toStrictEqual("effect-view-server@1.2.3-pending");
  });

  it("sanitizes the public package manifest", () => {
    expect(sanitizePublicPackageJson(publicPackageJson)).toStrictEqual({
      name: "effect-view-server",
      version: "1.2.3",
      description: "Typed Effect View Server.",
      keywords: ["effect", "view-server"],
      homepage: "https://github.com/bmvantunes/effect-view-server#readme",
      bugs: { url: "https://github.com/bmvantunes/effect-view-server/issues" },
      license: "MIT",
      repository: {
        type: "git",
        url: "git+https://github.com/bmvantunes/effect-view-server.git",
        directory: "packages/effect-view-server",
      },
      type: "module",
      sideEffects: false,
      exports: {
        ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
      },
      engines: { node: ">=26.0.0" },
      files: ["dist", "README.md"],
      publishConfig: { access: "public", provenance: true },
      dependencies: { effect: "4.0.0-rc.109" },
      peerDependencies: { react: "19.2.8" },
      peerDependenciesMeta: { react: { optional: true } },
    });
  });

  it("supplies public provenance when the source omits publishConfig", () => {
    const withoutPublishConfig = Object.fromEntries(
      Object.entries(publicPackageJson).filter(([name]) => name !== "publishConfig"),
    );

    expect(sanitizePublicPackageJson(withoutPublishConfig)).toStrictEqual({
      ...sanitizePublicPackageJson(publicPackageJson),
      publishConfig: { access: "public", provenance: true },
    });
  });

  it("accepts clean files and strips source map references", () => {
    expect(
      publishedFileViolations([
        { relativePath: "dist/client.js", contents: "export const ok = true;" },
        { relativePath: "README.md", contents: "# Public package\n" },
      ]),
    ).toStrictEqual([]);
    expect(stripSourceMapReference("export const ok = 1;\n//# sourceMappingURL=ok.js.map\n"))
      .toStrictEqual("export const ok = 1;\n");
  });

  it("rejects source maps and private workspace references", () => {
    expect(
      publishedFileViolations([
        { relativePath: "dist/client.js.map", contents: "{}" },
        { relativePath: "dist/client.js", contents: "//# sourceMappingURL=client.js.map" },
        { relativePath: "package.json", contents: '"@effect-view-server/client":"0.0.0"' },
        {
          relativePath: "dist/client.d.ts",
          contents: 'import type { Client } from "@effect-view-server/client";',
        },
      ]),
    ).toStrictEqual([
      "dist/client.js.map is a source map",
      "dist/client.js references a source map",
      "package.json references @effect-view-server/",
      "dist/client.d.ts references @effect-view-server/",
    ]);
  });

  it("keeps the workflow on direct publish and out of staged-only mode", () => {
    const releaseWorkflow = readFileSync(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");

    expect(releaseWorkflow).toContain("publish:");
    expect(releaseWorkflow).toContain("id-token: write");
    expect(releaseWorkflow).toContain("node scripts/release-publish.mjs");
    expect(releaseWorkflow).not.toContain("npm stage publish");
    expect(releaseWorkflow).not.toContain("NPM_TOKEN");
  });
});
