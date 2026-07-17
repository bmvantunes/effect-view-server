import { describe, expect, it } from "@effect/vitest";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const exampleDirectories = [
  "combined-sources-react",
  "grpc-leased-react",
  "grpc-materialized-react",
  "in-memory-react",
  "kafka-react",
  "ssr-react",
  "tcp-publisher-react",
];
const routeTreePaths = exampleDirectories.map(
  (directory) => `examples/${directory}/src/routeTree.gen.ts`,
);
const readRouteTrees = () => routeTreePaths.map((path) => readFileSync(path, "utf8"));

describe("example route cleanup policy", () => {
  it("keeps cleanup local to each independently runnable example", () => {
    expect(
      exampleDirectories.map((directory) => {
        const examplePackage = JSON.parse(
          readFileSync(`examples/${directory}/package.json`, "utf8"),
        );
        return {
          cleanRoutes: examplePackage.scripts["clean-routes"],
          directory,
        };
      }),
    ).toStrictEqual(
      exampleDirectories.map((directory) => ({
        cleanRoutes:
          "node ../../scripts/clean-tanstack-route-tree.mjs src/routeTree.gen.ts && vp check --fix src/routeTree.gen.ts",
        directory,
      })),
    );
  });

  it(
    "keeps Vite+ checks from constructing route-generating plugins",
    () => {
      const before = readRouteTrees();
      for (const directory of exampleDirectories) {
        execFileSync("vp", ["check", "vite.config.ts"], {
          cwd: `examples/${directory}`,
          stdio: "pipe",
        });
      }
      const after = readRouteTrees();

      expect(after).toStrictEqual(before);
    },
    30_000,
  );

  it(
    "keeps real Vitest config loading from regenerating route trees",
    () => {
      const before = readRouteTrees();
      execFileSync(
        "vp",
        [
          "test",
          "run",
          "src/index.test-d.ts",
          "--browser.enabled=false",
          "--typecheck",
        ],
        {
          cwd: "examples/grpc-materialized-react",
          stdio: "pipe",
        },
      );
      const after = readRouteTrees();

      expect(after).toStrictEqual(before);
    },
    30_000,
  );

});
