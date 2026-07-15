import { describe, expect, it } from "@effect/vitest";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileSystemBenchmarkArtifactIo } from "./benchmark-artifact-io.mjs";
import { makeDirectory } from "./benchmark-baseline-runner-test-support";

describe("benchmark artifact file I/O", () => {
  it("roundtrips and removes JSON artifacts through the replaceable I/O Interface", () => {
    const directory = makeDirectory();
    const path = join(directory, "nested", "artifact.json");
    const artifact = { artifactKind: "example", profile: "tiny" };

    fileSystemBenchmarkArtifactIo.writeJson(path, artifact);
    const persisted = fileSystemBenchmarkArtifactIo.readJson(path);
    const existedBeforeRemove = fileSystemBenchmarkArtifactIo.exists(path);
    fileSystemBenchmarkArtifactIo.remove(path);

    expect({
      existedAfterRemove: fileSystemBenchmarkArtifactIo.exists(path),
      existedBeforeRemove,
      persisted,
    }).toStrictEqual({
      existedAfterRemove: false,
      existedBeforeRemove: true,
      persisted: artifact,
    });
  });

  it("reports malformed persisted JSON with its artifact path", () => {
    const directory = makeDirectory();
    const path = join(directory, "malformed.json");
    writeFileSync(path, "{not-json}\n");

    expect(() => fileSystemBenchmarkArtifactIo.readJson(path)).toThrow(
      `Malformed benchmark artifact JSON at ${path}.`,
    );
  });
});
