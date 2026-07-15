import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname } from "node:path";

export const makeBenchmarkArtifactIo = ({
  exists,
  makeDirectory,
  readText,
  removeFile,
  writeText,
}) => ({
  exists,
  readJson: (path) => {
    const text = readText(path);
    try {
      return JSON.parse(text);
    } catch {
      throw new Error(`Malformed benchmark artifact JSON at ${path}.`);
    }
  },
  remove: (path) => {
    removeFile(path);
  },
  writeJson: (path, value) => {
    makeDirectory(dirname(path));
    writeText(path, `${JSON.stringify(value, undefined, 2)}\n`);
  },
});

export const fileSystemBenchmarkArtifactIo = makeBenchmarkArtifactIo({
  exists: existsSync,
  makeDirectory: (path) => mkdirSync(path, { recursive: true }),
  readText: (path) => readFileSync(path, "utf8"),
  removeFile: (path) => rmSync(path, { force: true }),
  writeText: (path, value) => writeFileSync(path, value),
});
