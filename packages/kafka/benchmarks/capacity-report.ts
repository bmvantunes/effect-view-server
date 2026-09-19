import { mkdtempSync, readFileSync, writeFileSync, symlinkSync, renameSync, rmSync } from "node:fs";
import { basename, resolve } from "node:path";

// A single symlink rename publishes the complete set on POSIX filesystems.
// Retaining prior generations also keeps already-open report paths readable.
export function publishCapacityReport(
  output: string,
  json: string,
  markdown: string,
  compare: boolean,
): void {
  const baseline = compare
    ? readFileSync(resolve(output, "current", "benchmark-baseline.json"), "utf8")
    : json;
  const generation = mkdtempSync(resolve(output, ".report-"));
  const pointer = `${generation}.link`;
  let committed = false;
  try {
    writeFileSync(resolve(generation, "benchmark.json"), json);
    writeFileSync(resolve(generation, "benchmark.md"), markdown);
    writeFileSync(resolve(generation, "benchmark-baseline.json"), baseline);
    symlinkSync(basename(generation), pointer, "dir");
    renameSync(pointer, resolve(output, "current"));
    committed = true;
  } finally {
    rmSync(pointer, { force: true });
    if (!committed) rmSync(generation, { recursive: true, force: true });
  }
}
