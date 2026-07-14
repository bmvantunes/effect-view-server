import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const vitestOutput = {
  files: [
    {
      groups: [
        {
          fullName: "src/runner-example.bench.ts > runner example benchmark group",
          benchmarks: [
            {
              max: 3,
              mean: 2,
              min: 1,
              name: "case a",
              p99: 3,
              sampleCount: 7,
            },
          ],
        },
      ],
    },
  ],
};

export const summary = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "runner example benchmark",
  benchmarkScope: "engine-runner",
  cleanupLeakCount: 0,
  latency: {
    outputJsonPath: "actual.json",
    source: "vitest-output-json",
  },
  memory: {
    totalDelta: {
      rssBytes: 1024,
    },
  },
  mutationCount: 100,
  queuedEventCount: 0,
  rowCount: 100,
  subscriberCount: 1,
  topics: ["orders"],
};

export const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "runner example benchmark",
  benchmarkScope: "engine-runner",
  benchmarks: [
    {
      groupName: "src/runner-example.bench.ts > runner example benchmark group",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "case a",
      p99Ms: 3,
      sampleCount: 7,
    },
  ],
  browser: undefined,
  cleanupLeakCount: 0,
  groupedWriteAdmission: undefined,
  kafkaIngestLanes: undefined,
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  topics: ["orders"],
};

export const mixedSamplingPolicy = {
  iterationBoundCases: [
    {
      name: "live case",
      sampleCount: 5,
      timeMs: 0,
      warmupIterations: 0,
      warmupTimeMs: 0,
    },
  ],
  memoryRssMetric: "process-peak-over-initial-current",
  measured: {
    minimumSampleCount: 200,
    timeMs: 250,
    warmupIterations: 5,
    warmupTimeMs: 100,
  },
};

export const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-benchmark-runner-"));

export const makeTask = (directory: string) => ({
  args: ["run", "--no-cache", "fake#bench"],
  command: "vp",
  env: {
    VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
  },
  expectedArtifactKind: "engine-benchmark-summary",
  expectedBenchmarkScope: "engine-runner",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath: join(directory, "actual.json"),
  packageOutputJsonPath: "actual.json",
  summaryPath: join(directory, "actual.summary.json"),
});

export const writeArtifacts = (
  task: ReturnType<typeof makeTask>,
  nextSummary = summary,
  nextVitestOutput = vitestOutput,
) => {
  const summaryForTask = {
    ...nextSummary,
    latency: {
      ...nextSummary.latency,
      outputJsonPath: task.packageOutputJsonPath,
    },
    memory:
      "samplingPolicy" in nextSummary && nextSummary.samplingPolicy !== undefined
        ? {
            ...nextSummary.memory,
            before: {
              rssBytes: 512,
            },
            processPeakRss: {
              afterBenchmarkBytes: 1536,
              afterSetupBytes: 1024,
              beforeBytes: 768,
              benchmarkDeltaBytes: 512,
              setupDeltaBytes: 512,
              totalDeltaBytes: 1024,
            },
          }
        : nextSummary.memory,
  };
  mkdirSync(join(task.outputJsonPath, ".."), { recursive: true });
  writeFileSync(task.outputJsonPath, `${JSON.stringify(nextVitestOutput)}\n`);
  writeFileSync(task.summaryPath, `${JSON.stringify(summaryForTask)}\n`);
};

export const silentLogger = () => {
  const messages: Array<string> = [];
  return {
    logger: {
      error: (message: string) => {
        messages.push(message);
      },
      log: (message: string) => {
        messages.push(message);
      },
    },
    messages,
  };
};
