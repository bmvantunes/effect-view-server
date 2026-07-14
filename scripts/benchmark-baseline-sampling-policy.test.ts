import { describe, expect, it } from "@effect/vitest";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildBenchmarkBaseline,
  compareBenchmarkBaseline,
  readBenchmarkObservation,
  validateBenchmarkBaseline,
} from "./benchmark-baseline.mjs";

const samplingPolicy = {
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

const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["read case", "live case"],
  benchmarkName: "sampling policy benchmark",
  benchmarkScope: "engine-raw-snapshot",
  benchmarks: [
    {
      groupName: "src/example.bench.ts > sampling policy benchmark",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "read case",
      p99Ms: 3,
      sampleCount: 200,
    },
    {
      groupName: "src/example.bench.ts > sampling policy benchmark",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "live case",
      p99Ms: 3,
      sampleCount: 5,
    },
  ],
  browser: undefined,
  cleanupLeakCount: 0,
  groupedKeyWidthParameters: undefined,
  groupedWriteAdmission: undefined,
  kafkaIngestLanes: undefined,
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  samplingPolicy,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  throughputCases: undefined,
  topics: ["orders"],
};

const writeSamplingArtifacts = (
  artifactSamplingPolicy: typeof samplingPolicy | undefined,
) => {
  const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-sampling-policy-"));
  const summaryPath = join(directory, "actual.summary.json");
  const outputJsonPath = join(directory, "actual.json");
  writeFileSync(
    summaryPath,
    `${JSON.stringify({
      artifactKind: "engine-benchmark-summary",
      backpressureCount: 0,
      benchmarkCases: ["read case", "live case"],
      benchmarkName: "sampling policy benchmark",
      benchmarkScope: "engine-raw-snapshot",
      cleanupLeakCount: 0,
      latency: {
        outputJsonPath: "actual.json",
        source: "vitest-output-json",
      },
      memory: {
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
        totalDelta: {
          rssBytes: 64,
        },
      },
      mutationCount: 100,
      queuedEventCount: 0,
      rowCount: 100,
      samplingPolicy: artifactSamplingPolicy,
      subscriberCount: 1,
      topics: ["orders"],
    })}\n`,
  );
  writeFileSync(
    outputJsonPath,
    `${JSON.stringify({
      files: [
        {
          groups: [
            {
              fullName: "src/example.bench.ts > sampling policy benchmark",
              benchmarks: observation.benchmarks.map((benchmark) => ({
                max: benchmark.maxMs,
                mean: benchmark.meanMs,
                min: benchmark.minMs,
                name: benchmark.name,
                p99: benchmark.p99Ms,
                sampleCount: benchmark.sampleCount,
              })),
            },
          ],
        },
      ],
    })}\n`,
  );
  return {
    expectedArtifactKind: "engine-benchmark-summary",
    expectedBenchmarkScope: "engine-raw-snapshot",
    expectedRowCount: 100,
    label: "task a",
    minimumSampleCount: 5,
    outputJsonPath,
    packageOutputJsonPath: "actual.json",
    summaryPath,
  };
};

describe("benchmark baseline sampling policy", () => {
  it("uses policy-owned peak RSS over the initial current RSS", () => {
    const paths = writeSamplingArtifacts(samplingPolicy);

    expect(readBenchmarkObservation({ ...paths, samplingPolicy }).memoryRssTotalDeltaBytes).toBe(
      1024,
    );
  });

  it("rejects missing and non-monotonic policy-owned peak RSS checkpoints", () => {
    const missingPaths = writeSamplingArtifacts(samplingPolicy);
    const missingSummary = JSON.parse(readFileSync(missingPaths.summaryPath, "utf8"));
    delete missingSummary.memory.processPeakRss;
    writeFileSync(missingPaths.summaryPath, `${JSON.stringify(missingSummary)}\n`);

    expect(() => readBenchmarkObservation({ ...missingPaths, samplingPolicy })).toThrow(
      `Benchmark artifact field ${missingPaths.summaryPath}.memory.processPeakRss is required for process-peak-over-initial-current RSS measurement.`,
    );

    const nonMonotonicPaths = writeSamplingArtifacts(samplingPolicy);
    const nonMonotonicSummary = JSON.parse(readFileSync(nonMonotonicPaths.summaryPath, "utf8"));
    nonMonotonicSummary.memory.processPeakRss.afterSetupBytes = 700;
    writeFileSync(nonMonotonicPaths.summaryPath, `${JSON.stringify(nonMonotonicSummary)}\n`);

    expect(() => readBenchmarkObservation({ ...nonMonotonicPaths, samplingPolicy })).toThrow(
      `Benchmark artifact field ${nonMonotonicPaths.summaryPath}.memory.processPeakRss checkpoints must be monotonic.`,
    );

    const initialCurrentAbovePeakPaths = writeSamplingArtifacts(samplingPolicy);
    const initialCurrentAbovePeakSummary = JSON.parse(
      readFileSync(initialCurrentAbovePeakPaths.summaryPath, "utf8"),
    );
    initialCurrentAbovePeakSummary.memory.processPeakRss.beforeBytes = 500;
    writeFileSync(
      initialCurrentAbovePeakPaths.summaryPath,
      `${JSON.stringify(initialCurrentAbovePeakSummary)}\n`,
    );

    expect(() =>
      readBenchmarkObservation({ ...initialCurrentAbovePeakPaths, samplingPolicy }),
    ).toThrow(
      `Benchmark artifact field ${initialCurrentAbovePeakPaths.summaryPath}.memory.processPeakRss checkpoints must be monotonic.`,
    );

    const finalBelowSetupPaths = writeSamplingArtifacts(samplingPolicy);
    const finalBelowSetupSummary = JSON.parse(
      readFileSync(finalBelowSetupPaths.summaryPath, "utf8"),
    );
    finalBelowSetupSummary.memory.processPeakRss.afterBenchmarkBytes = 900;
    writeFileSync(finalBelowSetupPaths.summaryPath, `${JSON.stringify(finalBelowSetupSummary)}\n`);

    expect(() => readBenchmarkObservation({ ...finalBelowSetupPaths, samplingPolicy })).toThrow(
      `Benchmark artifact field ${finalBelowSetupPaths.summaryPath}.memory.processPeakRss checkpoints must be monotonic.`,
    );

    const inconsistentDeltaPaths = writeSamplingArtifacts(samplingPolicy);
    const inconsistentDeltaSummary = JSON.parse(
      readFileSync(inconsistentDeltaPaths.summaryPath, "utf8"),
    );
    inconsistentDeltaSummary.memory.processPeakRss.totalDeltaBytes = 1023;
    writeFileSync(inconsistentDeltaPaths.summaryPath, `${JSON.stringify(inconsistentDeltaSummary)}\n`);

    expect(() => readBenchmarkObservation({ ...inconsistentDeltaPaths, samplingPolicy })).toThrow(
      `Benchmark artifact field ${inconsistentDeltaPaths.summaryPath}.memory.processPeakRss deltas must match its checkpoints.`,
    );

    const zeroDeltaPaths = writeSamplingArtifacts(samplingPolicy);
    const zeroDeltaSummary = JSON.parse(readFileSync(zeroDeltaPaths.summaryPath, "utf8"));
    zeroDeltaSummary.memory.before.rssBytes = 768;
    zeroDeltaSummary.memory.processPeakRss = {
      afterBenchmarkBytes: 768,
      afterSetupBytes: 768,
      beforeBytes: 768,
      benchmarkDeltaBytes: 0,
      setupDeltaBytes: 0,
      totalDeltaBytes: 0,
    };
    writeFileSync(zeroDeltaPaths.summaryPath, `${JSON.stringify(zeroDeltaSummary)}\n`);

    expect(() => readBenchmarkObservation({ ...zeroDeltaPaths, samplingPolicy })).toThrow(
      `Benchmark artifact field ${zeroDeltaPaths.summaryPath}.memory.processPeakRss.totalDeltaBytes must be positive.`,
    );
  });

  it("persists and validates mixed read and iteration-bound sampling policy", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);

    expect(validateBenchmarkBaseline(baseline)).toStrictEqual(baseline);
  });

  it("rejects a non-positive persisted process-peak RSS delta", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        memoryRssTotalDeltaBytes: 0,
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].memoryRssTotalDeltaBytes must be a positive integer.",
    );

    const { samplingPolicy: removedSamplingPolicy, ...legacyObservation } = observation;
    expect(removedSamplingPolicy).toStrictEqual(samplingPolicy);
    const legacyBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...legacyObservation,
        memoryRssTotalDeltaBytes: -1,
      },
    ]);
    expect(validateBenchmarkBaseline(legacyBaseline)).toStrictEqual(legacyBaseline);
  });

  it("rejects unknown persisted RSS measurement policies", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const unknownMetricBaseline = {
      ...baseline,
      tasks: [
        {
          ...observation,
          samplingPolicy: {
            ...samplingPolicy,
            memoryRssMetric: "endpoint-rss",
          },
        },
      ],
    };

    expect(() => validateBenchmarkBaseline(unknownMetricBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].samplingPolicy.memoryRssMetric must be process-peak-over-initial-current.",
    );
  });

  it("requires iteration-bound sampling to disable time and warmup", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const policyWith = (overrides: Record<string, number>) => ({
      ...baseline,
      tasks: [
        {
          ...observation,
          samplingPolicy: {
            ...samplingPolicy,
            iterationBoundCases: [
              {
                ...samplingPolicy.iterationBoundCases[0],
                ...overrides,
              },
            ],
          },
        },
      ],
    });
    const error =
      "Benchmark artifact field baseline.tasks[0].samplingPolicy.iterationBoundCases[0] must disable time and warmup for an iteration-bound case.";

    expect(() => validateBenchmarkBaseline(policyWith({ timeMs: 1 }))).toThrow(error);
    expect(() => validateBenchmarkBaseline(policyWith({ warmupIterations: 1 }))).toThrow(error);
    expect(() => validateBenchmarkBaseline(policyWith({ warmupTimeMs: 1 }))).toThrow(error);
  });

  it("accepts iteration-count-bounded release reads with zero measurement time", () => {
    const baseline = buildBenchmarkBaseline("release", [
      {
        ...observation,
        samplingPolicy: {
          ...samplingPolicy,
          measured: {
            ...samplingPolicy.measured,
            timeMs: 0,
          },
        },
      },
    ]);

    expect(validateBenchmarkBaseline(baseline)).toStrictEqual(baseline);
  });

  it("requires the task minimum to match the persisted sampling policy", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        minimumSampleCount: 4,
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "task a: minimumSampleCount must equal sampling policy minimum 5 but was 4.",
    );
  });

  it("reports sampling policy drift between baseline and actual observations", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        samplingPolicy: {
          ...samplingPolicy,
          measured: {
            ...samplingPolicy.measured,
            timeMs: 500,
          },
        },
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: [
        'task a: samplingPolicy changed from {"iterationBoundCases":[{"name":"live case","sampleCount":5,"timeMs":0,"warmupIterations":0,"warmupTimeMs":0}],"memoryRssMetric":"process-peak-over-initial-current","measured":{"minimumSampleCount":200,"timeMs":250,"warmupIterations":5,"warmupTimeMs":100}} to {"iterationBoundCases":[{"name":"live case","sampleCount":5,"timeMs":0,"warmupIterations":0,"warmupTimeMs":0}],"memoryRssMetric":"process-peak-over-initial-current","measured":{"minimumSampleCount":200,"timeMs":500,"warmupIterations":5,"warmupTimeMs":100}}.',
      ],
    });
  });

  it("rejects sampling policy drift between runner and benchmark summary", () => {
    const paths = writeSamplingArtifacts({
      ...samplingPolicy,
      measured: {
        ...samplingPolicy.measured,
        timeMs: 500,
      },
    });

    expect(() => readBenchmarkObservation({ ...paths, samplingPolicy })).toThrow(
      "task a: benchmark samplingPolicy did not match the runner policy.",
    );
  });

  it("rejects unexpected sampling policy from an unmarked benchmark", () => {
    const paths = writeSamplingArtifacts(samplingPolicy);

    expect(() => readBenchmarkObservation(paths)).toThrow(
      "task a: benchmark samplingPolicy did not match the runner policy.",
    );
  });

  it("rejects a missing artifact policy when the runner expects one", () => {
    const paths = writeSamplingArtifacts(undefined);

    expect(() => readBenchmarkObservation({ ...paths, samplingPolicy })).toThrow(
      "task a: benchmark samplingPolicy did not match the runner policy.",
    );
  });

  it("requires exact mutation counts for mixed sampling policies", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);
    const actual = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        mutationCount: observation.mutationCount + 1,
      },
    ]);

    expect(compareBenchmarkBaseline(baseline, actual)).toStrictEqual({
      ok: false,
      regressions: ["task a: mutationCount changed from 100 to 101."],
    });
  });
});
