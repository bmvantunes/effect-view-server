import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { readFileSync } from "node:fs";
import {
  activeFallbackGroupedViewCountFromEngineHealth,
  activeIncrementalGroupedViewCountFromEngineHealth,
  activeViewCountFromEngineHealth,
  backpressureCountFromEngineHealth,
  benchmarkOutputJsonPath,
  benchmarkSummaryPath,
  cleanupLeakCountFromEngineHealth,
  failOnBenchmarkCleanupLeaks,
  groupedFullEvaluationCountFromEngineHealth,
  groupedPatchedEvaluationCountFromEngineHealth,
  isBenchmarkEngineHealth,
  pendingMutationBatchCountFromEngineHealth,
  queuedEventCountFromEngineHealth,
  writeBenchmarkArtifact,
  type BenchmarkArtifactInput,
} from "./benchmark-artifact";
import { memoryDelta, type BenchmarkMemorySnapshot } from "./benchmark-memory-recorder";
import type { BenchmarkSamplingPolicy } from "./benchmark-sampling";

const memory = (value: number): BenchmarkMemorySnapshot => ({
  arrayBuffersBytes: value,
  externalBytes: value + 1,
  heapTotalBytes: value + 2,
  heapUsedBytes: value + 3,
  rssBytes: value + 4,
});

const samplingPolicy: BenchmarkSamplingPolicy = {
  iterationBoundCases: [],
  memoryRssMetric: "process-peak-over-initial-current",
  measured: {
    minimumSampleCount: 1_000,
    timeMs: 250,
    warmupIterations: 5,
    warmupTimeMs: 100,
  },
};

describe("benchmark artifact helpers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("builds benchmark artifact paths from env or fallback", () => {
    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", undefined);
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(
      ".artifacts/raw-snapshot-100rows.json",
    );
    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", " ");
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(
      ".artifacts/raw-snapshot-100rows.json",
    );

    vi.stubEnv("VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON", " .artifacts/custom.json ");
    expect(benchmarkOutputJsonPath("raw-snapshot-100rows.json")).toBe(".artifacts/custom.json");
  });

  it("builds summary paths for json and non-json outputs", () => {
    expect(benchmarkSummaryPath(".artifacts/raw.json")).toBe(".artifacts/raw.summary.json");
    expect(benchmarkSummaryPath(".artifacts/raw")).toBe(".artifacts/raw.summary.json");
  });

  it("computes memory and health counters", () => {
    const health = {
      activeSubscriptions: 2,
      backpressureEvents: 5,
      maxQueueDepth: 999,
      queuedEvents: 3,
      topics: {
        orders: {
          activeFallbackGroupedViews: 0,
          activeIncrementalGroupedViews: 0,
          activeViews: 7,
          groupedFullEvaluationCount: 0,
          groupedPatchedEvaluationCount: 0,
          pendingMutationBatches: 4,
        },
      },
    };
    expect(isBenchmarkEngineHealth(health)).toBe(true);
    expect(cleanupLeakCountFromEngineHealth(health)).toBe(12);
    expect(backpressureCountFromEngineHealth(health)).toBe(5);
    expect(queuedEventCountFromEngineHealth(health)).toBe(3);
    expect(activeViewCountFromEngineHealth(health)).toBe(7);
    expect(activeFallbackGroupedViewCountFromEngineHealth(health)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(health)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(health)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(health)).toBe(0);
    expect(pendingMutationBatchCountFromEngineHealth(health, ["orders"])).toBe(4);

    const healthWithGroupedDiagnostics = {
      activeSubscriptions: 0,
      backpressureEvents: 0,
      maxQueueDepth: 0,
      queuedEvents: 0,
      topics: {
        orders: {
          activeFallbackGroupedViews: 0,
          activeIncrementalGroupedViews: 1,
          activeViews: 1,
          groupedFullEvaluationCount: 2,
          groupedPatchedEvaluationCount: 3,
          pendingMutationBatches: 4,
        },
        trades: {
          activeFallbackGroupedViews: 1,
          activeIncrementalGroupedViews: 0,
          activeViews: 1,
          groupedFullEvaluationCount: 5,
          groupedPatchedEvaluationCount: 7,
          pendingMutationBatches: 6,
        },
      },
    };
    expect(isBenchmarkEngineHealth(healthWithGroupedDiagnostics)).toBe(true);
    expect(groupedFullEvaluationCountFromEngineHealth(healthWithGroupedDiagnostics)).toBe(7);
    expect(groupedPatchedEvaluationCountFromEngineHealth(healthWithGroupedDiagnostics)).toBe(10);
    expect(
      pendingMutationBatchCountFromEngineHealth(healthWithGroupedDiagnostics, ["orders"]),
    ).toBe(10);

    const minimalTopicHealth = {
      activeSubscriptions: 0,
      backpressureEvents: 0,
      maxQueueDepth: 0,
      queuedEvents: 0,
      topics: {
        orders: {
          activeViews: 1,
          pendingMutationBatches: 0,
        },
      },
    };
    expect(isBenchmarkEngineHealth(minimalTopicHealth)).toBe(true);
    expect(activeFallbackGroupedViewCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(minimalTopicHealth)).toBe(0);
    expect(pendingMutationBatchCountFromEngineHealth(minimalTopicHealth, ["orders"])).toBe(0);

    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 0,
        backpressureEvents: 0,
        maxQueueDepth: 0,
        queuedEvents: 0,
        topics: {
          orders: {
            activeViews: 1,
          },
        },
      }),
    ).toBe(false);

    const healthWithoutTopics = {
      activeSubscriptions: 2,
      backpressureEvents: 5,
      maxQueueDepth: 999,
      queuedEvents: 3,
    };
    expect(isBenchmarkEngineHealth(healthWithoutTopics)).toBe(true);
    expect(activeViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(activeFallbackGroupedViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(activeIncrementalGroupedViewCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(groupedFullEvaluationCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(groupedPatchedEvaluationCountFromEngineHealth(healthWithoutTopics)).toBe(0);
    expect(() =>
      pendingMutationBatchCountFromEngineHealth(healthWithoutTopics, ["orders"]),
    ).toThrow(
      "Benchmark engine health must include topic health to prove pending mutation batches are zero.",
    );
    expect(() => pendingMutationBatchCountFromEngineHealth(minimalTopicHealth, [])).toThrow(
      "Benchmark pending-mutation proof must name at least one expected topic.",
    );
    expect(() =>
      pendingMutationBatchCountFromEngineHealth(
        {
          ...minimalTopicHealth,
          topics: {},
        },
        ["orders"],
      ),
    ).toThrow(
      "Benchmark engine health must include expected topic orders to prove pending mutation batches are zero.",
    );
    const inheritedTopics = Object.setPrototypeOf(
      {},
      {
        orders: {
          activeViews: 0,
          pendingMutationBatches: 0,
        },
      },
    );
    expect(() =>
      pendingMutationBatchCountFromEngineHealth(
        {
          ...minimalTopicHealth,
          topics: inheritedTopics,
        },
        ["orders"],
      ),
    ).toThrow(
      "Benchmark engine health must include expected topic orders to prove pending mutation batches are zero.",
    );
    expect(() =>
      pendingMutationBatchCountFromEngineHealth(
        {
          ...minimalTopicHealth,
          topics: {
            trades: {
              activeViews: 0,
              pendingMutationBatches: 0,
            },
          },
        },
        ["orders"],
      ),
    ).toThrow(
      "Benchmark engine health must include expected topic orders to prove pending mutation batches are zero.",
    );
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: undefined,
      }),
    ).toBe(true);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: null,
      }),
    ).toBe(false);

    expect(isBenchmarkEngineHealth(null)).toBe(false);
    expect(isBenchmarkEngineHealth({ activeSubscriptions: 1 })).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: "2",
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeViews: 7,
            pendingMutationBatches: "1",
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: "7",
            pendingMutationBatches: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: null,
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            pendingMutationBatches: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: "0",
            activeIncrementalGroupedViews: 0,
            activeViews: 7,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            pendingMutationBatches: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
        topics: {
          orders: {
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: Number.NaN,
            activeViews: 7,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            pendingMutationBatches: 0,
          },
        },
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: Number.NaN,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: Number.POSITIVE_INFINITY,
        maxQueueDepth: 999,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: Number.NEGATIVE_INFINITY,
        queuedEvents: 3,
      }),
    ).toBe(false);
    expect(
      isBenchmarkEngineHealth({
        activeSubscriptions: 2,
        backpressureEvents: 5,
        maxQueueDepth: 999,
        queuedEvents: Number.NaN,
      }),
    ).toBe(false);
    expect(cleanupLeakCountFromEngineHealth({})).toBe(0);
    expect(backpressureCountFromEngineHealth({})).toBe(0);
    expect(queuedEventCountFromEngineHealth({})).toBe(0);
    expect(failOnBenchmarkCleanupLeaks(0)).toBeUndefined();
    expect(() => failOnBenchmarkCleanupLeaks(2)).toThrow(
      "Benchmark cleanup leaked 2 active resource(s).",
    );
  });

  it("writes benchmark summary artifacts", () => {
    const outputJsonPath = ".artifacts/benchmark-artifact-test.json";
    const artifactInput: BenchmarkArtifactInput = {
      artifactKind: "engine-benchmark-summary",
      backpressureCount: 0,
      benchmarkCases: ["case-a"],
      benchmarkName: "benchmark artifact test",
      benchmarkScope: "engine-raw-snapshot",
      cleanupLeakCount: 0,
      groupedWriteAdmission: {
        activeFallbackGroupedViewsAfterSetup: 0,
        activeFallbackGroupedViewsBeforeCleanup: 0,
        activeIncrementalGroupedViewsAfterSetup: 2,
        activeIncrementalGroupedViewsBeforeCleanup: 2,
        activeViewsAfterSetup: 2,
        activeViewsBeforeCleanup: 2,
        configuredMode: "incremental",
        expectedAdmission: "incremental",
        groupedFullEvaluationCountAfterSetup: 0,
        groupedFullEvaluationCountBeforeCleanup: 2,
        groupedPatchedEvaluationCountAfterSetup: 0,
        groupedPatchedEvaluationCountBeforeCleanup: 3,
        incrementalAdmissionLimits: {
          maxGroups: 10,
          maxMembers: 20,
          maxMembersPerGroup: 30,
          maxRetainedValueEntries: 40,
        },
        priceThreshold: 900,
        seedMutationCount: 100,
        timedMutationCount: 10,
        writeBatchSize: 32,
      },
      health: {
        status: "ready",
      },
      latency: {
        outputJsonPath,
        source: "vitest-output-json",
      },
      memoryAfterBenchmark: memory(5),
      memoryAfterSetup: memory(3),
      memoryBefore: memory(1),
      processPeakRss: {
        afterBenchmarkBytes: 40,
        afterSetupBytes: 30,
        beforeBytes: 20,
      },
      samplingPolicy,
      mutationCount: 10,
      notes: ["test artifact"],
      outputJsonPath,
      preCleanupHealth: {
        status: "ready",
      },
      queuedEventCount: 0,
      rowCount: 100,
      subscriberCount: 1,
      topics: ["orders"],
    };
    writeBenchmarkArtifact(artifactInput);

    expect(readFileSync(".artifacts/benchmark-artifact-test.summary.json", "utf8")).toBe(
      `${JSON.stringify(
        {
          artifactKind: "engine-benchmark-summary",
          backpressureCount: 0,
          benchmarkCases: ["case-a"],
          benchmarkName: "benchmark artifact test",
          benchmarkScope: "engine-raw-snapshot",
          cleanupLeakCount: 0,
          groupedWriteAdmission: {
            activeFallbackGroupedViewsAfterSetup: 0,
            activeFallbackGroupedViewsBeforeCleanup: 0,
            activeIncrementalGroupedViewsAfterSetup: 2,
            activeIncrementalGroupedViewsBeforeCleanup: 2,
            activeViewsAfterSetup: 2,
            activeViewsBeforeCleanup: 2,
            configuredMode: "incremental",
            expectedAdmission: "incremental",
            groupedFullEvaluationCountAfterSetup: 0,
            groupedFullEvaluationCountBeforeCleanup: 2,
            groupedPatchedEvaluationCountAfterSetup: 0,
            groupedPatchedEvaluationCountBeforeCleanup: 3,
            incrementalAdmissionLimits: {
              maxGroups: 10,
              maxMembers: 20,
              maxMembersPerGroup: 30,
              maxRetainedValueEntries: 40,
            },
            priceThreshold: 900,
            seedMutationCount: 100,
            timedMutationCount: 10,
            writeBatchSize: 32,
          },
          health: {
            status: "ready",
          },
          latency: {
            outputJsonPath,
            source: "vitest-output-json",
          },
          memory: {
            afterBenchmark: memory(5),
            afterSetup: memory(3),
            before: memory(1),
            benchmarkDelta: memoryDelta(memory(3), memory(5)),
            processPeakRss: {
              afterBenchmarkBytes: 40,
              afterSetupBytes: 30,
              beforeBytes: 20,
              benchmarkDeltaBytes: 10,
              setupDeltaBytes: 25,
              totalDeltaBytes: 35,
            },
            setupDelta: memoryDelta(memory(1), memory(3)),
            totalDelta: memoryDelta(memory(1), memory(5)),
          },
          mutationCount: 10,
          notes: ["test artifact"],
          outputJsonPath,
          preCleanupHealth: {
            status: "ready",
          },
          queuedEventCount: 0,
          rowCount: 100,
          samplingPolicy,
          subscriberCount: 1,
          topics: ["orders"],
        },
        undefined,
        2,
      )}\n`,
    );

    expect(() =>
      writeBenchmarkArtifact({
        ...artifactInput,
        processPeakRss: {
          afterBenchmarkBytes: 40,
          afterSetupBytes: 30,
          beforeBytes: 4,
        },
      }),
    ).toThrow("Process peak RSS checkpoints must be monotonic.");
    expect(() =>
      writeBenchmarkArtifact({
        ...artifactInput,
        processPeakRss: {
          afterBenchmarkBytes: 40,
          afterSetupBytes: 19,
          beforeBytes: 20,
        },
      }),
    ).toThrow("Process peak RSS checkpoints must be monotonic.");
    expect(() =>
      writeBenchmarkArtifact({
        ...artifactInput,
        processPeakRss: {
          afterBenchmarkBytes: 29,
          afterSetupBytes: 30,
          beforeBytes: 20,
        },
      }),
    ).toThrow("Process peak RSS checkpoints must be monotonic.");

    const endpointOnlyOutputJsonPath = ".artifacts/benchmark-artifact-endpoint-only-test.json";
    const {
      processPeakRss: configuredProcessPeakRss,
      samplingPolicy: configuredSamplingPolicy,
      ...endpointOnlyArtifactInput
    } = artifactInput;
    expect(configuredProcessPeakRss).toStrictEqual({
      afterBenchmarkBytes: 40,
      afterSetupBytes: 30,
      beforeBytes: 20,
    });
    expect(configuredSamplingPolicy).toStrictEqual(samplingPolicy);
    writeBenchmarkArtifact({
      ...endpointOnlyArtifactInput,
      outputJsonPath: endpointOnlyOutputJsonPath,
    });
    expect(
      JSON.parse(
        readFileSync(".artifacts/benchmark-artifact-endpoint-only-test.summary.json", "utf8"),
      ).memory,
    ).not.toHaveProperty("processPeakRss");

    const postGcOutputJsonPath = ".artifacts/benchmark-artifact-post-gc-test.json";
    const cleanupLedger = {
      activeSubscriptions: 0,
      activeViews: 0,
      pendingMutationBatches: 0,
      queuedEvents: 0,
    };
    const postGcEventLoopSamples = Array.from({ length: 9 }, (_value, eventLoopTurn) => ({
      cleanupLedger,
      eventLoopTurn,
      memory: memory(9 + eventLoopTurn),
    }));
    writeBenchmarkArtifact({
      ...endpointOnlyArtifactInput,
      measurementProtocol: {
        memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
        postGcEventLoopTurns: 8,
      },
      memoryAfterBenchmark: memory(17),
      outputJsonPath: postGcOutputJsonPath,
      postGcEventLoopSamples,
    });
    expect(
      JSON.parse(readFileSync(".artifacts/benchmark-artifact-post-gc-test.summary.json", "utf8"))
        .memory.postGcEventLoopSamples,
    ).toStrictEqual(postGcEventLoopSamples);
  });
});
