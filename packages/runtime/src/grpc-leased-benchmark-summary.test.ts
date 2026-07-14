import { describe, expect, it } from "@effect/vitest";

import {
  summarizeGrpcLeasedBenchmarkSamples,
  type GrpcLeasedBenchmarkSampleEvidence,
} from "../test-harness/grpc-leased-benchmark";

const cleanMeasuredCleanup = {
  activeLeasedFeeds: 0,
  activeSubscriptions: 0,
  activeViews: 0,
  clientActiveFeeds: 0,
  leakCount: 0,
  queuedEvents: 0,
  rowCount: 0,
} as const;

const deltaSample = (
  snapshotMs: number,
  rowsPerSecond: number,
  overrides: Partial<GrpcLeasedBenchmarkSampleEvidence> = {},
): GrpcLeasedBenchmarkSampleEvidence => ({
  acquiredFeedCount: 1,
  activeLeasedFeeds: 1,
  backpressureCount: 0,
  cleanupActiveLeasedFeeds: 0,
  cleanupClientActiveFeeds: 0,
  cleanupLeakCount: 0,
  cleanupMs: 1,
  cleanupRowCount: 0,
  deltaFanoutMs: snapshotMs,
  healthOverlayMs: 0.1,
  measurementRowCount: 101,
  measuredCleanup: cleanMeasuredCleanup,
  mutationCount: 100,
  name: "gRPC leased delta fanout",
  queuedEventCount: 0,
  releasedFeedCount: 1,
  rows: 100,
  rowsPerSecond,
  seedMutationCount: 1,
  snapshotMs,
  subscriberCount: 25,
  subscriptionMs: 2,
  ...overrides,
});

const invalidCleanupEvidence: ReadonlyArray<
  readonly [string, Partial<GrpcLeasedBenchmarkSampleEvidence>]
> = [
  ["active leased feeds", { cleanupActiveLeasedFeeds: 1 }],
  ["active client feeds", { cleanupClientActiveFeeds: 1 }],
  ["leaked runtime state", { cleanupLeakCount: 1 }],
  ["retained rows", { cleanupRowCount: 1 }],
  [
    "measured active leased feeds",
    { measuredCleanup: { ...cleanMeasuredCleanup, activeLeasedFeeds: 1 } },
  ],
  [
    "measured active subscriptions",
    { measuredCleanup: { ...cleanMeasuredCleanup, activeSubscriptions: 1 } },
  ],
  ["measured active views", { measuredCleanup: { ...cleanMeasuredCleanup, activeViews: 1 } }],
  [
    "measured client active feeds",
    { measuredCleanup: { ...cleanMeasuredCleanup, clientActiveFeeds: 1 } },
  ],
  ["measured leak count", { measuredCleanup: { ...cleanMeasuredCleanup, leakCount: 1 } }],
  ["measured queued events", { measuredCleanup: { ...cleanMeasuredCleanup, queuedEvents: 1 } }],
  ["measured retained rows", { measuredCleanup: { ...cleanMeasuredCleanup, rowCount: 1 } }],
  ["queued events", { queuedEventCount: 1 }],
  ["backpressure", { backpressureCount: 1 }],
  ["unreleased feeds", { releasedFeedCount: 0 }],
];

describe("gRPC leased benchmark summary", () => {
  it("keeps exact raw isolated samples and pools throughput across elapsed time", () => {
    const samples = [
      deltaSample(10, 10_000),
      deltaSample(10, 10_000),
      deltaSample(10, 10_000),
      deltaSample(10, 10_000),
      deltaSample(100, 1_000),
    ];
    const summary = summarizeGrpcLeasedBenchmarkSamples(samples, "gRPC leased delta fanout", 5);

    expect(summary.meanRowsPerSecond).toBe(8_200);
    expect(summary.medianRowsPerSecond).toBe(10_000);
    expect(summary.mutationCount).toBe(500);
    expect({
      maxCleanupClientActiveFeeds: summary.maxCleanupClientActiveFeeds,
      maxMeasuredCleanupActiveLeasedFeeds: summary.maxMeasuredCleanupActiveLeasedFeeds,
      maxMeasuredCleanupActiveSubscriptions: summary.maxMeasuredCleanupActiveSubscriptions,
      maxMeasuredCleanupActiveViews: summary.maxMeasuredCleanupActiveViews,
      maxMeasuredCleanupClientActiveFeeds: summary.maxMeasuredCleanupClientActiveFeeds,
      maxMeasuredCleanupLeakCount: summary.maxMeasuredCleanupLeakCount,
      maxMeasuredCleanupQueuedEvents: summary.maxMeasuredCleanupQueuedEvents,
      maxMeasuredCleanupRowCount: summary.maxMeasuredCleanupRowCount,
    }).toStrictEqual({
      maxCleanupClientActiveFeeds: 0,
      maxMeasuredCleanupActiveLeasedFeeds: 0,
      maxMeasuredCleanupActiveSubscriptions: 0,
      maxMeasuredCleanupActiveViews: 0,
      maxMeasuredCleanupClientActiveFeeds: 0,
      maxMeasuredCleanupLeakCount: 0,
      maxMeasuredCleanupQueuedEvents: 0,
      maxMeasuredCleanupRowCount: 0,
    });
    expect(summary.pooledRowsPerSecond).toBeCloseTo(3_571.428_571, 6);
    expect(summary.sampleCount).toBe(5);
    expect(summary.samples).toStrictEqual(samples);
    expect(summary.seedMutationCount).toBe(5);
  });

  it("handles even sample counts and zero-throughput samples", () => {
    const evenSummary = summarizeGrpcLeasedBenchmarkSamples(
      [deltaSample(10, 4_000), deltaSample(20, 2_000)],
      "gRPC leased delta fanout",
      2,
    );
    const zeroSummary = summarizeGrpcLeasedBenchmarkSamples(
      [
        deltaSample(0, 0, {
          measurementRowCount: 0,
          mutationCount: 0,
          rows: 0,
          seedMutationCount: 0,
        }),
      ],
      "gRPC leased delta fanout",
      1,
    );

    expect(evenSummary.medianRowsPerSecond).toBe(3_000);
    expect({
      pooledRowsPerSecond: zeroSummary.pooledRowsPerSecond,
      rowsPerSecondCoefficientOfVariation: zeroSummary.rowsPerSecondCoefficientOfVariation,
    }).toStrictEqual({
      pooledRowsPerSecond: 0,
      rowsPerSecondCoefficientOfVariation: 0,
    });
  });

  it("rejects the wrong sample count", () => {
    expect(() =>
      summarizeGrpcLeasedBenchmarkSamples([deltaSample(10, 10_000)], "gRPC leased delta fanout", 2),
    ).toThrow(
      "gRPC leased benchmark case gRPC leased delta fanout produced 1 sample(s), expected exactly 2.",
    );
  });

  it("rejects an empty sample set even when zero samples were requested", () => {
    expect(() => summarizeGrpcLeasedBenchmarkSamples([], "gRPC leased delta fanout", 0)).toThrow(
      "gRPC leased benchmark case gRPC leased delta fanout produced no samples.",
    );
  });

  it("rejects samples whose deterministic state differs", () => {
    expect(() =>
      summarizeGrpcLeasedBenchmarkSamples(
        [deltaSample(10, 10_000), deltaSample(20, 5_000, { subscriberCount: 26 })],
        "gRPC leased delta fanout",
        2,
      ),
    ).toThrow(
      "gRPC leased benchmark case gRPC leased delta fanout did not preserve identical sample state.",
    );
  });

  it.each(invalidCleanupEvidence)("rejects %s cleanup evidence", (_label, overrides) => {
    expect(() =>
      summarizeGrpcLeasedBenchmarkSamples(
        [deltaSample(10, 10_000, overrides)],
        "gRPC leased delta fanout",
        1,
      ),
    ).toThrow(
      "gRPC leased benchmark case gRPC leased delta fanout recorded non-zero cleanup, queue, or backpressure evidence.",
    );
  });
});
