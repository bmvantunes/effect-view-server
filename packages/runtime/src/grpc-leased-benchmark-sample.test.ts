import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import {
  grpcLeasedBenchmarkCases,
  runGrpcLeasedBenchmarkSample,
  type GrpcLeasedBenchmarkOptions,
  type GrpcLeasedBenchmarkSample,
} from "../test-harness/grpc-leased-benchmark";

const options = {
  convergenceTimeout: "2 seconds",
  retainedRows: 4,
  routeCount: 2,
  rowsPerFeed: 2,
} satisfies GrpcLeasedBenchmarkOptions;

const cleanMeasuredCleanup = {
  activeLeasedFeeds: 0,
  activeSubscriptions: 0,
  activeViews: 0,
  clientActiveFeeds: 0,
  leakCount: 0,
  queuedEvents: 0,
  rowCount: 0,
} as const;

const cleanSampleCleanup = {
  cleanupActiveLeasedFeeds: 0,
  cleanupClientActiveFeeds: 0,
  cleanupLeakCount: 0,
  cleanupRowCount: 0,
  measuredCleanup: cleanMeasuredCleanup,
} as const;

const sampleCleanupEvidence = (sample: GrpcLeasedBenchmarkSample) => ({
  cleanupActiveLeasedFeeds: sample.cleanupActiveLeasedFeeds,
  cleanupClientActiveFeeds: sample.cleanupClientActiveFeeds,
  cleanupLeakCount: sample.cleanupLeakCount,
  cleanupRowCount: sample.cleanupRowCount,
  measuredCleanup: sample.measuredCleanup,
});

describe("gRPC leased benchmark samples", () => {
  it.live("owns the same deterministic manager, subscriptions, and source state per sample", () =>
    Effect.gen(function* () {
      const first = yield* runGrpcLeasedBenchmarkSample("partitioned-write-convergence", options);
      const second = yield* runGrpcLeasedBenchmarkSample("partitioned-write-convergence", options);

      expect(
        [first, second].map((sample) => ({
          acquiredFeedCount: sample.acquiredFeedCount,
          activeLeasedFeeds: sample.activeLeasedFeeds,
          backpressureCount: sample.backpressureCount,
          ...sampleCleanupEvidence(sample),
          measurementRowCount: sample.measurementRowCount,
          mutationCount: sample.mutationCount,
          name: sample.name,
          queuedEventCount: sample.queuedEventCount,
          releasedFeedCount: sample.releasedFeedCount,
          rows: sample.rows,
          seedMutationCount: sample.seedMutationCount,
          subscriberCount: sample.subscriberCount,
        })),
      ).toStrictEqual([
        {
          acquiredFeedCount: 2,
          activeLeasedFeeds: 2,
          backpressureCount: 0,
          ...cleanSampleCleanup,
          measurementRowCount: 4,
          mutationCount: 4,
          name: "gRPC leased partitioned write convergence",
          queuedEventCount: 0,
          releasedFeedCount: 2,
          rows: 4,
          seedMutationCount: 0,
          subscriberCount: 2,
        },
        {
          acquiredFeedCount: 2,
          activeLeasedFeeds: 2,
          backpressureCount: 0,
          ...cleanSampleCleanup,
          measurementRowCount: 4,
          mutationCount: 4,
          name: "gRPC leased partitioned write convergence",
          queuedEventCount: 0,
          releasedFeedCount: 2,
          rows: 4,
          seedMutationCount: 0,
          subscriberCount: 2,
        },
      ]);
    }),
  );

  it.live("preserves the production leased-path invariants for every workload", () =>
    Effect.gen(function* () {
      const samples = yield* Effect.forEach(grpcLeasedBenchmarkCases, (benchmarkCase) =>
        runGrpcLeasedBenchmarkSample(benchmarkCase.workload, options),
      );

      expect(
        samples.map((sample) => ({
          acquiredFeedCount: sample.acquiredFeedCount,
          activeLeasedFeeds: sample.activeLeasedFeeds,
          ...sampleCleanupEvidence(sample),
          measurementRowCount: sample.measurementRowCount,
          mutationCount: sample.mutationCount,
          name: sample.name,
          releasedFeedCount: sample.releasedFeedCount,
          rows: sample.rows,
          seedMutationCount: sample.seedMutationCount,
          subscriberCount: sample.subscriberCount,
        })),
      ).toStrictEqual([
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased first subscriber",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 1,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased same-route reuse",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 10,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased one route many subscribers",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 50,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased local-filter live snapshot",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 1,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 4,
          mutationCount: 0,
          name: "gRPC leased retained local-filter snapshot",
          releasedFeedCount: 1,
          rows: 4,
          seedMutationCount: 4,
          subscriberCount: 2,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 3,
          mutationCount: 2,
          name: "gRPC leased delta fanout",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 1,
          subscriberCount: 25,
        },
        {
          acquiredFeedCount: 2,
          activeLeasedFeeds: 2,
          ...cleanSampleCleanup,
          measurementRowCount: 4,
          mutationCount: 4,
          name: "gRPC leased partitioned write convergence",
          releasedFeedCount: 2,
          rows: 4,
          seedMutationCount: 0,
          subscriberCount: 2,
        },
        {
          acquiredFeedCount: 2,
          activeLeasedFeeds: 2,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 0,
          name: "gRPC leased health refresh overhead",
          releasedFeedCount: 2,
          rows: 0,
          seedMutationCount: 2,
          subscriberCount: 2,
        },
        {
          acquiredFeedCount: 1,
          activeLeasedFeeds: 1,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased last-subscriber cleanup",
          releasedFeedCount: 1,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 1,
        },
        {
          acquiredFeedCount: 2,
          activeLeasedFeeds: 2,
          ...cleanSampleCleanup,
          measurementRowCount: 2,
          mutationCount: 2,
          name: "gRPC leased many routes",
          releasedFeedCount: 2,
          rows: 2,
          seedMutationCount: 0,
          subscriberCount: 2,
        },
      ]);
    }),
  );

  it.live("separates retained setup mutations from the measured snapshot", () =>
    Effect.gen(function* () {
      const sample = yield* runGrpcLeasedBenchmarkSample("retained-local-filter-snapshot", options);

      expect({
        acquiredFeedCount: sample.acquiredFeedCount,
        activeLeasedFeeds: sample.activeLeasedFeeds,
        cleanupClientActiveFeeds: sample.cleanupClientActiveFeeds,
        cleanupLeakCount: sample.cleanupLeakCount,
        cleanupRowCount: sample.cleanupRowCount,
        measurementRowCount: sample.measurementRowCount,
        measuredCleanup: sample.measuredCleanup,
        mutationCount: sample.mutationCount,
        releasedFeedCount: sample.releasedFeedCount,
        rows: sample.rows,
        seedMutationCount: sample.seedMutationCount,
        subscriberCount: sample.subscriberCount,
        snapshotIncludesSubscription: sample.snapshotMs >= sample.subscriptionMs,
      }).toStrictEqual({
        acquiredFeedCount: 1,
        activeLeasedFeeds: 1,
        cleanupClientActiveFeeds: 0,
        cleanupLeakCount: 0,
        cleanupRowCount: 0,
        measurementRowCount: 4,
        measuredCleanup: cleanMeasuredCleanup,
        mutationCount: 0,
        releasedFeedCount: 1,
        rows: 4,
        seedMutationCount: 4,
        subscriberCount: 2,
        snapshotIncludesSubscription: true,
      });
    }),
  );

  it.live("reports a failed sample only after its owned leases and runtime are cleaned up", () =>
    Effect.gen(function* () {
      const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", {
        ...options,
        convergenceTimeout: "0 millis",
      }).pipe(Effect.flip);

      expect({
        _tag: error._tag,
        acquiredFeedCount: error.acquiredFeedCount,
        cleanupActiveLeasedFeeds: error.cleanupActiveLeasedFeeds,
        cleanupClientActiveFeeds: error.cleanupClientActiveFeeds,
        cleanupLeakCount: error.cleanupLeakCount,
        cleanupObserved: error.cleanupObserved,
        cleanupRowCount: error.cleanupRowCount,
        releasedFeedCount: error.releasedFeedCount,
        workload: error.workload,
      }).toStrictEqual({
        _tag: "GrpcLeasedBenchmarkSampleError",
        acquiredFeedCount: 1,
        cleanupActiveLeasedFeeds: 0,
        cleanupClientActiveFeeds: 0,
        cleanupLeakCount: 0,
        cleanupObserved: true,
        cleanupRowCount: 0,
        releasedFeedCount: 1,
        workload: "first-subscriber",
      });
    }),
  );
});
