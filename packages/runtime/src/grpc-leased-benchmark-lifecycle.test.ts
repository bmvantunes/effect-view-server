import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { Cause, Effect } from "effect";

import {
  runGrpcLeasedBenchmarkSample,
  type GrpcLeasedBenchmarkOptions,
} from "../test-harness/grpc-leased-benchmark";
import * as benchmarkRuntime from "../test-harness/grpc-leased-benchmark-runtime";
import * as benchmarkWorkloads from "../test-harness/grpc-leased-benchmark-workloads";

const options = {
  convergenceTimeout: "2 seconds",
  retainedRows: 4,
  routeCount: 2,
  rowsPerFeed: 2,
} satisfies GrpcLeasedBenchmarkOptions;

const interrupted = <A>(effect: Effect.Effect<A, unknown>) =>
  effect.pipe(Effect.sandbox, Effect.flip, Effect.map(Cause.hasInterruptsOnly));

afterEach(() => {
  vi.restoreAllMocks();
});

describe("gRPC leased benchmark lifecycle", () => {
  it.live("reports acquisition defects without claiming cleanup evidence", () =>
    Effect.gen(function* () {
      vi.spyOn(benchmarkRuntime, "acquireGrpcLeasedBenchmarkContext").mockImplementation(() =>
        Effect.die("acquisition failed"),
      );

      const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options).pipe(
        Effect.flip,
      );

      expect({
        cleanupObserved: error.cleanupObserved,
        message: error.message,
        releasedFeedCount: error.releasedFeedCount,
      }).toStrictEqual({
        cleanupObserved: false,
        message: Cause.pretty(Cause.die("acquisition failed")),
        releasedFeedCount: 0,
      });
    }),
  );

  it.live("reports cleanup defects after the real owned runtime is closed", () =>
    Effect.gen(function* () {
      const cleanup = benchmarkRuntime.cleanupGrpcLeasedBenchmarkContext;
      vi.spyOn(benchmarkRuntime, "cleanupGrpcLeasedBenchmarkContext").mockImplementation(
        (context) => cleanup(context).pipe(Effect.andThen(Effect.die("cleanup failed"))),
      );

      const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options).pipe(
        Effect.flip,
      );

      expect({
        acquiredFeedCount: error.acquiredFeedCount,
        cleanupObserved: error.cleanupObserved,
        message: error.message,
        releasedFeedCount: error.releasedFeedCount,
      }).toStrictEqual({
        acquiredFeedCount: 0,
        cleanupObserved: false,
        message: Cause.pretty(Cause.die("cleanup failed")),
        releasedFeedCount: 0,
      });
    }),
  );

  it.live("keeps both measurement and cleanup causes when both fail", () =>
    Effect.gen(function* () {
      const cleanup = benchmarkRuntime.cleanupGrpcLeasedBenchmarkContext;
      const measurementError = new benchmarkWorkloads.GrpcLeasedBenchmarkWorkloadError({
        message: "measurement failed",
      });
      vi.spyOn(benchmarkRuntime, "cleanupGrpcLeasedBenchmarkContext").mockImplementation(
        (context) => cleanup(context).pipe(Effect.andThen(Effect.die("cleanup failed"))),
      );
      vi.spyOn(benchmarkWorkloads, "runGrpcLeasedBenchmarkWorkload").mockImplementation(
        () => measurementError,
      );

      const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options).pipe(
        Effect.flip,
      );

      expect(
        error.message.split("\n").filter((line) => !line.trimStart().startsWith("at ")),
      ).toStrictEqual([
        "GrpcLeasedBenchmarkWorkloadError: measurement failed",
        "Cleanup: Error: cleanup failed",
      ]);
    }),
  );

  it.live("preserves measurement interruption through workload and sample boundaries", () =>
    Effect.gen(function* () {
      vi.spyOn(benchmarkRuntime, "waitForGrpcLeasedRows").mockImplementation(
        () => Effect.interrupt,
      );

      expect(yield* interrupted(runGrpcLeasedBenchmarkSample("first-subscriber", options))).toBe(
        true,
      );
    }),
  );

  it.live("preserves cleanup interruption after the real owned runtime is closed", () =>
    Effect.gen(function* () {
      const cleanup = benchmarkRuntime.cleanupGrpcLeasedBenchmarkContext;
      vi.spyOn(benchmarkRuntime, "cleanupGrpcLeasedBenchmarkContext").mockImplementation(
        (context) => cleanup(context).pipe(Effect.andThen(Effect.interrupt)),
      );

      expect(yield* interrupted(runGrpcLeasedBenchmarkSample("first-subscriber", options))).toBe(
        true,
      );
    }),
  );
});
