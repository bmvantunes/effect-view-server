import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Option } from "effect";
import {
  grpcMaterializedBenchmarkCleanupLeakCount,
  runGrpcMaterializedBenchmarkSample,
  type GrpcMaterializedBenchmarkOptions,
  validateGrpcMaterializedBenchmarkHealth,
} from "../test-harness/grpc-materialized-benchmark";

describe("gRPC materialized benchmark samples", () => {
  it.live("owns the same deterministic runtime and source state for every sample", () =>
    Effect.gen(function* () {
      const options = {
        batchSize: 2,
        convergenceTimeout: "2 seconds",
        seedRows: 12,
      } satisfies GrpcMaterializedBenchmarkOptions;
      const first = yield* runGrpcMaterializedBenchmarkSample("stream-batch", options);
      const second = yield* runGrpcMaterializedBenchmarkSample("stream-batch", options);

      expect(
        [first, second].map((sample) => ({
          activeSourceFeeds: sample.health.grpc?.clients["orders"]?.activeFeeds,
          backpressureCount: sample.backpressureCount,
          cleanupLeakCount: sample.cleanupLeakCount,
          finalHealthRows: sample.health.engine.topics.orders.rowCount,
          name: sample.name,
          queuedEventCount: sample.queuedEventCount,
          resultRowId: sample.resultRowId,
          rows: sample.rows,
          seedRows: sample.seedRows,
          startTotalRows: sample.startTotalRows,
          totalRows: sample.totalRows,
        })),
      ).toStrictEqual([
        {
          activeSourceFeeds: 0,
          backpressureCount: 0,
          cleanupLeakCount: 0,
          finalHealthRows: 14,
          name: "gRPC materialized stream batch",
          queuedEventCount: 0,
          resultRowId: "order-13",
          rows: 2,
          seedRows: 12,
          startTotalRows: 12,
          totalRows: 14,
        },
        {
          activeSourceFeeds: 0,
          backpressureCount: 0,
          cleanupLeakCount: 0,
          finalHealthRows: 14,
          name: "gRPC materialized stream batch",
          queuedEventCount: 0,
          resultRowId: "order-13",
          rows: 2,
          seedRows: 12,
          startTotalRows: 12,
          totalRows: 14,
        },
      ]);
    }),
  );

  it.live("includes active source feeds in each sample's cleanup leak evidence", () =>
    Effect.gen(function* () {
      const sample = yield* runGrpcMaterializedBenchmarkSample("health-overlay", {
        batchSize: 2,
        convergenceTimeout: "2 seconds",
        seedRows: 2,
      });
      const grpcHealth = Option.getOrThrow(Option.fromNullishOr(sample.health.grpc));
      const ordersClientHealth = Option.getOrThrow(
        Option.fromNullishOr(grpcHealth.clients["orders"]),
      );
      const healthWithActiveSourceFeed = {
        ...sample.health,
        grpc: {
          ...grpcHealth,
          clients: {
            ...grpcHealth.clients,
            orders: {
              ...ordersClientHealth,
              activeFeeds: 1,
            },
          },
        },
      };
      const { grpc: presentGrpcHealth, ...healthWithoutGrpc } = sample.health;

      expect(sample.cleanupLeakCount).toBe(0);
      expect(presentGrpcHealth).toBe(grpcHealth);
      expect(grpcMaterializedBenchmarkCleanupLeakCount(healthWithoutGrpc)).toBe(0);
      expect(grpcMaterializedBenchmarkCleanupLeakCount(healthWithActiveSourceFeed)).toBe(1);
    }),
  );

  it.live("runs independently seeded burst and empty-result row samples", () =>
    Effect.gen(function* () {
      const burst = yield* runGrpcMaterializedBenchmarkSample("burst", {
        batchSize: 2,
        convergenceTimeout: "2 seconds",
        seedRows: 2,
      });
      const emptyResult = yield* runGrpcMaterializedBenchmarkSample("stream-batch", {
        batchSize: 0,
        convergenceTimeout: "2 seconds",
        seedRows: 1,
      });

      expect({
        burst: {
          name: burst.name,
          resultRowId: burst.resultRowId,
          rows: burst.rows,
          startTotalRows: burst.startTotalRows,
          totalRows: burst.totalRows,
        },
        emptyResult: {
          name: emptyResult.name,
          resultRowId: emptyResult.resultRowId,
          rows: emptyResult.rows,
          rowsPerSecond: emptyResult.rowsPerSecond,
          startTotalRows: emptyResult.startTotalRows,
          totalRows: emptyResult.totalRows,
        },
      }).toStrictEqual({
        burst: {
          name: "gRPC materialized burst",
          resultRowId: null,
          rows: 8,
          startTotalRows: 2,
          totalRows: 10,
        },
        emptyResult: {
          name: "gRPC materialized stream batch",
          resultRowId: null,
          rows: 0,
          rowsPerSecond: 0,
          startTotalRows: 1,
          totalRows: 1,
        },
      });
    }),
  );

  it.effect("requires ready health through the shared sample health invariant", () =>
    Effect.gen(function* () {
      yield* validateGrpcMaterializedBenchmarkHealth("ready");
      const startingError = yield* validateGrpcMaterializedBenchmarkHealth("starting").pipe(
        Effect.flip,
      );
      const stoppingError = yield* validateGrpcMaterializedBenchmarkHealth("stopping").pipe(
        Effect.flip,
      );
      const degradedError = yield* validateGrpcMaterializedBenchmarkHealth("degraded").pipe(
        Effect.flip,
      );

      expect(
        [startingError, stoppingError, degradedError].map((error) => ({
          _tag: error._tag,
          message: error.message,
        })),
      ).toStrictEqual([
        {
          _tag: "GrpcMaterializedBenchmarkHealthError",
          message: "gRPC materialized benchmark health must be ready; received starting.",
        },
        {
          _tag: "GrpcMaterializedBenchmarkHealthError",
          message: "gRPC materialized benchmark health must be ready; received stopping.",
        },
        {
          _tag: "GrpcMaterializedBenchmarkHealthError",
          message: "gRPC materialized benchmark health must be ready; received degraded.",
        },
      ]);
    }),
  );

  it.live("propagates a failed sample only after its owned resources are cleaned up", () =>
    Effect.gen(function* () {
      const error = yield* runGrpcMaterializedBenchmarkSample("stream-batch", {
        batchSize: 2,
        convergenceTimeout: "0 millis",
        seedRows: 12,
      }).pipe(Effect.flip);

      expect({
        _tag: error._tag,
        backpressureCount: error.backpressureCount,
        cleanupLeakCount: error.cleanupLeakCount,
        cleanupObserved: error.cleanupObserved,
        queuedEventCount: error.queuedEventCount,
        workload: error.workload,
      }).toStrictEqual({
        _tag: "GrpcMaterializedBenchmarkSampleError",
        backpressureCount: 0,
        cleanupLeakCount: 0,
        cleanupObserved: true,
        queuedEventCount: 0,
        workload: "stream-batch",
      });
      expect(error.message).toMatch(
        /^GrpcMaterializedBenchmarkConvergenceError: gRPC materialized benchmark did not converge to 12 rows\./u,
      );
    }),
  );

  it.live("preserves external interruption after owned resources are released", () =>
    Effect.gen(function* () {
      const fiber = yield* runGrpcMaterializedBenchmarkSample("stream-batch", {
        batchSize: 2,
        convergenceTimeout: "2 seconds",
        seedRows: 100_000,
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.sleep("10 millis");
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.hasInterrupts(exit)).toBe(true);
    }),
  );
});
