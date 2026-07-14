import type { ViewServerLiveSubscription } from "@effect-view-server/client";
import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { Effect, Fiber, Option, Queue, Stream } from "effect";

import {
  runGrpcLeasedBenchmarkSample,
  type GrpcLeasedBenchmarkOptions,
} from "../test-harness/grpc-leased-benchmark";
import * as benchmarkRuntime from "../test-harness/grpc-leased-benchmark-runtime";
import * as grpcRuntime from "../test-harness/grpc-runtime";

const options = {
  convergenceTimeout: "2 seconds",
  retainedRows: 4,
  routeCount: 2,
  rowsPerFeed: 2,
} satisfies GrpcLeasedBenchmarkOptions;

type ProjectedOrderRow = {
  readonly id: string;
  readonly price: number;
  readonly region: string;
  readonly status: "open" | "closed" | "cancelled";
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("gRPC leased benchmark core", () => {
  it("generates every deterministic order status", () => {
    expect(benchmarkRuntime.grpcLeasedRows("status", 3, 3).map((row) => row.status)).toStrictEqual([
      "closed",
      "open",
      "cancelled",
    ]);
  });

  it.live("rejects routes outside the acquired benchmark context", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* benchmarkRuntime.acquireGrpcLeasedBenchmarkContext(options);

        expect(() => benchmarkRuntime.queueForGrpcLeasedRoute(context, "missing")).toThrow(
          "gRPC leased benchmark route missing was not configured.",
        );

        yield* benchmarkRuntime.cleanupGrpcLeasedBenchmarkContext(context);
      }),
    ),
  );

  it.effect("ignores status events when watching row-count convergence", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const subscription = {
          events: Stream.make({
            code: "Ready",
            queryId: "status-only",
            status: "ready",
            topic: "orders",
            type: "status",
          }),
          close: () => Effect.void,
        } satisfies ViewServerLiveSubscription<ProjectedOrderRow>;
        const watched = yield* benchmarkRuntime.watchGrpcLeasedSubscription(subscription);

        yield* Fiber.join(watched.fiber);
        const totalRows = yield* Queue.poll(watched.totalRowsQueue);

        expect(Option.isNone(totalRows)).toBe(true);
      }),
    ),
  );

  it.live("treats an absent gRPC overlay as zero active feeds and subscribers", () =>
    Effect.gen(function* () {
      const sample = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options);
      const { grpc: _grpc, ...healthWithoutGrpc } = sample.health;

      expect({
        activeFeeds: benchmarkRuntime.activeGrpcLeasedFeedCount(healthWithoutGrpc),
        clientActiveFeeds: benchmarkRuntime.grpcLeasedClientActiveFeedCount(healthWithoutGrpc),
        subscribers: benchmarkRuntime.grpcLeasedSubscriberCount(healthWithoutGrpc),
      }).toStrictEqual({ activeFeeds: 0, clientActiveFeeds: 0, subscribers: 0 });
    }),
  );

  it.live("counts the orders client active-feed ledger as cleanup leakage", () =>
    Effect.gen(function* () {
      const sample = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options);
      const grpc = sample.health.grpc;
      if (grpc === undefined) {
        return yield* Effect.die("gRPC benchmark health overlay was absent.");
      }
      const ordersClient = grpc.clients["orders"];
      if (ordersClient === undefined) {
        return yield* Effect.die("gRPC benchmark orders client health was absent.");
      }
      const healthWithActiveClient = {
        ...sample.health,
        grpc: {
          ...grpc,
          clients: {
            ...grpc.clients,
            orders: {
              ...ordersClient,
              activeFeeds: 1,
            },
          },
        },
      } satisfies typeof sample.health;

      expect({
        clientActiveFeeds: benchmarkRuntime.grpcLeasedClientActiveFeedCount(healthWithActiveClient),
        cleanupLeakCount: benchmarkRuntime.grpcLeasedCleanupLeakCount(healthWithActiveClient),
      }).toStrictEqual({
        clientActiveFeeds: 1,
        cleanupLeakCount: 1,
      });
    }),
  );

  it.live.each(["starting", "degraded", "stopping"] as const)(
    "rejects %s benchmark health after still cleaning up the owned runtime",
    (status) =>
      Effect.gen(function* () {
        const readHealth = grpcRuntime.readGrpcHealthOverlayNow;
        vi.spyOn(grpcRuntime, "readGrpcHealthOverlayNow").mockImplementation((client, ledger) =>
          readHealth(client, ledger).pipe(
            Effect.map(
              (health) =>
                ({
                  ...health,
                  status,
                }) satisfies typeof health,
            ),
          ),
        );

        const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options).pipe(
          Effect.flip,
        );

        expect({
          _tag: error._tag,
          cleanupClientActiveFeeds: error.cleanupClientActiveFeeds,
          cleanupLeakCount: error.cleanupLeakCount,
          cleanupObserved: error.cleanupObserved,
        }).toStrictEqual({
          _tag: "GrpcLeasedBenchmarkSampleError",
          cleanupClientActiveFeeds: 0,
          cleanupLeakCount: 0,
          cleanupObserved: true,
        });
      }),
  );

  it.live("rejects a no-op measured close before emergency manager teardown repairs it", () =>
    Effect.gen(function* () {
      vi.spyOn(benchmarkRuntime, "closeGrpcLeasedSubscriptions").mockImplementation(
        () => Effect.void,
      );

      const error = yield* runGrpcLeasedBenchmarkSample("first-subscriber", options).pipe(
        Effect.flip,
      );

      expect(error.message).toMatch(
        /^GrpcLeasedBenchmarkWorkloadError: GrpcLeasedBenchmarkCleanupError: gRPC leased benchmark measured subscription cleanup leaked state:/u,
      );
      expect({
        cleanupActiveLeasedFeeds: error.cleanupActiveLeasedFeeds,
        cleanupClientActiveFeeds: error.cleanupClientActiveFeeds,
        cleanupLeakCount: error.cleanupLeakCount,
        cleanupObserved: error.cleanupObserved,
        cleanupRowCount: error.cleanupRowCount,
        releasedFeedCount: error.releasedFeedCount,
      }).toStrictEqual({
        cleanupActiveLeasedFeeds: 0,
        cleanupClientActiveFeeds: 0,
        cleanupLeakCount: 0,
        cleanupObserved: true,
        cleanupRowCount: 0,
        releasedFeedCount: 1,
      });
    }),
  );

  it.live("rejects an incomplete health-refresh snapshot", () =>
    Effect.gen(function* () {
      vi.spyOn(benchmarkRuntime, "activeGrpcLeasedFeedCount").mockImplementation(() => 0);

      const error = yield* runGrpcLeasedBenchmarkSample("health-refresh-overhead", options).pipe(
        Effect.flip,
      );

      expect({ _tag: error._tag, cleanupObserved: error.cleanupObserved }).toStrictEqual({
        _tag: "GrpcLeasedBenchmarkSampleError",
        cleanupObserved: true,
      });
    }),
  );

  it.live("rejects a health-refresh workload with no routes to sample", () =>
    Effect.gen(function* () {
      const error = yield* runGrpcLeasedBenchmarkSample("health-refresh-overhead", {
        ...options,
        routeCount: 0,
      }).pipe(Effect.flip);

      expect({ _tag: error._tag, cleanupObserved: error.cleanupObserved }).toStrictEqual({
        _tag: "GrpcLeasedBenchmarkSampleError",
        cleanupObserved: true,
      });
    }),
  );
});
