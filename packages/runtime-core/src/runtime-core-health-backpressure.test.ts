import { describe, expect, it } from "@effect/vitest";
import { createColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import { Deferred, Effect, Fiber, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine, readHealth } from "./health";
import { makeViewServerRuntimeCore } from "./index";
import { makeRuntimeCoreLiveClientModule } from "./live-client";
import { engineHealth, order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("closes slow pushed health summary subscriptions with typed backpressure", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
        viewServer,
        engine,
        health,
        Effect.sync(() => health.value),
      );
      const summary = yield* liveClient.subscribeHealthSummary();

      yield* Effect.forEach(
        Array.from({ length: 64 }, (_, index) => index),
        (index) =>
          Effect.sync(() => {
            health.update(() => healthFromEngine(engineHealth("ready", index + 1)));
          }).pipe(Effect.andThen(Effect.yieldNow)),
      );
      const events = yield* summary.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          status: "closed",
          code: "BackpressureExceeded",
          message:
            "Runtime health subscription closed because its event queue exceeded capacity with 64 queued event(s).",
        },
      ]);

      yield* summary.close();
      yield* liveClient.close;
    }),
  );

  it.effect(
    "keeps detailed health subscription acquisition stable when health changes during refresh",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
        const health = AtomRef.make(healthFromEngine(yield* engine.health()));
        const refreshStarted = yield* Deferred.make<void>();
        const releaseRefresh = yield* Deferred.make<void>();
        const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
          viewServer,
          engine,
          health,
          Effect.gen(function* () {
            yield* Deferred.succeed(refreshStarted, undefined);
            yield* Deferred.await(releaseRefresh);
            return health.value;
          }),
        );

        const detailFiber = yield* liveClient
          .subscribeHealth()
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(refreshStarted).pipe(Effect.timeout("1 second"));
        yield* Effect.forEach(
          Array.from({ length: 128 }, (_, index) => index),
          (index) =>
            Effect.sync(() => {
              health.update(() => healthFromEngine(engineHealth("ready", index + 1)));
            }),
        );
        yield* Deferred.succeed(releaseRefresh, undefined);

        const detail = yield* Fiber.join(detailFiber).pipe(Effect.timeout("1 second"));
        const events = yield* detail.events.pipe(Stream.take(2), Stream.runCollect);

        expect(Array.from(events)).toStrictEqual([
          {
            type: "snapshot",
            topic: "__view_server_health",
            queryId: "health",
            version: 1,
            keys: ["orders"],
            rows: [
              {
                id: "orders",
                status: "ready",
                rowCount: 128,
                liveRowCount: 128,
                deletedRowCount: 0,
                version: 1,
                lastMutationAt: null,
                mutationsPerSecond: 0,
                rowsPerSecond: 0,
                pendingMutationBatches: 0,
                activeFallbackGroupedViews: 0,
                activeIncrementalGroupedViews: 0,
                activeViews: 0,
                groupedFullEvaluationCount: 0,
                groupedPatchedEvaluationCount: 0,
                activeSubscriptions: 0,
                queuedEvents: 0,
                maxQueueDepth: 0,
                backpressureEvents: 0,
                memoryBytes: 0,
                tombstoneCount: 0,
                compactionPending: false,
                kafkaLag: null,
                updatedAtNanos: expect.anything(),
              },
            ],
            totalRows: 1,
          },
          {
            type: "snapshot",
            topic: "__view_server_health",
            queryId: "health",
            version: 1,
            keys: ["orders"],
            rows: [
              {
                id: "orders",
                status: "ready",
                rowCount: 128,
                liveRowCount: 128,
                deletedRowCount: 0,
                version: 1,
                lastMutationAt: null,
                mutationsPerSecond: 0,
                rowsPerSecond: 0,
                pendingMutationBatches: 0,
                activeFallbackGroupedViews: 0,
                activeIncrementalGroupedViews: 0,
                activeViews: 0,
                groupedFullEvaluationCount: 0,
                groupedPatchedEvaluationCount: 0,
                activeSubscriptions: 0,
                queuedEvents: 0,
                maxQueueDepth: 0,
                backpressureEvents: 0,
                memoryBytes: 0,
                tombstoneCount: 0,
                compactionPending: false,
                kafkaLag: null,
                updatedAtNanos: expect.anything(),
              },
            ],
            totalRows: 1,
          },
        ]);

        yield* detail.close();
        yield* liveClient.close;
      }),
  );

  it.effect(
    "rejects pending pushed health subscriptions when live client closes during refresh",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
        const health = AtomRef.make(healthFromEngine(yield* engine.health()));
        const refreshStarted = yield* Deferred.make<void>();
        const releaseRefresh = yield* Deferred.make<void>();
        const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
          viewServer,
          engine,
          health,
          Effect.gen(function* () {
            yield* Deferred.succeed(refreshStarted, undefined);
            yield* Deferred.await(releaseRefresh);
            return health.value;
          }),
        );

        const summaryFiber = yield* liveClient
          .subscribeHealthSummary()
          .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(refreshStarted).pipe(Effect.timeout("1 second"));
        const closeFiber = yield* liveClient.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(releaseRefresh, undefined);

        const closedSummary = yield* Fiber.join(summaryFiber).pipe(Effect.timeout("1 second"));
        yield* Fiber.join(closeFiber).pipe(Effect.timeout("1 second"));

        expect(closedSummary).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Runtime Core is closed.",
        });
      }),
  );

  it.effect("closes slow runtime health summary subscriptions from real health refreshes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 minute",
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();

      yield* Effect.forEach(
        Array.from({ length: 64 }, (_, index) => index),
        (index) =>
          runtimeCore.client
            .publish("orders", order(`slow-health-${index}`, index))
            .pipe(Effect.andThen(runtimeCore.client.health()), Effect.andThen(Effect.yieldNow)),
      );
      const events = yield* summary.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          status: "closed",
          code: "BackpressureExceeded",
          message:
            "Runtime health subscription closed because its event queue exceeded capacity with 64 queued event(s).",
        },
      ]);

      yield* summary.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("closes slow pushed detailed health subscriptions with typed backpressure", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
        viewServer,
        engine,
        health,
        Effect.sync(() => health.value),
      );
      const detail = yield* liveClient.subscribeHealth();

      yield* Effect.forEach(
        Array.from({ length: 64 }, (_, index) => index),
        (index) =>
          Effect.sync(() => {
            health.update(() => healthFromEngine(engineHealth("ready", index + 1)));
          }).pipe(Effect.andThen(Effect.yieldNow)),
      );
      const events = yield* detail.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "__view_server_health",
          queryId: "health",
          status: "closed",
          code: "BackpressureExceeded",
          message:
            "Runtime health subscription closed because its event queue exceeded capacity with 64 queued event(s).",
        },
      ]);

      yield* detail.close();
      yield* liveClient.close;
    }),
  );

  it.effect("closes active pushed health subscriptions when the live client closes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();
      const summaryFiber = yield* summary.events.pipe(Stream.runDrain, Effect.forkChild);
      const detailFiber = yield* detail.events.pipe(Stream.runDrain, Effect.forkChild);

      yield* Effect.yieldNow;
      yield* runtimeCore.close;
      yield* Fiber.join(summaryFiber);
      yield* Fiber.join(detailFiber);

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
    }),
  );

  it.effect("rejects pushed health subscriptions after live client close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});

      yield* runtimeCore.liveClient.close;
      const closedSummary = yield* Effect.flip(runtimeCore.liveClient.subscribeHealthSummary());
      const closedDetail = yield* Effect.flip(runtimeCore.liveClient.subscribeHealth());

      expect(closedSummary).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "Runtime Core is closed.",
      });
      expect(closedDetail).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "Runtime Core is closed.",
      });
      yield* runtimeCore.close;
    }),
  );

  it.effect("keeps pushed health subscriptions alive after the acquisition fiber completes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const acquisitionFiber = yield* runtimeCore.liveClient
        .subscribeHealthSummary()
        .pipe(Effect.forkChild);
      const summary = yield* Fiber.join(acquisitionFiber);
      const eventsFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtimeCore.client.publish("orders", order("a", 10));
      yield* runtimeCore.client.health();
      const events = yield* Fiber.join(eventsFiber).pipe(
        Effect.timeout("1 second"),
        Effect.ensuring(summary.close().pipe(Effect.orDie, Effect.andThen(runtimeCore.close))),
      );

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: null,
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: null,
            },
          ],
          totalRows: 1,
        },
      ]);
    }),
  );

  it.effect("does not let stale detached health refreshes overwrite stopping health", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const readyHealth = engineHealth("ready", 1);
      const stoppingHealth = engineHealth("stopping", 1);
      const health = AtomRef.make(healthFromEngine(readyHealth));
      let readCount = 0;
      const healthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return readyHealth;
        }),
        Effect.succeed(stoppingHealth),
      ];
      const engine = {
        health: () => {
          const nextRead = healthReads[readCount] ?? Effect.succeed(stoppingHealth);
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };

      const staleRefresh = yield* readHealth(engine, health).pipe(Effect.forkDetach);
      yield* Deferred.await(firstReadStarted);

      const closed = yield* readHealth(engine, health);
      expect(closed.status).toBe("stopping");

      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(staleRefresh);
      expect(health.value.status).toBe("stopping");
    }),
  );
});
