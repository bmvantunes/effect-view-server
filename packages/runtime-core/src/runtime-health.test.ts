import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Clock, Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { healthFromEngine, makeCoalescedHealthReader, makeHealthRefreshScheduler } from "./health";
import { makeViewServerRuntimeCore } from "./index";
import { engineHealth } from "./test-support/runtime-test-fixtures";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const order = (id: string, price: number): typeof Order.Type => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

describe("Runtime Core health", () => {
  it.effect(
    "keeps fresh Runtime Client health separate from cadence-controlled pushed health",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
          healthRefreshCadence: "1 second",
        });
        const detail = yield* runtimeCore.liveClient.subscribeHealth();
        const pushedEventsFiber = yield* detail.events.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );

        yield* runtimeCore.client.publish("orders", order("fresh-only", 10));
        const freshHealth = yield* runtimeCore.client.health();

        expect(freshHealth.engine.topics.orders.rowCount).toBe(1);
        expect(runtimeCore.liveClient.health.value.engine.topics.orders.rowCount).toBe(0);

        yield* TestClock.adjust("1 second");
        const pushedEvents = Array.from(yield* Fiber.join(pushedEventsFiber));
        expect(
          pushedEvents.map((event) =>
            event.type === "snapshot" ? event.rows[0]?.rowCount : undefined,
          ),
        ).toStrictEqual([0, 1]);

        yield* detail.close();
        yield* runtimeCore.close;
      }),
  );

  it.effect("shares one pushed-health cadence across Live Client views and subscribers", () =>
    Effect.gen(function* () {
      let healthBuildCount = 0;
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 second",
        healthOverlay: (health) => {
          healthBuildCount += 1;
          return health;
        },
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.serverLiveClient.subscribeHealth();
      const secondSummary = yield* runtimeCore.serverLiveClient.subscribeHealthSummary();
      const secondDetail = yield* runtimeCore.liveClient.subscribeHealth();
      const summaryEventsFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      const detailEventsFiber = yield* detail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      const secondSummaryEventsFiber = yield* secondSummary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      const secondDetailEventsFiber = yield* secondDetail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      expect(healthBuildCount).toBe(1);

      yield* runtimeCore.client.publish("orders", order("shared-cadence", 20));
      yield* TestClock.adjust("1 second");

      const summaryEvents = Array.from(yield* Fiber.join(summaryEventsFiber));
      const detailEvents = Array.from(yield* Fiber.join(detailEventsFiber));
      const secondSummaryEvents = Array.from(yield* Fiber.join(secondSummaryEventsFiber));
      const secondDetailEvents = Array.from(yield* Fiber.join(secondDetailEventsFiber));
      expect(summaryEvents[0]).toBe(secondSummaryEvents[0]);
      expect(summaryEvents[1]).toBe(secondSummaryEvents[1]);
      expect(detailEvents[0]).toBe(secondDetailEvents[0]);
      expect(detailEvents[1]).toBe(secondDetailEvents[1]);
      expect({
        detailVersions: detailEvents.map((event) =>
          event.type === "snapshot" ? event.version : undefined,
        ),
        healthBuildCount,
        secondSummaryVersions: secondSummaryEvents.map((event) =>
          event.type === "snapshot" ? event.version : undefined,
        ),
        summaryVersions: summaryEvents.map((event) =>
          event.type === "snapshot" ? event.version : undefined,
        ),
      }).toStrictEqual({
        detailVersions: [0, 1],
        healthBuildCount: 2,
        secondSummaryVersions: [0, 1],
        summaryVersions: [0, 1],
      });

      yield* summary.close();
      yield* detail.close();
      yield* secondSummary.close();
      yield* secondDetail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes one pending shared refresh before a new health subscriber snapshots", () =>
    Effect.gen(function* () {
      let healthBuildCount = 0;
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 minute",
        healthOverlay: (health) => {
          healthBuildCount += 1;
          return health;
        },
      });

      yield* runtimeCore.client.publish("orders", order("pending-refresh", 30));
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.serverLiveClient.subscribeHealth();
      const summaryEvents = yield* summary.events.pipe(Stream.take(1), Stream.runCollect);
      const detailEvents = yield* detail.events.pipe(Stream.take(1), Stream.runCollect);
      const summarySnapshots = Array.from(summaryEvents).filter(
        (event) => event.type === "snapshot",
      );
      const detailSnapshots = Array.from(detailEvents).filter((event) => event.type === "snapshot");

      expect({
        detailRowCount: detailSnapshots[0]?.rows[0]?.rowCount,
        healthBuildCount,
        summaryVersion: summarySnapshots[0]?.version,
      }).toStrictEqual({
        detailRowCount: 1,
        healthBuildCount: 2,
        summaryVersion: 1,
      });

      yield* summary.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("publishes through the public Runtime Core health refresh request", () =>
    Effect.gen(function* () {
      let healthBuildCount = 0;
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 second",
        healthOverlay: (health) => {
          healthBuildCount += 1;
          return health;
        },
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const eventsFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* runtimeCore.requestHealthRefresh;
      yield* TestClock.adjust("1 second");
      const events = Array.from(yield* Fiber.join(eventsFiber));

      expect({
        healthBuildCount,
        versions: events.map((event) => (event.type === "snapshot" ? event.version : undefined)),
      }).toStrictEqual({
        healthBuildCount: 2,
        versions: [0, 0],
      });

      yield* summary.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("finishes a shared publication when an Atom listener throws", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 minute",
      });
      const detail = yield* runtimeCore.liveClient.subscribeHealth();
      const eventsFiber = yield* detail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      let listenerCallCount = 0;
      const unsubscribe = runtimeCore.liveClient.health.subscribe(() => {
        listenerCallCount += 1;
        throw new Error("health listener failed");
      });

      yield* runtimeCore.client.publish("orders", order("listener-failure", 40));
      const refreshExit = yield* Effect.exit(runtimeCore.refreshHealth);
      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));

      expect(Exit.isFailure(refreshExit)).toBe(true);
      expect(listenerCallCount).toBe(1);
      expect(
        events.map((event) => (event.type === "snapshot" ? event.rows[0]?.rowCount : undefined)),
      ).toStrictEqual([0, 1]);

      unsubscribe();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("reports elapsed runtime uptime and passes refresh time to health overlays", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(10_000);
      const overlayTimes: Array<number> = [];
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health, nowMillis) => {
          overlayTimes.push(nowMillis);
          return health;
        },
      });

      expect(runtimeCore.liveClient.health.value.uptimeMs).toBe(0);
      expect(overlayTimes).toStrictEqual([10_000]);

      yield* TestClock.adjust("2500 millis");
      const refreshedHealth = yield* runtimeCore.refreshHealth;

      expect(refreshedHealth.uptimeMs).toBe(2_500);
      expect(overlayTimes).toStrictEqual([10_000, 12_500]);
      yield* runtimeCore.close;
    }),
  );

  it.effect("clamps runtime uptime when the clock moves before the runtime start", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(10_000);
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});

      yield* TestClock.setTime(9_000);
      const refreshedHealth = yield* runtimeCore.refreshHealth;

      expect(refreshedHealth.uptimeMs).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("uses monotonic time for uptime when wall time moves backward", () =>
    Effect.gen(function* () {
      let wallMillis = 10_000;
      let monotonicNanos = 5_000_000_000n;
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => wallMillis,
        currentTimeMillis: Effect.sync(() => wallMillis),
        currentTimeNanosUnsafe: () => monotonicNanos,
        currentTimeNanos: Effect.sync(() => monotonicNanos),
        sleep: () => Effect.void,
      };
      const overlayTimes: Array<number> = [];
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health, nowMillis) => {
          overlayTimes.push(nowMillis);
          return health;
        },
      }).pipe(Effect.provideService(Clock.Clock, clock));

      wallMillis = 9_000;
      monotonicNanos = 7_500_000_000n;
      const refreshedHealth = yield* runtimeCore.refreshHealth.pipe(
        Effect.provideService(Clock.Clock, clock),
      );

      expect(refreshedHealth.uptimeMs).toBe(2_500);
      expect(overlayTimes).toStrictEqual([10_000, 9_000]);
      yield* runtimeCore.close;
    }),
  );

  it.effect("pushes summary and detailed health snapshots", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "0 millis",
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const detailFiber = yield* detail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtimeCore.client.publish("orders", order("a", 10));

      const summaryEvents = yield* Fiber.join(summaryFiber);
      const detailEvents = yield* Fiber.join(detailFiber);
      expect(Array.from(summaryEvents)).toStrictEqual([
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
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "ready",
              rowCount: 0,
              liveRowCount: 0,
              deletedRowCount: 0,
              version: 0,
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
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: expect.anything(),
              mutationsPerSecond: 1,
              rowsPerSecond: 1,
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

      yield* summary.close();
      yield* summary.close();
      yield* detail.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("applies health overlays to pushed health subscriptions", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health) => ({
          ...health,
          status: "degraded",
          kafka: {
            startFrom: {
              consumerGroupId: "view-server-test",
              fallbackMode: "earliest",
              mode: "committed",
            },
            regions: {
              local: {
                status: "connected",
                brokers: "localhost:9092",
                lastConnectedAt: 1_000,
                lastError: null,
              },
            },
            topics: {
              sourceOrders: {
                status: "stalled",
                sourceTopic: "orders-source",
                viewServerTopic: "orders",
                regions: {
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    publishFailuresPerSecond: 0,
                    commitFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: 7n,
                    lagSampledAt: null,
                    committedOffset: "3",
                    lastError: "lagging",
                  },
                },
              },
            },
          },
        }),
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryEvents = yield* summary.events.pipe(Stream.take(1), Stream.runCollect);
      const detailEvents = yield* detail.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(summaryEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "degraded",
              runtimeStatus: "degraded",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 7n,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "degraded",
              rowCount: 0,
              liveRowCount: 0,
              deletedRowCount: 0,
              version: 0,
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
              kafkaLag: 7n,
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* summary.close();
      yield* detail.close();
      yield* runtimeCore.close;
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
            .pipe(Effect.andThen(runtimeCore.refreshHealth), Effect.andThen(Effect.yieldNow)),
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

  it.effect("coalesces concurrent health reads while an active same-epoch read is running", () =>
    Effect.gen(function* () {
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(() =>
        Effect.gen(function* () {
          readCount += 1;
          yield* Deferred.succeed(readStarted, undefined);
          yield* Deferred.await(releaseRead);
          return healthFromEngine(engineHealth("ready", readCount));
        }),
      );

      const first = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(readStarted);
      const second = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseRead, undefined);
      const [firstHealth, secondHealth] = yield* Effect.all(
        [Fiber.join(first), Fiber.join(second)],
        {
          concurrency: 2,
        },
      ).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(1);
      expect(firstHealth.engine.topics.orders.rowCount).toBe(1);
      expect(secondHealth.engine.topics.orders.rowCount).toBe(1);
      const thirdHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(thirdHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("clears the active health read after a failed read so the next read retries", () =>
    Effect.gen(function* () {
      let readCount = 0;
      const healthReads = [
        Effect.fail("boom"),
        Effect.succeed(healthFromEngine(engineHealth("ready", 2))),
      ];
      const coalescedHealth = makeCoalescedHealthReader(() =>
        Effect.gen(function* () {
          const nextRead =
            healthReads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
          readCount += 1;
          return yield* nextRead;
        }),
      );

      const failedHealth = yield* Effect.flip(coalescedHealth());
      expect(failedHealth).toBe("boom");
      const recoveredHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(recoveredHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("does not strand followers when the active health reader is interrupted", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(() =>
        Effect.gen(function* () {
          readCount += 1;
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return healthFromEngine(engineHealth("ready", readCount));
        }),
      );

      const leader = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstReadStarted);
      const follower = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;

      const interruptStarted = yield* Deferred.make<void>();
      const interruptLeader = yield* Effect.gen(function* () {
        yield* Deferred.succeed(interruptStarted, undefined);
        yield* Fiber.interrupt(leader);
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(interruptStarted);
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(interruptLeader);
      const followerHealth = yield* Fiber.join(follower);
      const nextHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(followerHealth.engine.topics.orders.rowCount).toBe(1);
      expect(nextHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect(
    "queues a trailing health scheduler refresh when requested while refresh is pending",
    () =>
      Effect.gen(function* () {
        const firstStarted = yield* Deferred.make<void>();
        const firstFinished = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        const secondFinished = yield* Deferred.make<void>();
        let refreshCount = 0;
        const refreshSteps = [
          { started: firstStarted, finished: firstFinished },
          { started: secondStarted, finished: secondFinished },
        ] as const;

        const scheduler = yield* makeHealthRefreshScheduler(
          Effect.gen(function* () {
            const refreshStep = refreshSteps[refreshCount] ?? refreshSteps[1];
            yield* Effect.sync(() => {
              refreshCount += 1;
            });
            yield* Deferred.succeed(refreshStep.started, undefined);
            yield* Deferred.await(refreshStep.finished);
          }),
          "0 millis",
        );

        yield* scheduler.request;
        yield* Deferred.await(firstStarted);

        yield* scheduler.request;
        yield* Deferred.succeed(firstFinished, undefined);
        yield* Deferred.await(secondStarted);

        expect(refreshCount).toBe(2);
        yield* Deferred.succeed(secondFinished, undefined);
        yield* scheduler.close;
      }),
  );

  it.effect("closes a sleeping health scheduler refresh fiber", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "1 minute",
      );

      yield* scheduler.request;
      yield* scheduler.close;

      expect(refreshCount).toBe(0);
      yield* scheduler.close;
    }),
  );

  it.effect("clears active health scheduler state when a refresh interrupts itself", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const startedSignals = [firstStarted, secondStarted] as const;
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const started = startedSignals[refreshCount] ?? secondStarted;
          yield* Effect.sync(() => {
            refreshCount += 1;
          });
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.interrupt;
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(firstStarted);
      yield* Effect.yieldNow;
      yield* scheduler.request;
      yield* Deferred.await(secondStarted).pipe(Effect.timeout("1 second"));

      expect(refreshCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("ignores health scheduler refresh requests after close", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "0 millis",
      );

      yield* scheduler.close;
      yield* scheduler.request;

      expect(refreshCount).toBe(0);
    }),
  );
});
