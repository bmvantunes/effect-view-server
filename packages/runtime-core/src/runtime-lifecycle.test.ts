import { describe, expect, it } from "@effect/vitest";
import { createColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { healthFromEngine } from "./health";
import { makeViewServerRuntimeCore } from "./index";
import { makeRuntimeCoreLiveClient } from "./live-client";
import { makeRuntimeCorePushedHealthHub } from "./pushed-health";
import { acquireRuntimeCoreResourceHandoff } from "./subscription-handoff";
import { engineHealth } from "./test-support/runtime-test-fixtures";

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Schema.Struct({
        id: Schema.String,
        value: Schema.Number,
      }),
      key: "id",
    },
  },
});

describe("Runtime Core lifecycle", () => {
  it.effect("releases the pushed-health hub once across concurrent public close owners", () =>
    Effect.gen(function* () {
      let healthBuildCount = 0;
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health) => {
          healthBuildCount += 1;
          return health;
        },
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const streamFiber = yield* summary.events.pipe(
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true }),
      );

      expect(runtimeCore.liveClient.close).toBe(runtimeCore.close);
      expect(runtimeCore.serverLiveClient.close).toBe(runtimeCore.close);

      yield* Effect.all([runtimeCore.close, runtimeCore.liveClient.close], {
        concurrency: "unbounded",
      });
      yield* Fiber.join(streamFiber);
      const freshHealthAfterClose = yield* runtimeCore.client.health();

      expect({
        cachedStatus: runtimeCore.liveClient.health.value.status,
        freshStatus: freshHealthAfterClose.status,
        healthBuildCount,
      }).toStrictEqual({
        cachedStatus: "stopping",
        freshStatus: "stopping",
        healthBuildCount: 2,
      });
    }),
  );

  it.effect(
    "releases interrupted and last subscribers once while keeping the shared hub reusable",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
          healthRefreshCadence: "1 second",
        });
        const interrupted = yield* runtimeCore.liveClient.subscribeHealthSummary();
        const remaining = yield* runtimeCore.serverLiveClient.subscribeHealthSummary();
        const interruptedStarted = yield* Deferred.make<void>();
        const interruptedFiber = yield* interrupted.events.pipe(
          Stream.tap(() => Deferred.succeed(interruptedStarted, undefined)),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true }),
        );
        const remainingFiber = yield* remaining.events.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );

        yield* Deferred.await(interruptedStarted);
        yield* Fiber.interrupt(interruptedFiber);
        const interruptedTail = yield* interrupted.events.pipe(
          Stream.runCollect,
          Effect.timeout("1 second"),
        );
        expect(Array.from(interruptedTail)).toStrictEqual([]);
        yield* interrupted.close();
        yield* interrupted.close();
        yield* runtimeCore.client.publish("orders", { id: "after-interrupt", value: 1 });
        yield* TestClock.adjust("1 second");

        const remainingEvents = Array.from(yield* Fiber.join(remainingFiber));
        expect(
          remainingEvents.map((event) => (event.type === "snapshot" ? event.version : undefined)),
        ).toStrictEqual([0, 1]);

        yield* remaining.close();
        yield* remaining.close();
        const replacement = yield* runtimeCore.liveClient.subscribeHealth();
        const replacementEvent = yield* replacement.events.pipe(Stream.runHead);
        expect(replacementEvent._tag).toBe("Some");

        yield* replacement.close();
        yield* runtimeCore.close;
      }),
  );

  it.effect("schedules a claimed health refresh when the request fiber is interrupted", () =>
    Effect.gen(function* () {
      const claimStarted = yield* Deferred.make<void>();
      const initialEventSeen = yield* Deferred.make<void>();
      const releaseClaim = yield* Deferred.make<void>();
      let readCount = 0;
      const hub = yield* makeRuntimeCorePushedHealthHub(
        healthFromEngine(engineHealth("ready", 0)),
        Effect.sync(() => {
          readCount += 1;
          return healthFromEngine(engineHealth("ready", 1));
        }),
        "1 second",
        {
          afterRefreshEpochClaim: Deferred.succeed(claimStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseClaim)),
          ),
        },
      );
      const detail = yield* hub.subscribeHealth();
      const eventsFiber = yield* detail.events.pipe(
        Stream.tap(() => Deferred.succeed(initialEventSeen, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(initialEventSeen);
      const requestFiber = yield* hub.requestRefresh.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(claimStarted);
      const interruptFiber = yield* Fiber.interrupt(requestFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseClaim, undefined);
      yield* Fiber.join(interruptFiber);
      yield* TestClock.adjust("1 second");
      const events = Array.from(yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second")));

      expect({
        readCount,
        rowCounts: events.map((event) =>
          event.type === "snapshot" ? event.rows[0]?.rowCount : undefined,
        ),
      }).toStrictEqual({
        readCount: 1,
        rowCounts: [0, 1],
      });
      yield* detail.close();
      yield* hub.close;
    }),
  );

  it.effect("ends a claimed health subscription close when the close fiber is interrupted", () =>
    Effect.gen(function* () {
      const claimStarted = yield* Deferred.make<void>();
      const releaseClaim = yield* Deferred.make<void>();
      let closeClaimCount = 0;
      const initialHealth = healthFromEngine(engineHealth("ready", 0));
      const hub = yield* makeRuntimeCorePushedHealthHub(
        initialHealth,
        Effect.succeed(initialHealth),
        "1 minute",
        {
          afterSubscriptionCloseClaim: Effect.suspend(() => {
            closeClaimCount += 1;
            return Deferred.succeed(claimStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseClaim)),
            );
          }),
        },
      );
      const summary = yield* hub.subscribeHealthSummary();
      const closeFiber = yield* summary.close().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(claimStarted);
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseClaim, undefined);
      yield* Fiber.join(interruptFiber);
      yield* summary.events.pipe(Stream.runDrain, Effect.timeout("1 second"));
      yield* summary.close();

      expect(closeClaimCount).toBe(1);
      yield* hub.close;
    }),
  );

  it.effect("releases hub streams once when a health listener throws during close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const streamFiber = yield* summary.events.pipe(
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true }),
      );
      let listenerCallCount = 0;
      const unsubscribe = runtimeCore.liveClient.health.subscribe(() => {
        listenerCallCount += 1;
        throw new Error("health listener failed");
      });

      const firstClose = yield* Effect.exit(runtimeCore.close);
      yield* Fiber.join(streamFiber).pipe(Effect.timeout("1 second"));
      const secondClose = yield* Effect.exit(runtimeCore.close);

      expect(Exit.isFailure(firstClose)).toBe(true);
      expect(Exit.isFailure(secondClose)).toBe(true);
      expect(listenerCallCount).toBe(1);
      unsubscribe();
    }),
  );

  it.effect("closes an interrupted acquired resource handoff exactly once", () =>
    Effect.gen(function* () {
      const acquired = yield* Deferred.make<void>();
      const keepHandoffOpen = yield* Deferred.make<void>();
      let closeCount = 0;
      const resource = {
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };
      const handoffFiber = yield* acquireRuntimeCoreResourceHandoff(
        (markAcquired) =>
          Effect.gen(function* () {
            yield* markAcquired(resource);
            return "ready";
          }),
        {
          beforeReturn: Deferred.succeed(acquired, undefined).pipe(
            Effect.andThen(Deferred.await(keepHandoffOpen)),
          ),
        },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(acquired);

      yield* Fiber.interrupt(handoffFiber);
      yield* Fiber.interrupt(handoffFiber);

      expect(closeCount).toBe(1);
    }),
  );

  it.effect("releases an interrupted pushed-health subscription handoff", () =>
    Effect.gen(function* () {
      const handoffStarted = yield* Deferred.make<void>();
      const keepHandoffOpen = yield* Deferred.make<void>();
      let handoffCount = 0;
      const initialHealth = healthFromEngine(engineHealth("ready", 0));
      const hub = yield* makeRuntimeCorePushedHealthHub(
        initialHealth,
        Effect.succeed(initialHealth),
        "1 minute",
        {
          subscriptionHandoff: {
            beforeReturn: Effect.suspend(() => {
              handoffCount += 1;
              return handoffCount === 1
                ? Deferred.succeed(handoffStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(keepHandoffOpen)),
                  )
                : Effect.void;
            }),
          },
        },
      );
      const interruptedFiber = yield* hub
        .subscribeHealthSummary()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(handoffStarted);

      yield* Fiber.interrupt(interruptedFiber);
      const replacement = yield* hub.subscribeHealthSummary();
      const replacementEvent = yield* replacement.events.pipe(Stream.runHead);

      expect(replacementEvent._tag).toBe("Some");
      expect(handoffCount).toBe(2);
      yield* replacement.close();
      yield* hub.close;
    }),
  );

  it.effect("keeps stopping health when an in-flight hub refresh finishes after close", () =>
    Effect.gen(function* () {
      const refreshStarted = yield* Deferred.make<void>();
      const releaseRefresh = yield* Deferred.make<void>();
      let readCount = 0;
      const hub = yield* makeRuntimeCorePushedHealthHub(
        healthFromEngine(engineHealth("ready", 0)),
        Effect.gen(function* () {
          readCount += 1;
          yield* Deferred.succeed(refreshStarted, undefined);
          yield* Deferred.await(releaseRefresh);
          return healthFromEngine(engineHealth("ready", 1));
        }),
        "1 minute",
      );
      const staleRefreshFiber = yield* hub.refresh.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(refreshStarted);

      yield* hub.close;
      yield* Deferred.succeed(releaseRefresh, undefined);
      const staleRefresh = yield* Fiber.join(staleRefreshFiber);
      const afterClose = yield* hub.refresh;
      yield* hub.requestRefresh;

      expect({
        afterCloseStatus: afterClose.status,
        cachedStatus: hub.health.value.status,
        readCount,
        staleRefreshStatus: staleRefresh.status,
      }).toStrictEqual({
        afterCloseStatus: "stopping",
        cachedStatus: "stopping",
        readCount: 1,
        staleRefreshStatus: "stopping",
      });
      yield* hub.close;
    }),
  );

  it.effect("releases engine subscriptions when pushed-health handoff is interrupted", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const initialHealth = healthFromEngine(yield* engine.health());
      const hub = yield* makeRuntimeCorePushedHealthHub(
        initialHealth,
        Effect.succeed(initialHealth),
        "1 minute",
      );
      const refreshStarted = yield* Deferred.make<void>();
      const liveClient = yield* makeRuntimeCoreLiveClient(viewServer, engine, {
        ...hub,
        requestRefresh: Deferred.succeed(refreshStarted, undefined).pipe(
          Effect.andThen(Effect.never),
        ),
      });
      const subscriptionFiber = yield* liveClient
        .subscribeInternal("orders", { select: ["id"] })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(refreshStarted);
      expect((yield* engine.health()).activeSubscriptions).toBe(1);

      yield* Fiber.interrupt(subscriptionFiber);

      expect((yield* engine.health()).activeSubscriptions).toBe(0);
      yield* hub.close;
      yield* engine.close();
    }),
  );

  it.effect("retries a superseded in-flight hub refresh at the newest request epoch", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const secondReadStarted = yield* Deferred.make<void>();
      const releaseSecondRead = yield* Deferred.make<void>();
      let readCount = 0;
      const reads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return healthFromEngine(engineHealth("ready", 1));
        }),
        Effect.gen(function* () {
          yield* Deferred.succeed(secondReadStarted, undefined);
          yield* Deferred.await(releaseSecondRead);
          return healthFromEngine(engineHealth("ready", 2));
        }),
      ];
      const hub = yield* makeRuntimeCorePushedHealthHub(
        healthFromEngine(engineHealth("ready", 0)),
        Effect.suspend(() => {
          const read =
            reads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
          readCount += 1;
          return read;
        }),
        "1 minute",
      );

      yield* hub.requestRefresh;
      const firstRefresh = yield* hub.refresh.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstReadStarted);
      yield* hub.requestRefresh;
      const secondRefresh = yield* hub.refresh.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(secondReadStarted);
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseSecondRead, undefined);
      const [firstHealth, secondHealth] = yield* Effect.all(
        [Fiber.join(firstRefresh), Fiber.join(secondRefresh)],
        { concurrency: 2 },
      );

      expect({
        cachedRowCount: hub.health.value.engine.topics.orders.rowCount,
        firstRowCount: firstHealth.engine.topics.orders.rowCount,
        readCount,
        secondRowCount: secondHealth.engine.topics.orders.rowCount,
      }).toStrictEqual({
        cachedRowCount: 2,
        firstRowCount: 2,
        readCount: 2,
        secondRowCount: 2,
      });
      yield* hub.close;
    }),
  );

  it.effect("retries health subscription registration when a refresh request wins the race", () =>
    Effect.gen(function* () {
      const registrationStarted = yield* Deferred.make<void>();
      const releaseRegistration = yield* Deferred.make<void>();
      let registrationCount = 0;
      let readCount = 0;
      const hub = yield* makeRuntimeCorePushedHealthHub(
        healthFromEngine(engineHealth("ready", 0)),
        Effect.sync(() => {
          readCount += 1;
          return healthFromEngine(engineHealth("ready", 1));
        }),
        "1 minute",
        {
          beforeSubscriptionRegistration: Effect.suspend(() => {
            registrationCount += 1;
            return registrationCount === 1
              ? Deferred.succeed(registrationStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseRegistration)),
                )
              : Effect.void;
          }),
        },
      );
      const subscriptionFiber = yield* hub
        .subscribeHealth()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(registrationStarted);

      yield* hub.requestRefresh;
      yield* Deferred.succeed(releaseRegistration, undefined);
      const subscription = yield* Fiber.join(subscriptionFiber);
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const snapshots = Array.from(events).filter((event) => event.type === "snapshot");

      expect({
        registrationCount,
        readCount,
        rowCount: snapshots[0]?.rows[0]?.rowCount,
      }).toStrictEqual({
        registrationCount: 2,
        readCount: 1,
        rowCount: 1,
      });
      yield* subscription.close();
      yield* hub.close;
    }),
  );

  it.effect("rejects a pending health subscription when Core closes during its refresh", () =>
    Effect.gen(function* () {
      const refreshStarted = yield* Deferred.make<void>();
      const releaseRefresh = yield* Deferred.make<void>();
      const hub = yield* makeRuntimeCorePushedHealthHub(
        healthFromEngine(engineHealth("ready", 0)),
        Effect.gen(function* () {
          yield* Deferred.succeed(refreshStarted, undefined);
          yield* Deferred.await(releaseRefresh);
          return healthFromEngine(engineHealth("ready", 1));
        }),
        "1 minute",
      );
      yield* hub.requestRefresh;
      const subscriptionFiber = yield* hub
        .subscribeHealthSummary()
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(refreshStarted).pipe(Effect.timeout("1 second"));

      yield* hub.close;
      yield* Deferred.succeed(releaseRefresh, undefined);
      const closedSubscription = yield* Fiber.join(subscriptionFiber).pipe(
        Effect.timeout("1 second"),
      );

      expect(closedSubscription).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "Runtime Core is closed.",
      });
    }),
  );

  it.effect("rejects new pushed-health subscriptions after Runtime Core close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.close;

      const [summaryError, detailError] = yield* Effect.all([
        Effect.flip(runtimeCore.liveClient.subscribeHealthSummary()),
        Effect.flip(runtimeCore.serverLiveClient.subscribeHealth()),
      ]);

      expect({ detailError, summaryError }).toStrictEqual({
        detailError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Runtime Core is closed.",
        },
        summaryError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Runtime Core is closed.",
        },
      });
    }),
  );
});
