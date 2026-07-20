import { describe, expect, it } from "@effect/vitest";
import { createColumnLiveViewEngineInternal } from "@effect-view-server/column-live-view-engine/internal";
import { Deferred, Effect, Exit, Fiber, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine } from "./health";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import { makeRuntimeCoreLiveClientModule } from "./live-client";
import { order, refreshFailed, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect(
    "keeps canonical filtered subscriptions alive across the acquisition health refresh",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
        const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
          where: [{ field: "id", type: "equals", filter: "missing" }],
          orderBy: [{ field: "price", direction: "asc" }],
          select: ["id", "price"],
          limit: 10,
        });

        const health = yield* runtimeCore.client.health();
        expect(health.engine.topics.orders.activeSubscriptions).toBe(1);

        yield* subscription.close();
        yield* runtimeCore.close;
      }),
  );

  it.effect("forwards grouped admission limits to the engine", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
      });
      yield* runtimeCore.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.activeFallbackGroupedViews).toBe(1);
      expect(health.engine.topics.orders.activeIncrementalGroupedViews).toBe(0);

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("subscribes through the runtime live-client entrypoint", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publish("orders", order("a", 10));

      const subscription = yield* runtimeCore.liveClient.subscribeRuntime("orders", {
        select: ["id", "price"],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", price: 10 }],
        totalRows: 1,
      });

      yield* subscription.close();
      const health = yield* runtimeCore.internalClient.health();
      expect(health.engine.topics.orders.activeSubscriptions).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("refreshes pushed health when an event consumer stops without explicit close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publish("orders", order("a", 10));
      const subscription = yield* runtimeCore.liveClient.subscribeRuntime("orders", {
        select: ["id"],
      });

      expect(runtimeCore.liveClient.health.value.engine.topics.orders.activeSubscriptions).toBe(1);
      yield* subscription.events.pipe(Stream.take(1), Stream.runDrain);
      expect(runtimeCore.liveClient.health.value.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* runtimeCore.close;
    }),
  );

  it.effect("forwards producer terminal observation through the internal live client", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {
        subscriptionQueueCapacity: 1,
      });
      const phases: Array<string> = [];
      const subscription = yield* runtimeCore.internalLiveClient.subscribeObservedInternal(
        "orders",
        {
          select: ["id"],
          limit: 10,
        },
        {
          onQueryRegistered: (queryId) =>
            Effect.sync(() => {
              phases.push(`registered:${queryId}`);
            }),
          onTerminalOccurrence: () =>
            Effect.sync(() => {
              phases.push("occurrence");
            }),
          onTerminalReady: () =>
            Effect.sync(() => {
              phases.push("ready");
            }),
        },
      );

      yield* runtimeCore.internalClient.publish("orders", order("observed", 10));
      const events = yield* subscription.events.pipe(Stream.runCollect);
      expect(phases).toStrictEqual(["registered:query-0", "occurrence", "ready"]);
      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "query-0",
          status: "closed",
          code: "BackpressureExceeded",
          message: "Subscription closed because its event queue exceeded capacity.",
        },
      ]);

      const runtimeQueryIds: Array<string> = [];
      const runtimeSubscription =
        yield* runtimeCore.internalLiveClient.subscribeRuntimeObservedInternal(
          "orders",
          { select: ["id"] },
          {
            onQueryRegistered: (queryId) =>
              Effect.sync(() => {
                runtimeQueryIds.push(queryId);
              }),
            onTerminalOccurrence: () => Effect.void,
            onTerminalReady: () => Effect.void,
          },
        );
      expect(runtimeQueryIds).toStrictEqual(["query-1"]);
      yield* runtimeSubscription.close();
      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("releases observed subscriptions when initial health refresh fails", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
        viewServer,
        engine,
        health,
        Effect.fail(refreshFailed),
      );
      const observer = {
        onQueryRegistered: () => Effect.void,
        onTerminalOccurrence: () => Effect.void,
        onTerminalReady: () => Effect.void,
      };

      const failedTyped = yield* liveClient
        .subscribeObservedInternal(
          "orders",
          {
            select: ["id"],
            limit: 1,
          },
          observer,
        )
        .pipe(Effect.flip);
      const afterTypedFailure = yield* engine.health();
      const failedRuntime = yield* liveClient
        .subscribeRuntimeObservedInternal(
          "orders",
          {
            select: ["id"],
            limit: 1,
          },
          observer,
        )
        .pipe(Effect.flip);
      const afterRuntimeFailure = yield* engine.health();

      expect({
        failedTyped,
        typedSubscriptions: afterTypedFailure.activeSubscriptions,
        failedRuntime,
        runtimeSubscriptions: afterRuntimeFailure.activeSubscriptions,
      }).toStrictEqual({
        failedTyped: refreshFailed,
        typedSubscriptions: 0,
        failedRuntime: refreshFailed,
        runtimeSubscriptions: 0,
      });
      yield* liveClient.close;
    }),
  );

  it.effect("releases acquired live subscriptions when initial health refresh fails", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
        viewServer,
        engine,
        health,
        Effect.fail(refreshFailed),
      );

      const failedRaw = yield* Effect.flip(
        liveClient.subscribe("orders", {
          select: ["id"],
          limit: 1,
        }),
      );
      const afterRawFailure = yield* engine.health();
      expect(failedRaw).toStrictEqual(refreshFailed);
      expect(afterRawFailure.activeSubscriptions).toBe(0);

      const failedRuntime = yield* Effect.flip(
        liveClient.subscribeRuntime("orders", {
          select: ["id"],
          limit: 1,
        }),
      );
      const afterRuntimeFailure = yield* engine.health();
      expect(failedRuntime).toStrictEqual(refreshFailed);
      expect(afterRuntimeFailure.activeSubscriptions).toBe(0);

      yield* liveClient.close;
    }),
  );

  it.effect(
    "releases an acquired live subscription when initial health refresh is interrupted",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
        const health = AtomRef.make(healthFromEngine(yield* engine.health()));
        const refreshStarted = yield* Deferred.make<void>();
        const allowRefresh = yield* Deferred.make<void>();
        const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
          viewServer,
          engine,
          health,
          Deferred.succeed(refreshStarted, undefined).pipe(
            Effect.andThen(Deferred.await(allowRefresh)),
            Effect.as(health.value),
          ),
        );

        const subscribeFiber = yield* liveClient
          .subscribeRuntime("orders", { select: ["id"], limit: 1 })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(refreshStarted).pipe(Effect.timeout("1 second"));
        const interruptFiber = yield* Fiber.interrupt(subscribeFiber).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(allowRefresh, undefined);
        yield* Fiber.join(interruptFiber).pipe(Effect.timeout("1 second"));
        const subscribeExit = yield* Fiber.await(subscribeFiber);
        const afterInterrupt = yield* engine.health();

        expect({
          activeSubscriptions: afterInterrupt.activeSubscriptions,
          interrupted: Exit.hasInterrupts(subscribeExit),
        }).toStrictEqual({ activeSubscriptions: 0, interrupted: true });
        yield* liveClient.close;
      }),
  );

  it.effect("releases pushed health subscriptions when initial health refresh fails", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const { liveClient } = yield* makeRuntimeCoreLiveClientModule(
        viewServer,
        engine,
        health,
        Effect.fail(refreshFailed),
      );

      const failedSummary = yield* Effect.flip(liveClient.subscribeHealthSummary());
      const failedDetail = yield* Effect.flip(liveClient.subscribeHealth());

      expect(failedSummary).toStrictEqual(refreshFailed);
      expect(failedDetail).toStrictEqual(refreshFailed);
      yield* liveClient.close.pipe(Effect.timeout("1 second"));
    }),
  );

  it.effect("refreshes health after close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "0 millis",
      });
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const ready = yield* runtimeCore.client.health();
      expect(ready.status).toBe("ready");

      yield* runtimeCore.liveClient.close;

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
      expect(closed.engine.topics.orders.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("live client close owns pending runtime health refresh cleanup", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 minute",
      });

      yield* runtimeCore.client.publish("orders", order("a", 10));
      yield* runtimeCore.liveClient.close;

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
      expect(closed.engine.topics.orders.rowCount).toBe(1);
      yield* runtimeCore.close;
    }),
  );
});
