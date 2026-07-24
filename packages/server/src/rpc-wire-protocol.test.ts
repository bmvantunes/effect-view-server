import { describe, expect, it } from "@effect/vitest";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { SourceFixture } from "@effect-view-server/source-adapter-testing";
import { Deferred, Effect, Fiber, Layer, Option, Schedule, Stream } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { makeViewServerWebSocketServer } from "./index";
import {
  createServerTestRuntime,
  makeServerTransportLifecycleProbe,
  Order,
  order,
  quote,
  trade,
  viewServer,
} from "../test-harness/server";

describe("Real View Server RPC wire protocol composition", () => {
  it.live("serves an in-memory runtime through Effect RPC WebSocket", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));

      yield* inMemory.client.publish("orders", order("b", 20));
      yield* inMemory.client.publishMany("orders", [order("a", 10)]);

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "b", row: { id: "b", price: 20 }, index: 0 }],
        totalRows: 1,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 2,
      });

      const healthSummarySubscription = yield* client.subscribeHealthSummary();
      yield* Effect.addFinalizer(() => healthSummarySubscription.close().pipe(Effect.orDie));
      const healthSummaryEvents = yield* healthSummarySubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      const healthSummarySnapshots = Array.from(healthSummaryEvents).filter(
        (event) => event.type === "snapshot",
      );
      expect(healthSummarySnapshots[0]?.rows[0]?.runtimeStatus).toBe("ready");
      expect(healthSummarySnapshots[0]?.rows[0]?.connectionStatus).toBe("connected");
      yield* healthSummarySubscription.close();

      const healthSubscription = yield* client.subscribeHealth();
      yield* Effect.addFinalizer(() => healthSubscription.close().pipe(Effect.orDie));
      const healthEvents = yield* healthSubscription.events.pipe(Stream.take(1), Stream.runCollect);
      const healthSnapshots = Array.from(healthEvents).filter((event) => event.type === "snapshot");
      expect(healthSnapshots[0]?.rows[0]?.rowCount).toBe(2);
      yield* healthSubscription.close();

      yield* inMemory.client.reset();
      expect((yield* inMemory.client.health()).engine.topics.orders.rowCount).toBe(0);

      yield* lifecycle.awaitCount("closedStreams", 3);
      const afterClose = yield* inMemory.client.health();
      expect(afterClose.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* client.close;
      yield* lifecycle.awaitCount("closedClients", 1);
      expect(yield* lifecycle.readCounts).toStrictEqual({
        openedClients: 1,
        closedClients: 1,
        openedStreams: 3,
        closedStreams: 3,
      });
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("isolates an exhausted Source Topic from an unrelated Topic over WebSocket", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const config = defineViewServerConfig({
        topics: {
          source_orders: {
            schema: Order,
            source: fixture.materializedSource(
              {
                label: "websocket-source-isolation",
              },
              Schedule.recurs(0),
            ),
          },
          manual_orders: {
            schema: Order,
            key: "id",
          },
        },
      });
      const fixtureContext = yield* Layer.build(fixture.layer);
      const runtime = yield* makeViewServerRuntimeCoreInternal(config, {}).pipe(
        Effect.provideContext(fixtureContext),
      );
      yield* Effect.addFinalizer(() => runtime.close);
      const server = yield* makeViewServerWebSocketServer(config, {
        liveClient: runtime.serverLiveClient,
        runtime: runtime.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(config, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const manualSubscription = yield* client.subscribe("manual_orders", {
        select: ["id", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      yield* Effect.addFinalizer(() => manualSubscription.close().pipe(Effect.orDie));
      const sourceHealth = yield* client.subscribeSourceHealth("source_orders");
      yield* Effect.addFinalizer(() => sourceHealth.close().pipe(Effect.orDie));
      const initialSnapshotSeen = yield* Deferred.make<void>();
      const manualEvents = yield* manualSubscription.events.pipe(
        Stream.tap((event) =>
          event.type === "snapshot"
            ? Deferred.succeed(initialSnapshotSeen, undefined)
            : Effect.void,
        ),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const exhausted = yield* sourceHealth.events.pipe(
        Stream.filter((health) => health.status._tag === "Exhausted"),
        Stream.take(1),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
        Effect.forkChild,
      );
      yield* Deferred.await(initialSnapshotSeen).pipe(Effect.timeout("1 second"));
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });

      yield* fixture.controls.fail(
        { _tag: "Materialized" },
        SourceFixture.failure("source topic failed", "stream"),
      );
      const exhaustedHealth = yield* Fiber.join(exhausted);
      yield* runtime.client.publish("manual_orders", {
        id: "survives",
        price: 42,
      });
      const events = yield* Fiber.join(manualEvents);

      if (exhaustedHealth.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected exhausted Source Health.");
      }
      const { startedAtNanos, lastAttemptStartedAtNanos, lastTerminationAtNanos } =
        exhaustedHealth.metrics.runtime;
      const { exhaustedAtNanos } = exhaustedHealth.status;
      const { sampledAtNanos } = exhaustedHealth;
      expect([
        typeof startedAtNanos,
        typeof lastAttemptStartedAtNanos,
        typeof lastTerminationAtNanos,
        typeof exhaustedAtNanos,
        typeof sampledAtNanos,
      ]).toStrictEqual(["bigint", "bigint", "bigint", "bigint", "bigint"]);
      expect(exhaustedHealth).toStrictEqual({
        adapter: {
          name: "controllable-fixture",
          version: "1",
        },
        target: {
          _tag: "Materialized",
        },
        status: {
          _tag: "Exhausted",
          exhaustion: {
            _tag: "RetryExhausted",
            lastTermination: {
              _tag: "Failed",
              failure: {
                _tag: "AdapterFailure",
                failure: {
                  _tag: "SourceFixtureFailure",
                  message: "source topic failed",
                  phase: "stream",
                },
              },
            },
          },
          exhaustedAtNanos,
        },
        metrics: {
          runtime: {
            startedAtNanos,
            lastAttemptStartedAtNanos,
            lastDeliveryAtNanos: null,
            lastRejectionAtNanos: null,
            lastAppliedMutationAtNanos: null,
            lastTerminationAtNanos,
            currentAttempt: 1n,
            retryCount: 0n,
            receivedDeliveryCount: 0n,
            rejectedItemCount: 0n,
            attemptedMutationCount: 0n,
            appliedUpsertCount: 0n,
            appliedDeleteCount: 0n,
            failedMutationCount: 0n,
            completedSettlementCount: 0n,
            failedSettlementCount: 0n,
            retainedRowCount: 0,
            lanes: [
              {
                id: "fixture",
                buffer: {
                  _tag: "Unbuffered",
                },
              },
            ],
          },
          adapter: {
            observed: 0n,
          },
        },
        sampledAtNanos,
      });
      expect(Array.from(events, (event) => event.type)).toStrictEqual(["snapshot", "delta"]);
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "manual_orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "survives",
            row: {
              id: "survives",
              price: 42,
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      yield* sourceHealth.close();
      yield* manualSubscription.close();
      yield* client.close;
      yield* server.close;
      yield* runtime.close;
    }).pipe(Effect.scoped),
  );

  it.live("round-trips BigInt rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const subscription = yield* client.subscribe("trades", {
        where: [{ field: "quantity", type: "greaterThanOrEqual", filter: 10n }],
        select: ["id", "quantity"],
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));

      yield* inMemory.client.publish("trades", trade("a", 5n));
      yield* inMemory.client.publish("trades", trade("b", 10n));

      const events = yield* Fiber.join(eventsFiber);
      yield* lifecycle.awaitCount("closedStreams", 1);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "trades",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "trades",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [{ type: "insert", key: "b", row: { id: "b", quantity: 10n }, index: 0 }],
        totalRows: 1,
      });

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("round-trips BigDecimal rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const subscription = yield* client.subscribe("quotes", {
        where: [
          {
            field: "price",
            type: "greaterThanOrEqual",
            filter: fromStringUnsafe("10.50"),
          },
        ],
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));

      yield* inMemory.client.publish("quotes", quote("a", "9.99"));
      yield* inMemory.client.publish("quotes", quote("b", "10.50"));

      const events = yield* Fiber.join(eventsFiber);
      yield* lifecycle.awaitCount("closedStreams", 1);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "quotes",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "quotes",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "b",
            row: { id: "b", price: fromStringUnsafe("10.5") },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("encodes snapshot rows, move/remove deltas, and close statuses", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);

      yield* inMemory.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));

      yield* inMemory.client.patch("orders", "a", { price: 30 });
      yield* inMemory.client.delete("orders", "b");

      const events = yield* Fiber.join(eventsFiber);
      yield* lifecycle.awaitCount("closedStreams", 1);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a", "b"],
        rows: [
          { id: "a", price: 10 },
          { id: "b", price: 20 },
        ],
        totalRows: 2,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "move", key: "a", fromIndex: 0, toIndex: 1 },
          { type: "update", key: "a", row: { id: "a", price: 30 }, index: 1 },
        ],
        totalRows: 2,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [{ type: "remove", key: "b" }],
        totalRows: 1,
      });

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("encodes subscription closed status when the runtime closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));

      yield* inMemory.close;

      const events = yield* Fiber.join(eventsFiber);
      yield* lifecycle.awaitCount("closedStreams", 1);
      expect(events[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine closed.",
      });

      yield* client.close;
      yield* server.close;
    }).pipe(Effect.scoped),
  );

  it.live("composes with the public runtime-core live client", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.addFinalizer(() => runtimeCore.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: runtimeCore.serverLiveClient,
        runtime: runtimeCore.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      yield* runtimeCore.client.publish("orders", order("public-core", 10));

      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.orDie));
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["public-core"],
          rows: [
            {
              id: "public-core",
              price: 10,
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* client.close;
      yield* server.close;
      yield* runtimeCore.close;
    }).pipe(Effect.scoped),
  );
});
