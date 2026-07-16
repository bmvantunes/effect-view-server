import { describe, expect, it } from "@effect/vitest";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Deferred, Effect, Fiber, Stream } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { makeViewServerWebSocketServer } from "./index";
import {
  createServerTestRuntime,
  makeServerTransportLifecycleProbe,
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
        where: {
          quantity: { gte: 10n },
        },
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
        where: {
          price: { gte: fromStringUnsafe("10.50") },
        },
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
