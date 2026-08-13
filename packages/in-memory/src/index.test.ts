import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceFixture } from "@effect-view-server/source-adapter-testing";
import { Effect, Fiber, Schema, Stream } from "effect";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";
import { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } from "./testing";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const sourceOwnedMutationError = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "UnsupportedQuery",
  topic,
  message:
    "Source-owned topics do not support direct runtime mutations; publish through the configured Source Adapter or use an externally-published topic.",
});

describe("@effect-view-server/in-memory", () => {
  it.effect("adapts the shared runtime core to the public in-memory API", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {
        subscriptionQueueCapacity: 8,
      });
      const subscription = yield* inMemory.liveClient.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });

      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);
      const health = yield* inMemory.client.health();

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
        operations: [
          {
            type: "insert",
            key: "order-1",
            row: { id: "order-1", price: 10 },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(health.engine.topics.orders.rowCount).toBe(1);

      yield* subscription.close();
      yield* inMemory.close;
    }),
  );

  it.effect("supports the synchronous public in-memory constructor", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect("subscribeRuntime" in inMemory.liveClient).toBe(false);
      yield* inMemory.close;
    }),
  );

  it.effect("rejects decorated query arrays through the in-memory runtime", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const decoratedOrderBy = [{ field: "price", direction: "asc" }];
      Object.defineProperty(decoratedOrderBy, "metadata", { enumerable: true, value: true });
      const decoratedGroupBy = ["price"];
      Object.defineProperty(decoratedGroupBy, "metadata", { enumerable: true, value: true });
      const decoratedRawQuery = { select: ["id"], orderBy: decoratedOrderBy };
      const decoratedGroupedQuery = {
        groupBy: decoratedGroupBy,
        aggregates: { rowCount: { aggFunc: "count" } },
      };

      const snapshotError = yield* Effect.flip(
        // @ts-expect-error hostile untyped callers can still pass decorated query arrays.
        inMemory.client.snapshot("orders", decoratedRawQuery),
      );
      const subscriptionError = yield* Effect.flip(
        // @ts-expect-error hostile untyped callers can still pass decorated query arrays.
        inMemory.liveClient.subscribe("orders", decoratedRawQuery),
      );
      const groupedError = yield* Effect.flip(
        // @ts-expect-error hostile untyped callers can still pass decorated query arrays.
        inMemory.client.snapshot("orders", decoratedGroupedQuery),
      );

      expect(snapshotError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Raw query orderBy must be a dense array without extra properties.",
        topic: "orders",
      });
      expect(subscriptionError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Raw query orderBy must be a dense array without extra properties.",
        topic: "orders",
      });
      expect(groupedError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Grouped query groupBy must be a non-empty array of strings.",
        topic: "orders",
      });

      yield* inMemory.close;
    }),
  );

  it.effect("forwards grouped admission limits through the public in-memory API", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
      });
      yield* inMemory.client.publishMany("orders", [
        { id: "order-1", price: 10 },
        { id: "order-2", price: 20 },
      ]);
      const subscription = yield* inMemory.liveClient.subscribe("orders", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const health = yield* inMemory.client.health();
      expect(health.engine.topics.orders.activeFallbackGroupedViews).toBe(1);
      expect(health.engine.topics.orders.activeIncrementalGroupedViews).toBe(0);

      yield* subscription.close();
      yield* inMemory.close;
    }),
  );

  it.effect("ignores smuggled runtime-core transport health options", () =>
    Effect.gen(function* () {
      const widenedOptions = {
        subscriptionQueueCapacity: 8,
        transportHealth: () => ({
          activeClients: 99,
          activeStreams: 99,
          activeSubscriptions: 99,
          messagesPerSecond: 99,
          bytesPerSecond: 99,
          queuedMessages: 99,
          queuedBytes: 99,
          droppedClients: 99,
          backpressureEvents: 99,
          reconnects: 99,
          lastError: "should not leak",
        }),
      };
      const inMemory = createInMemoryViewServer(viewServer, widenedOptions);
      const health = yield* inMemory.client.health();

      expect(health.transport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
      yield* inMemory.close;
    }),
  );

  it.effect("live client close owns shared runtime core cleanup", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {
        healthRefreshCadence: "1 minute",
      });

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      yield* inMemory.liveClient.close;

      const health = yield* inMemory.client.health();
      expect(health.status).toBe("stopping");
      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* inMemory.close;
    }),
  );

  it.effect("testing adapter exposes runtime subscriptions without widening mutation client", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServerTesting(viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
        healthRefreshCadence: "1 minute",
        subscriptionQueueCapacity: 8,
      });

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      const runtimeSubscription = yield* inMemory.liveClient.subscribeRuntime("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const events = yield* runtimeSubscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["order-1"],
        rows: [{ id: "order-1", price: 10 }],
        totalRows: 1,
      });
      expect("subscribeRuntime" in inMemory.client).toBe(false);

      yield* runtimeSubscription.close();
      yield* inMemory.close;
    }),
  );

  it.effect(
    "testing adapter subscribes to leased Source topics through the internal live seam",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Order);
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: fixture.leasedSource(["id"], {
                label: "testing-public-query",
              }),
            },
          },
        });
        return yield* Effect.gen(function* () {
          const inMemory = yield* makeInMemoryViewServerTesting(config, {});
          const subscription = yield* inMemory.liveClient.subscribe("orders", {
            select: ["id", "price"],
            where: [{ field: "id", type: "equals", filter: "order-1" }],
            routeBy: { id: "order-1" },
            limit: 10,
          });
          const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

          expect(events[0]).toStrictEqual({
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          });

          yield* subscription.close();
          yield* inMemory.close;
        }).pipe(Effect.provide(fixture.layer));
      }),
  );

  it.effect("exposes direct in-memory Source Health and closes observers with active leases", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.leasedSource(["id"], {
              label: "direct-in-memory-source-health",
            }),
          },
        },
      });
      const target = {
        _tag: "Leased",
        route: { id: "order-1" },
      } satisfies Parameters<typeof fixture.controls.counts>[0];

      return yield* Effect.gen(function* () {
        const inMemory = yield* makeInMemoryViewServer(config, {});
        const inactiveDiagnostics = yield* inMemory.liveClient.subscribeSourceHealth({
          topic: "orders",
          routeBy: { id: "order-1" },
        });
        const inactive = yield* inactiveDiagnostics.events.pipe(Stream.take(1), Stream.runCollect);

        expect(Array.from(inactive)).toStrictEqual([
          {
            _tag: "Inactive",
            route: { id: "order-1" },
          },
        ]);
        expect(fixture.controls.counts(target)).toStrictEqual({
          acquisitions: 0n,
          finalizations: 0n,
        });
        yield* inactiveDiagnostics.close();

        const live = yield* inMemory.liveClient.subscribe("orders", {
          select: ["id", "price"],
          routeBy: { id: "order-1" },
        });
        yield* fixture.controls.awaitActive(target);
        const activeDiagnostics = yield* inMemory.liveClient.subscribeSourceHealth({
          topic: "orders",
          routeBy: { id: "order-1" },
        });
        const active = yield* activeDiagnostics.events.pipe(
          Stream.map((health) =>
            health._tag === "Inactive"
              ? health
              : {
                  _tag: health._tag,
                  route: health.route,
                  adapter: health.health.adapter,
                  adapterMetrics: health.health.metrics.adapter,
                },
          ),
          Stream.take(1),
          Stream.runCollect,
        );

        expect(Array.from(active)).toStrictEqual([
          {
            _tag: "Active",
            route: { id: "order-1" },
            adapter: {
              name: "controllable-fixture",
              version: "1",
            },
            adapterMetrics: { observed: 0n },
          },
        ]);
        yield* activeDiagnostics.close();

        const shutdownDiagnostics = yield* inMemory.liveClient.subscribeSourceHealth({
          topic: "orders",
          routeBy: { id: "order-1" },
        });
        const observerFiber = yield* shutdownDiagnostics.events.pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;

        yield* inMemory.close;
        yield* Fiber.join(observerFiber);
        yield* fixture.controls.awaitCounts(target, {
          acquisitions: 1n,
          finalizations: 1n,
        });
        yield* shutdownDiagnostics.close();
        yield* live.close();

        expect(fixture.controls.counts(target)).toStrictEqual({
          acquisitions: 1n,
          finalizations: 1n,
        });
      }).pipe(Effect.provide(fixture.layer));
    }),
  );

  it.effect("testing adapter keeps routeBy on runtime-erased leased subscriptions", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.leasedSource(["id"], {
              label: "testing-runtime-query",
            }),
          },
        },
      });
      return yield* Effect.gen(function* () {
        const inMemory = yield* makeInMemoryViewServerTesting(config, {});
        const subscription = yield* inMemory.liveClient.subscribeRuntime("orders", {
          select: ["id", "price"],
          where: [{ field: "id", type: "equals", filter: "order-1" }],
          routeBy: { id: "order-1" },
          limit: 10,
        });
        const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

        expect(events[0]).toStrictEqual({
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        });

        yield* subscription.close();
        yield* inMemory.close;
      }).pipe(Effect.provide(fixture.layer));
    }),
  );

  it.effect("blocks public mutations for source-owned in-memory topics", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Order);
      const firstSourceViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.materializedSource({
              label: "first-source-owned-runtime",
            }),
          },
        },
      });
      const secondSourceViewServer = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: fixture.materializedSource({
              label: "second-source-owned-runtime",
            }),
          },
        },
      });
      return yield* Effect.gen(function* () {
        const firstInMemory = yield* makeInMemoryViewServer(firstSourceViewServer, {});
        const secondInMemory = yield* makeInMemoryViewServer(secondSourceViewServer, {});
        const firstTesting = yield* makeInMemoryViewServerTesting(firstSourceViewServer, {});
        const secondTesting = yield* makeInMemoryViewServerTesting(secondSourceViewServer, {});

        const firstPublicSnapshot = yield* firstInMemory.client.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const secondPublicSnapshot = yield* secondInMemory.client.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        yield* firstTesting.client.publish("orders", { id: "first", price: 10 });
        yield* secondTesting.client.publish("orders", { id: "second", price: 20 });
        const firstTestingSnapshot = yield* firstTesting.client.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const secondTestingSnapshot = yield* secondTesting.client.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });
        const firstPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          firstInMemory.client.publish,
          firstInMemory.client,
          ["orders", { id: "blocked", price: 30 }],
        );
        const firstPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          firstInMemory.client.publishMany,
          firstInMemory.client,
          ["orders", [{ id: "blocked-many", price: 35 }]],
        );
        const firstPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          firstInMemory.client.patch,
          firstInMemory.client,
          ["orders", "first", { price: 35 }],
        );
        const firstDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          firstInMemory.client.delete,
          firstInMemory.client,
          ["orders", "first"],
        );
        const secondPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          secondInMemory.client.publish,
          secondInMemory.client,
          ["orders", { id: "blocked", price: 40 }],
        );
        const secondPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          secondInMemory.client.publishMany,
          secondInMemory.client,
          ["orders", [{ id: "blocked-many", price: 45 }]],
        );
        const secondPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          secondInMemory.client.patch,
          secondInMemory.client,
          ["orders", "second", { price: 45 }],
        );
        const secondDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          secondInMemory.client.delete,
          secondInMemory.client,
          ["orders", "second"],
        );
        const firstResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          firstInMemory.client.reset,
          firstInMemory.client,
          [],
        );
        const secondResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
          secondInMemory.client.reset,
          secondInMemory.client,
          [],
        );
        const firstPublish = yield* Effect.flip(firstPublishEffect);
        const firstPublishMany = yield* Effect.flip(firstPublishManyEffect);
        const firstPatch = yield* Effect.flip(firstPatchEffect);
        const firstDelete = yield* Effect.flip(firstDeleteEffect);
        const secondPublish = yield* Effect.flip(secondPublishEffect);
        const secondPublishMany = yield* Effect.flip(secondPublishManyEffect);
        const secondPatch = yield* Effect.flip(secondPatchEffect);
        const secondDelete = yield* Effect.flip(secondDeleteEffect);
        const firstReset = yield* Effect.flip(firstResetEffect);
        const secondReset = yield* Effect.flip(secondResetEffect);

        expect(firstPublicSnapshot).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 0,
          status: "ready",
          statusCode: "Ready",
        });
        expect(secondPublicSnapshot).toStrictEqual({
          rows: [],
          totalRows: 0,
          version: 0,
          status: "ready",
          statusCode: "Ready",
        });
        expect(firstTestingSnapshot).toStrictEqual({
          rows: [{ id: "first", price: 10 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(secondTestingSnapshot).toStrictEqual({
          rows: [{ id: "second", price: 20 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(firstPublish).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(firstPublishMany).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(firstPatch).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(firstDelete).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(secondPublish).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(secondPublishMany).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(secondPatch).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(secondDelete).toStrictEqual(sourceOwnedMutationError("orders"));
        expect(firstReset).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          message:
            "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
        });
        expect(secondReset).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          message:
            "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
        });

        yield* firstInMemory.close;
        yield* secondInMemory.close;
        yield* firstTesting.close;
        yield* secondTesting.close;
      }).pipe(Effect.provide(fixture.layer));
    }),
  );

  it.effect("supports synchronous testing adapter construction", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServerTesting(viewServer);

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect("subscribeRuntime" in inMemory.liveClient).toBe(true);
      expect("subscribeProtocolQuery" in inMemory.serverLiveClient).toBe(true);

      yield* inMemory.close;
    }),
  );
});
