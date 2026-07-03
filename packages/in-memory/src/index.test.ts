import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect, Schema, Stream } from "effect";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";
import { createInMemoryViewServerTesting, makeInMemoryViewServerTesting } from "./testing";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({
        routeBy: ["id"],
      }),
    },
  },
});

const materializedGrpcSourceViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpc.materialized(),
    },
  },
});

const kafkaOwnedViewServer = defineViewServerConfig({
  kafka: {
    usa: "localhost:9092",
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(Order),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
        }),
      }),
    },
  },
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

  it.effect("testing adapter subscribes to leased gRPC topics through the internal live seam", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServerTesting(leasedViewServer, {});
      const subscription = yield* inMemory.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          id: { eq: "order-1" },
        },
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
    }),
  );

  it.effect("blocks public mutations for source-owned in-memory topics", () =>
    Effect.gen(function* () {
      const kafkaInMemory = yield* makeInMemoryViewServer(kafkaOwnedViewServer, {});
      const grpcInMemory = yield* makeInMemoryViewServer(materializedGrpcSourceViewServer, {});
      const kafkaTesting = yield* makeInMemoryViewServerTesting(kafkaOwnedViewServer, {});
      const grpcTesting = yield* makeInMemoryViewServerTesting(
        materializedGrpcSourceViewServer,
        {},
      );

      const publicKafkaSnapshot = yield* kafkaInMemory.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const publicGrpcSnapshot = yield* grpcInMemory.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      yield* kafkaTesting.client.publish("orders", { id: "kafka", price: 10 });
      yield* grpcTesting.client.publish("orders", { id: "grpc", price: 20 });
      const kafkaSnapshot = yield* kafkaTesting.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const grpcSnapshot = yield* grpcTesting.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const kafkaPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaInMemory.client.publish,
        kafkaInMemory.client,
        ["orders", { id: "blocked", price: 30 }],
      );
      const kafkaPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaInMemory.client.publishMany,
        kafkaInMemory.client,
        ["orders", [{ id: "blocked-many", price: 35 }]],
      );
      const kafkaPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaInMemory.client.patch,
        kafkaInMemory.client,
        ["orders", "kafka", { price: 35 }],
      );
      const kafkaDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaInMemory.client.delete,
        kafkaInMemory.client,
        ["orders", "kafka"],
      );
      const grpcPublishEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcInMemory.client.publish,
        grpcInMemory.client,
        ["orders", { id: "blocked", price: 40 }],
      );
      const grpcPublishManyEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcInMemory.client.publishMany,
        grpcInMemory.client,
        ["orders", [{ id: "blocked-many", price: 45 }]],
      );
      const grpcPatchEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcInMemory.client.patch,
        grpcInMemory.client,
        ["orders", "grpc", { price: 45 }],
      );
      const grpcDeleteEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcInMemory.client.delete,
        grpcInMemory.client,
        ["orders", "grpc"],
      );
      const kafkaResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        kafkaInMemory.client.reset,
        kafkaInMemory.client,
        [],
      );
      const grpcResetEffect: Effect.Effect<void, ViewServerRuntimeError> = Reflect.apply(
        grpcInMemory.client.reset,
        grpcInMemory.client,
        [],
      );
      const kafkaPublish = yield* Effect.flip(kafkaPublishEffect);
      const kafkaPublishMany = yield* Effect.flip(kafkaPublishManyEffect);
      const kafkaPatch = yield* Effect.flip(kafkaPatchEffect);
      const kafkaDelete = yield* Effect.flip(kafkaDeleteEffect);
      const grpcPublish = yield* Effect.flip(grpcPublishEffect);
      const grpcPublishMany = yield* Effect.flip(grpcPublishManyEffect);
      const grpcPatch = yield* Effect.flip(grpcPatchEffect);
      const grpcDelete = yield* Effect.flip(grpcDeleteEffect);
      const kafkaReset = yield* Effect.flip(kafkaResetEffect);
      const grpcReset = yield* Effect.flip(grpcResetEffect);

      expect(publicKafkaSnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      expect(publicGrpcSnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      expect(kafkaSnapshot).toStrictEqual({
        rows: [{ id: "kafka", price: 10 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(grpcSnapshot).toStrictEqual({
        rows: [{ id: "grpc", price: 20 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(kafkaPublish).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(kafkaPublishMany).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(kafkaPatch).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(kafkaDelete).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(grpcPublish).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(grpcPublishMany).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(grpcPatch).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(grpcDelete).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      });
      expect(kafkaReset).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        message:
          "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
      });
      expect(grpcReset).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        message:
          "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
      });

      yield* kafkaInMemory.close;
      yield* grpcInMemory.close;
      yield* kafkaTesting.close;
      yield* grpcTesting.close;
    }),
  );

  it.effect("supports synchronous testing adapter construction", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServerTesting(viewServer);

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect("subscribeRuntime" in inMemory.liveClient).toBe(true);

      yield* inMemory.close;
    }),
  );
});
