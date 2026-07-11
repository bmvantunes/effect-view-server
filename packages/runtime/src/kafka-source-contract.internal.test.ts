import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Option, Schema } from "effect";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import { processKafkaMessage, ViewServerKafkaIngressError } from "./kafka-ingress";
import { resolveViewServerRuntimeOptions } from "./runtime-options";

import {
  IncomingOrder,
  IncomingTransformedOrder,
  kafkaProcessorMessage,
  Order,
  TransformedOrder,
} from "../test-harness/kafka-source-fixtures";

import { kafkaBootstrapServers } from "../test-harness/kafka-e2e";

describe("Kafka source mapping contracts", () => {
  it.effect("rejects topic-owned Kafka source messages for missing View Server topics", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const corruptedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
          },
          ghost: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "ghost-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(corruptedViewServer, {
        kafka: {
          consumerGroupId: "view-server-missing-topic-guard",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(corruptedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof corruptedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "ghost-source": {
            regions: ["local"],
            viewServerTopic: "ghost",
          },
        },
      });

      Reflect.deleteProperty(corruptedViewServer.topics, "ghost");
      const error = yield* Effect.flip(
        processKafkaMessage(
          corruptedViewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          health,
          "local",
          kafkaProcessorMessage({
            key: "ghost-1",
            topic: "ghost-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
        ),
      );
      yield* runtimeCore.close;

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message: "Kafka source references unknown View Server topic: ghost",
          cause: "missing-view-server-topic",
          region: "local",
          sourceTopic: "ghost-source",
        }),
      );
    }),
  );

  it.effect("decodes topic-owned Kafka source messages into runtime rows", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-decode",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            price: 10,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("validates topic-owned Kafka mapped rows with decoded transform schema values", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          transformedOrders: {
            schema: TransformedOrder,
            key: "id",
            kafkaSource: kafka.source({
              topic: "transformed-orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingTransformedOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                quantity: value.quantity,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-transformed-mapped-row",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "transformed-orders-source": {
            regions: ["local"],
            viewServerTopic: "transformedOrders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "transformed-order-1",
          topic: "transformed-orders-source",
          value: JSON.stringify({
            quantity: "9007199254740993",
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("transformedOrders", {
        select: ["id", "quantity"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            id: "transformed-order-1",
            quantity: 9007199254740993n,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("uses Kafka source row keys as source-owned storage keys", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-storage-key",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        }),
      );
      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-2",
            price: 20,
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 2,
        rows: [
          {
            id: "order-1",
            customerId: "customer-2",
            price: 20,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("deletes topic-owned rows when Kafka emits tombstones", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const topicOwnedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-tombstone",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(topicOwnedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        }),
      );
      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "order-1",
          topic: "orders-source",
          value: null,
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        version: 2,
        rows: [],
        totalRows: 0,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("validates topic-owned Kafka mapped rows with decoded transform schema values", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const kafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          transformedOrders: {
            schema: TransformedOrder,
            key: "id",
            kafkaSource: kafka.source({
              topic: "transformed-orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingTransformedOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                quantity: value.quantity,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-direct-transformed-mapped-row",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(kafkaBackedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof kafkaBackedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "transformed-orders-source": {
            regions: ["local"],
            viewServerTopic: "transformedOrders",
          },
        },
      });

      yield* processKafkaMessage(
        kafkaBackedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "transformed-order-1",
          offset: 1n,
          topic: "transformed-orders-source",
          value: JSON.stringify({
            quantity: "9007199254740993",
          }),
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("transformedOrders", {
        select: ["id", "quantity"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(snapshot).toStrictEqual({
        rows: [
          {
            id: "transformed-order-1",
            quantity: 9_007_199_254_740_993n,
          },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("validates topic-owned Kafka mapped rows before publishing", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const invalidOrder: typeof Order.Type = Object.assign(Object.create(null), { price: 10 });
      const kafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
        },
      });
      Object.defineProperty(kafkaBackedViewServer.topics.orders.kafkaSource, "map", {
        value: () => invalidOrder,
      });
      const resolved = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
        kafka: {
          consumerGroupId: "view-server-direct-invalid-mapped-row",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(kafkaBackedViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof kafkaBackedViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      const error = yield* Effect.flip(
        processKafkaMessage(
          kafkaBackedViewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          health,
          "local",
          kafkaProcessorMessage({
            key: "order-invalid",
            offset: 1n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-invalid",
              price: 10,
            }),
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      const degradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 0);
      yield* runtimeCore.close;

      expect(error).toBeInstanceOf(ViewServerKafkaIngressError);
      expect(error.message).toContain("Failed to map Kafka message for source topic orders-source");
      expect({
        decodeFailures:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.decodeFailuresPerSecond,
        lastError: degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.lastError,
        mappingFailures:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.mappingFailuresPerSecond,
      }).toStrictEqual({
        decodeFailures: 0,
        lastError: "Kafka mapped row failed topic schema",
        mappingFailures: 1,
      });
      expect(snapshot).toStrictEqual({
        rows: [],
        status: "ready",
        statusCode: "Ready",
        totalRows: 0,
        version: 0,
      });
    }),
  );
});
