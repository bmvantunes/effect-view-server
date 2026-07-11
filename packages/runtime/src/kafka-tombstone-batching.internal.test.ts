import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Option, Schema } from "effect";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import {
  processKafkaMessage,
  processKafkaMessageBatch,
  ViewServerKafkaIngressError,
} from "./kafka-ingress";
import { resolveViewServerRuntimeOptions } from "./runtime-options";

import {
  IncomingOrder,
  IncomingTrade,
  kafkaProcessorMessage,
  Order,
  Trade,
} from "../test-harness/kafka-source-fixtures";

import { kafkaBootstrapServers } from "../test-harness/kafka-e2e";

describe("Kafka tombstone batching contracts", () => {
  it.effect("rejects undefined topic-owned values without deleting rows", () =>
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
          consumerGroupId: "view-server-topic-owned-undefined-value",
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
      const error = yield* Effect.flip(
        processKafkaMessage(
          topicOwnedViewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          health,
          "local",
          kafkaProcessorMessage({
            key: "order-1",
            topic: "orders-source",
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(error).toStrictEqual(
        new ViewServerKafkaIngressError({
          message: "Failed to decode Kafka message for source topic orders-source",
          cause: "missing-kafka-value",
          region: "local",
          sourceTopic: "orders-source",
        }),
      );
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

  it.effect("commits and skips topic-owned tombstones without Kafka keys", () =>
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
          consumerGroupId: "view-server-topic-owned-null-key-tombstone",
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

      const committedOffsets: Array<bigint> = [];
      const keylessTombstone = kafkaProcessorMessage({
        key: null,
        offset: 41n,
        topic: "orders-source",
        value: null,
      });

      yield* processKafkaMessage(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        {
          ...keylessTombstone,
          commit: () => {
            committedOffsets.push(keylessTombstone.offset);
          },
        },
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      const degradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 41);
      yield* runtimeCore.close;

      expect(committedOffsets).toStrictEqual([41n]);
      expect({
        committedOffset:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.committedOffset,
        lastError: degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.lastError,
        mappingFailures:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.mappingFailuresPerSecond,
      }).toStrictEqual({
        committedOffset: "42",
        lastError: "Kafka source key bytes are required",
        mappingFailures: 1,
      });
      expect(snapshot).toStrictEqual({
        version: 0,
        rows: [],
        totalRows: 0,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("flushes valid topic-owned batch prefix and commits keyless tombstones", () =>
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
          consumerGroupId: "view-server-topic-owned-keyless-tombstone-prefix",
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
      const committedOffsets: Array<bigint> = [];
      const validMessage = kafkaProcessorMessage({
        key: "order-1",
        offset: 1n,
        topic: "orders-source",
        value: JSON.stringify({
          customerId: "customer-1",
          price: 10,
        }),
      });
      const keylessTombstone = kafkaProcessorMessage({
        key: null,
        offset: 2n,
        topic: "orders-source",
        value: null,
      });

      yield* processKafkaMessageBatch(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        [
          {
            ...validMessage,
            commit: () => {
              committedOffsets.push(validMessage.offset);
            },
          },
          {
            ...keylessTombstone,
            commit: () => {
              committedOffsets.push(keylessTombstone.offset);
            },
          },
        ],
      );
      const ordersSnapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      const degradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2);
      yield* runtimeCore.close;

      expect(committedOffsets).toStrictEqual([1n, 2n]);
      expect({
        committedOffset:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.committedOffset,
        lastError: degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.lastError,
        mappingFailures:
          degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.mappingFailuresPerSecond,
      }).toStrictEqual({
        committedOffset: "3",
        lastError: "Kafka source key bytes are required",
        mappingFailures: 1,
      });
      expect(ordersSnapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            customerId: "customer-1",
            id: "order-1",
            price: 10,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("preserves Kafka order when a batch mixes topic-owned source rows and tombstones", () =>
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
          consumerGroupId: "view-server-topic-owned-mixed-tombstone-batch",
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

      yield* processKafkaMessageBatch(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        [
          kafkaProcessorMessage({
            key: "order-1",
            offset: 1n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
          kafkaProcessorMessage({
            key: "order-1",
            offset: 2n,
            topic: "orders-source",
            value: null,
          }),
        ],
      );
      const ordersSnapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(ordersSnapshot).toStrictEqual({
        version: 2,
        rows: [],
        totalRows: 0,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("batches contiguous upsert runs around topic-owned tombstones", () =>
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
          consumerGroupId: "view-server-topic-owned-tombstone-contiguous-upsert-runs",
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

      yield* processKafkaMessageBatch(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        [
          kafkaProcessorMessage({
            key: "order-1",
            offset: 1n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
          kafkaProcessorMessage({
            key: "order-2",
            offset: 2n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-2",
              price: 20,
            }),
          }),
          kafkaProcessorMessage({
            key: "order-1",
            offset: 3n,
            topic: "orders-source",
            value: null,
          }),
          kafkaProcessorMessage({
            key: "order-3",
            offset: 4n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-3",
              price: 30,
            }),
          }),
          kafkaProcessorMessage({
            key: "order-4",
            offset: 5n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-4",
              price: 40,
            }),
          }),
        ],
      );
      const ordersSnapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(ordersSnapshot).toStrictEqual({
        version: 3,
        rows: [
          {
            customerId: "customer-2",
            id: "order-2",
            price: 20,
          },
          {
            customerId: "customer-3",
            id: "order-3",
            price: 30,
          },
          {
            customerId: "customer-4",
            id: "order-4",
            price: 40,
          },
        ],
        totalRows: 3,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("preserves Kafka order when a topic-owned tombstone is followed by a source row", () =>
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
          consumerGroupId: "view-server-topic-owned-tombstone-then-row-batch",
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

      yield* processKafkaMessageBatch(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        [
          kafkaProcessorMessage({
            key: "order-1",
            offset: 1n,
            topic: "orders-source",
            value: null,
          }),
          kafkaProcessorMessage({
            key: "order-1",
            offset: 2n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
        ],
      );
      const ordersSnapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(ordersSnapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            customerId: "customer-1",
            id: "order-1",
            price: 10,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect("publishes topic-owned Kafka upserts when a batch also has tombstones", () =>
    Effect.gen(function* () {
      const regions = {
        local: kafkaBootstrapServers,
      };
      const mixedSourceViewServer = defineViewServerConfig({
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
          trades: {
            schema: Trade,
            key: "id",
            kafkaSource: kafka.source({
              topic: "trades-source",
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingTrade)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                symbol: value.symbol,
                quantity: value.quantity,
              }),
            }),
          },
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(mixedSourceViewServer, {
        kafka: {
          consumerGroupId: "view-server-mixed-direct-upsert-topic-owned-tombstone",
        },
      });
      const kafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(mixedSourceViewServer, {});
      const health = makeViewServerKafkaHealthLedger<typeof mixedSourceViewServer.topics>({
        regions: kafkaOptions.regions,
        startFrom: kafkaOptions.consume,
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          "trades-source": {
            regions: ["local"],
            viewServerTopic: "trades",
          },
        },
      });

      yield* processKafkaMessage(
        mixedSourceViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        kafkaProcessorMessage({
          key: "trade-1",
          offset: 0n,
          topic: "trades-source",
          value: JSON.stringify({
            quantity: 50,
            symbol: "AAPL",
          }),
        }),
      );
      yield* processKafkaMessageBatch(
        mixedSourceViewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        health,
        "local",
        [
          kafkaProcessorMessage({
            key: "order-1",
            offset: 1n,
            topic: "orders-source",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
          }),
          kafkaProcessorMessage({
            key: "trade-1",
            offset: 2n,
            topic: "trades-source",
            value: null,
          }),
          kafkaProcessorMessage({
            key: "trade-2",
            offset: 3n,
            topic: "trades-source",
            value: JSON.stringify({
              quantity: 25,
              symbol: "MSFT",
            }),
          }),
        ],
      );
      const ordersSnapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        limit: 10,
      });
      const tradesSnapshot = yield* runtimeCore.client.snapshot("trades", {
        select: ["id", "symbol", "quantity"],
        limit: 10,
      });
      yield* runtimeCore.close;

      expect(ordersSnapshot).toStrictEqual({
        version: 1,
        rows: [
          {
            customerId: "customer-1",
            id: "order-1",
            price: 10,
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(tradesSnapshot).toStrictEqual({
        version: 3,
        rows: [
          {
            id: "trade-2",
            quantity: 25,
            symbol: "MSFT",
          },
        ],
        totalRows: 1,
        status: "ready",
        statusCode: "Ready",
      });
    }),
  );

  it.effect(
    "commits and skips topic-owned Kafka source rows when Kafka key bytes are missing",
    () =>
      Effect.gen(function* () {
        const regions = {
          local: kafkaBootstrapServers,
        };
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
        const resolved = yield* resolveViewServerRuntimeOptions(kafkaBackedViewServer, {
          kafka: {
            consumerGroupId: "view-server-direct-null-key-upsert",
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
        const committedOffsets: Array<bigint> = [];
        const keylessMessage = kafkaProcessorMessage({
          key: null,
          offset: 1n,
          topic: "orders-source",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
        });

        yield* processKafkaMessage(
          kafkaBackedViewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          health,
          "local",
          {
            ...keylessMessage,
            commit: () => {
              committedOffsets.push(keylessMessage.offset);
            },
          },
        );
        const snapshot = yield* runtimeCore.client.snapshot("orders", {
          select: ["id", "customerId", "price"],
          limit: 10,
        });
        const degradedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 41);
        yield* runtimeCore.close;

        expect(committedOffsets).toStrictEqual([1n]);
        expect({
          committedOffset:
            degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.committedOffset,
          lastError: degradedHealth.kafka?.topics["orders-source"]?.regions["local"]?.lastError,
          mappingFailures:
            degradedHealth.kafka?.topics["orders-source"]?.regions["local"]
              ?.mappingFailuresPerSecond,
        }).toStrictEqual({
          committedOffset: "2",
          lastError: "Kafka source key bytes are required",
          mappingFailures: 1,
        });
        expect(snapshot).toStrictEqual({
          version: 0,
          rows: [],
          totalRows: 0,
          status: "ready",
          statusCode: "Ready",
        });
      }),
  );
});
