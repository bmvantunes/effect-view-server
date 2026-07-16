import { describe, expect, it } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal as makeViewServerRuntimeCore } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import * as BigDecimal from "effect/BigDecimal";
import { Effect, Exit, Schema, Scope } from "effect";
import {
  makeViewServerKafkaIngress,
  messageFromUnknown,
  processKafkaMessage,
  registerKafkaConsumerHealthListeners,
} from "./kafka-ingress";
import { makeViewServerKafkaHealthObserver } from "./kafka-health-observation";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./kafka-runtime-options";
import {
  committedKafkaStart,
  failingClient,
  forgedMappingTagCodecError,
  IncomingOrder,
  IncomingPrecisePosition,
  kafkaMessage,
  kafkaOptions,
  kafkaOptionsForConfig,
  makeViewServerKafkaHealthLedger,
  nonStringTagCodecError,
  nullRecord,
  Order,
  ordersSourceTopic,
  PrecisePosition,
  regions,
  type Topics,
  unknownSourceTopic,
  viewServer,
} from "../test-harness/kafka-ingress";

describe("Kafka ingress source processing internals", () => {
  it.effect("creates a no-op ingress when no regions own source topics", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const emptyKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-empty-test",
        ...committedKafkaStart("view-server-empty-test"),
        regions: {
          cold: "localhost:9093",
        },
        topics: {},
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        startFrom: emptyKafkaOptions.consume,
        regions: emptyKafkaOptions.regions,
        topics: {},
      });
      const observer = yield* makeViewServerKafkaHealthObserver(ledger, Effect.void);

      const ingress = yield* makeViewServerKafkaIngress(
        viewServer,
        runtimeCore.internalClient,
        emptyKafkaOptions,
        observer,
      );
      yield* ingress.close;
      yield* ingress.close;
      yield* observer.close;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(health.status).toBe("ready");
      expect(health.kafka).toStrictEqual({
        startFrom: emptyKafkaOptions.consume,
        regions: nullRecord({}),
        topics: nullRecord({}),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves dangerous Kafka health source topic and region keys", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const dangerousRegions: Record<string, string> = Object.create(null);
      dangerousRegions["__proto__"] = "localhost:9092";
      const dangerousTopics: Record<
        string,
        {
          readonly viewServerTopic: "orders";
          readonly regions: ReadonlyArray<string>;
        }
      > = Object.create(null);
      dangerousTopics["__proto__"] = {
        regions: ["__proto__"],
        viewServerTopic: "orders",
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: dangerousRegions,
        topics: dangerousTopics,
      });

      yield* ledger.regionConnected("__proto__", 1_000);
      yield* ledger.topicConnected("__proto__", "__proto__", 2, 1_000);
      yield* ledger.messageDecoded("__proto__", "__proto__", {
        bytes: 12,
        committedOffset: "4",
        nowMillis: 2_000,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const expectedRegions: Record<string, unknown> = Object.create(null);
      expectedRegions["__proto__"] = {
        status: "connected",
        brokers: "localhost:9092",
        lastConnectedAt: 1_000,
        lastError: null,
      };
      const expectedTopicRegions: Record<string, unknown> = Object.create(null);
      expectedTopicRegions["__proto__"] = {
        connected: true,
        assignedPartitions: 2,
        messagesPerSecond: 1,
        bytesPerSecond: 12,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        commitFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 2_000,
        lastCommitAt: 2_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "4",
        lastError: null,
      };
      const expectedTopics: Record<string, unknown> = Object.create(null);
      expectedTopics["__proto__"] = {
        status: "ready",
        sourceTopic: "__proto__",
        viewServerTopic: "orders",
        regions: expectedTopicRegions,
      };

      expect(Object.hasOwn(health.kafka?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(health.kafka?.topics ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(health.kafka?.topics["__proto__"]?.regions ?? {}, "__proto__")).toBe(
        true,
      );
      expect(health.kafka).toStrictEqual({
        startFrom: kafkaOptions.consume,
        regions: expectedRegions,
        topics: expectedTopics,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not run Kafka listener callbacks after their scope closes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-scoped-listener-test",
        groupId: "view-server-scoped-listener-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        "local",
        [ordersSourceTopic],
        scope,
      );

      yield* Scope.close(scope, Exit.void);
      consumer.emit("consumer:group:join", {
        groupId: "view-server-scoped-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      consumer.emit("consumer:group:rebalance", {
        groupId: "view-server-scoped-listener-test",
      });
      consumer.emit("consumer:lag:error", new Error("late lag failure"));
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        region: health.kafka?.regions["local"],
        topic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        region: {
          status: "starting",
          brokers: regions.local,
          lastConnectedAt: null,
          lastError: null,
        },
        topic: {
          status: "starting",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 0,
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
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: null,
            },
          }),
        },
      });

      yield* listenerRegistration.close;
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("fails Kafka ingress startup when Kafka consumer cannot start", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const invalidKafkaOptions = kafkaOptionsForConfig(
        viewServer,
        "view-server-invalid-broker-test",
        {
          local: "",
        },
      );
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        startFrom: invalidKafkaOptions.consume,
        regions: invalidKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const observer = yield* makeViewServerKafkaHealthObserver(ledger, Effect.void);

      const exit = yield* Effect.exit(
        makeViewServerKafkaIngress(
          viewServer,
          runtimeCore.internalClient,
          invalidKafkaOptions,
          observer,
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);

      yield* observer.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("processes source messages into runtime rows and Kafka health", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      let committedMessages = 0;
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.internalClient,
        kafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: unknownSourceTopic,
          key: "ignored",
          value: "{}",
        }),
      );
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.internalClient,
        kafkaOptions,
        ledger,
        "cold",
        kafkaMessage({
          topic: ordersSourceTopic,
          key: "wrong-region",
          value: JSON.stringify({
            customerId: "wrong-region",
            price: 999,
          }),
          offset: 9n,
          onCommit: () => {
            committedMessages += 1;
          },
        }),
      );
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.internalClient,
        kafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
          headers: new Map([[Buffer.from("trace"), Buffer.from("abc")]]),
          offset: 1n,
          onCommit: () => {
            committedMessages += 1;
          },
        }),
      );
      const decodeExit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.internalClient,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "bad-json",
            value: "{",
            offset: 2n,
            onCommit: () => {
              committedMessages += 1;
            },
          }),
        ),
      );

      const publishExit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          failingClient,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-2",
            value: JSON.stringify({
              customerId: "customer-2",
              price: 20,
            }),
            offset: 3n,
            onCommit: () => {
              committedMessages += 1;
            },
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(decodeExit),
        publishFailed: Exit.isFailure(publishExit),
        committedMessages,
        snapshot,
      }).toStrictEqual({
        decodeFailed: true,
        publishFailed: true,
        committedMessages: 1,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-1",
              customerId: "customer-1",
              price: 10,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 3,
            bytesPerSecond: 99,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 1,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 1,
            lastMessageAt: 0,
            lastCommitAt: 0,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "2",
            lastError: "publish failed",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("ignores keyless Kafka messages for unknown source topics and wrong regions", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      let committedMessages = 0;

      yield* processKafkaMessage(
        viewServer,
        runtimeCore.internalClient,
        kafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: unknownSourceTopic,
          key: null,
          value: null,
          offset: 1n,
          onCommit: () => {
            committedMessages += 1;
          },
        }),
      );
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.internalClient,
        kafkaOptions,
        ledger,
        "cold",
        kafkaMessage({
          topic: ordersSourceTopic,
          key: null,
          value: null,
          offset: 2n,
          onCommit: () => {
            committedMessages += 1;
          },
        }),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        committedMessages,
        topicHealth: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        committedMessages: 0,
        topicHealth: {
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
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
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: null,
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves high-precision Kafka JSON values through runtime snapshots", () =>
    Effect.gen(function* () {
      const preciseSourceTopic = "precise-position-source";
      const preciseViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          precisePositions: {
            schema: PrecisePosition,
            key: "id",
            kafkaSource: kafka.source({
              topic: preciseSourceTopic,
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingPrecisePosition)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: ({ value }) => ({
                accountId: value.accountId,
                quantity: value.quantity,
                price: value.price,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(preciseViewServer, {});
      const preciseKafkaOptions = kafkaOptionsForConfig(
        preciseViewServer,
        "view-server-precise-json-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof preciseViewServer.topics>({
        startFrom: preciseKafkaOptions.consume,
        regions: preciseKafkaOptions.regions,
        topics: {
          [preciseSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "precisePositions",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(preciseSourceTopic, "local", 1, 1_000);

      yield* processKafkaMessage(
        preciseViewServer,
        runtimeCore.internalClient,
        preciseKafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: preciseSourceTopic,
          key: "position-precise-1",
          value: JSON.stringify({
            accountId: "account-precise-1",
            quantity: "9007199254740993",
            price: "1234567890.123456789",
          }),
          offset: 12n,
        }),
      );
      const snapshot = yield* runtimeCore.client.snapshot("precisePositions", {
        select: ["id", "accountId", "quantity", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        ...snapshot,
        rows: snapshot.rows.map((row) => ({
          ...row,
          price: BigDecimal.format(row.price),
        })),
      }).toStrictEqual({
        status: "ready",
        statusCode: "Ready",
        rows: [
          {
            id: "position-precise-1",
            accountId: "account-precise-1",
            quantity: 9007199254740993n,
            price: "1234567890.123456789",
          },
        ],
        totalRows: 1,
        version: 1,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records mapping failures separately from decode failures", () =>
    Effect.gen(function* () {
      const throwingViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.json(() => Schema.toCodecJson(IncomingOrder)),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: (): never => {
                throw new Error("mapping failed");
              },
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(throwingViewServer, {});
      const throwingKafkaOptions = kafkaOptionsForConfig(
        throwingViewServer,
        "view-server-mapping-failure-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof throwingViewServer.topics>({
        startFrom: throwingKafkaOptions.consume,
        regions: throwingKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const error = yield* Effect.flip(
        processKafkaMessage(
          throwingViewServer,
          runtimeCore.internalClient,
          throwingKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-mapping-failed",
            value: JSON.stringify({
              customerId: "customer-mapping-failed",
              price: 70,
            }),
            offset: 7n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          causeMessage: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          causeMessage: "Failed to map Kafka payload",
          message: `Failed to map Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 71,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 1,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "Failed to map Kafka payload",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records untagged codec failures as decode failures", () =>
    Effect.gen(function* () {
      const untaggedDecodeViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "untagged-error",
                decode: () => Effect.fail(nonStringTagCodecError),
              }),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: () => ({
                customerId: "unused",
                price: 0,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(untaggedDecodeViewServer, {});
      const untaggedDecodeKafkaOptions = kafkaOptionsForConfig(
        untaggedDecodeViewServer,
        "view-server-untagged-decode-failure-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof untaggedDecodeViewServer.topics>({
        startFrom: untaggedDecodeKafkaOptions.consume,
        regions: untaggedDecodeKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          untaggedDecodeViewServer,
          runtimeCore.internalClient,
          untaggedDecodeKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-untagged-codec-failed",
            value: JSON.stringify({
              customerId: "customer-untagged-codec-failed",
              price: 90,
            }),
            offset: 8n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(exit),
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        decodeFailed: true,
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 85,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "non-string tag",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not classify custom codec errors as mapping failures by public tag alone", () =>
    Effect.gen(function* () {
      const forgedMappingTagViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "forged-mapping-tag-error",
                decode: () => Effect.fail(forgedMappingTagCodecError),
              }),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: () => ({
                customerId: "unused",
                price: 0,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(forgedMappingTagViewServer, {});
      const forgedMappingTagKafkaOptions = kafkaOptionsForConfig(
        forgedMappingTagViewServer,
        "view-server-forged-mapping-tag-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof forgedMappingTagViewServer.topics>({
        startFrom: forgedMappingTagKafkaOptions.consume,
        regions: forgedMappingTagKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const error = yield* Effect.flip(
        processKafkaMessage(
          forgedMappingTagViewServer,
          runtimeCore.internalClient,
          forgedMappingTagKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-forged-mapping-tag-codec-failed",
            value: JSON.stringify({
              customerId: "customer-forged-mapping-tag-codec-failed",
              price: 95,
            }),
            offset: 9n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          message: error.message,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          sourceTopic: ordersSourceTopic,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 105,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "forged mapping tag",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records primitive codec failures as decode failures", () =>
    Effect.gen(function* () {
      const primitiveDecodeViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "primitive-error",
                decode: () => Effect.fail("raw codec failed"),
              }),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: () => ({
                customerId: "unused",
                price: 0,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(primitiveDecodeViewServer, {});
      const primitiveDecodeKafkaOptions = kafkaOptionsForConfig(
        primitiveDecodeViewServer,
        "view-server-primitive-decode-failure-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof primitiveDecodeViewServer.topics>({
        startFrom: primitiveDecodeKafkaOptions.consume,
        regions: primitiveDecodeKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          primitiveDecodeViewServer,
          runtimeCore.internalClient,
          primitiveDecodeKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-primitive-codec-failed",
            value: JSON.stringify({
              customerId: "customer-primitive-codec-failed",
              price: 100,
            }),
            offset: 9n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(exit),
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        decodeFailed: true,
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 88,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "raw codec failed",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records nullable Kafka key bytes as mapping failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.internalClient,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: null,
            value: null,
            offset: 6n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 1,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 1,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 0,
            lastCommitAt: 0,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "7",
            lastError: "Kafka source key bytes are required",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("fails processing when keyless Kafka skip commit fails", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const commitFailedMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: null,
        value: null,
        offset: 6n,
        commitFailure: new Error("commit failed"),
      });

      const error = yield* Effect.flip(
        processKafkaMessage(
          viewServer,
          runtimeCore.internalClient,
          kafkaOptions,
          ledger,
          "local",
          commitFailedMessage,
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          causeMessage: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          causeMessage: "commit failed",
          message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 1,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 1,
              processingFailuresPerSecond: 1,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: `Failed to commit Kafka message for source topic ${ordersSourceTopic}: commit failed`,
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("fails processing when Kafka commit fails after publish", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const commitFailedMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: "order-commit-failed",
        value: JSON.stringify({
          customerId: "customer-commit-failed",
          price: 50,
        }),
        offset: 5n,
        commitFailure: new Error("commit failed"),
      });
      const expectedMessageBytes =
        (commitFailedMessage.key?.byteLength ?? 0) + (commitFailedMessage.value?.byteLength ?? 0);
      const error = yield* Effect.flip(
        processKafkaMessage(
          viewServer,
          runtimeCore.internalClient,
          kafkaOptions,
          ledger,
          "local",
          commitFailedMessage,
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          causeMessage: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          causeMessage: "commit failed",
          message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-commit-failed",
              customerId: "customer-commit-failed",
              price: 50,
            },
          ],
          totalRows: 1,
          version: 1,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: expectedMessageBytes,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 1,
              processingFailuresPerSecond: 1,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: `Failed to commit Kafka message for source topic ${ordersSourceTopic}: commit failed`,
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );
});
