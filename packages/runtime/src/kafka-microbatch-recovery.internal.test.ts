import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import {
  makeViewServerRuntimeCoreInternal as makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { Cause, Effect, Exit, Option, Schema } from "effect";
import {
  messageFromUnknown,
  processKafkaMessageBatch,
  runKafkaMessageStream,
} from "./kafka-ingress";
import {
  causeReasonSummary,
  decodeFailureThenSuccessKafkaStream,
  IncomingOrder,
  kafkaIngressErrorSummary,
  kafkaMessage,
  kafkaOptions,
  kafkaOptionsForConfig,
  makeViewServerKafkaHealthLedger,
  nullRecord,
  Order,
  ordersSourceTopic,
  paymentsSourceTopic,
  regions,
  runtimeUnavailable,
  type Topics,
  viewServer,
} from "../test-harness/kafka-ingress";

describe("Kafka microbatch failure recovery internals", () => {
  it.effect("combines batch processing and same-batch terminal stream failures", () =>
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
      const operations: Array<string> = [];
      const publishManyFailingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.fail(runtimeUnavailable))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.fail(runtimeUnavailable))),
      };

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          publishManyFailingClient,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "same-batch-terminal-failure",
              value: JSON.stringify({
                customerId: "customer-same-batch-terminal-failure",
                price: 10,
              }),
              offset: 1n,
            });
            throw new Error("producer terminal failure");
          })(),
        ),
      );
      const exitSummary = Exit.match(exit, {
        onFailure: causeReasonSummary,
        onSuccess: () => [],
      });

      expect({
        exit: exitSummary,
        operations,
      }).toStrictEqual({
        exit: [
          {
            tag: "Fail",
            message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
          },
          {
            tag: "Fail",
            message: "publish failed",
          },
          {
            tag: "Fail",
            message: "Kafka stream failed for region local",
          },
        ],
        operations: ["publishMany:orders:1"],
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes decoded Kafka microbatch messages before a later decode failure", () =>
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
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          batchingClient,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "decode-batch-1",
              value: JSON.stringify({
                customerId: "customer-decode-batch-1",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "decode-batch-2",
              value: JSON.stringify({
                customerId: "customer-decode-batch-2",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "bad-json",
              value: "{",
              offset: 3n,
              onCommit: () => {
                operations.push("commit:3");
              },
            });
          })(),
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
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        operations,
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: "Failed to decode Kafka message for source topic orders-source",
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        operations: ["publishMany:orders:2", "commit:1", "commit:2"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "decode-batch-1",
              customerId: "customer-decode-batch-1",
              price: 10,
            },
            {
              id: "decode-batch-2",
              customerId: "customer-decode-batch-2",
              price: 20,
            },
          ],
          totalRows: 2,
          version: 1,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 0,
              messagesPerSecond: 3,
              bytesPerSecond: 139,
              decodedMessagesPerSecond: 2,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 0,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: 0,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: "3",
              lastError: "Failed to parse Kafka JSON payload",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes and commits the valid Kafka microbatch prefix before Mapping failure", () =>
    Effect.gen(function* () {
      const mappingViewServer = defineViewServerConfig({
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
              map: ({ value }) => {
                if (value.customerId === "customer-mapping-poison") {
                  throw new Error("mapping failed");
                }
                return {
                  customerId: value.customerId,
                  price: value.price,
                };
              },
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(mappingViewServer, {});
      const mappingKafkaOptions = kafkaOptionsForConfig(
        mappingViewServer,
        "view-server-mapping-prefix-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof mappingViewServer.topics>({
        regions: mappingKafkaOptions.regions,
        startFrom: mappingKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<typeof mappingViewServer.topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const error = yield* Effect.flip(
        processKafkaMessageBatch(
          mappingViewServer,
          batchingClient,
          mappingKafkaOptions,
          ledger,
          "local",
          [
            kafkaMessage({
              topic: ordersSourceTopic,
              key: "mapping-prefix-1",
              value: JSON.stringify({
                customerId: "customer-mapping-prefix-1",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            }),
            kafkaMessage({
              topic: ordersSourceTopic,
              key: "mapping-prefix-2",
              value: JSON.stringify({
                customerId: "customer-mapping-prefix-2",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
              },
            }),
            kafkaMessage({
              topic: ordersSourceTopic,
              key: "mapping-poison",
              value: JSON.stringify({
                customerId: "customer-mapping-poison",
                price: 30,
              }),
              offset: 3n,
              onCommit: () => {
                operations.push("commit:3");
              },
            }),
          ],
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        operations,
        snapshot,
      }).toStrictEqual({
        error: {
          message: `Failed to map Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        operations: ["publishMany:orders:2", "commit:1", "commit:2"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "mapping-prefix-1",
              customerId: "customer-mapping-prefix-1",
              price: 10,
            },
            {
              id: "mapping-prefix-2",
              customerId: "customer-mapping-prefix-2",
              price: 20,
            },
          ],
          totalRows: 2,
          version: 1,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("clears unaffected source topic errors during poison-message recovery flush", () =>
    Effect.gen(function* () {
      const multiSourceViewServer = defineViewServerConfig({
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
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
          payments: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: paymentsSourceTopic,
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
      const runtimeCore = yield* makeViewServerRuntimeCore(multiSourceViewServer, {});
      const multiSourceKafkaOptions = kafkaOptionsForConfig(
        multiSourceViewServer,
        "view-server-unaffected-source-recovery-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof multiSourceViewServer.topics>({
        regions: multiSourceKafkaOptions.regions,
        startFrom: multiSourceKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [paymentsSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "payments",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.topicConnected(paymentsSourceTopic, "local", 1, 1_000);
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 1,
        message: "stale orders decode error",
        nowMillis: 0,
      });
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<
        typeof multiSourceViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          multiSourceViewServer,
          batchingClient,
          multiSourceKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "orders-prefix",
              value: JSON.stringify({
                customerId: "customer-orders-prefix",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:orders-prefix");
              },
            });
            yield kafkaMessage({
              topic: paymentsSourceTopic,
              key: "payments-poison",
              value: "{",
              offset: 2n,
              onCommit: () => {
                operations.push("commit:payments-poison");
              },
            });
          })(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      yield* ledger.regionRecovered("local", 2_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 2_000);
      yield* ledger.topicConnected(paymentsSourceTopic, "local", 1, 2_000);
      const reconnectedHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        afterReconnect: {
          ordersSource: {
            lastError:
              reconnectedHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
            status: reconnectedHealth.kafka?.topics[ordersSourceTopic]?.status,
          },
          paymentsSource: {
            lastError:
              reconnectedHealth.kafka?.topics[paymentsSourceTopic]?.regions["local"]?.lastError,
            status: reconnectedHealth.kafka?.topics[paymentsSourceTopic]?.status,
          },
        },
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        operations,
        ordersSource: {
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
        paymentsSource: {
          lastError: health.kafka?.topics[paymentsSourceTopic]?.regions["local"]?.lastError,
          status: health.kafka?.topics[paymentsSourceTopic]?.status,
        },
        snapshot,
      }).toStrictEqual({
        afterReconnect: {
          ordersSource: {
            lastError: null,
            status: "ready",
          },
          paymentsSource: {
            lastError: "Failed to parse Kafka JSON payload",
            status: "degraded",
          },
        },
        error: {
          message: `Failed to decode Kafka message for source topic ${paymentsSourceTopic}`,
          region: "local",
          sourceTopic: paymentsSourceTopic,
        },
        operations: ["publishMany:orders:1", "commit:orders-prefix"],
        ordersSource: {
          lastError: `Failed to decode Kafka message for source topic ${paymentsSourceTopic}`,
          status: "degraded",
        },
        paymentsSource: {
          lastError: "Failed to parse Kafka JSON payload",
          status: "degraded",
        },
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "orders-prefix",
              customerId: "customer-orders-prefix",
              price: 10,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes decoded Kafka microbatch messages before a missing topic failure", () =>
    Effect.gen(function* () {
      const multiSourceViewServer = defineViewServerConfig({
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
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
          payments: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: paymentsSourceTopic,
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
      const runtimeCore = yield* makeViewServerRuntimeCore(multiSourceViewServer, {});
      const multiSourceKafkaOptions = kafkaOptionsForConfig(
        multiSourceViewServer,
        "view-server-missing-topic-prefix-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof multiSourceViewServer.topics>({
        regions: multiSourceKafkaOptions.regions,
        startFrom: multiSourceKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [paymentsSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "payments",
          },
        },
      });
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.topicConnected(paymentsSourceTopic, "local", 1, 1_000);
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<
        typeof multiSourceViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };
      const missingPaymentsTopics = { ...multiSourceViewServer.topics };
      Reflect.deleteProperty(missingPaymentsTopics, "payments");
      const missingPaymentsViewServer = {
        ...multiSourceViewServer,
        topics: missingPaymentsTopics,
      };

      const error = yield* Effect.flip(
        processKafkaMessageBatch(
          missingPaymentsViewServer,
          batchingClient,
          multiSourceKafkaOptions,
          ledger,
          "local",
          [
            kafkaMessage({
              topic: ordersSourceTopic,
              key: "orders-prefix",
              value: JSON.stringify({
                customerId: "customer-orders-prefix",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:orders-prefix");
              },
            }),
            kafkaMessage({
              topic: paymentsSourceTopic,
              key: "payments-poison",
              value: JSON.stringify({
                customerId: "customer-payments-poison",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:payments-poison");
              },
            }),
          ],
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        operations,
        snapshot,
      }).toStrictEqual({
        error: {
          message: "Kafka source references unknown View Server topic: payments",
          region: "local",
          sourceTopic: paymentsSourceTopic,
        },
        operations: ["publishMany:orders:1", "commit:orders-prefix"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "orders-prefix",
              customerId: "customer-orders-prefix",
              price: 10,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves missing topic failures when decoded prefix flush fails", () =>
    Effect.gen(function* () {
      const multiSourceViewServer = defineViewServerConfig({
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
              map: ({ value }) => ({
                customerId: value.customerId,
                price: value.price,
              }),
            }),
          },
          payments: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: paymentsSourceTopic,
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
      const runtimeCore = yield* makeViewServerRuntimeCore(multiSourceViewServer, {});
      const multiSourceKafkaOptions = kafkaOptionsForConfig(
        multiSourceViewServer,
        "view-server-missing-topic-prefix-flush-failure-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof multiSourceViewServer.topics>({
        regions: multiSourceKafkaOptions.regions,
        startFrom: multiSourceKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [paymentsSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "payments",
          },
        },
      });
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.topicConnected(paymentsSourceTopic, "local", 1, 1_000);
      const operations: Array<string> = [];
      const publishManyFailingClient: ViewServerRuntimeCoreInternalClient<
        typeof multiSourceViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.fail(runtimeUnavailable))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.fail(runtimeUnavailable))),
      };
      const missingPaymentsTopics = { ...multiSourceViewServer.topics };
      Reflect.deleteProperty(missingPaymentsTopics, "payments");
      const missingPaymentsViewServer = {
        ...multiSourceViewServer,
        topics: missingPaymentsTopics,
      };

      const exit = yield* Effect.exit(
        processKafkaMessageBatch(
          missingPaymentsViewServer,
          publishManyFailingClient,
          multiSourceKafkaOptions,
          ledger,
          "local",
          [
            kafkaMessage({
              topic: ordersSourceTopic,
              key: "orders-prefix",
              value: JSON.stringify({
                customerId: "customer-orders-prefix",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:orders-prefix");
              },
            }),
            kafkaMessage({
              topic: paymentsSourceTopic,
              key: "payments-poison",
              value: JSON.stringify({
                customerId: "customer-payments-poison",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:payments-poison");
              },
            }),
          ],
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const exitSummary = Exit.match(exit, {
        onFailure: causeReasonSummary,
        onSuccess: () => [],
      });

      expect({
        exit: exitSummary,
        operations,
        snapshot,
      }).toStrictEqual({
        exit: [
          {
            tag: "Fail",
            message: "Kafka source references unknown View Server topic: payments",
          },
          {
            tag: "Fail",
            message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
          },
          {
            tag: "Fail",
            message: "publish failed",
          },
        ],
        operations: ["publishMany:orders:1"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 0,
          version: 0,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes decoded Kafka microbatch messages before a later codec defect", () =>
    Effect.gen(function* () {
      const defectingViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "defecting-value-codec",
                decode: (input) => {
                  const raw = Buffer.from(input.bytes).toString("utf8");
                  const defect = Effect.die(new Error("decode defect"));
                  const [customerId = "", price = "0"] = raw.split(":");
                  return raw === "defect"
                    ? defect
                    : Effect.succeed({
                        customerId,
                        price: Number(price),
                      });
                },
              }),
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
      const runtimeCore = yield* makeViewServerRuntimeCore(defectingViewServer, {});
      const defectingKafkaOptions = kafkaOptionsForConfig(
        defectingViewServer,
        "view-server-codec-defect-microbatch-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof defectingViewServer.topics>({
        regions: defectingKafkaOptions.regions,
        startFrom: defectingKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<typeof defectingViewServer.topics> =
        {
          ...runtimeCore.internalClient,
          publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
          publishManyDecodedRows: (topic, rows) =>
            Effect.sync(() => {
              operations.push(`publishMany:${topic}:${rows.length}`);
            }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
          publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
            Effect.sync(() => {
              operations.push(`publishMany:${topic}:${rows.length}`);
            }).pipe(
              Effect.andThen(
                runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
              ),
            ),
        };
      const firstMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: "defect-batch-1",
        value: "customer-defect-batch-1:10",
        offset: 1n,
        onCommit: () => {
          operations.push("commit:1");
        },
      });
      const secondMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: "defect-batch-2",
        value: "customer-defect-batch-2:20",
        offset: 2n,
        onCommit: () => {
          operations.push("commit:2");
        },
      });
      const defectMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: "defect-batch-3",
        value: "defect",
        offset: 3n,
        onCommit: () => {
          operations.push("commit:3");
        },
      });
      const expectedMessageBytes = [firstMessage, secondMessage, defectMessage].reduce(
        (totalBytes, message) =>
          totalBytes + (message.key?.byteLength ?? 0) + (message.value?.byteLength ?? 0),
        0,
      );

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          defectingViewServer,
          batchingClient,
          defectingKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield firstMessage;
            yield secondMessage;
            yield defectMessage;
          })(),
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
          causeHasDefect: Cause.isCause(error.cause) && Cause.hasDies(error.cause),
          causeIsCause: Cause.isCause(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        operations,
        snapshot,
        topicHealth: {
          bytesPerSecond: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.bytesPerSecond,
          committedOffset:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.committedOffset,
          decodeFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
          decodedMessagesPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodedMessagesPerSecond,
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          messagesPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.messagesPerSecond,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        error: {
          causeHasDefect: true,
          causeIsCause: true,
          message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        operations: ["publishMany:orders:2", "commit:1", "commit:2"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "defect-batch-1",
              customerId: "customer-defect-batch-1",
              price: 10,
            },
            {
              id: "defect-batch-2",
              customerId: "customer-defect-batch-2",
              price: 20,
            },
          ],
          totalRows: 2,
          version: 1,
        },
        topicHealth: {
          bytesPerSecond: expectedMessageBytes,
          committedOffset: "3",
          decodeFailuresPerSecond: 1,
          decodedMessagesPerSecond: 2,
          lastError: "decode defect",
          messagesPerSecond: 3,
          status: "degraded",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves poison-message cause when recovery commit fails", () =>
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
      const operations: Array<string> = [];

      const batchingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };
      const commitFailure = new Error("recovery commit failed");

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          batchingClient,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "recovery-commit-failure-1",
              value: JSON.stringify({
                customerId: "customer-recovery-commit-failure-1",
                price: 10,
              }),
              offset: 1n,
              commitFailure,
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "recovery-commit-failure-2",
              value: JSON.stringify({
                customerId: "customer-recovery-commit-failure-2",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "bad-json",
              value: "{",
              offset: 3n,
            });
          })(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      const exitSummary = Exit.match(exit, {
        onFailure: causeReasonSummary,
        onSuccess: () => [],
      });

      expect({
        exit: exitSummary,
        operations,
        snapshot,
        topicHealth: {
          commitFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.commitFailuresPerSecond,
          decodeFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        exit: [
          {
            tag: "Fail",
            message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          },
          {
            tag: "Fail",
            message: "Failed to parse Kafka JSON payload",
          },
          {
            tag: "Fail",
            message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
          },
        ],
        operations: ["publishMany:orders:2"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "recovery-commit-failure-1",
            },
            {
              id: "recovery-commit-failure-2",
            },
          ],
          totalRows: 2,
          version: 1,
        },
        topicHealth: {
          commitFailuresPerSecond: 1,
          decodeFailuresPerSecond: 1,
          status: "degraded",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves mixed codec typed failures and interrupts", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const mixedInterruptViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "mixed-interrupt-value-codec",
                decode: (input) => {
                  const raw = Buffer.from(input.bytes).toString("utf8");
                  const [customerId = "", price = "0"] = raw.split(":");
                  return raw === "typed-failure-with-interrupt"
                    ? Effect.failCause(
                        Cause.fromReasons([
                          Cause.makeFailReason("typed decode interrupted"),
                          Cause.makeInterruptReason(),
                        ]),
                      )
                    : Effect.succeed({
                        customerId,
                        price: Number(price),
                      });
                },
              }),
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
      const runtimeCore = yield* makeViewServerRuntimeCore(mixedInterruptViewServer, {});
      const mixedInterruptKafkaOptions = kafkaOptionsForConfig(
        mixedInterruptViewServer,
        "view-server-codec-mixed-interrupt-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof mixedInterruptViewServer.topics>({
        regions: mixedInterruptKafkaOptions.regions,
        startFrom: mixedInterruptKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const batchingClient: ViewServerRuntimeCoreInternalClient<
        typeof mixedInterruptViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          mixedInterruptViewServer,
          batchingClient,
          mixedInterruptKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "mixed-interrupt-batch-1",
              value: "customer-mixed-interrupt-batch-1:10",
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "mixed-interrupt-batch-2",
              value: "typed-failure-with-interrupt",
              offset: 2n,
            });
          })(),
        ),
      );
      const exitSummary = Exit.match(exit, {
        onFailure: (cause) => {
          const error = Cause.findErrorOption(cause);
          return {
            error: Option.match(error, {
              onNone: () => null,
              onSome: (value) => ({
                cause: causeReasonSummary(value.cause),
                message: value.message,
              }),
            }),
            topLevel: causeReasonSummary(cause),
          };
        },
        onSuccess: () => ({ error: null, topLevel: [] }),
      });
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        exit: exitSummary,
        operations,
        snapshot,
        topicHealth: {
          decodeFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        exit: {
          error: {
            cause: [
              {
                tag: "Fail",
                message: "typed decode interrupted",
              },
              {
                tag: "Interrupt",
                message: null,
              },
            ],
            message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          },
          topLevel: [
            {
              tag: "Fail",
              message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
            },
            {
              tag: "Fail",
              message: "typed decode interrupted",
            },
            {
              tag: "Interrupt",
              message: null,
            },
          ],
        },
        operations: ["publishMany:orders:1", "commit:1"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "mixed-interrupt-batch-1",
            },
          ],
          totalRows: 1,
          version: 1,
        },
        topicHealth: {
          decodeFailuresPerSecond: 1,
          lastError: "typed decode interrupted",
          status: "degraded",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves mixed codec typed failures and finalizer defects", () =>
    Effect.gen(function* () {
      const mixedCauseViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "mixed-cause-value-codec",
                decode: () =>
                  Effect.failCause(
                    Cause.fromReasons([
                      Cause.makeFailReason("typed decode failed"),
                      Cause.makeDieReason(new Error("decode finalizer defect")),
                    ]),
                  ),
              }),
              key: kafka.stringKey(),
              rowKey: ({ key }) => key,
              map: () => ({
                customerId: "unreachable",
                price: 0,
              }),
            }),
          },
        },
      });
      const runtimeCore = yield* makeViewServerRuntimeCore(mixedCauseViewServer, {});
      const mixedCauseKafkaOptions = kafkaOptionsForConfig(
        mixedCauseViewServer,
        "view-server-codec-mixed-cause-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof mixedCauseViewServer.topics>({
        regions: mixedCauseKafkaOptions.regions,
        startFrom: mixedCauseKafkaOptions.consume,
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
        runKafkaMessageStream(
          mixedCauseViewServer,
          runtimeCore.internalClient,
          mixedCauseKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "mixed-cause-batch-1",
              value: "typed-failure-with-defect",
              offset: 1n,
            });
          })(),
        ),
      );
      const exitSummary = Exit.match(exit, {
        onFailure: (cause) => {
          const error = Cause.findErrorOption(cause);
          return {
            error: Option.match(error, {
              onNone: () => null,
              onSome: kafkaIngressErrorSummary,
            }),
            topLevel: causeReasonSummary(cause),
          };
        },
        onSuccess: () => ({ error: null, topLevel: [] }),
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        exit: exitSummary,
        topicHealth: {
          decodeFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        exit: {
          error: {
            cause: [
              {
                tag: "Fail",
                message: "typed decode failed",
              },
              {
                tag: "Die",
                message: "decode finalizer defect",
              },
            ],
            message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
            region: "local",
            sourceTopic: ordersSourceTopic,
          },
          topLevel: [
            {
              tag: "Fail",
              message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
            },
            {
              tag: "Fail",
              message: "typed decode failed",
            },
            {
              tag: "Die",
              message: "decode finalizer defect",
            },
          ],
        },
        topicHealth: {
          decodeFailuresPerSecond: 1,
          lastError: "typed decode failed",
          status: "degraded",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not record Kafka stream failures when microbatch decoding is interrupted", () =>
    Effect.gen(function* () {
      const interruptingViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: ordersSourceTopic,
              regions: ["local"],
              value: kafka.codec({
                name: "interrupting-value-codec",
                decode: (input) => {
                  const raw = Buffer.from(input.bytes).toString("utf8");
                  const [customerId = "", price = "0"] = raw.split(":");
                  return raw === "interrupt"
                    ? Effect.interrupt
                    : Effect.succeed({
                        customerId,
                        price: Number(price),
                      });
                },
              }),
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
      const runtimeCore = yield* makeViewServerRuntimeCore(interruptingViewServer, {});
      const interruptingKafkaOptions = kafkaOptionsForConfig(
        interruptingViewServer,
        "view-server-codec-interrupt-microbatch-test",
      );
      const ledger = makeViewServerKafkaHealthLedger<typeof interruptingViewServer.topics>({
        regions: interruptingKafkaOptions.regions,
        startFrom: interruptingKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const operations: Array<string> = [];
      const batchingClient: ViewServerRuntimeCoreInternalClient<
        typeof interruptingViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(runtimeCore.internalClient.publishManyDecodedRows(topic, rows))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          interruptingViewServer,
          batchingClient,
          interruptingKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "interrupt-batch-1",
              value: "customer-interrupt-batch-1:10",
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "interrupt-batch-2",
              value: "interrupt",
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
              },
            });
          })(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        interrupted: Exit.hasInterrupts(exit),
        operations,
        snapshot,
        topicHealth: {
          connected: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.connected,
          decodeFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodeFailuresPerSecond,
          decodedMessagesPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.decodedMessagesPerSecond,
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          messagesPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.messagesPerSecond,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        interrupted: true,
        operations: [],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 0,
          version: 0,
        },
        topicHealth: {
          connected: true,
          decodeFailuresPerSecond: 0,
          decodedMessagesPerSecond: 0,
          lastError: null,
          messagesPerSecond: 0,
          status: "ready",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect(
    "preserves commit failure health when Kafka stream finalization marks the region down",
    () =>
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
          key: "order-stream-commit-failed",
          value: JSON.stringify({
            customerId: "customer-stream-commit-failed",
            price: 70,
          }),
          offset: 7n,
          commitFailure: new Error("commit failed"),
        });
        const expectedMessageBytes =
          (commitFailedMessage.key?.byteLength ?? 0) + (commitFailedMessage.value?.byteLength ?? 0);
        const error = yield* Effect.flip(
          runKafkaMessageStream(
            viewServer,
            runtimeCore.internalClient,
            kafkaOptions,
            ledger,
            "local",
            (async function* () {
              yield commitFailedMessage;
            })(),
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
          health: {
            status: health.status,
            region: health.kafka?.regions["local"],
            topic: health.kafka?.topics[ordersSourceTopic],
          },
          snapshot,
        }).toStrictEqual({
          error: {
            causeMessage: "commit failed",
            message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
            region: "local",
            sourceTopic: ordersSourceTopic,
          },
          health: {
            status: "degraded",
            region: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
            },
            topic: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: nullRecord({
                local: {
                  connected: false,
                  assignedPartitions: 0,
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
          },
          snapshot: {
            status: "ready",
            statusCode: "Ready",
            rows: [
              {
                id: "order-stream-commit-failed",
                customerId: "customer-stream-commit-failed",
                price: 70,
              },
            ],
            totalRows: 1,
            version: 1,
          },
        });

        yield* runtimeCore.close;
      }),
  );

  it.effect("fails Kafka streams before later records can skip failed offsets", () =>
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
      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          kafkaOptions,
          ledger,
          "local",
          decodeFailureThenSuccessKafkaStream(() => {
            committedMessages += 1;
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
        streamFailure: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        committedMessages,
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        streamFailure: {
          message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        committedMessages: 0,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 0,
          version: 0,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 0,
              messagesPerSecond: 1,
              bytesPerSecond: 9,
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
              lastError: "Failed to parse Kafka JSON payload",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );
});
