import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import {
  makeViewServerRuntimeCoreInternal as makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import { Cause, Clock, Effect, Exit, Option, Schema } from "effect";
import { processKafkaMessageBatch, runKafkaMessageStream } from "./kafka-ingress";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import {
  causeReasonSummary,
  IncomingOrder,
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

describe("Kafka microbatch publishing internals", () => {
  it.effect(
    "microbatches Kafka stream messages through publishMany before committing offsets",
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
        yield* runKafkaMessageStream(
          viewServer,
          batchingClient,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "batch-1",
              value: JSON.stringify({
                customerId: "customer-batch-1",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "batch-2",
              value: JSON.stringify({
                customerId: "customer-batch-2",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "batch-3",
              value: JSON.stringify({
                customerId: "customer-batch-3",
                price: 30,
              }),
              offset: 3n,
              onCommit: () => {
                operations.push("commit:3");
              },
            });
          })(),
        );
        const snapshot = yield* runtimeCore.client.snapshot("orders", {
          select: ["id", "customerId", "price"],
          orderBy: [{ field: "id", direction: "asc" }],
          limit: 10,
        });
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

        expect({
          operations,
          snapshot,
          kafkaTopic: health.kafka?.topics[ordersSourceTopic],
        }).toStrictEqual({
          operations: ["publishMany:orders:3", "commit:1", "commit:2", "commit:3"],
          snapshot: {
            status: "ready",
            statusCode: "Ready",
            rows: [
              {
                id: "batch-1",
                customerId: "customer-batch-1",
                price: 10,
              },
              {
                id: "batch-2",
                customerId: "customer-batch-2",
                price: 20,
              },
              {
                id: "batch-3",
                customerId: "customer-batch-3",
                price: 30,
              },
            ],
            totalRows: 3,
            version: 1,
          },
          kafkaTopic: {
            status: "ready",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 3,
                bytesPerSecond: 153,
                decodedMessagesPerSecond: 3,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: 0,
                lastCommitAt: 0,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: "4",
                lastError: null,
              },
            }),
          },
        });

        yield* runtimeCore.close;
      }),
  );

  it.effect("flushes Kafka microbatches when the configured batch size is reached", () =>
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

      yield* runKafkaMessageStream(
        viewServer,
        batchingClient,
        kafkaOptions,
        ledger,
        "local",
        (async function* () {
          for (let index = 0; index < 256; index += 1) {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: `size-batch-${index}`,
              value: JSON.stringify({
                customerId: `customer-size-batch-${index}`,
                price: index,
              }),
              offset: BigInt(index),
            });
          }
        })(),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 0,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        operations,
        snapshot,
        engineRows: health.engine.topics.orders.rowCount,
        committedOffset: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.committedOffset,
      }).toStrictEqual({
        operations: ["publishMany:orders:256"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 256,
          version: 1,
        },
        engineRows: 256,
        committedOffset: "256",
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("flushes Kafka microbatches against the batch-start wall clock deadline", () =>
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
      const clockReads = [0, 3, 3, 3, 3, 3];
      const currentTimeMillis = () => clockReads.shift() ?? 3;
      const wallClockFlushClock: Clock.Clock = {
        currentTimeMillisUnsafe: currentTimeMillis,
        currentTimeMillis: Effect.sync(currentTimeMillis),
        currentTimeNanosUnsafe: () => 0n,
        currentTimeNanos: Effect.succeed(0n),
        sleep: () => Effect.void,
      };
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

      yield* runKafkaMessageStream(
        viewServer,
        batchingClient,
        kafkaOptions,
        ledger,
        "local",
        (async function* () {
          yield kafkaMessage({
            topic: ordersSourceTopic,
            key: "wall-clock-deadline",
            value: JSON.stringify({
              customerId: "customer-wall-clock-deadline",
              price: 40,
            }),
            offset: 1n,
            onCommit: () => {
              operations.push("commit:1");
            },
          });
        })(),
      ).pipe(Effect.provideService(Clock.Clock, wallClockFlushClock));
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        operations,
        snapshot,
      }).toStrictEqual({
        operations: ["publishMany:orders:1", "commit:1"],
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "wall-clock-deadline",
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not commit Kafka microbatch messages when publishMany fails", () =>
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

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          publishManyFailingClient,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "failed-batch-1",
              value: JSON.stringify({
                customerId: "customer-failed-batch-1",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:1");
              },
            });
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "failed-batch-2",
              value: JSON.stringify({
                customerId: "customer-failed-batch-2",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:2");
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
          message: "Failed to process Kafka message for source topic orders-source",
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        operations: ["publishMany:orders:2"],
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
              messagesPerSecond: 2,
              bytesPerSecond: 130,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 2,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 2,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "publish failed",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves mixed Kafka publish failures and defects", () =>
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
      const publishFailure = Cause.fromReasons([
        Cause.makeFailReason(runtimeUnavailable),
        Cause.makeDieReason(new Error("publish finalizer defect")),
      ]);
      const publishManyFailingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.failCause(publishFailure))),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(Effect.andThen(Effect.failCause(publishFailure))),
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
              key: "mixed-publish-failure",
              value: JSON.stringify({
                customerId: "customer-mixed-publish-failure",
                price: 10,
              }),
              offset: 1n,
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
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

      expect({
        exit: exitSummary,
        operations,
        topicHealth: {
          lastError: health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.lastError,
          publishFailuresPerSecond:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.publishFailuresPerSecond,
          status: health.kafka?.topics[ordersSourceTopic]?.status,
        },
      }).toStrictEqual({
        exit: {
          error: {
            cause: [
              {
                tag: "Fail",
                message: "publish failed",
              },
              {
                tag: "Die",
                message: "publish finalizer defect",
              },
            ],
            message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
          },
          topLevel: [
            {
              tag: "Fail",
              message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
            },
            {
              tag: "Fail",
              message: "publish failed",
            },
            {
              tag: "Die",
              message: "publish finalizer defect",
            },
          ],
        },
        operations: ["publishMany:orders:1"],
        topicHealth: {
          lastError: "publish failed",
          publishFailuresPerSecond: 1,
          status: "degraded",
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves source-interleaved upsert order for topic-owned source topics", () =>
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
        "view-server-source-interleaved-test",
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

      yield* processKafkaMessageBatch(
        multiSourceViewServer,
        runtimeCore.internalClient,
        multiSourceKafkaOptions,
        ledger,
        "local",
        [
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "shared-order",
            value: JSON.stringify({
              customerId: "customer-from-orders-v1",
              price: 10,
            }),
            offset: 1n,
          }),
          kafkaMessage({
            topic: paymentsSourceTopic,
            key: "shared-order",
            value: JSON.stringify({
              customerId: "customer-from-payments-v2",
              price: 20,
            }),
            offset: 2n,
          }),
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "shared-order",
            value: JSON.stringify({
              customerId: "customer-from-orders-v3",
              price: 30,
            }),
            offset: 3n,
          }),
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "shared-order",
            value: JSON.stringify({
              customerId: "customer-from-orders-v4",
              price: 40,
            }),
            offset: 4n,
          }),
        ],
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const paymentsSnapshot = yield* runtimeCore.client.snapshot("payments", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        orders: snapshot,
        payments: paymentsSnapshot,
      }).toStrictEqual({
        orders: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "shared-order",
              customerId: "customer-from-orders-v4",
              price: 40,
            },
          ],
          totalRows: 1,
          version: 2,
        },
        payments: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "shared-order",
              customerId: "customer-from-payments-v2",
              price: 20,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("batches contiguous topic-owned Kafka source rowKey upserts", () =>
    Effect.gen(function* () {
      const topicOwnedViewServer = defineViewServerConfig({
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
        },
      });
      const resolved = yield* resolveViewServerRuntimeOptions(topicOwnedViewServer, {
        kafka: {
          consumerGroupId: "view-server-topic-owned-storage-run",
        },
      });
      const topicOwnedKafkaOptions = Option.getOrThrow(Option.fromNullishOr(resolved.kafkaOptions));
      const runtimeCore = yield* makeViewServerRuntimeCore(topicOwnedViewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<typeof topicOwnedViewServer.topics>({
        regions: topicOwnedKafkaOptions.regions,
        startFrom: topicOwnedKafkaOptions.consume,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      yield* processKafkaMessageBatch(
        topicOwnedViewServer,
        runtimeCore.internalClient,
        topicOwnedKafkaOptions,
        ledger,
        "local",
        [
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-1",
            value: JSON.stringify({
              customerId: "customer-1",
              price: 10,
            }),
            offset: 1n,
          }),
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-2",
            value: JSON.stringify({
              customerId: "customer-2",
              price: 20,
            }),
            offset: 2n,
          }),
        ],
      );

      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        status: "ready",
        statusCode: "Ready",
        rows: [
          {
            id: "order-1",
            customerId: "customer-1",
            price: 10,
          },
          {
            id: "order-2",
            customerId: "customer-2",
            price: 20,
          },
        ],
        totalRows: 2,
        version: 1,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("attributes publish failures to the failing topic-owned Kafka source topic", () =>
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
        "view-server-source-publish-failure-test",
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
      const operations: Array<string> = [];
      const publishManyFailingClient: ViewServerRuntimeCoreInternalClient<
        typeof multiSourceViewServer.topics
      > = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("Kafka stream should publish batches with publishMany"),
        publishManyDecodedRows: () =>
          Effect.die("Kafka stream should publish decoded rows with storage keys"),
        publishManyDecodedRowsWithStorageKeys: (topic, rows) =>
          Effect.sync(() => {
            operations.push(`publishMany:${topic}:${rows.length}`);
          }).pipe(
            Effect.andThen(
              rows.some(
                (row) => Reflect.get(row.row, "customerId") === "customer-payment-publish-failed",
              )
                ? Effect.fail(runtimeUnavailable)
                : runtimeCore.internalClient.publishManyDecodedRowsWithStorageKeys(topic, rows),
            ),
          ),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          multiSourceViewServer,
          publishManyFailingClient,
          multiSourceKafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "order-publish-succeeds",
              value: JSON.stringify({
                customerId: "customer-order-publish-succeeds",
                price: 10,
              }),
              offset: 1n,
              onCommit: () => {
                operations.push("commit:orders:1");
              },
            });
            yield kafkaMessage({
              topic: paymentsSourceTopic,
              key: "payment-publish-fails",
              value: JSON.stringify({
                customerId: "customer-payment-publish-failed",
                price: 20,
              }),
              offset: 2n,
              onCommit: () => {
                operations.push("commit:payments:2");
              },
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect({
        error: {
          message: error.message,
          sourceTopic: error.sourceTopic,
        },
        operations,
        health: {
          ordersPublishFailures:
            health.kafka?.topics[ordersSourceTopic]?.regions["local"]?.publishFailuresPerSecond,
          paymentsPublishFailures:
            health.kafka?.topics[paymentsSourceTopic]?.regions["local"]?.publishFailuresPerSecond,
        },
        snapshot,
      }).toStrictEqual({
        error: {
          message: `Failed to process Kafka message for source topic ${paymentsSourceTopic}`,
          sourceTopic: paymentsSourceTopic,
        },
        operations: ["publishMany:orders:1", "commit:orders:1", "publishMany:payments:1"],
        health: {
          ordersPublishFailures: 0,
          paymentsPublishFailures: 1,
        },
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-publish-succeeds",
              customerId: "customer-order-publish-succeeds",
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
});
