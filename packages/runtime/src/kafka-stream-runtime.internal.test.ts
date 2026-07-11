import { describe, expect, it } from "@effect/vitest";
import {
  makeViewServerRuntimeCoreInternal as makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Queue } from "effect";
import {
  kafkaConsumerStartError,
  kafkaStreamError,
  messageFromUnknown,
  offerKafkaStreamProducerFailure,
  recordKafkaStreamError,
  runKafkaMessageStream,
  startKafkaRegionConsumers,
  ViewServerKafkaIngressError,
} from "./kafka-ingress";
import type { StartedKafkaRegionConsumer, KafkaStreamQueueEvent } from "./kafka-ingress";
import {
  causeReasonSummary,
  failingKafkaStream,
  KafkaIngressTestError,
  kafkaMessage,
  kafkaOptions,
  makeViewServerKafkaHealthLedger,
  nullRecord,
  ordersSourceTopic,
  regions,
  type KafkaMessage,
  type Topics,
  viewServer,
} from "../test-harness/kafka-ingress";

describe("Kafka message stream runtime internals", () => {
  it.effect("closes already started region consumers when a later region fails", () =>
    Effect.gen(function* () {
      const closedConsumers: Array<string> = [];
      const starts: Record<
        string,
        Effect.Effect<StartedKafkaRegionConsumer, ViewServerKafkaIngressError>
      > = {
        cold: Effect.fail(kafkaConsumerStartError("cold", "no-broker")),
        local: Effect.succeed({
          close: Effect.sync(() => {
            closedConsumers.push("local");
          }),
        }),
      };
      const regionStarts: ReadonlyArray<readonly [string, string]> = [
        ["local", regions.local],
        ["cold", regions.cold],
      ];

      const exit = yield* Effect.exit(
        startKafkaRegionConsumers(
          regionStarts,
          (region) =>
            starts[region] ?? Effect.fail(kafkaConsumerStartError(region, "unexpected-region")),
        ),
      );

      expect({
        startupFailed: Exit.isFailure(exit),
        closedConsumers,
      }).toStrictEqual({
        startupFailed: true,
        closedConsumers: ["local"],
      });
    }),
  );

  it.effect("records stream errors before refailing them", () =>
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
      const streamError = kafkaStreamError("local", "stream-down");

      const exit = yield* Effect.exit(recordKafkaStreamError(ledger, "local", streamError));
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamRecordingFailed: Exit.isFailure(exit),
        regions: health.kafka?.regions,
      }).toStrictEqual({
        streamRecordingFailed: true,
        regions: nullRecord({
          local: {
            status: "disconnected",
            brokers: regions.local,
            lastConnectedAt: null,
            lastError: "Kafka stream failed for region local",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records defects in Kafka stream processing as generic stream failures", () =>
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
      const defectiveClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.die("publish defect"),
        publishManyDecodedRows: () => Effect.die("publish defect"),
        publishManyDecodedRowsWithStorageKeys: () => Effect.die("publish defect"),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          defectiveClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "order-defective-publish",
              value: JSON.stringify({
                customerId: "customer-defective-publish",
                price: 60,
              }),
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
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
              bytesPerSecond: 77,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              publishFailuresPerSecond: 1,
              commitFailuresPerSecond: 0,
              processingFailuresPerSecond: 1,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "publish defect",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves Kafka health when message stream processing is interrupted", () =>
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
      const interruptingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
        ...runtimeCore.internalClient,
        publish: () => Effect.interrupt,
        publishManyDecodedRows: () => Effect.interrupt,
        publishManyDecodedRowsWithStorageKeys: () => Effect.interrupt,
      };

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          interruptingClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "order-interrupted",
              value: JSON.stringify({
                customerId: "customer-interrupted",
                price: 80,
              }),
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        interrupted: Exit.hasInterrupts(exit),
        region: health.kafka?.regions["local"],
        topic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        interrupted: true,
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: 1_000,
          lastError: null,
        },
        topic: {
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

  it.effect("closes the Kafka async iterator when message stream processing is interrupted", () =>
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
      const nextRequested = yield* Deferred.make<void>();
      const iteratorClosed = yield* Deferred.make<void>();
      const services = yield* Effect.context<never>();
      const runPromise = Effect.runPromiseWith(services);
      const blockedStream: AsyncIterable<KafkaMessage> = {
        [Symbol.asyncIterator]: () => ({
          next: () =>
            runPromise(
              Deferred.succeed(nextRequested, undefined).pipe(Effect.andThen(Effect.never)),
            ),
          return: () =>
            runPromise(
              Deferred.succeed(iteratorClosed, undefined).pipe(
                Effect.as({
                  done: true,
                  value: undefined,
                }),
              ),
            ),
        }),
      };

      const streamFiber = yield* runKafkaMessageStream(
        viewServer,
        runtimeCore.internalClient,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        ledger,
        "local",
        blockedStream,
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(nextRequested).pipe(Effect.timeout("1 second"));
      yield* Fiber.interrupt(streamFiber);

      expect(yield* Deferred.await(iteratorClosed).pipe(Effect.as(true))).toBe(true);

      yield* runtimeCore.close;
    }),
  );

  it.effect("runs Kafka message streams and records stream failures", () =>
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
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          failingKafkaStream(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamFailed: Exit.isFailure(exit),
        snapshot,
      }).toStrictEqual({
        streamFailed: true,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-stream-1",
              customerId: "customer-stream-1",
              price: 30,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });
      expect({
        status: health.status,
        kafka: health.kafka,
      }).toStrictEqual({
        status: "degraded",
        kafka: {
          startFrom: kafkaOptions.consume,
          regions: nullRecord({
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "Kafka stream failed for region local",
            },
          }),
          topics: nullRecord({
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: nullRecord({
                local: {
                  connected: false,
                  assignedPartitions: 0,
                  messagesPerSecond: 1,
                  bytesPerSecond: 59,
                  decodedMessagesPerSecond: 1,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  publishFailuresPerSecond: 0,
                  commitFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 0,
                  lastCommitAt: 0,
                  consumerLagMessages: null,
                  lagSampledAt: null,
                  committedOffset: "5",
                  lastError: "Kafka stream failed for region local",
                },
              }),
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("exits cleanly when a Kafka message stream ends before yielding messages", () =>
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
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {})(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamSucceeded: Exit.isSuccess(exit),
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        streamSucceeded: true,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 0,
          version: 0,
        },
        kafkaTopic: {
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

  it.effect("records Kafka stream failures that happen before any message is yielded", () =>
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
      const failingStream: AsyncIterable<KafkaMessage> = {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(new Error("stream-start-failed")),
        }),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          failingStream,
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: "Kafka stream failed for region local",
          region: "local",
          sourceTopic: undefined,
        },
        kafkaTopic: {
          status: "degraded",
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
              lastError: "Kafka stream failed for region local",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records generic Kafka stream defects before iterator creation", () =>
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
      const defectiveStream: AsyncIterable<KafkaMessage> = {
        [Symbol.asyncIterator]: () => {
          throw new Error("iterator creation defect");
        },
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          defectiveStream,
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          cause: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          cause: "iterator creation defect",
          message: "Kafka stream failed for region local",
          region: "local",
          sourceTopic: undefined,
        },
        kafkaTopic: {
          status: "degraded",
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
              lastError: "Kafka stream failed for region local",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records generic Kafka stream defects during message metadata construction", () =>
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
      const defectiveMessage = kafkaMessage({
        topic: ordersSourceTopic,
        key: "metadata-defect",
        value: JSON.stringify({
          customerId: "customer-metadata-defect",
          price: 10,
        }),
      });
      Object.defineProperty(defectiveMessage, "timestamp", {
        get: () => {
          throw new Error("timestamp metadata defect");
        },
      });

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.internalClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield defectiveMessage;
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          cause: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          cause: "timestamp metadata defect",
          message: "Kafka stream failed for region local",
          region: "local",
          sourceTopic: undefined,
        },
        kafkaTopic: {
          status: "degraded",
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
              lastError: "Kafka stream failed for region local",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("offers typed Kafka stream producer failures without remapping", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<KafkaStreamQueueEvent>();
      const streamError = kafkaStreamError("local", "typed producer failure");

      yield* offerKafkaStreamProducerFailure("local", queue, Cause.fail(streamError));
      const event = yield* Queue.take(queue);

      expect(event).toStrictEqual({
        _tag: "Failed",
        cause: Cause.fail(streamError),
        error: streamError,
      });
    }),
  );

  it.effect("does not enqueue Kafka stream producer interrupts as failures", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<KafkaStreamQueueEvent>();

      const producerExit = yield* Effect.exit(
        offerKafkaStreamProducerFailure("local", queue, Cause.interrupt()),
      );
      const queueEvent = yield* Queue.poll(queue);

      expect({
        producerInterrupted: Exit.hasInterrupts(producerExit),
        queueEmpty: Option.isNone(queueEvent),
      }).toStrictEqual({
        producerInterrupted: true,
        queueEmpty: true,
      });
    }),
  );

  it.effect("preserves mixed Kafka stream producer failures and interrupts", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<KafkaStreamQueueEvent>();
      const streamError = kafkaStreamError("local", "typed producer failure with interrupt");

      yield* offerKafkaStreamProducerFailure(
        "local",
        queue,
        Cause.fromReasons([Cause.makeFailReason(streamError), Cause.makeInterruptReason()]),
      );
      const event = yield* Queue.take(queue);

      expect(event).toStrictEqual({
        _tag: "Failed",
        cause: Cause.fromReasons([Cause.makeFailReason(streamError), Cause.makeInterruptReason()]),
        error: streamError,
      });
    }),
  );

  it.effect("preserves raw Kafka stream producer failure reasons", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<KafkaStreamQueueEvent>();
      const rawFailure = new Error("raw producer typed failure");

      yield* offerKafkaStreamProducerFailure(
        "local",
        queue,
        Cause.fromReasons([Cause.makeFailReason(rawFailure), Cause.makeInterruptReason()]),
      );
      const event = yield* Queue.take(queue).pipe(
        Effect.filterOrFail(
          (value): value is Extract<KafkaStreamQueueEvent, { readonly _tag: "Failed" }> =>
            value._tag === "Failed",
          () =>
            new KafkaIngressTestError({
              message: "Expected Kafka stream queue failure event.",
            }),
        ),
      );

      expect({
        cause: causeReasonSummary(event.cause),
        error: {
          cause: messageFromUnknown(event.error.cause),
          message: event.error.message,
          region: event.error.region,
        },
      }).toStrictEqual({
        cause: [
          {
            tag: "Fail",
            message: "Kafka stream failed for region local",
          },
          {
            tag: "Fail",
            message: "raw producer typed failure",
          },
          {
            tag: "Interrupt",
            message: null,
          },
        ],
        error: {
          cause: "raw producer typed failure",
          message: "Kafka stream failed for region local",
          region: "local",
        },
      });
    }),
  );

  it.effect("preserves raw Kafka stream producer failures without optional source context", () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<KafkaStreamQueueEvent>();
      const primaryError = new ViewServerKafkaIngressError({
        cause: "primary producer failure",
        message: "primary producer failure",
      });
      const failureReasons: ReadonlyArray<Cause.Reason<unknown>> = [
        Cause.makeFailReason(primaryError),
        Cause.makeFailReason("raw producer failure without context"),
      ];

      yield* offerKafkaStreamProducerFailure("local", queue, Cause.fromReasons(failureReasons));
      const event = yield* Queue.take(queue).pipe(
        Effect.filterOrFail(
          (value): value is Extract<KafkaStreamQueueEvent, { readonly _tag: "Failed" }> =>
            value._tag === "Failed",
          () =>
            new KafkaIngressTestError({
              message: "Expected Kafka stream queue failure event.",
            }),
        ),
      );

      expect({
        cause: causeReasonSummary(event.cause),
        error: {
          message: event.error.message,
          region: event.error.region,
          sourceTopic: event.error.sourceTopic,
        },
      }).toStrictEqual({
        cause: [
          {
            tag: "Fail",
            message: "primary producer failure",
          },
          {
            tag: "Fail",
            message: "raw producer failure without context",
          },
        ],
        error: {
          message: "primary producer failure",
          region: undefined,
          sourceTopic: undefined,
        },
      });
    }),
  );

  it.effect("records Kafka stream producer defects that happen before iterator creation", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* Effect.gen(function* () {
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
        const defectiveStream: AsyncIterable<KafkaMessage> = {
          [Symbol.asyncIterator]: () => {
            throw new Error("stream-iterator-defect");
          },
        };

        const error = yield* Effect.flip(
          runKafkaMessageStream(
            viewServer,
            runtimeCore.internalClient,
            runtimeCore.requestHealthRefresh,
            kafkaOptions,
            ledger,
            "local",
            defectiveStream,
          ).pipe(
            Effect.timeout("1 second"),
            Effect.catchTag("TimeoutError", (error) =>
              Effect.fail(kafkaStreamError("local", error)),
            ),
          ),
        );
        const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

        expect({
          error: {
            message: error.message,
            region: error.region,
            sourceTopic: error.sourceTopic,
          },
          kafkaTopic: health.kafka?.topics[ordersSourceTopic],
        }).toStrictEqual({
          error: {
            message: "Kafka stream failed for region local",
            region: "local",
            sourceTopic: undefined,
          },
          kafkaTopic: {
            status: "degraded",
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
                lastError: "Kafka stream failed for region local",
              },
            }),
          },
        });
      }).pipe(Effect.ensuring(runtimeCore.close));
    }),
  );
});
