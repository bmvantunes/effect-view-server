import { describe, expect, it, vi } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import { Effect, Exit, Fiber, Scope } from "effect";
import {
  acquireKafkaRegionResources,
  bootstrapBrokers,
  closeKafkaConsumer,
  closeKafkaConsumerAfterStartFailure,
  closeKafkaConsumerOnStartFailure,
  kafkaConsumerCloseError,
  kafkaConsumerStartError,
  kafkaHeadersFromMessage,
  kafkaMessageCommitError,
  kafkaMessageDecodeError,
  kafkaMessageProcessingError,
  kafkaStreamCloseError,
  kafkaStreamError,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  registerKafkaConsumerHealthListeners,
  sourceTopicsForRegion,
} from "./kafka-ingress";
import { assignedPartitionsForSourceTopic } from "./kafka-health-observation";
import { acquireKafkaDeliveryResource, makeScopedKafkaDelivery } from "./kafka-delivery";
import {
  kafkaIngressErrorSourceTopicOrNull,
  kafkaOptions,
  makeViewServerKafkaHealthLedger,
  ordersSourceTopic,
  regions,
  runtimeUnavailable,
  type Topics,
  unknownSourceTopic,
} from "../test-harness/kafka-ingress";

describe("Kafka consumer Adapter", () => {
  it("normalizes consumer metadata and typed errors", () => {
    const normalizedHeaders = kafkaHeadersFromMessage(
      new Map([
        [Buffer.from("trace"), Buffer.from("abc")],
        [Buffer.from("trace"), Buffer.from("def")],
        [Buffer.from("trace"), Buffer.from("ghi")],
        [Buffer.from("__proto__"), Buffer.from("safe")],
      ]),
    );
    const consumerStart = kafkaConsumerStartError("local", "no-broker");
    const stream = kafkaStreamError("local", "stream-down");
    const consumerClose = kafkaConsumerCloseError("close-down");
    const streamClose = kafkaStreamCloseError("stream-close-down");
    const commit = kafkaMessageCommitError("local", ordersSourceTopic, "commit-down");
    const decode = kafkaMessageDecodeError("local", ordersSourceTopic, "decode-down");
    const processing = kafkaMessageProcessingError("local", ordersSourceTopic, "processing-down");

    expect(Object.getPrototypeOf(normalizedHeaders)).toBe(null);
    expect(normalizedHeaders["trace"]).toStrictEqual([
      Buffer.from("abc"),
      Buffer.from("def"),
      Buffer.from("ghi"),
    ]);
    expect(normalizedHeaders["__proto__"]).toStrictEqual(Buffer.from("safe"));
    expect({
      messages: {
        error: messageFromUnknown(new Error("boom")),
        tagged: messageFromUnknown(runtimeUnavailable),
        nonString: messageFromUnknown({ message: 123 }),
        plain: messageFromUnknown("plain"),
      },
      brokers: bootstrapBrokers(regions.local),
      assignments: {
        orders: assignedPartitionsForSourceTopic(
          [{ topic: ordersSourceTopic, partitions: [0, 1] }],
          ordersSourceTopic,
        ),
        missing: assignedPartitionsForSourceTopic(
          [{ topic: ordersSourceTopic, partitions: [0, 1] }],
          unknownSourceTopic,
        ),
      },
      sourceTopics: {
        local: sourceTopicsForRegion(kafkaOptions, "local"),
        cold: sourceTopicsForRegion(kafkaOptions, "cold"),
      },
      errorSources: {
        decode: kafkaIngressErrorSourceTopicOrNull(decode),
        close: kafkaIngressErrorSourceTopicOrNull(consumerClose),
        unrelated: kafkaIngressErrorSourceTopicOrNull(new Error("not kafka")),
      },
    }).toStrictEqual({
      messages: {
        error: "boom",
        tagged: "publish failed",
        nonString: "[object Object]",
        plain: "plain",
      },
      brokers: ["localhost:9092", "localhost:9094"],
      assignments: {
        orders: 2,
        missing: 0,
      },
      sourceTopics: {
        local: [ordersSourceTopic],
        cold: [],
      },
      errorSources: {
        decode: ordersSourceTopic,
        close: null,
        unrelated: null,
      },
    });
    expect({
      consumerStart: {
        message: consumerStart.message,
        cause: consumerStart.cause,
        region: consumerStart.region,
      },
      mappedConsumerStart: {
        message: mapKafkaConsumerStartError("local")("no-broker").message,
        cause: mapKafkaConsumerStartError("local")("no-broker").cause,
        region: mapKafkaConsumerStartError("local")("no-broker").region,
      },
      stream: {
        message: stream.message,
        cause: stream.cause,
        region: stream.region,
      },
      mappedStream: {
        message: mapKafkaStreamError("local")("stream-down").message,
        cause: mapKafkaStreamError("local")("stream-down").cause,
        region: mapKafkaStreamError("local")("stream-down").region,
      },
      consumerClose: {
        message: consumerClose.message,
        cause: consumerClose.cause,
      },
      streamClose: {
        message: streamClose.message,
        cause: streamClose.cause,
      },
      commit: {
        message: commit.message,
        cause: commit.cause,
        region: commit.region,
        sourceTopic: commit.sourceTopic,
      },
      decode: {
        message: decode.message,
        cause: decode.cause,
        region: decode.region,
        sourceTopic: decode.sourceTopic,
      },
      processing: {
        message: processing.message,
        cause: processing.cause,
        region: processing.region,
        sourceTopic: processing.sourceTopic,
      },
    }).toStrictEqual({
      consumerStart: {
        message: "Failed to start Kafka consumer for region local",
        cause: "no-broker",
        region: "local",
      },
      mappedConsumerStart: {
        message: "Failed to start Kafka consumer for region local",
        cause: "no-broker",
        region: "local",
      },
      stream: {
        message: "Kafka stream failed for region local",
        cause: "stream-down",
        region: "local",
      },
      mappedStream: {
        message: "Kafka stream failed for region local",
        cause: "stream-down",
        region: "local",
      },
      consumerClose: {
        message: "Failed to close Kafka consumer",
        cause: "close-down",
      },
      streamClose: {
        message: "Failed to close Kafka stream",
        cause: "stream-close-down",
      },
      commit: {
        message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
        cause: "commit-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
      decode: {
        message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
        cause: "decode-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
      processing: {
        message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
        cause: "processing-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
    });
  });

  it.effect("removes observation listeners without owning lag monitoring or the consumer", () =>
    Effect.gen(function* () {
      const health = makeViewServerKafkaHealthLedger<Topics>({
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
        clientId: "view-server-observation-ownership-test",
        groupId: "view-server-observation-ownership-test",
      });
      const closeConsumer = vi.spyOn(consumer, "close").mockImplementation(() => Promise.resolve());
      const stopLagMonitoring = vi
        .spyOn(consumer, "stopLagMonitoring")
        .mockImplementation(() => undefined);
      const scope = yield* Scope.make("parallel");
      const registration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        health,
        "local",
        [ordersSourceTopic],
        scope,
      );

      yield* registration.close;
      yield* Scope.close(scope, Exit.void);

      expect({
        consumerCloseCalls: closeConsumer.mock.calls.length,
        stopLagMonitoringCalls: stopLagMonitoring.mock.calls.length,
      }).toStrictEqual({
        consumerCloseCalls: 0,
        stopLagMonitoringCalls: 0,
      });

      closeConsumer.mockRestore();
      stopLagMonitoring.mockRestore();
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
    }),
  );

  it.effect("closes a constructed consumer when consume startup fails or is interrupted", () =>
    Effect.gen(function* () {
      const closeForces: Array<boolean | undefined> = [];
      const consumer = {
        close: (force?: boolean) => {
          closeForces.push(force);
        },
      };

      yield* closeKafkaConsumerAfterStartFailure(consumer);
      const failed = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          consumer,
          Effect.fail(kafkaConsumerStartError("local", "no-broker")),
        ),
      );
      const interrupted = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(consumer, Effect.interrupt),
      );

      expect({
        closeForces,
        failed: Exit.isFailure(failed),
        interrupted: Exit.hasInterrupts(interrupted),
      }).toStrictEqual({
        closeForces: [true, true, true],
        failed: true,
        interrupted: true,
      });
    }),
  );

  it.effect("rolls back Region resources when listener registration defects", () =>
    Effect.gen(function* () {
      const successfulCleanupOperations: Array<string> = [];
      const successfulCleanup = yield* Effect.exit(
        acquireKafkaRegionResources(
          Effect.sync(() => {
            successfulCleanupOperations.push("acquire");
            return "consumer";
          }),
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("register");
            }).pipe(Effect.andThen(Effect.die(new Error("registration defect")))),
          () => Effect.void,
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("close consumer");
            }),
          () => Effect.void,
        ),
      );
      const failedCleanupOperations: Array<string> = [];
      const failedCleanup = yield* Effect.exit(
        acquireKafkaRegionResources(
          Effect.sync(() => {
            failedCleanupOperations.push("acquire");
            return "consumer";
          }),
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("register");
            }).pipe(Effect.andThen(Effect.die(new Error("registration defect")))),
          () => Effect.void,
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("close consumer");
            }).pipe(Effect.andThen(Effect.die(new Error("consumer close defect")))),
          () => Effect.void,
        ),
      );

      expect({
        failedCleanup: Exit.isFailure(failedCleanup),
        failedCleanupOperations,
        successfulCleanup: Exit.isFailure(successfulCleanup),
        successfulCleanupOperations,
      }).toStrictEqual({
        failedCleanup: true,
        failedCleanupOperations: ["acquire", "register", "close consumer"],
        successfulCleanup: true,
        successfulCleanupOperations: ["acquire", "register", "close consumer"],
      });
    }),
  );

  it.effect("rolls back listeners and consumer when lag monitoring defects", () =>
    Effect.gen(function* () {
      const successfulCleanupOperations: Array<string> = [];
      const successfulCleanup = yield* Effect.exit(
        acquireKafkaRegionResources(
          Effect.sync(() => {
            successfulCleanupOperations.push("acquire");
            return "consumer";
          }),
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("register");
              return "listeners";
            }),
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("start lag");
            }).pipe(Effect.andThen(Effect.die(new Error("lag monitoring defect")))),
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("close consumer");
            }),
          () =>
            Effect.sync(() => {
              successfulCleanupOperations.push("close listeners");
            }),
        ),
      );
      const failedCleanupOperations: Array<string> = [];
      const failedCleanup = yield* Effect.exit(
        acquireKafkaRegionResources(
          Effect.sync(() => {
            failedCleanupOperations.push("acquire");
            return "consumer";
          }),
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("register");
              return "listeners";
            }),
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("start lag");
            }).pipe(Effect.andThen(Effect.die(new Error("lag monitoring defect")))),
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("close consumer");
            }).pipe(Effect.andThen(Effect.die(new Error("consumer close defect")))),
          () =>
            Effect.sync(() => {
              failedCleanupOperations.push("close listeners");
            }).pipe(Effect.andThen(Effect.die(new Error("listener close defect")))),
        ),
      );

      expect({
        failedCleanup: Exit.isFailure(failedCleanup),
        failedCleanupOperations,
        successfulCleanup: Exit.isFailure(successfulCleanup),
        successfulCleanupOperations,
      }).toStrictEqual({
        failedCleanup: true,
        failedCleanupOperations: [
          "acquire",
          "register",
          "start lag",
          "close listeners",
          "close consumer",
        ],
        successfulCleanup: true,
        successfulCleanupOperations: [
          "acquire",
          "register",
          "start lag",
          "close listeners",
          "close consumer",
        ],
      });
    }),
  );

  it.effect(
    "interrupts a never-resolving consume startup and closes its constructed consumer",
    () =>
      Effect.gen(function* () {
        const closeForces: Array<boolean | undefined> = [];
        const consumer = {
          close: (force?: boolean) => {
            closeForces.push(force);
          },
        };
        let signalConsumeStarted = () => {};
        const consumeStarted = new Promise<void>((resolve) => {
          signalConsumeStarted = resolve;
        });
        const pendingConsume = new Promise<never>(() => {});
        const startup = yield* makeScopedKafkaDelivery((startWorker) =>
          startWorker(
            acquireKafkaDeliveryResource(
              closeKafkaConsumerOnStartFailure(
                consumer,
                Effect.tryPromise({
                  try: () => {
                    signalConsumeStarted();
                    return pendingConsume;
                  },
                  catch: (cause) => kafkaConsumerStartError("local", cause),
                }),
              ),
              () => Effect.void,
            ),
            () => Effect.never,
          ),
        ).pipe(Effect.forkChild({ startImmediately: true }));

        yield* Effect.promise(() => consumeStarted);
        yield* Fiber.interrupt(startup);
        const interrupted = yield* Fiber.await(startup);

        expect({
          closeForces,
          interrupted: Exit.hasInterrupts(interrupted),
        }).toStrictEqual({
          closeForces: [true],
          interrupted: true,
        });
      }),
  );

  it.effect("closes the stream and consumer even when stream close fails", () =>
    Effect.gen(function* () {
      const operations: Array<string> = [];
      const successful = yield* Effect.exit(
        closeKafkaConsumer({
          consumer: {
            close: (force) => {
              operations.push(`consumer:${String(force)}`);
            },
          },
          stream: {
            close: () => {
              operations.push("stream:success");
            },
          },
        }),
      );
      const failed = yield* Effect.exit(
        closeKafkaConsumer({
          consumer: {
            close: (force) => {
              operations.push(`consumer-after-failure:${String(force)}`);
            },
          },
          stream: {
            close: () => {
              operations.push("stream:failure");
              throw new Error("stream close failed");
            },
          },
        }),
      );

      expect({
        operations,
        successful: Exit.isSuccess(successful),
        failed: Exit.isFailure(failed),
      }).toStrictEqual({
        operations: [
          "stream:success",
          "consumer:true",
          "stream:failure",
          "consumer-after-failure:true",
        ],
        successful: true,
        failed: true,
      });
    }),
  );
});
