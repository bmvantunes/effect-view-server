import { describe, expect, it } from "@effect/vitest";
import { Buffer } from "node:buffer";
import { Deferred, Effect, Exit, Fiber, Scope } from "effect";
import {
  assignedPartitionsForSourceTopic,
  acquireStartedKafkaConsumerResources,
  bootstrapBrokers,
  closeKafkaConsumer,
  closeKafkaConsumerAfterStartFailure,
  closeKafkaConsumerOnPostConsumeStartupFailure,
  closeKafkaConsumerOnStartFailure,
  closeKafkaMessageStreamFiber,
  closeStartedKafkaRegionConsumers,
  closeStartedKafkaConsumerResources,
  kafkaHeadersFromMessage,
  kafkaConsumerCloseError,
  kafkaConsumerStartError,
  kafkaMessageCommitError,
  kafkaMessageDecodeError,
  kafkaMessageProcessingError,
  kafkaStreamCloseError,
  kafkaStreamError,
  makeScopedKafkaIngress,
  makeStartedKafkaConsumerResourcesFinalizer,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  registerStartedKafkaConsumerResourcesFinalizer,
  sourceTopicsForRegion,
} from "./kafka-ingress";
import type { StartedKafkaConsumerResources } from "./kafka-ingress";
import {
  kafkaIngressErrorSourceTopicOrNull,
  kafkaOptions,
  ordersSourceTopic,
  regions,
  runtimeUnavailable,
  unknownSourceTopic,
} from "../test-harness/kafka-ingress";

describe("Kafka ingress resource lifecycle internals", () => {
  it("normalizes Kafka helper values", () => {
    const headers = new Map([
      [Buffer.from("trace"), Buffer.from("abc")],
      [Buffer.from("trace"), Buffer.from("def")],
      [Buffer.from("trace"), Buffer.from("ghi")],
      [Buffer.from("__proto__"), Buffer.from("safe")],
    ]);
    const normalizedHeaders = kafkaHeadersFromMessage(headers);

    expect({
      errorMessage: messageFromUnknown(new Error("boom")),
      taggedErrorMessage: messageFromUnknown(runtimeUnavailable),
      nonStringMessage: messageFromUnknown({ message: 123 }),
      plainMessage: messageFromUnknown("plain"),
      bootstrapBrokers: bootstrapBrokers(regions.local),
      assignedOrdersPartitions: assignedPartitionsForSourceTopic(
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        ordersSourceTopic,
      ),
      assignedMissingPartitions: assignedPartitionsForSourceTopic(
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        unknownSourceTopic,
      ),
      errorSourceTopic: kafkaIngressErrorSourceTopicOrNull(
        kafkaMessageDecodeError("local", ordersSourceTopic, "decode-down"),
      ),
      errorSourceTopicWithoutSource: kafkaIngressErrorSourceTopicOrNull(
        kafkaConsumerCloseError("close-down"),
      ),
      unrelatedErrorSourceTopic: kafkaIngressErrorSourceTopicOrNull(new Error("not kafka")),
    }).toStrictEqual({
      errorMessage: "boom",
      taggedErrorMessage: "publish failed",
      nonStringMessage: "[object Object]",
      plainMessage: "plain",
      bootstrapBrokers: ["localhost:9092", "localhost:9094"],
      assignedOrdersPartitions: 2,
      assignedMissingPartitions: 0,
      errorSourceTopic: ordersSourceTopic,
      errorSourceTopicWithoutSource: null,
      unrelatedErrorSourceTopic: null,
    });
    expect(Object.getPrototypeOf(normalizedHeaders)).toBe(null);
    expect(normalizedHeaders["trace"]).toStrictEqual([
      Buffer.from("abc"),
      Buffer.from("def"),
      Buffer.from("ghi"),
    ]);
    expect(normalizedHeaders["__proto__"]).toStrictEqual(Buffer.from("safe"));
    const consumerError = kafkaConsumerStartError("local", "no-broker");
    const streamError = kafkaStreamError("local", "stream-down");
    const consumerCloseError = kafkaConsumerCloseError("close-down");
    const streamCloseError = kafkaStreamCloseError("stream-close-down");
    const commitError = kafkaMessageCommitError("local", ordersSourceTopic, "commit-down");
    const decodeError = kafkaMessageDecodeError("local", ordersSourceTopic, "decode-down");
    const processingError = kafkaMessageProcessingError(
      "local",
      ordersSourceTopic,
      "processing-down",
    );
    expect({
      _tag: consumerError._tag,
      message: consumerError.message,
      cause: consumerError.cause,
      region: consumerError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to start Kafka consumer for region local",
      cause: "no-broker",
      region: "local",
    });
    expect({
      _tag: mapKafkaConsumerStartError("local")("no-broker")._tag,
      message: mapKafkaConsumerStartError("local")("no-broker").message,
      cause: mapKafkaConsumerStartError("local")("no-broker").cause,
      region: mapKafkaConsumerStartError("local")("no-broker").region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to start Kafka consumer for region local",
      cause: "no-broker",
      region: "local",
    });
    expect({
      _tag: streamError._tag,
      message: streamError.message,
      cause: streamError.cause,
      region: streamError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Kafka stream failed for region local",
      cause: "stream-down",
      region: "local",
    });
    expect({
      _tag: mapKafkaStreamError("local")("stream-down")._tag,
      message: mapKafkaStreamError("local")("stream-down").message,
      cause: mapKafkaStreamError("local")("stream-down").cause,
      region: mapKafkaStreamError("local")("stream-down").region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Kafka stream failed for region local",
      cause: "stream-down",
      region: "local",
    });
    expect({
      _tag: consumerCloseError._tag,
      message: consumerCloseError.message,
      cause: consumerCloseError.cause,
      region: consumerCloseError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to close Kafka consumer",
      cause: "close-down",
      region: undefined,
    });
    expect({
      _tag: streamCloseError._tag,
      message: streamCloseError.message,
      cause: streamCloseError.cause,
      region: streamCloseError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to close Kafka stream",
      cause: "stream-close-down",
      region: undefined,
    });
    expect({
      _tag: commitError._tag,
      message: commitError.message,
      cause: commitError.cause,
      region: commitError.region,
      sourceTopic: commitError.sourceTopic,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
      cause: "commit-down",
      region: "local",
      sourceTopic: ordersSourceTopic,
    });
    expect({
      decode: {
        _tag: decodeError._tag,
        message: decodeError.message,
        cause: decodeError.cause,
        region: decodeError.region,
        sourceTopic: decodeError.sourceTopic,
      },
      processing: {
        _tag: processingError._tag,
        message: processingError.message,
        cause: processingError.cause,
        region: processingError.region,
        sourceTopic: processingError.sourceTopic,
      },
    }).toStrictEqual({
      decode: {
        _tag: "ViewServerKafkaIngressError",
        message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
        cause: "decode-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
      processing: {
        _tag: "ViewServerKafkaIngressError",
        message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
        cause: "processing-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
    });
    expect(sourceTopicsForRegion(kafkaOptions, "local")).toStrictEqual([ordersSourceTopic]);
    expect(sourceTopicsForRegion(kafkaOptions, "cold")).toStrictEqual([]);
  });

  it.effect("closes constructed Kafka consumers after consume startup failures", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;

      yield* closeKafkaConsumerAfterStartFailure({
        close: (force) => {
          closeForce = force;
        },
      });

      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes Kafka consumers when startup effects fail or are interrupted", () =>
    Effect.gen(function* () {
      let failedCloseForce: boolean | undefined = undefined;
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          {
            close: (force) => {
              failedCloseForce = force;
            },
          },
          Effect.fail(kafkaConsumerStartError("local", "no-broker")),
        ),
      );
      let interruptedCloseForce: boolean | undefined = undefined;
      const interruptedExit = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          {
            close: (force) => {
              interruptedCloseForce = force;
            },
          },
          Effect.interrupt,
        ),
      );

      expect({
        failed: Exit.isFailure(failedExit),
        failedCloseForce,
        interrupted: Exit.hasInterrupts(interruptedExit),
        interruptedCloseForce,
      }).toStrictEqual({
        failed: true,
        failedCloseForce: true,
        interrupted: true,
        interruptedCloseForce: true,
      });
    }),
  );

  it.effect("closes stream and consumer resources", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;

      yield* closeKafkaConsumer({
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
      });

      expect(streamCloseCount).toBe(1);
      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes Kafka consumers even when stream close fails", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;

      const closeExit = yield* Effect.exit(
        closeKafkaConsumer({
          consumer: {
            close: (force) => {
              closeForce = force;
            },
          },
          stream: {
            close: () => {
              streamCloseCount += 1;
              throw new Error("stream close failed");
            },
          },
        }),
      );

      expect({
        closeForce,
        closeFailurePreserved: Exit.isFailure(closeExit),
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        closeFailurePreserved: true,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("closes started Kafka resources with and without health listeners", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      let listenerCloseCount = 0;
      const resourcesWithListeners: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.sync(() => {
            listenerCloseCount += 1;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };
      const resourcesWithoutListeners: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };

      yield* closeStartedKafkaConsumerResources(resourcesWithListeners);
      yield* closeStartedKafkaConsumerResources(resourcesWithoutListeners);

      expect(streamCloseCount).toBe(2);
      expect(listenerCloseCount).toBe(1);
      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes started Kafka resources when health listener cleanup defects", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.die(new Error("listener close failed")),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };

      const exit = yield* Effect.exit(closeStartedKafkaConsumerResources(resources));

      expect({
        closeForce,
        defectPreserved: Exit.hasDies(exit),
        failed: Exit.isFailure(exit),
        interrupted: Exit.hasInterrupts(exit),
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        defectPreserved: true,
        failed: true,
        interrupted: false,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("retries started Kafka resource cleanup after a defecting close attempt", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let listenerCloseCount = 0;
      let streamCloseCount = 0;
      let listenerClose: Effect.Effect<void> = Effect.die(new Error("listener close failed"));
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.suspend(() => {
            listenerCloseCount += 1;
            const close = listenerClose;
            listenerClose = Effect.void;
            return close;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };
      const finalizer = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);
      const firstExit = yield* Effect.exit(finalizer);
      yield* finalizer;
      yield* finalizer;

      expect({
        closeForce,
        firstAttemptDefected: Exit.hasDies(firstExit),
        listenerCloseCount,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        firstAttemptDefected: true,
        listenerCloseCount: 2,
        streamCloseCount: 2,
      });
    }),
  );

  it.effect("registers started Kafka resource cleanup on scope close", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      const scope = yield* Scope.make();
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };
      const closeResources = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);

      yield* registerStartedKafkaConsumerResourcesFinalizer(scope, closeResources);
      yield* Scope.close(scope, Exit.void);

      expect({
        closeForce,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("keeps scoped started Kafka resource cleanup idempotent after explicit close", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      const scope = yield* Scope.make();
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };
      const closeResources = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);

      yield* registerStartedKafkaConsumerResourcesFinalizer(scope, closeResources);
      yield* closeResources;
      yield* Scope.close(scope, Exit.void);

      expect({
        closeForce,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("registers started Kafka resource cleanup atomically after acquire", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let listenerCloseCount = 0;
      let streamCloseCount = 0;
      const scope = yield* Scope.make();
      const consumer: StartedKafkaConsumerResources["consumer"] = {
        close: (force?: boolean) => {
          closeForce = force;
        },
      };
      const stream: StartedKafkaConsumerResources["stream"] = {
        close: () => {
          streamCloseCount += 1;
        },
      };
      const resources = yield* acquireStartedKafkaConsumerResources(
        scope,
        Effect.succeed({
          consumer,
          stream,
        }),
      );

      resources.setHealthListeners({
        close: Effect.sync(() => {
          listenerCloseCount += 1;
        }),
        processed: Effect.succeed(0),
        waitForProcessed: () => Effect.void,
      });
      yield* Scope.close(scope, Exit.void);

      expect({
        closeForce,
        listenerCloseCount,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        listenerCloseCount: 1,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("closes scoped Kafka ingress resources when acquisition is interrupted", () =>
    Effect.gen(function* () {
      let scopeFinalizerCount = 0;
      const consumerStarted = yield* Deferred.make<void>();
      const ingressFiber = yield* makeScopedKafkaIngress((scope) =>
        Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            scopeFinalizerCount += 1;
          }),
        ).pipe(
          Effect.andThen(Deferred.succeed(consumerStarted, undefined)),
          Effect.andThen(Effect.never),
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(consumerStarted).pipe(Effect.timeout("1 second"));
      yield* Fiber.interrupt(ingressFiber);

      expect(scopeFinalizerCount).toBe(1);
    }),
  );

  it.effect("requests Kafka stream interruption before closing resources", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const interruptStarted = yield* Deferred.make<void>();
      const releaseStreamFinalizer = yield* Deferred.make<void>();
      const resourceCloseObservedInterrupt = yield* Deferred.make<boolean>();
      const streamFiber = yield* Effect.never.pipe(
        Effect.onInterrupt(() => Deferred.succeed(interruptStarted, undefined)),
        Effect.ensuring(
          Deferred.await(releaseStreamFinalizer).pipe(
            Effect.andThen(
              Effect.sync(() => {
                streamFinalizerCount += 1;
              }),
            ),
          ),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* closeKafkaMessageStreamFiber(
        streamFiber,
        Effect.gen(function* () {
          closeResourcesCount += 1;
          yield* Deferred.await(interruptStarted);
          yield* Deferred.succeed(resourceCloseObservedInterrupt, true);
          yield* Deferred.succeed(releaseStreamFinalizer, undefined);
        }),
      );

      expect({
        closeResourcesCount,
        resourceCloseObservedInterrupt: yield* Deferred.await(resourceCloseObservedInterrupt),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        resourceCloseObservedInterrupt: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("closes Kafka resources to unblock a pending stream before awaiting finalizers", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const unblockPendingStreamRead = yield* Deferred.make<void>();
      const streamFiber = yield* Deferred.await(unblockPendingStreamRead).pipe(
        Effect.uninterruptible,
        Effect.ensuring(
          Effect.sync(() => {
            streamFinalizerCount += 1;
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* closeKafkaMessageStreamFiber(
        streamFiber,
        Effect.gen(function* () {
          closeResourcesCount += 1;
          yield* Deferred.succeed(unblockPendingStreamRead, undefined);
        }),
      ).pipe(Effect.timeout("1 second"));

      expect({
        closeResourcesCount,
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect(
    "continues Kafka stream cleanup when close is interrupted during resource cleanup",
    () =>
      Effect.gen(function* () {
        let closeResourcesCount = 0;
        let streamFinalizerCount = 0;
        const closeResourcesStarted = yield* Deferred.make<void>();
        const releaseCloseResources = yield* Deferred.make<void>();
        const unblockPendingStreamRead = yield* Deferred.make<void>();
        const streamFiber = yield* Deferred.await(unblockPendingStreamRead).pipe(
          Effect.uninterruptible,
          Effect.ensuring(
            Effect.sync(() => {
              streamFinalizerCount += 1;
            }),
          ),
          Effect.forkChild({ startImmediately: true }),
        );
        const closeFiber = yield* closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.gen(function* () {
            closeResourcesCount += 1;
            yield* Deferred.succeed(closeResourcesStarted, undefined);
            yield* Deferred.await(releaseCloseResources);
            yield* Deferred.succeed(unblockPendingStreamRead, undefined);
          }),
        ).pipe(Effect.forkChild({ startImmediately: true }));

        yield* Deferred.await(closeResourcesStarted).pipe(Effect.timeout("1 second"));
        const interruptCloseFiber = yield* Fiber.interrupt(closeFiber).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(releaseCloseResources, undefined);
        yield* Fiber.join(interruptCloseFiber);

        expect({
          closeResourcesCount,
          streamFinalizerCount,
        }).toStrictEqual({
          closeResourcesCount: 1,
          streamFinalizerCount: 1,
        });
      }),
  );

  it.effect("waits for Kafka stream fiber finalizers when resource cleanup defects", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const streamFiber = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            streamFinalizerCount += 1;
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      const exit = yield* Effect.exit(
        closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.gen(function* () {
            closeResourcesCount += 1;
            return yield* Effect.die(new Error("resource close failed"));
          }),
        ),
      );

      expect({
        closeResourcesCount,
        defectPreserved: Exit.hasDies(exit),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        defectPreserved: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("preserves Kafka stream fiber finalizer defects during close", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const streamFiber = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            streamFinalizerCount += 1;
            return yield* Effect.die(new Error("stream finalizer failed"));
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      const exit = yield* Effect.exit(
        closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.sync(() => {
            closeResourcesCount += 1;
          }),
        ),
      );

      expect({
        closeResourcesCount,
        defectPreserved: Exit.hasDies(exit),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        defectPreserved: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("closes all started region consumers before returning close defects", () =>
    Effect.gen(function* () {
      const closed: Array<string> = [];
      const closeExit = yield* Effect.exit(
        closeStartedKafkaRegionConsumers([
          {
            close: Effect.gen(function* () {
              closed.push("first");
              return yield* Effect.die(new Error("first close failed"));
            }),
          },
          {
            close: Effect.sync(() => {
              closed.push("second");
            }),
          },
          {
            close: Effect.sync(() => {
              closed.push("third");
            }),
          },
        ]),
      );

      expect({
        closed,
        defectPreserved: Exit.hasDies(closeExit),
      }).toStrictEqual({
        closed: ["first", "second", "third"],
        defectPreserved: true,
      });
    }),
  );

  it.effect("cleans post-consume Kafka resources only when later startup fails", () =>
    Effect.gen(function* () {
      let successStreamCloseCount = 0;
      let failedStreamCloseCount = 0;
      let failedListenerCloseCount = 0;
      let successCloseForce: boolean | undefined = undefined;
      let failedCloseForce: boolean | undefined = undefined;
      const successResources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            successCloseForce = force;
          },
        },
        stream: {
          close: () => {
            successStreamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };
      const failedResources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            failedCloseForce = force;
          },
        },
        stream: {
          close: () => {
            failedStreamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.sync(() => {
            failedListenerCloseCount += 1;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };

      const successCloseResources =
        yield* makeStartedKafkaConsumerResourcesFinalizer(successResources);
      const failedCloseResources =
        yield* makeStartedKafkaConsumerResourcesFinalizer(failedResources);
      const success = yield* closeKafkaConsumerOnPostConsumeStartupFailure(
        successCloseResources,
        Effect.succeed("started"),
      );
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnPostConsumeStartupFailure(
          failedCloseResources,
          Effect.fail(kafkaConsumerStartError("local", "post-consume-down")),
        ),
      );

      expect(success).toBe("started");
      expect(successStreamCloseCount).toBe(0);
      expect(successCloseForce).toBe(undefined);
      expect(Exit.isFailure(failedExit)).toBe(true);
      expect(failedStreamCloseCount).toBe(1);
      expect(failedListenerCloseCount).toBe(1);
      expect(failedCloseForce).toBe(true);
    }),
  );

  it.effect("keeps post-consume startup failure cleanup idempotent with scoped cleanup", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let listenerCloseCount = 0;
      let streamCloseCount = 0;
      const scope = yield* Scope.make();
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.sync(() => {
            listenerCloseCount += 1;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };
      const closeResources = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);

      yield* registerStartedKafkaConsumerResourcesFinalizer(scope, closeResources);
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnPostConsumeStartupFailure(
          closeResources,
          Effect.fail(kafkaConsumerStartError("local", "post-consume-down")),
        ),
      );
      yield* Scope.close(scope, Exit.void);

      expect({
        closeForce,
        failed: Exit.isFailure(failedExit),
        listenerCloseCount,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        failed: true,
        listenerCloseCount: 1,
        streamCloseCount: 1,
      });
    }),
  );
});
