import { describe, expect, it } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import type { Message } from "@platformatic/kafka";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "@view-server/config";
import { makeViewServerRuntimeCore } from "@view-server/runtime-core";
import { Buffer } from "node:buffer";
import { Effect, Exit, Schema } from "effect";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import {
  assignedPartitionsForSourceTopic,
  bootstrapBrokers,
  closeKafkaConsumer,
  closeKafkaConsumerAfterStartFailure,
  closeKafkaConsumerOnPostConsumeStartupFailure,
  closeKafkaConsumerOnStartFailure,
  closeStartedKafkaConsumerResources,
  kafkaHeadersFromMessage,
  kafkaConsumerCloseError,
  kafkaConsumerStartError,
  kafkaMessageCommitError,
  kafkaMessageDecodeError,
  kafkaMessageProcessingError,
  kafkaStreamCloseError,
  kafkaStreamError,
  makeViewServerKafkaIngress,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  processKafkaMessage,
  recordKafkaAssignments,
  recordKafkaLag,
  recordKafkaStreamError,
  registerKafkaConsumerHealthListeners,
  runKafkaMessageStream,
  sourceTopicsForRegion,
  startKafkaRegionConsumers,
} from "./kafka-ingress";
import type {
  StartedKafkaConsumerResources,
  StartedKafkaRegionConsumer,
  ViewServerKafkaIngressError,
} from "./kafka-ingress";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./runtime-options";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
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

type Topics = typeof viewServer.topics;
type KafkaMessageBytes = Buffer | null | undefined;
type KafkaMessage = Message<KafkaMessageBytes, KafkaMessageBytes, Buffer, Buffer>;

const regions = {
  cold: "localhost:9093",
  local: " localhost:9092, ,localhost:9094 ",
};
const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
const ordersSourceTopic = "orders-source";
const unknownSourceTopic = "unknown-source";

const kafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
  consumerGroupId: "view-server-test",
  regions,
  topics: {
    [ordersSourceTopic]: localKafkaTopic({
      regions: ["local"],
      value: kafka.json(IncomingOrder),
      key: kafka.stringKey(),
      viewServerTopic: "orders",
      mapping: ({ key, value }) => ({
        id: key,
        customerId: value.customerId,
        price: value.price,
      }),
    }),
  },
};

const kafkaMessage = (input: {
  readonly topic: string;
  readonly key?: string | null;
  readonly value?: string | null;
  readonly headers?: ReadonlyMap<Buffer, Buffer>;
  readonly offset?: bigint;
  readonly onCommit?: () => void;
  readonly commitFailure?: Error;
}): KafkaMessage => {
  const headers = new Map(input.headers ?? []);
  const key = input.key === undefined || input.key === null ? input.key : Buffer.from(input.key);
  const value =
    input.value === undefined || input.value === null ? input.value : Buffer.from(input.value);
  const offset = input.offset ?? 0n;
  return {
    key,
    value,
    headers,
    topic: input.topic,
    partition: 0,
    timestamp: 1_234n,
    offset,
    metadata: {},
    commit: () => {
      if (input.commitFailure !== undefined) {
        return Promise.reject(input.commitFailure);
      }
      input.onCommit?.();
      return undefined;
    },
    toJSON: () => ({
      key,
      value,
      headers: Array.from(headers.entries()),
      topic: input.topic,
      partition: 0,
      timestamp: "1234",
      offset: String(offset),
      metadata: {},
    }),
  };
};

const runtimeUnavailable: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "publish failed",
};

const failingClient: ViewServerRuntimeClient<Topics> = {
  delete: () => Effect.fail(runtimeUnavailable),
  health: () => Effect.fail(runtimeUnavailable),
  patch: () => Effect.fail(runtimeUnavailable),
  publish: () => Effect.fail(runtimeUnavailable),
  publishMany: () => Effect.fail(runtimeUnavailable),
  reset: () => Effect.fail(runtimeUnavailable),
  snapshot: () => Effect.fail(runtimeUnavailable),
};

async function* failingKafkaStream(): AsyncIterable<KafkaMessage> {
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "order-stream-1",
    value: JSON.stringify({
      customerId: "customer-stream-1",
      price: 30,
    }),
    offset: 4n,
  });
  throw new Error("stream-down");
}

async function* decodeFailureThenSuccessKafkaStream(
  onCommit: () => void,
): AsyncIterable<KafkaMessage> {
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "bad-json",
    value: "{",
    offset: 1n,
    onCommit,
  });
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "order-after-failure",
    value: JSON.stringify({
      customerId: "customer-after-failure",
      price: 40,
    }),
    offset: 2n,
    onCommit,
  });
}

describe("@view-server/runtime Kafka ingress internals", () => {
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
    }).toStrictEqual({
      errorMessage: "boom",
      taggedErrorMessage: "publish failed",
      nonStringMessage: "[object Object]",
      plainMessage: "plain",
      bootstrapBrokers: ["localhost:9092", "localhost:9094"],
      assignedOrdersPartitions: 2,
      assignedMissingPartitions: 0,
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

  it.effect("closes Kafka consumers when startup effects fail", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      const exit = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          {
            close: (force) => {
              closeForce = force;
            },
          },
          Effect.fail(kafkaConsumerStartError("local", "no-broker")),
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(closeForce).toBe(true);
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
        }),
      };

      const success = yield* closeKafkaConsumerOnPostConsumeStartupFailure(
        successResources,
        Effect.succeed("started"),
      );
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnPostConsumeStartupFailure(
          failedResources,
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

  it.effect("records Kafka health transitions and ignores unknown ledger keys", () =>
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
      yield* ledger.topicConnected(ordersSourceTopic, "local", 2, 1_000);
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 20,
        committedOffset: "2",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 2_000,
      });
      yield* ledger.regionConnected("missing", 2_000);
      yield* ledger.regionDisconnected("local", "lost");
      yield* ledger.regionDisconnected("missing", "ignored");
      yield* ledger.topicConnected("missing", "local", 1, 2_000);
      yield* ledger.messageDecoded("missing", "local", {
        bytes: 1,
        committedOffset: "3",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.messageProcessingFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        status: health.status,
        kafka: health.kafka,
      }).toStrictEqual({
        status: "degraded",
        kafka: {
          regions: {
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "lost",
            },
          },
          topics: {
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: {
                local: {
                  connected: false,
                  assignedPartitions: 2,
                  messagesPerSecond: 2,
                  bytesPerSecond: 25,
                  decodedMessagesPerSecond: 1,
                  decodeFailuresPerSecond: 1,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 2_000,
                  lastCommitAt: 2_000,
                  consumerLagMessages: null,
                  consumerLagMs: null,
                  lagSampledAt: null,
                  highWatermarkOffset: null,
                  committedOffset: "2",
                  lastError: "lost",
                },
              },
            },
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records Kafka assignments and lag samples", () =>
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
      let healthReadCount = 0;
      const refreshingClient: ViewServerRuntimeClient<Topics> = {
        ...runtimeCore.client,
        health: () =>
          Effect.sync(() => {
            healthReadCount += 1;
          }).pipe(Effect.andThen(runtimeCore.client.health())),
      };
      yield* ledger.regionConnected("local", 1_000);
      yield* recordKafkaAssignments(
        ledger,
        refreshingClient,
        "local",
        [ordersSourceTopic],
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        1_000,
      );
      yield* recordKafkaLag(
        ledger,
        refreshingClient,
        "local",
        new Map([
          [ordersSourceTopic, [3n, -1n, 2n]],
          [unknownSourceTopic, [99n]],
        ]),
        2_000,
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(healthReadCount).toBe(2);
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "ready",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: {
          local: {
            connected: true,
            assignedPartitions: 2,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 1_000,
            lastCommitAt: null,
            consumerLagMessages: 5n,
            consumerLagMs: null,
            lagSampledAt: 2_000,
            highWatermarkOffset: null,
            committedOffset: null,
            lastError: null,
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records Kafka health from consumer listener callbacks", () =>
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
      let healthReadCount = 0;
      const refreshingClient: ViewServerRuntimeClient<Topics> = {
        ...runtimeCore.client,
        health: () =>
          Effect.sync(() => {
            healthReadCount += 1;
          }).pipe(Effect.andThen(runtimeCore.client.health())),
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-test",
        groupId: "view-server-listener-test",
      });
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        refreshingClient,
        "local",
        [ordersSourceTopic],
      );

      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
      });
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [4n, 1n]]]));
      yield* Effect.promise<void>(() => new Promise((resolve) => setTimeout(resolve, 0)));
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(healthReadCount).toBe(3);
      expect(health.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 2,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        decodedMessagesPerSecond: 0,
        decodeFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: expect.any(Number),
        lastCommitAt: null,
        consumerLagMessages: 5n,
        consumerLagMs: null,
        lagSampledAt: expect.any(Number),
        highWatermarkOffset: null,
        committedOffset: null,
        lastError: null,
      });

      yield* listenerRegistration.close;
      yield* Effect.promise(() => Promise.resolve(consumer.close(true))).pipe(Effect.ignore);
      yield* runtimeCore.close;
    }),
  );

  it.effect("reports Kafka overlay ready and starting runtime statuses", () =>
    Effect.gen(function* () {
      const readyRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const readyLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions: {
          local: regions.local,
        },
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* readyLedger.regionConnected("local", 1_000);
      yield* readyLedger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const readyHealth = readyLedger.healthOverlay(yield* readyRuntimeCore.client.health(), 1_000);

      const startingRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const startingLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local", "cold"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* startingLedger.regionConnected("local", 2_000);
      yield* startingLedger.topicConnected(ordersSourceTopic, "local", 1, 2_000);
      const startingHealth = startingLedger.healthOverlay(
        yield* startingRuntimeCore.client.health(),
        2_000,
      );

      expect({
        ready: {
          status: readyHealth.status,
          kafka: readyHealth.kafka,
        },
        starting: {
          status: startingHealth.status,
          kafka: startingHealth.kafka,
        },
      }).toStrictEqual({
        ready: {
          status: "ready",
          kafka: {
            regions: {
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 1_000,
                lastError: null,
              },
            },
            topics: {
              [ordersSourceTopic]: {
                status: "ready",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: {
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: 1_000,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    consumerLagMs: null,
                    lagSampledAt: null,
                    highWatermarkOffset: null,
                    committedOffset: null,
                    lastError: null,
                  },
                },
              },
            },
          },
        },
        starting: {
          status: "starting",
          kafka: {
            regions: {
              cold: {
                status: "starting",
                brokers: regions.cold,
                lastConnectedAt: null,
                lastError: null,
              },
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 2_000,
                lastError: null,
              },
            },
            topics: {
              [ordersSourceTopic]: {
                status: "starting",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: {
                  cold: {
                    connected: false,
                    assignedPartitions: 0,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    consumerLagMs: null,
                    lagSampledAt: null,
                    highWatermarkOffset: null,
                    committedOffset: null,
                    lastError: null,
                  },
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: 2_000,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    consumerLagMs: null,
                    lagSampledAt: null,
                    highWatermarkOffset: null,
                    committedOffset: null,
                    lastError: null,
                  },
                },
              },
            },
          },
        },
      });

      yield* readyRuntimeCore.close;
      yield* startingRuntimeCore.close;
    }),
  );

  it.effect("resets idle Kafka per-second counters on health reads", () =>
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
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });

      const activeHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      const idleHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(activeHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 1,
        bytesPerSecond: 10,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        consumerLagMs: null,
        lagSampledAt: null,
        highWatermarkOffset: null,
        committedOffset: "1",
        lastError: null,
      });
      expect(idleHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        decodedMessagesPerSecond: 0,
        decodeFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        consumerLagMs: null,
        lagSampledAt: null,
        highWatermarkOffset: null,
        committedOffset: "1",
        lastError: null,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("recovers degraded topic status after successful Kafka decoding resumes", () =>
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
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "2",
        nowMillis: 1_000,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(health.status).toBe("ready");
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "ready",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: {
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 2,
            bytesPerSecond: 15,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            processingFailuresPerSecond: 0,
            lastMessageAt: 1_000,
            lastCommitAt: 1_000,
            consumerLagMessages: null,
            consumerLagMs: null,
            lagSampledAt: null,
            highWatermarkOffset: null,
            committedOffset: "2",
            lastError: null,
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );

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
        regions: {
          local: {
            status: "disconnected",
            brokers: regions.local,
            lastConnectedAt: null,
            lastError: "Kafka stream failed for region local",
          },
        },
      });

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
          runtimeCore.client,
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
          regions: {
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "Kafka stream failed for region local",
            },
          },
          topics: {
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: {
                local: {
                  connected: false,
                  assignedPartitions: 1,
                  messagesPerSecond: 1,
                  bytesPerSecond: 59,
                  decodedMessagesPerSecond: 1,
                  decodeFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 0,
                  lastCommitAt: 0,
                  consumerLagMessages: null,
                  consumerLagMs: null,
                  lagSampledAt: null,
                  highWatermarkOffset: null,
                  committedOffset: "5",
                  lastError: "Kafka stream failed for region local",
                },
              },
            },
          },
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
      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.client,
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
        streamFailed: Exit.isFailure(exit),
        committedMessages,
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        streamFailed: true,
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
          regions: {
            local: {
              connected: false,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 9,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              consumerLagMs: null,
              lagSampledAt: null,
              highWatermarkOffset: null,
              committedOffset: null,
              lastError: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
            },
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("creates a no-op ingress when no regions own source topics", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const emptyKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-empty-test",
        regions: {
          cold: "localhost:9093",
        },
        topics: {},
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: emptyKafkaOptions.regions,
        topics: {},
      });

      const ingress = yield* makeViewServerKafkaIngress(
        viewServer,
        runtimeCore.client,
        emptyKafkaOptions,
        ledger,
      );
      yield* ingress.close;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(health.status).toBe("ready");
      expect(health.kafka).toStrictEqual({
        regions: {},
        topics: {},
      });

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
        runtimeCore.client,
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
        runtimeCore.client,
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
          runtimeCore.client,
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
        regions: {
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 3,
            bytesPerSecond: 99,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            processingFailuresPerSecond: 1,
            lastMessageAt: 0,
            lastCommitAt: 0,
            consumerLagMessages: null,
            consumerLagMs: null,
            lagSampledAt: null,
            highWatermarkOffset: null,
            committedOffset: "2",
            lastError: "publish failed",
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records nullable Kafka key and value bytes as decode failures", () =>
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
      let healthReadCount = 0;
      const refreshingClient: ViewServerRuntimeClient<Topics> = {
        ...runtimeCore.client,
        health: () =>
          Effect.sync(() => {
            healthReadCount += 1;
          }).pipe(Effect.andThen(runtimeCore.client.health())),
      };

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          refreshingClient,
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

      expect(Exit.isFailure(exit)).toBe(true);
      expect(healthReadCount).toBe(1);
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: {
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 1,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 1,
            processingFailuresPerSecond: 0,
            lastMessageAt: 0,
            lastCommitAt: null,
            consumerLagMessages: null,
            consumerLagMs: null,
            lagSampledAt: null,
            highWatermarkOffset: null,
            committedOffset: null,
            lastError: "Failed to parse Kafka JSON payload",
          },
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

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-commit-failed",
            value: JSON.stringify({
              customerId: "customer-commit-failed",
              price: 50,
            }),
            offset: 5n,
            commitFailure: new Error("commit failed"),
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        commitFailed: Exit.isFailure(exit),
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        commitFailed: true,
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
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: {
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 1_000,
              lastCommitAt: null,
              consumerLagMessages: null,
              consumerLagMs: null,
              lagSampledAt: null,
              highWatermarkOffset: null,
              committedOffset: null,
              lastError: null,
            },
          },
        },
      });

      yield* runtimeCore.close;
    }),
  );
});
