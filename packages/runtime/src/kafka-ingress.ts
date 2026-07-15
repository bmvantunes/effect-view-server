import { Consumer } from "@platformatic/kafka";
import type { ConsumerGroupJoinPayload, GroupAssignment, Offsets } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import {
  kafkaErrorIsMapping,
  type RuntimeRegions,
  type KafkaMessageMetadata,
  type ViewServerTopicConfig,
} from "@effect-view-server/config";
import { decodeKafkaTopicMessage } from "@effect-view-server/config/internal";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  MutableRef,
  Option,
  Queue,
  Ref,
  Scope,
} from "effect";
import {
  recordKafkaAssignments,
  recordKafkaLag,
  type ViewServerKafkaHealthObservation,
} from "./kafka-health-observation";
import {
  publishAndCommitKafkaDecodedBatch,
  recordAndCommitKeylessKafkaMessage,
  type DecodedKafkaBatchMessage,
  type KafkaDeliveryRuntimeClient,
  type KafkaBatchTopic,
  kafkaConsumerMessageHasKey,
  type KafkaConsumerMessage,
  type KeyedKafkaConsumerMessage,
} from "./kafka-delivery-contract";
import {
  acquireKafkaDeliveryResource,
  makeScopedKafkaDelivery,
  type KafkaDelivery,
  type StartKafkaDeliveryWorker,
} from "./kafka-delivery";
import {
  kafkaConsumerCloseError,
  kafkaFailureCause,
  kafkaIngressErrorSourceTopic,
  kafkaIngressFailureCause,
  kafkaMessageDecodeError,
  kafkaMessageMappingError,
  kafkaNonFailureCause,
  kafkaStreamCloseError,
  kafkaStreamError,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  ViewServerKafkaIngressError,
} from "./kafka-ingress-error";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export {
  kafkaConsumerCloseError,
  kafkaConsumerStartError,
  kafkaIngressErrorSourceTopic,
  kafkaMessageCommitError,
  kafkaMessageDecodeError,
  kafkaMessageMappingError,
  kafkaMessageProcessingError,
  kafkaStreamCloseError,
  kafkaStreamError,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  ViewServerKafkaIngressError,
} from "./kafka-ingress-error";

export type ViewServerKafkaIngress = KafkaDelivery;

type KafkaConsumer = Consumer<Buffer, Buffer, Buffer, Buffer>;
type CloseableKafkaConsumer = {
  readonly close: (force?: boolean) => unknown;
};
type CloseableKafkaStream = {
  readonly close: () => unknown;
};
type KafkaConsumerHealthListenerRegistration = {
  readonly close: Effect.Effect<void>;
  readonly processed: Effect.Effect<number>;
  readonly waitForProcessed: (expected: number) => Effect.Effect<void>;
};
export type KafkaStreamQueueEvent =
  | {
      readonly _tag: "Message";
      readonly message: KafkaConsumerMessage;
    }
  | {
      readonly _tag: "Failed";
      readonly cause: Cause.Cause<ViewServerKafkaIngressError>;
      readonly error: ViewServerKafkaIngressError;
    }
  | {
      readonly _tag: "End";
    };
type KafkaStreamTerminalEvent = Extract<KafkaStreamQueueEvent, { readonly _tag: "Failed" | "End" }>;
type KafkaMessageBatchTakeResult =
  | {
      readonly _tag: "Terminal";
      readonly terminal: KafkaStreamTerminalEvent;
    }
  | {
      readonly _tag: "Batch";
      readonly batch: ReadonlyArray<KafkaConsumerMessage>;
      readonly terminal: KafkaStreamTerminalEvent | null;
    };
type KafkaIngressRuntimeClient<Topics extends ViewServerRuntimeTopicDefinitions> =
  KafkaDeliveryRuntimeClient<Topics>;
type KafkaConsumerHealthListenerWaiter = {
  readonly expected: number;
  readonly deferred: Deferred.Deferred<void>;
};
type KafkaConsumerHealthListenerProcessedState = {
  readonly count: number;
  readonly waiters: ReadonlyArray<KafkaConsumerHealthListenerWaiter>;
};
const kafkaMessageBatchSize = 256;
const kafkaMessageQueueCapacity = kafkaMessageBatchSize * 4;
const kafkaMessageBatchFlushInterval = Duration.millis(2);
const kafkaMessageBatchFlushIntervalMillis = Duration.toMillis(kafkaMessageBatchFlushInterval);
const isKafkaBatchTopic = <Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  topic: string,
): topic is KafkaBatchTopic<Topics> => Object.hasOwn(config.topics, topic);
const kafkaBatchTopicDefinition = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends KafkaBatchTopic<Topics>,
>(
  config: ViewServerTopicConfig<Topics>,
  topic: Topic,
): Topics[Topic] => config.topics[topic];
const ignoreKafkaConsumerCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka consumer close failure.",
);
const ignoreKafkaStartedResourceCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka started resource close failure.",
);
const ignoreKafkaAsyncIteratorCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka stream iterator close failure.",
);
const logKafkaHealthListenerDispatchFailure = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<void, never, R> =>
  effect.pipe(
    Effect.asVoid,
    Effect.catchCause((cause) => Effect.logError("Kafka health listener dispatch failed.", cause)),
  );

const kafkaHeaderIsRepeated = (
  value: string | Uint8Array | ReadonlyArray<string | Uint8Array>,
): value is ReadonlyArray<string | Uint8Array> => Array.isArray(value);

export const bootstrapBrokers = (brokers: string): ReadonlyArray<string> =>
  brokers
    .split(",")
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);

export const kafkaHeadersFromMessage = (
  headers: ReadonlyMap<Buffer, Buffer>,
): KafkaMessageMetadata["headers"] => {
  const output: Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>> =
    Object.create(null);
  const textDecoder = new TextDecoder();
  for (const [key, value] of headers) {
    const name = textDecoder.decode(key);
    const existing = output[name];
    if (existing === undefined) {
      output[name] = value;
    } else if (kafkaHeaderIsRepeated(existing)) {
      output[name] = [...existing, value];
    } else {
      output[name] = [existing, value];
    }
  }
  return output;
};

export const sourceTopicsForRegion = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  region: string,
): ReadonlyArray<string> => {
  const topics: Array<string> = [];
  for (const [sourceTopic, topic] of Object.entries(options.topics)) {
    if (
      topic.regions.some((topicRegion: (typeof topic.regions)[number]) => topicRegion === region)
    ) {
      topics.push(sourceTopic);
    }
  }
  return topics;
};

const snapshotKafkaAssignments = (
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
): ReadonlyArray<GroupAssignment> | null | undefined =>
  assignments?.map((assignment) => ({
    topic: assignment.topic,
    partitions: [...assignment.partitions],
  }));

export const recordKafkaStreamError = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  error: ViewServerKafkaIngressError,
  options?: {
    readonly preserveTopicErrors?: boolean;
  },
): Effect.Effect<never, ViewServerKafkaIngressError> =>
  health
    .regionDisconnected(region, error.message, options)
    .pipe(Effect.andThen(Effect.fail(error)));

const recordKafkaStreamCause = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  error: ViewServerKafkaIngressError,
  cause: Cause.Cause<ViewServerKafkaIngressError>,
  options?: {
    readonly preserveTopicErrors?: boolean;
  },
): Effect.Effect<never, ViewServerKafkaIngressError> =>
  health
    .regionDisconnected(region, error.message, options)
    .pipe(Effect.andThen(Effect.failCause(cause)));

export const closeKafkaConsumerAfterStartFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeAfterStartFailure",
)(function* (consumer: CloseableKafkaConsumer) {
  yield* Effect.tryPromise({
    try: () => Promise.resolve(consumer.close(true)),
    catch: kafkaConsumerCloseError,
  }).pipe(ignoreKafkaConsumerCloseFailure);
});

export const closeKafkaConsumerOnStartFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeOnStartFailure",
)(function* <A>(
  consumer: CloseableKafkaConsumer,
  start: Effect.Effect<A, ViewServerKafkaIngressError>,
) {
  return yield* start.pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit) ? closeKafkaConsumerAfterStartFailure(consumer) : Effect.void,
    ),
  );
});

const makeKafkaConsumer = Effect.fn("ViewServerRuntime.kafka.consumer.make")(function* (
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  consume: ResolvedViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>["consume"],
) {
  const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
    autocreateTopics: false,
    bootstrapBrokers: [...bootstrapBrokers(brokers)],
    clientId: `view-server-${region}`,
    groupId: consume.consumerGroupId,
    retries: true,
  });
  const stream = yield* closeKafkaConsumerOnStartFailure(
    consumer,
    Effect.tryPromise({
      try: () =>
        consumer.consume({
          autocommit: false,
          fallbackMode: consume.fallbackMode,
          mode: consume.mode,
          topics: [...topics],
        }),
      catch: mapKafkaConsumerStartError(region),
    }),
  );
  return { consumer, stream };
});

export const closeKafkaConsumer = Effect.fn("ViewServerRuntime.kafka.consumer.close")(
  function* (input: {
    readonly consumer: CloseableKafkaConsumer;
    readonly stream: CloseableKafkaStream;
  }) {
    yield* runAllFinalizers([
      Effect.tryPromise({
        try: () => Promise.resolve(input.stream.close()),
        catch: kafkaStreamCloseError,
      }),
      Effect.tryPromise({
        try: () => Promise.resolve(input.consumer.close(true)),
        catch: kafkaConsumerCloseError,
      }),
    ]);
  },
);

const decodeKafkaMessageForBatch = Effect.fn("ViewServerRuntime.kafka.message.decodeForBatch")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
    const Topic extends ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>["topics"][string],
  >(
    health: ViewServerKafkaHealthObservation<Topics>,
    region: string,
    sourceTopic: string,
    topic: Topic,
    topicRegion: Topic["regions"][number],
    viewServerTopic: KafkaBatchTopic<Topics>,
    topicDefinition: Topics[KafkaBatchTopic<Topics>],
    message: KeyedKafkaConsumerMessage,
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    const keyBytes = message.key;
    const valueBytes = message.value;
    const messageBytes = (valueBytes?.byteLength ?? 0) + keyBytes.byteLength;
    const metadata: KafkaMessageMetadata<typeof topicRegion> = {
      sourceTopic,
      sourceRegion: topicRegion,
      partition: message.partition,
      offset: String(message.offset),
      timestamp: Number(message.timestamp),
      headers: kafkaHeadersFromMessage(message.headers),
    };
    const handleMappingFailure = (failure: unknown, cause: Cause.Cause<unknown>) =>
      health
        .mappingFailed(sourceTopic, region, {
          bytes: messageBytes,
          message: messageFromUnknown(failure),
          nowMillis,
        })
        .pipe(
          Effect.andThen(
            Effect.failCause(
              kafkaIngressFailureCause(
                kafkaMessageMappingError(region, sourceTopic, kafkaFailureCause(failure, cause)),
                cause,
              ),
            ),
          ),
        );
    const handleDecodeFailure = (cause: Cause.Cause<unknown>) => {
      const error = Cause.findErrorOption(cause);
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(kafkaNonFailureCause(cause));
      }
      if (Option.isSome(error)) {
        if (kafkaErrorIsMapping(error.value)) {
          return handleMappingFailure(error.value, cause);
        }
        return health
          .decodeFailed(sourceTopic, region, {
            bytes: messageBytes,
            message: messageFromUnknown(error.value),
            nowMillis,
          })
          .pipe(
            Effect.andThen(
              Effect.failCause(
                kafkaIngressFailureCause(
                  kafkaMessageDecodeError(
                    region,
                    sourceTopic,
                    kafkaFailureCause(error.value, cause),
                  ),
                  cause,
                ),
              ),
            ),
          );
      }
      return health
        .decodeFailed(sourceTopic, region, {
          bytes: messageBytes,
          message: messageFromUnknown(Cause.squash(cause)),
          nowMillis,
        })
        .pipe(
          Effect.andThen(
            Effect.failCause(
              kafkaIngressFailureCause(
                kafkaMessageDecodeError(
                  region,
                  sourceTopic,
                  kafkaFailureCause(Cause.squash(cause), cause),
                ),
                cause,
              ),
            ),
          ),
        );
    };
    if (valueBytes === undefined) {
      return yield* health
        .decodeFailed(sourceTopic, region, {
          bytes: messageBytes,
          message: "Kafka message value bytes are required",
          nowMillis,
        })
        .pipe(
          Effect.andThen(
            Effect.fail(
              new ViewServerKafkaIngressError({
                message: `Failed to decode Kafka message for source topic ${sourceTopic}`,
                cause: "missing-kafka-value",
                region,
                sourceTopic,
              }),
            ),
          ),
        );
    }
    const decoded = yield* decodeKafkaTopicMessage(topic, {
      keyBytes,
      valueBytes,
      region: topicRegion,
      metadata,
      rowKeyField: topicDefinition.key,
      schema: topicDefinition.schema,
      viewServerTopic,
    }).pipe(
      Effect.matchCauseEffect({
        onFailure: handleDecodeFailure,
        onSuccess: (decodedMessage) => Effect.succeed(decodedMessage),
      }),
    );
    let decodedBatchMessage: DecodedKafkaBatchMessage<Topics>;
    if ("row" in decoded) {
      decodedBatchMessage = {
        decoded: {
          row: decoded.row,
          rowKey: decoded.rowKey,
          viewServerTopic,
        },
        message,
        messageBytes,
        nowMillis,
        sourceTopic,
      };
    } else {
      decodedBatchMessage = {
        decoded: {
          rowKey: decoded.rowKey,
          tombstone: true,
          viewServerTopic,
        },
        message,
        messageBytes,
        nowMillis,
        sourceTopic,
      };
    }
    return decodedBatchMessage;
  },
);

export const processKafkaMessageBatch = Effect.fn("ViewServerRuntime.kafka.messageBatch.process")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(
    config: ViewServerTopicConfig<Topics>,
    client: KafkaIngressRuntimeClient<Topics>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthObservation<Topics>,
    region: string,
    batch: ReadonlyArray<KafkaConsumerMessage>,
  ) {
    const decodedMessages: Array<DecodedKafkaBatchMessage<Topics>> = [];
    for (const message of batch) {
      const sourceTopic = message.topic;
      const topic = options.topics[sourceTopic];
      if (topic === undefined) {
        continue;
      }
      const topicRegion = topic.regions.find(
        (candidate: (typeof topic.regions)[number]) => candidate === region,
      );
      if (topicRegion === undefined) {
        continue;
      }
      if (!isKafkaBatchTopic(config, topic.viewServerTopic)) {
        const viewServerTopicMessage = String(topic.viewServerTopic);
        const missingTopicError = new ViewServerKafkaIngressError({
          message: `Kafka source references unknown View Server topic: ${viewServerTopicMessage}`,
          cause: "missing-view-server-topic",
          region,
          sourceTopic,
        });
        const flushExit = yield* Effect.exit(
          publishAndCommitKafkaDecodedBatch(client, health, region, decodedMessages, {
            preserveLastErrorForSourceTopic: sourceTopic,
          }),
        );
        decodedMessages.length = 0;
        return yield* Effect.failCause(
          Exit.isFailure(flushExit)
            ? Cause.combine(Cause.fail(missingTopicError), flushExit.cause)
            : Cause.fail(missingTopicError),
        );
      }
      if (!kafkaConsumerMessageHasKey(message)) {
        yield* publishAndCommitKafkaDecodedBatch(client, health, region, decodedMessages);
        decodedMessages.length = 0;
        yield* recordAndCommitKeylessKafkaMessage(health, region, message);
        continue;
      }
      const decoded = yield* decodeKafkaMessageForBatch(
        health,
        region,
        sourceTopic,
        topic,
        topicRegion,
        topic.viewServerTopic,
        kafkaBatchTopicDefinition(config, topic.viewServerTopic),
        message,
      ).pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(cause);
          }
          const preserveLastErrorForSourceTopic = Option.getOrUndefined(
            Option.flatMap(Cause.findErrorOption(cause), kafkaIngressErrorSourceTopic),
          );
          return Effect.exit(
            publishAndCommitKafkaDecodedBatch(client, health, region, decodedMessages, {
              preserveLastErrorForSourceTopic,
            }),
          ).pipe(
            Effect.andThen((flushExit) =>
              Exit.isFailure(flushExit)
                ? Effect.failCause(Cause.combine(cause, flushExit.cause))
                : Effect.failCause(cause),
            ),
          );
        }),
      );
      decodedMessages.push(decoded);
    }
    yield* publishAndCommitKafkaDecodedBatch(client, health, region, decodedMessages);
  },
);

export const processKafkaMessage = Effect.fn("ViewServerRuntime.kafka.message.process")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerTopicConfig<Topics>,
  client: KafkaIngressRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  message: KafkaConsumerMessage,
) {
  yield* processKafkaMessageBatch(config, client, options, health, region, [message]);
});

const produceKafkaStreamQueueEvents = Effect.fn(
  "ViewServerRuntime.kafka.stream.produceQueueEvents",
)(function* (
  region: string,
  stream: AsyncIterable<KafkaConsumerMessage>,
  queue: Queue.Enqueue<KafkaStreamQueueEvent>,
) {
  const iterator = yield* Effect.acquireRelease(
    Effect.sync(() => stream[Symbol.asyncIterator]()),
    (currentIterator) => closeKafkaAsyncIterator(region, currentIterator),
  );
  while (true) {
    const item = yield* Effect.tryPromise({
      try: () => iterator.next(),
      catch: mapKafkaStreamError(region),
    }).pipe(
      Effect.catch((error: ViewServerKafkaIngressError) =>
        Queue.offer(queue, {
          _tag: "Failed",
          cause: Cause.fail(error),
          error,
        }).pipe(Effect.as(undefined)),
      ),
    );
    if (item === undefined) {
      return;
    }
    if (item.done === true) {
      yield* Queue.offer(queue, {
        _tag: "End",
      });
      return;
    }
    yield* Queue.offer(queue, {
      _tag: "Message",
      message: item.value,
    });
  }
});

const closeKafkaAsyncIterator = Effect.fn("ViewServerRuntime.kafka.stream.closeIterator")(
  function* (region: string, iterator: AsyncIterator<KafkaConsumerMessage>) {
    yield* Effect.tryPromise({
      try: () => Promise.resolve(iterator.return?.()),
      catch: mapKafkaStreamError(region),
    }).pipe(ignoreKafkaAsyncIteratorCloseFailure);
  },
);

export const offerKafkaStreamProducerFailure = Effect.fn(
  "ViewServerRuntime.kafka.stream.offerProducerFailure",
)(function* (
  region: string,
  queue: Queue.Enqueue<KafkaStreamQueueEvent>,
  cause: Cause.Cause<unknown>,
) {
  if (Cause.hasInterruptsOnly(cause)) {
    return yield* Effect.failCause(cause);
  }
  const error = Cause.findErrorOption(cause).pipe(
    Option.filter((value) => value instanceof ViewServerKafkaIngressError),
    Option.getOrElse(() => kafkaStreamError(region, Cause.squash(cause))),
  );
  yield* Queue.offer(queue, {
    _tag: "Failed",
    cause: kafkaIngressFailureCause(error, cause),
    error,
  });
});

const takeKafkaMessageBatch: (
  queue: Queue.Dequeue<KafkaStreamQueueEvent>,
) => Effect.Effect<KafkaMessageBatchTakeResult> = Effect.fn(
  "ViewServerRuntime.kafka.stream.takeBatch",
)(function* (queue: Queue.Dequeue<KafkaStreamQueueEvent>) {
  const first = yield* Queue.take(queue);
  if (first._tag === "End" || first._tag === "Failed") {
    return {
      _tag: "Terminal",
      terminal: first,
    };
  }
  const batch: Array<KafkaConsumerMessage> = [first.message];
  const batchStartedAt = yield* Clock.currentTimeMillis;
  const batchDeadline = batchStartedAt + kafkaMessageBatchFlushIntervalMillis;
  while (batch.length < kafkaMessageBatchSize) {
    const nowMillis = yield* Clock.currentTimeMillis;
    const remainingMillis = batchDeadline - nowMillis;
    if (remainingMillis <= 0) {
      return {
        _tag: "Batch",
        batch,
        terminal: null,
      };
    }
    const next = yield* Queue.take(queue).pipe(
      Effect.timeoutOption(Duration.millis(remainingMillis)),
    );
    if (Option.isNone(next)) {
      return {
        _tag: "Batch",
        batch,
        terminal: null,
      };
    }
    const event = next.value;
    if (event._tag === "End" || event._tag === "Failed") {
      return {
        _tag: "Batch",
        batch,
        terminal: event,
      };
    }
    batch.push(event.message);
  }
  return {
    _tag: "Batch",
    batch,
    terminal: null,
  };
});

export const runKafkaMessageStream = Effect.fn("ViewServerRuntime.kafka.stream.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerTopicConfig<Topics>,
  client: KafkaIngressRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  stream: AsyncIterable<KafkaConsumerMessage>,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const queue = yield* Queue.bounded<KafkaStreamQueueEvent>(kafkaMessageQueueCapacity);
      yield* produceKafkaStreamQueueEvents(region, stream, queue).pipe(
        Effect.catchCause((cause) => offerKafkaStreamProducerFailure(region, queue, cause)),
        Effect.forkScoped({ startImmediately: true }),
      );
      while (true) {
        const next = yield* takeKafkaMessageBatch(queue);
        if (next._tag === "Terminal") {
          const terminal = next.terminal;
          if (terminal._tag === "End") {
            return;
          }
          return yield* Effect.failCause(terminal.cause);
        }
        const processExit = yield* Effect.exit(
          processKafkaMessageBatch(config, client, options, health, region, next.batch),
        );
        const terminal = next.terminal;
        if (Exit.isFailure(processExit)) {
          if (terminal?._tag === "Failed") {
            return yield* Effect.failCause(Cause.combine(processExit.cause, terminal.cause));
          }
          return yield* Effect.failCause(processExit.cause);
        }
        if (terminal?._tag === "End") {
          return;
        }
        if (terminal?._tag === "Failed") {
          return yield* Effect.failCause(terminal.cause);
        }
      }
    }),
  ).pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(kafkaNonFailureCause(cause));
      }
      const error = Cause.findErrorOption(cause);
      if (Option.isSome(error) && error.value instanceof ViewServerKafkaIngressError) {
        return recordKafkaStreamCause(
          health,
          region,
          error.value,
          kafkaIngressFailureCause(error.value, cause),
          {
            preserveTopicErrors: true,
          },
        );
      }
      const streamError = kafkaStreamError(region, Cause.squash(cause));
      return recordKafkaStreamCause(
        health,
        region,
        streamError,
        kafkaIngressFailureCause(streamError, cause),
      );
    }),
  );
});

export const registerKafkaConsumerHealthListeners = Effect.fn(
  "ViewServerRuntime.kafka.consumer.registerHealthListeners",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  consumer: KafkaConsumer,
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  topics: ReadonlyArray<string>,
  scope: Scope.Scope,
) {
  const listenersOpen = MutableRef.make(true);
  const healthEventQueue = yield* Queue.unbounded<Effect.Effect<void>>();
  const processed = yield* Ref.make<KafkaConsumerHealthListenerProcessedState>({
    count: 0,
    waiters: [],
  });
  yield* Scope.addFinalizer(
    scope,
    Effect.sync(() => {
      listenersOpen.current = false;
    }),
  );
  const markProcessed = Effect.fn("ViewServerRuntime.kafka.consumer.healthEvent.markProcessed")(
    function* () {
      const ready = yield* Ref.modify(processed, (state) => {
        const count = state.count + 1;
        const ready = state.waiters.filter((waiter) => count >= waiter.expected);
        const pending = state.waiters.filter((waiter) => count < waiter.expected);
        return [
          ready,
          {
            count,
            waiters: pending,
          },
        ];
      });
      yield* Effect.forEach(ready, (waiter) => Deferred.succeed(waiter.deferred, undefined), {
        discard: true,
      });
    },
  );
  const waitForProcessed = Effect.fn("ViewServerRuntime.kafka.consumer.healthEvent.waitProcessed")(
    function* (expected: number) {
      const deferred = yield* Deferred.make<void>();
      const shouldWait = yield* Ref.modify(processed, (state) => {
        if (state.count >= expected) {
          return [false, state];
        }
        return [
          true,
          {
            count: state.count,
            waiters: [
              ...state.waiters,
              {
                deferred,
                expected,
              },
            ],
          },
        ];
      });
      if (!shouldWait) {
        return;
      }
      yield* Deferred.await(deferred);
    },
  );
  yield* Effect.forever(
    Queue.take(healthEventQueue).pipe(
      Effect.flatMap((effect) =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.failCause(cause),
          ),
          logKafkaHealthListenerDispatchFailure,
          Effect.ensuring(markProcessed()),
        ),
      ),
    ),
  ).pipe(Effect.forkIn(scope, { startImmediately: true }));
  yield* Scope.addFinalizer(
    scope,
    Effect.gen(function* () {
      listenersOpen.current = false;
      yield* Queue.shutdown(healthEventQueue);
    }),
  );
  const enqueueHealthEvent = (effect: Effect.Effect<void>) => {
    if (listenersOpen.current) {
      Queue.offerUnsafe(healthEventQueue, effect);
    }
  };
  const groupJoinListener = (payload: ConsumerGroupJoinPayload) => {
    const assignments = snapshotKafkaAssignments(payload.assignments ?? consumer.assignments);
    enqueueHealthEvent(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaAssignments(health, region, topics, assignments, nowMillis);
      }),
    );
  };
  const lagListener = (lag: Offsets) => {
    enqueueHealthEvent(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaLag(health, region, topics, lag, nowMillis);
      }),
    );
  };
  const groupLeaveListener = () => {
    enqueueHealthEvent(health.regionDisconnected(region, "Kafka consumer left group"));
  };
  const groupRebalanceListener = () => {
    enqueueHealthEvent(
      health.regionDisconnected(region, "Kafka consumer group rebalance in progress", {
        preserveTopicErrors: true,
      }),
    );
  };
  const lagErrorListener = (error: unknown) => {
    enqueueHealthEvent(health.regionDegraded(region, messageFromUnknown(error)));
  };
  consumer.on("consumer:group:join", groupJoinListener);
  consumer.on("consumer:group:leave", groupLeaveListener);
  consumer.on("consumer:group:rebalance", groupRebalanceListener);
  consumer.on("consumer:lag", lagListener);
  consumer.on("consumer:lag:error", lagErrorListener);
  const registration: KafkaConsumerHealthListenerRegistration = {
    close: Effect.gen(function* () {
      yield* Effect.sync(() => {
        listenersOpen.current = false;
        consumer.off("consumer:group:join", groupJoinListener);
        consumer.off("consumer:group:leave", groupLeaveListener);
        consumer.off("consumer:group:rebalance", groupRebalanceListener);
        consumer.off("consumer:lag", lagListener);
        consumer.off("consumer:lag:error", lagErrorListener);
      });
      yield* Queue.shutdown(healthEventQueue);
    }),
    processed: Ref.get(processed).pipe(Effect.map((state) => state.count)),
    waitForProcessed,
  };
  return registration;
});

const startKafkaLagMonitoring = Effect.fn("ViewServerRuntime.kafka.consumer.startLagMonitoring")(
  function* (consumer: KafkaConsumer, topics: ReadonlyArray<string>) {
    yield* Effect.sync(() => {
      consumer.startLagMonitoring({ topics: [...topics] }, 1_000);
    });
  },
);

const stopKafkaLagMonitoring = Effect.fn("ViewServerRuntime.kafka.consumer.stopLagMonitoring")(
  function* (consumer: KafkaConsumer) {
    yield* Effect.sync(() => {
      consumer.stopLagMonitoring();
    });
  },
);

const closeStartedKafkaConsumerResources = (resources: {
  readonly consumer: CloseableKafkaConsumer;
  readonly stream: CloseableKafkaStream;
}) => closeKafkaConsumer(resources).pipe(ignoreKafkaStartedResourceCloseFailure);

const closeKafkaConsumerHealthListenerRegistration = (
  registration: KafkaConsumerHealthListenerRegistration,
) => registration.close;

export const acquireKafkaRegionResources = Effect.fn(
  "ViewServerRuntime.kafka.region.acquireResourceStages",
)(function* <Resources, Registration, E, R, R2, R3, R4, R5>(
  acquireResources: Effect.Effect<Resources, E, R>,
  register: (resources: Resources) => Effect.Effect<Registration, never, R2>,
  start: (resources: Resources) => Effect.Effect<void, never, R3>,
  closeResources: (resources: Resources) => Effect.Effect<void, never, R4>,
  closeRegistration: (registration: Registration) => Effect.Effect<void, never, R5>,
) {
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const resources = yield* restore(acquireResources);
      const registrationExit = yield* Effect.exit(register(resources));
      if (Exit.isFailure(registrationExit)) {
        const cleanupExit = yield* Effect.exit(closeResources(resources));
        return yield* Effect.failCause(
          Exit.isFailure(cleanupExit)
            ? Cause.combine(registrationExit.cause, cleanupExit.cause)
            : registrationExit.cause,
        );
      }
      const registration = registrationExit.value;
      const startExit = yield* Effect.exit(start(resources));
      if (Exit.isFailure(startExit)) {
        const registrationCleanupExit = yield* Effect.exit(closeRegistration(registration));
        const resourceCleanupExit = yield* Effect.exit(closeResources(resources));
        const cleanupExit = Exit.asVoidAll([registrationCleanupExit, resourceCleanupExit]);
        return yield* Effect.failCause(
          Exit.isFailure(cleanupExit)
            ? Cause.combine(startExit.cause, cleanupExit.cause)
            : startExit.cause,
        );
      }
      return { registration, resources };
    }),
  );
});

const acquireKafkaRegionConsumerResources = Effect.fn(
  "ViewServerRuntime.kafka.region.acquireResources",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  consume: ResolvedViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>["consume"],
  workerScope: Scope.Scope,
) {
  const acquired = yield* acquireKafkaRegionResources(
    makeKafkaConsumer(region, brokers, topics, consume),
    (resources) =>
      registerKafkaConsumerHealthListeners(resources.consumer, health, region, topics, workerScope),
    (resources) => startKafkaLagMonitoring(resources.consumer, topics),
    closeStartedKafkaConsumerResources,
    closeKafkaConsumerHealthListenerRegistration,
  );
  return {
    ...acquired.resources,
    listenerRegistration: acquired.registration,
  };
});

const closeKafkaRegionConsumerResources = Effect.fn(
  "ViewServerRuntime.kafka.region.closeResources",
)(function* (resources: {
  readonly consumer: KafkaConsumer;
  readonly stream: CloseableKafkaStream;
  readonly listenerRegistration: KafkaConsumerHealthListenerRegistration;
}) {
  yield* runAllFinalizers([
    stopKafkaLagMonitoring(resources.consumer),
    closeKafkaConsumerHealthListenerRegistration(resources.listenerRegistration),
    closeStartedKafkaConsumerResources(resources),
  ]);
});

const startRegionConsumer = Effect.fn("ViewServerRuntime.kafka.region.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerTopicConfig<Topics>,
  client: KafkaIngressRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthObservation<Topics>,
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  startWorker: StartKafkaDeliveryWorker,
) {
  yield* startWorker(
    Effect.gen(function* () {
      const workerScope = yield* Effect.scope;
      const { consumer, stream } = yield* acquireKafkaDeliveryResource(
        acquireKafkaRegionConsumerResources(
          health,
          region,
          brokers,
          topics,
          options.consume,
          workerScope,
        ),
        closeKafkaRegionConsumerResources,
      );
      const nowMillis = yield* Clock.currentTimeMillis;
      yield* health.regionConnected(region, nowMillis);
      yield* recordKafkaAssignments(health, region, topics, consumer.assignments, nowMillis);
      return stream;
    }),
    (stream) => runKafkaMessageStream(config, client, options, health, region, stream),
    health.regionStopped(region),
  );
});

const startKafkaRegionDeliveries = Effect.fn("ViewServerRuntime.kafka.regions.start")(function* <
  E,
  R,
>(
  regions: Iterable<readonly [string, string]>,
  start: (region: string, brokers: string) => Effect.Effect<void, E, R>,
) {
  yield* Effect.forEach(regions, ([region, brokers]) => start(region, brokers), {
    discard: true,
  });
});

export const makeViewServerKafkaIngress: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerTopicConfig<Topics>,
  client: KafkaIngressRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthObservation<Topics>,
) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError> = Effect.fn(
  "ViewServerRuntime.kafka.ingress.make",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerTopicConfig<Topics>,
  client: KafkaIngressRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthObservation<Topics>,
) {
  return yield* makeScopedKafkaDelivery((startWorker) =>
    startKafkaRegionDeliveries(Object.entries(options.regions), (region, brokers) => {
      const topics = sourceTopicsForRegion(options, region);
      if (topics.length === 0) {
        return Effect.void;
      }
      return startRegionConsumer(
        config,
        client,
        options,
        health,
        region,
        brokers,
        topics,
        startWorker,
      );
    }),
  );
});
