import type { Message } from "@platformatic/kafka";
import type { ViewServerRuntimeCoreInternalClient } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@effect-view-server/effect-utils";
import { Cause, Clock, Effect, Option } from "effect";
import type { ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  kafkaFailureCause,
  kafkaIngressFailureCause,
  kafkaMessageCommitError,
  kafkaMessageProcessingError,
  kafkaNonFailureCause,
  messageFromUnknown,
} from "./kafka-ingress-error";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type KafkaMessageBytes = Buffer | null | undefined;

export type KafkaConsumerMessage = Message<KafkaMessageBytes, KafkaMessageBytes, Buffer, Buffer>;

export type KeyedKafkaConsumerMessage = KafkaConsumerMessage & {
  readonly key: Buffer;
};

export const kafkaConsumerMessageHasKey = (
  message: KafkaConsumerMessage,
): message is KeyedKafkaConsumerMessage => message.key !== null && message.key !== undefined;

export type KafkaBatchTopic<Topics extends ViewServerRuntimeTopicDefinitions> = Extract<
  keyof Topics,
  string
>;

export type DecodedKafkaBatchUpsertMessage<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly decoded: {
    readonly viewServerTopic: KafkaBatchTopic<Topics>;
    readonly row: object;
    readonly rowKey: string;
  };
  readonly message: KafkaConsumerMessage;
  readonly messageBytes: number;
  readonly nowMillis: number;
  readonly sourceTopic: string;
};

export type DecodedKafkaBatchTombstoneMessage<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly decoded: {
    readonly rowKey: string;
    readonly tombstone: true;
    readonly viewServerTopic: KafkaBatchTopic<Topics>;
  };
  readonly message: KafkaConsumerMessage;
  readonly messageBytes: number;
  readonly nowMillis: number;
  readonly sourceTopic: string;
};

export type DecodedKafkaBatchMessage<Topics extends ViewServerRuntimeTopicDefinitions> =
  | DecodedKafkaBatchUpsertMessage<Topics>
  | DecodedKafkaBatchTombstoneMessage<Topics>;

export type KafkaDecodedPublishRun<Topics extends ViewServerRuntimeTopicDefinitions> =
  | {
      readonly _tag: "Upserts";
      readonly messages: ReadonlyArray<DecodedKafkaBatchUpsertMessage<Topics>>;
    }
  | {
      readonly _tag: "Tombstone";
      readonly message: DecodedKafkaBatchTombstoneMessage<Topics>;
    };

export type KafkaStorageUpsertBatchRow<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly message: DecodedKafkaBatchUpsertMessage<Topics>;
  readonly row: object;
  readonly storageKey: string;
};

export type KafkaUpsertPublishRun<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly rows: ReadonlyArray<KafkaStorageUpsertBatchRow<Topics>>;
  readonly sourceTopic: string;
  readonly topic: KafkaBatchTopic<Topics>;
};

export type KafkaDeliveryRuntimeClient<Topics extends ViewServerRuntimeTopicDefinitions> = Pick<
  ViewServerRuntimeCoreInternalClient<Topics>,
  "delete" | "publishManyDecodedRowsWithStorageKeys"
>;

export type KafkaDeliveryHealthRefreshRequest = Effect.Effect<void>;

export type KafkaDecodedCommitOptions = {
  readonly preserveLastErrorForSourceTopic: string | undefined;
};

const ignoreKafkaDeliveryHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka delivery health refresh failure.",
);

const requestKafkaDeliveryHealthRefresh = (
  requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
) => requestHealthRefresh.pipe(ignoreKafkaDeliveryHealthRefreshFailure);

export const kafkaBatchMessageIsTombstone = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  message: DecodedKafkaBatchMessage<Topics>,
): message is DecodedKafkaBatchTombstoneMessage<Topics> => "tombstone" in message.decoded;

export const planKafkaDecodedPublishRuns = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
): ReadonlyArray<KafkaDecodedPublishRun<Topics>> => {
  const runs: Array<
    | {
        readonly _tag: "Upserts";
        readonly messages: Array<DecodedKafkaBatchUpsertMessage<Topics>>;
      }
    | {
        readonly _tag: "Tombstone";
        readonly message: DecodedKafkaBatchTombstoneMessage<Topics>;
      }
  > = [];
  for (const message of messages) {
    if (kafkaBatchMessageIsTombstone(message)) {
      runs.push({
        _tag: "Tombstone",
        message,
      });
    } else {
      const previousRun = runs[runs.length - 1];
      if (previousRun?._tag === "Upserts") {
        previousRun.messages.push(message);
      } else {
        runs.push({
          _tag: "Upserts",
          messages: [message],
        });
      }
    }
  }
  return runs;
};

export const planKafkaUpsertPublishRuns = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  messages: ReadonlyArray<DecodedKafkaBatchUpsertMessage<Topics>>,
): ReadonlyArray<KafkaUpsertPublishRun<Topics>> => {
  const runs: Array<{
    readonly rows: Array<KafkaStorageUpsertBatchRow<Topics>>;
    readonly sourceTopic: string;
    readonly topic: KafkaBatchTopic<Topics>;
  }> = [];
  for (const message of messages) {
    const previousRun = runs[runs.length - 1];
    const row: KafkaStorageUpsertBatchRow<Topics> = {
      message,
      row: message.decoded.row,
      storageKey: message.decoded.rowKey,
    };
    if (
      previousRun?.topic === message.decoded.viewServerTopic &&
      previousRun.sourceTopic === message.sourceTopic
    ) {
      previousRun.rows.push(row);
    } else {
      runs.push({
        rows: [row],
        sourceTopic: message.sourceTopic,
        topic: message.decoded.viewServerTopic,
      });
    }
  }
  return runs;
};

const publishKafkaRowsForMessages = Effect.fn("ViewServerRuntime.kafka.delivery.publishRows")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    sourceTopic: string,
    messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
    publishEffect: Effect.Effect<void, unknown>,
  ) {
    yield* publishEffect.pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(kafkaNonFailureCause(cause));
          }
          const error = Cause.findErrorOption(cause);
          const failure = Option.match(error, {
            onNone: () => Cause.squash(cause),
            onSome: (value) => value,
          });
          const processingError = kafkaMessageProcessingError(
            region,
            sourceTopic,
            kafkaFailureCause(failure, cause),
          );
          return Effect.forEach(
            messages,
            (message) =>
              health.messagePublishFailed(message.sourceTopic, region, {
                bytes: message.messageBytes,
                message: messageFromUnknown(failure),
                nowMillis: message.nowMillis,
              }),
            { discard: true },
          ).pipe(
            Effect.andThen(requestKafkaDeliveryHealthRefresh(requestHealthRefresh)),
            Effect.andThen(Effect.failCause(kafkaIngressFailureCause(processingError, cause))),
          );
        },
        onSuccess: () => Effect.void,
      }),
    );
  },
);

export const commitSkippedKafkaMessage = Effect.fn(
  "ViewServerRuntime.kafka.delivery.skipAndCommit",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  sourceTopic: string,
  message: KafkaConsumerMessage,
  input: {
    readonly nowMillis: number;
  },
) {
  const messageBytes = (message.value?.byteLength ?? 0) + (message.key?.byteLength ?? 0);
  yield* Effect.uninterruptible(
    Effect.tryPromise({
      try: () => Promise.resolve(message.commit()),
      catch: (cause) => kafkaMessageCommitError(region, sourceTopic, cause),
    }).pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          health
            .messageCommitFailed(sourceTopic, region, {
              bytes: messageBytes,
              message: `${error.message}: ${messageFromUnknown(error.cause)}`,
              nowMillis: input.nowMillis,
              recountMessage: false,
            })
            .pipe(Effect.andThen(Effect.fail(error))),
        onSuccess: () =>
          health.messageSkippedCommitted(sourceTopic, region, {
            committedOffset: String(message.offset + 1n),
            nowMillis: input.nowMillis,
          }),
      }),
    ),
  ).pipe(Effect.ensuring(requestKafkaDeliveryHealthRefresh(requestHealthRefresh)));
});

export const recordAndCommitKeylessKafkaMessage = Effect.fn(
  "ViewServerRuntime.kafka.delivery.keylessSkipAndCommit",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  message: KafkaConsumerMessage,
) {
  const nowMillis = yield* Clock.currentTimeMillis;
  const sourceTopic = message.topic;
  const messageBytes = (message.value?.byteLength ?? 0) + (message.key?.byteLength ?? 0);
  yield* health
    .mappingFailed(sourceTopic, region, {
      bytes: messageBytes,
      message: "Kafka source key bytes are required",
      nowMillis,
    })
    .pipe(Effect.andThen(requestKafkaDeliveryHealthRefresh(requestHealthRefresh)));
  yield* commitSkippedKafkaMessage(requestHealthRefresh, health, region, sourceTopic, message, {
    nowMillis,
  });
});

const publishKafkaDecodedTombstone = Effect.fn("ViewServerRuntime.kafka.delivery.publishTombstone")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    client: KafkaDeliveryRuntimeClient<Topics>,
    requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    message: DecodedKafkaBatchTombstoneMessage<Topics>,
  ) {
    yield* publishKafkaRowsForMessages(
      requestHealthRefresh,
      health,
      region,
      message.sourceTopic,
      [message],
      client.delete(message.decoded.viewServerTopic, message.decoded.rowKey),
    );
  },
);

const publishKafkaDecodedUpsertRun = Effect.fn("ViewServerRuntime.kafka.delivery.publishUpserts")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    client: KafkaDeliveryRuntimeClient<Topics>,
    requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    run: KafkaUpsertPublishRun<Topics>,
  ) {
    yield* publishKafkaRowsForMessages(
      requestHealthRefresh,
      health,
      region,
      run.sourceTopic,
      run.rows.map((row) => row.message),
      client.publishManyDecodedRowsWithStorageKeys(
        run.topic,
        run.rows.map((row) => ({
          row: row.row,
          storageKey: row.storageKey,
        })),
      ),
    );
  },
);

export const commitKafkaDecodedBatch = Effect.fn("ViewServerRuntime.kafka.delivery.commit")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
    options?: KafkaDecodedCommitOptions,
  ) {
    yield* Effect.gen(function* () {
      for (const message of messages) {
        yield* Effect.uninterruptible(
          Effect.tryPromise({
            try: () => Promise.resolve(message.message.commit()),
            catch: (cause) => kafkaMessageCommitError(region, message.sourceTopic, cause),
          }).pipe(
            Effect.matchEffect({
              onFailure: (error) =>
                health
                  .messageCommitFailed(message.sourceTopic, region, {
                    bytes: message.messageBytes,
                    message: `${error.message}: ${messageFromUnknown(error.cause)}`,
                    nowMillis: message.nowMillis,
                  })
                  .pipe(Effect.andThen(Effect.fail(error))),
              onSuccess: () =>
                health.messageDecoded(message.sourceTopic, region, {
                  bytes: message.messageBytes,
                  committedOffset: String(message.message.offset + 1n),
                  nowMillis: message.nowMillis,
                  ...(options?.preserveLastErrorForSourceTopic === message.sourceTopic
                    ? { preserveLastError: true }
                    : {}),
                }),
            }),
          ),
        );
      }
    }).pipe(Effect.ensuring(requestKafkaDeliveryHealthRefresh(requestHealthRefresh)));
  },
);

export const publishAndCommitKafkaDecodedBatch = Effect.fn(
  "ViewServerRuntime.kafka.delivery.publishAndCommit",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  client: KafkaDeliveryRuntimeClient<Topics>,
  requestHealthRefresh: KafkaDeliveryHealthRefreshRequest,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
  options?: KafkaDecodedCommitOptions,
) {
  const runs = planKafkaDecodedPublishRuns(messages);

  for (const run of runs) {
    if (run._tag === "Upserts") {
      const upsertRuns = planKafkaUpsertPublishRuns(run.messages);
      for (const upsertRun of upsertRuns) {
        yield* publishKafkaDecodedUpsertRun(
          client,
          requestHealthRefresh,
          health,
          region,
          upsertRun,
        );
        yield* commitKafkaDecodedBatch(
          requestHealthRefresh,
          health,
          region,
          upsertRun.rows.map((row) => row.message),
          options,
        );
      }
    } else {
      yield* publishKafkaDecodedTombstone(
        client,
        requestHealthRefresh,
        health,
        region,
        run.message,
      );
      yield* commitKafkaDecodedBatch(requestHealthRefresh, health, region, [run.message], options);
    }
  }
});
