import type { Message } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
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
