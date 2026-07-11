import type { Message } from "@platformatic/kafka";
import {
  defineViewServerConfig,
  kafka,
  type RuntimeRegions,
  type ViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import {
  makeKafkaSourceTopicsForConfig,
  type KafkaResolvedSourceTopicDefinition,
} from "@effect-view-server/config/internal";
import type { ViewServerRuntimeCoreInternalClient } from "@effect-view-server/runtime-core/internal";
import { Buffer } from "node:buffer";
import { Cause, Effect, Logger, Option, Schema } from "effect";
import type { ViewServerKafkaHealthLedger } from "../src/kafka-health";
import { makeViewServerKafkaHealthLedger as makeViewServerKafkaHealthLedgerBase } from "../src/kafka-health";
import {
  kafkaIngressErrorSourceTopic,
  messageFromUnknown,
  ViewServerKafkaIngressError,
} from "../src/kafka-ingress";
import type { ResolvedViewServerKafkaRuntimeOptions } from "../src/runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "../src/runtime-types";

export class KafkaIngressTestError extends Schema.TaggedErrorClass<KafkaIngressTestError>()(
  "KafkaIngressTestError",
  {
    message: Schema.String,
  },
) {}

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

export const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

export const PrecisePosition = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
});

export const IncomingPrecisePosition = Schema.Struct({
  accountId: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
});

export const regions = {
  cold: "localhost:9093",
  local: " localhost:9092, ,localhost:9094 ",
};
export const ordersSourceTopic = "orders-source";
export const paymentsSourceTopic = "payments-source";
export const unknownSourceTopic = "unknown-source";

export const viewServer = defineViewServerConfig({
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
    precisePositions: {
      schema: PrecisePosition,
      key: "id",
    },
  },
});

export type Topics = typeof viewServer.topics;
export type KafkaMessageBytes = Buffer | null | undefined;
export type KafkaMessage = Message<KafkaMessageBytes, KafkaMessageBytes, Buffer, Buffer>;
type CapturedLog = {
  readonly cause: Cause.Cause<unknown>;
  readonly message: unknown;
};

export const makeCapturedLogs = () => {
  const logs: Array<CapturedLog> = [];
  const logger = Logger.make<unknown, void>((options) => {
    logs.push({
      cause: options.cause,
      message: options.message,
    });
  });
  return { logger, logs };
};

export const nonStringTagCodecError: { readonly _tag: 123; readonly message: "non-string tag" } = {
  _tag: 123,
  message: "non-string tag",
};
export const forgedMappingTagCodecError: {
  readonly _tag: "KafkaMappingError";
  readonly [key: symbol]: symbol;
  readonly message: "forged mapping tag";
} = {
  _tag: "KafkaMappingError",
  [Symbol.for("@effect-view-server/config/KafkaMappingError")]: Symbol.for(
    "@effect-view-server/config/KafkaMappingError",
  ),
  message: "forged mapping tag",
};

const makeCommittedKafkaStart = (consumerGroupId: string) => ({
  consume: {
    consumerGroupId,
    fallbackMode: "earliest" as const,
    mode: "committed" as const,
  },
  startFrom: {
    committedConsumerGroup: consumerGroupId,
  },
});

export const committedKafkaStart = (
  consumerGroupId: string,
): Pick<ResolvedViewServerKafkaRuntimeOptions<Topics>, "consume" | "startFrom"> =>
  makeCommittedKafkaStart(consumerGroupId);

const makeRuntimeTopicRecord = <
  const CurrentTopics extends ViewServerRuntimeTopicDefinitions,
  const CurrentRegions extends RuntimeRegions,
>(
  config: ViewServerConfig<CurrentTopics, CurrentRegions>,
): Record<string, KafkaResolvedSourceTopicDefinition<CurrentTopics, CurrentRegions>> => {
  const topics: Record<
    string,
    KafkaResolvedSourceTopicDefinition<CurrentTopics, CurrentRegions>
  > = Object.create(null);
  for (const topic of makeKafkaSourceTopicsForConfig<CurrentTopics, CurrentRegions>(config)) {
    topics[topic.topic] = topic;
  }
  return topics;
};

export const kafkaOptionsForConfig = <
  const CurrentTopics extends ViewServerRuntimeTopicDefinitions,
  const CurrentRegions extends RuntimeRegions,
>(
  config: ViewServerConfig<CurrentTopics, CurrentRegions>,
  consumerGroupId: string,
  runtimeRegions: Record<string, string> = regions,
): ResolvedViewServerKafkaRuntimeOptions<CurrentTopics, CurrentRegions> => ({
  consumerGroupId,
  ...makeCommittedKafkaStart(consumerGroupId),
  regions: runtimeRegions,
  topics: makeRuntimeTopicRecord(config),
});

export const kafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
  consumerGroupId: "view-server-test",
  ...committedKafkaStart("view-server-test"),
  regions,
  topics: makeRuntimeTopicRecord(viewServer),
};

type KafkaHealthLedgerInput<LedgerTopics extends ViewServerRuntimeTopicDefinitions> = Parameters<
  typeof makeViewServerKafkaHealthLedgerBase<LedgerTopics>
>[0];

export const makeViewServerKafkaHealthLedger = <
  const LedgerTopics extends ViewServerRuntimeTopicDefinitions,
>(
  input: Omit<KafkaHealthLedgerInput<LedgerTopics>, "startFrom"> &
    Partial<Pick<KafkaHealthLedgerInput<LedgerTopics>, "startFrom">>,
): ViewServerKafkaHealthLedger<LedgerTopics> =>
  makeViewServerKafkaHealthLedgerBase<LedgerTopics>({
    startFrom: kafkaOptions.consume,
    ...input,
  });

export const nullRecord = <Value>(entries: Record<string, Value>): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  return Object.assign(record, entries);
};

export const causeReasonSummary = (
  cause: unknown,
): ReadonlyArray<{
  readonly tag: string;
  readonly message: string | null;
}> =>
  Cause.isCause(cause)
    ? cause.reasons.map((reason) => ({
        tag: reason._tag,
        message:
          reason._tag === "Fail"
            ? messageFromUnknown(reason.error)
            : reason._tag === "Die"
              ? messageFromUnknown(reason.defect)
              : null,
      }))
    : [];

export const kafkaIngressErrorSummary = (error: unknown) =>
  error instanceof ViewServerKafkaIngressError
    ? {
        cause: causeReasonSummary(error.cause),
        message: error.message,
        region: error.region,
        sourceTopic: error.sourceTopic,
      }
    : null;

export const kafkaIngressErrorSourceTopicOrNull = (error: unknown): string | null =>
  Option.getOrNull(kafkaIngressErrorSourceTopic(error));

export const kafkaMessage = (input: {
  readonly topic: string;
  readonly key?: string | null;
  readonly value?: string | null;
  readonly headers?: ReadonlyMap<Buffer, Buffer>;
  readonly offset?: bigint;
  readonly onCommit?: () => void;
  readonly commitFailure?: Error;
}): KafkaMessage => {
  const headers = new Map(input.headers ?? []);
  const key =
    input.key === undefined ? undefined : input.key === null ? null : Buffer.from(input.key);
  const value =
    input.value === undefined ? undefined : input.value === null ? null : Buffer.from(input.value);
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

export const runtimeUnavailable: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "publish failed",
};

export const failingClient: ViewServerRuntimeCoreInternalClient<Topics> = {
  delete: () => Effect.fail(runtimeUnavailable),
  health: () => Effect.fail(runtimeUnavailable),
  patch: () => Effect.fail(runtimeUnavailable),
  patchDecodedFields: () => Effect.fail(runtimeUnavailable),
  publish: () => Effect.fail(runtimeUnavailable),
  publishMany: () => Effect.fail(runtimeUnavailable),
  publishManyDecodedRows: () => Effect.fail(runtimeUnavailable),
  publishManyDecodedRowsWithStorageKeys: () => Effect.fail(runtimeUnavailable),
  publishManyWithStorageKeys: () => Effect.fail(runtimeUnavailable),
  reset: () => Effect.fail(runtimeUnavailable),
  snapshot: () => Effect.fail(runtimeUnavailable),
};

export async function* failingKafkaStream(): AsyncIterable<KafkaMessage> {
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

export async function* decodeFailureThenSuccessKafkaStream(
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
