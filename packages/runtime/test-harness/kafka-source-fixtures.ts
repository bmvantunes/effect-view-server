import { type Message as KafkaMessage } from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import { Schema } from "effect";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
});

export const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

export const IncomingTrade = Schema.Struct({
  symbol: Schema.String,
  quantity: Schema.Number,
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

export const TransformedOrder = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigIntFromString,
});

export const IncomingTransformedOrder = Schema.Struct({
  quantity: Schema.BigIntFromString,
});

export type ProducerMessage = {
  readonly topic: string;
  readonly key: string;
  readonly value: string;
};

export type BinaryProducerMessage = {
  readonly topic: string;
  readonly key: Buffer;
  readonly value: Buffer;
};

export const nullRecord = <Value>(entries: Record<string, Value>): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  return Object.assign(record, entries);
};

export const kafkaProcessorMessage = (input: {
  readonly key: string | null;
  readonly offset?: bigint;
  readonly topic: string;
  readonly value?: string | null;
}): KafkaMessage<Buffer | null, Buffer | null | undefined, Buffer, Buffer> => ({
  commit: () => undefined,
  headers: new Map(),
  key: input.key === null ? null : Buffer.from(input.key),
  metadata: {},
  offset: input.offset ?? 0n,
  partition: 0,
  timestamp: 0n,
  toJSON: () => ({
    headers: [],
    key: input.key === null ? null : Buffer.from(input.key),
    metadata: {},
    offset: String(input.offset ?? 0n),
    partition: 0,
    timestamp: "0",
    topic: input.topic,
    value:
      input.value === null || input.value === undefined ? input.value : Buffer.from(input.value),
  }),
  topic: input.topic,
  value: input.value === null || input.value === undefined ? input.value : Buffer.from(input.value),
});
