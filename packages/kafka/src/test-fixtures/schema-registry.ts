import { create, toBinary } from "@bufbuild/protobuf";
import type { KafkaMessageMetadata } from "../contract";
import { OrderValueSchema } from "./orders_pb";

export const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

export const schemaRegistryFrame = (schemaId: number, payload: Uint8Array): Uint8Array =>
  Uint8Array.from([
    0,
    Math.floor(schemaId / 0x1_00_00_00) % 0x100,
    Math.floor(schemaId / 0x1_00_00) % 0x100,
    Math.floor(schemaId / 0x1_00) % 0x100,
    schemaId % 0x100,
    0,
    ...payload,
  ]);

export const schemaRegistryPayload = (frame: Uint8Array): Uint8Array => frame.slice(6);

export const schemaRegistryRecordPayloads = (input: {
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
}) =>
  Object.freeze({
    key: input.key === null ? undefined : schemaRegistryPayload(input.key),
    value: input.value === null ? undefined : schemaRegistryPayload(input.value),
  });

export const metadata = (
  region: string,
  offset: bigint,
  headers: KafkaMessageMetadata["headers"] = {},
  partition = 0,
  timestampNanos = offset * 1_000_000n,
): KafkaMessageMetadata => ({
  sourceTopic: "source-orders",
  sourceRegion: region,
  partition,
  offset,
  timestampNanos,
  headers,
});

export const foreverBrokerContract = (
  viewServerTopic: string,
  sourceTopic: string,
  region: string,
  cleanupPolicy: "delete" | "compact" | "compact-and-delete" = "delete",
) => ({
  viewServerTopic,
  sourceTopic,
  region,
  cleanupPolicy,
  retentionPolicy: { _tag: "Forever" as const },
  observedCleanupPolicy: cleanupPolicy,
  observedRetentionMs: -1n,
  resolvedRetention: { _tag: "Forever" as const },
});

export const valueFrame = (schemaId: number, price: number): Uint8Array =>
  schemaRegistryFrame(
    schemaId,
    toBinary(
      OrderValueSchema,
      create(OrderValueSchema, {
        customerId: `customer-${String(price)}`,
        price,
      }),
    ),
  );
