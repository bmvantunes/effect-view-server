import type { KafkaMessageMetadata, KafkaProtobufCodec } from "../src/index";
import { kafka } from "../src/index";
import { runtimeConfig } from "../src/runtime";
import { ordersKeySchema, ordersValueSchema, tradesValueSchema } from "./protobuf";

export type CustomKafkaCodecError = {
  readonly _tag: "CustomKafkaCodecError";
  readonly message: string;
};

export const textEncoder = new TextEncoder();

export const kafkaTestMetadata = <const Region extends "usa" | "london">(
  region: Region,
): KafkaMessageMetadata<Region> => ({
  sourceTopic: "orders-source",
  sourceRegion: region,
  partition: 0,
  offset: "1",
  timestamp: null,
  headers: {},
});

export const forceKafkaSourceRowKeyForRuntimeGuard = (source: object, rowKey: () => unknown) => {
  Object.defineProperty(source, "rowKey", {
    configurable: true,
    value: rowKey,
  });
};

export const forceKafkaSourceMapForRuntimeGuard = (source: object, map: () => unknown) => {
  Object.defineProperty(source, "map", {
    configurable: true,
    value: map,
  });
};

export const kafkaRegions = {
  usa: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_USA_BOOTSTRAP_SERVERS"),
  london: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"),
};

export const ordersValueKafkaCodec: KafkaProtobufCodec<typeof ordersValueSchema> =
  kafka.protobuf(ordersValueSchema);
export const ordersKeyKafkaCodec: KafkaProtobufCodec<typeof ordersKeySchema> =
  kafka.protobuf(ordersKeySchema);
export const tradesValueKafkaCodec: KafkaProtobufCodec<typeof tradesValueSchema> =
  kafka.protobuf(tradesValueSchema);
