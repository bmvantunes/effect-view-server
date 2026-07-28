import { describe, expect, it } from "@effect/vitest";
import { decodeKafkaCodec, type KafkaMessageMetadata } from "effect-view-server/kafka/contract";
import { Effect } from "effect";
import { viewServer } from "./view-server.config";

const textEncoder = new TextEncoder();

describe("Kafka React example topic-owned sources", () => {
  it.effect("decodes, keys, and maps order and trade source messages", () =>
    Effect.gen(function* () {
      const orderSource = viewServer.topics.orders.source;
      const orderOptions = orderSource.options;
      const orderMetadata = {
        sourceTopic: orderOptions.topic,
        sourceRegion: "usa",
        partition: 0,
        offset: 1n,
        timestampNanos: 0n,
        headers: {},
      } satisfies KafkaMessageMetadata<"usa">;
      const orderKey = yield* decodeKafkaCodec(orderOptions.key, {
        bytes: textEncoder.encode("order-kafka-config"),
        metadata: orderMetadata,
      });
      const orderValue = yield* decodeKafkaCodec(orderOptions.value, {
        bytes: textEncoder.encode(
          '{"customerId":"customer-kafka-config","status":"open","price":42,"updatedAt":1}',
        ),
        metadata: orderMetadata,
      });
      const orderLocalRowKey = orderOptions.localRowKey({
        key: orderKey,
        region: "usa",
        metadata: orderMetadata,
      });
      const orderRow = orderOptions.map({
        key: orderKey,
        value: orderValue,
        region: "usa",
        localRowKey: orderLocalRowKey,
        metadata: orderMetadata,
      });

      const tradeSource = viewServer.topics.trades.source;
      const tradeOptions = tradeSource.options;
      const tradeMetadata = {
        sourceTopic: tradeOptions.topic,
        sourceRegion: "london",
        partition: 1,
        offset: 2n,
        timestampNanos: 1n,
        headers: {},
      } satisfies KafkaMessageMetadata<"london">;
      const tradeKey = yield* decodeKafkaCodec(tradeOptions.key, {
        bytes: textEncoder.encode("trade-kafka-config"),
        metadata: tradeMetadata,
      });
      const tradeValue = yield* decodeKafkaCodec(tradeOptions.value, {
        bytes: textEncoder.encode('{"symbol":"EFFECT","side":"buy","quantity":7,"updatedAt":2}'),
        metadata: tradeMetadata,
      });
      const tradeLocalRowKey = tradeOptions.localRowKey({
        key: tradeKey,
        region: "london",
        metadata: tradeMetadata,
      });
      const tradeRow = tradeOptions.map({
        key: tradeKey,
        value: tradeValue,
        region: "london",
        localRowKey: tradeLocalRowKey,
        metadata: tradeMetadata,
      });

      expect({
        orders: {
          source: {
            topic: orderOptions.topic,
            regions: orderOptions.regions,
            keyFormat: orderOptions.key.format,
            valueFormat: orderOptions.value.format,
          },
          row: { id: `usa:${orderLocalRowKey}`, ...orderRow },
        },
        trades: {
          source: {
            topic: tradeOptions.topic,
            regions: tradeOptions.regions,
            keyFormat: tradeOptions.key.format,
            valueFormat: tradeOptions.value.format,
          },
          row: { id: `london:${tradeLocalRowKey}`, ...tradeRow },
        },
      }).toStrictEqual({
        orders: {
          source: {
            topic: "view-server-example-orders-usa",
            regions: ["usa"],
            keyFormat: "string",
            valueFormat: "json",
          },
          row: {
            id: "usa:order-kafka-config",
            customerId: "customer-kafka-config",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
          },
        },
        trades: {
          source: {
            topic: "view-server-example-trades-london",
            regions: ["london"],
            keyFormat: "string",
            valueFormat: "json",
          },
          row: {
            id: "london:trade-kafka-config",
            symbol: "EFFECT",
            side: "buy",
            quantity: 7,
            region: "london",
            updatedAt: 2,
          },
        },
      });
    }),
  );
});
