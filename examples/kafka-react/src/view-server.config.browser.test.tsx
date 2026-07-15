import { describe, expect, it } from "@effect/vitest";
import { decodeKafkaCodec, type KafkaMessageMetadata } from "effect-view-server/config";
import { Effect } from "effect";
import { viewServer } from "./view-server.config";

const textEncoder = new TextEncoder();

describe("Kafka React example topic-owned sources", () => {
  it.effect("decodes, keys, and maps order and trade source messages", () =>
    Effect.gen(function* () {
      const orderSource = viewServer.topics.orders.kafkaSource;
      const orderMetadata = {
        sourceTopic: orderSource.topic,
        sourceRegion: "usa",
        partition: 0,
        offset: "1",
        timestamp: null,
        headers: {},
      } satisfies KafkaMessageMetadata<"usa">;
      const orderKey = yield* decodeKafkaCodec(orderSource.key, {
        bytes: textEncoder.encode("order-kafka-config"),
        metadata: orderMetadata,
      });
      const orderValue = yield* decodeKafkaCodec(orderSource.value, {
        bytes: textEncoder.encode(
          '{"customerId":"customer-kafka-config","status":"open","price":42,"updatedAt":1}',
        ),
        metadata: orderMetadata,
      });
      const orderRowKey = orderSource.rowKey({
        key: orderKey,
        region: "usa",
        metadata: orderMetadata,
      });
      const orderRow = orderSource.map({
        key: orderKey,
        value: orderValue,
        region: "usa",
        rowKey: orderRowKey,
        metadata: orderMetadata,
      });

      const tradeSource = viewServer.topics.trades.kafkaSource;
      const tradeMetadata = {
        sourceTopic: tradeSource.topic,
        sourceRegion: "london",
        partition: 1,
        offset: "2",
        timestamp: 1,
        headers: {},
      } satisfies KafkaMessageMetadata<"london">;
      const tradeKey = yield* decodeKafkaCodec(tradeSource.key, {
        bytes: textEncoder.encode("trade-kafka-config"),
        metadata: tradeMetadata,
      });
      const tradeValue = yield* decodeKafkaCodec(tradeSource.value, {
        bytes: textEncoder.encode('{"symbol":"EFFECT","side":"buy","quantity":7,"updatedAt":2}'),
        metadata: tradeMetadata,
      });
      const tradeRowKey = tradeSource.rowKey({
        key: tradeKey,
        region: "london",
        metadata: tradeMetadata,
      });
      const tradeRow = tradeSource.map({
        key: tradeKey,
        value: tradeValue,
        region: "london",
        rowKey: tradeRowKey,
        metadata: tradeMetadata,
      });

      expect({
        orders: {
          source: {
            topic: orderSource.topic,
            regions: orderSource.regions,
            keyFormat: orderSource.key.format,
            valueFormat: orderSource.value.format,
          },
          row: { id: orderRowKey, ...orderRow },
        },
        trades: {
          source: {
            topic: tradeSource.topic,
            regions: tradeSource.regions,
            keyFormat: tradeSource.key.format,
            valueFormat: tradeSource.value.format,
          },
          row: { id: tradeRowKey, ...tradeRow },
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
            id: "order-kafka-config",
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
            id: "trade-kafka-config",
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
