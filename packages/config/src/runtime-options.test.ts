import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Config, Effect } from "effect";
import {
  defineViewServerConfig,
  decodeKafkaCodec,
  kafka,
  type KafkaCodecError,
  type KafkaCodecType,
  type KafkaDecodeError,
} from "./index";
import { runtimeConfig, runtimeEnvironmentConfig } from "./runtime";

import { kafkaRegions, kafkaTestMetadata, textEncoder } from "../test-harness/kafka";
import { ordersKeySchema, ordersValueSchema, tradesValueSchema } from "../test-harness/protobuf";
import { Order, Trade } from "../test-harness/schemas";

import type { CustomKafkaCodecError } from "../test-harness/kafka";
import type {
  OrdersKeyMessage,
  OrdersValueMessage,
  TradesValueMessage,
} from "../test-harness/protobuf";

describe("Runtime option contracts", () => {
  it("defines topics and pure runtime option contracts without starting a runtime", () => {
    const kafkaViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa", "london"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            rowKey: ({ key }) => key.orderId,
            map: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
              return {
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region,
                updatedAt: value.updatedAt,
              };
            },
          }),
        },
        trades: {
          schema: Trade,
          key: "id",
          kafkaSource: kafka.source({
            topic: "trades-source",
            regions: ["usa"],
            value: kafka.protobuf(tradesValueSchema),
            rowKey: ({ key }) => key,
            map: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              return {
                symbol: value.symbol,
                quantity: value.quantity,
                price: value.price,
                region,
              };
            },
          }),
        },
      },
    });
    const runtimeOptions = kafkaViewServer.defineRuntimeOptions({
      websocketPort: runtimeEnvironmentConfig.websocketPort,
      kafka: {
        consumerGroupId: "view-server-config-test",
        regions: kafkaRegions,
      },
    });

    expect(runtimeOptions.kafka.regions["usa"]).toBe(kafkaRegions.usa);
    expect(kafkaViewServer.topics.orders.key).toBe("id");
    expect(runtimeOptions.websocketPort).toBe(runtimeEnvironmentConfig.websocketPort);
    expect(runtimeOptions.kafka.consumerGroupId).toBe("view-server-config-test");
    expect(Config.isConfig(runtimeConfig.port("VIEW_SERVER_WEBSOCKET_PORT"))).toBe(true);
  });

  it.effect("defines non-JSON Kafka source codecs", () =>
    Effect.gen(function* () {
      const bytesCodec = kafka.bytes();
      const stringCodec = kafka.string();
      const stringKeyCodec = kafka.stringKey();
      const protobufCodec = kafka.protobuf(ordersValueSchema);
      const customCodec = kafka.codec({
        name: "custom-order-value",
        decode: ({ bytes }): Effect.Effect<{ readonly byteLength: number }, never> =>
          Effect.succeed({ byteLength: bytes.byteLength }),
      });
      const customErrorCodec = kafka.codec({
        name: "custom-order-value-with-error",
        decode: (): Effect.Effect<{ readonly id: string }, CustomKafkaCodecError> =>
          Effect.fail({
            _tag: "CustomKafkaCodecError",
            message: "decode failed",
          }),
      });

      expect(bytesCodec.format).toBe("bytes");
      expect(stringCodec.format).toBe("string");
      expect(stringKeyCodec.format).toBe("string");
      expect(protobufCodec.descriptor).toBe(ordersValueSchema);
      expect(customCodec.name).toBe("custom-order-value");
      expect(
        yield* decodeKafkaCodec(bytesCodec, {
          bytes: new Uint8Array([1, 2, 3]),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual(new Uint8Array([1, 2, 3]));
      expect(
        yield* decodeKafkaCodec(stringCodec, {
          bytes: textEncoder.encode("order-value"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-value");
      expect(
        yield* decodeKafkaCodec(stringKeyCodec, {
          bytes: textEncoder.encode("order-key"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-key");
      const protobufError = yield* decodeKafkaCodec(protobufCodec, {
        bytes: new Uint8Array([0]),
        metadata: kafkaTestMetadata("usa"),
      }).pipe(Effect.flip);
      expect(protobufError._tag).toBe("KafkaDecodeError");
      expect(protobufError.message).toBe("Failed to decode Kafka protobuf payload");

      expectTypeOf<KafkaCodecType<typeof bytesCodec>>().toEqualTypeOf<Uint8Array>();
      expectTypeOf<KafkaCodecType<typeof stringCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof stringKeyCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof protobufCodec>>().toEqualTypeOf<OrdersValueMessage>();
      expectTypeOf<KafkaCodecType<typeof customCodec>>().toEqualTypeOf<{
        readonly byteLength: number;
      }>();
      expectTypeOf<KafkaCodecError<typeof protobufCodec>>().toEqualTypeOf<KafkaDecodeError>();
      expectTypeOf<KafkaCodecError<typeof customCodec>>().toEqualTypeOf<never>();
      expectTypeOf<
        KafkaCodecError<typeof customErrorCodec>
      >().toEqualTypeOf<CustomKafkaCodecError>();
    }),
  );
});
