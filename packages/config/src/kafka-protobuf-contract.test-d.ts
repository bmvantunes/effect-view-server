import { describe, expectTypeOf, it } from "@effect/vitest";
import type { Message } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
import { kafka, type KafkaCodecType } from "./index";

declare const generatedOrdersValueSchema: GenMessage<
  Message<"viewserver.test.OrderValue"> & {
    readonly customerId: string;
    readonly status: "open" | "closed" | "cancelled";
    readonly price: number;
    readonly updatedAt: number;
  }
>;

declare const generatedOrdersKeySchema: GenMessage<
  Message<"viewserver.test.OrderKey"> & {
    readonly orderId: string;
  }
>;

describe("Kafka Protobuf generic contracts", () => {
  it("infers real Protobuf-ES v2 generated schemas", () => {
    const keyedTopic = kafka.source({
      topic: "orders-source",
      regions: ["usa", "london"],
      value: kafka.protobuf(generatedOrdersValueSchema),
      key: kafka.protobuf(generatedOrdersKeySchema),
      rowKey: ({ key }) => key.orderId,
      map: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<
          Message<"viewserver.test.OrderKey"> & { readonly orderId: string }
        >();
        expectTypeOf(value).toEqualTypeOf<
          Message<"viewserver.test.OrderValue"> & {
            readonly customerId: string;
            readonly status: "open" | "closed" | "cancelled";
            readonly price: number;
            readonly updatedAt: number;
          }
        >();
        expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
        return {
          id: key.orderId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        };
      },
    });

    expectTypeOf<KafkaCodecType<typeof keyedTopic.key>>().toEqualTypeOf<
      Message<"viewserver.test.OrderKey"> & {
        readonly orderId: string;
      }
    >();

    kafka.source({
      topic: "orders-source",
      regions: ["usa", "london"],
      value: kafka.protobuf(generatedOrdersValueSchema),
      rowKey: ({ key }) => key,
      map: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<
          Message<"viewserver.test.OrderValue"> & {
            readonly customerId: string;
            readonly status: "open" | "closed" | "cancelled";
            readonly price: number;
            readonly updatedAt: number;
          }
        >();
        expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
        return {
          id: key,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        };
      },
    });
  });
});
