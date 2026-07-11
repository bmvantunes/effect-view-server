import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "./index";
import { grpcSourceMarkers } from "./internal";
import { makeKafkaSourceTopicsForConfig } from "./internal";

import { grpcOrdersMaterializedTopic } from "../test-harness/grpc";
import { kafkaRegions, ordersKeyKafkaCodec, ordersValueKafkaCodec } from "../test-harness/kafka";
import { Order } from "../test-harness/schemas";

describe("Topic source ownership", () => {
  it("rejects malformed topic-owned Kafka sources at config derivation time", () => {
    // @ts-expect-error malformed Kafka sources are rejected for typed callers and still guarded at runtime.
    const malformedViewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: {},
        },
      },
    });

    expect(() => makeKafkaSourceTopicsForConfig(malformedViewServer)).toThrow(
      "View Server topic orders has an invalid Kafka source.",
    );
  });

  it("ignores inherited Kafka source topic entries during config derivation", () => {
    const viewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
        },
      },
    });
    Object.setPrototypeOf(viewServer.topics, {
      ghost: {
        schema: Order,
        key: "id",
        kafkaSource: kafka.source({
          topic: "ghost-source",
          regions: ["usa"],
          value: ordersValueKafkaCodec,
          key: ordersKeyKafkaCodec,
          rowKey: ({ key }) => key.orderId,
          map: ({ value, region }) => ({
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          }),
        }),
      },
    });

    expect(makeKafkaSourceTopicsForConfig(viewServer)).toStrictEqual([]);
  });

  it("ignores non-object topic entries during config derivation", () => {
    const viewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
        },
      },
    });
    Object.defineProperty(viewServer.topics, "ghost", {
      configurable: true,
      enumerable: true,
      value: undefined,
    });

    expect(makeKafkaSourceTopicsForConfig(viewServer)).toStrictEqual([]);
  });

  it("ignores inherited Kafka source properties during config derivation", () => {
    const viewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
        },
      },
    });
    Object.setPrototypeOf(viewServer.topics.orders, {
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: ordersValueKafkaCodec,
        key: ordersKeyKafkaCodec,
        rowKey: ({ key }) => key.orderId,
        map: ({ value, region }) => ({
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    });

    expect(makeKafkaSourceTopicsForConfig(viewServer)).toStrictEqual([]);
  });

  it("rejects topics with multiple source owners at config definition time", () => {
    const inheritedKafkaSource = kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      value: ordersValueKafkaCodec,
      rowKey: ({ key }) => key,
      map: ({ value, region }) => ({
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region,
        updatedAt: value.updatedAt,
      }),
    });
    const topicWithInheritedGrpcSource: {
      readonly schema: typeof Order;
      readonly key: "id";
      readonly kafkaSource: typeof inheritedKafkaSource;
    } = Object.assign(
      Object.create({
        grpcSource: grpcOrdersMaterializedTopic.grpcSource,
      }),
      {
        schema: Order,
        key: "id",
        kafkaSource: inheritedKafkaSource,
      },
    );

    expect(() =>
      defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: topicWithInheritedGrpcSource,
        },
      }),
    ).not.toThrow();
    expect(() =>
      // @ts-expect-error a View Server topic cannot declare more than one source owner.
      defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              rowKey: ({ key }) => key,
              map: ({ value, region }) => ({
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region,
                updatedAt: value.updatedAt,
              }),
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders cannot declare more than one source owner: kafkaSource, grpcSource.",
    );
    expect(() =>
      // @ts-expect-error a View Server topic cannot declare both kafkaSource and grpcSource.
      defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              rowKey: ({ key }) => key,
              map: ({ value, region }) => ({
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region,
                updatedAt: value.updatedAt,
              }),
            }),
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
          },
        },
      }),
    ).toThrow(
      "View Server topic orders cannot declare more than one source owner: kafkaSource, grpcSource.",
    );
    const oldSourceAliasConfig = {
      topics: {
        orders: {
          schema: Order,
          key: "id",
          source: grpcSourceMarkers.materialized(),
        },
      },
    };
    expect(() =>
      // @ts-expect-error generic source ownership is not part of the public topic API.
      defineViewServerConfig(oldSourceAliasConfig),
    ).toThrow("View Server topic orders cannot declare source; use kafkaSource or grpcSource.");
  });
});
