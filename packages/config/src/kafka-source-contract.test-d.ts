import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  defineViewServerConfig,
  kafka,
  VIEW_SERVER_HEALTH_TOPIC,
  type GrpcMaterializedTopicSource,
  type TopicRouteBy,
  type ViewServerConfig,
  type KafkaMessageMetadata,
  type KafkaTopicSourceMapInput,
  type RuntimeRegions,
  type DefineViewServerConfigInput,
} from "./index";

import { Order, Trade } from "../test-harness/schemas";
import { viewServer } from "../test-harness/live-query";
import {
  grpcOrdersByRegionStatusTopic,
  grpcPositionsByAccountSymbolTopic,
  grpcTestClients,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import {
  kafkaRegions,
  ordersKeyKafkaCodec,
  ordersValueKafkaCodec,
  tradesValueKafkaCodec,
} from "../test-harness/kafka";
import { ordersKeySchema, ordersValueSchema, tradesValueSchema } from "../test-harness/protobuf";

import type {
  OrdersKeyMessage,
  OrdersValueMessage,
  TradesValueMessage,
} from "../test-harness/protobuf";

describe("Kafka source generic contracts", () => {
  it("infers callbacks and rejects unsafe topic-owned source contracts", () => {
    const topicOwnedViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa", "london"],
            value: ordersValueKafkaCodec,
            key: ordersKeyKafkaCodec,
            rowKey: ({ key, region, metadata }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"usa" | "london">>();
              return key.orderId;
            },
            map: (input) => {
              const { key, value, region, rowKey, metadata } = input;
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"usa" | "london">>();
              // @ts-expect-error standalone kafka.source helper inputs do not expose the enclosing topic schema.
              expectTypeOf(input.schema).not.toBeAny();
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
      },
    });

    type TopicOwnedOrdersRowKeyInput = Parameters<
      typeof topicOwnedViewServer.topics.orders.kafkaSource.rowKey
    >[0];

    expectTypeOf<TopicOwnedOrdersRowKeyInput>().toEqualTypeOf<{
      readonly key: OrdersKeyMessage;
      readonly region: "usa" | "london";
      readonly metadata: KafkaMessageMetadata<"usa" | "london">;
    }>();

    // @ts-expect-error rowKey is value-independent so Kafka tombstones can delete by key.
    type _TopicOwnedOrdersRowKeyValue = TopicOwnedOrdersRowKeyInput["value"];

    // @ts-expect-error rowKey must return a concrete string, not any.
    defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          // @ts-expect-error rowKey must return a concrete string, not any.
          kafkaSource: kafka.source({
            topic: "orders-any-row-key-source",
            regions: ["usa"],
            value: ordersValueKafkaCodec,
            rowKey: () => JSON.parse("{}"),
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
    });

    // @ts-expect-error map must return a concrete exact mapped row, not any.
    defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-any-map-source",
            regions: ["usa"],
            value: ordersValueKafkaCodec,
            rowKey: ({ key }) => key,
            map: () => JSON.parse("{}"),
          }),
        },
      },
    });

    const stringKeyViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        trades: {
          schema: Trade,
          key: "id",
          kafkaSource: kafka.source({
            topic: "trades-source",
            regions: ["london"],
            value: tradesValueKafkaCodec,
            rowKey: ({ key, region, metadata }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(region).toEqualTypeOf<"london">();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"london">>();
              return key;
            },
            map: ({ key, value, region, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"london">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
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

    expectTypeOf(stringKeyViewServer.topics.trades.kafkaSource.rowKey).not.toBeAny();

    // @ts-expect-error topic-owned Kafka source regions must exist in config.kafka.
    const invalidRegionViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["paris"],
            value: ordersValueKafkaCodec,
            key: ordersKeyKafkaCodec,
            rowKey: ({ key }) => key.orderId,
            map: ({ key, value, region, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"paris">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
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
      },
    });

    // @ts-expect-error topic-owned Kafka source map return must exactly match topic schema.
    const extraMapperPropertyViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
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
              extra: true,
            }),
          }),
        },
      },
    });

    const broadKafkaRegions: RuntimeRegions = kafkaRegions;

    // @ts-expect-error topic-owned Kafka sources require concrete config.kafka region keys.
    const invalidBroadKafkaRegionsViewServer = defineViewServerConfig({
      kafka: broadKafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
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
        },
      },
    });

    const validKafkaSource = kafka.source({
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
    });

    // @ts-expect-error topic-owned Kafka source maps must not return the configured row key field.
    const sourceMapperReturningTopicKeyViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa"],
            value: ordersValueKafkaCodec,
            key: ordersKeyKafkaCodec,
            rowKey: ({ key }) => key.orderId,
            map: ({ value, region }) => ({
              id: "value-derived-id",
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region,
              updatedAt: value.updatedAt,
            }),
          }),
        },
      },
    });

    // @ts-expect-error topic-owned Kafka source objects must not contain extra keys.
    const extraKafkaSourceKeyViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: {
            ...validKafkaSource,
            extra: true,
          },
        },
      },
    });

    // @ts-expect-error topic-owned leased gRPC source objects must not contain extra keys.
    const extraGrpcLeasedSourceKeyViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: {
          schema: Order,
          key: "id",
          grpcSource: {
            ...grpcOrdersByRegionStatusTopic.grpcSource,
            extra: true,
          },
        },
      },
    });

    // @ts-expect-error topic-owned materialized gRPC source objects must not contain extra keys.
    const extraGrpcMaterializedSourceKeyViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        trades: {
          schema: Trade,
          key: "id",
          grpcSource: {
            ...grpcTradesMaterializedTopic.grpcSource,
            extra: true,
          },
        },
      },
    });

    expectTypeOf(invalidRegionViewServer).not.toBeAny();

    expectTypeOf(extraMapperPropertyViewServer).not.toBeAny();

    expectTypeOf(invalidBroadKafkaRegionsViewServer).not.toBeAny();

    expectTypeOf(sourceMapperReturningTopicKeyViewServer).not.toBeAny();

    expectTypeOf(extraKafkaSourceKeyViewServer).not.toBeAny();

    expectTypeOf(extraGrpcLeasedSourceKeyViewServer).not.toBeAny();

    expectTypeOf(extraGrpcMaterializedSourceKeyViewServer).not.toBeAny();
  });

  it("infers JSON and custom Kafka source codecs without weakening mapping exactness", () => {
    const jsonTopic = kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      value: kafka.json(() => Schema.toCodecJson(Order)),
      rowKey: ({ key }) => key,
      map: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<typeof Order.Type>();
        expectTypeOf(region).toEqualTypeOf<"usa">();
        return value;
      },
    });

    const customTopic = kafka.source({
      topic: "trades-source",
      regions: ["london"],
      value: kafka.codec({
        name: "trade-json-lines",
        decode: (): Effect.Effect<
          {
            readonly tradeId: string;
            readonly symbol: string;
            readonly quantity: number;
            readonly price: number;
          },
          never
        > =>
          Effect.succeed({
            tradeId: "trade-1",
            symbol: "AAPL",
            quantity: 10,
            price: 42,
          }),
      }),
      rowKey: ({ key }) => key,
      map: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<{
          readonly tradeId: string;
          readonly symbol: string;
          readonly quantity: number;
          readonly price: number;
        }>();
        expectTypeOf(region).toEqualTypeOf<"london">();
        return {
          id: value.tradeId,
          symbol: value.symbol,
          quantity: value.quantity,
          price: value.price,
          region,
        };
      },
    });

    expectTypeOf(jsonTopic.value.format).toEqualTypeOf<"json">();
    expectTypeOf(customTopic.value.format).toEqualTypeOf<"custom">();
  });

  it("types topic-owned sources across two Kafka regions and two gRPC clients", () => {
    type DirectInvalidKeyConfig = ViewServerConfig<{
      readonly orders: {
        readonly schema: typeof Order;
        readonly key: "missing";
      };
    }>;

    type DirectInvalidConflictConfig = ViewServerConfig<{
      readonly orders: {
        readonly schema: typeof Order;
        readonly key: "id";
        readonly kafkaSource: object;
        readonly grpcSource: GrpcMaterializedTopicSource;
      };
    }>;

    expectTypeOf<DirectInvalidKeyConfig>().toEqualTypeOf<never>();

    expectTypeOf<DirectInvalidConflictConfig>().toEqualTypeOf<never>();

    const multiSourceViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-usa-source",
            regions: ["usa"],
            value: ordersValueKafkaCodec,
            key: ordersKeyKafkaCodec,
            rowKey: ({ key, region, metadata }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"usa">>();
              return key.orderId;
            },
            map: ({ key, value, region, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
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
            topic: "trades-london-source",
            regions: ["london"],
            value: tradesValueKafkaCodec,
            key: kafka.stringKey(),
            rowKey: ({ key, region, metadata }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(region).toEqualTypeOf<"london">();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"london">>();
              return key;
            },
            map: ({ key, value, region, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"london">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
              return {
                symbol: value.symbol,
                quantity: value.quantity,
                price: value.price,
                region,
              };
            },
          }),
        },
        positions: grpcPositionsByAccountSymbolTopic,
      },
    });

    expectTypeOf(multiSourceViewServer.topics.orders.kafkaSource.regions).toEqualTypeOf<
      readonly ["usa"]
    >();

    type OrdersRowKeyInput = Parameters<
      typeof multiSourceViewServer.topics.orders.kafkaSource.rowKey
    >[0];

    expectTypeOf<OrdersRowKeyInput>().toEqualTypeOf<{
      readonly key: OrdersKeyMessage;
      readonly region: "usa";
      readonly metadata: KafkaMessageMetadata<"usa">;
    }>();

    // @ts-expect-error rowKey is value-independent so Kafka tombstones can delete by key.
    type _OrdersRowKeyValue = OrdersRowKeyInput["value"];

    expectTypeOf(multiSourceViewServer.topics.trades.kafkaSource.regions).toEqualTypeOf<
      readonly ["london"]
    >();

    type MultiSourceGrpc = NonNullable<(typeof multiSourceViewServer)["grpc"]>;

    expectTypeOf<keyof MultiSourceGrpc["clients"]>().toEqualTypeOf<"orders" | "trades">();

    expectTypeOf<TopicRouteBy<typeof multiSourceViewServer.topics, "positions">>().toEqualTypeOf<
      "accountId" | "symbol"
    >();
  });

  it("accepts valid contracts and rejects invalid contracts", () => {
    const localKafkaRegions = {
      usa: "broker-a:9092",
    };

    const londonKafkaRegions = {
      london: "broker-b:9092",
    };

    const broadKafkaRegions: RuntimeRegions = localKafkaRegions;

    const topicOwnedViewServer = defineViewServerConfig({
      kafka: localKafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            rowKey: ({ key, region, metadata }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              expectTypeOf(metadata).toEqualTypeOf<KafkaMessageMetadata<"usa">>();
              return key.orderId;
            },
            map: ({ key, value, region, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
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
      },
    });

    const validConfigInput: DefineViewServerConfigInput<
      typeof topicOwnedViewServer.topics,
      typeof localKafkaRegions
    > = {
      kafka: localKafkaRegions,
      topics: topicOwnedViewServer.topics,
    };

    type ReservedTopicConfigInput = DefineViewServerConfigInput<
      {
        readonly [VIEW_SERVER_HEALTH_TOPIC]: {
          readonly schema: typeof Order;
          readonly key: "id";
        };
      },
      typeof localKafkaRegions
    >;

    type ConflictingSourceConfigInput = DefineViewServerConfigInput<
      {
        readonly orders: {
          readonly schema: typeof Order;
          readonly key: "id";
          readonly kafkaSource: typeof topicOwnedViewServer.topics.orders.kafkaSource;
          readonly grpcSource: GrpcMaterializedTopicSource;
        };
      },
      typeof localKafkaRegions
    >;

    type InvalidKeyConfigInput = DefineViewServerConfigInput<
      {
        readonly orders: {
          readonly schema: typeof Order;
          readonly key: "missing";
        };
      },
      typeof localKafkaRegions
    >;

    type BroadRegionConfigInput = DefineViewServerConfigInput<
      typeof topicOwnedViewServer.topics,
      RuntimeRegions
    >;

    expectTypeOf(validConfigInput.topics.orders.kafkaSource.rowKey).not.toBeAny();

    expectTypeOf<BroadRegionConfigInput["topics"]>().toEqualTypeOf<never>();

    expectTypeOf<
      ReservedTopicConfigInput["topics"][typeof VIEW_SERVER_HEALTH_TOPIC]
    >().toEqualTypeOf<never>();

    expectTypeOf<ConflictingSourceConfigInput["topics"]["orders"]>().toEqualTypeOf<never>();

    expectTypeOf<InvalidKeyConfigInput["topics"]["orders"]["key"]>().toEqualTypeOf<never>();

    // @ts-expect-error Kafka source keys must either be omitted or be a real Kafka codec.
    const invalidUndefinedKeyKafkaSourceViewServer = defineViewServerConfig({
      kafka: localKafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa"],
            value: kafka.protobuf(ordersValueSchema),
            // @ts-expect-error Kafka source helper keys must either be omitted or be a real Kafka codec.
            key: undefined,
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
    });

    // @ts-expect-error topic-owned Kafka sources require runtime Kafka options with a consumer group.
    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-topic-owned-type-test",
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-topic-owned-explicit-regions-type-test",
        regions: localKafkaRegions,
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-topic-owned-broad-regions-type-test",
        // @ts-expect-error topic-owned Kafka source runtime regions must be exact enough to prove source coverage.
        regions: broadKafkaRegions,
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-topic-owned-wrong-regions-type-test",
        // @ts-expect-error topic-owned Kafka source runtime regions must include the source regions.
        regions: londonKafkaRegions,
      },
    });

    // @ts-expect-error protobuf descriptors cannot be inferred from any
    kafka.protobuf(JSON.parse("{}"));

    // @ts-expect-error JSON codec factories cannot be inferred from any
    kafka.json(JSON.parse("{}"));

    // @ts-expect-error custom Kafka codec values cannot infer any
    kafka.codec({
      name: "unsafe-effect-json-parse",
      decode: () => Effect.succeed(JSON.parse("{}")),
    });

    // @ts-expect-error custom Kafka codec errors cannot infer any
    kafka.codec({
      name: "unsafe-effect-json-parse-error",
      decode: () => Effect.fail(JSON.parse("{}")),
    });

    expectTypeOf(invalidUndefinedKeyKafkaSourceViewServer).not.toBeAny();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "orders",
        "usa" | "london",
        typeof ordersValueKafkaCodec,
        typeof ordersKeyKafkaCodec
      >["key"]
    >().toEqualTypeOf<OrdersKeyMessage>();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "orders",
        "usa" | "london",
        typeof ordersValueKafkaCodec,
        typeof ordersKeyKafkaCodec
      >["value"]
    >().toEqualTypeOf<OrdersValueMessage>();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "orders",
        "usa" | "london",
        typeof ordersValueKafkaCodec,
        typeof ordersKeyKafkaCodec
      >["region"]
    >().toEqualTypeOf<"usa" | "london">();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "orders",
        "usa" | "london",
        typeof ordersValueKafkaCodec,
        typeof ordersKeyKafkaCodec
      >["schema"]
    >().toEqualTypeOf<typeof Order>();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "orders",
        "usa" | "london",
        typeof ordersValueKafkaCodec,
        typeof ordersKeyKafkaCodec
      >["metadata"]["sourceRegion"]
    >().toEqualTypeOf<"usa" | "london">();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "trades",
        "usa",
        typeof tradesValueKafkaCodec,
        undefined
      >["key"]
    >().toEqualTypeOf<string>();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "trades",
        "usa",
        typeof tradesValueKafkaCodec,
        undefined
      >["value"]
    >().toEqualTypeOf<TradesValueMessage>();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "trades",
        "usa",
        typeof tradesValueKafkaCodec,
        undefined
      >["region"]
    >().toEqualTypeOf<"usa">();

    expectTypeOf<
      KafkaTopicSourceMapInput<
        typeof viewServer.topics,
        "trades",
        "usa",
        typeof tradesValueKafkaCodec,
        undefined
      >["schema"]
    >().toEqualTypeOf<typeof Trade>();

    defineViewServerConfig({
      kafka: localKafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            rowKey: ({ key }) => key.orderId,
            map: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
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

    kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      // @ts-expect-error unsupported Kafka value codecs must fail instead of inferring unknown
      value: {},
      rowKey: () => "order-1",
      map: () => ({}),
    });

    kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      // @ts-expect-error $typeName-only objects are message instances, not generated schemas/codecs
      value: { $typeName: "viewserver.test.OrderValue" },
      rowKey: () => "order-1",
      map: () => ({}),
    });

    kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      // @ts-expect-error arbitrary decoder shapes are not accepted as Kafka codecs
      value: { fromBinary: (_bytes: Uint8Array) => ({}) },
      rowKey: () => "order-1",
      map: () => ({}),
    });

    kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      // @ts-expect-error row Effect schemas are not Kafka codecs unless wrapped with kafka.json
      value: Order,
      rowKey: () => "order-1",
      map: () => ({}),
    });

    viewServer.defineRuntimeOptions({
      websocketPort: 8080,
      // @ts-expect-error runtime options reject unknown top-level fields
      extraRuntimeField: true,
    });

    viewServer.defineRuntimeOptions({
      websocketPort: 8080,
    });

    viewServer.defineRuntimeOptions({
      websocketPort: 8080,
      // @ts-expect-error source-free runtime options reject Kafka settings.
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: {
          usa: "broker-a:9092",
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: "latest",
        regions: {
          usa: "broker-a:9092",
        },
        // @ts-expect-error runtime kafka options reject unknown fields
        extraKafkaField: true,
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-type-test",
        regions: localKafkaRegions,
        // @ts-expect-error runtime Kafka options do not accept topic definitions.
        topics: {},
      },
    });

    defineViewServerConfig({
      kafka: localKafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-source",
            regions: ["usa"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            rowKey: ({ key }) => key.orderId,
            map: ({ key, value, metadata, rowKey }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(rowKey).toEqualTypeOf<string>();
              expectTypeOf(metadata.sourceRegion).toEqualTypeOf<"usa">();
              expectTypeOf(metadata.headers).toEqualTypeOf<
                Readonly<Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>>>
              >();
              return {
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region: metadata.sourceRegion,
                updatedAt: value.updatedAt,
              };
            },
          }),
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "fail",
        },
        regions: {
          usa: "broker-a:9092",
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      // @ts-expect-error committed Kafka start config requires committedConsumerGroup.
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          fallback: "earliest",
        },
        regions: {
          usa: "broker-a:9092",
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      // @ts-expect-error runtime Kafka startFrom only accepts earliest, latest, or committed group config.
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: "middle",
        regions: {
          usa: "broker-a:9092",
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      // @ts-expect-error committed Kafka start fallback must be earliest, latest, or fail.
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          fallback: "middle",
        },
        regions: {
          usa: "broker-a:9092",
        },
      },
    });

    topicOwnedViewServer.defineRuntimeOptions({
      websocketPort: 8080,
      kafka: {
        consumerGroupId: "view-server-type-test",
        startFrom: {
          committedConsumerGroup: "view-server-existing-group",
          // @ts-expect-error committed Kafka start config rejects unknown keys.
          committedConsumerGroupId: "view-server-typo",
        },
        regions: {
          usa: "broker-a:9092",
        },
      },
    });
  });
});
