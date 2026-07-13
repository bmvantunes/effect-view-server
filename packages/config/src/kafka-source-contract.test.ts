import { describe, expect, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { Effect, Schema } from "effect";
import { defineViewServerConfig, kafka, kafkaErrorIsMapping } from "./index";
import { decodeKafkaTopicMessage, isKafkaResolvedSourceTopicDefinition } from "./kafka-contract";
import { isKafkaTopicSourceDefinition, makeKafkaSourceTopicsForConfig } from "./internal";

import {
  forceKafkaSourceMapForRuntimeGuard,
  forceKafkaSourceRowKeyForRuntimeGuard,
  kafkaRegions,
  kafkaTestMetadata,
  ordersKeyKafkaCodec,
  ordersValueKafkaCodec,
  textEncoder,
  tradesValueKafkaCodec,
} from "../test-harness/kafka";
import { ordersKeySchema, ordersValueSchema, tradesValueSchema } from "../test-harness/protobuf";
import { Order, Trade } from "../test-harness/schemas";

describe("Kafka source contracts", () => {
  it.effect("infers and decodes topic-owned Kafka sources", () =>
    Effect.gen(function* () {
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
              rowKey: ({ key }) => {
                return key.orderId;
              },
              map: (input) => {
                const { value, region } = input;
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
      const configSourceTopics = makeKafkaSourceTopicsForConfig(topicOwnedViewServer);
      const sourceTopic = configSourceTopics[0]!;
      expect(
        configSourceTopics.map((configSourceTopic) => ({
          regions: configSourceTopic.regions,
          topic: configSourceTopic.topic,
          viewServerTopic: configSourceTopic.viewServerTopic,
        })),
      ).toStrictEqual([
        {
          regions: ["usa", "london"],
          topic: "orders-source",
          viewServerTopic: "orders",
        },
      ]);
      expect(sourceTopic.topic).toBe("orders-source");
      expect(sourceTopic.viewServerTopic).toBe("orders");
      expect(
        yield* decodeKafkaTopicMessage(sourceTopic, {
          keyBytes: toBinary(
            ordersKeySchema,
            create(ordersKeySchema, {
              orderId: "order-owned-1",
            }),
          ),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-owned-1",
              status: "open",
              price: 42,
              updatedAt: 100,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "order-owned-1",
        row: {
          id: "order-owned-1",
          customerId: "customer-owned-1",
          status: "open",
          price: 42,
          region: "usa",
          updatedAt: 100,
        },
      });
      expect(
        yield* decodeKafkaTopicMessage(sourceTopic, {
          keyBytes: toBinary(
            ordersKeySchema,
            create(ordersKeySchema, {
              orderId: "order-owned-tombstone-1",
            }),
          ),
          valueBytes: null,
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "order-owned-tombstone-1",
        tombstone: true,
      });
      // @ts-expect-error topic-owned runtime topics require rowKeyField/schema/viewServerTopic metadata.
      const invalidTopicOwnedDecode = decodeKafkaTopicMessage(sourceTopic, {
        keyBytes: textEncoder.encode("order-owned-missing-metadata"),
        valueBytes: toBinary(
          ordersValueSchema,
          create(ordersValueSchema, {
            customerId: "customer-owned-missing-metadata",
            status: "open",
            price: 1,
            updatedAt: 1,
          }),
        ),
        region: "usa",
        metadata: kafkaTestMetadata("usa"),
      });
      const missingTopicMetadataFailure = yield* Effect.flip(invalidTopicOwnedDecode);
      expect(kafkaErrorIsMapping(missingTopicMetadataFailure)).toBe(true);
      // @ts-expect-error topic-owned runtime topics require a concrete schema.
      const invalidPartialTopicOwnedDecode = decodeKafkaTopicMessage(sourceTopic, {
        keyBytes: textEncoder.encode("order-owned-partial-metadata"),
        valueBytes: toBinary(
          ordersValueSchema,
          create(ordersValueSchema, {
            customerId: "customer-owned-partial-metadata",
            status: "open",
            price: 1,
            updatedAt: 1,
          }),
        ),
        region: "usa",
        metadata: kafkaTestMetadata("usa"),
        rowKeyField: "id",
        schema: undefined,
        viewServerTopic: "orders",
      });
      const partialTopicMetadataFailure = yield* Effect.flip(invalidPartialTopicOwnedDecode);
      expect(kafkaErrorIsMapping(partialTopicMetadataFailure)).toBe(true);

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
              rowKey: ({ key }) => {
                return key;
              },
              map: ({ value, region }) => {
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
      const stringKeySourceTopic = makeKafkaSourceTopicsForConfig(stringKeyViewServer)[0]!;
      expect(
        yield* decodeKafkaTopicMessage(stringKeySourceTopic, {
          keyBytes: textEncoder.encode("trade-owned-1"),
          valueBytes: toBinary(
            tradesValueSchema,
            create(tradesValueSchema, {
              symbol: "AAPL",
              quantity: 10,
              price: 123,
            }),
          ),
          region: "london",
          metadata: kafkaTestMetadata("london"),
          rowKeyField: "id",
          schema: Trade,
          viewServerTopic: "trades",
        }),
      ).toStrictEqual({
        viewServerTopic: "trades",
        rowKey: "trade-owned-1",
        row: {
          id: "trade-owned-1",
          symbol: "AAPL",
          quantity: 10,
          price: 123,
          region: "london",
        },
      });
      expect(
        yield* decodeKafkaTopicMessage(stringKeySourceTopic, {
          keyBytes: textEncoder.encode("trade-owned-tombstone-1"),
          valueBytes: null,
          region: "london",
          metadata: kafkaTestMetadata("london"),
          rowKeyField: "id",
          schema: Trade,
          viewServerTopic: "trades",
        }),
      ).toStrictEqual({
        viewServerTopic: "trades",
        rowKey: "trade-owned-tombstone-1",
        tombstone: true,
      });

      const throwingRowKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-row-key-throws-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              rowKey: () => {
                throw new Error("row key failed");
              },
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
      const throwingRowKeySourceTopic =
        makeKafkaSourceTopicsForConfig(throwingRowKeyViewServer)[0]!;
      const rowKeyFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(throwingRowKeySourceTopic, {
          keyBytes: textEncoder.encode("order-row-key-throws"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-row-key-throws",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      );
      expect(kafkaErrorIsMapping(rowKeyFailure)).toBe(true);

      const throwingMapViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-map-throws-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              rowKey: ({ key }) => key,
              map: () => {
                throw new Error("map failed");
              },
            }),
          },
        },
      });
      const throwingMapSourceTopic = makeKafkaSourceTopicsForConfig(throwingMapViewServer)[0]!;
      const mapFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(throwingMapSourceTopic, {
          keyBytes: textEncoder.encode("order-map-throws"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-map-throws",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      );
      expect({
        isMappingError: kafkaErrorIsMapping(mapFailure),
        message: Reflect.get(Object(mapFailure), "message"),
      }).toStrictEqual({
        isMappingError: true,
        message: "Failed to map Kafka payload",
      });

      const nonStringRowKeySource = kafka.source({
        topic: "orders-non-string-row-key-source",
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
      forceKafkaSourceRowKeyForRuntimeGuard(nonStringRowKeySource, () => 123);
      const nonStringRowKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: nonStringRowKeySource,
          },
        },
      });
      const nonStringRowKeySourceTopic =
        makeKafkaSourceTopicsForConfig(nonStringRowKeyViewServer)[0]!;
      const nonStringRowKeyFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(nonStringRowKeySourceTopic, {
          keyBytes: textEncoder.encode("order-non-string-row-key"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-non-string-row-key",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      );
      expect(kafkaErrorIsMapping(nonStringRowKeyFailure)).toBe(true);

      const schemaInvalidMappedRowSource = kafka.source({
        topic: "orders-schema-invalid-mapped-row-source",
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
      forceKafkaSourceMapForRuntimeGuard(schemaInvalidMappedRowSource, () => ({
        customerId: "customer-schema-invalid-mapped-row",
        price: 1,
        region: "usa",
        updatedAt: 1,
      }));
      const schemaInvalidMappedRowViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: schemaInvalidMappedRowSource,
          },
        },
      });
      const schemaInvalidMappedRowSourceTopic = makeKafkaSourceTopicsForConfig(
        schemaInvalidMappedRowViewServer,
      )[0]!;
      const schemaInvalidMappedRowFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(schemaInvalidMappedRowSourceTopic, {
          keyBytes: textEncoder.encode("order-schema-invalid-mapped-row"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-schema-invalid-mapped-row",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      );
      expect({
        isMappingError: kafkaErrorIsMapping(schemaInvalidMappedRowFailure),
        message: Reflect.get(Object(schemaInvalidMappedRowFailure), "message"),
      }).toStrictEqual({
        isMappingError: true,
        message: "Kafka mapped row failed topic schema",
      });

      const invalidMappedRowSource = kafka.source({
        topic: "orders-invalid-mapped-row-source",
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
      forceKafkaSourceMapForRuntimeGuard(invalidMappedRowSource, () => ({
        id: "order-invalid-mapped-row",
        customerId: "customer-invalid-mapped-row",
        price: 1,
        region: "usa",
        updatedAt: 1,
      }));
      const invalidMappedRowViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: invalidMappedRowSource,
          },
        },
      });
      const invalidMappedRowSourceTopic = makeKafkaSourceTopicsForConfig(
        invalidMappedRowViewServer,
      )[0]!;
      const invalidMappedRowFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(invalidMappedRowSourceTopic, {
          keyBytes: textEncoder.encode("order-invalid-mapped-row"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-invalid-mapped-row",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      );
      expect({
        isMappingError: kafkaErrorIsMapping(invalidMappedRowFailure),
        message: Reflect.get(Object(invalidMappedRowFailure), "message"),
      }).toStrictEqual({
        isMappingError: true,
        message: "Kafka mapped row must not include the configured row key field",
      });

      const KeyTransformId = Schema.StringFromUriComponent;
      const KeyTransformOrder = Schema.Struct({
        id: KeyTransformId,
        customerId: Schema.String,
        status: Schema.Literals(["open", "closed", "cancelled"]),
        price: Schema.Number,
        region: Schema.String,
        updatedAt: Schema.Number,
      });
      const keyTransformViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: KeyTransformOrder,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-key-transform-source",
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
      });
      const keyTransformSourceTopic = makeKafkaSourceTopicsForConfig(keyTransformViewServer)[0]!;
      expect(
        yield* decodeKafkaTopicMessage(keyTransformSourceTopic, {
          keyBytes: textEncoder.encode("order-key-transform"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-key-transform",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: KeyTransformOrder,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "order-key-transform",
        row: {
          id: "order-key-transform",
          customerId: "customer-key-transform",
          status: "open",
          price: 1,
          region: "usa",
          updatedAt: 1,
        },
      });

      const directFieldKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-direct-key-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              key: kafka.codec({
                name: "direct-id-key",
                decode: (): Effect.Effect<{ readonly id: string; readonly other: string }, never> =>
                  Effect.succeed({
                    id: "order-direct-key-1",
                    other: "ignored",
                  }),
              }),
              rowKey: ({ key }) => key.id,
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
      const directFieldSourceTopic = makeKafkaSourceTopicsForConfig(directFieldKeyViewServer)[0]!;
      expect(
        yield* decodeKafkaTopicMessage(directFieldSourceTopic, {
          keyBytes: textEncoder.encode("ignored"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-direct-key-1",
              status: "closed",
              price: 11,
              updatedAt: 12,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "order-direct-key-1",
        row: {
          id: "order-direct-key-1",
          customerId: "customer-direct-key-1",
          status: "closed",
          price: 11,
          region: "usa",
          updatedAt: 12,
        },
      });

      const nonStringConfiguredFieldKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-non-string-field-key-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              key: kafka.codec({
                name: "non-string-field-key",
                decode: (): Effect.Effect<
                  { readonly id: number; readonly fallback: string },
                  never
                > =>
                  Effect.succeed({
                    id: 123,
                    fallback: "must-not-be-used",
                  }),
              }),
              rowKey: ({ key }) => key.fallback,
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
      const nonStringConfiguredFieldSourceTopic = makeKafkaSourceTopicsForConfig(
        nonStringConfiguredFieldKeyViewServer,
      )[0]!;
      expect(
        yield* decodeKafkaTopicMessage(nonStringConfiguredFieldSourceTopic, {
          keyBytes: textEncoder.encode("ignored"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-non-string-configured-field",
              status: "closed",
              price: 11,
              updatedAt: 12,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "must-not-be-used",
        row: {
          id: "must-not-be-used",
          customerId: "customer-non-string-configured-field",
          status: "closed",
          price: 11,
          region: "usa",
          updatedAt: 12,
        },
      });

      const ambiguousKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-ambiguous-key-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              key: kafka.codec({
                name: "ambiguous-key",
                decode: (): Effect.Effect<
                  { readonly left: string; readonly right: string },
                  never
                > =>
                  Effect.succeed({
                    left: "left-key",
                    right: "right-key",
                  }),
              }),
              rowKey: ({ key }) => `${key.left}:${key.right}`,
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
      const ambiguousSourceTopic = makeKafkaSourceTopicsForConfig(ambiguousKeyViewServer)[0]!;
      expect(
        yield* decodeKafkaTopicMessage(ambiguousSourceTopic, {
          keyBytes: textEncoder.encode("ignored"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-ambiguous-key",
              status: "closed",
              price: 11,
              updatedAt: 12,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "left-key:right-key",
        row: {
          id: "left-key:right-key",
          customerId: "customer-ambiguous-key",
          status: "closed",
          price: 11,
          region: "usa",
          updatedAt: 12,
        },
      });

      const numericKeyViewServer = defineViewServerConfig({
        kafka: kafkaRegions,
        topics: {
          orders: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-numeric-key-source",
              regions: ["usa"],
              value: ordersValueKafkaCodec,
              key: kafka.codec({
                name: "numeric-key",
                decode: (): Effect.Effect<number, never> => Effect.succeed(123),
              }),
              rowKey: ({ key }) => `order-${key}`,
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
      const numericKeySourceTopic = makeKafkaSourceTopicsForConfig(numericKeyViewServer)[0]!;
      expect(
        yield* decodeKafkaTopicMessage(numericKeySourceTopic, {
          keyBytes: textEncoder.encode("ignored"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-numeric-key",
              status: "closed",
              price: 11,
              updatedAt: 12,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
          rowKeyField: "id",
          schema: Order,
          viewServerTopic: "orders",
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        rowKey: "order-123",
        row: {
          id: "order-123",
          customerId: "customer-numeric-key",
          status: "closed",
          price: 11,
          region: "usa",
          updatedAt: 12,
        },
      });
    }),
  );

  it("rejects malformed topic-owned Kafka sources during runtime topic derivation", () => {
    // @ts-expect-error Kafka source must use kafka.source.
    const invalidSourceViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: {},
        },
      },
    });

    expect(() => makeKafkaSourceTopicsForConfig(invalidSourceViewServer)).toThrow(
      "View Server topic orders has an invalid Kafka source.",
    );

    const erasedPrimitiveSourceViewServer = {
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
            }),
          }),
        },
      },
    };
    Object.defineProperty(erasedPrimitiveSourceViewServer.topics.orders, "kafkaSource", {
      value: "not-a-source",
    });

    expect(() => makeKafkaSourceTopicsForConfig(erasedPrimitiveSourceViewServer)).toThrow(
      "View Server topic orders has an invalid Kafka source.",
    );
  });

  it("keeps the topic-owned Kafka runtime marker internal and exact", () => {
    const runtimeMarkerViewServer = defineViewServerConfig({
      kafka: kafkaRegions,
      topics: {
        orders: {
          schema: Order,
          key: "id",
          kafkaSource: kafka.source({
            topic: "orders-runtime-marker-source",
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
    });
    const sourceTopic = makeKafkaSourceTopicsForConfig(runtimeMarkerViewServer)[0]!;
    expect({
      nullValue: isKafkaTopicSourceDefinition(null),
      primitiveValue: isKafkaTopicSourceDefinition("orders-source"),
      runtimeNullValue: isKafkaResolvedSourceTopicDefinition(null),
      runtimePrimitiveValue: isKafkaResolvedSourceTopicDefinition("orders-source"),
      sourceTopic: isKafkaResolvedSourceTopicDefinition(sourceTopic),
    }).toStrictEqual({
      nullValue: false,
      primitiveValue: false,
      runtimeNullValue: false,
      runtimePrimitiveValue: false,
      sourceTopic: true,
    });
  });

  it("supports json and custom Kafka source codecs without weakening mapping exactness", () => {
    const jsonTopic = kafka.source({
      topic: "orders-source",
      regions: ["usa"],
      value: kafka.json(() => Schema.toCodecJson(Order)),
      rowKey: ({ key }) => key,
      map: ({ value }) => {
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
      map: ({ value, region }) => {
        return {
          id: value.tradeId,
          symbol: value.symbol,
          quantity: value.quantity,
          price: value.price,
          region,
        };
      },
    });

    expect(jsonTopic.value.format).toBe("json");
    expect(customTopic.value.format).toBe("custom");
  });
});
