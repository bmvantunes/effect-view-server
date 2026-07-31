import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { kafka } from "effect-view-server/kafka/contract";
import { KafkaOrder, useLiveQuery, viewServer } from "./view-server.config";
import { Schema } from "effect";

describe("kafka example type contracts", () => {
  it("preserves selected Kafka-backed order row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "region", "price"],
      where: [{ field: "status", type: "equals", filter: "open" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly region: string;
        readonly price: number;
      }>
    >();
  });

  it("preserves selected Kafka-backed trade row types", () => {
    const result = useLiveQuery("trades", {
      select: ["id", "symbol", "side", "region"],
      where: [{ field: "side", type: "equals", filter: "buy" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly symbol: string;
        readonly side: "buy" | "sell";
        readonly region: string;
      }>
    >();
  });

  it("keeps the Kafka mapping typed", () => {
    expectTypeOf(viewServer.topics.orders.source.options.topic).toEqualTypeOf<string>();
    expectTypeOf(viewServer.topics.orders.source.options.regions).toEqualTypeOf<readonly ["usa"]>();
    expectTypeOf(viewServer.topics.trades.source.options.topic).toEqualTypeOf<string>();
    expectTypeOf(viewServer.topics.trades.source.options.regions).toEqualTypeOf<
      readonly ["london"]
    >();

    const validSourceOptions = {
      cleanupPolicy: "delete" as const,
      retentionPolicy: "Infinity" as const,
      topic: "view-server-valid-option",
      regions: ["usa"] as const,
      value: kafka.json(() => Schema.toCodecJson(KafkaOrder)),
      key: kafka.string(),
      localRowKey: ({ key }: { readonly key: string }) => key,
      map: ({
        value,
        region,
      }: {
        readonly value: typeof KafkaOrder.Type;
        readonly region: "usa";
      }) => ({
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: String(region),
        updatedAt: value.updatedAt,
      }),
      startFrom: "latest" as const,
    };
    const numericTopicSource = {
      ...validSourceOptions,
      topic: 42,
    };
    // @ts-expect-error Kafka Source topics must be strings.
    kafka.source(numericTopicSource);

    const unsupportedSourceOption = {
      ...validSourceOptions,
      unsupported: true,
    };
    // @ts-expect-error Kafka Source options are exact.
    kafka.source(unsupportedSourceOption);
  });
});
