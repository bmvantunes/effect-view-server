import type { Client } from "@connectrpc/connect";
import { describe, expect, it } from "@effect/vitest";
import { decodeKafkaCodec, type KafkaMessageMetadata } from "effect-view-server/config";
import { Effect, Stream } from "effect";
import { combinedService } from "./grpc-descriptors";
import { Order, Strategy, viewServer } from "./view-server.config";

const client = {
  streamOrders: async function* streamOrders() {},
  streamStrategies: async function* streamStrategies() {},
} satisfies Client<typeof combinedService>;

const textEncoder = new TextEncoder();

describe("combined-sources React example topic-owned sources", () => {
  it.effect("maps leased, materialized, and Kafka source values", () =>
    Effect.gen(function* () {
      const session = {
        id: null,
        forwardedHeaders: {},
        systemHeaders: {},
      };

      const orderSource = viewServer.topics.orders.grpcSource;
      const orderRoute = {
        strategyId: "strategy-alpha",
        region: "usa",
      };
      const orderRequest = orderSource.request(orderRoute);
      const orderValues = yield* orderSource
        .acquire({
          client,
          request: orderRequest,
          route: orderRoute,
          session,
        })
        .pipe(Stream.take(1), Stream.runCollect);
      const orderRows = Array.from(orderValues, (value) =>
        orderSource.map({
          value,
          route: orderRoute,
          schema: Order,
        }),
      );

      const strategySource = viewServer.topics.strategies.grpcSource;
      const strategyRequest = strategySource.request();
      const strategyValues = yield* strategySource
        .acquire({
          client,
          request: strategyRequest,
          route: undefined,
          session,
        })
        .pipe(Stream.take(1), Stream.runCollect);
      const strategyRows = Array.from(strategyValues, (value) =>
        strategySource.map({
          value,
          route: undefined,
          schema: Strategy,
        }),
      );

      const tradeSource = viewServer.topics.trades.kafkaSource;
      const tradeMetadata = {
        sourceTopic: tradeSource.topic,
        sourceRegion: "london",
        partition: 0,
        offset: "1",
        timestamp: null,
        headers: {},
      } satisfies KafkaMessageMetadata<"london">;
      const tradeKey = yield* decodeKafkaCodec(tradeSource.key, {
        bytes: textEncoder.encode("trade-combined-config"),
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
        descriptors: {
          serviceTypeName: combinedService.typeName,
          orders: {
            name: combinedService.method.streamOrders.name,
            localName: combinedService.method.streamOrders.localName,
            methodKind: combinedService.method.streamOrders.methodKind,
            input: combinedService.method.streamOrders.input.typeName,
            output: combinedService.method.streamOrders.output.typeName,
          },
          strategies: {
            name: combinedService.method.streamStrategies.name,
            localName: combinedService.method.streamStrategies.localName,
            methodKind: combinedService.method.streamStrategies.methodKind,
            input: combinedService.method.streamStrategies.input.typeName,
            output: combinedService.method.streamStrategies.output.typeName,
          },
        },
        sources: {
          orders: {
            lifecycle: orderSource.lifecycle,
            routeBy: orderSource.routeBy,
            client: orderSource.client,
            method: orderSource.method,
            request: orderRequest,
          },
          strategies: {
            lifecycle: strategySource.lifecycle,
            client: strategySource.client,
            method: strategySource.method,
            request: strategyRequest,
          },
          trades: {
            topic: tradeSource.topic,
            regions: tradeSource.regions,
            keyFormat: tradeSource.key.format,
            valueFormat: tradeSource.value.format,
          },
        },
        rows: {
          orders: orderRows,
          strategies: strategyRows,
          trades: [{ id: tradeRowKey, ...tradeRow }],
        },
      }).toStrictEqual({
        descriptors: {
          serviceTypeName: "viewserver.combined.CombinedService",
          orders: {
            name: "StreamOrders",
            localName: "streamOrders",
            methodKind: "server_streaming",
            input: "viewserver.combined.OrderRoute",
            output: "viewserver.combined.OrderValue",
          },
          strategies: {
            name: "StreamStrategies",
            localName: "streamStrategies",
            methodKind: "server_streaming",
            input: "viewserver.combined.StrategyRequest",
            output: "viewserver.combined.StrategyValue",
          },
        },
        sources: {
          orders: {
            lifecycle: "leased",
            routeBy: ["strategyId", "region"],
            client: "orders",
            method: "streamOrders",
            request: {
              strategyId: "strategy-alpha",
              region: "usa",
            },
          },
          strategies: {
            lifecycle: "materialized",
            client: "strategies",
            method: "streamStrategies",
            request: { universe: "global" },
          },
          trades: {
            topic: "view-server-example-trades",
            regions: ["usa", "london"],
            keyFormat: "string",
            valueFormat: "json",
          },
        },
        rows: {
          orders: [
            {
              id: "strategy-alpha:usa:customer-strategy-alpha",
              customerId: "customer-strategy-alpha",
              status: "open",
              price: 15,
              region: "usa",
              strategyId: "strategy-alpha",
              updatedAt: 1,
            },
          ],
          strategies: [
            {
              id: "strategy-alpha:usa",
              strategyId: "strategy-alpha",
              region: "usa",
              status: "active",
              notional: 100,
              updatedAt: 1,
            },
          ],
          trades: [
            {
              id: "trade-combined-config",
              symbol: "EFFECT",
              side: "buy",
              quantity: 7,
              region: "london",
              updatedAt: 2,
            },
          ],
        },
      });
    }),
  );
});
