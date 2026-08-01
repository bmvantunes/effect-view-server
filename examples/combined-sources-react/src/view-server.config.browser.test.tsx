import { describe, expect, it } from "@effect/vitest";
import { decodeKafkaCodec, type KafkaMessageMetadata } from "effect-view-server/kafka/contract";
import { Effect } from "effect";
import { combinedService } from "./grpc-descriptors";
import { viewServer } from "./view-server.config";

const textEncoder = new TextEncoder();

describe("combined-sources React example topic-owned sources", () => {
  it.effect("maps leased, materialized, and Kafka source values", () =>
    Effect.gen(function* () {
      const orderSource = viewServer.topics.orders.source;
      const orderRoute = {
        strategyId: "strategy-alpha",
        region: "usa",
      };
      const orderRequest = orderSource.options.request(orderRoute);
      const orderValues = [
        {
          $typeName: "viewserver.combined.OrderValue",
          customerId: "customer-strategy-alpha",
          status: "open",
          price: 15,
          updatedAt: 1,
        },
      ] as const;
      const orderRows = Array.from(orderValues, (value) =>
        orderSource.options.mapValue(value, orderRoute),
      );

      const strategySource = viewServer.topics.strategies.source;
      const strategyRequest = strategySource.options.request();
      const strategyValues = [
        {
          $typeName: "viewserver.combined.StrategyValue",
          strategyId: "strategy-alpha",
          region: "usa",
          status: "active",
          notional: 100,
          updatedAt: 1,
        },
      ] as const;
      const strategyRows = Array.from(strategyValues, (value) =>
        strategySource.options.mapValue(value),
      );

      const tradeSource = viewServer.topics.trades.source;
      const tradeOptions = tradeSource.options;
      const tradeMetadata = {
        sourceTopic: tradeOptions.topic,
        sourceRegion: "london",
        partition: 0,
        offset: 1n,
        timestampNanos: 0n,
        headers: {},
      } satisfies KafkaMessageMetadata<"london">;
      const tradeKey = yield* decodeKafkaCodec(tradeOptions.key, {
        bytes: textEncoder.encode("trade-combined-config"),
        metadata: tradeMetadata,
      });
      const tradeValue = yield* decodeKafkaCodec(tradeOptions.value, {
        bytes: textEncoder.encode('{"symbol":"EFFECT","side":"buy","quantity":7,"updatedAt":2}'),
        metadata: tradeMetadata,
      });
      const tradeLocalRowKey = tradeOptions.localRowKey({
        key: tradeKey,
        value: tradeValue,
        region: "london",
      });
      const tradeRow = tradeOptions.map({
        key: tradeKey,
        value: tradeValue,
        region: "london",
        localRowKey: tradeLocalRowKey,
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
            client: orderSource.options.client,
            method: orderSource.options.method,
            request: orderRequest,
          },
          strategies: {
            lifecycle: strategySource.lifecycle,
            client: strategySource.options.client,
            method: strategySource.options.method,
            request: strategyRequest,
          },
          trades: {
            topic: tradeOptions.topic,
            regions: tradeOptions.regions,
            keyFormat: tradeOptions.key.format,
            valueFormat: tradeOptions.value.format,
          },
        },
        rows: {
          orders: orderRows,
          strategies: strategyRows,
          trades: [{ id: `london:${tradeLocalRowKey}`, ...tradeRow }],
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
              id: "london:trade-combined-config",
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
