import { defineViewServerConfig, grpc, kafka } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Schema, Stream } from "effect";
import { combinedService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const KafkaTrade = Schema.Struct({
  symbol: Schema.String,
  side: Schema.Literals(["buy", "sell"]),
  quantity: Schema.Number,
  updatedAt: Schema.Number,
});

export const kafkaRegions = {
  usa: "127.0.0.1:9092",
  london: "127.0.0.1:9094",
};

export const grpcClients = {
  orders: grpc.connectClient({
    service: combinedService,
    baseUrl: "http://127.0.0.1:4319",
  }),
  strategies: grpc.connectClient({
    service: combinedService,
    baseUrl: "http://127.0.0.1:4320",
  }),
};

const grpcTopics = grpc.topicSources(grpcClients);

export const viewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  grpc: {
    clients: grpcClients,
  },
  topics: {
    orders: grpcTopics.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["strategyId", "region"],
      request: ({ strategyId, region }) => ({ strategyId, region }),
      acquire: ({ route }) =>
        Stream.make({
          $typeName: "viewserver.combined.OrderValue",
          customerId: `customer-${route.strategyId}`,
          status: "open",
          price: 15,
          updatedAt: 1,
        }).pipe(Stream.concat(Stream.never)),
      map: ({ value, route }) => ({
        id: `${route.strategyId}:${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        strategyId: route.strategyId,
        updatedAt: value.updatedAt,
      }),
    }),
    strategies: grpcTopics.materialized({
      schema: Strategy,
      key: "id",
      client: "strategies",
      method: "streamStrategies",
      request: () => ({ universe: "global" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.combined.StrategyValue",
          strategyId: "strategy-alpha",
          region: "usa",
          status: "active",
          notional: 100,
          updatedAt: 1,
        }).pipe(Stream.concat(Stream.never)),
      map: ({ value }) => ({
        id: `${value.strategyId}:${value.region}`,
        strategyId: value.strategyId,
        region: value.region,
        status: value.status,
        notional: value.notional,
        updatedAt: value.updatedAt,
      }),
    }),
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "view-server-example-trades",
        regions: ["usa", "london"],
        value: kafka.json(KafkaTrade),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealth, useViewServerHealthSummary } =
  viewServerReact;
