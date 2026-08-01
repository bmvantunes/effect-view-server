import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { kafka } from "effect-view-server/kafka/contract";
import { createViewServerReact } from "effect-view-server/react";
import { Schema } from "effect";
import { combinedService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const Strategy = Schema.Struct({
  id: ViewServerId,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const Trade = Schema.Struct({
  id: ViewServerId,
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

export const grpcSources = grpc.topicSources({
  orders: combinedService,
  strategies: combinedService,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      source: grpcSources.leased({
        client: "orders",
        method: "streamOrders",
        routeBy: ["strategyId", "region"],
        request: ({ strategyId, region }) => ({ strategyId, region }),
        map: ({ value, route }): typeof Order.Type => {
          return {
            id: `${String(route.strategyId)}:${String(route.region)}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: String(route.region),
            strategyId: String(route.strategyId),
            updatedAt: value.updatedAt,
          };
        },
      }),
    },
    strategies: {
      schema: Strategy,
      source: grpcSources.materialized({
        client: "strategies",
        method: "streamStrategies",
        request: () => ({ universe: "global" }),
        map: ({ value }) => ({
          id: String(`${value.strategyId}:${value.region}`),
          strategyId: value.strategyId,
          region: value.region,
          status: value.status,
          notional: value.notional,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    trades: {
      schema: Trade,
      source: kafka.source({
        cleanupPolicy: "delete",
        retentionPolicy: "Infinity",
        topic: "view-server-example-trades",
        regions: ["usa", "london"],
        value: kafka.json(() => Schema.toCodecJson(KafkaTrade)),
        key: kafka.string(),
        localRowKey: ({ key }) => key,
        map: ({ value, region }) => ({
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region: String(region),
          updatedAt: value.updatedAt,
        }),
        startFrom: "latest",
      }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const {
  ViewServerProvider,
  useLiveQuery,
  useSourceHealth,
  useViewServerHealth,
  useViewServerHealthSummary,
} = viewServerReact;
