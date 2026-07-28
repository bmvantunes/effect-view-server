import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { createViewServerReact } from "effect-view-server/react";
import { Schema } from "effect";
import { ordersService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: ViewServerId,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const grpcSources = grpc.topicSources({
  orders: ordersService,
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
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
