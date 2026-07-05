import { defineViewServerConfig, grpc } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Effect, Schema, Stream } from "effect";
import { ordersService } from "./grpc-descriptors";

export const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  strategyId: Schema.String,
  updatedAt: Schema.Number,
});

export const grpcClients = {
  orders: grpc.connectClient({
    service: ordersService,
    baseUrl: "http://127.0.0.1:4317",
  }),
};

const grpcTopics = grpc.topicSources(grpcClients);

export const viewServer = defineViewServerConfig({
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
        Stream.make(
          {
            $typeName: "viewserver.example.OrderValue",
            customerId: `customer-${route.strategyId}`,
            status: "open",
            price: 10,
            updatedAt: 1,
          },
          {
            $typeName: "viewserver.example.OrderValue",
            customerId: `customer-${route.region}`,
            status: "open",
            price: 20,
            updatedAt: 2,
          },
        ).pipe(Stream.concat(Stream.never)),
      release: () => Effect.logInfo("Released leased gRPC orders feed."),
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
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
