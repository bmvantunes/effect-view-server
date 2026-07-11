import * as BigDecimal from "effect/BigDecimal";
import { Stream } from "effect";
import { grpc } from "../src/index";
import { Order, Position, Trade } from "./schemas";
import { ordersService, tradesOnlyService } from "./protobuf";

export const grpcTestClients = {
  orders: grpc.connectClient({
    service: ordersService,
    baseUrl: "https://orders-grpc.example.test",
  }),
  trades: grpc.connectClient({
    service: tradesOnlyService,
    baseUrl: "https://trades-grpc.example.test",
  }),
};

export const grpcTestTopicSources = grpc.topicSources(grpcTestClients);

export const grpcOrdersByRegionStatusTopic = grpcTestTopicSources.leased({
  schema: Order,
  key: "id",
  client: "orders",
  method: "streamOrders",
  routeBy: ["region", "status"],
  request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
  acquire: () => Stream.never,
  map: ({ value, route }) => ({
    id: value.customerId,
    customerId: value.customerId,
    status: route.status,
    price: value.price,
    region: route.region,
    updatedAt: value.updatedAt,
  }),
});

export const grpcOrdersMaterializedTopic = grpcTestTopicSources.materialized({
  schema: Order,
  key: "id",
  client: "orders",
  method: "streamOrders",
  request: () => ({ orderId: "all-orders" }),
  acquire: () => Stream.never,
  map: ({ value }) => ({
    id: value.customerId,
    customerId: value.customerId,
    status: value.status,
    price: value.price,
    region: "usa",
    updatedAt: value.updatedAt,
  }),
});

export const grpcTradesMaterializedTopic = grpcTestTopicSources.materialized({
  schema: Trade,
  key: "id",
  client: "trades",
  method: "streamTrades",
  request: () => ({ orderId: "all-trades" }),
  acquire: () => Stream.never,
  map: ({ value }) => ({
    id: value.symbol,
    symbol: value.symbol,
    quantity: value.quantity,
    price: value.price,
    region: "usa",
  }),
});

export const grpcPositionsByAccountSymbolTopic = grpcTestTopicSources.leased({
  schema: Position,
  key: "id",
  client: "orders",
  method: "streamOrders",
  routeBy: ["accountId", "symbol"],
  request: ({ accountId, symbol }) => ({ orderId: `${accountId}:${symbol}` }),
  acquire: () => Stream.never,
  map: ({ route }) => ({
    id: `${route.accountId}:${route.symbol}`,
    accountId: route.accountId,
    symbol: route.symbol,
    active: true,
    quantity: 0n,
    optionalQuantity: undefined,
    price: BigDecimal.fromStringUnsafe("0"),
    notional: 0,
    optionalNotional: undefined,
  }),
});
