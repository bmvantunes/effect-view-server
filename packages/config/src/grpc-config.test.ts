import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { defineViewServerConfig, grpc, validateLiveQuerySourceRoute } from "./index";
import { grpcSourceMarkers } from "./internal";
import {
  grpcOrdersByRegionStatusTopic,
  grpcOrdersMaterializedTopic,
  grpcTestClients,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import { ordersService, tradesOnlyService } from "../test-harness/protobuf";
import { Order, Position, Trade } from "../test-harness/schemas";

describe("gRPC configuration runtime behavior", () => {
  it("keeps gRPC source markers exact at runtime", () => {
    expect(grpcSourceMarkers.leased({ routeBy: ["region"] }).routeBy).toStrictEqual(["region"]);
  });

  it("validates leased gRPC route predicates at runtime", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
      },
    });

    const grpcRouteValidationViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
        positions: {
          schema: Position,
          key: "id",
        },
      },
    });

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "missing", {})).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "positions", {}),
    ).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "trades", {}),
    ).toBeUndefined();

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", null)).toBe(
      "Leased topic orders requires a query object.",
    );

    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {})).toBe(
      "Leased topic orders requires exact equality filters for route fields: region, status.",
    );

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open", neq: "closed" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { neq: "closed" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");

    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open" },
        },
      }),
    ).toBeUndefined();

    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: [] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");

    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            grpcSource: { kind: "grpc", lifecycle: "leased", routeBy: ["region", 1] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");
  });

  it("rejects malformed gRPC source metadata and preserves runtime source shape", () => {
    const clients = {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders.example.test",
      }),
      trades: grpc.connectClient({
        service: tradesOnlyService,
        baseUrl: "https://trades.example.test",
      }),
    };

    const topicSources = grpc.topicSources(clients);

    const otherClients = {
      otherOrders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://other-orders.example.test",
      }),
    };

    const otherTopicSources = grpc.topicSources(otherClients);

    const topicOwnedGrpcViewServer = defineViewServerConfig({
      grpc: {
        clients,
      },
      topics: {
        orders: topicSources.leased({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          routeBy: ["region", "status"],
          request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
          acquire: () =>
            Stream.make({
              $typeName: "viewserver.test.OrderValue",
              customerId: "customer-topic-source-1",
              status: "open",
              price: 10,
              updatedAt: 1,
            }),
          release: () => Effect.void,
          map: ({ value, route }) => ({
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        }),
        trades: topicSources.materialized({
          schema: Trade,
          key: "id",
          client: "orders",
          method: "streamTrades",
          request: () => ({ orderId: "all-topic-source-trades" }),
          acquire: () =>
            Stream.make({
              $typeName: "viewserver.test.TradeValue",
              symbol: "AAPL",
              quantity: 1,
              price: 10,
            }),
          release: () => Effect.void,
          map: ({ value }) => ({
            id: value.symbol,
            symbol: value.symbol,
            quantity: value.quantity,
            price: value.price,
            region: "usa",
          }),
        }),
      },
    });

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must be bound to this config's grpc.clients.
      defineViewServerConfig({
        grpc: {
          clients,
        },
        topics: {
          orders: otherTopicSources.materialized({
            schema: Order,
            key: "id",
            client: "otherOrders",
            method: "streamOrders",
            request: () => ({ orderId: "wrong-client-set" }),
            acquire: () =>
              Stream.make({
                $typeName: "viewserver.test.OrderValue",
                customerId: "customer-wrong-client-set",
                status: "open",
                price: 10,
                updatedAt: 1,
              }),
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource client otherOrders, but defineViewServerConfig.grpc.clients does not define it.",
    );

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must use the same row key as the topic.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "customerId",
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource for row key id, but topic key is customerId.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: 1,
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method 1, but grpc client orders does not define it.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: "missingMethod",
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method missingMethod, but grpc client orders does not define it.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign({}, grpcOrdersMaterializedTopic.grpcSource, {
              method: "getOrder",
            }),
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource method getOrder, but grpc client orders method is not server-streaming.",
    );

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources must use the same schema as the topic.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Trade,
            key: "id",
            grpcSource: grpcOrdersMaterializedTopic.grpcSource,
          },
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource for a different schema than the topic schema.",
    );

    expect(() =>
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: grpcSourceMarkers.materialized(),
          },
        },
      }),
    ).not.toThrow();

    expect(() =>
      // @ts-expect-error unbranded gRPC source objects are not valid public TypeScript config.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              client: "orders",
              method: "streamOrders",
              request: () => ({ orderId: "unbranded-source" }),
              acquire: () =>
                Stream.make({
                  $typeName: "viewserver.test.OrderValue",
                  customerId: "customer-unbranded-source",
                  status: "open",
                  price: 10,
                  updatedAt: 1,
                }),
              map: () => ({
                id: "customer-unbranded-source",
                customerId: "customer-unbranded-source",
                status: "open",
                price: 10,
                region: "usa",
                updatedAt: 1,
              }),
            }),
          },
        },
      }),
    ).not.toThrow();

    expect(() =>
      // @ts-expect-error unbranded gRPC source objects are not valid public TypeScript config.
      defineViewServerConfig({
        grpc: {
          clients: grpcTestClients,
        },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              client: "orders",
              method: "streamOrders",
            }),
          },
        },
      }),
    ).toThrow("View Server topic orders declares invalid gRPC source metadata.");

    expect(() =>
      // @ts-expect-error incomplete concrete gRPC source bindings remain runtime-guarded.
      defineViewServerConfig({
        grpc: { clients },
        topics: {
          orders: {
            schema: Order,
            key: "id",
            grpcSource: Object.assign(grpcSourceMarkers.materialized(), {
              method: "streamOrders",
              request: () => ({ orderId: "missing-client" }),
              acquire: () => Stream.never,
              map: () => ({
                id: "missing-client",
                customerId: "missing-client",
                status: "open",
                price: 1,
                region: "usa",
                updatedAt: 1,
              }),
            }),
          },
        },
      }),
    ).toThrow("View Server topic orders declares invalid gRPC source metadata.");

    expect(() =>
      // @ts-expect-error topic-owned gRPC sources require defineViewServerConfig.grpc.clients.
      defineViewServerConfig({
        topics: {
          orders: topicSources.materialized({
            schema: Order,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "missing-clients" }),
            acquire: () => Stream.never,
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
        },
      }),
    ).toThrow(
      "View Server topic orders declares grpcSource, but defineViewServerConfig.grpc.clients was not provided.",
    );

    const grpcInfraViewServer = defineViewServerConfig({
      grpc: {
        clients,
      },
      topics: {
        orders: topicSources.materialized({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          request: () => ({ orderId: "all-infra-orders" }),
          acquire: () => Stream.never,
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        }),
      },
    });

    expect(grpcInfraViewServer.grpc).toStrictEqual({
      clients,
    });

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.lifecycle).toBe("leased");

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.client).toBe("orders");

    expect(topicOwnedGrpcViewServer.topics.orders.grpcSource.method).toBe("streamOrders");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.lifecycle).toBe("materialized");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.client).toBe("orders");

    expect(topicOwnedGrpcViewServer.topics.trades.grpcSource.method).toBe("streamTrades");

    expect(
      Object.getOwnPropertySymbols(topicOwnedGrpcViewServer.topics.orders.grpcSource).some(
        (symbol) =>
          Reflect.get(topicOwnedGrpcViewServer.topics.orders.grpcSource, symbol) === clients,
      ),
    ).toBe(false);

    expect(
      Object.getOwnPropertySymbols(topicOwnedGrpcViewServer.topics.trades.grpcSource).some(
        (symbol) =>
          Reflect.get(topicOwnedGrpcViewServer.topics.trades.grpcSource, symbol) === clients,
      ),
    ).toBe(false);
  });
});
