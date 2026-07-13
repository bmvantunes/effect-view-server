import { describe, expectTypeOf, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { defineViewServerConfig, grpc, type GrpcClientValue } from "./index";
import { grpcSourceMarkers } from "./internal";
import {
  grpcOrdersByRegionStatusTopic,
  grpcOrdersMaterializedTopic,
  grpcTestClients,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import { ordersService, tradesOnlyService } from "../test-harness/protobuf";
import type { OrdersValueMessage, TradesValueMessage } from "../test-harness/protobuf";
import { Order, Position, Trade } from "../test-harness/schemas";

const openOrderStatus: (typeof Order.Type)["status"] = "open";

describe("gRPC source generic contracts", () => {
  it("types gRPC clients and feed mapping contracts", () => {
    defineViewServerConfig({
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
          request: ({ region, status }) => {
            expectTypeOf(region).toEqualTypeOf<string>();
            expectTypeOf(status).toEqualTypeOf<"open" | "closed" | "cancelled">();
            return { orderId: `${region}:${status}` };
          },
          acquire: ({ client, request, route, session }) => {
            expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
            expectTypeOf(client.streamOrders).toBeFunction();
            expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
            expectTypeOf(route).toEqualTypeOf<{
              readonly region: string;
              readonly status: "open" | "closed" | "cancelled";
            }>();
            expectTypeOf(session.forwardedHeaders).toEqualTypeOf<
              Readonly<Record<string, string>>
            >();
            return Stream.make({
              $typeName: "viewserver.test.OrderValue",
              customerId: "customer-topic-source-1",
              status: "open",
              price: 10,
              updatedAt: 1,
            });
          },
          release: ({ client, request, route, session }) => {
            expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
            expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
            expectTypeOf(route.status).toEqualTypeOf<"open" | "closed" | "cancelled">();
            expectTypeOf(session.systemHeaders).toEqualTypeOf<Readonly<Record<string, string>>>();
            return Effect.void;
          },
          map: ({ value, route, schema }) => {
            expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
            expectTypeOf(route.region).toEqualTypeOf<string>();
            expectTypeOf(schema).toEqualTypeOf<typeof Order>();
            return {
              id: `${route.region}:${value.customerId}`,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: route.region,
              updatedAt: value.updatedAt,
            };
          },
        }),
        trades: topicSources.materialized({
          schema: Trade,
          key: "id",
          client: "orders",
          method: "streamTrades",
          request: () => ({ orderId: "all-topic-source-trades" }),
          acquire: ({ client, request, route, session }) => {
            expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
            expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
            expectTypeOf(route).toEqualTypeOf<undefined>();
            expectTypeOf(session.forwardedHeaders).toEqualTypeOf<
              Readonly<Record<string, string>>
            >();
            return Stream.make({
              $typeName: "viewserver.test.TradeValue",
              symbol: "AAPL",
              quantity: 1,
              price: 10,
            });
          },
          release: ({ client, request, route, session }) => {
            expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
            expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
            expectTypeOf(route).toEqualTypeOf<undefined>();
            expectTypeOf(session.systemHeaders).toEqualTypeOf<Readonly<Record<string, string>>>();
            return Effect.void;
          },
          map: ({ value, route, schema }) => {
            expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
            expectTypeOf(route).toEqualTypeOf<undefined>();
            expectTypeOf(schema).toEqualTypeOf<typeof Trade>();
            return {
              id: value.symbol,
              symbol: value.symbol,
              quantity: value.quantity,
              price: value.price,
              region: "usa",
            };
          },
        }),
      },
    });

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
    });

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
    });

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
    });

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
    });

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
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      // @ts-expect-error topic-owned gRPC sources reject clients outside grpc.clients.
      client: "missing",
      method: "streamOrders",
      request: () => ({ orderId: "order-1" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: () => ({
        id: "customer-1",
        customerId: "customer-1",
        status: openOrderStatus,
        price: 10,
        region: "usa",
        updatedAt: 1,
      }),
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      request: () => ({ order: { orderId: "nested-order" } }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
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
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      // @ts-expect-error nested gRPC request messages reject fields outside the nested method input schema.
      request: () => ({ order: { orderId: "nested-order", extra: true } }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
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
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      // @ts-expect-error nested gRPC request messages reject any-typed nested values.
      request: () => ({ order: JSON.parse("{}") }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
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
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      // @ts-expect-error topic-owned gRPC sources must use server-streaming methods.
      method: "getOrder",
      // @ts-expect-error non-server-streaming methods do not expose a streaming request shape.
      request: () => ({ orderId: "order-1" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: () => ({
        id: "customer-1",
        customerId: "customer-1",
        status: openOrderStatus,
        price: 10,
        region: "usa",
        updatedAt: 1,
      }),
    });

    topicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      // @ts-expect-error leased route fields must exist on the topic row schema.
      routeBy: ["unknown"],
      request: () => ({ orderId: "bad-route" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
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
    });

    topicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      routeBy: ["region"],
      request: ({ region }) => ({ order: { orderId: region } }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value, route }) => ({
        id: `${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });

    topicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      routeBy: ["region"],
      // @ts-expect-error leased nested gRPC request messages reject fields outside the nested method input schema.
      request: ({ region }) => ({ order: { orderId: region, extra: true } }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value, route }) => ({
        id: `${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });

    topicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamWrappedOrders",
      routeBy: ["region"],
      // @ts-expect-error leased nested gRPC request messages reject any-typed nested values.
      request: () => ({ order: JSON.parse("{}") }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value, route }) => ({
        id: `${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({ orderId: "order-1" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error topic-owned gRPC maps must not return fields outside the topic schema.
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
        ze: true,
      }),
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({ orderId: "order-1" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error topic-owned gRPC maps must return every topic schema field.
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
      }),
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      // @ts-expect-error topic-owned gRPC requests reject fields outside the method input schema.
      request: () => ({ orderId: "order-1", extra: true }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
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
    });

    topicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region"],
      // @ts-expect-error leased gRPC requests reject fields outside the method input schema.
      request: ({ region }) => ({ orderId: region, extra: true }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value, route }) => ({
        id: `${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });

    topicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({ orderId: "order-1" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error topic-owned gRPC maps cannot return any.
      map: () => JSON.parse("{}"),
    });

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
    });

    const _grpcInfraViewServer = defineViewServerConfig({
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

    const invalidSpreadTopicOwnedMaterializedRequest: typeof topicOwnedGrpcViewServer.topics.trades.grpcSource =
      {
        ...topicOwnedGrpcViewServer.topics.trades.grpcSource,
        // @ts-expect-error spread-mutated topic-owned materialized gRPC sources cannot replace helper-branded requests.
        request: () => ({ orderId: "bad-materialized-request", extra: true }),
      };

    const invalidSpreadTopicOwnedLeasedRequest: typeof topicOwnedGrpcViewServer.topics.orders.grpcSource =
      {
        ...topicOwnedGrpcViewServer.topics.orders.grpcSource,
        // @ts-expect-error spread-mutated topic-owned leased gRPC sources cannot replace helper-branded requests.
        request: ({ region, status }) => ({
          orderId: `${region}:${status}`,
          extra: true,
        }),
      };

    expectTypeOf(invalidSpreadTopicOwnedMaterializedRequest).not.toBeNever();

    expectTypeOf(invalidSpreadTopicOwnedLeasedRequest).not.toBeNever();

    const invalidSpreadTopicOwnedMaterializedMap: typeof topicOwnedGrpcViewServer.topics.trades.grpcSource =
      {
        ...topicOwnedGrpcViewServer.topics.trades.grpcSource,
        // @ts-expect-error spread-mutated topic-owned materialized gRPC sources cannot replace helper-branded exact maps.
        map: ({ value, route, schema }) => {
          expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
          expectTypeOf(route).toEqualTypeOf<undefined>();
          expectTypeOf(schema).toEqualTypeOf<typeof Trade>();
          return {
            id: value.symbol,
            symbol: value.symbol,
            quantity: value.quantity,
            price: value.price,
            region: "usa",
            extra: true,
          };
        },
      };

    const invalidSpreadTopicOwnedLeasedMap: typeof topicOwnedGrpcViewServer.topics.orders.grpcSource =
      {
        ...topicOwnedGrpcViewServer.topics.orders.grpcSource,
        // @ts-expect-error spread-mutated topic-owned leased gRPC sources cannot replace helper-branded exact maps.
        map: ({ value, route, schema }) => {
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(route).toEqualTypeOf<{
            readonly region: string;
            readonly status: "open" | "closed" | "cancelled";
          }>();
          expectTypeOf(schema).toEqualTypeOf<typeof Order>();
          return {
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
            extra: true,
          };
        },
      };

    expectTypeOf(invalidSpreadTopicOwnedMaterializedMap).not.toBeNever();

    expectTypeOf(invalidSpreadTopicOwnedLeasedMap).not.toBeNever();
  });
});
