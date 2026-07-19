import { describe, expectTypeOf, it } from "@effect/vitest";
import { Schema, Stream } from "effect";
import { defineViewServerConfig, type LiveQueryResult, type TopicRouteBy } from "./index";
import {
  grpcOrdersByRegionStatusTopic,
  grpcTestClients,
  grpcTestTopicSources,
  grpcTradesMaterializedTopic,
} from "../test-harness/grpc";
import type { LiveQueryCall } from "../test-harness/live-query";
import { Order, Position } from "../test-harness/schemas";

describe("gRPC route generic contracts", () => {
  it("types leased topic route metadata", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
        positions: { schema: Position, key: "id" },
      },
    });

    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "orders">>().toEqualTypeOf<
      "region" | "status"
    >();
    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "trades">>().toEqualTypeOf<never>();

    defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcTestTopicSources.leased({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          // @ts-expect-error routeBy fields must exist on the Topic Row.
          routeBy: ["strategyId"],
          request: () => ({ orderId: "invalid" }),
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

    grpcTestTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      // @ts-expect-error leased route declarations must not repeat a field.
      routeBy: ["region", "region"],
      request: () => ({ orderId: "duplicate" }),
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

    const OptionalRouteOrder = Schema.Struct({
      id: Schema.String,
      customerId: Schema.String,
      status: Schema.Literals(["open", "closed", "cancelled"]),
      price: Schema.Number,
      region: Schema.optionalKey(Schema.String),
      updatedAt: Schema.Number,
    });
    grpcTestTopicSources.leased({
      schema: OptionalRouteOrder,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region"],
      request: (route) => {
        expectTypeOf(route).toEqualTypeOf<{ readonly region: string }>();
        return { orderId: route.region };
      },
      acquire: () => Stream.never,
      map: ({ value, route }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    });
  });

  it("requires an exact routeBy object independently from where", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: { clients: grpcTestClients },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
      },
    });

    const assertGrpcRouteQueryTypes = (
      useLiveQuery: LiveQueryCall<typeof grpcViewServer.topics>,
    ) => {
      const valid = useLiveQuery("orders", {
        routeBy: { region: "UsÁ", status: "open" },
        where: [
          {
            type: "OR",
            conditions: [
              { field: "region", type: "equals", filter: "usa" },
              { field: "region", type: "equals", filter: "london" },
            ],
          },
        ],
        select: ["id", "price"],
      });
      expectTypeOf(valid).toEqualTypeOf<
        LiveQueryResult<{ readonly id: string; readonly price: number }>
      >();

      const missingRoute = { select: ["id"] } satisfies {
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased topics require routeBy.
      useLiveQuery("orders", missingRoute);

      const missingField = {
        routeBy: { region: "usa" },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa" };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy must contain every configured route field.
      useLiveQuery("orders", missingField);

      const extraField = {
        routeBy: { region: "usa", status: "open", desk: "equities" },
        select: ["id"],
      } satisfies {
        readonly routeBy: {
          readonly region: "usa";
          readonly status: "open";
          readonly desk: "equities";
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy must contain only configured route fields.
      useLiveQuery("orders", extraField);

      const wrongValue = {
        routeBy: { region: "usa", status: 1 },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa"; readonly status: 1 };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error routeBy values must match their Topic Row field types.
      useLiveQuery("orders", wrongValue);

      const materializedRoute = {
        routeBy: { region: "usa" },
        select: ["id"],
      } satisfies {
        readonly routeBy: { readonly region: "usa" };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error non-leased topics reject routeBy.
      useLiveQuery("trades", materializedRoute);
    };

    expectTypeOf(assertGrpcRouteQueryTypes).toBeFunction();
  });
});
