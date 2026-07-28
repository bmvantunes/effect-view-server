import { describe, expect, it } from "@effect/vitest";
import { ordersService, orderRouteSchema, orderValueSchema } from "./grpc-descriptors";
import { viewServer } from "./view-server.config";

describe("leased gRPC React example topic-owned source", () => {
  it("constructs descriptors and maps leased values", () => {
    const source = viewServer.topics.orders.source;
    const route = {
      strategyId: "strategy-alpha",
      region: "usa",
    };
    const request = source.options.request(route);
    const values = [
      {
        $typeName: "viewserver.example.OrderValue",
        customerId: "customer-strategy-alpha",
        status: "open",
        price: 10,
        updatedAt: 1,
      },
      {
        $typeName: "viewserver.example.OrderValue",
        customerId: "customer-usa",
        status: "open",
        price: 20,
        updatedAt: 2,
      },
    ] as const;
    const rows = Array.from(values, (value) => source.options.mapValue(value, route));

    expect({
      descriptors: {
        valueTypeName: orderValueSchema.typeName,
        valueFields: orderValueSchema.fields.map((field) => ({
          name: field.name,
          localName: field.localName,
        })),
        routeTypeName: orderRouteSchema.typeName,
        routeFields: orderRouteSchema.fields.map((field) => ({
          name: field.name,
          localName: field.localName,
        })),
        serviceTypeName: ordersService.typeName,
        method: {
          name: ordersService.method.streamOrders.name,
          localName: ordersService.method.streamOrders.localName,
          methodKind: ordersService.method.streamOrders.methodKind,
          input: ordersService.method.streamOrders.input.typeName,
          output: ordersService.method.streamOrders.output.typeName,
        },
      },
      source: {
        lifecycle: source.lifecycle,
        routeBy: source.routeBy,
        client: source.options.client,
        method: source.options.method,
        request,
      },
      rows,
    }).toStrictEqual({
      descriptors: {
        valueTypeName: "viewserver.example.OrderValue",
        valueFields: [
          { name: "customer_id", localName: "customerId" },
          { name: "status", localName: "status" },
          { name: "price", localName: "price" },
          { name: "updated_at", localName: "updatedAt" },
        ],
        routeTypeName: "viewserver.example.OrderRoute",
        routeFields: [
          { name: "strategy_id", localName: "strategyId" },
          { name: "region", localName: "region" },
        ],
        serviceTypeName: "viewserver.example.OrdersService",
        method: {
          name: "StreamOrders",
          localName: "streamOrders",
          methodKind: "server_streaming",
          input: "viewserver.example.OrderRoute",
          output: "viewserver.example.OrderValue",
        },
      },
      source: {
        lifecycle: "leased",
        routeBy: ["strategyId", "region"],
        client: "orders",
        method: "streamOrders",
        request: {
          strategyId: "strategy-alpha",
          region: "usa",
        },
      },
      rows: [
        {
          id: "strategy-alpha:usa:customer-strategy-alpha",
          customerId: "customer-strategy-alpha",
          status: "open",
          price: 10,
          region: "usa",
          strategyId: "strategy-alpha",
          updatedAt: 1,
        },
        {
          id: "strategy-alpha:usa:customer-usa",
          customerId: "customer-usa",
          status: "open",
          price: 20,
          region: "usa",
          strategyId: "strategy-alpha",
          updatedAt: 2,
        },
      ],
    });
  });
});
