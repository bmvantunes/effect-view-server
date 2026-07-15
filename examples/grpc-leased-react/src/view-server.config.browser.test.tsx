import type { Client } from "@connectrpc/connect";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { ordersService, orderRouteSchema, orderValueSchema } from "./grpc-descriptors";
import { Order, viewServer } from "./view-server.config";

const client = {
  streamOrders: async function* streamOrders() {},
} satisfies Client<typeof ordersService>;

describe("leased gRPC React example topic-owned source", () => {
  it.effect("constructs descriptors and maps the leased route stream", () =>
    Effect.gen(function* () {
      const source = viewServer.topics.orders.grpcSource;
      const route = {
        strategyId: "strategy-alpha",
        region: "usa",
      };
      const request = source.request(route);
      const sourceInput = {
        client,
        request,
        route,
        session: {
          id: null,
          forwardedHeaders: {},
          systemHeaders: {},
        },
      };
      const values = yield* source.acquire(sourceInput).pipe(Stream.take(2), Stream.runCollect);
      const release = source.release ?? (() => Effect.fail("Expected leased source release"));
      yield* release(sourceInput);
      const rows = Array.from(values, (value) =>
        source.map({
          value,
          route,
          schema: Order,
        }),
      );

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
          client: source.client,
          method: source.method,
          request,
          hasRelease: source.release !== undefined,
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
          hasRelease: true,
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
    }),
  );
});
