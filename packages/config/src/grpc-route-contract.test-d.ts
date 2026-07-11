import { describe, expectTypeOf, it } from "@effect/vitest";
import { Stream } from "effect";
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
  it("types gRPC leased topic route metadata", () => {
    const grpcViewServer = defineViewServerConfig({
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

    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "orders">>().toEqualTypeOf<
      "region" | "status"
    >();

    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "trades">>().toEqualTypeOf<never>();

    const grpcSourceViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
        trades: grpcTradesMaterializedTopic,
      },
    });

    expectTypeOf<TopicRouteBy<typeof grpcSourceViewServer.topics, "orders">>().toEqualTypeOf<
      "region" | "status"
    >();

    expectTypeOf<
      TopicRouteBy<typeof grpcSourceViewServer.topics, "trades">
    >().toEqualTypeOf<never>();

    defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcTestTopicSources.leased({
          schema: Order,
          key: "id",
          client: "orders",
          method: "streamOrders",
          // @ts-expect-error routeBy fields must exist on the target topic row.
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
  });

  it("requires exact equality predicates for leased gRPC route fields", () => {
    const grpcViewServer = defineViewServerConfig({
      grpc: {
        clients: grpcTestClients,
      },
      topics: {
        orders: grpcOrdersByRegionStatusTopic,
      },
    });

    const assertGrpcRouteQueryTypes = (
      useLiveQuery: LiveQueryCall<typeof grpcViewServer.topics>,
    ) => {
      const validRouteQuery = useLiveQuery("orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open" },
          price: { gte: 10 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        select: ["id", "price", "updatedAt"],
        limit: 50,
      });

      expectTypeOf(validRouteQuery).toEqualTypeOf<
        LiveQueryResult<{
          readonly id: string;
          readonly price: number;
          readonly updatedAt: number;
        }>
      >();

      const missingRouteFieldQuery = {
        where: {
          region: { eq: "usa" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC topics require every routeBy field.
      useLiveQuery("orders", missingRouteFieldQuery);

      const routeInOperatorQuery = {
        where: {
          region: { eq: "usa" },
          status: { in: ["open", "closed"] },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
          };
          readonly status: {
            readonly in: readonly ["open", "closed"];
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must be exact eq predicates.
      useLiveQuery("orders", routeInOperatorQuery);

      const routeShorthandQuery = {
        where: {
          region: "usa",
          status: { eq: "open" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: "usa";
          readonly status: {
            readonly eq: "open";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must not use shorthand equality.
      useLiveQuery("orders", routeShorthandQuery);

      const routeExtraOperatorQuery = {
        where: {
          region: {
            eq: "usa",
            neq: "london",
          },
          status: { eq: "open" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
            readonly neq: "london";
          };
          readonly status: {
            readonly eq: "open";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must not include extra operators.
      useLiveQuery("orders", routeExtraOperatorQuery);
    };

    expectTypeOf(assertGrpcRouteQueryTypes).toBeFunction();
  });
});
