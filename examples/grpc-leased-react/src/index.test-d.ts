import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { ordersService } from "./grpc-descriptors";
import { useLiveQuery, viewServer } from "./view-server.config";

describe("leased gRPC example type contracts", () => {
  it("requires leased route values and preserves selected row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "strategyId", "region"],
      where: [
        { field: "strategyId", type: "equals", filter: "strategy-alpha" },
        { field: "region", type: "equals", filter: "usa" },
      ],
      routeBy: { strategyId: "strategy-alpha", region: "usa" },
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly region: string;
      }>
    >();
  });

  it("rejects leased queries missing required route fields", () => {
    const missingRouteQuery = {
      select: ["id", "region"],
      where: [{ field: "region", type: "equals", filter: "usa" }],
      limit: 20,
    } satisfies {
      readonly select: readonly ["id", "region"];
      readonly where: readonly [
        { readonly field: "region"; readonly type: "equals"; readonly filter: "usa" },
      ];
      readonly limit: 20;
    };
    // @ts-expect-error leased gRPC order queries must include the exact routeBy object.
    const invalidRouteQuery = useLiveQuery("orders", missingRouteQuery);

    expectTypeOf(invalidRouteQuery).not.toBeAny();
  });

  it("keeps the generated descriptor and canonical source typed", () => {
    expectTypeOf(ordersService.method.streamOrders.methodKind).toEqualTypeOf<"server_streaming">();
    expectTypeOf(viewServer.topics.orders.source.lifecycle).toEqualTypeOf<"leased">();
    expectTypeOf(viewServer.topics.orders.source.options.client).toEqualTypeOf<string>();
  });
});
