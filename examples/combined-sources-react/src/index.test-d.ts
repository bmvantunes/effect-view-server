import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { grpcSources, useLiveQuery, useSourceHealth, viewServer } from "./view-server.config";

describe("combined sources example type contracts", () => {
  it("types Kafka, leased gRPC, and materialized gRPC topics independently", () => {
    const orders = useLiveQuery("orders", {
      select: ["id", "strategyId", "region"],
      where: [
        { field: "strategyId", type: "equals", filter: "strategy-alpha" },
        { field: "region", type: "equals", filter: "usa" },
      ],
      routeBy: { strategyId: "strategy-alpha", region: "usa" },
      limit: 10,
    });
    const strategies = useLiveQuery("strategies", {
      select: ["id", "notional"],
      where: [{ field: "status", type: "equals", filter: "active" }],
      limit: 10,
    });
    const trades = useLiveQuery("trades", {
      select: ["id", "symbol"],
      limit: 10,
    });

    expectTypeOf(orders).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly region: string;
      }>
    >();
    expectTypeOf(strategies).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly notional: number;
      }>
    >();
    expectTypeOf(trades).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly symbol: string;
      }>
    >();
  });

  it("keeps Kafka ownership separate from gRPC source topics", () => {
    expectTypeOf(viewServer.topics.trades.source.options.topic).toEqualTypeOf<string>();
    expectTypeOf(viewServer.topics.trades.source.options.regions).toEqualTypeOf<
      readonly ["usa", "london"]
    >();
    expectTypeOf(grpcSources.materialized).not.toBeAny();
    expectTypeOf(grpcSources.leased).not.toBeAny();
  });

  it("types materialized and leased Source Health by Topic", () => {
    const trades = useSourceHealth({ topic: "trades" });
    const orders = useSourceHealth({
      topic: "orders",
      routeBy: { strategyId: "strategy-alpha", region: "usa" },
    });

    expectTypeOf(trades).not.toBeAny();
    expectTypeOf(orders).not.toBeAny();

    // @ts-expect-error leased Source Health requires the exact route.
    useSourceHealth({ topic: "orders" });
    // @ts-expect-error materialized Source Health rejects routes.
    useSourceHealth({ topic: "trades", routeBy: { region: "usa" } });
  });
});
