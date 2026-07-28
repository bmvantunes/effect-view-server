import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { useLiveQuery, viewServer } from "./view-server.config";

describe("kafka example type contracts", () => {
  it("preserves selected Kafka-backed order row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "region", "price"],
      where: [{ field: "status", type: "equals", filter: "open" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly region: string;
        readonly price: number;
      }>
    >();
  });

  it("preserves selected Kafka-backed trade row types", () => {
    const result = useLiveQuery("trades", {
      select: ["id", "symbol", "side", "region"],
      where: [{ field: "side", type: "equals", filter: "buy" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly symbol: string;
        readonly side: "buy" | "sell";
        readonly region: string;
      }>
    >();
  });

  it("keeps the Kafka mapping typed", () => {
    expectTypeOf(viewServer.topics.orders.source.options.topic).toEqualTypeOf<string>();
    expectTypeOf(viewServer.topics.orders.source.options.regions).toEqualTypeOf<readonly ["usa"]>();
    expectTypeOf(viewServer.topics.trades.source.options.topic).toEqualTypeOf<string>();
    expectTypeOf(viewServer.topics.trades.source.options.regions).toEqualTypeOf<
      readonly ["london"]
    >();
  });
});
