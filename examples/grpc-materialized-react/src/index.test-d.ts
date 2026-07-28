import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { strategiesService } from "./grpc-descriptors";
import { grpcSources, useLiveQuery, viewServer } from "./view-server.config";

describe("materialized gRPC example type contracts", () => {
  it("preserves selected strategy row types", () => {
    const result = useLiveQuery("strategies", {
      select: ["id", "strategyId", "notional"],
      where: [{ field: "status", type: "equals", filter: "active" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly notional: number;
      }>
    >();
  });

  it("keeps the generated descriptor and canonical source typed", () => {
    expectTypeOf(
      strategiesService.method.streamStrategies.methodKind,
    ).toEqualTypeOf<"server_streaming">();
    expectTypeOf(viewServer.topics.strategies.source.lifecycle).toEqualTypeOf<"materialized">();
    expectTypeOf(viewServer.topics.strategies.source.options.client).toEqualTypeOf<string>();

    grpcSources.materialized({
      client: "strategies",
      method: "streamStrategies",
      request: () => ({ universe: "global" }),
      map: ({ value }) => ({
        id: String(`${value.strategyId}:${value.region}`),
        strategyId: value.strategyId,
        region: value.region,
        status: value.status,
        notional: value.notional,
        updatedAt: value.updatedAt,
      }),
      // @ts-expect-error Materialized gRPC Source options are exact.
      unsupported: true,
    });
  });
});
