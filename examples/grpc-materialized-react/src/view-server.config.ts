import { ViewServerId, defineViewServerConfig } from "effect-view-server/config";
import { grpc } from "effect-view-server/grpc/contract";
import { createViewServerReact } from "effect-view-server/react";
import { Schema } from "effect";
import { strategiesService } from "./grpc-descriptors";

export const Strategy = Schema.Struct({
  id: ViewServerId,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const grpcSources = grpc.topicSources({
  strategies: strategiesService,
});

export const viewServer = defineViewServerConfig({
  topics: {
    strategies: {
      schema: Strategy,
      source: grpcSources.materialized({
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
      }),
    },
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
