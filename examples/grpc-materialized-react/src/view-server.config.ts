import { defineViewServerConfig, grpc } from "effect-view-server/config";
import { createViewServerReact } from "effect-view-server/react";
import { Schema, Stream } from "effect";
import { strategiesService } from "./grpc-descriptors";

export const Strategy = Schema.Struct({
  id: Schema.String,
  strategyId: Schema.String,
  region: Schema.String,
  status: Schema.Literals(["active", "paused"]),
  notional: Schema.Number,
  updatedAt: Schema.Number,
});

export const grpcClients = {
  strategies: grpc.connectClient({
    service: strategiesService,
    baseUrl: "http://127.0.0.1:4318",
  }),
};

const grpcTopics = grpc.topicSources(grpcClients);

export const viewServer = defineViewServerConfig({
  grpc: {
    clients: grpcClients,
  },
  topics: {
    strategies: grpcTopics.materialized({
      schema: Strategy,
      key: "id",
      client: "strategies",
      method: "streamStrategies",
      request: () => ({ universe: "global" }),
      acquire: () =>
        Stream.make(
          {
            $typeName: "viewserver.example.StrategyValue",
            strategyId: "strategy-alpha",
            region: "usa",
            status: "active",
            notional: 100,
            updatedAt: 1,
          },
          {
            $typeName: "viewserver.example.StrategyValue",
            strategyId: "strategy-beta",
            region: "london",
            status: "paused",
            notional: 75,
            updatedAt: 2,
          },
        ).pipe(Stream.concat(Stream.never)),
      map: ({ value }) => ({
        id: `${value.strategyId}:${value.region}`,
        strategyId: value.strategyId,
        region: value.region,
        status: value.status,
        notional: value.notional,
        updatedAt: value.updatedAt,
      }),
    }),
  },
});

export const viewServerReact = createViewServerReact(viewServer);
export const { ViewServerProvider, useLiveQuery, useViewServerHealthSummary } = viewServerReact;
