import { describe, expect, it } from "@effect/vitest";
import { strategiesService, strategyRequestSchema, strategyValueSchema } from "./grpc-descriptors";
import { viewServer } from "./view-server.config";

describe("materialized gRPC React example topic-owned source", () => {
  it("constructs descriptors and maps materialized values", () => {
    const source = viewServer.topics.strategies.source;
    const request = source.options.request();
    const values = [
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
    ] as const;
    const rows = Array.from(values, (value) => source.options.mapValue(value));

    expect({
      descriptors: {
        valueTypeName: strategyValueSchema.typeName,
        valueFields: strategyValueSchema.fields.map((field) => ({
          name: field.name,
          localName: field.localName,
        })),
        requestTypeName: strategyRequestSchema.typeName,
        requestFields: strategyRequestSchema.fields.map((field) => ({
          name: field.name,
          localName: field.localName,
        })),
        serviceTypeName: strategiesService.typeName,
        method: {
          name: strategiesService.method.streamStrategies.name,
          localName: strategiesService.method.streamStrategies.localName,
          methodKind: strategiesService.method.streamStrategies.methodKind,
          input: strategiesService.method.streamStrategies.input.typeName,
          output: strategiesService.method.streamStrategies.output.typeName,
        },
      },
      source: {
        lifecycle: source.lifecycle,
        client: source.options.client,
        method: source.options.method,
        request,
      },
      rows,
    }).toStrictEqual({
      descriptors: {
        valueTypeName: "viewserver.example.StrategyValue",
        valueFields: [
          { name: "strategy_id", localName: "strategyId" },
          { name: "region", localName: "region" },
          { name: "status", localName: "status" },
          { name: "notional", localName: "notional" },
          { name: "updated_at", localName: "updatedAt" },
        ],
        requestTypeName: "viewserver.example.StrategyRequest",
        requestFields: [{ name: "universe", localName: "universe" }],
        serviceTypeName: "viewserver.example.StrategiesService",
        method: {
          name: "StreamStrategies",
          localName: "streamStrategies",
          methodKind: "server_streaming",
          input: "viewserver.example.StrategyRequest",
          output: "viewserver.example.StrategyValue",
        },
      },
      source: {
        lifecycle: "materialized",
        client: "strategies",
        method: "streamStrategies",
        request: { universe: "global" },
      },
      rows: [
        {
          id: "strategy-alpha:usa",
          strategyId: "strategy-alpha",
          region: "usa",
          status: "active",
          notional: 100,
          updatedAt: 1,
        },
        {
          id: "strategy-beta:london",
          strategyId: "strategy-beta",
          region: "london",
          status: "paused",
          notional: 75,
          updatedAt: 2,
        },
      ],
    });
  });
});
