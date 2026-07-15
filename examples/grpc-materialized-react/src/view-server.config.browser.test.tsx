import type { Client } from "@connectrpc/connect";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { strategiesService, strategyRequestSchema, strategyValueSchema } from "./grpc-descriptors";
import { Strategy, viewServer } from "./view-server.config";

const client = {
  streamStrategies: async function* streamStrategies() {},
} satisfies Client<typeof strategiesService>;

describe("materialized gRPC React example topic-owned source", () => {
  it.effect("constructs descriptors and maps the materialized stream", () =>
    Effect.gen(function* () {
      const source = viewServer.topics.strategies.grpcSource;
      const request = source.request();
      const values = yield* source
        .acquire({
          client,
          request,
          route: undefined,
          session: {
            id: null,
            forwardedHeaders: {},
            systemHeaders: {},
          },
        })
        .pipe(Stream.take(2), Stream.runCollect);
      const rows = Array.from(values, (value) =>
        source.map({
          value,
          route: undefined,
          schema: Strategy,
        }),
      );

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
          client: source.client,
          method: source.method,
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
    }),
  );
});
