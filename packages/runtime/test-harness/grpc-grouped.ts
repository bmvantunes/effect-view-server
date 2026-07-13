import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { Chunk, HashMap, Option, Schema, Stream } from "effect";

import { grpcClients, type GrpcOrderValueMessage, grpcTopicSources } from "./grpc-config";
import { RouteEncodingOrder } from "./grpc-leased";

export class SemanticGroupedKeyClass extends Schema.Class<SemanticGroupedKeyClass>(
  "SemanticGroupedKeyClass",
)({
  value: Schema.String,
}) {}
viewSchema.admitClass(SemanticGroupedKeyClass);

const SemanticGroupedPlainValue = Schema.StringFromUriComponent;

export const SemanticGroupedKeyOrder = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
  semanticClass: SemanticGroupedKeyClass,
  semanticOption: viewSchema.Option(Schema.String),
  semanticChunk: viewSchema.Chunk(Schema.String),
  semanticHashMap: viewSchema.HashMap(Schema.String, Schema.String),
  semanticPlain: SemanticGroupedPlainValue,
  optionalValue: Schema.optionalKey(Schema.Union([Schema.String, Schema.Undefined])),
});

export const semanticGroupedKeyValues = () => ({
  semanticClass: SemanticGroupedKeyClass.make({ value: "class-value" }),
  semanticOption: Option.some("option-value"),
  semanticChunk: Chunk.make("chunk-a", "chunk-b"),
  semanticHashMap: HashMap.make(["alpha", "one"], ["beta", "two"]),
  semanticPlain: "plain-value",
});

export const grpcGroupedKeyEncodingLeasedViewServer = (input: {
  readonly acquire: () => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly map: (value: GrpcOrderValueMessage) => typeof RouteEncodingOrder.Type;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: RouteEncodingOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["text"],
        request: ({ text }) => ({ orderId: text }),
        acquire: input.acquire,
        map: ({ value }) => input.map(value),
      }),
    },
  });

export const grpcSemanticGroupedKeyLeasedViewServer = (input: {
  readonly acquire: () => Stream.Stream<GrpcOrderValueMessage, unknown, never>;
  readonly map: (value: GrpcOrderValueMessage) => typeof SemanticGroupedKeyOrder.Type;
}) =>
  defineViewServerConfig({
    grpc: { clients: grpcClients },
    topics: {
      orders: grpcTopicSources.leased({
        schema: SemanticGroupedKeyOrder,
        key: "id",
        client: "orders",
        method: "streamOrders",
        routeBy: ["text"],
        request: ({ text }) => ({ orderId: text }),
        acquire: input.acquire,
        map: ({ value }) => input.map(value),
      }),
    },
  });
