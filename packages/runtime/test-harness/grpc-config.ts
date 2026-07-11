import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { defineViewServerConfig, grpc } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Config, Effect, Schema, Stream } from "effect";

import { Order } from "./runtime-config";

export type GrpcOrderValueMessage = Message<"viewserver.runtime.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

export type GrpcOrderKeyMessage = Message<"viewserver.runtime.OrderKey"> & {
  readonly orderId: string;
};

export const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

export const runtimeGrpcProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/runtime.proto",
        package: "viewserver.runtime",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.runtime.OrderKey",
                outputType: ".viewserver.runtime.OrderValue",
                serverStreaming: true,
              },
              {
                name: "GetOrder",
                inputType: ".viewserver.runtime.OrderKey",
                outputType: ".viewserver.runtime.OrderValue",
              },
            ],
          },
        ],
      }),
    ),
  ),
);

export const grpcOrderValueSchema = messageDesc<GrpcOrderValueMessage>(runtimeGrpcProtoFile, 0);

export const grpcOrderKeySchema = messageDesc<GrpcOrderKeyMessage>(runtimeGrpcProtoFile, 1);

export const grpcOrdersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof grpcOrderKeySchema;
    readonly output: typeof grpcOrderValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly getOrder: {
    readonly input: typeof grpcOrderKeySchema;
    readonly output: typeof grpcOrderValueSchema;
    readonly methodKind: "unary";
  };
}>(runtimeGrpcProtoFile, 0);

export const GrpcOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

export const grpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      grpcSource: grpcSourceMarkers.materialized(),
    },
  },
});

export type GrpcTopics = typeof grpcViewServer.topics;

export const grpcAndKafkaViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      grpcSource: grpcSourceMarkers.materialized(),
    },
    audit: {
      schema: Order,
      key: "id",
    },
  },
});

export const grpcClients = {
  orders: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orders.example.test"),
  }),
};

export const grpcClientsWithOrphan = {
  ...grpcClients,
  orphan: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orphan.example.test"),
  }),
};

export const grpcTopicSources = grpc.topicSources(grpcClients);

export const grpcTopicOwnedSourceViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcClients,
  },
  topics: {
    orders: grpcTopicSources.materialized({
      schema: GrpcOrder,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({ orderId: "all-config-owned-orders" }),
      acquire: () => Stream.never,
      release: () => Effect.void,
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      }),
    }),
    routedOrders: grpcTopicSources.leased({
      schema: GrpcOrder,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region"],
      request: ({ region }) => ({ orderId: region }),
      acquire: () => Stream.never,
      release: () => Effect.void,
      map: ({ value, route }) => ({
        id: `${route.region}:${value.customerId}`,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: route.region,
        updatedAt: value.updatedAt,
      }),
    }),
  },
});

export const grpcTopicSourcesWithOrphan = grpc.topicSources(grpcClientsWithOrphan);

export const grpcOrderValue = (
  customerId: string,
  price: number,
  status: GrpcOrderValueMessage["status"] = "open",
): GrpcOrderValueMessage => ({
  $typeName: "viewserver.runtime.OrderValue",
  customerId,
  status,
  price,
  updatedAt: price,
});
