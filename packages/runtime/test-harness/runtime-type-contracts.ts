import {
  defineViewServerConfig,
  grpc,
  kafka,
  type GrpcConnectClientDefinition,
  type GrpcRuntimeClients,
} from "@effect-view-server/config";
import type { ViewServerAuth } from "@effect-view-server/server";
import { Effect, Schema } from "effect";
import type { Stream } from "effect";
import { makeViewServerRuntime, runViewServerRuntime } from "../src/index";

export const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
});

export declare const grpcRuntimeClients: GrpcRuntimeClients;

export declare const exactGrpcRuntimeClients: {
  readonly ordersClient: GrpcConnectClientDefinition;
};

export declare const grpcRuntimeStream: Stream.Stream<unknown, unknown, never>;

export const grpcTopicSources = grpc.topicSources(grpcRuntimeClients);

export const exactGrpcTopicSources = grpc.topicSources(exactGrpcRuntimeClients);

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

export const leasedViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.leased({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      routeBy: ["id"],
      request: ({ id }) => ({ id }),
      acquire: () => grpcRuntimeStream,
      map: ({ route }) => ({
        id: route.id,
        price: 0,
      }),
    }),
  },
});

export const materializedGrpcViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({}),
      acquire: () => grpcRuntimeStream,
      map: () => ({
        id: "order-1",
        price: 0,
      }),
    }),
  },
});

export const multiMaterializedGrpcViewServer = defineViewServerConfig({
  grpc: {
    clients: grpcRuntimeClients,
  },
  topics: {
    orders: grpcTopicSources.materialized({
      schema: Order,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({}),
      acquire: () => grpcRuntimeStream,
      map: () => ({
        id: "order-1",
        price: 0,
      }),
    }),
    trades: grpcTopicSources.materialized({
      schema: Trade,
      key: "id",
      client: "orders",
      method: "streamOrders",
      request: () => ({}),
      acquire: () => grpcRuntimeStream,
      map: () => ({
        id: "trade-1",
        symbol: "AAPL",
      }),
    }),
  },
});

export const usaKafkaRegions = {
  usa: "localhost:9092",
};

export const kafkaOwnedViewServer = defineViewServerConfig({
  kafka: usaKafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["usa"],
        value: kafka.json(() => Schema.toCodecJson(Order)),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
        }),
      }),
    },
  },
});

export const runtimeEffect = makeViewServerRuntime(viewServer);

export const kafkaOwnedRuntimeEffect = makeViewServerRuntime(kafkaOwnedViewServer, {
  kafka: {
    consumerGroupId: "view-server-kafka-owned-type-test",
  },
});

export const runtimeWithGroupedAdmissionLimits = makeViewServerRuntime(viewServer, {
  groupedIncrementalAdmissionLimits: {
    maxGroups: 1,
  },
});

export const runtimeWithAuth = makeViewServerRuntime(viewServer, {
  auth: {
    validateRequest: () =>
      Effect.succeed({
        forwardedHeaders: {},
        id: null,
        systemHeaders: {},
      }),
  } satisfies ViewServerAuth,
});

export const runEffect = runViewServerRuntime(viewServer);

export declare const runtime: Effect.Success<typeof runtimeEffect>;

export declare const kafkaOwnedRuntime: Effect.Success<typeof kafkaOwnedRuntimeEffect>;

export const materializedGrpcViewServerWithConfigClients = defineViewServerConfig({
  grpc: {
    clients: exactGrpcRuntimeClients,
  },
  topics: {
    orders: exactGrpcTopicSources.materialized({
      schema: Order,
      key: "id",
      client: "ordersClient",
      method: "streamOrders",
      request: () => ({}),
      acquire: () => grpcRuntimeStream,
      map: () => ({
        id: "order-1",
        price: 0,
      }),
    }),
  },
});

export const leasedRuntimeEffect = makeViewServerRuntime(leasedViewServer);

export declare const leasedRuntime: Effect.Success<typeof leasedRuntimeEffect>;

export const materializedGrpcRuntimeWithConfigClientsEffect = makeViewServerRuntime(
  materializedGrpcViewServerWithConfigClients,
);

export declare const materializedGrpcRuntime: Effect.Success<
  typeof materializedGrpcRuntimeWithConfigClientsEffect
>;
