import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { Effect, Exit, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import { Order } from "../test-harness/runtime-config";
import {
  grpcClients,
  GrpcOrder,
  grpcTopicOwnedSourceViewServer,
  grpcTopicSources,
} from "../test-harness/grpc-config";

describe("Runtime startup rollback", () => {
  it.live("closes started resources when gRPC ingress startup fails", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const mixedSourceViewServer = defineViewServerConfig({
        kafka: regions,
        grpc: {
          clients: grpcClients,
        },
        topics: {
          orders: grpcTopicSources.materialized({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "all" }),
            acquire: () => Stream.never,
            map: ({ value }) => ({
              id: value.customerId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: "usa",
              updatedAt: value.updatedAt,
            }),
          }),
          audit: {
            schema: Order,
            key: "id",
            kafkaSource: kafka.source({
              topic: "orders-source",
              regions: ["local"],
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
      type MixedTopics = typeof mixedSourceViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<MixedTopics>;
      let serverClosed = 0;
      let kafkaClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<MixedTopics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<MixedTopics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.succeed({
            close: Effect.sync(() => {
              kafkaClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed during startup",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, mixedSourceViewServer, {
          kafka: {
            consumerGroupId: "view-server-grpc-startup-failure",
          },
        }),
      );

      expect({
        failed: Exit.isFailure(exit),
        kafkaClosed,
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        kafkaClosed: 1,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );

  it.live("closes server and runtime core when gRPC ingress startup fails without Kafka", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<
        typeof grpcTopicOwnedSourceViewServer.topics
      >;
      let serverClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed before Kafka existed",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, grpcTopicOwnedSourceViewServer),
      );

      expect({
        failed: Exit.isFailure(exit),
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );
});
