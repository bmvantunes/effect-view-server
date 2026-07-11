import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { makeViewServerRuntime } from "./index";
import { resolveViewServerRuntimeOptions, validateSourceOwnership } from "./runtime-options";
import { order, Order, viewServer } from "../test-harness/runtime-config";
import {
  grpcClients,
  GrpcOrder,
  grpcTopicOwnedSourceViewServer,
} from "../test-harness/grpc-config";
import {
  grpcMaterializedViewServer,
  makeGrpcHealth,
  resolveGrpcRuntimeOptions,
  resolveLeasedGrpcRuntimeOptions,
} from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
  leasedOrdersQuery,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";

describe("Runtime source ownership and mutation policy", () => {
  it.live("rejects public one-shot snapshots for leased gRPC topics", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<
        typeof grpcTopicOwnedSourceViewServer.topics
      >;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
      );

      const snapshot: (
        topic: string,
        query: unknown,
      ) => Effect.Effect<unknown, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "snapshot",
      )?.value;
      expect(typeof snapshot).toBe("function");
      const error = yield* snapshot("routedOrders", leasedOrdersQuery("usa")).pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "routedOrders",
        message:
          "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
      });
      yield* runtime.close;
    }),
  );

  it.live("rejects direct runtime mutations for leased gRPC topics", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<
        typeof grpcTopicOwnedSourceViewServer.topics
      >;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof grpcTopicOwnedSourceViewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcTopicOwnedSourceViewServer,
      );
      const publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "publish")?.value;
      const publishMany: (
        topic: string,
        rows: ReadonlyArray<unknown>,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "publishMany",
      )?.value;
      const patch: (
        topic: string,
        key: string,
        patchValue: unknown,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "patch",
      )?.value;
      const deleteRow: (topic: string, key: string) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "delete")?.value;
      expect(typeof publish).toBe("function");
      expect(typeof publishMany).toBe("function");
      expect(typeof patch).toBe("function");
      expect(typeof deleteRow).toBe("function");
      const row = {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 10,
        region: "usa",
        updatedAt: 10,
      };

      const publishError = yield* publish("routedOrders", row).pipe(Effect.flip);
      const publishManyError = yield* publishMany("routedOrders", [row]).pipe(Effect.flip);
      const patchError = yield* patch("routedOrders", "order-1", { price: 11 }).pipe(Effect.flip);
      const deleteError = yield* deleteRow("routedOrders", "order-1").pipe(Effect.flip);

      expect([publishError, publishManyError, patchError, deleteError]).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "routedOrders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "routedOrders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "routedOrders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "routedOrders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("delegates public runtime reset when no topic is source-owned", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        websocketPort: 0,
      });

      yield* runtime.client.publish("orders", order("order-1", 10));
      yield* runtime.client.reset();
      const snapshot = yield* runtime.client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      expect(snapshot).toStrictEqual({
        version: 0,
        rows: [],
        totalRows: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.live(
    "delegates direct manager reset when gRPC is configured without source-owned topics",
    () =>
      Effect.gen(function* () {
        const sourceFreeGrpcViewServer = defineViewServerConfig({
          grpc: { clients: grpcClients },
          topics: {
            orders: {
              schema: Order,
              key: "id",
            },
          },
        });
        const options = yield* resolveViewServerRuntimeOptions(sourceFreeGrpcViewServer, {
          grpc: {},
        });
        const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(sourceFreeGrpcViewServer, {});
        const health = makeDefaultRuntimeDependencies<
          typeof sourceFreeGrpcViewServer.topics
        >().makeGrpcHealthLedger(sourceFreeGrpcViewServer, grpcOptions);
        const manager = yield* makeViewServerGrpcLeaseManager(
          sourceFreeGrpcViewServer,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          health,
        );

        yield* manager.client.publish("orders", order("order-1", 10));
        yield* manager.client.reset();
        const snapshot = yield* manager.client.snapshot("orders", {
          select: ["id", "price"],
          limit: 10,
        });

        expect(snapshot).toStrictEqual({
          version: 0,
          rows: [],
          totalRows: 0,
          status: "ready",
          statusCode: "Ready",
        });
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("rejects public runtime mutations for topic-owned Kafka topics", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const kafkaBackedViewServer = defineViewServerConfig({
        kafka: regions,
        topics: {
          orders: {
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
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof kafkaBackedViewServer.topics>;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof kafkaBackedViewServer.topics>(),
        makeKafkaIngress: () =>
          Effect.succeed({
            close: Effect.void,
          }),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Effect.void,
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        kafkaBackedViewServer,
        {
          kafka: {
            consumerGroupId: "view-server-source-owned-public-client",
          },
        },
      );
      const row = {
        id: "order-1",
        price: 10,
      };
      const publish: (topic: string, row: unknown) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "publish")?.value;
      const publishMany: (
        topic: string,
        rows: ReadonlyArray<unknown>,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "publishMany",
      )?.value;
      const patch: (
        topic: string,
        key: string,
        patchValue: unknown,
      ) => Effect.Effect<void, ViewServerRuntimeError> = Object.getOwnPropertyDescriptor(
        runtime.client,
        "patch",
      )?.value;
      const deleteRow: (topic: string, key: string) => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "delete")?.value;
      const reset: () => Effect.Effect<void, ViewServerRuntimeError> =
        Object.getOwnPropertyDescriptor(runtime.client, "reset")?.value;
      expect(typeof publish).toBe("function");
      expect(typeof publishMany).toBe("function");
      expect(typeof patch).toBe("function");
      expect(typeof deleteRow).toBe("function");
      expect(typeof reset).toBe("function");

      const publishError = yield* publish("orders", row).pipe(Effect.flip);
      const publishManyError = yield* publishMany("orders", [row]).pipe(Effect.flip);
      const patchError = yield* patch("orders", "order-1", { price: 11 }).pipe(Effect.flip);
      const deleteError = yield* deleteRow("orders", "order-1").pipe(Effect.flip);
      const resetError = yield* reset().pipe(Effect.flip);

      expect([publishError, publishManyError, patchError, deleteError, resetError]).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          message:
            "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
        },
      ]);
      yield* runtime.close;
    }),
  );

  it.live("rejects direct manager mutations for materialized gRPC source-owned topics", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(feed, {});
      const health = makeGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        feed,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const row: typeof GrpcOrder.Type = {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 10,
        region: "usa",
        updatedAt: 10,
      };
      const publishError = yield* manager.client.publish("orders", row).pipe(Effect.flip);
      const publishManyError = yield* manager.client.publishMany("orders", [row]).pipe(Effect.flip);
      const patchError = yield* manager.client
        .patch("orders", "order-1", { price: 11 })
        .pipe(Effect.flip);
      const deleteError = yield* manager.client.delete("orders", "order-1").pipe(Effect.flip);
      const resetError = yield* manager.client.reset().pipe(Effect.flip);
      const sourceOwnedMutationError = {
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        topic: "orders",
        message:
          "Source-owned topics do not support direct runtime mutations; publish through the configured Kafka/gRPC source or use an externally-published topic.",
      };
      const sourceOwnedResetError = {
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        message:
          "Source-owned topics do not support direct runtime reset; close the runtime or reset source-free topics through their owner.",
      };

      expect([publishError, publishManyError, patchError, deleteError, resetError]).toStrictEqual([
        sourceOwnedMutationError,
        sourceOwnedMutationError,
        sourceOwnedMutationError,
        sourceOwnedMutationError,
        sourceOwnedResetError,
      ]);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects direct manager reset when leased gRPC topics exist", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(feed, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        feed,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const leasedRow: typeof GrpcOrder.Type = {
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 10,
        region: "usa",
        updatedAt: 10,
      };
      const publishError = yield* manager.client.publish("orders", leasedRow).pipe(Effect.flip);
      const publishManyError = yield* manager.client
        .publishMany("orders", [leasedRow])
        .pipe(Effect.flip);
      const patchError = yield* manager.client
        .patch("orders", "order-1", { price: 11 })
        .pipe(Effect.flip);
      const deleteError = yield* manager.client.delete("orders", "order-1").pipe(Effect.flip);
      const resetError = yield* manager.client.reset().pipe(Effect.flip);

      expect([publishError, publishManyError, patchError, deleteError]).toStrictEqual([
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
        {
          _tag: "ViewServerRuntimeError",
          code: "UnsupportedQuery",
          topic: "orders",
          message:
            "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
        },
      ]);
      expect(resetError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "UnsupportedQuery",
        message:
          "Leased gRPC topics do not support direct runtime reset; close the runtime or leased subscriptions so the lease manager owns cleanup.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("rejects resolved Kafka and gRPC ownership of the same View Server topic", () =>
    Effect.gen(function* () {
      const error = yield* validateSourceOwnership(
        {
          topics: {
            "orders-source": {
              viewServerTopic: "orders",
            },
          },
        },
        {
          feeds: {
            orders: {
              topic: "orders",
            },
          },
        },
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "View Server topic orders cannot be owned by both Kafka source orders-source and gRPC feed orders.",
      );
    }),
  );
});
