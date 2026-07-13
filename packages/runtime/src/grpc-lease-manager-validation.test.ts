import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import { nullRecord } from "../test-harness/runtime";

import { order, Order } from "../test-harness/runtime-config";
import { grpcClients, GrpcOrder, grpcTopicSources } from "../test-harness/grpc-config";
import {
  fastGrpcMaterializedReconnect,
  resolveLeasedGrpcRuntimeOptions,
} from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
  grpcLeasedViewServerFromCallbacks,
  leasedGrpcViewServer,
  leasedOrdersQuery,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";

const cloneWithMutableOrdersGrpcSource = <
  const Config extends {
    readonly topics: {
      readonly orders: {
        readonly grpcSource: object;
      };
    };
  },
>(
  config: Config,
) => ({
  ...config,
  topics: {
    ...config.topics,
    orders: {
      ...config.topics.orders,
      grpcSource: { ...config.topics.orders.grpcSource },
    },
  },
});

describe("gRPC lease manager validation", () => {
  it.live("fails leased gRPC subscription when acquire fails before returning a stream", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        streamForRegion: () => {
          throw new Error("leased acquire exploded");
        },
        release: Effect.sync(() => {
          released += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        error,
        released,
        leasedFeeds: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        error: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "gRPC leased feed acquire failed for orders",
        },
        released: 1,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when client creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
        () => {
          throw new Error("client exploded");
        },
      );

      const error = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased client creation failed for orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when request creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServerFromCallbacks({
        request: () => {
          throw new Error("request exploded");
        },
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed request creation failed for orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when acquire does not return a Stream", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServerFromCallbacks({
          request: ({ region }) => ({ orderId: region }),
          acquire: () => Stream.never,
          release: () =>
            Effect.sync(() => {
              released += 1;
            }),
          map: ({ value, route }) => ({
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        }),
      );
      Object.defineProperty(feed.topics.orders.grpcSource, "acquire", {
        value: () => "not-a-stream",
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        error,
        released,
        leasedFeeds: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        error: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "gRPC leased feed acquire did not return a Stream for orders",
        },
        released: 1,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when the client configuration is missing", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const resolvedOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const grpcOptions = {
        ...resolvedOptions,
        clients: {},
        clientBaseUrls: {},
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        resolvedOptions.sourceConfig,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {},
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        resolvedOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed orders references missing client: orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails leased gRPC subscription when the client URL is unresolved", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const resolvedOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const grpcOptions = {
        ...resolvedOptions,
        clientBaseUrls: {},
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(
        resolvedOptions.sourceConfig,
        {},
      );
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {},
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        resolvedOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed orders references unresolved client URL: orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC topics when no leased feed is configured", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        leasedGrpcViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        {
          clients: grpcClients,
          clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
          feeds: {},
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
        health,
      );

      const error = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "Leased gRPC topic orders has no configured leased feed.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("delegates non-leased topics through the gRPC lease manager", () =>
    Effect.gen(function* () {
      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          orders: grpcTopicSources.leased({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["region"],
            request: ({ region }) => ({ orderId: region }),
            acquire: () => Stream.never,
            map: ({ value, route }) => ({
              id: `${route.region}:${value.customerId}`,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region: route.region,
              updatedAt: value.updatedAt,
            }),
          }),
          audit: {
            schema: Order,
            key: "id",
          },
        },
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions(localViewServer).pipe(
        Effect.flatMap((options) => Effect.fromNullishOr(options.grpcOptions)),
      );
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* manager.client.publish("audit", order("audit-1", 42));
      yield* manager.client.publishMany("audit", [order("audit-2", 5)]);
      yield* manager.client.patch("audit", "audit-2", { price: 7 });
      yield* manager.client.delete("audit", "audit-1");
      const snapshot = yield* manager.client.snapshot("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const subscription = yield* manager.liveClient.subscribe("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const runtimeSubscription = yield* manager.liveClient.subscribeRuntime("audit", {
        select: ["id", "price"],
        limit: 10,
      });
      const event = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const runtimeEvent = yield* runtimeSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect({
        snapshot,
        event: Array.from(event),
        runtimeEvent: Array.from(runtimeEvent),
      }).toStrictEqual({
        snapshot: {
          version: 4,
          rows: [{ id: "audit-2", price: 7 }],
          totalRows: 1,
          status: "ready",
          statusCode: "Ready",
        },
        event: [
          {
            type: "snapshot",
            topic: "audit",
            queryId: "query-0",
            version: 4,
            keys: ["audit-2"],
            rows: [{ id: "audit-2", price: 7 }],
            totalRows: 1,
          },
        ],
        runtimeEvent: [
          {
            type: "snapshot",
            topic: "audit",
            queryId: "query-1",
            version: 4,
            keys: ["audit-2"],
            rows: [{ id: "audit-2", price: 7 }],
            totalRows: 1,
          },
        ],
      });
      yield* runtimeSubscription.close();
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
