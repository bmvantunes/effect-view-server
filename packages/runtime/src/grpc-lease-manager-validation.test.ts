import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Cause, Effect, Exit, Schema, Stream } from "effect";
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

describe("gRPC lease manager validation", () => {
  it.live("rejects leased gRPC subscriptions without exact route equality", () =>
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
      );

      const error = yield* Effect.flip(
        manager.liveClient.subscribeRuntime("orders", {
          select: ["id"],
          where: {
            region: { startsWith: "u" },
          },
          limit: 10,
        }),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders route field region must use an exact eq filter.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("maps leased gRPC acquisition topic metadata failures to runtime unavailable", () =>
    Effect.gen(function* () {
      const localViewServer = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      Reflect.deleteProperty(localViewServer.topics, "orders");
      const manager = yield* makeViewServerGrpcLeaseManager(
        localViewServer,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const error = yield* manager.liveClient
        .subscribeRuntime("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "gRPC leased feed orders references unknown topic orders",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("accepts decoded transform schema values for leased gRPC routes", () =>
    Effect.gen(function* () {
      const BigIntRouteOrder = Schema.Struct({
        id: Schema.String,
        accountId: Schema.BigIntFromString,
        customerId: Schema.String,
        price: Schema.Number,
      });
      let acquiredRoute: bigint | null = null;
      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          orders: grpcTopicSources.leased({
            schema: BigIntRouteOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["accountId"],
            request: ({ accountId }) => ({ orderId: accountId.toString() }),
            acquire: ({ route }) => {
              acquiredRoute = route.accountId;
              return Stream.never;
            },
            map: ({ value, route }) => ({
              id: `${route.accountId}:${value.customerId}`,
              accountId: route.accountId,
              customerId: value.customerId,
              price: value.price,
            }),
          }),
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

      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        select: ["id", "accountId", "customerId"],
        where: {
          accountId: { eq: 7n },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const snapshot = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect({
        acquiredRoute,
        snapshot,
      }).toStrictEqual({
        acquiredRoute: 7n,
        snapshot: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
        ],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects decoded leased gRPC route values that fail topic schema validation", () =>
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
      );
      const delayedRuntimeQuery = leasedOrdersQuery("usa");
      const subscribeRuntimeEffect = manager.liveClient.subscribeRuntime(
        "orders",
        delayedRuntimeQuery,
      );
      Object.defineProperty(delayedRuntimeQuery.where.region, "eq", {
        value: 123,
      });

      const error = yield* Effect.flip(subscribeRuntimeEffect);

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders route field region value does not match the topic schema.",
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when query shape or route schema is invalid", () =>
    Effect.gen(function* () {
      const localViewServer = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      const missingRouteFieldFeed = yield* Effect.fromNullishOr(grpcOptions.feeds["orders"]);
      Object.defineProperty(localViewServer.topics.orders.grpcSource, "routeBy", {
        value: ["missing"],
      });
      Object.defineProperty(missingRouteFieldFeed, "routeBy", {
        value: ["missing"],
      });
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

      const missingWhereError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const missingFieldQuery = leasedOrdersQuery("usa");
      Object.defineProperty(missingFieldQuery.where, "missing", {
        value: { eq: "usa" },
      });
      const missingFieldError = yield* manager.liveClient
        .subscribeRuntime("orders", missingFieldQuery)
        .pipe(Effect.flip);

      expect({
        missingWhereError,
        missingFieldError,
      }).toStrictEqual({
        missingWhereError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires exact equality filters for route fields: missing.",
        },
        missingFieldError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders route field missing is not in the topic schema.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when topic source metadata is corrupted", () =>
    Effect.gen(function* () {
      const localViewServer = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      Object.defineProperty(localViewServer.topics.orders, "source", {
        value: grpcSourceMarkers.materialized(),
      });
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

      const missingWhereError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const nonExactRouteError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            region: { startsWith: "u" },
          },
          limit: 10,
        })
        .pipe(Effect.flip);

      expect({
        missingWhereError,
        nonExactRouteError,
      }).toStrictEqual({
        missingWhereError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires exact equality filters for route fields: region.",
        },
        nonExactRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders route field region must use an exact eq filter.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when route validation metadata disappears", () =>
    Effect.gen(function* () {
      const localViewServer = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
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
      Object.defineProperty(localViewServer.topics.orders, "grpcSource", {
        value: undefined,
      });

      const missingWhereError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const nonExactRouteError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            region: "usa",
          },
          limit: 10,
        })
        .pipe(Effect.flip);

      expect({
        missingWhereError,
        nonExactRouteError,
      }).toStrictEqual({
        missingWhereError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires exact equality filters for route fields: region.",
        },
        nonExactRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders route field region must use an exact eq filter.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "fails leased gRPC subscription when feed route metadata changes during acquisition",
    () =>
      Effect.gen(function* () {
        const feed = grpcLeasedViewServer({
          streamForRegion: () => Stream.empty,
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        const resolvedFeed = yield* Effect.fromNullishOr(grpcOptions.feeds["orders"]);
        let routeByReads = 0;
        Object.defineProperty(resolvedFeed, "routeBy", {
          get: () => {
            routeByReads += 1;
            return routeByReads <= 2 ? ["region"] : ["region", "status"];
          },
        });
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

        const exit = yield* Effect.exit(
          manager.liveClient.subscribe("orders", leasedOrdersQuery("usa")),
        );

        expect({
          error: Exit.isFailure(exit)
            ? exit.cause.reasons.find(Cause.isFailReason)?.error
            : undefined,
          routeByReads,
        }).toStrictEqual({
          error: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "orders",
            message: "Leased gRPC route is missing configured field status",
          },
          routeByReads: 3,
        });
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

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
      const feed = grpcLeasedViewServerFromCallbacks({
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
      });
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
