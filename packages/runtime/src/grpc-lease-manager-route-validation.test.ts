import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Cause, Effect, Exit, Fiber, Queue, Schema, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { resolveViewServerRuntimeOptions } from "./runtime-options";

import {
  grpcClients,
  grpcOrderValue,
  grpcTopicSources,
  type GrpcOrderValueMessage,
} from "../test-harness/grpc-config";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
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

describe("gRPC lease manager route validation", () => {
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
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
        }),
      );
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
      const encodedRouteError = yield* Effect.flip(
        manager.liveClient.subscribeRuntime("orders", {
          select: ["id", "accountId"],
          where: {
            // @ts-expect-error Runtime validation also protects untyped encoded route values.
            accountId: { eq: "7" },
          },
          limit: 10,
        }),
      );

      expect({
        acquiredRoute,
        encodedRouteError,
        snapshot,
      }).toStrictEqual({
        acquiredRoute: 7n,
        encodedRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message:
            "Leased topic orders route field accountId value does not match the topic schema or cannot be used as a stable leased gRPC route key.",
        },
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

  it.live("captures decoded leased gRPC route values before subscribeRuntime returns", () =>
    Effect.gen(function* () {
      let acquiredRegion: string | null = null;
      const feed = grpcLeasedViewServer({
        acquired: (region) => {
          acquiredRegion = region;
        },
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

      const subscription = yield* subscribeRuntimeEffect;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect({ acquiredRegion, events }).toStrictEqual({
        acquiredRegion: "usa",
        events: [
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

  it.live("fails a leased gRPC feed when a mapped route value has no canonical identity", () =>
    Effect.gen(function* () {
      const UnknownRouteOrder = Schema.Struct({
        id: Schema.String,
        route: Schema.Unknown,
      });
      const upstream = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          orders: grpcTopicSources.leased({
            schema: UnknownRouteOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["route"],
            request: () => ({ orderId: "stable-route" }),
            acquire: () => Stream.fromQueue(upstream),
            map: ({ value }) => ({
              id: value.customerId,
              route: new Map([["key", "value"]]),
            }),
          }),
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
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
        select: ["id", "route"],
        where: {
          route: { eq: "stable-route" },
        },
        limit: 10,
      });
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const initial = yield* Queue.take(eventQueue);
      yield* Queue.offer(upstream, grpcOrderValue("bad-route", 10));
      const terminal = yield* Queue.take(eventQueue);

      expect(initial).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(terminal).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream failed.",
      });
      yield* subscription.close();
      yield* Fiber.interrupt(eventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC route extraction when query shape or route schema is invalid", () =>
    Effect.gen(function* () {
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
        }),
      );
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
        enumerable: true,
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
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
        }),
      );
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
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
        }),
      );
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
});
