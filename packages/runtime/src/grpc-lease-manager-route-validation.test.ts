import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schema, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { resolveGrpcRuntimeSourceOptions as resolveViewServerRuntimeOptions } from "./grpc-runtime-source";

import { grpcClients, grpcTopicSources } from "../test-harness/grpc-config";
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
  it.live("rejects routeBy on ordinary topics before the manager strips routing", () =>
    Effect.gen(function* () {
      const leased = grpcLeasedViewServer({ streamForRegion: () => Stream.never });
      const sourceConfig = {
        ...leased,
        topics: {
          ...leased.topics,
          positions: {
            schema: leased.topics.orders.schema,
            key: leased.topics.orders.key,
          },
        },
      };
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(sourceConfig);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof sourceConfig.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscribe: Effect.Effect<unknown, ViewServerRuntimeError | ViewServerTransportError> =
        Reflect.apply(manager.liveClient.subscribeRuntime, manager.liveClient, [
          "positions",
          { routeBy: { region: "AbÇ" }, select: ["id"] },
        ]);

      expect(yield* Effect.flip(subscribe)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "positions",
        message: "Topic positions does not accept routeBy.",
      });

      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects leased gRPC subscriptions without routeBy", () =>
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
        // @ts-expect-error runtime validation deliberately exercises a missing leased route.
        manager.liveClient.subscribeRuntime("orders", {
          select: ["id"],
          where: [{ field: "region", type: "startsWith", filter: "u" }],
          limit: 10,
        }),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message: "Leased topic orders requires routeBy fields: region.",
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
        Effect.flatMap(Effect.fromNullishOr),
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
        routeBy: { accountId: 7n },
        select: ["id", "accountId", "customerId"],
        where: [{ field: "accountId", type: "equals", filter: 7n }],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const snapshot = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const encodedRouteQuery: object = {
        routeBy: { accountId: "7" },
        select: ["id", "accountId"],
        where: [{ field: "accountId", type: "equals", filter: 7n }],
        limit: 10,
      };
      const encodedRouteError = yield* Effect.flip(
        // @ts-expect-error hostile runtime route values are rejected.
        manager.liveClient.subscribeRuntime("orders", encodedRouteQuery),
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

  it.live("frames lone-surrogate topic, feed, and route identities without defects", () =>
    Effect.gen(function* () {
      const identityName = "\ud800";
      const LoneSurrogateRouteOrder = Schema.Struct({
        id: Schema.String,
        [identityName]: Schema.String,
        customerId: Schema.String,
        price: Schema.Number,
      });
      let acquiredRoute: string | undefined;
      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          [identityName]: grpcTopicSources.leased({
            schema: LoneSurrogateRouteOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: [identityName],
            request: (route) => ({ orderId: route[identityName] }),
            acquire: ({ route }) => {
              acquiredRoute = route[identityName];
              return Stream.never;
            },
            map: ({ value, route }) => ({
              id: `${route[identityName]}:${value.customerId}`,
              [identityName]: route[identityName],
              customerId: value.customerId,
              price: value.price,
            }),
          }),
        },
      });
      const grpcOptions = yield* resolveViewServerRuntimeOptions(localViewServer).pipe(
        Effect.flatMap(Effect.fromNullishOr),
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

      const subscription = yield* manager.liveClient.subscribeRuntime(identityName, {
        routeBy: { [identityName]: "route" },
        select: ["id", identityName],
        where: [{ field: identityName, type: "equals", filter: "route" }],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });

      expect(acquiredRoute).toBe("route");
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("captures decoded leased gRPC route values before subscribeRuntime returns", () =>
    Effect.gen(function* () {
      let acquiredRegion: string | null = null;
      let requestedRegion: string | null = null;
      const feed = grpcLeasedViewServer({
        acquired: (region) => {
          acquiredRegion = region;
        },
        requested: (region) => {
          requestedRegion = region;
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
      const delayedRuntimeQuery = leasedOrdersQuery("AbÇDEfgh");
      const subscribeRuntimeEffect = manager.liveClient.subscribeRuntime(
        "orders",
        delayedRuntimeQuery,
      );
      Object.defineProperty(delayedRuntimeQuery.routeBy, "region", { value: "changed" });

      const subscription = yield* subscribeRuntimeEffect;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect({ acquiredRegion, requestedRegion, events }).toStrictEqual({
        acquiredRegion: "AbÇDEfgh",
        requestedRegion: "AbÇDEfgh",
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

      const missingRouteError = yield* manager.liveClient
        // @ts-expect-error runtime validation deliberately exercises a missing leased route.
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const missingFieldQuery = leasedOrdersQuery("usa");
      const missingFieldError = yield* manager.liveClient
        .subscribeRuntime("orders", missingFieldQuery)
        .pipe(Effect.flip);

      expect({
        missingRouteError,
        missingFieldError,
      }).toStrictEqual({
        missingRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "Leased topic orders route field missing is not in the topic schema.",
        },
        missingFieldError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "Leased topic orders route field missing is not in the topic schema.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects corrupted leased gRPC source metadata before runtime construction", () =>
    Effect.gen(function* () {
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
        }),
      );
      Object.defineProperty(localViewServer.topics.orders, "source", {
        value: grpcSourceMarkers.materialized(),
      });

      expect(
        yield* makeViewServerRuntimeCoreInternal(localViewServer, {}).pipe(Effect.flip),
      ).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "Source-owned Topic orders has an invalid Source Definition envelope.",
      });
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

      const missingRouteError = yield* manager.liveClient
        // @ts-expect-error runtime validation deliberately exercises a missing leased route.
        .subscribeRuntime("orders", {
          select: ["id"],
          limit: 10,
        })
        .pipe(Effect.flip);
      const filteredWithoutRouteError = yield* manager.liveClient
        // @ts-expect-error runtime validation proves local filters cannot replace a leased route.
        .subscribeRuntime("orders", {
          select: ["id"],
          where: [{ field: "region", type: "equals", filter: "usa" }],
          limit: 10,
        })
        .pipe(Effect.flip);

      expect({
        missingRouteError,
        filteredWithoutRouteError,
      }).toStrictEqual({
        missingRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires routeBy fields: region.",
        },
        filteredWithoutRouteError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Leased topic orders requires routeBy fields: region.",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("snapshots leased gRPC route metadata once for the identity contract", () =>
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
          return routeByReads === 1 ? ["region"] : ["region", "status"];
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));

      expect(routeByReads).toBe(1);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
