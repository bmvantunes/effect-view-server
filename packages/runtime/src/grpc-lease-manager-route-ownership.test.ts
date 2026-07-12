import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Schema, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";

import { grpcClients, grpcOrderValue, grpcTopicSources } from "../test-harness/grpc-config";
import { makeLeasedGrpcHealth } from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

type RouteView = {
  readonly region: {
    readonly name: string;
  };
};

type RouteOwnershipGroupedQuery = {
  groupBy: ["region"];
  aggregates: {
    rowCount: {
      aggFunc: "count";
    };
  };
  where: {
    region: {
      eq: {
        name: string;
      };
    };
  };
  orderBy: [
    {
      aggregate: "rowCount";
      direction: "desc";
    },
  ];
  limit: 10;
};

type RouteOwnershipRawQuery = {
  readonly select: readonly ["id"];
  readonly where: {
    readonly region: {
      readonly eq: {
        readonly name: string;
      };
    };
  };
};

const RouteOwnershipOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.Struct({
    name: Schema.Trim,
  }),
  updatedAt: Schema.Number,
});

const routeGroupKey = JSON.stringify([
  ["region", JSON.stringify(["present", JSON.stringify({ name: "usa" })])],
]);

describe("gRPC lease manager route ownership", () => {
  it.live("owns the query and gives every source callback a fresh schema-materialized route", () =>
    Effect.gen(function* () {
      const queryRoute = { name: "usa" };
      const query: RouteOwnershipGroupedQuery = {
        groupBy: ["region"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: queryRoute },
        },
        orderBy: [{ aggregate: "rowCount", direction: "desc" }],
        limit: 10,
      };
      const requestRoutes: Array<RouteView> = [];
      const acquireRoutes: Array<RouteView> = [];
      const mapRoutes: Array<RouteView> = [];
      const releaseRoutes: Array<RouteView> = [];
      const requestRegionNames: Array<string> = [];
      const acquireRegionNames: Array<string> = [];
      const mapRegionNames: Array<string> = [];
      const releaseRegionNames: Array<string> = [];

      const localViewServer = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          orders: grpcTopicSources.leased({
            schema: RouteOwnershipOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["region"],
            request: (route) => {
              requestRoutes.push(route);
              requestRegionNames.push(route.region.name);
              Reflect.set(route.region, "name", "request-route-mutation");
              Reflect.set(query.where.region.eq, "name", "request-query-mutation");
              Reflect.set(query.groupBy, 0, "customerId");
              Reflect.set(query.aggregates.rowCount, "aggFunc", "sum");
              Reflect.set(query.aggregates.rowCount, "field", "price");
              Reflect.set(query.orderBy[0], "aggregate", "changedTotal");
              return { orderId: "owned-route" };
            },
            acquire: ({ route }) => {
              acquireRoutes.push(route);
              acquireRegionNames.push(route.region.name);
              Reflect.set(route.region, "name", "acquire-route-mutation");
              return Stream.make(grpcOrderValue("order-1", 10), grpcOrderValue("order-2", 20)).pipe(
                Stream.concat(Stream.never),
              );
            },
            release: ({ route }) => {
              releaseRoutes.push(route);
              releaseRegionNames.push(route.region.name);
              Reflect.set(route.region, "name", "release-route-mutation");
              return Effect.void;
            },
            map: ({ value, route }) => {
              mapRoutes.push(route);
              const regionName = route.region.name;
              mapRegionNames.push(regionName);
              Reflect.set(route.region, "name", "map-route-mutation");
              return {
                id: `${regionName}:${value.customerId}`,
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region: { name: regionName },
                updatedAt: value.updatedAt,
              };
            },
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

      const accessorQuery: RouteOwnershipRawQuery = {
        select: ["id"],
        where: {
          region: { eq: { name: "usa" } },
        },
      };
      Object.defineProperty(accessorQuery, "select", {
        enumerable: true,
        get: () => ["id"],
      });
      const publicSnapshotError = yield* manager.liveClient
        .subscribe("orders", accessorQuery)
        .pipe(Effect.flip);
      const runtimeSnapshotError = yield* manager.liveClient
        .subscribeRuntime("orders", accessorQuery)
        .pipe(Effect.flip);
      const expectedSnapshotError = {
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message:
          "Leased gRPC query could not be snapshotted before acquisition: TypeError: Leased gRPC query fields must be own data properties.",
      };
      expect(publicSnapshotError).toStrictEqual(expectedSnapshotError);
      expect(runtimeSnapshotError).toStrictEqual(expectedSnapshotError);

      const encodedRouteError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          groupBy: ["region"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: { eq: { name: "  usa  " } },
          },
          limit: 10,
        })
        .pipe(Effect.flip);
      expect(encodedRouteError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message:
          "Leased topic orders route field region value does not match the topic schema or cannot be used as a stable leased gRPC route key.",
      });

      const invalidRouteValue = { name: "invalid" };
      Reflect.set(invalidRouteValue, "name", 123);
      const invalidRouteError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          groupBy: ["region"],
          aggregates: {
            rowCount: { aggFunc: "count" },
          },
          where: {
            region: { eq: invalidRouteValue },
          },
          limit: 10,
        })
        .pipe(Effect.flip);
      expect(invalidRouteError).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "orders",
        message:
          "Leased topic orders route field region value does not match the topic schema or cannot be used as a stable leased gRPC route key.",
      });

      const subscriptionEffect = manager.liveClient.subscribe("orders", query);
      Reflect.set(queryRoute, "name", "caller-after-subscribe-call");
      Reflect.set(query.groupBy, 0, "status");
      Reflect.set(query.aggregates.rowCount, "aggFunc", "sum");
      Reflect.set(query.aggregates.rowCount, "field", "price");
      Reflect.set(query.orderBy[0], "aggregate", "callerTotal");
      const subscription = yield* subscriptionEffect;
      Reflect.set(queryRoute, "name", "caller-after-subscription-acquire");
      Reflect.set(query.groupBy, 0, "customerId");
      Reflect.set(query.aggregates.rowCount, "aggFunc", "avg");
      Reflect.set(query.orderBy[0], "aggregate", "afterAcquireTotal");
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        {
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [
            {
              type: "insert",
              key: routeGroupKey,
              row: {
                region: { name: "usa" },
                rowCount: 2n,
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* subscription.close();

      expect({
        requestRegionNames,
        acquireRegionNames,
        mapRegionNames,
        releaseRegionNames,
      }).toStrictEqual({
        requestRegionNames: ["usa"],
        acquireRegionNames: ["usa"],
        mapRegionNames: ["usa", "usa"],
        releaseRegionNames: ["usa"],
      });
      expect(requestRoutes).toHaveLength(1);
      expect(acquireRoutes).toHaveLength(1);
      expect(mapRoutes).toHaveLength(2);
      expect(releaseRoutes).toHaveLength(1);
      expect(requestRoutes[0]?.region).not.toBe(queryRoute);
      expect(requestRoutes[0]).not.toBe(acquireRoutes[0]);
      expect(requestRoutes[0]?.region).not.toBe(acquireRoutes[0]?.region);
      expect(acquireRoutes[0]).not.toBe(mapRoutes[0]);
      expect(acquireRoutes[0]?.region).not.toBe(mapRoutes[0]?.region);
      expect(mapRoutes[0]).not.toBe(mapRoutes[1]);
      expect(mapRoutes[0]?.region).not.toBe(mapRoutes[1]?.region);
      expect(releaseRoutes[0]).not.toBe(requestRoutes[0]);
      expect(releaseRoutes[0]).not.toBe(acquireRoutes[0]);
      expect(releaseRoutes[0]).not.toBe(mapRoutes[0]);
      expect(releaseRoutes[0]).not.toBe(mapRoutes[1]);
      expect(releaseRoutes[0]?.region).not.toBe(mapRoutes[1]?.region);

      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
