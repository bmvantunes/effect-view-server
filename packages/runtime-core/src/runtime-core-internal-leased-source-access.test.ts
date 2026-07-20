import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { grpcSourceMarkers } from "@effect-view-server/config/internal";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { Order, order, publicLeasedRuntimeAccessError } from "./runtime-core-test-fixtures";

const leasedViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpcSourceMarkers.leased({ routeBy: ["region", "status"] }),
    },
  },
});

describe("@effect-view-server/runtime-core", () => {
  it.effect("allows internal runtime core access for leased gRPC manager internals", () =>
    Effect.acquireUseRelease(
      makeViewServerRuntimeCoreInternal(leasedViewServer, {}),
      (runtimeCore) =>
        Effect.gen(function* () {
          yield* runtimeCore.internalClient.publish("orders", order("a", 10));
          yield* runtimeCore.internalClient.publishManyWithStorageKeys("orders", [
            {
              storageKey: "orders/lease/row/public-b",
              row: order("public-b", 20),
            },
          ]);

          const snapshot = yield* runtimeCore.internalClient.snapshotRuntimeInternal("orders", {
            where: [
              { field: "customerId", type: "equals", filter: "customer-a" },
              { field: "region", type: "equals", filter: "usa" },
              { field: "status", type: "equals", filter: "open" },
            ],
            routeBy: { region: "usa", status: "open" },
            select: ["id", "region", "status"],
            limit: 1,
          });
          const storageKeySnapshot = yield* runtimeCore.internalClient.snapshotRuntimeInternal(
            "orders",
            {
              where: [
                { field: "customerId", type: "equals", filter: "customer-public-b" },
                { field: "region", type: "equals", filter: "usa" },
                { field: "status", type: "equals", filter: "open" },
              ],
              routeBy: { region: "usa", status: "open" },
              select: ["id", "region", "status"],
              limit: 1,
            },
          );
          const invalidRouteSnapshot = yield* Effect.flip(
            runtimeCore.internalClient.snapshotRuntimeInternal("orders", {
              where: [{ field: "region", type: "equals", filter: "usa" }],
              select: ["id"],
              limit: 1,
            }),
          );
          const publicRuntimeSubscribe = yield* Effect.flip(
            runtimeCore.liveClient.subscribeRuntime("orders", {
              where: [
                { field: "customerId", type: "equals", filter: "customer-a" },
                { field: "region", type: "equals", filter: "usa" },
                { field: "status", type: "equals", filter: "open" },
              ],
              routeBy: { region: "usa", status: "open" },
              select: ["id"],
              limit: 1,
            }),
          );
          const events = yield* Effect.acquireUseRelease(
            runtimeCore.internalLiveClient.subscribeRuntimeInternal("orders", {
              where: [
                { field: "customerId", type: "equals", filter: "customer-a" },
                { field: "region", type: "equals", filter: "usa" },
                { field: "status", type: "equals", filter: "open" },
              ],
              select: ["id"],
              limit: 1,
            }),
            (subscription) => subscription.events.pipe(Stream.take(1), Stream.runCollect),
            (subscription) => subscription.close(),
          );
          const routedEvents = yield* Effect.acquireUseRelease(
            runtimeCore.internalLiveClient.subscribeRuntimeRoutedInternal("orders", {
              where: [
                { field: "customerId", type: "equals", filter: "customer-a" },
                { field: "region", type: "equals", filter: "usa" },
                { field: "status", type: "equals", filter: "open" },
              ],
              routeBy: { region: "usa", status: "open" },
              select: ["id"],
              limit: 1,
            }),
            (subscription) => subscription.events.pipe(Stream.take(1), Stream.runCollect),
            (subscription) => subscription.close(),
          );

          expect(snapshot).toStrictEqual({
            rows: [
              {
                id: "a",
                region: "usa",
                status: "open",
              },
            ],
            totalRows: 1,
            version: 2,
            status: "ready",
            statusCode: "Ready",
          });
          expect(storageKeySnapshot).toStrictEqual({
            rows: [
              {
                id: "public-b",
                region: "usa",
                status: "open",
              },
            ],
            totalRows: 1,
            version: 2,
            status: "ready",
            statusCode: "Ready",
          });
          expect(invalidRouteSnapshot).toStrictEqual({
            _tag: "ViewServerRuntimeError",
            code: "InvalidQuery",
            topic: "orders",
            message: "Leased topic orders requires routeBy fields: region, status.",
          });
          expect(publicRuntimeSubscribe).toStrictEqual(publicLeasedRuntimeAccessError);
          expect(events[0]).toStrictEqual({
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 2,
            keys: ["a"],
            rows: [{ id: "a" }],
            totalRows: 1,
          });
          expect(routedEvents[0]).toStrictEqual({
            type: "snapshot",
            topic: "orders",
            queryId: "query-1",
            version: 2,
            keys: ["a"],
            rows: [{ id: "a" }],
            totalRows: 1,
          });
        }),
      (runtimeCore) => runtimeCore.close,
    ),
  );
});
