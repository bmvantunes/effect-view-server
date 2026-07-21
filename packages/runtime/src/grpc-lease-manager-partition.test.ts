import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { BigDecimal, Effect, Fiber, Option, Queue, Schema, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import {
  grpcClients,
  grpcOrderValue,
  grpcTopicSources,
  type GrpcOrderValueMessage,
} from "../test-harness/grpc-config";
import {
  grpcLeasedViewServerFromCallbacks,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";

const groupedStatusKey = (status: string): string =>
  JSON.stringify([["status", JSON.stringify(["present", JSON.stringify(status)])]]);

describe("gRPC leased query partitions", () => {
  it.live("keeps BigDecimal routes isolated after row decoding normalizes their scale", () =>
    Effect.gen(function* () {
      const scaleOneRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const scaleZeroRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const ScaleRouteRow = Schema.Struct({
        id: Schema.String,
        amount: Schema.BigDecimal,
      });
      const scaleOne = BigDecimal.make(10n, 1);
      const scaleZero = BigDecimal.make(1n, 0);
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          positions: grpcTopicSources.leased({
            schema: ScaleRouteRow,
            key: "id",
            client: "orders",
            method: "streamOrders",
            routeBy: ["amount"],
            request: () => ({ orderId: "amount" }),
            acquire: ({ route }) =>
              route.amount.scale === 1
                ? Stream.fromQueue(scaleOneRows)
                : Stream.fromQueue(scaleZeroRows),
            map: ({ value, route }) => ({ id: value.customerId, amount: route.amount }),
          }),
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(config);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        makeLeasedGrpcHealth(grpcOptions),
      );

      const scaleOneSubscription = yield* manager.liveClient.subscribe("positions", {
        routeBy: { amount: scaleOne },
        select: ["id"],
      });
      const scaleZeroSubscription = yield* manager.liveClient.subscribe("positions", {
        routeBy: { amount: scaleZero },
        select: ["id"],
      });
      const scaleOneEvents = yield* Queue.unbounded<unknown>();
      const scaleZeroEvents = yield* Queue.unbounded<unknown>();
      const scaleOneFiber = yield* scaleOneSubscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(scaleOneEvents, event)),
        Effect.forkChild,
      );
      const scaleZeroFiber = yield* scaleZeroSubscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(scaleZeroEvents, event)),
        Effect.forkChild,
      );

      yield* Queue.take(scaleOneEvents);
      yield* Queue.take(scaleZeroEvents);
      yield* Queue.offer(scaleOneRows, grpcOrderValue("scale-one", 1));
      const scaleOneInsert = yield* Queue.take(scaleOneEvents);
      yield* Queue.offer(scaleZeroRows, grpcOrderValue("scale-zero", 1));
      const scaleZeroInsert = yield* Queue.take(scaleZeroEvents);

      expect(scaleOneInsert).toStrictEqual({
        type: "delta",
        topic: "positions",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "scale-one",
            row: { id: "scale-one" },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(scaleZeroInsert).toStrictEqual({
        type: "delta",
        topic: "positions",
        queryId: "query-1",
        fromVersion: 0,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "scale-zero",
            row: { id: "scale-zero" },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(yield* Queue.poll(scaleOneEvents)).toStrictEqual(Option.none());
      expect(yield* Queue.poll(scaleZeroEvents)).toStrictEqual(Option.none());

      yield* scaleOneSubscription.close();
      yield* scaleZeroSubscription.close();
      yield* Fiber.interrupt(scaleOneFiber);
      yield* Fiber.interrupt(scaleZeroFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "partitions raw snapshots and live deltas for identical local queries and public keys",
    () =>
      Effect.gen(function* () {
        const usaRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
        const euRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
        const config = grpcLeasedViewServerFromCallbacks({
          acquire: ({ route }) =>
            route.region === "usa" ? Stream.fromQueue(usaRows) : Stream.fromQueue(euRows),
          map: ({ value, route }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          }),
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(config);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const manager = yield* makeViewServerGrpcLeaseManager(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          Effect.void,
          grpcOptions,
          makeLeasedGrpcHealth(grpcOptions),
        );

        const usa = yield* manager.liveClient.subscribe("orders", {
          routeBy: { region: "usa" },
          select: ["id", "region", "price"],
          where: [
            {
              type: "OR",
              conditions: [
                { field: "status", type: "equals", filter: "open" },
                { field: "price", type: "greaterThan", filter: 0 },
              ],
            },
          ],
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        });
        const eu = yield* manager.liveClient.subscribeRuntime("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region", "price"],
          where: [
            {
              type: "OR",
              conditions: [
                { field: "status", type: "equals", filter: "open" },
                { field: "price", type: "greaterThan", filter: 0 },
              ],
            },
          ],
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        });
        const usaEvents = yield* Queue.unbounded<unknown>();
        const euEvents = yield* Queue.unbounded<unknown>();
        const usaEventsFiber = yield* usa.events.pipe(
          Stream.runForEach((event) => Queue.offer(usaEvents, event)),
          Effect.forkChild,
        );
        const euEventsFiber = yield* eu.events.pipe(
          Stream.runForEach((event) => Queue.offer(euEvents, event)),
          Effect.forkChild,
        );

        const usaSnapshot = yield* Queue.take(usaEvents);
        const euSnapshot = yield* Queue.take(euEvents);
        yield* Queue.offer(usaRows, grpcOrderValue("shared", 10));
        const usaInsert = yield* Queue.take(usaEvents);
        yield* Queue.offer(euRows, grpcOrderValue("shared", 20));
        const euInsert = yield* Queue.take(euEvents);
        yield* Queue.offer(usaRows, grpcOrderValue("usa-only", 30));
        const usaSecondInsert = yield* Queue.take(usaEvents);
        yield* Queue.offer(euRows, grpcOrderValue("eu-only", 40));
        const euSecondInsert = yield* Queue.take(euEvents);

        expect(usaSnapshot).toStrictEqual({
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        });
        expect(euSnapshot).toStrictEqual({
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        });
        expect(usaInsert).toStrictEqual({
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 0,
          toVersion: 1,
          operations: [
            {
              type: "insert",
              key: "shared",
              row: { id: "shared", region: "usa", price: 10 },
              index: 0,
            },
          ],
          totalRows: 1,
        });
        expect(euInsert).toStrictEqual({
          type: "delta",
          topic: "orders",
          queryId: "query-1",
          fromVersion: 0,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: "shared",
              row: { id: "shared", region: "eu", price: 20 },
              index: 0,
            },
          ],
          totalRows: 1,
        });
        expect(usaSecondInsert).toStrictEqual({
          type: "delta",
          topic: "orders",
          queryId: "query-0",
          fromVersion: 1,
          toVersion: 3,
          operations: [
            {
              type: "insert",
              key: "usa-only",
              row: { id: "usa-only", region: "usa", price: 30 },
              index: 1,
            },
          ],
          totalRows: 2,
        });
        expect(euSecondInsert).toStrictEqual({
          type: "delta",
          topic: "orders",
          queryId: "query-1",
          fromVersion: 2,
          toVersion: 4,
          operations: [
            {
              type: "insert",
              key: "eu-only",
              row: { id: "eu-only", region: "eu", price: 40 },
              index: 1,
            },
          ],
          totalRows: 2,
        });

        yield* usa.close();
        yield* eu.close();
        yield* Fiber.interrupt(usaEventsFiber);
        yield* Fiber.interrupt(euEventsFiber);
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("partitions grouped materialization and live deltas when local where is omitted", () =>
    Effect.gen(function* () {
      const usaRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const euRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const config = grpcLeasedViewServerFromCallbacks({
        acquire: ({ route }) =>
          route.region === "usa" ? Stream.fromQueue(usaRows) : Stream.fromQueue(euRows),
        map: ({ value, route }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(config);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        makeLeasedGrpcHealth(grpcOptions),
      );

      const usa = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });
      const eu = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { region: "eu" },
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });
      const usaEvents = yield* Queue.unbounded<unknown>();
      const euEvents = yield* Queue.unbounded<unknown>();
      const usaEventsFiber = yield* usa.events.pipe(
        Stream.runForEach((event) => Queue.offer(usaEvents, event)),
        Effect.forkChild,
      );
      const euEventsFiber = yield* eu.events.pipe(
        Stream.runForEach((event) => Queue.offer(euEvents, event)),
        Effect.forkChild,
      );

      const usaSnapshot = yield* Queue.take(usaEvents);
      const euSnapshot = yield* Queue.take(euEvents);
      yield* Queue.offer(usaRows, grpcOrderValue("shared", 10));
      const usaInsert = yield* Queue.take(usaEvents);
      yield* Queue.offer(euRows, grpcOrderValue("shared", 20));
      const euInsert = yield* Queue.take(euEvents);
      yield* Queue.offer(usaRows, grpcOrderValue("usa-only", 30));
      const usaUpdate = yield* Queue.take(usaEvents);
      yield* Queue.offer(euRows, grpcOrderValue("eu-only", 40));
      const euUpdate = yield* Queue.take(euEvents);
      const openKey = groupedStatusKey("open");

      expect(usaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(euSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(usaInsert).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: openKey,
            row: { status: "open", rowCount: 1n },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(euInsert).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 0,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: openKey,
            row: { status: "open", rowCount: 1n },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(usaUpdate).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 3,
        operations: [
          {
            type: "update",
            key: openKey,
            row: { status: "open", rowCount: 2n },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(euUpdate).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 2,
        toVersion: 4,
        operations: [
          {
            type: "update",
            key: openKey,
            row: { status: "open", rowCount: 2n },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      yield* usa.close();
      yield* eu.close();
      yield* Fiber.interrupt(usaEventsFiber);
      yield* Fiber.interrupt(euEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
