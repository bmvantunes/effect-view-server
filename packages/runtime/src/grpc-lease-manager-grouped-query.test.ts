import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Effect, Fiber, Queue, Stream } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";

import { grpcOrderValue } from "../test-harness/grpc-config";
import {
  grpcLeasedViewServer,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

const presentGroupedFieldKey = (field: string, canonicalKey: string): string =>
  JSON.stringify([[field, JSON.stringify(["present", canonicalKey])]]);

describe("gRPC lease manager grouped query behavior", () => {
  it.live("shares leased gRPC feeds while applying grouped queries locally", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
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

      const first = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "region", type: "equals", filter: "usa" }],
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });
      const firstEventQueue = yield* Queue.unbounded<unknown>();
      const firstEventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(firstEventQueue, event)),
        Effect.forkChild,
      );
      const firstSnapshot = yield* Queue.take(firstEventQueue);
      const firstDelta = yield* Queue.take(firstEventQueue);
      const openStatusGroupKey = presentGroupedFieldKey("status", '"open"');
      const second = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        groupBy: ["status"],
        aggregates: {
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: [{ field: "region", type: "equals", filter: "usa" }],
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
        limit: 10,
      });
      const secondEvents = yield* second.events.pipe(Stream.take(1), Stream.runCollect);

      expect(acquired).toBe(1);
      expect(firstSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(firstDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: openStatusGroupKey,
            row: {
              status: "open",
              rowCount: 2n,
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(secondEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [openStatusGroupKey],
        rows: [
          {
            status: "open",
            totalPrice: BigDecimal.fromStringUnsafe("30"),
          },
        ],
        totalRows: 1,
      });
      yield* first.close();
      yield* second.close();
      yield* Fiber.interrupt(firstEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
