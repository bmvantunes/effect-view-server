import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Effect, Fiber, HashMap, Queue, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";

import { grpcOrderValue } from "../test-harness/grpc-config";
import {
  grpcGroupedKeyEncodingLeasedViewServer,
  grpcSemanticGroupedKeyLeasedViewServer,
  semanticGroupedKeyValues,
} from "../test-harness/grpc-grouped";
import {
  grpcLeasedViewServer,
  leasedGrpcViewServer,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
  routeEncodingValues,
} from "../test-harness/grpc-leased";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";

const groupedPublicKey = (
  fields: ReadonlyArray<readonly [field: string, canonicalKey: string]>,
): string =>
  JSON.stringify(
    fields.map(([field, canonicalKey]) => [field, JSON.stringify(["present", canonicalKey])]),
  );

const semanticGroupedPublicKey = groupedPublicKey([
  ["semanticClass", '{"value":"class-value"}'],
  ["semanticOption", '{"_tag":"Some","value":"option-value"}'],
  ["semanticChunk", '["chunk-a","chunk-b"]'],
  ["semanticHashMap", '[["alpha","one"],["beta","two"]]'],
  ["semanticPlain", '"plain-value"'],
]);

const semanticGroupedPlainValue = "plain-value";

const missingOptionalGroupedPublicKey = JSON.stringify([
  ["optionalValue", JSON.stringify(["missing"])],
]);

const presentUndefinedGroupedPublicKey = JSON.stringify([
  ["optionalValue", JSON.stringify(["present", "null"])],
]);

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

const collisionGroupedPublicKey = groupedPublicKey([
  ["semanticHashMap", `[["${collisionLeft}","left"],["${collisionRight}","right"]]`],
]);

describe("gRPC lease manager semantic route and grouped identity", () => {
  it.live("externalizes semantic grouped keys in leased gRPC delta events", () =>
    Effect.gen(function* () {
      const semanticValues = semanticGroupedKeyValues();
      const feed = grpcSemanticGroupedKeyLeasedViewServer({
        acquire: () => longRunningGrpcStream([grpcOrderValue("route-encoding-1", 10)]),
        map: (value) => ({
          id: value.customerId,
          text: routeEncodingValues.text,
          ...semanticValues,
          semanticPlain: semanticGroupedPlainValue,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof grpcOptions.sourceConfig.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        groupBy: [
          "semanticClass",
          "semanticOption",
          "semanticChunk",
          "semanticHashMap",
          "semanticPlain",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
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
              key: semanticGroupedPublicKey,
              row: {
                ...semanticValues,
                rowCount: 1n,
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes semantic grouped keys in leased gRPC snapshots", () =>
    Effect.gen(function* () {
      const semanticValues = semanticGroupedKeyValues();
      const feed = grpcSemanticGroupedKeyLeasedViewServer({
        acquire: () => longRunningGrpcStream([grpcOrderValue("route-encoding-1", 10)]),
        map: (value) => ({
          id: value.customerId,
          text: routeEncodingValues.text,
          ...semanticValues,
          semanticPlain: semanticGroupedPlainValue,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof grpcOptions.sourceConfig.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const rawSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        select: [
          "id",
          "semanticClass",
          "semanticOption",
          "semanticChunk",
          "semanticHashMap",
          "semanticPlain",
        ],
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
      const rawEventQueue = yield* Queue.unbounded<unknown>();
      const rawEventsFiber = yield* rawSubscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(rawEventQueue, event)),
        Effect.forkChild,
      );
      const rawSnapshot = yield* Queue.take(rawEventQueue);
      const rawDelta = yield* Queue.take(rawEventQueue);
      const groupedSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        groupBy: [
          "semanticClass",
          "semanticOption",
          "semanticChunk",
          "semanticHashMap",
          "semanticPlain",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
      const groupedEvents = yield* groupedSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect([rawSnapshot, rawDelta]).toStrictEqual([
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
              key: "route-encoding-1",
              row: {
                id: "route-encoding-1",
                ...semanticValues,
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(groupedEvents).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: [semanticGroupedPublicKey],
          rows: [
            {
              ...semanticValues,
              rowCount: 1n,
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* rawSubscription.close();
      yield* groupedSubscription.close();
      yield* Fiber.interrupt(rawEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps missing and present undefined leased gRPC grouped keys distinct", () =>
    Effect.gen(function* () {
      const semanticValues = semanticGroupedKeyValues();
      const feed = grpcSemanticGroupedKeyLeasedViewServer({
        acquire: () =>
          longRunningGrpcStream([
            grpcOrderValue("missing-optional", 10),
            grpcOrderValue("present-undefined", 20),
          ]),
        map: (value) =>
          value.price === 10
            ? {
                id: value.customerId,
                text: routeEncodingValues.text,
                ...semanticValues,
                semanticPlain: semanticGroupedPlainValue,
              }
            : {
                id: value.customerId,
                text: routeEncodingValues.text,
                ...semanticValues,
                semanticPlain: semanticGroupedPlainValue,
                optionalValue: undefined,
              },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof grpcOptions.sourceConfig.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        groupBy: ["optionalValue"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
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
              key: missingOptionalGroupedPublicKey,
              row: {
                rowCount: 1n,
              },
              index: 0,
            },
            {
              type: "insert",
              key: presentUndefinedGroupedPublicKey,
              row: {
                optionalValue: undefined,
                rowCount: 1n,
              },
              index: 1,
            },
          ],
          totalRows: 2,
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("coalesces collision-node HashMap groups behind one public result key", () =>
    Effect.gen(function* () {
      const semanticValues = semanticGroupedKeyValues();
      const feed = grpcSemanticGroupedKeyLeasedViewServer({
        acquire: () =>
          longRunningGrpcStream([
            grpcOrderValue("collision-left", 10),
            grpcOrderValue("collision-right", 20),
          ]),
        map: (value) => ({
          id: value.customerId,
          text: routeEncodingValues.text,
          ...semanticValues,
          semanticHashMap:
            value.price === 10
              ? HashMap.make([collisionLeft, "left"], [collisionRight, "right"])
              : HashMap.make([collisionRight, "right"], [collisionLeft, "left"]),
          semanticPlain: semanticGroupedPlainValue,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof grpcOptions.sourceConfig.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {},
      });
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const rawSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        select: ["id", "semanticHashMap"],
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
      const rawEventQueue = yield* Queue.unbounded<unknown>();
      const rawEventsFiber = yield* rawSubscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(rawEventQueue, event)),
        Effect.forkChild,
      );
      yield* Queue.take(rawEventQueue);
      yield* Queue.take(rawEventQueue);
      const groupedSubscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        groupBy: ["semanticHashMap"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
      const groupedEvents = yield* groupedSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect(groupedEvents).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: [collisionGroupedPublicKey],
          rows: [
            {
              semanticHashMap: HashMap.make([collisionLeft, "left"], [collisionRight, "right"]),
              rowCount: 2n,
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* rawSubscription.close();
      yield* groupedSubscription.close();
      yield* Fiber.interrupt(rawEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("turns grouped row reflection failures into the stable public-key error status", () =>
    Effect.gen(function* () {
      const hostileGroupedRow = new Proxy(
        {
          customerId: "customer-1",
          rowCount: 1n,
        },
        {
          getOwnPropertyDescriptor() {
            throw new Error("grouped row reflection failed");
          },
        },
      );
      const internalSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "hostile-grouped-row",
        version: 0,
        keys: ["internal-group"],
        rows: [hostileGroupedRow],
        totalRows: 1,
      } as const;
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(internalSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(internalSnapshot),
              close: () => Effect.void,
            }),
          ),
      };
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { region: "usa" },
        groupBy: ["customerId"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "region", type: "equals", filter: "usa" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.runCollect);

      expect(events).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "hostile-grouped-row",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("turns non-materializable canonical grouped values into the stable error status", () =>
    Effect.gen(function* () {
      const internalSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "non-materializable-grouped-value",
        version: 0,
        keys: ["internal-group"],
        rows: [{ weird: new Map([["desk", "equities"]]), rowCount: 1n }],
        totalRows: 1,
      } as const;
      const feed = grpcGroupedKeyEncodingLeasedViewServer({
        acquire: () => Stream.never,
        map: () => ({
          id: "unused",
          ...routeEncodingValues,
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof grpcOptions.sourceConfig.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(internalSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(internalSnapshot),
              close: () => Effect.void,
            }),
          ),
      };
      const health = makeLeasedGrpcHealth(grpcOptions);
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const subscription = yield* manager.liveClient.subscribeRuntime("orders", {
        routeBy: { text: routeEncodingValues.text },
        groupBy: ["weird"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: [{ field: "text", type: "equals", filter: routeEncodingValues.text }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.runCollect);

      expect(events).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "non-materializable-grouped-value",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
