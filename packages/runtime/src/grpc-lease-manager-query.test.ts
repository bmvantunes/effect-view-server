import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent } from "@effect-view-server/client";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Clock, Deferred, Effect, Fiber, Queue, Schedule, Stream } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import type { GrpcGroupedKeyRetentionView } from "./grpc-grouped-key-translations";
import { makeDefaultRuntimeDependencies } from "./internal";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { makeDefaultGrpcClient } from "./grpc-source-lifecycle";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import { makeLeasedGrpcRuntimeHarness } from "../test-harness/grpc-runtime";

import { grpcOrderValue } from "../test-harness/grpc-config";
import {
  fastGrpcMaterializedReconnect,
  resolveLeasedGrpcRuntimeOptions,
} from "../test-harness/grpc-materialized";
import {
  grpcGroupedKeyEncodingLeasedViewServer,
  grpcKeyLeasedViewServer,
  grpcLeasedViewServer,
  grpcLeasedViewServerFromCallbacks,
  grpcPublicKeyLeasedViewServer,
  grpcRouteEncodingLeasedViewServer,
  leasedGrpcViewServer,
  leasedOrdersQuery,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
  routeEncodingValues,
  waitForLeasedGrpcSnapshotRows,
} from "../test-harness/grpc-leased";

import type { GrpcOrderValueMessage } from "../test-harness/grpc-config";

describe("gRPC lease manager query translation", () => {
  it.live("opens a leased gRPC feed on first subscriber and removes rows after last close", () =>
    Effect.gen(function* () {
      let acquired = 0;
      let released = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-order-1`, 10),
            grpcOrderValue(`${region}-order-2`, 20),
          ]),
      });
      const harness = yield* makeLeasedGrpcRuntimeHarness({
        config: feed,
      });
      const { health, manager, runtimeCore } = harness;
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const readySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        2,
      );
      const readyHealthNow = yield* Clock.currentTimeMillis;
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), readyHealthNow);

      yield* subscription.close().pipe(Effect.timeout("1 second"));
      yield* Effect.yieldNow;
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect(acquired).toBe(1);
      expect(released).toBe(1);
      expect({
        status: idleHealth.status,
        client: idleHealth.grpc?.clients["orders"],
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        status: "ready",
        client: {
          status: "connected",
          baseUrl: "https://orders.example.test",
          activeFeeds: 0,
          lastConnectedAt: null,
          lastError: null,
        },
        leasedFeeds: [],
      });
      expect(readySnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-order-1",
          customerId: "usa-order-1",
          price: 10,
          region: "usa",
        },
        {
          id: "usa:usa-order-2",
          customerId: "usa-order-2",
          price: 20,
          region: "usa",
        },
      ]);
      expect(Object.keys(readyHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/orders/leased/region=string%3A3%3Ausa",
      ]);
      expect(
        readyHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=string%3A3%3Ausa"],
      ).toStrictEqual({
        status: "ready",
        lifecycle: "leased",
        feedName: "orders",
        feedKey: "orders/orders/leased/region=string%3A3%3Ausa",
        topic: "orders",
        subscriberCount: 1,
        rowCount: 2,
        messagesPerSecond: 2,
        rowsPerSecond: 2,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt:
          readyHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=string%3A3%3Ausa"]
            ?.lastMessageAt,
        lastError: null,
      });
      expect(emptySnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: emptySnapshot.version,
        status: "ready",
        statusCode: "Ready",
      });
      expect(Object.keys(stoppedHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([]);
      yield* harness.close;
    }),
  );

  it.live("reuses a leased gRPC feed for same-route subscribers", () =>
    Effect.gen(function* () {
      let acquired = 0;
      let released = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const harness = yield* makeLeasedGrpcRuntimeHarness({
        config: feed,
      });
      const { health, manager, runtimeCore } = harness;

      const first = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const second = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const sharedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      yield* first.close();
      const afterFirstClose = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      yield* second.close();
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 0);

      expect(acquired).toBe(1);
      expect(released).toBe(1);
      expect(
        sharedHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=string%3A3%3Ausa"]
          ?.subscriberCount,
      ).toBe(2);
      expect(
        afterFirstClose.grpc?.feeds["orders"]?.leased[
          "orders/orders/leased/region=string%3A3%3Ausa"
        ]?.subscriberCount,
      ).toBe(1);
      yield* harness.close;
    }),
  );

  it.live("externalizes leased gRPC row keys on public live events", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const harness = yield* makeLeasedGrpcRuntimeHarness({
        config: feed,
      });
      const { manager } = harness;

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:usa-order-1",
            row: {
              id: "usa:usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      yield* subscription.close();
      yield* harness.close;
    }),
  );

  it.live("preserves public row-key tie-break ordering for leased gRPC feeds", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-a `, 10),
            grpcOrderValue(`${region}-a!`, 10),
          ]),
      });
      const harness = yield* makeLeasedGrpcRuntimeHarness({
        config: feed,
      });
      const { manager } = harness;

      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:usa-a ",
            row: {
              id: "usa:usa-a ",
              price: 10,
            },
            index: 0,
          },
          {
            type: "insert",
            key: "usa:usa-a!",
            row: {
              id: "usa:usa-a!",
              price: 10,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* harness.close;
    }),
  );

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
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
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
      const openStatusGroupKey = '["array",[["array",[["string","status"],["string","open"]]]]]';
      const second = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["status"],
        aggregates: {
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: {
          region: { eq: "usa" },
        },
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

  it.live("externalizes grouped leased gRPC keys that include the topic key field", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
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
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const firstEventQueue = yield* Queue.unbounded<unknown>();
      const firstEventsFiber = yield* first.events.pipe(
        Stream.runForEach((event) => Queue.offer(firstEventQueue, event)),
        Effect.forkChild,
      );
      const firstSnapshot = yield* Queue.take(firstEventQueue);
      const firstDelta = yield* Queue.take(firstEventQueue);
      const second = yield* manager.liveClient.subscribe("orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const secondEvents = yield* second.events.pipe(Stream.take(1), Stream.runCollect);
      const firstPublicGroupKey =
        '["array",[["array",[["string","id"],["string","usa:usa-order-1"]]]]]';
      const secondPublicGroupKey =
        '["array",[["array",[["string","id"],["string","usa:usa-order-2"]]]]]';

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
            key: firstPublicGroupKey,
            row: {
              id: "usa:usa-order-1",
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: secondPublicGroupKey,
            row: {
              id: "usa:usa-order-2",
              rowCount: 1n,
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      expect(secondEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [firstPublicGroupKey, secondPublicGroupKey],
        rows: [
          {
            id: "usa:usa-order-1",
            rowCount: 1n,
          },
          {
            id: "usa:usa-order-2",
            rowCount: 1n,
          },
        ],
        totalRows: 2,
      });

      yield* first.close();
      yield* second.close();
      yield* Fiber.interrupt(firstEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("supports leased gRPC feeds routed by the topic key", () =>
    Effect.gen(function* () {
      const feed = grpcKeyLeasedViewServer({
        streamForId: (id) => longRunningGrpcStream([grpcOrderValue(`${id}-customer`, 10)]),
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

      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price"],
        where: {
          id: { eq: "order-1" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "order-1",
            row: {
              id: "order-1",
              customerId: "order-1-customer",
              price: 10,
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("creates stable leased feed keys for non-string route values", () =>
    Effect.gen(function* () {
      const feed = grpcRouteEncodingLeasedViewServer();
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
        select: ["id"],
        where: {
          amount: { eq: routeEncodingValues.amount },
          count: { eq: routeEncodingValues.count },
          disabled: { eq: routeEncodingValues.disabled },
          flag: { eq: routeEncodingValues.flag },
          meta: { eq: routeEncodingValues.meta },
          none: { eq: routeEncodingValues.none },
          plainScore: { eq: routeEncodingValues.plainScore },
          score: { eq: routeEncodingValues.score },
          tags: { eq: routeEncodingValues.tags },
          text: { eq: routeEncodingValues.text },
          weird: { eq: routeEncodingValues.weird },
        },
        limit: 10,
      });
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/orders/leased/amount=bigDecimal%3A6%3A123.45&count=bigint%3A16%3A9007199254740993&disabled=boolean%3A5%3Afalse&flag=boolean%3A4%3Atrue&meta=object%3A28%3A6%3A%22desk%2217%3Astring%3A8%3Aequities&none=null%3A4%3Anull&plainScore=number%3A2%3A42&score=number%3A2%3A-0&tags=array%3A34%3A13%3Astring%3A4%3Afast15%3Astring%3A6%3Ashared&text=string%3A5%3Aroute&weird=object%3A53%3A7%3A%22alpha%2214%3Astring%3A5%3Afirst8%3A%22stable%2214%3Astring%3A5%3Aroute",
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes grouped leased gRPC route values with public grouped keys", () =>
    Effect.gen(function* () {
      const feed = grpcGroupedKeyEncodingLeasedViewServer({
        acquire: () =>
          Stream.make(
            grpcOrderValue("route-encoding-1", 10),
            grpcOrderValue("route-encoding-2", 20),
            grpcOrderValue("route-encoding-3", 30),
          ),
        map: (value) => ({
          id: value.customerId,
          ...routeEncodingValues,
          meta: {
            desk: value.price === 30 ? "credit" : value.price === 20 ? "rates" : "equities",
          },
          tags:
            value.price === 30
              ? ["unsupported"]
              : value.price === 20
                ? ["slow", "shared"]
                : routeEncodingValues.tags,
          weird: undefined,
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
        groupBy: [
          "amount",
          "count",
          "disabled",
          "flag",
          "none",
          "plainScore",
          "score",
          "text",
          "weird",
          "meta",
          "tags",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const deltaEvents = yield* subscription.events.pipe(
        Stream.filter((event) => event.type === "delta"),
        Stream.take(4),
        Stream.runCollect,
      );
      const publicGroupedKeyOne =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:28:6:\\"desk\\"17:string:8:equities"]]],["array",[["string","tags"],["canonical","array:34:13:string:4:fast15:string:6:shared"]]]]]';
      const publicGroupedKeyTwo =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:25:6:\\"desk\\"14:string:5:rates"]]],["array",[["string","tags"],["canonical","array:34:13:string:4:slow15:string:6:shared"]]]]]';
      const publicGroupedKeyThree =
        '["array",[["array",[["string","amount"],["bigDecimal","123.45"]]],["array",[["string","count"],["bigint","9007199254740993"]]],["array",[["string","disabled"],["boolean",false]]],["array",[["string","flag"],["boolean",true]]],["array",[["string","none"],["null"]]],["array",[["string","plainScore"],["number","42"]]],["array",[["string","score"],["number","-0"]]],["array",[["string","text"],["string","route"]]],["array",[["string","weird"],["undefined"]]],["array",[["string","meta"],["canonical","object:26:6:\\"desk\\"15:string:6:credit"]]],["array",[["string","tags"],["canonical","array:24:21:string:11:unsupported"]]]]]';

      expect(deltaEvents[0]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: publicGroupedKeyThree,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: {
                desk: "credit",
              },
              tags: ["unsupported"],
              rowCount: 1n,
            },
            index: 0,
          },
          {
            type: "insert",
            key: publicGroupedKeyOne,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: routeEncodingValues.meta,
              tags: routeEncodingValues.tags,
              rowCount: 1n,
            },
            index: 1,
          },
          {
            type: "insert",
            key: publicGroupedKeyTwo,
            row: {
              amount: routeEncodingValues.amount,
              count: routeEncodingValues.count,
              disabled: routeEncodingValues.disabled,
              flag: routeEncodingValues.flag,
              none: routeEncodingValues.none,
              plainScore: routeEncodingValues.plainScore,
              score: routeEncodingValues.score,
              text: routeEncodingValues.text,
              weird: undefined,
              meta: {
                desk: "rates",
              },
              tags: ["slow", "shared"],
              rowCount: 1n,
            },
            index: 2,
          },
        ],
        totalRows: 3,
      });
      expect(deltaEvents[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyOne,
          },
        ],
        totalRows: 2,
      });
      expect(deltaEvents[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyTwo,
          },
        ],
        totalRows: 1,
      });
      expect(deltaEvents[3]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 3,
        toVersion: 4,
        operations: [
          {
            type: "remove",
            key: publicGroupedKeyThree,
          },
        ],
        totalRows: 0,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("bounds grouped-key translations to each leased subscription lifetime", () =>
    Effect.gen(function* () {
      const churnRows = Array.from({ length: 64 }, (_, index) => ({
        customerId: `customer-${index}`,
        rowCount: 1n,
      }));
      const churnInternalKeys = churnRows.map((_row, index) => `customer-internal-${index}`);
      const removalOperations: ReadonlyArray<{ readonly type: "remove"; readonly key: string }> =
        churnInternalKeys.slice(1).map((key) => ({ type: "remove", key }));
      const replacementRows = [
        { customerId: "replacement-a", rowCount: 1n },
        { customerId: "replacement-b", rowCount: 1n },
      ];
      const replacementInternalKeys = ["replacement-internal-a", "replacement-internal-b"];
      const customerSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "customer-groups",
        version: 0,
        keys: churnInternalKeys,
        rows: churnRows,
        totalRows: churnRows.length,
      } as const;
      const customerRemovalDelta = {
        type: "delta",
        topic: "orders",
        queryId: "customer-groups",
        fromVersion: 0,
        toVersion: 1,
        operations: removalOperations,
        totalRows: 1,
      } as const;
      const customerReplacementSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "customer-groups",
        version: 2,
        keys: replacementInternalKeys,
        rows: replacementRows,
        totalRows: replacementRows.length,
      } as const;
      const statusSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "status-groups",
        version: 0,
        keys: ["status-internal-open"],
        rows: [{ status: "open", rowCount: 64n }],
        totalRows: 1,
      } as const;
      const statusMoveDelta = {
        type: "delta",
        topic: "orders",
        queryId: "status-groups",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "update",
            key: "status-internal-open",
            row: { status: "open", rowCount: 65n },
            index: 0,
          },
          {
            type: "move",
            key: "status-internal-open",
            fromIndex: 0,
            toIndex: 0,
          },
        ],
        totalRows: 1,
      } as const;
      const publicGroupedStringKey = (field: string, value: string): string => {
        const fieldToken = `["array",[["string",${JSON.stringify(field)}],["string",${JSON.stringify(value)}]]]`;
        return `["array",[${fieldToken}]]`;
      };
      const releaseCustomerRemovals = yield* Deferred.make<void>();
      const releaseCustomerReplacement = yield* Deferred.make<void>();
      const releaseStatusMove = yield* Deferred.make<void>();
      const retentionViews: Array<GrpcGroupedKeyRetentionView> = [];
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {}),
        (runtimeCore) => {
          let subscriptionIndex = 0;
          const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
            typeof leasedGrpcViewServer.topics
          > = {
            ...runtimeCore.internalLiveClient,
            subscribeRuntimeObservedInternal: (_topic, _query, observer) => {
              if (subscriptionIndex === 0) {
                subscriptionIndex += 1;
                return observer.onQueryRegistered(customerSnapshot.queryId).pipe(
                  Effect.as({
                    events: Stream.make(customerSnapshot).pipe(
                      Stream.concat(
                        Stream.fromEffect(
                          Deferred.await(releaseCustomerRemovals).pipe(
                            Effect.as(customerRemovalDelta),
                          ),
                        ),
                      ),
                      Stream.concat(
                        Stream.fromEffect(
                          Deferred.await(releaseCustomerReplacement).pipe(
                            Effect.as(customerReplacementSnapshot),
                          ),
                        ),
                      ),
                      Stream.concat(Stream.never),
                    ),
                    close: () => Effect.void,
                  }),
                );
              }
              subscriptionIndex += 1;
              return observer.onQueryRegistered(statusSnapshot.queryId).pipe(
                Effect.as({
                  events: Stream.make(statusSnapshot).pipe(
                    Stream.concat(
                      Stream.fromEffect(
                        Deferred.await(releaseStatusMove).pipe(Effect.as(statusMoveDelta)),
                      ),
                    ),
                    Stream.concat(Stream.never),
                  ),
                  close: () => Effect.void,
                }),
              );
            },
          };
          const health = makeLeasedGrpcHealth(grpcOptions);
          return Effect.acquireUseRelease(
            makeViewServerGrpcLeaseManager(
              grpcOptions.sourceConfig,
              runtimeCore.internalClient,
              runtimeCore.liveClient,
              fakeInternalLiveClient,
              Effect.void,
              grpcOptions,
              health,
              makeDefaultGrpcClient,
              (retention) => {
                retentionViews.push(retention);
              },
            ),
            (manager) =>
              Effect.acquireUseRelease(
                manager.liveClient.subscribeRuntime("orders", {
                  groupBy: ["customerId"],
                  aggregates: {
                    rowCount: { aggFunc: "count" },
                  },
                  where: {
                    region: { eq: "usa" },
                  },
                  limit: 100,
                }),
                (customerSubscription) =>
                  Effect.acquireUseRelease(
                    manager.liveClient.subscribeRuntime("orders", {
                      groupBy: ["status"],
                      aggregates: {
                        rowCount: { aggFunc: "count" },
                      },
                      where: {
                        region: { eq: "usa" },
                      },
                      limit: 10,
                    }),
                    (statusSubscription) =>
                      Effect.gen(function* () {
                        const customerRetention = yield* Effect.fromNullishOr(retentionViews[0]);
                        const statusRetention = yield* Effect.fromNullishOr(retentionViews[1]);
                        const customerEventQueue = yield* Queue.unbounded<unknown>();
                        const customerEventsFiber = yield* customerSubscription.events.pipe(
                          Stream.runForEach((event) => Queue.offer(customerEventQueue, event)),
                          Effect.forkChild({ startImmediately: true }),
                        );
                        const customerSnapshotEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(64);

                        yield* Deferred.succeed(releaseCustomerRemovals, undefined);
                        const customerRemovalEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(1);

                        yield* Deferred.succeed(releaseCustomerReplacement, undefined);
                        const customerReplacementEvent = yield* Queue.take(customerEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(customerRetention.retainedEntryCount()).toBe(2);
                        const customerEvents = [
                          customerSnapshotEvent,
                          customerRemovalEvent,
                          customerReplacementEvent,
                        ];
                        const statusEventQueue = yield* Queue.unbounded<unknown>();
                        const statusEventsFiber = yield* statusSubscription.events.pipe(
                          Stream.runForEach((event) => Queue.offer(statusEventQueue, event)),
                          Effect.forkChild({ startImmediately: true }),
                        );
                        const statusSnapshotEvent = yield* Queue.take(statusEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        expect(statusRetention.retainedEntryCount()).toBe(1);

                        yield* customerSubscription.close();
                        yield* Fiber.interrupt(customerEventsFiber);
                        expect(customerRetention.retainedEntryCount()).toBe(0);
                        yield* Deferred.succeed(releaseStatusMove, undefined);
                        const statusMoveEvent = yield* Queue.take(statusEventQueue).pipe(
                          Effect.timeout("1 second"),
                        );
                        const statusEvents = [statusSnapshotEvent, statusMoveEvent];

                        expect(customerEvents).toStrictEqual([
                          {
                            ...customerSnapshot,
                            keys: churnRows.map((row) =>
                              publicGroupedStringKey("customerId", row.customerId),
                            ),
                          },
                          {
                            ...customerRemovalDelta,
                            operations: churnRows.slice(1).map((row) => ({
                              type: "remove",
                              key: publicGroupedStringKey("customerId", row.customerId),
                            })),
                          },
                          {
                            ...customerReplacementSnapshot,
                            keys: replacementRows.map((row) =>
                              publicGroupedStringKey("customerId", row.customerId),
                            ),
                          },
                        ]);
                        expect(statusEvents).toStrictEqual([
                          {
                            ...statusSnapshot,
                            keys: [publicGroupedStringKey("status", "open")],
                          },
                          {
                            ...statusMoveDelta,
                            operations: [
                              {
                                ...statusMoveDelta.operations[0],
                                key: publicGroupedStringKey("status", "open"),
                              },
                              {
                                ...statusMoveDelta.operations[1],
                                key: publicGroupedStringKey("status", "open"),
                              },
                            ],
                          },
                        ]);
                        yield* manager.close;
                        expect(statusRetention.retainedEntryCount()).toBe(0);
                        expect(customerRetention.retainedEntryCount()).toBe(0);
                        yield* manager.close;
                        expect(statusRetention.retainedEntryCount()).toBe(0);
                        yield* Fiber.interrupt(statusEventsFiber);
                      }).pipe(
                        Effect.ensuring(
                          Deferred.succeed(releaseCustomerRemovals, undefined).pipe(
                            Effect.andThen(Deferred.succeed(releaseCustomerReplacement, undefined)),
                            Effect.andThen(Deferred.succeed(releaseStatusMove, undefined)),
                          ),
                        ),
                      ),
                    (subscription) => subscription.close(),
                  ),
                (subscription) => subscription.close(),
              ),
            (manager) => manager.close,
          );
        },
        (runtimeCore) => runtimeCore.close,
      );
    }),
  );

  it.live("fails grouped leased gRPC event streams for non-canonical public key values", () =>
    Effect.gen(function* () {
      const feed = grpcGroupedKeyEncodingLeasedViewServer({
        acquire: () => longRunningGrpcStream([grpcOrderValue("route-encoding-1", 10)]),
        map: (value) => ({
          id: value.customerId,
          ...routeEncodingValues,
          weird: new Uint8Array([1]),
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
        groupBy: ["weird"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.runCollect);

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
          type: "status",
          topic: "orders",
          queryId: "query-0",
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

  it.live("fails grouped leased gRPC snapshots for non-canonical public key values", () =>
    Effect.gen(function* () {
      const feed = grpcGroupedKeyEncodingLeasedViewServer({
        acquire: () => longRunningGrpcStream([grpcOrderValue("route-encoding-1", 10)]),
        map: (value) => ({
          id: value.customerId,
          ...routeEncodingValues,
          weird: new Uint8Array([1]),
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
        select: ["id", "weird"],
        where: {
          text: { eq: routeEncodingValues.text },
        },
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
        groupBy: ["weird"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          text: { eq: routeEncodingValues.text },
        },
        limit: 10,
      });
      const groupedEvents = yield* groupedSubscription.events.pipe(Stream.runCollect);

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
                weird: new Uint8Array([1]),
              },
              index: 0,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(groupedEvents).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "query-1",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      yield* rawSubscription.close();
      yield* groupedSubscription.close();
      yield* Fiber.interrupt(rawEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rejects non-canonical leased gRPC route values instead of sharing a fallback key", () =>
    Effect.gen(function* () {
      const feed = grpcRouteEncodingLeasedViewServer();
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

      const symbolError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            amount: { eq: routeEncodingValues.amount },
            count: { eq: routeEncodingValues.count },
            disabled: { eq: routeEncodingValues.disabled },
            flag: { eq: routeEncodingValues.flag },
            meta: { eq: routeEncodingValues.meta },
            none: { eq: routeEncodingValues.none },
            plainScore: { eq: routeEncodingValues.plainScore },
            score: { eq: routeEncodingValues.score },
            tags: { eq: routeEncodingValues.tags },
            text: { eq: routeEncodingValues.text },
            weird: { eq: Symbol("leased-route") },
          },
          limit: 1,
        })
        .pipe(Effect.flip);
      const objectError = yield* manager.liveClient
        .subscribeRuntime("orders", {
          select: ["id"],
          where: {
            amount: { eq: routeEncodingValues.amount },
            count: { eq: routeEncodingValues.count },
            disabled: { eq: routeEncodingValues.disabled },
            flag: { eq: routeEncodingValues.flag },
            meta: { eq: routeEncodingValues.meta },
            none: { eq: routeEncodingValues.none },
            plainScore: { eq: routeEncodingValues.plainScore },
            score: { eq: routeEncodingValues.score },
            tags: { eq: routeEncodingValues.tags },
            text: { eq: routeEncodingValues.text },
            weird: { eq: new Map([["stable", "route"]]) },
          },
          limit: 1,
        })
        .pipe(Effect.flip);

      expect({
        symbolError,
        objectError,
        leasedFeeds: Object.keys(
          health.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
            ?.leased ?? {},
        ),
      }).toStrictEqual({
        symbolError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message:
            "Leased topic orders route field weird value cannot be used as a stable leased gRPC route key.",
        },
        objectError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message:
            "Leased topic orders route field weird value cannot be used as a stable leased gRPC route key.",
        },
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("opens independent leased gRPC feeds for different routes", () =>
    Effect.gen(function* () {
      const acquiredRegions: Array<string> = [];
      const feed = grpcLeasedViewServer({
        acquired: (region) => {
          acquiredRegions.push(region);
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, region.length)]),
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

      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const usaSnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        1,
      );
      const euSnapshot = yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "eu", 1);
      const routeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(acquiredRegions).toStrictEqual(["usa", "eu"]);
      expect(usaSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-order-1",
          customerId: "usa-order-1",
          price: 3,
          region: "usa",
        },
      ]);
      expect(euSnapshot.rows).toStrictEqual([
        {
          id: "eu:eu-order-1",
          customerId: "eu-order-1",
          price: 2,
          region: "eu",
        },
      ]);
      expect(Object.keys(routeHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/orders/leased/region=string%3A3%3Ausa",
        "orders/orders/leased/region=string%3A2%3Aeu",
      ]);
      yield* usa.close();
      yield* eu.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC snapshot and delta live events", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fromQueue(values),
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

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* Queue.unbounded<unknown>();
      const fiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(events);
      yield* Queue.offer(values, grpcOrderValue("order-2", 20));
      const delta = yield* Queue.take(events).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (event) =>
            typeof event === "object" && event !== null && Reflect.get(event, "type") === "delta",
        }),
      );

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: ["usa:order-1"],
        rows: [
          {
            id: "usa:order-1",
            customerId: "order-1",
            price: 10,
            region: "usa",
          },
        ],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "usa:order-2",
            row: {
              id: "usa:order-2",
              customerId: "order-2",
              price: 20,
              region: "usa",
            },
            index: 1,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* starter.close();
      yield* Fiber.interrupt(fiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("rewrites leased gRPC public row-key filters before local query execution", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fromQueue(values),
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

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* Queue.offer(values, grpcOrderValue("order-2", 20));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 2);
      const arrayFilterSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { in: ["usa:order-2"] },
        },
        limit: 10,
      });
      const scalarFilterSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: "usa:order-1",
        },
        limit: 10,
      });
      const arrayFilterEvents = yield* arrayFilterSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      const scalarFilterEvents = yield* scalarFilterSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );

      expect({
        arrayFilterEvents: Array.from(arrayFilterEvents),
        scalarFilterEvents: Array.from(scalarFilterEvents),
      }).toStrictEqual({
        arrayFilterEvents: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-1",
            version: 1,
            keys: ["usa:order-2"],
            rows: [
              {
                id: "usa:order-2",
                price: 20,
              },
            ],
            totalRows: 1,
          },
        ],
        scalarFilterEvents: [
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-2",
            version: 1,
            keys: ["usa:order-1"],
            rows: [
              {
                id: "usa:order-1",
                price: 10,
              },
            ],
            totalRows: 1,
          },
        ],
      });
      yield* scalarFilterSubscription.close();
      yield* arrayFilterSubscription.close();
      yield* starter.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC remove deltas when rows leave a result window", () =>
    Effect.gen(function* () {
      const values = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fromQueue(values),
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

      const starter = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* Queue.offer(values, grpcOrderValue("order-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 1,
      });
      const events = yield* Queue.unbounded<unknown>();
      const fiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(events);
      yield* Queue.offer(values, grpcOrderValue("order-0", 5));
      const delta = yield* Queue.take(events).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (event) =>
            typeof event === "object" && event !== null && Reflect.get(event, "type") === "delta",
        }),
      );

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: ["usa:order-1"],
        rows: [
          {
            id: "usa:order-1",
            price: 10,
          },
        ],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: "usa:order-1",
          },
          {
            type: "insert",
            key: "usa:order-0",
            row: {
              id: "usa:order-0",
              price: 5,
            },
            index: 0,
          },
        ],
        totalRows: 2,
      });
      yield* subscription.close();
      yield* starter.close();
      yield* Fiber.interrupt(fiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("passes leased gRPC runtime status events through the manager", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const statusEvent = {
        type: "status",
        topic: "orders",
        queryId: "internal-status",
        status: "ready",
        code: "Ready",
        message: "internal status",
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () =>
          Effect.succeed({
            events: Stream.make(statusEvent),
            close: () => Effect.void,
          }),
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(statusEvent.queryId).pipe(
            Effect.as({
              events: Stream.make(statusEvent),
              close: () => Effect.void,
            }),
          ),
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.make(statusEvent),
            close: () => Effect.void,
          }),
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(statusEvent.queryId).pipe(
            Effect.as({
              events: Stream.make(statusEvent),
              close: () => Effect.void,
            }),
          ),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const events = yield* subscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.timeout("1 second"),
      );

      expect(Array.from(events)).toStrictEqual([statusEvent]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
  it.live("passes through malformed internal leased rows without rewriting non-string keys", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const malformedSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "internal-malformed",
        version: 1,
        keys: ["internal-key"],
        rows: [
          {
            id: 123,
            price: 10,
          },
        ],
        totalRows: 1,
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: runtimeCore.internalLiveClient.subscribeInternal,
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.make(malformedSnapshot),
            close: () => Effect.void,
          }),
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(malformedSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(malformedSnapshot),
              close: () => Effect.void,
            }),
          ),
      };
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
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { eq: "usa:order-1" },
        },
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "internal-malformed",
          version: 1,
          keys: ["internal-key"],
          rows: [
            {
              id: 123,
              price: 10,
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

  it.live("keeps non-string leased row-key predicates unchanged in internal runtime queries", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: (_topic, query) =>
          Effect.succeed({
            events: Stream.make({
              type: "status",
              topic: "orders",
              queryId: "internal-query",
              status: "ready",
              code: "Ready",
              message: JSON.stringify(query),
            }),
            close: () => Effect.void,
          }),
        subscribeObservedInternal: (_topic, query, observer) =>
          observer.onQueryRegistered("internal-query").pipe(
            Effect.as({
              events: Stream.make({
                type: "status",
                topic: "orders",
                queryId: "internal-query",
                status: "ready",
                code: "Ready",
                message: JSON.stringify(query),
              }),
              close: () => Effect.void,
            }),
          ),
        subscribeRuntimeInternal: (_topic, query) =>
          Effect.succeed({
            events: Stream.make({
              type: "status",
              topic: "orders",
              queryId: "internal-query",
              status: "ready",
              code: "Ready",
              message: JSON.stringify(query),
            }),
            close: () => Effect.void,
          }),
        subscribeRuntimeObservedInternal: (_topic, query, observer) =>
          observer.onQueryRegistered("internal-query").pipe(
            Effect.as({
              events: Stream.make({
                type: "status",
                topic: "orders",
                queryId: "internal-query",
                status: "ready",
                code: "Ready",
                message: JSON.stringify(query),
              }),
              close: () => Effect.void,
            }),
          ),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      type LeasedOrdersRuntimeKeyQuery = {
        readonly select: readonly ["id", "price"];
        readonly where: {
          readonly region: {
            readonly eq: string;
          };
          readonly id: {
            readonly eq: string;
          };
        };
        readonly limit: 10;
      };
      const query = {
        select: ["id", "price"],
        where: {
          region: { eq: "usa" },
          id: { eq: "usa:order-1" },
        },
        limit: 10,
      } satisfies LeasedOrdersRuntimeKeyQuery;
      const subscribeEffect = manager.liveClient.subscribeRuntime("orders", query);
      Object.defineProperty(query.where.id, "eq", { value: 123 });

      const subscription = yield* subscribeEffect;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "internal-query",
          status: "ready",
          code: "Ready",
          message:
            '{"select":["id","price"],"where":{"region":{"eq":"usa"},"id":{"eq":123}},"limit":10}',
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("isolates identical public leased row keys across different routes", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServerFromCallbacks({
        request: ({ region }) => ({ orderId: region }),
        acquire: () => longRunningGrpcStream([grpcOrderValue("shared-order", 10)]),
        map: ({ value, route }) => ({
          id: value.customerId,
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

      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const usaSnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        1,
      );
      const euSnapshot = yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "eu", 1);
      yield* usa.close();
      const euAfterUsaClose = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "eu",
        1,
      );

      expect(usaSnapshot.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "usa",
        },
      ]);
      expect(euSnapshot.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "eu",
        },
      ]);
      expect(euAfterUsaClose.rows).toStrictEqual([
        {
          id: "shared-order",
          customerId: "shared-order",
          price: 10,
          region: "eu",
        },
      ]);
      yield* eu.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("isolates leased feeds by internal feed partition before local route predicates", () =>
    Effect.gen(function* () {
      const usaQueue = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const euQueue = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedViewServerFromCallbacks({
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) =>
          route.region === "usa" ? Stream.fromQueue(usaQueue) : Stream.fromQueue(euQueue),
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
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

      const eu = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("eu"));
      const euEvents = yield* Queue.unbounded<unknown>();
      const euEventsFiber = yield* eu.events.pipe(
        Stream.runForEach((event) => Queue.offer(euEvents, event)),
        Effect.forkChild,
      );
      const euInitialSnapshot = yield* Queue.take(euEvents);
      yield* Queue.offer(euQueue, grpcOrderValue("shared-order", 10));
      const euRouteMismatchedEvent = yield* Queue.take(euEvents);
      const routeMismatchSnapshot = yield* runtimeCore.internalClient.snapshot("orders", {
        select: ["id", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const usa = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const usaEventsFiber = yield* usa.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Queue.offer(usaQueue, grpcOrderValue("shared-order", 10));
      const usaEvents = yield* Fiber.join(usaEventsFiber);
      const usaSnapshot = usaEvents[0];
      const usaDelta = usaEvents[1];

      expect(usaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(usaDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "insert",
            key: "usa:shared-order",
            row: {
              id: "usa:shared-order",
              customerId: "shared-order",
              price: 10,
              region: "usa",
            },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(euInitialSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(euRouteMismatchedEvent).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream failed.",
      });
      expect(routeMismatchSnapshot).toStrictEqual({
        rows: [],
        totalRows: 0,
        version: 0,
        status: "ready",
        statusCode: "Ready",
      });
      yield* usa.close();
      yield* eu.close();
      yield* Fiber.interrupt(euEventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("shares one leased gRPC feed while applying different local filters", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([
            grpcOrderValue(`${region}-cheap`, 10),
            grpcOrderValue(`${region}-expensive`, 90),
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

      const cheap = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price", "region"],
        where: {
          region: { eq: "usa" },
          price: { lte: 20 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const expensive = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "price", "region"],
        where: {
          region: { eq: "usa" },
          price: { gte: 50 },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const cheapSnapshot = yield* runtimeCore.internalClient
        .snapshot("orders", {
          select: ["id", "price", "region"],
          where: {
            region: { eq: "usa" },
            price: { lte: 20 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        })
        .pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (snapshot) => snapshot.totalRows === 1,
          }),
        );
      const expensiveSnapshot = yield* runtimeCore.internalClient
        .snapshot("orders", {
          select: ["id", "price", "region"],
          where: {
            region: { eq: "usa" },
            price: { gte: 50 },
          },
          orderBy: [{ field: "price", direction: "asc" }],
          limit: 10,
        })
        .pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (snapshot) => snapshot.totalRows === 1,
          }),
        );

      expect(acquired).toBe(1);
      expect(cheapSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-cheap",
          price: 10,
          region: "usa",
        },
      ]);
      expect(expensiveSnapshot.rows).toStrictEqual([
        {
          id: "usa:usa-expensive",
          price: 90,
          region: "usa",
        },
      ]);
      yield* cheap.close();
      yield* expensive.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
  it.live("keeps refined public leased row keys out of internal storage keys", () =>
    Effect.gen(function* () {
      const feed = grpcPublicKeyLeasedViewServer({
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const options = yield* resolveViewServerRuntimeOptions(feed);
      const grpcOptions = yield* Effect.fromNullishOr(options.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(feed, {});
      const health = makeDefaultRuntimeDependencies<typeof feed.topics>().makeGrpcHealthLedger(
        feed,
        grpcOptions,
      );
      const manager = yield* makeViewServerGrpcLeaseManager(
        feed,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const firstSubscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "price", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 1,
          keys: ["public-usa-usa-order-1"],
          rows: [
            {
              id: "public-usa-usa-order-1",
              customerId: "usa-order-1",
              price: 10,
              region: "usa",
            },
          ],
          totalRows: 1,
        },
      ]);
      yield* subscription.close();
      yield* firstSubscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("subscribes topic-owned grpcSource leased feeds", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const config = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions(config, {
        grpc: {
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
      });
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const health = makeDefaultRuntimeDependencies<typeof config.topics>().makeGrpcHealthLedger(
        config,
        grpcOptions,
      );
      const manager = yield* makeViewServerGrpcLeaseManager(
        config,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscription = yield* manager.liveClient.subscribe("orders", {
        select: ["id", "customerId", "region"],
        where: {
          region: { eq: "usa" },
        },
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);

      expect(acquired).toBe(1);
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
              key: "usa:usa-order-1",
              row: {
                id: "usa:usa-order-1",
                customerId: "usa-order-1",
                region: "usa",
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

  it.live("externalizes a 200,000-row grouped snapshot without caller-side spread limits", () =>
    Effect.gen(function* () {
      const cardinality = 200_000;
      const midpoint = Math.floor(cardinality / 2);
      const internalKeys = Array.from(
        { length: cardinality },
        (_value, index) => "customer-internal-" + index,
      );
      const rows = Array.from({ length: cardinality }, (_value, index) => ({
        customerId: "customer-" + index,
        rowCount: 1n,
      }));
      const internalSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "large-customer-groups",
        version: 0,
        keys: internalKeys,
        rows,
        totalRows: cardinality,
      } as const;
      const publicGroupedCustomerKey = (customerId: string): string => {
        const fieldToken =
          '["array",[["string","customerId"],["string",' + JSON.stringify(customerId) + "]]]";
        return '["array",[' + fieldToken + "]]";
      };
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {}),
        (runtimeCore) => {
          const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
            typeof leasedGrpcViewServer.topics
          > = {
            ...runtimeCore.internalLiveClient,
            subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
              observer.onQueryRegistered(internalSnapshot.queryId).pipe(
                Effect.as({
                  events: Stream.make(internalSnapshot).pipe(Stream.concat(Stream.never)),
                  close: () => Effect.void,
                }),
              ),
          };
          const health = makeLeasedGrpcHealth(grpcOptions);

          return Effect.acquireUseRelease(
            makeViewServerGrpcLeaseManager(
              grpcOptions.sourceConfig,
              runtimeCore.internalClient,
              runtimeCore.liveClient,
              fakeInternalLiveClient,
              Effect.void,
              grpcOptions,
              health,
            ),
            (manager) =>
              Effect.acquireUseRelease(
                manager.liveClient.subscribeRuntime("orders", {
                  groupBy: ["customerId"],
                  aggregates: {
                    rowCount: { aggFunc: "count" },
                  },
                  where: {
                    region: { eq: "usa" },
                  },
                  limit: cardinality,
                }),
                (subscription) =>
                  Effect.gen(function* () {
                    const firstEventOption = yield* Stream.runHead(subscription.events);
                    const firstEvent: ViewServerLiveEvent<object> =
                      yield* Effect.fromOption(firstEventOption);
                    const snapshot = yield* Effect.succeed(firstEvent).pipe(
                      Effect.filterOrFail(
                        (
                          event,
                        ): event is Extract<typeof firstEvent, { readonly type: "snapshot" }> =>
                          event.type === "snapshot",
                      ),
                    );

                    expect(snapshot.keys.length).toBe(cardinality);
                    expect(snapshot.keys[0]).toBe(publicGroupedCustomerKey("customer-0"));
                    expect(snapshot.keys[cardinality - 1]).toBe(
                      publicGroupedCustomerKey("customer-" + (cardinality - 1)),
                    );
                    expect(snapshot.rows.length).toBe(cardinality);
                    expect([
                      snapshot.rows[0],
                      snapshot.rows[midpoint],
                      snapshot.rows[cardinality - 1],
                    ]).toStrictEqual([
                      { customerId: "customer-0", rowCount: 1n },
                      { customerId: "customer-" + midpoint, rowCount: 1n },
                      { customerId: "customer-" + (cardinality - 1), rowCount: 1n },
                    ]);
                    expect(snapshot.totalRows).toBe(cardinality);
                  }),
                (subscription) => subscription.close(),
              ),
            (manager) => manager.close,
          );
        },
        (runtimeCore) => runtimeCore.close,
      );
    }),
  );
});
