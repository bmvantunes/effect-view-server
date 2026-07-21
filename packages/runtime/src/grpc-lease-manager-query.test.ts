import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type {
  ViewServerRuntimeCoreInternalLiveClient,
  ViewServerRuntimeCoreTerminalObserver,
} from "@effect-view-server/runtime-core/internal";
import { Clock, Effect, Fiber, Queue, Schedule, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeDefaultGrpcRuntimeSourceDependencies,
  resolveGrpcRuntimeSourceOptions as resolveViewServerRuntimeOptions,
} from "./grpc-runtime-source";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { makeLeasedGrpcRuntimeHarness } from "../test-harness/grpc-runtime";

import { grpcOrderValue } from "../test-harness/grpc-config";
import {
  fastGrpcMaterializedReconnect,
  resolveLeasedGrpcRuntimeOptions,
} from "../test-harness/grpc-materialized";
import {
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

const usaFeedKey = "orders/orders/leased/region=%5B%22string%22%2C%22usa%22%5D";
const euFeedKey = "orders/orders/leased/region=%5B%22string%22%2C%22eu%22%5D";

describe("gRPC lease manager query translation", () => {
  it.live("rejects unsnapshotable typed and runtime leased queries before acquisition", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: () => Stream.never,
      });
      const harness = yield* makeLeasedGrpcRuntimeHarness({ config: feed });
      const hostileThrownValue = {
        toString: () => {
          throw new Error("hostile toString must never run");
        },
      };
      const hostileHandler = {
        ownKeys: () => {
          throw hostileThrownValue;
        },
      };
      const typedQuery = new Proxy(leasedOrdersQuery("usa"), hostileHandler);
      const runtimeQuery = new Proxy(leasedOrdersQuery("usa"), hostileHandler);

      const typedError = yield* harness.manager.liveClient
        .subscribe("orders", typedQuery)
        .pipe(Effect.flip);
      const runtimeError = yield* harness.manager.liveClient
        .subscribeRuntime("orders", runtimeQuery)
        .pipe(Effect.flip);

      expect({ acquired, runtimeError, typedError }).toStrictEqual({
        acquired: 0,
        runtimeError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Query input could not be snapshotted.",
        },
        typedError: {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Query input could not be snapshotted.",
        },
      });
      yield* harness.manager.close;
      yield* harness.runtimeCore.close;
    }),
  );

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
        usaFeedKey,
      ]);
      expect(readyHealth.grpc?.feeds["orders"]?.leased[usaFeedKey]).toStrictEqual({
        status: "ready",
        lifecycle: "leased",
        feedName: "orders",
        feedKey: usaFeedKey,
        topic: "orders",
        subscriberCount: 1,
        rowCount: 2,
        messagesPerSecond: 2,
        rowsPerSecond: 2,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: readyHealth.grpc?.feeds["orders"]?.leased[usaFeedKey]?.lastMessageAt,
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
      expect(sharedHealth.grpc?.feeds["orders"]?.leased[usaFeedKey]?.subscriberCount).toBe(2);
      expect(afterFirstClose.grpc?.feeds["orders"]?.leased[usaFeedKey]?.subscriberCount).toBe(1);
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
        routeBy: { region: "usa" },
        select: ["id", "price"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
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
        routeBy: { id: "order-1" },
        select: ["id", "customerId", "price"],
        where: [{ field: "id", type: "equals", filter: "order-1" }],
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
        routeBy: {
          amount: routeEncodingValues.amount,
          count: routeEncodingValues.count,
          disabled: routeEncodingValues.disabled,
          flag: routeEncodingValues.flag,
          none: routeEncodingValues.none,
          plainScore: routeEncodingValues.plainScore,
          score: routeEncodingValues.score,
          text: routeEncodingValues.text,
        },
        select: ["id"],
        limit: 10,
      });
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([
        "orders/orders/leased/amount=%5B%22bigDecimal%22%2C%2212345%22%2C%222%22%5D&count=%5B%22bigint%22%2C%229007199254740993%22%5D&disabled=%5B%22boolean%22%2Cfalse%5D&flag=%5B%22boolean%22%2Ctrue%5D&none=%5B%22null%22%5D&plainScore=%5B%22number%22%2C%2242%22%5D&score=%5B%22number%22%2C%22-0%22%5D&text=%5B%22string%22%2C%22route%22%5D",
      ]);
      yield* subscription.close();
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
        usaFeedKey,
        euFeedKey,
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
        routeBy: { region: "usa" },
        select: ["id", "price"],
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "id", type: "in", filter: ["usa:order-2"] },
        ],
        limit: 10,
      });
      const scalarFilterSubscription = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        select: ["id", "price"],
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "id", type: "equals", filter: "usa:order-1" },
        ],
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
        routeBy: { region: "usa" },
        select: ["id", "price"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
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
        subscribeObservedInternal: (
          _topic: string,
          _query: unknown,
          observer: ViewServerRuntimeCoreTerminalObserver,
        ) =>
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
  it.live("rejects malformed internal leased Row Keys through the public subscription", () =>
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
        routeBy: { region: "usa" },
        select: ["id", "price"],
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "id", type: "equals", filter: "usa:order-1" },
        ],
        limit: 10,
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "internal-malformed",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC internal Row Key does not belong to the acquired feed identity.",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("captures leased row-key predicates before caller mutation", () =>
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
        subscribeObservedInternal: (
          _topic: string,
          query: unknown,
          observer: ViewServerRuntimeCoreTerminalObserver,
        ) =>
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
        readonly routeBy: { readonly region: string };
        readonly where: readonly [
          { readonly field: "region"; readonly type: "equals"; readonly filter: string },
          { readonly field: "id"; readonly type: "equals"; readonly filter: string },
        ];
        readonly limit: 10;
      };
      const idCondition = {
        field: "id",
        type: "equals",
        filter: "usa:order-1",
      } satisfies LeasedOrdersRuntimeKeyQuery["where"][1];
      const query = {
        select: ["id", "price"],
        routeBy: { region: "usa" },
        where: [{ field: "region", type: "equals", filter: "usa" }, idCondition],
        limit: 10,
      } satisfies LeasedOrdersRuntimeKeyQuery;
      const subscribeEffect = manager.liveClient.subscribeRuntime("orders", query);
      Object.defineProperty(idCondition, "filter", { value: 123 });

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
            '{"select":["id","price"],"where":[{"field":"region","type":"equals","filter":"usa"},{"field":"id","type":"equals","filter":"usa:order-1"}],"limit":10}',
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
        routeBy: { region: "eu" },
        select: ["id", "region"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
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
        routeBy: { region: "usa" },
        select: ["id", "price", "region"],
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "price", type: "lessThanOrEqual", filter: 20 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const expensive = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        select: ["id", "price", "region"],
        where: [
          { field: "region", type: "equals", filter: "usa" },
          { field: "price", type: "greaterThanOrEqual", filter: 50 },
        ],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const cheapSnapshot = yield* runtimeCore.internalClient
        .snapshot("orders", {
          routeBy: { region: "usa" },
          select: ["id", "price", "region"],
          where: [
            { field: "region", type: "equals", filter: "usa" },
            { field: "price", type: "lessThanOrEqual", filter: 20 },
          ],
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
          routeBy: { region: "usa" },
          select: ["id", "price", "region"],
          where: [
            { field: "region", type: "equals", filter: "usa" },
            { field: "price", type: "greaterThanOrEqual", filter: 50 },
          ],
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
      const grpcOptions = yield* Effect.fromNullishOr(options);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(feed, {});
      const health = makeDefaultGrpcRuntimeSourceDependencies<
        typeof feed.topics
      >().makeHealthLedger(feed, grpcOptions);
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
        routeBy: { region: "usa" },
        select: ["id", "customerId", "price", "region"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const subscription = yield* manager.liveClient.subscribe("orders", {
        routeBy: { region: "usa" },
        select: ["id", "customerId", "price", "region"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
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
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const health = makeDefaultGrpcRuntimeSourceDependencies<
        typeof config.topics
      >().makeHealthLedger(config, grpcOptions);
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
        routeBy: { region: "usa" },
        select: ["id", "customerId", "region"],
        where: [{ field: "region", type: "equals", filter: "usa" }],
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
});
