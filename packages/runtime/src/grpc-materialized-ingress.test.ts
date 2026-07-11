import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalClient } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Fiber, Queue, Schedule, Stream } from "effect";
import { makeDefaultRuntimeDependencies } from "./internal";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcIngress } from "./grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import {
  makeMaterializedGrpcRuntimeHarness,
  readGrpcHealthOverlay,
  readGrpcHealthOverlayNow,
} from "../test-harness/grpc-runtime";

import {
  grpcClients,
  GrpcOrder,
  grpcOrderValue,
  grpcTopicSources,
} from "../test-harness/grpc-config";
import {
  fastGrpcMaterializedReconnect,
  grpcHealthClient,
  grpcHealthFeed,
  grpcMaterializedViewServer,
  grpcMaterializedViewServerFromCallbacks,
  grpcMaterializedViewServerWithAcquireFailure,
  grpcMaterializedViewServerWithMappingFailure,
  grpcMaterializedViewServerWithOrphanClient,
  grpcMaterializedViewServerWithRelease,
  grpcMaterializedViewServerWithRequestFailure,
  makeGrpcHealth,
  resolveGrpcRuntimeOptions,
  resolveLeasedGrpcRuntimeOptions,
  waitForGrpcSnapshotRows,
} from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
  leasedGrpcViewServer,
  leasedOrdersQuery,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";

import type { GrpcOrderValueMessage, GrpcTopics } from "../test-harness/grpc-config";

describe("Materialized gRPC ingress", () => {
  it.live(
    "delivers terminal status when a leased gRPC stream completes before publishing rows",
    () =>
      Effect.gen(function* () {
        const feed = grpcLeasedViewServer({
          streamForRegion: () => Stream.empty,
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

        const subscription = yield* manager.liveClient.subscribe(
          "orders",
          leasedOrdersQuery("usa"),
        );
        const eventQueue = yield* Queue.unbounded<unknown>();
        const eventsFiber = yield* subscription.events.pipe(
          Stream.runForEach((event) => Queue.offer(eventQueue, event)),
          Effect.forkChild,
        );
        const snapshotEvent = yield* Queue.take(eventQueue);
        const terminalStatus = yield* Queue.take(eventQueue).pipe(Effect.timeout("1 second"));
        const cleanedHealth = yield* Effect.gen(function* () {
          return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        }).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) =>
              Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
          }),
        );

        expect({
          snapshotEvent,
          terminalStatus,
          runtimeStatus: cleanedHealth.status,
          leasedFeeds: Object.keys(cleanedHealth.grpc?.feeds["orders"]?.leased ?? {}),
        }).toStrictEqual({
          snapshotEvent: {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
          terminalStatus: {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "error",
            code: "RuntimeUnavailable",
            message: "gRPC leased upstream completed unexpectedly.",
          },
          runtimeStatus: "ready",
          leasedFeeds: [],
        });
        yield* subscription.close();
        yield* Fiber.interrupt(eventsFiber);
        yield* manager.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks materialized gRPC feed degraded when stream completion exhausts reconnects", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.make(grpcOrderValue("order-1", 10)));
      const harness = yield* makeMaterializedGrpcRuntimeHarness({
        config: feed,
        grpc: {
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
      });

      const degradedHealth = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => {
            const feedHealth = grpcHealthFeed(currentHealth);
            return (
              feedHealth?.status === "degraded" &&
              feedHealth.reconnects === 3 &&
              feedHealth.lastError === "gRPC feed orders completed unexpectedly."
            );
          },
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed orders completed unexpectedly.",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(3);
      expect(grpcHealthClient(degradedHealth)?.activeFeeds).toBe(0);
      yield* harness.close;
    }),
  );

  it.live(
    "marks materialized gRPC feed degraded when delayed stream completion exhausts reconnects",
    () =>
      Effect.gen(function* () {
        let acquireCount = 0;
        const streams = [
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(Stream.drain),
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(Stream.drain),
        ];
        const feed = grpcMaterializedViewServerFromCallbacks({
          request: () => ({ orderId: "all" }),
          acquire: () => {
            const stream = streams[acquireCount] ?? Stream.never;
            acquireCount += 1;
            return stream;
          },
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const harness = yield* makeMaterializedGrpcRuntimeHarness({
          config: feed,
          grpc: {
            materializedReconnect: {
              delay: "10 millis",
              maxReconnects: 1,
            },
          },
        });

        const degradedHealth = yield* readGrpcHealthOverlayNow(
          harness.runtimeCore.client,
          harness.health,
        ).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
          }),
        );

        expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
          "gRPC feed orders completed unexpectedly.",
        );
        expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(1);
        expect(acquireCount).toBe(2);
        yield* harness.close;
      }),
  );

  it.live("uses one materialized gRPC reconnect budget across completion and failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      const streams = [Stream.empty, Stream.fail("upstream down"), Stream.never];
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const harness = yield* makeMaterializedGrpcRuntimeHarness({
        config: feed,
        grpc: {
          materializedReconnect: {
            delay: "10 millis",
            maxReconnects: 1,
          },
        },
      });

      const degradedHealth = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        acquireCount,
        lastError: grpcHealthFeed(degradedHealth)?.lastError,
        reconnects: grpcHealthFeed(degradedHealth)?.reconnects,
      }).toStrictEqual({
        acquireCount: 2,
        lastError: "gRPC feed orders failed: gRPC feed stream failed for orders: upstream down",
        reconnects: 1,
      });
      yield* harness.close;
    }),
  );

  it.live("marks materialized gRPC feed stopping when the stream is interrupted", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.failCause(Cause.interrupt()));
      const harness = yield* makeMaterializedGrpcRuntimeHarness({
        config: feed,
        grpc: {
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
      });

      const stoppingHealth = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect(grpcHealthFeed(stoppingHealth)?.lastError).toBe(null);
      yield* harness.close;
    }),
  );

  it.live("keeps materialized gRPC stream interruption terminal when release fails", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.failCause(Cause.interrupt()),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.fail("release down");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const harness = yield* makeMaterializedGrpcRuntimeHarness({
        config: feed,
        grpc: {
          materializedReconnect: fastGrpcMaterializedReconnect,
        },
      });

      const stoppingHealth = yield* readGrpcHealthOverlayNow(
        harness.runtimeCore.client,
        harness.health,
      ).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect({
        lastError: grpcHealthFeed(stoppingHealth)?.lastError,
        reconnects: grpcHealthFeed(stoppingHealth)?.reconnects,
        releaseCount,
      }).toStrictEqual({
        lastError: null,
        reconnects: 0,
        releaseCount: 1,
      });
      yield* harness.close;
    }),
  );

  it.live(
    "does not reconnect materialized gRPC feed when failure cause includes interruption",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedViewServer(
          Stream.failCause(
            Cause.fromReasons([
              Cause.makeFailReason("upstream down during shutdown"),
              Cause.makeInterruptReason(),
            ]),
          ),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
          }),
        );

        expect(grpcHealthFeed(stoppingHealth)).toStrictEqual({
          status: "stopping",
          lifecycle: "materialized",
          feedName: "orders",
          feedKey: "orders/orders/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        });
        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("publishes materialized gRPC stream rows into runtime core and health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(
        longRunningGrpcStream([grpcOrderValue("order-1", 10), grpcOrderValue("order-2", 5)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          orders: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 2);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const clientHealth = currentHealth.grpc?.clients["orders"];
      const feedHealth = currentHealth.grpc?.feeds["orders"]?.materialized["orders"];

      expect(snapshot).toStrictEqual({
        rows: [
          { id: "order-2", price: 5 },
          { id: "order-1", price: 10 },
        ],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(clientHealth).toStrictEqual({
        status: "connected",
        baseUrl: "https://orders.example.test",
        activeFeeds: 1,
        lastConnectedAt: clientHealth?.lastConnectedAt,
        lastError: null,
      });
      expect(feedHealth).toStrictEqual({
        status: "ready",
        lifecycle: "materialized",
        feedName: "orders",
        feedKey: "orders/orders/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 2,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: feedHealth?.lastMessageAt,
        lastError: null,
      });
      expect(typeof clientHealth?.lastConnectedAt).toBe("number");
      expect(typeof feedHealth?.lastMessageAt).toBe("number");

      yield* ingress.close;
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(stoppedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.status).toBe("stopping");
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "reports materialized gRPC row count from engine health instead of cumulative publishes",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedViewServer(
          longRunningGrpcStream([
            grpcOrderValue("order-1", 10),
            grpcOrderValue("order-1", 20),
            grpcOrderValue("order-1", 30),
          ]),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(snapshot).toStrictEqual({
          rows: [{ id: "order-1", price: 30 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(currentHealth.grpc?.feeds["orders"]?.materialized["orders"]?.rowCount).toBe(1);

        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks materialized gRPC feed degraded when the stream defects", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.fromEffect(Effect.die("defect down")));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("gRPC feed orders failed:");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("defect down");
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("does not reset materialized gRPC reconnect budget during slow release", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => {
          acquireCount += 1;
          return Stream.fail("upstream down");
        },
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Effect.sleep("25 millis");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed, {
        delay: "10 millis",
        maxReconnects: 1,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        acquireCount,
        releaseCount,
        reconnects: grpcHealthFeed(degradedHealth)?.reconnects,
      }).toStrictEqual({
        acquireCount: 2,
        releaseCount: 2,
        reconnects: 1,
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("resets materialized gRPC reconnect failure streak after publishing a batch", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      const failAfterProgress = yield* Deferred.make<void>();
      const streams = [
        Stream.fail("first transient failure"),
        Stream.make(grpcOrderValue("progress-row", 11)).pipe(
          Stream.concat(
            Stream.fromEffect(Deferred.await(failAfterProgress)).pipe(
              Stream.drain,
              Stream.concat(Stream.fail("second transient failure after progress")),
            ),
          ),
        ),
        longRunningGrpcStream([grpcOrderValue("final-row", 12)]),
      ];
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed, {
        delay: "10 millis",
        maxReconnects: 1,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const progressSnapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
      yield* Deferred.succeed(failAfterProgress, undefined);
      const finalSnapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 2);
      const finalHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.reconnects === 2,
        }),
      );

      expect(progressSnapshot).toStrictEqual({
        rows: [{ id: "progress-row", price: 11 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(finalSnapshot).toStrictEqual({
        rows: [
          { id: "progress-row", price: 11 },
          { id: "final-row", price: 12 },
        ],
        totalRows: 2,
        version: 2,
        status: "ready",
        statusCode: "Ready",
      });
      expect(grpcHealthFeed(finalHealth)?.reconnects).toBe(2);
      expect(acquireCount).toBe(3);

      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "resets materialized gRPC reconnect failure streak after staying open for one delay",
    () =>
      Effect.gen(function* () {
        let acquireCount = 0;
        const streams = [
          Stream.fail("first transient failure"),
          Stream.fromEffect(Effect.sleep("20 millis")).pipe(
            Stream.drain,
            Stream.concat(Stream.fail("second transient failure after stable open")),
          ),
          longRunningGrpcStream([grpcOrderValue("stable-reset-row", 13)]),
        ];
        const feed = grpcMaterializedViewServerFromCallbacks({
          request: () => ({ orderId: "all" }),
          acquire: () => {
            const stream = streams[acquireCount] ?? Stream.never;
            acquireCount += 1;
            return stream;
          },
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed, {
          delay: "10 millis",
          maxReconnects: 1,
        });
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
        const finalHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.reconnects === 2,
          }),
        );

        expect(snapshot).toStrictEqual({
          rows: [{ id: "stable-reset-row", price: 13 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(grpcHealthFeed(finalHealth)?.reconnects).toBe(2);
        expect(acquireCount).toBe(3);

        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("reconnects materialized gRPC feed after a transient upstream failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const streams = [
        Stream.fail("upstream down"),
        longRunningGrpcStream([grpcOrderValue("order-after-reconnect", 10)]),
      ];
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => {
          const stream = streams[acquireCount] ?? Stream.never;
          acquireCount += 1;
          return stream;
        },
        release: () =>
          Effect.sync(() => {
            releaseCount += 1;
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const readyHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => {
            const currentFeed = grpcHealthFeed(currentHealth);
            return (
              currentFeed?.status === "ready" &&
              currentFeed.reconnects === 1 &&
              currentFeed.rowCount === 1
            );
          },
        }),
      );
      const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
      const feedHealth = grpcHealthFeed(readyHealth);

      expect(snapshot).toStrictEqual({
        rows: [{ id: "order-after-reconnect", price: 10 }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(feedHealth).toStrictEqual({
        status: "ready",
        lifecycle: "materialized",
        feedName: "orders",
        feedKey: "orders/orders/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 1,
        messagesPerSecond: 1,
        rowsPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 1,
        lastMessageAt: feedHealth?.lastMessageAt,
        lastError: null,
      });
      expect(acquireCount).toBe(2);
      expect(releaseCount).toBe(1);

      yield* ingress.close;
      expect(releaseCount).toBe(2);
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when release fails after stream failure", () =>
    Effect.gen(function* () {
      let acquireCount = 0;
      let releaseCount = 0;
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => {
          acquireCount += 1;
          return Stream.fail("upstream down");
        },
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.fail("release down");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed, {
        delay: "10 millis",
        maxReconnects: 3,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed orders failed: gRPC feed release failed for orders: release down",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(acquireCount).toBe(1);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when release defects after completion", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.empty,
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.die("release defect");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("gRPC feed orders failed:");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("release defect");
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed stopping when release is interrupted", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.fail("upstream down"),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            return yield* Effect.interrupt;
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect(grpcHealthFeed(stoppingHealth)?.lastError).toBe(null);
      expect(grpcHealthFeed(stoppingHealth)?.reconnects).toBe(0);
      expect(releaseCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed health degraded when the stream fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.fail("upstream down"));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed, {
        delay: "10 millis",
        maxReconnects: 0,
      });
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          orders: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlay(runtimeCore.client, health, 2_000).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.materialized["orders"]?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(degradedHealth.grpc?.clients["orders"]?.status).toBe("degraded");
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.status).toBe("degraded");
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.lastError).toBe(
        "gRPC feed orders failed: gRPC feed stream failed for orders: upstream down",
      );
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.reconnects).toBe(0);

      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases materialized gRPC feed resources when ingress closes", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const feed = grpcMaterializedViewServerWithRelease(
        Stream.never,
        Deferred.succeed(released, undefined),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      yield* Deferred.await(released);
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      ).toStrictEqual({
        status: "stopping",
        lifecycle: "materialized",
        feedName: "orders",
        feedKey: "orders/orders/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "does not reconnect completed materialized gRPC feed when close starts during release",
    () =>
      Effect.gen(function* () {
        let releaseCount = 0;
        const releaseStarted = yield* Deferred.make<void>();
        const releaseContinue = yield* Deferred.make<void>();
        const feed = grpcMaterializedViewServerFromCallbacks({
          request: () => ({ orderId: "all" }),
          acquire: () => Stream.empty,
          release: () =>
            Effect.gen(function* () {
              releaseCount += 1;
              yield* Deferred.succeed(releaseStarted, undefined);
              yield* Deferred.await(releaseContinue);
            }),
          map: ({ value }) => ({
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          }),
        });
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        );

        yield* Deferred.await(releaseStarted);
        const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
        yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
          }),
        );
        yield* Deferred.succeed(releaseContinue, undefined);
        yield* Fiber.join(closeFiber);

        expect(releaseCount).toBe(1);
        expect(
          grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
        ).toStrictEqual({
          status: "stopping",
          lifecycle: "materialized",
          feedName: "orders",
          feedKey: "orders/orders/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        });
        yield* runtimeCore.close;
      }),
  );

  it.live("ignores materialized gRPC release failure when close starts during release", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseStarted = yield* Deferred.make<void>();
      const releaseContinue = yield* Deferred.make<void>();
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.empty,
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Deferred.succeed(releaseStarted, undefined);
            yield* Deferred.await(releaseContinue);
            return yield* Effect.fail("release down after close");
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* Deferred.await(releaseStarted);
      const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
      yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );
      yield* Deferred.succeed(releaseContinue, undefined);
      yield* Fiber.join(closeFiber);

      expect({
        releaseCount,
        feed: grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      }).toStrictEqual({
        releaseCount: 1,
        feed: {
          status: "stopping",
          lifecycle: "materialized",
          feedName: "orders",
          feedKey: "orders/orders/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        },
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("does not reconnect failed materialized gRPC feed when close starts during release", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseStarted = yield* Deferred.make<void>();
      const releaseContinue = yield* Deferred.make<void>();
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.fail("upstream down"),
        release: () =>
          Effect.gen(function* () {
            releaseCount += 1;
            yield* Deferred.succeed(releaseStarted, undefined);
            yield* Deferred.await(releaseContinue);
          }),
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* Deferred.await(releaseStarted);
      const closeFiber = yield* ingress.close.pipe(Effect.forkChild);
      yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );
      yield* Deferred.succeed(releaseContinue, undefined);
      yield* Fiber.join(closeFiber);

      expect(releaseCount).toBe(1);
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      ).toStrictEqual({
        status: "stopping",
        lifecycle: "materialized",
        feedName: "orders",
        feedKey: "orders/orders/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores materialized gRPC release construction failures during ingress close", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all-orders" }),
        acquire: () => Stream.never,
        release: () => {
          throw new Error("release exploded");
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000))?.status,
      ).toBe("stopping");
      yield* runtimeCore.close;
    }),
  );

  it.live("refreshes materialized gRPC health after an idle feed becomes ready", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(grpcHealthFeed(readyHealth)?.status).toBe("ready");
      expect(grpcHealthClient(readyHealth)?.activeFeeds).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores reconnect health updates for unknown gRPC feeds", () =>
    Effect.gen(function* () {
      const grpcOptions = yield* resolveGrpcRuntimeOptions(
        grpcMaterializedViewServer(Stream.never),
      );
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);

      yield* health.feedReconnecting("missingFeed", "ignored reconnect");
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(grpcHealthFeed(currentHealth)).toStrictEqual({
        status: "starting",
        lifecycle: "materialized",
        feedName: "orders",
        feedKey: "orders/orders/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when request creation fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerWithRequestFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC feed request creation failed for orders");
      expect(error.feedName).toBe("orders");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when acquire does not return a Stream", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.never);
      Object.defineProperty(feed.topics.orders.grpcSource, "acquire", {
        value: () => "not-a-stream",
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        status: degradedHealth.status,
        feed: grpcHealthFeed(degradedHealth),
      }).toStrictEqual({
        status: "degraded",
        feed: {
          status: "degraded",
          lifecycle: "materialized",
          feedName: "orders",
          feedKey: "orders/orders/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 3,
          lastMessageAt: null,
          lastError:
            "gRPC feed orders failed: gRPC feed acquire did not return a Stream for orders: not-a-stream",
        },
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when client creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
          () => {
            throw new Error("client factory exploded");
          },
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC client creation failed for orders");
      expect(error.feedName).toBe("orders");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when release does not return an Effect", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all-orders" }),
        acquire: () => Stream.empty,
        release: () => Effect.void,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      Object.defineProperty(feed.topics.orders.grpcSource, "release", {
        value: () => "not-an-effect",
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect({
        status: degradedHealth.status,
        feed: grpcHealthFeed(degradedHealth),
      }).toStrictEqual({
        status: "degraded",
        feed: {
          status: "degraded",
          lifecycle: "materialized",
          feedName: "orders",
          feedKey: "orders/orders/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError:
            "gRPC feed orders failed: gRPC feed release did not return an Effect for orders: not-an-effect",
        },
      });
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores leased feeds in the materialized gRPC ingress", () =>
    Effect.gen(function* () {
      let acquired = 0;
      const feed = grpcLeasedViewServer({
        acquired: () => {
          acquired += 1;
        },
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {},
      });

      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(acquired).toBe(0);
      expect(Object.keys(currentHealth.grpc?.feeds ?? {})).toStrictEqual([]);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes already-started gRPC feed resources when another feed fails startup", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const mapOrder = ({ value }: { readonly value: GrpcOrderValueMessage }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      });
      const config = defineViewServerConfig({
        grpc: { clients: grpcClients },
        topics: {
          activeOrders: grpcTopicSources.materialized({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => ({ orderId: "running" }),
            acquire: () => Stream.never,
            release: () => Deferred.succeed(released, undefined),
            map: mapOrder,
          }),
          failingOrders: grpcTopicSources.materialized({
            schema: GrpcOrder,
            key: "id",
            client: "orders",
            method: "streamOrders",
            request: () => {
              throw new Error("request exploded");
            },
            acquire: () => Stream.never,
            map: mapOrder,
          }),
        },
      });
      const resolvedOptions = yield* resolveViewServerRuntimeOptions(config);
      const grpcOptions = yield* Effect.fromNullishOr(resolvedOptions.grpcOptions);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      const health = makeDefaultRuntimeDependencies<typeof config.topics>().makeGrpcHealthLedger(
        config,
        grpcOptions,
      );

      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          config,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      yield* Deferred.await(released);
      expect(error.message).toBe("gRPC feed request creation failed for failingOrders");
      expect(error.feedName).toBe("failingOrders");
      expect(error.topic).toBe("failingOrders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client is missing", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerWithOrphanClient();
      const resolvedOptions = yield* resolveGrpcRuntimeOptions(feed);
      const grpcOptions = {
        ...resolvedOptions,
        clients: {},
        clientBaseUrls: {},
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          orders: {
            client: "orphan",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed orders references missing client: orphan");
      expect(error.feedName).toBe("orders");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client URL is unresolved", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(Stream.never);
      const resolvedOptions = yield* resolveGrpcRuntimeOptions(feed);
      const grpcOptions = {
        ...resolvedOptions,
        clientBaseUrls: {},
      };
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          orders: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcOptions.sourceConfig,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed orders references unresolved client URL: orders");
      expect(error.feedName).toBe("orders");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when acquire throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerWithAcquireFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthClient(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed orders failed: gRPC feed acquire failed for orders: acquire exploded",
      );
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(3);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC mapping failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerWithMappingFailure(
        longRunningGrpcStream([grpcOrderValue("order-1", 10)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed orders failed: gRPC feed mapping failed for orders: mapping exploded",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC invalid mapped rows as mapping failures", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServerFromCallbacks({
        request: () => ({ orderId: "all" }),
        acquire: () => longRunningGrpcStream([grpcOrderValue("invalid-materialized-row", 10)]),
        map: ({ value }) => {
          const row = {
            id: value.customerId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: "usa",
            updatedAt: value.updatedAt,
          };
          Object.defineProperty(row, "status", { value: "not-a-status" });
          return row;
        },
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain(
        "gRPC feed mapping produced an invalid row for orders",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "marks materialized gRPC feed degraded when topic metadata disappears before mapping",
    () =>
      Effect.gen(function* () {
        const localViewServer = grpcMaterializedViewServer(
          longRunningGrpcStream([grpcOrderValue("order-without-topic", 10)]),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(localViewServer);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
        const health = makeViewServerGrpcHealthLedger<typeof localViewServer.topics>({
          clients: grpcOptions.clientBaseUrls,
          feeds: {
            orders: {
              client: "orders",
              lifecycle: "materialized",
              topic: "orders",
            },
          },
        });
        Reflect.deleteProperty(localViewServer.topics, "orders");
        const ingress = yield* makeViewServerGrpcIngress(
          localViewServer,
          runtimeCore.internalClient,
          Effect.void,
          grpcOptions,
          health,
        );

        const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) =>
              currentHealth.grpc?.feeds["orders"]?.materialized["orders"]?.status === "degraded",
          }),
        );

        expect({
          runtimeStatus: degradedHealth.status,
          feedStatus: degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.status,
          mappingFailures:
            degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.mappingFailuresPerSecond,
          lastError: degradedHealth.grpc?.feeds["orders"]?.materialized["orders"]?.lastError,
        }).toStrictEqual({
          runtimeStatus: "degraded",
          feedStatus: "degraded",
          mappingFailures: 1,
          lastError:
            "gRPC feed orders failed: gRPC feed orders references unknown topic orders: orders",
        });
        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("records materialized gRPC publish failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedViewServer(
        longRunningGrpcStream([grpcOrderValue("order-1", 10)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const publishFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "publish unavailable",
        topic: "orders",
      };
      const failingRuntimeClient: ViewServerRuntimeCoreInternalClient<GrpcTopics> = {
        ...runtimeCore.internalClient,
        publishManyDecodedRows: () => Effect.fail(publishFailure),
      };
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcOptions.sourceConfig,
        failingRuntimeClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.reconnects).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed orders failed: gRPC feed publish failed for orders: publish unavailable",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );
});
