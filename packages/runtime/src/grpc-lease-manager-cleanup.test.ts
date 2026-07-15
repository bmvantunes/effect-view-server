import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Queue, Schedule, Stream } from "effect";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { RuntimeTestFailure } from "../test-harness/runtime";

import { grpcOrderValue } from "../test-harness/grpc-config";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import {
  captureLeasedGrpcDegradation,
  grpcLeasedViewServer,
  grpcLeasedViewServerFromCallbacks,
  leasedOrdersQuery,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
  waitForLeasedGrpcSnapshotRows,
} from "../test-harness/grpc-leased";

import type { GrpcOrderValueMessage } from "../test-harness/grpc-config";

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

describe("gRPC lease manager cleanup", () => {
  it.live(
    "rolls back a failed leased acquire when release defects and allows a fresh acquire",
    () =>
      Effect.gen(function* () {
        let acquireCount = 0;
        let releaseCount = 0;
        const acquireFailure = new Error("leased acquire exploded before stream creation");
        const releaseDefect = { _tag: "AcquireRollbackReleaseDefect" } as const;
        const feed = grpcLeasedViewServer({
          streamForRegion: () => {
            acquireCount += 1;
            if (acquireCount === 1) {
              throw acquireFailure;
            }
            return Stream.never;
          },
          release: Effect.suspend(() => {
            releaseCount += 1;
            return releaseCount === 1 ? Effect.die(releaseDefect) : Effect.void;
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

        const failedAcquireExit = yield* manager.liveClient
          .subscribe("orders", leasedOrdersQuery("usa"))
          .pipe(Effect.exit);
        const failedAcquireTypedError = Exit.isFailure(failedAcquireExit)
          ? failedAcquireExit.cause.reasons.find(Cause.isFailReason)?.error
          : undefined;
        const failedAcquireDefect = Exit.isFailure(failedAcquireExit)
          ? failedAcquireExit.cause.reasons.find(Cause.isDieReason)?.defect
          : undefined;
        const afterFailedAcquire = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
        const releaseCountAfterFailedAcquire = releaseCount;

        const freshSubscription = yield* manager.liveClient.subscribe(
          "orders",
          leasedOrdersQuery("usa"),
        );
        const afterFreshAcquire = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        const releaseCountAfterFreshAcquire = releaseCount;
        const freshCloseExit = yield* freshSubscription.close().pipe(Effect.exit);
        const releaseCountAfterFreshClose = releaseCount;
        const managerCloseExit = yield* manager.close.pipe(Effect.exit);
        yield* runtimeCore.close;

        expect(failedAcquireTypedError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "gRPC leased feed acquire failed for orders",
        });
        expect(failedAcquireDefect).toBe(releaseDefect);
        expect({
          acquireCount,
          releaseCountAfterFailedAcquire,
          releaseCountAfterFreshAcquire,
          releaseCountAfterFreshClose,
          failedLeaseKeys: Object.keys(afterFailedAcquire.grpc?.feeds.orders?.leased ?? {}),
          freshSubscriberCount:
            afterFreshAcquire.grpc?.feeds.orders?.leased["orders/orders/leased/region=%22usa%22"]
              ?.subscriberCount,
          freshCloseSucceeded: Exit.isSuccess(freshCloseExit),
          managerCloseSucceeded: Exit.isSuccess(managerCloseExit),
        }).toStrictEqual({
          acquireCount: 2,
          releaseCountAfterFailedAcquire: 1,
          releaseCountAfterFreshAcquire: 1,
          releaseCountAfterFreshClose: 2,
          failedLeaseKeys: [],
          freshSubscriberCount: 1,
          freshCloseSucceeded: true,
          managerCloseSucceeded: true,
        });
      }),
  );

  it.live("keeps leased gRPC key identity stable during subscription wrapping", () =>
    Effect.gen(function* () {
      let released = 0;
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
          release: Effect.sync(() => {
            released += 1;
          }),
        }),
      );
      Object.defineProperty(localViewServer.topics.orders.grpcSource, "request", {
        value: ({ region }: { readonly region: string }) => {
          Object.defineProperty(localViewServer.topics.orders, "key", {
            value: "price",
          });
          return { orderId: region };
        },
      });
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* subscription.close();
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        released: 1,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC key identity stable during runtime subscription wrapping", () =>
    Effect.gen(function* () {
      let released = 0;
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
          release: Effect.sync(() => {
            released += 1;
          }),
        }),
      );
      Object.defineProperty(localViewServer.topics.orders.grpcSource, "request", {
        value: ({ region }: { readonly region: string }) => {
          Object.defineProperty(localViewServer.topics.orders, "key", {
            value: "price",
          });
          return { orderId: region };
        },
      });
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

      const subscription = yield* manager.liveClient.subscribeRuntime(
        "orders",
        leasedOrdersQuery("usa"),
      );
      yield* subscription.close();
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        released: 1,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks leased gRPC cleanup degraded when runtime topic disappears before close", () =>
    Effect.gen(function* () {
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: (region) =>
            longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
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
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      Reflect.deleteProperty(localViewServer.topics, "orders");

      yield* subscription.close();
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(
        currentHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
          ?.lastError,
      ).toBe("gRPC leased feed row cleanup failed for orders");
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when runtime publishMany violates the client contract", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      Object.defineProperty(runtimeCore.internalClient, "publishManyDecodedRowsWithStorageKeys", {
        value: () => "not-an-effect",
      });
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: Runtime publishManyDecodedRowsWithStorageKeys did not return an Effect for leased gRPC feed orders",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when runtime publishMany fails", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      Object.defineProperty(runtimeCore.internalClient, "publishManyDecodedRowsWithStorageKeys", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime publishManyDecodedRowsWithStorageKeys failed",
            }),
          ),
      });
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: gRPC leased feed publish failed for orders",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC close total when runtime delete violates the client contract", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
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
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () => "not-an-effect",
      });

      yield* subscription.close();
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeed:
          idleHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"],
      }).toStrictEqual({
        released: 1,
        leasedFeed: {
          status: "degraded",
          lifecycle: "leased",
          feedName: "orders",
          feedKey: "orders/orders/leased/region=%22usa%22",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 1,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt:
            idleHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
              ?.lastMessageAt,
          lastError: "gRPC leased feed row cleanup failed for orders",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC close total when runtime delete fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
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
      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime delete failed",
            }),
          ),
      });

      yield* subscription.close();
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        leasedFeed:
          idleHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"],
      }).toStrictEqual({
        released: 1,
        leasedFeed: {
          status: "degraded",
          lifecycle: "leased",
          feedName: "orders",
          feedKey: "orders/orders/leased/region=%22usa%22",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 1,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt:
            idleHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
              ?.lastMessageAt,
          lastError: "gRPC leased feed row cleanup failed for orders",
        },
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed with a configured non-string Row Key", () =>
    Effect.gen(function* () {
      const firstValue = yield* Deferred.make<GrpcOrderValueMessage>();
      const localViewServer = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: () =>
            Stream.fromEffect(Deferred.await(firstValue)).pipe(Stream.concat(Stream.never)),
        }),
      );
      Object.defineProperty(localViewServer.topics.orders, "key", {
        value: "price",
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(localViewServer);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(localViewServer, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
        select: ["id"],
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });
      yield* Deferred.succeed(firstValue, grpcOrderValue("numeric-key", 10));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: gRPC leased feed row key price for orders is not a string",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes active leased gRPC feeds when the manager closes", () =>
    Effect.gen(function* () {
      let released = 0;
      const streamInterrupted = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: (region) =>
          longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]).pipe(
            Stream.ensuring(Deferred.succeed(streamInterrupted, undefined)),
          ),
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshotEvent = yield* Queue.take(eventQueue);
      const insertEvent = yield* Queue.take(eventQueue);
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* manager.close;
      const shutdownStatusEvent = yield* Queue.poll(eventQueue);
      yield* Deferred.await(streamInterrupted);
      const closedSubscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        closedSubscribeError,
        snapshotEvent,
        insertEvent,
        shutdownStatusEvent,
        released,
        rows: emptySnapshot.rows,
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(stoppedHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        closedSubscribeError: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "orders",
          message: "gRPC leased feed manager is closed.",
        },
        snapshotEvent: {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        insertEvent: {
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
        },
        shutdownStatusEvent: Option.none(),
        released: 1,
        rows: [],
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* Fiber.interrupt(eventsFiber);
      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.live("closes leased gRPC feeds when release callback throws", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServerFromCallbacks({
        request: ({ region }) => ({ orderId: region }),
        acquire: ({ route }) =>
          longRunningGrpcStream([grpcOrderValue(`${route.region}-order-1`, 10)]),
        release: () => {
          throw new Error("release exploded");
        },
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* subscription.close();
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes leased gRPC feeds when release callback returns a non-Effect", () =>
    Effect.gen(function* () {
      const feed = cloneWithMutableOrdersGrpcSource(
        grpcLeasedViewServer({
          streamForRegion: (region) =>
            longRunningGrpcStream([grpcOrderValue(`${region}-order-1`, 10)]),
        }),
      );
      Object.defineProperty(feed.topics.orders.grpcSource, "release", {
        value: () => "not-an-effect",
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* subscription.close();
      const emptySnapshot = yield* waitForLeasedGrpcSnapshotRows(
        runtimeCore.internalClient,
        "usa",
        0,
      );
      const idleHealth = health.healthOverlay(yield* runtimeCore.client.health(), 3_000);

      expect({
        totalRows: emptySnapshot.totalRows,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        totalRows: 0,
        leasedFeeds: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when mapping throws", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServerFromCallbacks({
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.make(grpcOrderValue("bad-map", 10)).pipe(Stream.concat(Stream.never)),
        map: () => {
          throw new Error("mapping exploded");
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: gRPC leased feed mapping failed for orders",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when mapping returns an invalid row", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServerFromCallbacks({
        request: ({ region }) => ({ orderId: region }),
        acquire: () =>
          Stream.make(grpcOrderValue("invalid-row", 10)).pipe(Stream.concat(Stream.never)),
        map: ({ value, route }) => {
          const row = {
            id: `${route.region}:${value.customerId}`,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region: route.region,
            updatedAt: value.updatedAt,
          };
          Object.defineProperty(row, "status", { value: "not-a-status" });
          return row;
        },
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: gRPC leased feed mapping produced an invalid row for orders",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when its upstream stream fails", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fail("upstream down"),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders failed: ViewServerGrpcIngressError: gRPC leased feed stream failed for orders",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        clientStatus: cleanedHealth.grpc?.clients["orders"]?.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        clientStatus: "connected",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("externalizes leased gRPC cleanup remove deltas after upstream completion", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(eventQueue);
      const insertDelta = yield* Queue.take(eventQueue);
      const removeDelta = yield* Queue.take(eventQueue);
      const terminalStatus = yield* Queue.take(eventQueue);

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(insertDelta).toStrictEqual({
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
      expect(removeDelta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "remove",
            key: "usa:usa-order-1",
          },
        ],
        totalRows: 0,
      });
      expect(terminalStatus).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream completed unexpectedly.",
      });
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );
      expect(Object.keys(cleanedHealth.grpc?.feeds["orders"]?.leased ?? {})).toStrictEqual([]);

      yield* subscription.close();
      yield* Fiber.interrupt(eventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC subscribers informed when upstream cleanup delete fails", () =>
    Effect.gen(function* () {
      const completeUpstream = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) =>
          Stream.make(grpcOrderValue(`${region}-order-1`, 10)).pipe(
            Stream.concat(Stream.fromEffect(Deferred.await(completeUpstream)).pipe(Stream.drain)),
          ),
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

      const subscription = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const eventQueue = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(eventQueue, event)),
        Effect.forkChild,
      );
      const snapshot = yield* Queue.take(eventQueue);
      const insertDelta = yield* Queue.take(eventQueue);
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: () =>
          Effect.fail(
            new RuntimeTestFailure({
              message: "runtime delete failed during upstream cleanup",
            }),
          ),
      });
      yield* Deferred.succeed(completeUpstream, undefined);
      const terminalStatus = yield* Queue.take(eventQueue);

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(insertDelta).toStrictEqual({
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
      expect(terminalStatus).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream completed unexpectedly.",
      });
      const degradedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
              ?.rowCount === 1,
        }),
      );
      expect(
        degradedHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
          ?.lastError,
      ).toContain("gRPC leased feed row cleanup failed for orders");

      yield* subscription.close();
      yield* Fiber.interrupt(eventsFiber);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans a leased gRPC feed when upstream self-interrupts", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fromEffect(Effect.interrupt),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const degradationMessages: Array<string> = [];
      const health = captureLeasedGrpcDegradation(
        makeLeasedGrpcHealth(grpcOptions),
        degradationMessages,
      );
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
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            degradationMessages.length > 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );

      expect(degradationMessages).toStrictEqual([
        "gRPC leased feed orders interrupted unexpectedly.",
      ]);
      expect({
        runtimeStatus: cleanedHealth.status,
        clientStatus: cleanedHealth.grpc?.clients["orders"]?.status,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        runtimeStatus: "ready",
        clientStatus: "connected",
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans completed leased feeds and allows later subscribers to reacquire", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        streamForRegion: (region) => Stream.make(grpcOrderValue(`${region}-order-1`, 10)),
        release: Effect.sync(() => {
          released += 1;
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

      const first = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const idleHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
      );
      const firstEvents = yield* first.events.pipe(Stream.runCollect, Effect.timeout("1 second"));
      expect({
        released,
        runtimeStatus: idleHealth.status,
        clientStatus: idleHealth.grpc?.clients["orders"]?.status,
        leasedFeeds: Object.keys(idleHealth.grpc?.feeds.orders?.leased ?? {}),
        eventTypes: Array.from(firstEvents).map((event) => event.type),
      }).toStrictEqual({
        released: 1,
        runtimeStatus: "ready",
        clientStatus: "connected",
        leasedFeeds: [],
        eventTypes: ["snapshot", "delta", "delta", "status"],
      });
      yield* first.close();

      const second = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const secondEvents = yield* second.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect({
        released,
        eventTypes: Array.from(secondEvents).map((event) => event.type),
      }).toStrictEqual({
        released: 2,
        eventTypes: ["snapshot", "delta", "delta", "status"],
      });
      yield* second.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
