import { describe, expect, it } from "@effect/vitest";
import { type ViewServerRuntimeError } from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Queue, Schedule, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";

import { grpcOrderValue } from "../test-harness/grpc-config";
import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
  leasedGrpcViewServer,
  leasedOrdersQuery,
  longRunningGrpcStream,
  makeLeasedGrpcHealth,
  waitForLeasedGrpcSnapshotRows,
} from "../test-harness/grpc-leased";

import type { GrpcOrderValueMessage } from "../test-harness/grpc-config";

describe("gRPC lease manager lifecycle", () => {
  it.live(
    "keeps the first engine terminal and releases a stalled leased gRPC consumer automatically",
    () =>
      Effect.gen(function* () {
        let releaseCount = 0;
        const upstreamRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
        const upstreamFinalized = yield* Deferred.make<void>();
        const feed = grpcLeasedViewServer({
          streamForRegion: () =>
            Stream.fromQueue(upstreamRows).pipe(
              Stream.ensuring(Deferred.succeed(upstreamFinalized, undefined)),
            ),
          release: Effect.sync(() => {
            releaseCount += 1;
          }),
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {
          subscriptionQueueCapacity: 1,
        });
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
        const events = yield* Queue.unbounded<unknown>();
        const releaseConsumer = yield* Deferred.make<void>();
        let eventCount = 0;
        const eventsFiber = yield* subscription.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              yield* Queue.offer(events, event);
              eventCount += 1;
              if (eventCount === 1) {
                yield* Deferred.await(releaseConsumer);
              }
            }),
          ),
          Effect.forkChild,
        );
        const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        yield* Queue.offer(upstreamRows, grpcOrderValue("queued-order", 10));
        yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
        const upstreamFinalizedBeforeEngineTerminal = yield* Deferred.isDone(upstreamFinalized);
        yield* Queue.offer(upstreamRows, grpcOrderValue("backpressured-order", 20));
        const backpressuredHealth = yield* runtimeCore.client.health().pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) => currentHealth.engine.topics.orders.backpressureEvents === 1,
          }),
          Effect.timeout("1 second"),
        );
        yield* Deferred.await(upstreamFinalized).pipe(Effect.timeout("1 second"));
        const upstreamStillAccepted = yield* Queue.offer(
          upstreamRows,
          grpcOrderValue("still-open-upstream", 20),
        );
        const cleanedHealth = yield* Effect.gen(function* () {
          return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
        }).pipe(
          Effect.repeat({
            schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
            until: (currentHealth) =>
              currentHealth.engine.topics.orders.activeSubscriptions === 0 &&
              currentHealth.engine.topics.orders.rowCount === 0 &&
              Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
          }),
          Effect.timeout("1 second"),
        );

        yield* Deferred.succeed(releaseConsumer, undefined);
        const terminal = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
        yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

        expect([snapshot, terminal]).toStrictEqual([
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
            status: "closed",
            code: "BackpressureExceeded",
            message: "Subscription closed because its event queue exceeded capacity.",
          },
        ]);
        expect({
          releaseCount,
          upstreamFinalizedBeforeEngineTerminal,
          upstreamStillAccepted,
          backpressureEvents: backpressuredHealth.engine.topics.orders.backpressureEvents,
          activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
          retainedRows: cleanedHealth.engine.topics.orders.rowCount,
          leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
        }).toStrictEqual({
          releaseCount: 1,
          upstreamFinalizedBeforeEngineTerminal: false,
          upstreamStillAccepted: true,
          backpressureEvents: 1,
          activeSubscriptions: 0,
          retainedRows: 0,
          leasedFeedKeys: [],
        });
        yield* subscription.close();
        yield* manager.close;
        expect(releaseCount).toBe(1);
        yield* runtimeCore.close;
      }),
  );

  it.live("isolates an engine-terminal subscriber from its shared leased gRPC peer", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const upstreamRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const upstreamFinalized = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromQueue(upstreamRows).pipe(
            Stream.ensuring(Deferred.succeed(upstreamFinalized, undefined)),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {
        subscriptionQueueCapacity: 1,
      });
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

      const stalled = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const peer = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const stalledEvents = yield* Queue.unbounded<unknown>();
      const peerEvents = yield* Queue.unbounded<unknown>();
      const releaseStalledConsumer = yield* Deferred.make<void>();
      let stalledEventCount = 0;
      const stalledFiber = yield* stalled.events.pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            yield* Queue.offer(stalledEvents, event);
            stalledEventCount += 1;
            if (stalledEventCount === 1) {
              yield* Deferred.await(releaseStalledConsumer);
            }
          }),
        ),
        Effect.forkChild,
      );
      const peerFiber = yield* peer.events.pipe(
        Stream.runForEach((event) => Queue.offer(peerEvents, event)),
        Effect.forkChild,
      );
      const stalledSnapshot = yield* Queue.take(stalledEvents).pipe(Effect.timeout("1 second"));
      const peerSnapshot = yield* Queue.take(peerEvents).pipe(Effect.timeout("1 second"));

      yield* Queue.offer(upstreamRows, grpcOrderValue("shared-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      const firstPeerDelta = yield* Queue.take(peerEvents).pipe(Effect.timeout("1 second"));
      yield* Queue.offer(upstreamRows, grpcOrderValue("shared-2", 20));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 2);
      const secondPeerDelta = yield* Queue.take(peerEvents).pipe(Effect.timeout("1 second"));
      const isolatedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.activeSubscriptions === 1 &&
            currentHealth.engine.topics.orders.backpressureEvents === 1 &&
            currentHealth.grpc?.feeds.orders?.leased["orders/orders/leased/region=%22usa%22"]
              ?.subscriberCount === 1,
        }),
        Effect.timeout("1 second"),
      );
      const upstreamFinalizedWithPeerActive = yield* Deferred.isDone(upstreamFinalized);

      yield* Deferred.succeed(releaseStalledConsumer, undefined);
      const stalledTerminal = yield* Queue.take(stalledEvents).pipe(Effect.timeout("1 second"));
      yield* Fiber.join(stalledFiber).pipe(Effect.timeout("1 second"));
      yield* Queue.offer(upstreamRows, grpcOrderValue("shared-3", 30));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 3);
      const thirdPeerDelta = yield* Queue.take(peerEvents).pipe(Effect.timeout("1 second"));

      expect([stalledSnapshot, stalledTerminal]).toStrictEqual([
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
          status: "closed",
          code: "BackpressureExceeded",
          message: "Subscription closed because its event queue exceeded capacity.",
        },
      ]);
      expect({
        peerSnapshot,
        peerEventTypes: [firstPeerDelta, secondPeerDelta, thirdPeerDelta].map(
          (event) => Object(event)["type"],
        ),
        activeSubscriptions: isolatedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: isolatedHealth.engine.topics.orders.rowCount,
        subscriberCount:
          isolatedHealth.grpc?.feeds.orders?.leased["orders/orders/leased/region=%22usa%22"]
            ?.subscriberCount,
        releaseCount,
        upstreamFinalizedWithPeerActive,
      }).toStrictEqual({
        peerSnapshot: {
          type: "snapshot",
          topic: "orders",
          queryId: "query-1",
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        },
        peerEventTypes: ["delta", "delta", "delta"],
        activeSubscriptions: 1,
        retainedRows: 2,
        subscriberCount: 1,
        releaseCount: 0,
        upstreamFinalizedWithPeerActive: false,
      });

      yield* peer.close();
      yield* Fiber.join(peerFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(upstreamFinalized).pipe(Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 3_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.activeSubscriptions === 0 &&
            currentHealth.engine.topics.orders.rowCount === 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* stalled.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases an unconsumed leased subscription after upstream failure", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const upstreamReleased = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(Effect.andThen(Deferred.succeed(upstreamReleased, undefined)), Effect.asVoid),
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
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(upstreamReleased).pipe(Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.activeSubscriptions === 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([
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
          message: "gRPC leased upstream failed.",
        },
      ]);
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes a raw subscription that registers after leased upstream failure", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      let rawCloseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const subscribeStarted = yield* Deferred.make<void>();
      const releaseSubscribe = yield* Deferred.make<void>();
      const rawClosed = yield* Deferred.make<void>();
      const degradeReached = yield* Deferred.make<void>();
      const allowDegrade = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        clientDegraded: (clientName: string, message: string) =>
          baseHealth
            .clientDegraded(clientName, message)
            .pipe(
              Effect.andThen(Deferred.succeed(degradeReached, undefined)),
              Effect.andThen(Deferred.await(allowDegrade)),
            ),
      };
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(subscribeStarted, undefined);
            yield* Deferred.await(releaseSubscribe);
            yield* observer.onQueryRegistered("delayed-query");
            return {
              events: Stream.fromEffect(Deferred.await(rawClosed)).pipe(Stream.drain),
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(Effect.andThen(Deferred.succeed(rawClosed, undefined)), Effect.asVoid),
            };
          }),
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

      const subscriptionFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(subscribeStarted).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(degradeReached).pipe(Effect.timeout("1 second"));
      const rejectedLateSubscriberFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(allowDegrade, undefined);
      const rejectedLateSubscriber = yield* Fiber.join(rejectedLateSubscriberFiber).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.succeed(releaseSubscribe, undefined);
      const subscription = yield* Fiber.join(subscriptionFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(rawClosed).pipe(Effect.timeout("1 second"));
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );

      expect(rejectedLateSubscriber).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message:
          "gRPC leased upstream is not accepting new subscribers after completion or failure.",
      });
      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "delayed-query",
          status: "error",
          code: "RuntimeUnavailable",
          message: "gRPC leased upstream failed.",
        },
      ]);
      expect({
        releaseCount,
        rawCloseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        rawCloseCount: 1,
        activeSubscriptions: 0,
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes a raw subscription that registers after the lease manager closes", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      let rawCloseCount = 0;
      const subscribeStarted = yield* Deferred.make<void>();
      const releaseSubscribe = yield* Deferred.make<void>();
      const rawClosed = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(subscribeStarted, undefined);
            yield* Deferred.await(releaseSubscribe);
            yield* observer.onQueryRegistered("manager-close-query");
            return {
              events: Stream.fromEffect(Deferred.await(rawClosed)).pipe(Stream.drain),
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(Effect.andThen(Deferred.succeed(rawClosed, undefined)), Effect.asVoid),
            };
          }),
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

      const subscriptionFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(subscribeStarted).pipe(Effect.timeout("1 second"));
      yield* manager.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releaseSubscribe, undefined);
      const subscription = yield* Fiber.join(subscriptionFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(rawClosed).pipe(Effect.timeout("1 second"));
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(Array.from(events)).toStrictEqual([]);
      expect({
        releaseCount,
        rawCloseCount,
        activeSubscriptions: currentHealth.engine.topics.orders.activeSubscriptions,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        rawCloseCount: 1,
        activeSubscriptions: 0,
        leasedFeedKeys: [],
      });
      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.live("joins overlapping leased subscription close callers through full cleanup", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      const rawCloseStarted = yield* Deferred.make<void>();
      const allowRawClose = yield* Deferred.make<void>();
      const secondCloseReturned = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("overlapping-subscription-close").pipe(
            Effect.as({
              events: Stream.never,
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(
                  Effect.andThen(Deferred.succeed(rawCloseStarted, undefined)),
                  Effect.andThen(Deferred.await(allowRawClose)),
                ),
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

      const firstClose = yield* subscription
        .close()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(rawCloseStarted).pipe(Effect.timeout("1 second"));
      const secondClose = yield* subscription
        .close()
        .pipe(
          Effect.andThen(Deferred.succeed(secondCloseReturned, undefined)),
          Effect.forkChild({ startImmediately: true }),
        );
      yield* Effect.yieldNow;
      const secondReturnedBeforeCleanup = yield* Deferred.isDone(secondCloseReturned);
      yield* Deferred.succeed(allowRawClose, undefined);
      yield* Fiber.join(firstClose).pipe(Effect.timeout("1 second"));
      yield* Fiber.join(secondClose).pipe(Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );

      expect({
        secondReturnedBeforeCleanup,
        secondReturnedAfterCleanup: yield* Deferred.isDone(secondCloseReturned),
        rawCloseCount,
        releaseCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        secondReturnedBeforeCleanup: false,
        secondReturnedAfterCleanup: true,
        rawCloseCount: 1,
        releaseCount: 1,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "joins overlapping subscription and manager close callers and replays the raw-close defect",
    () =>
      Effect.gen(function* () {
        let rawCloseCount = 0;
        let releaseCount = 0;
        const rawCloseDefect = { _tag: "LeasedRawCloseDefect" } as const;
        const rawCloseStarted = yield* Deferred.make<void>();
        const allowRawClose = yield* Deferred.make<void>();
        const feed = grpcLeasedViewServer({
          streamForRegion: () => Stream.never,
          release: Effect.sync(() => {
            releaseCount += 1;
          }),
        });
        const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
        const health = makeLeasedGrpcHealth(grpcOptions);
        const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
          typeof leasedGrpcViewServer.topics
        > = {
          ...runtimeCore.internalLiveClient,
          subscribeObservedInternal: (_topic, _query, observer) =>
            observer.onQueryRegistered("subscription-manager-overlap").pipe(
              Effect.as({
                events: Stream.never,
                close: () =>
                  Effect.sync(() => {
                    rawCloseCount += 1;
                  }).pipe(
                    Effect.andThen(Deferred.succeed(rawCloseStarted, undefined)),
                    Effect.andThen(Deferred.await(allowRawClose)),
                    Effect.andThen(Effect.die(rawCloseDefect)),
                  ),
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
        const subscription = yield* manager.liveClient.subscribe(
          "orders",
          leasedOrdersQuery("usa"),
        );

        const subscriptionClose = yield* subscription
          .close()
          .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(rawCloseStarted).pipe(Effect.timeout("1 second"));
        const managerClose = yield* manager.close.pipe(
          Effect.exit,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        const managerDoneBeforeRawClose = managerClose.pollUnsafe() !== undefined;
        yield* Deferred.succeed(allowRawClose, undefined);
        const subscriptionExit = yield* Fiber.join(subscriptionClose).pipe(
          Effect.timeout("1 second"),
        );
        const managerExit = yield* Fiber.join(managerClose).pipe(Effect.timeout("1 second"));
        const lateExit = yield* subscription.close().pipe(Effect.exit, Effect.timeout("1 second"));
        const subscriptionDefect = Exit.isFailure(subscriptionExit)
          ? subscriptionExit.cause.reasons.find(Cause.isDieReason)?.defect
          : undefined;
        const managerDefect = Exit.isFailure(managerExit)
          ? managerExit.cause.reasons.find(Cause.isDieReason)?.defect
          : undefined;
        const lateDefect = Exit.isFailure(lateExit)
          ? lateExit.cause.reasons.find(Cause.isDieReason)?.defect
          : undefined;
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect({
          managerDoneBeforeRawClose,
          rawCloseCount,
          releaseCount,
          retainedRows: currentHealth.engine.topics.orders.rowCount,
          leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
        }).toStrictEqual({
          managerDoneBeforeRawClose: false,
          rawCloseCount: 1,
          releaseCount: 1,
          retainedRows: 0,
          leasedFeedKeys: [],
        });
        expect(subscriptionDefect).toBe(rawCloseDefect);
        expect(managerDefect).toBe(rawCloseDefect);
        expect(lateDefect).toBe(rawCloseDefect);
        yield* runtimeCore.close;
      }),
  );

  it.live("continues explicit leased cleanup after a feed-release defect", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseDefect = { _tag: "LeasedFeedReleaseDefect" } as const;
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          longRunningGrpcStream([grpcOrderValue("explicit-release-defect-row", 10)]),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(Effect.andThen(Effect.die(releaseDefect))),
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

      const firstExit = yield* subscription.close().pipe(Effect.exit, Effect.timeout("1 second"));
      const lateExit = yield* subscription.close().pipe(Effect.exit, Effect.timeout("1 second"));
      const firstDefect = Exit.isFailure(firstExit)
        ? firstExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const lateDefect = Exit.isFailure(lateExit)
        ? lateExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        releaseCount,
        retainedRows: currentHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      expect(firstDefect).toBe(releaseDefect);
      expect(lateDefect).toBe(releaseDefect);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );

  it.live("continues engine-terminal cleanup after a feed-release defect", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseDefect = { _tag: "EngineTerminalFeedReleaseDefect" } as const;
      const upstreamRows = yield* Queue.unbounded<GrpcOrderValueMessage>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.fromQueue(upstreamRows),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(Effect.andThen(Effect.die(releaseDefect))),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {
        subscriptionQueueCapacity: 1,
      });
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
      const emittedEvents = yield* Queue.unbounded<unknown>();
      const releaseConsumer = yield* Deferred.make<void>();
      let eventCount = 0;
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            yield* Queue.offer(emittedEvents, event);
            eventCount += 1;
            if (eventCount === 1) {
              yield* Deferred.await(releaseConsumer);
            }
          }),
        ),
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      const snapshot = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));

      yield* Queue.offer(upstreamRows, grpcOrderValue("engine-defect-1", 10));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);
      yield* Queue.offer(upstreamRows, grpcOrderValue("engine-defect-2", 20));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.activeSubscriptions === 0 &&
            currentHealth.engine.topics.orders.rowCount === 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      yield* Deferred.succeed(releaseConsumer, undefined);
      const terminal = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));
      const streamExit = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
      const lateExit = yield* subscription.close().pipe(Effect.exit, Effect.timeout("1 second"));
      const streamDefect = Exit.isFailure(streamExit)
        ? streamExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const lateDefect = Exit.isFailure(lateExit)
        ? lateExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;

      expect([snapshot, terminal]).toStrictEqual([
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
          status: "closed",
          code: "BackpressureExceeded",
          message: "Subscription closed because its event queue exceeded capacity.",
        },
      ]);
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      expect(streamDefect).toBe(releaseDefect);
      expect(lateDefect).toBe(releaseDefect);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );

  it.live("delivers upstream failure and cleanup when feed release defects", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const releaseDefect = { _tag: "UpstreamFeedReleaseDefect" } as const;
      const failUpstream = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(Effect.andThen(Effect.die(releaseDefect))),
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
      const emittedEvents = yield* Queue.unbounded<unknown>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(emittedEvents, event)),
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      const snapshot = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));

      yield* Deferred.succeed(failUpstream, undefined);
      const terminal = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));
      const streamExit = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
      const lateExit = yield* subscription.close().pipe(Effect.exit, Effect.timeout("1 second"));
      const streamDefect = Exit.isFailure(streamExit)
        ? streamExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const lateDefect = Exit.isFailure(lateExit)
        ? lateExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.activeSubscriptions === 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );

      expect([snapshot, terminal]).toStrictEqual([
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
          message: "gRPC leased upstream failed.",
        },
      ]);
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      expect(streamDefect).toBe(releaseDefect);
      expect(lateDefect).toBe(releaseDefect);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );

  it.live("cleans retained rows exactly once when leased upstream terminates", () =>
    Effect.gen(function* () {
      let deleteCount = 0;
      let releaseCount = 0;
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.make(grpcOrderValue("exactly-once-row", 10)),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const runtimeDelete = runtimeCore.internalClient.delete;
      Object.defineProperty(runtimeCore.internalClient, "delete", {
        value: (topic: "orders", key: string) =>
          Effect.sync(() => {
            deleteCount += 1;
          }).pipe(Effect.andThen(runtimeDelete(topic, key))),
      });
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
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.engine.topics.orders.rowCount === 0 &&
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      yield* subscription.close();
      yield* manager.close;

      expect(Array.from(events).map((event) => event.type)).toStrictEqual([
        "snapshot",
        "delta",
        "delta",
        "status",
      ]);
      expect({
        deleteCount,
        releaseCount,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        deleteCount: 1,
        releaseCount: 1,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("releases a leased subscription and shares raw-close defects across callers", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      const rawCloseDefect = { _tag: "OverlappingRawCloseDefect" } as const;
      const releaseStarted = yield* Deferred.make<void>();
      const allowRelease = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(
          Effect.andThen(Deferred.succeed(releaseStarted, undefined)),
          Effect.andThen(Deferred.await(allowRelease)),
        ),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("raw-close-defect").pipe(
            Effect.as({
              events: Stream.never,
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(Effect.andThen(Effect.die(rawCloseDefect))),
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

      const firstClose = yield* subscription
        .close()
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      const secondClose = yield* subscription
        .close()
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(releaseStarted).pipe(Effect.timeout("1 second"));
      const firstDoneBeforeRelease = firstClose.pollUnsafe() !== undefined;
      const secondDoneBeforeRelease = secondClose.pollUnsafe() !== undefined;
      yield* Deferred.succeed(allowRelease, undefined);
      const firstExit = yield* Fiber.join(firstClose).pipe(Effect.timeout("1 second"));
      const secondExit = yield* Fiber.join(secondClose).pipe(Effect.timeout("1 second"));
      const firstDefect = Exit.isFailure(firstExit)
        ? firstExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const secondDefect = Exit.isFailure(secondExit)
        ? secondExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        firstDoneBeforeRelease,
        secondDoneBeforeRelease,
        rawCloseCount,
        releaseCount,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        firstDoneBeforeRelease: false,
        secondDoneBeforeRelease: false,
        rawCloseCount: 1,
        releaseCount: 1,
        leasedFeedKeys: [],
      });
      expect(firstDefect).toBe(rawCloseDefect);
      expect(secondDefect).toBe(rawCloseDefect);
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("joins overlapping leased manager close callers through full cleanup", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      const releaseStarted = yield* Deferred.make<void>();
      const allowRelease = yield* Deferred.make<void>();
      const secondCloseReturned = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => longRunningGrpcStream([grpcOrderValue("manager-close-row", 10)]),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(
          Effect.andThen(Deferred.succeed(releaseStarted, undefined)),
          Effect.andThen(Deferred.await(allowRelease)),
        ),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("overlapping-manager-close").pipe(
            Effect.as({
              events: Stream.never,
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }),
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
      yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);

      const firstClose = yield* manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(releaseStarted).pipe(Effect.timeout("1 second"));
      const secondClose = yield* manager.close.pipe(
        Effect.andThen(Deferred.succeed(secondCloseReturned, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      const secondReturnedBeforeCleanup = yield* Deferred.isDone(secondCloseReturned);
      yield* Deferred.succeed(allowRelease, undefined);
      yield* Fiber.join(firstClose).pipe(Effect.timeout("1 second"));
      yield* Fiber.join(secondClose).pipe(Effect.timeout("1 second"));
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        secondReturnedBeforeCleanup,
        secondReturnedAfterCleanup: yield* Deferred.isDone(secondCloseReturned),
        rawCloseCount,
        releaseCount,
        retainedRows: currentHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        secondReturnedBeforeCleanup: false,
        secondReturnedAfterCleanup: true,
        rawCloseCount: 1,
        releaseCount: 1,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("continues manager cleanup and shares lease-release defects across callers", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      const releaseDefect = { _tag: "ManagerLeaseReleaseDefect" } as const;
      const rawCloseStarted = yield* Deferred.make<void>();
      const allowRawClose = yield* Deferred.make<void>();
      const rawEvents = yield* Queue.unbounded<never, Cause.Done>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => longRunningGrpcStream([grpcOrderValue("release-defect-row", 10)]),
        release: Effect.sync(() => {
          releaseCount += 1;
        }).pipe(Effect.andThen(Effect.die(releaseDefect))),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("lease-release-defect").pipe(
            Effect.as({
              events: Stream.fromQueue(rawEvents),
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(
                  Effect.andThen(Deferred.succeed(rawCloseStarted, undefined)),
                  Effect.andThen(Deferred.await(allowRawClose)),
                  Effect.andThen(Queue.end(rawEvents)),
                ),
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
      yield* waitForLeasedGrpcSnapshotRows(runtimeCore.internalClient, "usa", 1);

      const firstClose = yield* manager.close.pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(rawCloseStarted).pipe(Effect.timeout("1 second"));
      const secondClose = yield* manager.close.pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      const secondDoneBeforeCleanup = secondClose.pollUnsafe() !== undefined;
      yield* Deferred.succeed(allowRawClose, undefined);
      const firstExit = yield* Fiber.join(firstClose).pipe(Effect.timeout("1 second"));
      const secondExit = yield* Fiber.join(secondClose).pipe(Effect.timeout("1 second"));
      const lateExit = yield* manager.close.pipe(Effect.exit, Effect.timeout("1 second"));
      const firstDefect = Exit.isFailure(firstExit)
        ? firstExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const secondDefect = Exit.isFailure(secondExit)
        ? secondExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const lateDefect = Exit.isFailure(lateExit)
        ? lateExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        secondDoneBeforeCleanup,
        events: Array.from(events),
        rawCloseCount,
        releaseCount,
        retainedRows: currentHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        secondDoneBeforeCleanup: false,
        events: [],
        rawCloseCount: 1,
        releaseCount: 1,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      expect(firstDefect).toBe(releaseDefect);
      expect(secondDefect).toBe(releaseDefect);
      expect(lateDefect).toBe(releaseDefect);
      yield* runtimeCore.close;
    }),
  );

  it.live("closes an additional raw subscriber that registers after upstream failure", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      let subscribeCount = 0;
      let firstRawCloseCount = 0;
      let secondRawCloseCount = 0;
      let firstRawCloseStarted = false;
      let secondRawCloseStarted = false;
      const failUpstream = yield* Deferred.make<void>();
      const secondSubscribeStarted = yield* Deferred.make<void>();
      const releaseSecondSubscribe = yield* Deferred.make<void>();
      const firstRawClosed = yield* Deferred.make<void>();
      const secondRawClosed = yield* Deferred.make<void>();
      const degradeReached = yield* Deferred.make<void>();
      const allowDegrade = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        clientDegraded: (clientName: string, message: string) =>
          baseHealth
            .clientDegraded(clientName, message)
            .pipe(
              Effect.andThen(Deferred.succeed(degradeReached, undefined)),
              Effect.andThen(Deferred.await(allowDegrade)),
            ),
      };
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            const currentSubscribe = subscribeCount;
            subscribeCount += 1;
            if (currentSubscribe === 1) {
              yield* Deferred.succeed(secondSubscribeStarted, undefined);
              yield* Deferred.await(releaseSecondSubscribe);
            }
            const queryId = currentSubscribe === 0 ? "first-query" : "second-query";
            const rawClosed = currentSubscribe === 0 ? firstRawClosed : secondRawClosed;
            yield* observer.onQueryRegistered(queryId);
            return {
              events: Stream.fromEffect(Deferred.await(rawClosed)).pipe(Stream.drain),
              close: () =>
                Effect.sync(() => {
                  if (currentSubscribe === 0 && !firstRawCloseStarted) {
                    firstRawCloseStarted = true;
                    firstRawCloseCount += 1;
                  }
                  if (currentSubscribe === 1 && !secondRawCloseStarted) {
                    secondRawCloseStarted = true;
                    secondRawCloseCount += 1;
                  }
                }).pipe(Effect.andThen(Deferred.succeed(rawClosed, undefined)), Effect.asVoid),
            };
          }),
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

      const firstSubscription = yield* manager.liveClient.subscribe(
        "orders",
        leasedOrdersQuery("usa"),
      );
      const secondSubscriptionFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(secondSubscribeStarted).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(degradeReached).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(allowDegrade, undefined);
      yield* Deferred.await(firstRawClosed).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releaseSecondSubscribe, undefined);
      const secondSubscription = yield* Fiber.join(secondSubscriptionFiber).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(secondRawClosed).pipe(Effect.timeout("1 second"));
      const firstEvents = yield* firstSubscription.events.pipe(
        Stream.runCollect,
        Effect.timeout("1 second"),
      );
      const secondEvents = yield* secondSubscription.events.pipe(
        Stream.runCollect,
        Effect.timeout("1 second"),
      );
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );

      expect([Array.from(firstEvents), Array.from(secondEvents)]).toStrictEqual([
        [
          {
            type: "status",
            topic: "orders",
            queryId: "first-query",
            status: "error",
            code: "RuntimeUnavailable",
            message: "gRPC leased upstream failed.",
          },
        ],
        [
          {
            type: "status",
            topic: "orders",
            queryId: "second-query",
            status: "error",
            code: "RuntimeUnavailable",
            message: "gRPC leased upstream failed.",
          },
        ],
      ]);
      expect({
        releaseCount,
        firstRawCloseCount,
        secondRawCloseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        firstRawCloseCount: 1,
        secondRawCloseCount: 1,
        activeSubscriptions: 0,
        leasedFeedKeys: [],
      });
      yield* firstSubscription.close();
      yield* secondSubscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("delivers one upstream failure status to an already-running leased consumer", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const initialEventSeen = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
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
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(initialEventSeen, undefined)),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(initialEventSeen).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      yield* subscription.close();
      yield* manager.close;

      expect(Array.from(events)).toStrictEqual([
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
          message: "gRPC leased upstream failed.",
        },
      ]);
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("emits only the first leased terminal source when upstream fails before the engine", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const initialEventSeen = yield* Deferred.make<void>();
      const rawSubscriptionClosed = yield* Deferred.make<void>();
      const emitEngineTerminal = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const initialSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "engine-query",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      } as const;
      const engineTerminalStatus = {
        type: "status",
        topic: "orders",
        queryId: "engine-query",
        status: "closed",
        code: "BackpressureExceeded",
        message: "engine terminal arrived second",
      } as const;
      const controlledRuntimeEvents = Stream.make(initialSnapshot).pipe(
        Stream.concat(
          Stream.fromEffect(
            Deferred.await(emitEngineTerminal).pipe(Effect.as(engineTerminalStatus)),
          ),
        ),
      );
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () =>
          Effect.succeed({
            events: controlledRuntimeEvents,
            close: () => Deferred.succeed(rawSubscriptionClosed, undefined).pipe(Effect.asVoid),
          }),
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(initialSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(initialSnapshot).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.await(emitEngineTerminal).pipe(
                      Effect.tap(() => observer.onTerminalOccurrence(engineTerminalStatus)),
                      Effect.as(engineTerminalStatus),
                      Effect.tap(() => observer.onTerminalReady(engineTerminalStatus)),
                    ),
                  ),
                ),
              ),
              close: () => Deferred.succeed(rawSubscriptionClosed, undefined).pipe(Effect.asVoid),
            }),
          ),
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: controlledRuntimeEvents,
            close: () => Deferred.succeed(rawSubscriptionClosed, undefined).pipe(Effect.asVoid),
          }),
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(initialSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(initialSnapshot).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.await(emitEngineTerminal).pipe(
                      Effect.tap(() => observer.onTerminalOccurrence(engineTerminalStatus)),
                      Effect.as(engineTerminalStatus),
                      Effect.tap(() => observer.onTerminalReady(engineTerminalStatus)),
                    ),
                  ),
                ),
              ),
              close: () => Deferred.succeed(rawSubscriptionClosed, undefined).pipe(Effect.asVoid),
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
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(initialEventSeen, undefined)),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(initialEventSeen).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(rawSubscriptionClosed).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(emitEngineTerminal, undefined);
      const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      yield* subscription.close();
      yield* manager.close;

      expect(Array.from(events)).toStrictEqual([
        initialSnapshot,
        {
          type: "status",
          topic: "orders",
          queryId: "engine-query",
          status: "error",
          code: "RuntimeUnavailable",
          message: "gRPC leased upstream failed.",
        },
      ]);
      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps an engine terminal that occurs before a later upstream failure", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const engineOccurred = yield* Deferred.make<void>();
      const allowEngineReady = yield* Deferred.make<void>();
      const rawSubscriptionClosed = yield* Deferred.make<void>();
      const degradeReached = yield* Deferred.make<void>();
      const allowDegrade = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        clientDegraded: (clientName: string, message: string) =>
          baseHealth
            .clientDegraded(clientName, message)
            .pipe(
              Effect.andThen(Deferred.succeed(degradeReached, undefined)),
              Effect.andThen(Deferred.await(allowDegrade)),
            ),
      };
      const engineTerminalStatus = {
        type: "status",
        topic: "orders",
        queryId: "engine-first-query",
        status: "closed",
        code: "BackpressureExceeded",
        message: "engine terminal occurred first",
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            yield* observer.onQueryRegistered(engineTerminalStatus.queryId);
            yield* observer.onTerminalOccurrence(engineTerminalStatus);
            yield* Deferred.succeed(engineOccurred, undefined);
            yield* Deferred.await(allowEngineReady).pipe(
              Effect.andThen(observer.onTerminalReady(engineTerminalStatus)),
              Effect.forkChild({ startImmediately: true }),
            );
            return {
              events: Stream.fromEffect(Deferred.await(allowEngineReady)).pipe(
                Stream.map(() => engineTerminalStatus),
              ),
              close: () => Deferred.succeed(rawSubscriptionClosed, undefined).pipe(Effect.asVoid),
            };
          }),
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

      const subscriptionFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(engineOccurred).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(degradeReached).pipe(Effect.timeout("1 second"));
      const rejectedLateSubscriberFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(allowDegrade, undefined);
      const rejectedLateSubscriber = yield* Fiber.join(rejectedLateSubscriberFiber).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.succeed(allowEngineReady, undefined);
      const subscription = yield* Fiber.join(subscriptionFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(rawSubscriptionClosed).pipe(Effect.timeout("1 second"));
      const events = yield* subscription.events.pipe(Stream.runCollect, Effect.timeout("1 second"));

      expect(rejectedLateSubscriber).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message:
          "gRPC leased upstream is not accepting new subscribers after completion or failure.",
      });
      expect(Array.from(events)).toStrictEqual([engineTerminalStatus]);
      expect(releaseCount).toBe(1);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps a grouped runtime terminal that occurs before upstream failure", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const allowRawClose = yield* Deferred.make<void>();
      const allowConsumer = yield* Deferred.make<void>();
      const rawCloseCalls = yield* Queue.unbounded<void>();
      const emittedEvents = yield* Queue.unbounded<unknown>();
      const degradeReached = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        clientDegraded: (clientName: string, message: string) =>
          baseHealth
            .clientDegraded(clientName, message)
            .pipe(Effect.andThen(Deferred.succeed(degradeReached, undefined))),
      };
      const malformedGroupedSnapshot = {
        type: "delta",
        topic: "orders",
        queryId: "grouped-query",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "remove",
            key: "missing-internal-group",
          },
        ],
        totalRows: 0,
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(malformedGroupedSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(malformedGroupedSnapshot),
              close: () =>
                Queue.offer(rawCloseCalls, undefined).pipe(
                  Effect.andThen(Deferred.await(allowRawClose)),
                ),
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
        groupBy: ["customerId"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });

      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) =>
          Queue.offer(emittedEvents, event).pipe(Effect.andThen(Deferred.await(allowConsumer))),
        ),
        Effect.forkChild({ startImmediately: true }),
      );
      const groupedTerminal = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));
      yield* Queue.take(rawCloseCalls).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(failUpstream, undefined);
      yield* Deferred.await(degradeReached).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(allowRawClose, undefined);
      yield* Deferred.succeed(allowConsumer, undefined);
      yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

      expect(groupedTerminal).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "grouped-query",
        status: "error",
        code: "RuntimeUnavailable",
        message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
      });
      expect(releaseCount).toBe(1);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("suppresses a grouped runtime terminal after upstream failure wins", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const failUpstream = yield* Deferred.make<void>();
      const allowRawClose = yield* Deferred.make<void>();
      const rawCloseCalls = yield* Queue.unbounded<void>();
      const emittedEvents = yield* Queue.unbounded<unknown>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () =>
          Stream.fromEffect(
            Deferred.await(failUpstream).pipe(Effect.andThen(Effect.fail("upstream down"))),
          ),
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const malformedGroupedSnapshot = {
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-query",
        version: 1,
        keys: ["internal-group"],
        rows: [
          {
            customerId: new Uint8Array([1]),
            rowCount: 1n,
          },
        ],
        totalRows: 1,
      } as const;
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered(malformedGroupedSnapshot.queryId).pipe(
            Effect.as({
              events: Stream.make(malformedGroupedSnapshot),
              close: () =>
                Queue.offer(rawCloseCalls, undefined).pipe(
                  Effect.andThen(Deferred.await(allowRawClose)),
                ),
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
        groupBy: ["customerId"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });

      yield* Deferred.succeed(failUpstream, undefined);
      yield* Queue.take(rawCloseCalls).pipe(Effect.timeout("1 second"));
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runForEach((event) => Queue.offer(emittedEvents, event)),
        Effect.forkChild({ startImmediately: true }),
      );
      const upstreamTerminal = yield* Queue.take(emittedEvents).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(allowRawClose, undefined);
      yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

      expect(upstreamTerminal).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "grouped-query",
        status: "error",
        code: "RuntimeUnavailable",
        message: "gRPC leased upstream failed.",
      });
      expect(releaseCount).toBe(1);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps an engine terminal when grouped runtime termination occurs second", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      let runtimeEventRead = false;
      const emitEngineTerminal = yield* Deferred.make<void>();
      const engineOccurred = yield* Deferred.make<void>();
      const allowEngineReady = yield* Deferred.make<void>();
      const engineTerminalStatus = {
        type: "status",
        topic: "orders",
        queryId: "engine-runtime-order",
        status: "closed",
        code: "BackpressureExceeded",
        message: "engine terminal occurred first",
      } as const;
      const malformedGroupedDelta = {
        type: "delta",
        topic: "orders",
        queryId: "engine-runtime-order",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "remove",
            get key() {
              runtimeEventRead = true;
              return "missing-internal-group";
            },
          },
        ],
        totalRows: 0,
      } as const;
      const rawEvents = yield* Queue.unbounded<typeof malformedGroupedDelta, Cause.Done>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            yield* observer.onQueryRegistered(engineTerminalStatus.queryId);
            yield* Deferred.await(emitEngineTerminal).pipe(
              Effect.andThen(observer.onTerminalOccurrence(engineTerminalStatus)),
              Effect.andThen(Deferred.succeed(engineOccurred, undefined)),
              Effect.andThen(Deferred.await(allowEngineReady)),
              Effect.andThen(Queue.end(rawEvents)),
              Effect.andThen(observer.onTerminalReady(engineTerminalStatus)),
              Effect.forkChild({ startImmediately: true }),
            );
            return {
              events: Stream.fromQueue(rawEvents),
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(Effect.andThen(Queue.end(rawEvents)), Effect.asVoid),
            };
          }),
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
        groupBy: ["customerId"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Deferred.succeed(emitEngineTerminal, undefined);
      yield* Deferred.await(engineOccurred).pipe(Effect.timeout("1 second"));
      yield* Queue.offer(rawEvents, malformedGroupedDelta);
      yield* Effect.sync(() => runtimeEventRead).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("1 millis")),
          until: (wasRead) => wasRead,
        }),
        Effect.timeout("1 second"),
      );
      yield* Deferred.succeed(allowEngineReady, undefined);
      const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([engineTerminalStatus]);
      expect({ rawCloseCount, releaseCount }).toStrictEqual({
        rawCloseCount: 1,
        releaseCount: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps a grouped runtime terminal when engine termination occurs second", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      let runtimeEventRead = false;
      const emitEngineTerminal = yield* Deferred.make<void>();
      const engineOccurred = yield* Deferred.make<void>();
      const allowEngineReady = yield* Deferred.make<void>();
      const engineTerminalStatus = {
        type: "status",
        topic: "orders",
        queryId: "runtime-engine-order",
        status: "closed",
        code: "BackpressureExceeded",
        message: "engine terminal occurred second",
      } as const;
      const malformedGroupedDelta = {
        type: "delta",
        topic: "orders",
        queryId: "runtime-engine-order",
        fromVersion: 0,
        toVersion: 1,
        operations: [
          {
            type: "remove",
            get key() {
              runtimeEventRead = true;
              return "missing-internal-group";
            },
          },
        ],
        totalRows: 0,
      } as const;
      const rawEvents = yield* Queue.unbounded<typeof malformedGroupedDelta, Cause.Done>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          Effect.gen(function* () {
            yield* observer.onQueryRegistered(engineTerminalStatus.queryId);
            yield* Deferred.await(emitEngineTerminal).pipe(
              Effect.andThen(observer.onTerminalOccurrence(engineTerminalStatus)),
              Effect.andThen(Deferred.succeed(engineOccurred, undefined)),
              Effect.andThen(Deferred.await(allowEngineReady)),
              Effect.andThen(Queue.end(rawEvents)),
              Effect.andThen(observer.onTerminalReady(engineTerminalStatus)),
              Effect.forkChild({ startImmediately: true }),
            );
            return {
              events: Stream.fromQueue(rawEvents),
              close: () =>
                Effect.sync(() => {
                  rawCloseCount += 1;
                }).pipe(Effect.andThen(Queue.end(rawEvents)), Effect.asVoid),
            };
          }),
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
        groupBy: ["customerId"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        where: {
          region: { eq: "usa" },
        },
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Queue.offer(rawEvents, malformedGroupedDelta);
      yield* Effect.sync(() => runtimeEventRead).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("1 millis")),
          until: (wasRead) => wasRead,
        }),
        Effect.timeout("1 second"),
      );
      yield* Deferred.succeed(emitEngineTerminal, undefined);
      yield* Deferred.await(engineOccurred).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(allowEngineReady, undefined);
      const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "runtime-engine-order",
          status: "error",
          code: "RuntimeUnavailable",
          message: "Leased gRPC grouped key value cannot be encoded as a stable public key",
        },
      ]);
      expect({ rawCloseCount, releaseCount }).toStrictEqual({
        rawCloseCount: 1,
        releaseCount: 1,
      });
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases a leased gRPC subscription once when event consumption is interrupted", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      const initialEventSeen = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
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
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(initialEventSeen, undefined)),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* Deferred.await(initialEventSeen).pipe(Effect.timeout("1 second"));
      yield* Fiber.interrupt(eventsFiber);
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds.orders?.leased ?? {}).length === 0,
        }),
        Effect.timeout("1 second"),
      );
      yield* subscription.close();
      yield* manager.close;

      expect({
        releaseCount,
        activeSubscriptions: cleanedHealth.engine.topics.orders.activeSubscriptions,
        retainedRows: cleanedHealth.engine.topics.orders.rowCount,
        leasedFeedKeys: Object.keys(cleanedHealth.grpc?.feeds.orders?.leased ?? {}),
      }).toStrictEqual({
        releaseCount: 1,
        activeSubscriptions: 0,
        retainedRows: 0,
        leasedFeedKeys: [],
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("delivers leased terminal status when the runtime stream has no initial event", () =>
    Effect.gen(function* () {
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.empty,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () =>
          Effect.succeed({
            events: Stream.empty,
            close: () => Effect.void,
          }),
        subscribeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("empty-query").pipe(
            Effect.as({
              events: Stream.empty,
              close: () => Effect.void,
            }),
          ),
        subscribeRuntimeInternal: () =>
          Effect.succeed({
            events: Stream.empty,
            close: () => Effect.void,
          }),
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("empty-query").pipe(
            Effect.as({
              events: Stream.empty,
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
      const terminalStatus = yield* subscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.timeout("1 second"),
      );

      expect(Array.from(terminalStatus)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "empty-query",
          status: "error",
          code: "RuntimeUnavailable",
          message: "gRPC leased upstream completed unexpectedly.",
        },
      ]);
      yield* subscription.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases leased gRPC leases when internal subscription creation fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const subscriptionFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "internal subscription failed",
      };
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () => Effect.fail(subscriptionFailure),
        subscribeObservedInternal: () => Effect.fail(subscriptionFailure),
        subscribeRuntimeInternal: () => Effect.fail(subscriptionFailure),
        subscribeRuntimeObservedInternal: () => Effect.fail(subscriptionFailure),
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

      const subscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const subscribeRuntimeError = yield* manager.liveClient
        .subscribeRuntime("orders", leasedOrdersQuery("eu"))
        .pipe(Effect.flip);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        subscribeError,
        subscribeRuntimeError,
        released,
        leasedFeedKeys: Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        subscribeError: subscriptionFailure,
        subscribeRuntimeError: subscriptionFailure,
        released: 2,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps leased gRPC leases alive when an additional internal subscription fails", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const subscriptionFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "second internal subscription failed",
      };
      const internalSubscription = {
        events: Stream.never,
        close: () => Effect.void,
      };
      const subscribeResults =
        yield* Queue.unbounded<
          Effect.Effect<typeof internalSubscription, ViewServerRuntimeError>
        >();
      yield* Queue.offer(subscribeResults, Effect.succeed(internalSubscription));
      yield* Queue.offer(subscribeResults, Effect.fail(subscriptionFailure));
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<
        typeof leasedGrpcViewServer.topics
      > = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
        subscribeObservedInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
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

      const firstSubscription = yield* manager.liveClient.subscribe(
        "orders",
        leasedOrdersQuery("usa"),
      );
      const secondSubscribeError = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.flip);
      const activeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        secondSubscribeError,
        released,
        subscriberCount:
          activeHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
            ?.subscriberCount,
      }).toStrictEqual({
        secondSubscribeError: subscriptionFailure,
        released: 0,
        subscriberCount: 1,
      });
      yield* firstSubscription.close().pipe(Effect.timeout("1 second"));
      const closedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect({
        released,
        leasedFeedKeys: Object.keys(closedHealth.grpc?.feeds["orders"]?.leased ?? {}),
      }).toStrictEqual({
        released: 1,
        leasedFeedKeys: [],
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
  it.live("releases an interrupted same-route leased subscriber acquisition", () =>
    Effect.gen(function* () {
      let released = 0;
      const feed = grpcLeasedViewServer({
        release: Effect.sync(() => {
          released += 1;
        }),
        streamForRegion: () => Stream.never,
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(feed, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const secondSubscribeStarted = yield* Deferred.make<void>();
      const internalSubscription = {
        events: Stream.never,
        close: () => Effect.void,
      };
      const subscribeResults =
        yield* Queue.unbounded<
          Effect.Effect<typeof internalSubscription, ViewServerRuntimeError>
        >();
      yield* Queue.offer(subscribeResults, Effect.succeed(internalSubscription));
      yield* Queue.offer(
        subscribeResults,
        Effect.gen(function* () {
          yield* Deferred.succeed(secondSubscribeStarted, undefined);
          return yield* Effect.never;
        }),
      );
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<typeof feed.topics> = {
        ...runtimeCore.internalLiveClient,
        subscribeInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
        subscribeObservedInternal: () => Queue.take(subscribeResults).pipe(Effect.flatten),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        feed,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        fakeInternalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );
      const firstSubscription = yield* manager.liveClient.subscribe(
        "orders",
        leasedOrdersQuery("usa"),
      );
      const secondFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild);
      yield* Deferred.await(secondSubscribeStarted);
      yield* Fiber.interrupt(secondFiber);
      const activeHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        released,
        subscriberCount:
          activeHealth.grpc?.feeds["orders"]?.leased["orders/orders/leased/region=%22usa%22"]
            ?.subscriberCount,
      }).toStrictEqual({
        released: 0,
        subscriberCount: 1,
      });
      yield* firstSubscription.close();
      const cleanedHealth = yield* Effect.gen(function* () {
        return health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      }).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            Object.keys(currentHealth.grpc?.feeds["orders"]?.leased ?? {}).length === 0,
        }),
      );

      expect({
        released,
        leasedFeeds: cleanedHealth.grpc?.feeds["orders"]?.leased ?? {},
      }).toStrictEqual({
        released: 1,
        leasedFeeds: {},
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );
});
