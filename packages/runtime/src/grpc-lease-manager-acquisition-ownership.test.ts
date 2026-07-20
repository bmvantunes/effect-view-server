import { describe, expect, it } from "@effect/vitest";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Semaphore, Stream } from "effect";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import { withLeaseAcquisitionPermit } from "./grpc-lease-manager-substrate";
import { makeDefaultGrpcClient } from "./grpc-source-lifecycle";

import { resolveLeasedGrpcRuntimeOptions } from "../test-harness/grpc-materialized";
import {
  grpcLeasedViewServer,
  leasedOrdersQuery,
  makeLeasedGrpcHealth,
} from "../test-harness/grpc-leased";

describe("gRPC lease manager acquisition ownership", () => {
  it.effect("holds the acquisition permit until provisional cleanup completes", () =>
    Effect.gen(function* () {
      const lock = yield* Semaphore.make(1);
      const cleanupStarted = yield* Deferred.make<void>();
      const allowCleanup = yield* Deferred.make<void>();
      const acquisitionFiber = yield* withLeaseAcquisitionPermit(lock, (handoff) =>
        handoff
          .own(
            Deferred.succeed(cleanupStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowCleanup)),
            ),
          )
          .pipe(Effect.andThen(Effect.fail("acquisition failed"))),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(cleanupStarted).pipe(Effect.timeout("1 second"));
      const permitProbe = yield* lock.withPermitsIfAvailable(1)(Effect.void);
      yield* Deferred.succeed(allowCleanup, undefined);
      const acquisitionExit = yield* Fiber.await(acquisitionFiber).pipe(Effect.timeout("1 second"));

      expect({
        acquisitionFailed: Exit.isFailure(acquisitionExit),
        permitAvailableDuringCleanup: Option.isSome(permitProbe),
      }).toStrictEqual({
        acquisitionFailed: true,
        permitAvailableDuringCleanup: false,
      });
    }),
  );

  it.live("rolls back a stored first-route lease when starting health defects", () =>
    Effect.gen(function* () {
      const healthDefect = { _tag: "LeasedFeedStartingHealthDefect" } as const;
      const feed = grpcLeasedViewServer({ streamForRegion: () => Stream.never });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        leasedFeedStarting: (input: Parameters<typeof baseHealth.leasedFeedStarting>[0]) =>
          baseHealth.leasedFeedStarting(input).pipe(Effect.andThen(Effect.die(healthDefect))),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscribeExit = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.exit, Effect.timeout("1 second"));
      const defect = Exit.isFailure(subscribeExit)
        ? subscribeExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const currentFeedKeys = Object.keys(
        baseHealth.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
          ?.leased ?? {},
      );

      expect(currentFeedKeys).toStrictEqual([]);
      expect(defect).toBe(healthDefect);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );

  it.live("deletes a closed lease when health retirement defects", () =>
    Effect.gen(function* () {
      let removalCount = 0;
      const retirementDefect = { _tag: "LeasedFeedRetirementDefect" } as const;
      const feed = grpcLeasedViewServer({ streamForRegion: () => Stream.never });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        leasedFeedRemoved: (feedKey: string) =>
          Effect.suspend(() => {
            removalCount += 1;
            const retirement = baseHealth.leasedFeedRemoved(feedKey);
            return removalCount === 1
              ? retirement.pipe(Effect.andThen(Effect.die(retirementDefect)))
              : retirement;
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const firstSubscription = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.timeout("1 second"));
      const firstCloseExit = yield* firstSubscription
        .close()
        .pipe(Effect.exit, Effect.timeout("1 second"));
      const firstCloseDefect = Exit.isFailure(firstCloseExit)
        ? firstCloseExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const replacement = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.timeout("1 second"));

      expect({ firstCloseDefect, removalCount }).toStrictEqual({
        firstCloseDefect: retirementDefect,
        removalCount: 1,
      });
      yield* replacement.close().pipe(Effect.timeout("1 second"));
      expect(removalCount).toBe(2);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );

  it.live("creates one mutable lease identity for active subscribers on one route", () =>
    Effect.gen(function* () {
      const partitionKeys: Array<string> = [];
      const feed = grpcLeasedViewServer({ streamForRegion: () => Stream.never });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<typeof feed.topics> = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (topic, query, observer, partition) =>
          Effect.sync(() => {
            partitionKeys.push(partition?.key ?? "missing-partition");
          }).pipe(
            Effect.andThen(
              runtimeCore.internalLiveClient.subscribeRuntimeObservedInternal(
                topic,
                query,
                observer,
                partition,
              ),
            ),
          ),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        internalLiveClient,
        Effect.void,
        grpcOptions,
        makeLeasedGrpcHealth(grpcOptions),
      );

      const first = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      const second = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      yield* first.close();
      yield* second.close();
      const replacement = yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));

      const feedKey = "orders/orders/leased/region=%5B%22string%22%2C%22usa%22%5D";
      expect(partitionKeys).toStrictEqual([
        `${feedKey}/lease:1`,
        `${feedKey}/lease:1`,
        `${feedKey}/lease:2`,
      ]);
      yield* replacement.close();
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("keeps failed first-route rollback inside the manager permit", () =>
    Effect.gen(function* () {
      let releaseCount = 0;
      let startingCount = 0;
      let removalCount = 0;
      const firstStarting = yield* Deferred.make<void>();
      const allowStartingFailure = yield* Deferred.make<void>();
      const firstRemoval = yield* Deferred.make<void>();
      const allowFirstRemoval = yield* Deferred.make<void>();
      const secondAttempting = yield* Deferred.make<void>();
      const healthDefect = { _tag: "FirstRouteStartingHealthDefect" } as const;
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const acquisitionLock = yield* Semaphore.make(1);
      const health = {
        ...baseHealth,
        leasedFeedStarting: (input: Parameters<typeof baseHealth.leasedFeedStarting>[0]) =>
          Effect.suspend(() => {
            startingCount += 1;
            return startingCount === 1
              ? baseHealth
                  .leasedFeedStarting(input)
                  .pipe(
                    Effect.andThen(Deferred.succeed(firstStarting, undefined)),
                    Effect.andThen(Deferred.await(allowStartingFailure)),
                    Effect.andThen(Effect.die(healthDefect)),
                  )
              : baseHealth.leasedFeedStarting(input);
          }),
        leasedFeedRemoved: (feedKey: string) =>
          Effect.suspend(() => {
            removalCount += 1;
            return removalCount === 1
              ? baseHealth
                  .leasedFeedRemoved(feedKey)
                  .pipe(
                    Effect.andThen(Deferred.succeed(firstRemoval, undefined)),
                    Effect.andThen(Deferred.await(allowFirstRemoval)),
                  )
              : baseHealth.leasedFeedRemoved(feedKey);
          }),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
        makeDefaultGrpcClient,
        undefined,
        acquisitionLock,
      );

      const firstFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstStarting).pipe(Effect.timeout("1 second"));
      const secondFiber = yield* Effect.gen(function* () {
        yield* Deferred.succeed(secondAttempting, undefined);
        return yield* manager.liveClient.subscribe("orders", leasedOrdersQuery("usa"));
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(secondAttempting).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(allowStartingFailure, undefined);
      yield* Deferred.await(firstRemoval).pipe(Effect.timeout("1 second"));
      const acquisitionPermitProbe = yield* acquisitionLock.withPermitsIfAvailable(1)(Effect.void);
      yield* Deferred.succeed(allowFirstRemoval, undefined);
      const firstExit = yield* Fiber.await(firstFiber).pipe(Effect.timeout("1 second"));
      const secondSubscription = yield* Fiber.join(secondFiber).pipe(Effect.timeout("1 second"));
      const firstDefect = Exit.isFailure(firstExit)
        ? firstExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;

      expect({
        firstDefect,
        permitAvailableDuringRollback: Option.isSome(acquisitionPermitProbe),
        releaseCountBeforeSecondClose: releaseCount,
        startingCount,
      }).toStrictEqual({
        firstDefect: healthDefect,
        permitAvailableDuringRollback: false,
        releaseCountBeforeSecondClose: 0,
        startingCount: 2,
      });
      yield* secondSubscription.close().pipe(Effect.timeout("1 second"));
      const currentFeedKeys = Object.keys(
        baseHealth.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
          ?.leased ?? {},
      );
      expect({ currentFeedKeys, releaseCount }).toStrictEqual({
        currentFeedKeys: [],
        releaseCount: 1,
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases an interrupted first-route lease acquisition", () =>
    Effect.gen(function* () {
      const startingReached = yield* Deferred.make<void>();
      const allowStarting = yield* Deferred.make<void>();
      const feed = grpcLeasedViewServer({ streamForRegion: () => Stream.never });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const baseHealth = makeLeasedGrpcHealth(grpcOptions);
      const health = {
        ...baseHealth,
        leasedFeedStarting: (input: Parameters<typeof baseHealth.leasedFeedStarting>[0]) =>
          baseHealth
            .leasedFeedStarting(input)
            .pipe(
              Effect.andThen(Deferred.succeed(startingReached, undefined)),
              Effect.andThen(Deferred.await(allowStarting)),
            ),
      };
      const manager = yield* makeViewServerGrpcLeaseManager(
        grpcOptions.sourceConfig,
        runtimeCore.internalClient,
        runtimeCore.liveClient,
        runtimeCore.internalLiveClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const subscribeFiber = yield* manager.liveClient
        .subscribe("orders", leasedOrdersQuery("usa"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(startingReached).pipe(Effect.timeout("1 second"));
      const interruptFiber = yield* Fiber.interrupt(subscribeFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(allowStarting, undefined);
      yield* Fiber.join(interruptFiber).pipe(Effect.timeout("1 second"));
      const subscribeExit = yield* Fiber.await(subscribeFiber);
      const currentFeedKeys = Object.keys(
        baseHealth.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
          ?.leased ?? {},
      );

      expect({ currentFeedKeys, interrupted: Exit.hasInterrupts(subscribeExit) }).toStrictEqual({
        currentFeedKeys: [],
        interrupted: true,
      });
      yield* manager.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("closes the raw grouped subscription when retention observation defects", () =>
    Effect.gen(function* () {
      let rawCloseCount = 0;
      let releaseCount = 0;
      const observerDefect = { _tag: "GroupedRetentionObserverDefect" } as const;
      const feed = grpcLeasedViewServer({
        streamForRegion: () => Stream.never,
        release: Effect.sync(() => {
          releaseCount += 1;
        }),
      });
      const grpcOptions = yield* resolveLeasedGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(grpcOptions.sourceConfig, {});
      const health = makeLeasedGrpcHealth(grpcOptions);
      const fakeInternalLiveClient: ViewServerRuntimeCoreInternalLiveClient<typeof feed.topics> = {
        ...runtimeCore.internalLiveClient,
        subscribeRuntimeObservedInternal: (_topic, _query, observer) =>
          observer.onQueryRegistered("grouped-observer-defect").pipe(
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
        makeDefaultGrpcClient,
        () => {
          throw observerDefect;
        },
      );

      const subscribeExit = yield* manager.liveClient
        .subscribeRuntime("orders", {
          routeBy: { region: "usa" },
          groupBy: ["customerId"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [{ field: "region", type: "equals", filter: "usa" }],
          limit: 10,
        })
        .pipe(Effect.exit, Effect.timeout("1 second"));
      const defect = Exit.isFailure(subscribeExit)
        ? subscribeExit.cause.reasons.find(Cause.isDieReason)?.defect
        : undefined;
      const currentFeedKeys = Object.keys(
        health.healthOverlay(yield* runtimeCore.client.health(), 1_000).grpc?.feeds["orders"]
          ?.leased ?? {},
      );

      expect({ currentFeedKeys, rawCloseCount, releaseCount }).toStrictEqual({
        currentFeedKeys: [],
        rawCloseCount: 1,
        releaseCount: 1,
      });
      expect(defect).toBe(observerDefect);
      yield* manager.close.pipe(Effect.exit);
      yield* runtimeCore.close;
    }),
  );
});
