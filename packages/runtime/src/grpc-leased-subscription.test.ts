import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Option, Result, Schema, Scope, Stream } from "effect";
import { makeGrpcLeasedIdentityContract } from "./grpc-leased-identity";
import { makeGrpcLeasedSubscription } from "./grpc-leased-subscription";

const SubscriptionRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
});

describe("leased gRPC Subscription", () => {
  it.live("owns upstream, client, retained-row, and health cleanup exactly once", () =>
    Effect.gen(function* () {
      const identityContract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: SubscriptionRow,
          keyField: "id",
        }),
      );
      const identity = yield* Effect.fromResult(
        identityContract.leaseFromQuery({ where: { region: { eq: "usa" } } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const counters = {
        cleanup: 0,
        closed: 0,
        rawClose: 0,
        release: 0,
        rowsCleared: 0,
        subscriberAdded: 0,
        subscriberRemoved: 0,
        stopping: 0,
      };
      const subscription = yield* makeGrpcLeasedSubscription({
        parentScope,
        topic: "orders",
        identity,
        cleanupRows: (keys) =>
          Effect.sync(() => {
            counters.cleanup += 1;
            expect(Array.from(keys)).toStrictEqual([
              '["leased-row","orders/orders/leased/region=%22usa%22","order-1"]',
            ]);
          }),
        onCleanupFailure: () => Effect.die("cleanup must succeed"),
        onClosed: Effect.sync(() => {
          counters.closed += 1;
        }),
        onRowsCleared: Effect.sync(() => {
          counters.rowsCleared += 1;
        }),
        onStopping: Effect.sync(() => {
          counters.stopping += 1;
        }),
        onSubscriberAdded: Effect.sync(() => {
          counters.subscriberAdded += 1;
        }),
        onSubscriberRemoved: Effect.sync(() => {
          counters.subscriberRemoved += 1;
        }),
        onUpstreamTerminal: () => Effect.void,
      });
      const internalKey = subscription.internalizeRowKey({ id: "order-1", region: "usa" });
      expect(Result.isSuccess(internalKey)).toBe(true);
      const acquired = yield* subscription.acquire;
      const lease = Option.getOrThrow(acquired);
      const wrapped = yield* lease.attach({
        query: { select: ["id"] },
        subscription: {
          events: Stream.never,
          close: () =>
            Effect.sync(() => {
              counters.rawClose += 1;
            }),
        },
      });
      yield* subscription.start({
        acquire: Effect.succeed(Effect.never),
        release: Effect.sync(() => {
          counters.release += 1;
        }),
      });

      yield* Effect.all([wrapped.close(), wrapped.close(), Scope.close(parentScope, Exit.void)], {
        concurrency: "unbounded",
        discard: true,
      });

      expect(counters).toStrictEqual({
        cleanup: 1,
        closed: 1,
        rawClose: 1,
        release: 1,
        rowsCleared: 1,
        subscriberAdded: 1,
        subscriberRemoved: 1,
        stopping: 1,
      });
    }),
  );

  it.live("keeps client release as the winner over a queued upstream terminal", () =>
    Effect.gen(function* () {
      const identityContract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: SubscriptionRow,
          keyField: "id",
        }),
      );
      const identity = yield* Effect.fromResult(
        identityContract.leaseFromQuery({ where: { region: { eq: "usa" } } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const stoppingStarted = yield* Deferred.make<void>();
      const allowStopping = yield* Deferred.make<void>();
      const finishUpstream = yield* Deferred.make<void>();
      let closed = 0;
      let released = 0;
      let upstreamTerminals = 0;
      const subscription = yield* makeGrpcLeasedSubscription({
        parentScope,
        topic: "orders",
        identity,
        cleanupRows: () => Effect.void,
        onCleanupFailure: () => Effect.die("cleanup must succeed"),
        onClosed: Effect.sync(() => {
          closed += 1;
        }),
        onRowsCleared: Effect.void,
        onStopping: Deferred.succeed(stoppingStarted, undefined).pipe(
          Effect.andThen(Deferred.await(allowStopping)),
        ),
        onSubscriberAdded: Effect.void,
        onSubscriberRemoved: Effect.void,
        onUpstreamTerminal: () =>
          Effect.sync(() => {
            upstreamTerminals += 1;
          }),
      });
      const lease = Option.getOrThrow(yield* subscription.acquire);
      yield* subscription.start({
        acquire: Effect.succeed(
          Deferred.await(finishUpstream).pipe(
            Effect.as({
              message: "upstream completed",
              healthMessage: "upstream completed",
            }),
          ),
        ),
        release: Effect.sync(() => {
          released += 1;
        }),
      });

      const closeFiber = yield* lease.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(stoppingStarted);
      yield* Deferred.succeed(finishUpstream, undefined);
      yield* Deferred.succeed(allowStopping, undefined);
      yield* Fiber.join(closeFiber);
      yield* Scope.close(parentScope, Exit.void);

      expect({ closed, released, upstreamTerminals }).toStrictEqual({
        closed: 1,
        released: 1,
        upstreamTerminals: 0,
      });
    }),
  );

  it.live("reports a failed upstream cleanup exactly once", () =>
    Effect.gen(function* () {
      const identityContract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: SubscriptionRow,
          keyField: "id",
        }),
      );
      const identity = yield* Effect.fromResult(
        identityContract.leaseFromQuery({ where: { region: { eq: "usa" } } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const cleanupReported = yield* Deferred.make<void>();
      const counters = {
        cleanup: 0,
        cleanupFailure: 0,
        closed: 0,
        release: 0,
        subscriberAdded: 0,
        subscriberRemoved: 0,
      };
      const subscription = yield* makeGrpcLeasedSubscription({
        parentScope,
        topic: "orders",
        identity,
        cleanupRows: () =>
          Effect.sync(() => {
            counters.cleanup += 1;
          }).pipe(Effect.andThen(Effect.fail("cleanup failed"))),
        onCleanupFailure: () =>
          Effect.sync(() => {
            counters.cleanupFailure += 1;
          }).pipe(Effect.andThen(Deferred.succeed(cleanupReported, undefined)), Effect.asVoid),
        onClosed: Effect.sync(() => {
          counters.closed += 1;
        }),
        onRowsCleared: Effect.die("failed cleanup must not clear rows"),
        onStopping: Effect.void,
        onSubscriberAdded: Effect.sync(() => {
          counters.subscriberAdded += 1;
        }),
        onSubscriberRemoved: Effect.sync(() => {
          counters.subscriberRemoved += 1;
        }),
        onUpstreamTerminal: () => Effect.void,
      });
      const lease = Option.getOrThrow(yield* subscription.acquire);
      yield* subscription.start({
        acquire: Effect.succeed(
          Effect.succeed({
            message: "upstream completed",
            healthMessage: "upstream completed",
          }),
        ),
        release: Effect.sync(() => {
          counters.release += 1;
        }),
      });

      yield* Deferred.await(cleanupReported);
      yield* lease.close;
      yield* Scope.close(parentScope, Exit.void);

      expect(counters).toStrictEqual({
        cleanup: 1,
        cleanupFailure: 1,
        closed: 0,
        release: 1,
        subscriberAdded: 1,
        subscriberRemoved: 1,
      });
    }),
  );
});
