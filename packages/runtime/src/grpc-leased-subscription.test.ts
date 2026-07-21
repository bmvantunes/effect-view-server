import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Option, Result, Schema, Scope, Stream } from "effect";
import {
  makeGrpcLeasedIdentityContract,
  type GrpcLeasedIdentityContract,
} from "./grpc-leased-identity";
import { makeGrpcLeasedSubscription } from "./grpc-leased-subscription";

const SubscriptionRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
});

const leaseFromQuery = (contract: GrpcLeasedIdentityContract, query: unknown) =>
  Result.map(contract.resolveQueryRoute(query), contract.leaseFromRoute);

describe("leased gRPC Subscription", () => {
  it.effect("closes its forked owner when construction defects", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const input = {
        parentScope,
        topic: "orders",
        identity,
        cleanupRows: () => Effect.void,
        onCleanupFailure: () => Effect.void,
        onClosed: Effect.void,
        onRowsCleared: Effect.void,
        onStopping: Effect.void,
        onSubscriberAdded: Effect.void,
        onSubscriberRemoved: Effect.void,
        onUpstreamTerminal: () => Effect.void,
      };
      Object.defineProperty(input, "cleanupRows", {
        enumerable: true,
        get() {
          throw new Error("subscription construction failed");
        },
      });

      const exit = yield* makeGrpcLeasedSubscription(input).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      yield* Scope.close(parentScope, Exit.void);
    }),
  );

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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
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
              JSON.stringify(["leased-row", identity.feedKey, "order-1"]),
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
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

  it.live("closes a client lease when acquisition is interrupted after registration", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const subscriberRegistered = yield* Deferred.make<void>();
      const allowAcquireToReturn = yield* Deferred.make<void>();
      let closed = 0;
      let subscriberRemoved = 0;
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
        onStopping: Effect.void,
        onSubscriberAdded: Deferred.succeed(subscriberRegistered, undefined).pipe(
          Effect.andThen(Deferred.await(allowAcquireToReturn)),
        ),
        onSubscriberRemoved: Effect.sync(() => {
          subscriberRemoved += 1;
        }),
        onUpstreamTerminal: () => Effect.void,
      });

      const acquireFiber = yield* subscription.acquire.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(subscriberRegistered);
      const interruptFiber = yield* Fiber.interrupt(acquireFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(allowAcquireToReturn, undefined);
      yield* Fiber.join(interruptFiber);
      const acquireExit = yield* Fiber.await(acquireFiber);

      expect({
        closed,
        interrupted: Exit.hasInterrupts(acquireExit),
        subscriberRemoved,
      }).toStrictEqual({
        closed: 1,
        interrupted: true,
        subscriberRemoved: 1,
      });
      yield* Scope.close(parentScope, Exit.void);
    }),
  );

  it.live("closes a client lease when acquisition defects after registration", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      let closed = 0;
      let subscriberRemoved = 0;
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
        onStopping: Effect.void,
        onSubscriberAdded: Effect.die("subscriber ledger failed"),
        onSubscriberRemoved: Effect.sync(() => {
          subscriberRemoved += 1;
        }),
        onUpstreamTerminal: () => Effect.void,
      });

      const acquireExit = yield* subscription.acquire.pipe(Effect.exit);

      expect({
        closed,
        failed: Exit.isFailure(acquireExit),
        subscriberRemoved,
      }).toStrictEqual({
        closed: 1,
        failed: true,
        subscriberRemoved: 1,
      });
      yield* Scope.close(parentScope, Exit.void);
    }),
  );

  it.live("completes leased cleanup when subscriber removal defects", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const removalDefect = { _tag: "SubscriberRemovalDefect" } as const;
      let closed = 0;
      let subscriberRemoved = 0;
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
        onStopping: Effect.void,
        onSubscriberAdded: Effect.void,
        onSubscriberRemoved: Effect.sync(() => {
          subscriberRemoved += 1;
        }).pipe(Effect.andThen(Effect.die(removalDefect))),
        onUpstreamTerminal: () => Effect.void,
      });
      const lease = Option.getOrThrow(yield* subscription.acquire);

      const closeExit = yield* lease.close.pipe(Effect.exit, Effect.timeout("1 second"));
      const defect = Exit.isFailure(closeExit)
        ? closeExit.cause.reasons.find((reason) => reason._tag === "Die")?.defect
        : undefined;

      expect({ closed, subscriberRemoved }).toStrictEqual({
        closed: 1,
        subscriberRemoved: 1,
      });
      expect(defect).toBe(removalDefect);
      yield* Scope.close(parentScope, Exit.void).pipe(Effect.exit);
    }),
  );

  it.live("completes client and scope cleanup when stopping defects", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      const stoppingDefect = { _tag: "StoppingDefect" } as const;
      let closed = 0;
      let stopping = 0;
      let subscriberRemoved = 0;
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
        onStopping: Effect.sync(() => {
          stopping += 1;
        }).pipe(Effect.andThen(Effect.die(stoppingDefect))),
        onSubscriberAdded: Effect.void,
        onSubscriberRemoved: Effect.sync(() => {
          subscriberRemoved += 1;
        }),
        onUpstreamTerminal: () => Effect.void,
      });
      yield* subscription.acquire;

      const closeExit = yield* subscription.close.pipe(Effect.exit, Effect.timeout("1 second"));
      const defect = Exit.isFailure(closeExit)
        ? closeExit.cause.reasons.find((reason) => reason._tag === "Die")?.defect
        : undefined;

      expect({ closed, stopping, subscriberRemoved }).toStrictEqual({
        closed: 1,
        stopping: 1,
        subscriberRemoved: 1,
      });
      expect(defect).toBe(stoppingDefect);
      yield* Scope.close(parentScope, Exit.void).pipe(Effect.exit);
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
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

  it.live("detaches completed subscriptions from the parent scope under route churn", () =>
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
        leaseFromQuery(identityContract, { routeBy: { region: "usa" } }),
      );
      const parentScope = yield* Scope.make("sequential");
      let cleaned = 0;
      let closed = 0;

      yield* Effect.forEach(
        Array.from({ length: 25 }),
        () =>
          Effect.gen(function* () {
            const subscription = yield* makeGrpcLeasedSubscription({
              parentScope,
              topic: "orders",
              identity,
              cleanupRows: () =>
                Effect.sync(() => {
                  cleaned += 1;
                }),
              onCleanupFailure: () => Effect.die("cleanup must succeed"),
              onClosed: Effect.sync(() => {
                closed += 1;
              }),
              onRowsCleared: Effect.void,
              onStopping: Effect.void,
              onSubscriberAdded: Effect.void,
              onSubscriberRemoved: Effect.void,
              onUpstreamTerminal: () => Effect.void,
            });
            yield* subscription.close;
          }),
        { discard: true },
      );

      const retainedParentFinalizers =
        parentScope.state._tag === "Open" ? parentScope.state.finalizers.size : 0;
      expect({ cleaned, closed, retainedParentFinalizers }).toStrictEqual({
        cleaned: 25,
        closed: 25,
        retainedParentFinalizers: 0,
      });
      yield* Scope.close(parentScope, Exit.void);
    }),
  );
});
