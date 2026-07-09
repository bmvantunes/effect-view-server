import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber } from "effect";
import {
  makeViewServerRuntimeLifecycle,
  type ViewServerRuntimeLifecycleResource,
} from "./runtime-lifecycle";

const resourceNames = [
  "runtimeCore",
  "grpcLeaseManager",
  "tcpPublishIngress",
  "server",
  "kafkaIngress",
  "grpcIngress",
] satisfies ReadonlyArray<ViewServerRuntimeLifecycleResource>;

const expectedCloseOrder = [
  "tcpPublishIngress",
  "grpcIngress",
  "kafkaIngress",
  "server",
  "grpcLeaseManager",
  "runtimeCore",
] satisfies ReadonlyArray<ViewServerRuntimeLifecycleResource>;

const trackedClose =
  (closed: Array<ViewServerRuntimeLifecycleResource>) =>
  (resource: ViewServerRuntimeLifecycleResource): Effect.Effect<void> =>
    Effect.sync(() => {
      closed.push(resource);
    });

describe("ViewServerRuntimeLifecycle", () => {
  it.effect("closes acquired runtime resources in canonical order", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];

      for (const resource of resourceNames) {
        yield* lifecycle.acquire(resource, Effect.succeed(resource), trackedClose(closed));
      }

      yield* lifecycle.close;
      yield* lifecycle.close;

      expect(closed).toStrictEqual(expectedCloseOrder);
    }),
  );

  it.effect("closes already acquired resources when a later acquisition fails", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      const server: ViewServerRuntimeLifecycleResource = "server";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), trackedClose(closed));
      yield* lifecycle.acquire("server", Effect.succeed(server), trackedClose(closed));

      const exit = yield* lifecycle
        .acquire("kafkaIngress", Effect.fail("kafka startup failed"), trackedClose(closed))
        .pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(closed).toStrictEqual(["server", "runtimeCore"]);
    }),
  );

  it.effect("preserves the startup failure when cleanup also fails", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      const server: ViewServerRuntimeLifecycleResource = "server";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), trackedClose(closed));
      yield* lifecycle.acquire("server", Effect.succeed(server), (resource) =>
        trackedClose(closed)(resource).pipe(Effect.andThen(Effect.die("server close failed"))),
      );

      const exit = yield* lifecycle
        .acquire("kafkaIngress", Effect.fail("kafka startup failed"), trackedClose(closed))
        .pipe(Effect.exit);

      expect(exit).toStrictEqual(Exit.fail("kafka startup failed"));
      expect(closed).toStrictEqual(["server", "runtimeCore"]);
    }),
  );

  it.effect("rejects duplicate resource acquisition without overwriting finalizers", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      let acquiredDuplicateResourceCount = 0;
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), trackedClose(closed));

      const duplicateExit = yield* lifecycle
        .acquire(
          "runtimeCore",
          Effect.sync(() => {
            acquiredDuplicateResourceCount += 1;
            return runtimeCore;
          }),
          trackedClose(closed),
        )
        .pipe(Effect.exit);

      expect(Exit.hasDies(duplicateExit)).toBe(true);
      expect(acquiredDuplicateResourceCount).toBe(0);
      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );

  it.effect("serializes concurrent duplicate resource acquisition", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const acquireStarted = yield* Deferred.make<void>();
      const releaseAcquire = yield* Deferred.make<void>();
      let acquiredDuplicateResourceCount = 0;
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";

      const firstAcquire = yield* lifecycle
        .acquire(
          "runtimeCore",
          Deferred.succeed(acquireStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseAcquire)),
            Effect.as(runtimeCore),
          ),
          trackedClose(closed),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(acquireStarted);

      const duplicateAcquire = yield* lifecycle
        .acquire(
          "runtimeCore",
          Effect.sync(() => {
            acquiredDuplicateResourceCount += 1;
            return runtimeCore;
          }),
          trackedClose(closed),
        )
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));

      yield* Deferred.succeed(releaseAcquire, undefined);
      yield* Fiber.join(firstAcquire);
      const duplicateExit = yield* Fiber.join(duplicateAcquire);

      expect(Exit.hasDies(duplicateExit)).toBe(true);
      expect(acquiredDuplicateResourceCount).toBe(0);
      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );

  it.effect("waits for in-flight resource acquisition before closing", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const acquireStarted = yield* Deferred.make<void>();
      const releaseAcquire = yield* Deferred.make<void>();
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";

      const acquireFiber = yield* lifecycle
        .acquire(
          "runtimeCore",
          Deferred.succeed(acquireStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseAcquire)),
            Effect.as(runtimeCore),
          ),
          trackedClose(closed),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(acquireStarted);
      const closeFiber = yield* lifecycle.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseAcquire, undefined);
      yield* Fiber.join(acquireFiber);
      yield* Fiber.join(closeFiber);

      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );

  it.effect("keeps pending acquisition cancellable and drains already acquired resources", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const acquireStarted = yield* Deferred.make<void>();
      const releaseAcquire = yield* Deferred.make<void>();
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      const server: ViewServerRuntimeLifecycleResource = "server";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), trackedClose(closed));

      const acquireFiber = yield* lifecycle
        .acquire(
          "server",
          Deferred.succeed(acquireStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseAcquire)),
            Effect.as(server),
          ),
          trackedClose(closed),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(acquireStarted);
      yield* Fiber.interrupt(acquireFiber);
      const interruptExit = yield* Fiber.await(acquireFiber);

      expect(Exit.hasInterrupts(interruptExit)).toBe(true);
      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );

  it.effect("finishes finalizers when close is interrupted during cleanup", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const cleanupStarted = yield* Deferred.make<void>();
      const releaseCleanup = yield* Deferred.make<void>();
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), (resource) =>
        Deferred.succeed(cleanupStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseCleanup)),
          Effect.andThen(trackedClose(closed)(resource)),
        ),
      );

      const closeFiber = yield* lifecycle.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cleanupStarted);
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(releaseCleanup, undefined);
      yield* Fiber.join(interruptFiber);
      yield* lifecycle.close;

      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );

  it.effect("rejects queued acquisitions while startup failure cleanup is draining", () =>
    Effect.gen(function* () {
      const lifecycle = yield* makeViewServerRuntimeLifecycle();
      const closed: Array<ViewServerRuntimeLifecycleResource> = [];
      const cleanupStarted = yield* Deferred.make<void>();
      const releaseCleanup = yield* Deferred.make<void>();
      let queuedAcquireCount = 0;
      const runtimeCore: ViewServerRuntimeLifecycleResource = "runtimeCore";
      const kafkaIngress: ViewServerRuntimeLifecycleResource = "kafkaIngress";
      yield* lifecycle.acquire("runtimeCore", Effect.succeed(runtimeCore), (resource) =>
        Deferred.succeed(cleanupStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseCleanup)),
          Effect.andThen(trackedClose(closed)(resource)),
        ),
      );

      const failingAcquire = yield* lifecycle
        .acquire("server", Effect.fail("server startup failed"), trackedClose(closed))
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cleanupStarted);
      const queuedAcquire = yield* lifecycle
        .acquire(
          "kafkaIngress",
          Effect.sync(() => {
            queuedAcquireCount += 1;
            return kafkaIngress;
          }),
          trackedClose(closed),
        )
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));

      yield* Deferred.succeed(releaseCleanup, undefined);
      const failingExit = yield* Fiber.join(failingAcquire);
      const queuedExit = yield* Fiber.join(queuedAcquire);

      expect(failingExit).toStrictEqual(Exit.fail("server startup failed"));
      expect(Exit.hasDies(queuedExit)).toBe(true);
      expect(queuedAcquireCount).toBe(0);
      expect(closed).toStrictEqual(["runtimeCore"]);
    }),
  );
});
