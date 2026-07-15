import { runAllFinalizers } from "@effect-view-server/effect-utils";
import { Deferred, Effect, Exit, Fiber, MutableRef, Scope } from "effect";

export type KafkaDelivery = {
  readonly close: Effect.Effect<void>;
};

export type StartKafkaDeliveryWorker = <A, E, R, R2>(
  start: Effect.Effect<A, E, R>,
  deliver: (resource: A) => Effect.Effect<void, E, R2>,
  onShutdown?: Effect.Effect<void>,
) => Effect.Effect<void, E, Exclude<R, Scope.Scope> | R2>;

export const acquireKafkaDeliveryResource = Effect.fn(
  "ViewServerRuntime.kafka.delivery.acquireResource",
)(function* <A, E, R, R2>(
  acquire: Effect.Effect<A, E, R>,
  release: (resource: A, exit: Exit.Exit<unknown, unknown>) => Effect.Effect<unknown, never, R2>,
) {
  return yield* Effect.acquireRelease(acquire, release, {
    interruptible: true,
  });
});

const startScopedKafkaDeliveryWorker = Effect.fn("ViewServerRuntime.kafka.delivery.startWorker")(
  function* <A, E, R, R2>(
    scope: Scope.Scope,
    start: Effect.Effect<A, E, R>,
    deliver: (resource: A) => Effect.Effect<void, E, R2>,
    onShutdown: Effect.Effect<void>,
  ) {
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const workerScope = yield* Scope.make("parallel");
        const shutdownExit = yield* Deferred.make<Exit.Exit<unknown, unknown>>();
        const shutdownStarted = MutableRef.make(false);
        const shutdownCompleted = yield* Deferred.make<Exit.Exit<void, never>>();
        const awaitShutdownResult = Effect.fn(
          "ViewServerRuntime.kafka.delivery.awaitShutdownResult",
        )(function* () {
          const shutdownResult = yield* Deferred.await(shutdownCompleted);
          if (Exit.isFailure(shutdownResult)) {
            return yield* Effect.failCause(shutdownResult.cause);
          }
        });
        const requestWorkerShutdown = (exit: Exit.Exit<unknown, unknown>) =>
          Deferred.succeed(shutdownExit, exit).pipe(
            Effect.andThen(
              Effect.gen(function* () {
                const isShutdownLeader = yield* Effect.sync(() => {
                  if (shutdownStarted.current) {
                    return false;
                  }
                  shutdownStarted.current = true;
                  return true;
                });
                if (isShutdownLeader) {
                  const shutdownResult = yield* Effect.exit(
                    Deferred.await(shutdownExit).pipe(
                      Effect.flatMap((shutdownExit) =>
                        runAllFinalizers([Scope.close(workerScope, shutdownExit), onShutdown]),
                      ),
                    ),
                  );
                  yield* Deferred.succeed(shutdownCompleted, shutdownResult);
                }
                yield* awaitShutdownResult();
              }).pipe(Effect.uninterruptible),
            ),
          );
        yield* Scope.addFinalizerExit(scope, requestWorkerShutdown);
        const started = yield* Deferred.make<void, E>();
        const worker = yield* Effect.gen(function* () {
          const resource = yield* start;
          yield* Deferred.succeed(started, undefined);
          yield* deliver(resource);
        }).pipe(
          Effect.onExit((exit) => Deferred.done(started, exit).pipe(Effect.asVoid)),
          Effect.provideService(Scope.Scope, workerScope),
          Effect.forkIn(workerScope, { startImmediately: true }),
        );
        yield* Fiber.await(worker).pipe(
          Effect.flatMap(requestWorkerShutdown),
          Effect.forkIn(scope, { startImmediately: true }),
        );
        yield* restore(Deferred.await(started));
      }),
    );
  },
);

export const makeScopedKafkaDelivery = Effect.fn("ViewServerRuntime.kafka.delivery.makeScoped")(
  function* <E, R>(start: (startWorker: StartKafkaDeliveryWorker) => Effect.Effect<void, E, R>) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make("parallel");
        const startWorker: StartKafkaDeliveryWorker = (workerStart, deliver, onShutdown) =>
          startScopedKafkaDeliveryWorker(scope, workerStart, deliver, onShutdown ?? Effect.void);
        yield* restore(start(startWorker)).pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        );
        const close = (yield* Effect.cached(Scope.close(scope, Exit.void))).pipe(
          Effect.uninterruptible,
        );
        return {
          close,
        };
      }),
    );
  },
);
