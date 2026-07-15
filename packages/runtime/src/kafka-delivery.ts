import { Deferred, Effect, Exit, Fiber, Scope } from "effect";

export type KafkaDelivery = {
  readonly close: Effect.Effect<void>;
};

export type StartKafkaDeliveryWorker = <A, E, R, R2>(
  start: Effect.Effect<A, E, R>,
  deliver: (resource: A) => Effect.Effect<void, E, R2>,
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
  ) {
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const workerScope = yield* Scope.make("parallel");
        yield* Scope.addFinalizerExit(scope, (exit) => Scope.close(workerScope, exit));
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
          Effect.flatMap((exit) => Scope.close(workerScope, exit).pipe(Effect.uninterruptible)),
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
        const startWorker: StartKafkaDeliveryWorker = (workerStart, deliver) =>
          startScopedKafkaDeliveryWorker(scope, workerStart, deliver);
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
