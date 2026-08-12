import type {
  RuntimeDependency,
  RuntimeDependencyIssue,
  RuntimeHeartbeat,
  RuntimeSourceReportingSnapshot,
} from "@effect-view-server/runtime-core";
import {
  sameRuntimeDependencies,
  sameRuntimeSourceReportingSnapshot,
} from "@effect-view-server/runtime-core";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Queue,
  Ref,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import type { ResolvedViewServerRuntimeBaseOptions } from "./runtime-options";

type ReportingOptions = NonNullable<ResolvedViewServerRuntimeBaseOptions["reporting"]>;

const runCallback = (
  name: "heartbeat" | "dependencies",
  callback: Effect.Effect<void>,
): Effect.Effect<void> =>
  callback.pipe(
    Effect.catchCause((cause) => {
      const interruptReasons = cause.reasons.filter(Cause.isInterruptReason);
      const nonInterruptReasons = cause.reasons.filter(Cause.isDieReason);
      const logFailure =
        nonInterruptReasons.length === 0
          ? Effect.void
          : Effect.logWarning(
              `View Server ${name} reporting callback failed.`,
              Cause.fromReasons(nonInterruptReasons),
            );
      return interruptReasons.length === 0
        ? logFailure
        : logFailure.pipe(
            Effect.andThen(Effect.failCause(Cause.fromReasons<never>(interruptReasons))),
          );
    }),
  );

const forkCallback = <Value>(
  scope: Scope.Scope,
  name: "heartbeat" | "dependencies",
  callback: (value: Value) => Effect.Effect<void>,
  value: Value,
) =>
  Effect.gen(function* () {
    const invoked = yield* Deferred.make<void>();
    const callbackEffect = Effect.matchCauseEffect(
      Effect.sync(() => callback(value)),
      {
        onFailure: (cause) =>
          Deferred.succeed(invoked, undefined).pipe(Effect.andThen(Effect.failCause(cause))),
        onSuccess: (effect) => Deferred.succeed(invoked, undefined).pipe(Effect.andThen(effect)),
      },
    );
    const fiber = yield* Effect.forkIn(runCallback(name, callbackEffect), scope, {
      startImmediately: true,
    });
    yield* Deferred.await(invoked);
    return fiber;
  });

const makeEmitter = Effect.fn("ViewServerRuntime.reporting.emitter.make")(function* <Value>(
  name: "heartbeat" | "dependencies",
  scope: Scope.Scope,
  interval: Duration.Duration,
  changeInterval: Duration.Duration,
  read: Effect.Effect<Value>,
  callback: (value: Value) => Effect.Effect<void>,
) {
  const changeQueue = yield* Queue.dropping<void>(1);
  const lock = Semaphore.makeUnsafe(1);
  const lastEmission = yield* Ref.make<bigint | undefined>(undefined);
  const requestedChange = yield* Ref.make(0n);
  const emittedChange = yield* Ref.make(0n);
  const changeIntervalNanos = Duration.toNanosUnsafe(changeInterval);
  const emitPeriodic = lock.withPermit(
    Effect.gen(function* () {
      const value = yield* read;
      yield* runCallback(
        name,
        Effect.suspend(() => callback(value)),
      );
      yield* Ref.set(lastEmission, yield* Clock.currentTimeNanos);
    }),
  );
  const emitChanged = Effect.gen(function* () {
    while (true) {
      const remaining = yield* lock.withPermit(
        Effect.gen(function* () {
          const requested = yield* Ref.get(requestedChange);
          if (requested === (yield* Ref.get(emittedChange))) {
            return undefined;
          }
          const now = yield* Clock.currentTimeNanos;
          const previous = yield* Ref.get(lastEmission);
          if (previous !== undefined && now - previous < changeIntervalNanos) {
            return Duration.nanos(changeIntervalNanos - (now - previous));
          }
          const value = yield* read;
          yield* runCallback(
            name,
            Effect.suspend(() => callback(value)),
          );
          yield* Ref.set(lastEmission, yield* Clock.currentTimeNanos);
          yield* Ref.set(emittedChange, requested);
          return undefined;
        }),
      );
      if (remaining === undefined) {
        return;
      }
      yield* Effect.sleep(remaining);
    }
  });
  const changeFiber = yield* Effect.forkIn(
    Effect.forever(Queue.take(changeQueue).pipe(Effect.andThen(emitChanged))),
    scope,
    { startImmediately: true },
  );
  const periodicFiber = yield* Effect.forkIn(
    Effect.forever(Effect.sleep(interval).pipe(Effect.andThen(emitPeriodic))),
    scope,
    {
      startImmediately: true,
    },
  );
  return {
    signal: Ref.update(requestedChange, (version) => version + 1n).pipe(
      Effect.andThen(Queue.offer(changeQueue, undefined)),
      Effect.asVoid,
    ),
    stop: Fiber.interrupt(changeFiber).pipe(
      Effect.andThen(Fiber.interrupt(periodicFiber)),
      Effect.asVoid,
    ),
  };
});

const lifecycleHeartbeat = (
  scope: Scope.Scope,
  options: ReportingOptions,
  status: "Starting" | "Stopping",
) =>
  forkCallback(
    scope,
    "heartbeat",
    options.onHeartbeat,
    Object.freeze({
      status,
      problems: Object.freeze([]),
    }),
  );

export const startingHeartbeat = (scope: Scope.Scope, options: ReportingOptions) =>
  lifecycleHeartbeat(scope, options, "Starting");

export const stoppingHeartbeat = (
  scope: Scope.Scope,
  options: ReportingOptions,
  startingFiber: Fiber.Fiber<void, never>,
): Effect.Effect<void> =>
  Fiber.interrupt(startingFiber).pipe(
    Effect.andThen(lifecycleHeartbeat(scope, options, "Stopping")),
    Effect.asVoid,
  );

export const makeRuntimeReporting = Effect.fn("ViewServerRuntime.reporting.make")(function* (
  scope: Scope.Scope,
  options: ReportingOptions,
  reporting: {
    readonly snapshot: Effect.Effect<RuntimeSourceReportingSnapshot>;
    readonly changes: Stream.Stream<RuntimeSourceReportingSnapshot>;
  },
) {
  const heartbeat = yield* makeEmitter(
    "heartbeat",
    scope,
    options.heartbeatInterval,
    options.changeInterval,
    reporting.snapshot.pipe(Effect.map(({ heartbeat }) => heartbeat)),
    options.onHeartbeat,
  );
  const dependencies = yield* makeEmitter(
    "dependencies",
    scope,
    options.dependenciesInterval,
    options.changeInterval,
    reporting.snapshot.pipe(Effect.map(({ dependencies }) => dependencies)),
    options.onDependenciesUpdate,
  );
  const previous = yield* Ref.make(yield* reporting.snapshot);
  yield* Effect.forkIn(
    reporting.changes.pipe(
      Stream.runForEach((next) =>
        Effect.gen(function* () {
          const prior = yield* Ref.getAndSet(previous, next);
          if (sameRuntimeSourceReportingSnapshot(prior, next)) {
            return;
          }
          yield* heartbeat.signal;
          if (!sameRuntimeDependencies(prior.dependencies, next.dependencies)) {
            yield* dependencies.signal;
          }
        }),
      ),
    ),
    scope,
    { startImmediately: true },
  );
  yield* heartbeat.signal;
  const stopping: RuntimeHeartbeat = Object.freeze({
    status: "Stopping",
    problems: Object.freeze([]),
  });
  const stoppingFiber = yield* Effect.cached(
    dependencies.stop.pipe(
      Effect.andThen(heartbeat.stop),
      Effect.andThen(forkCallback(scope, "heartbeat", options.onHeartbeat, stopping)),
      Effect.asVoid,
      Effect.forkDetach({ startImmediately: true }),
    ),
  );
  return {
    stopping: stoppingFiber.pipe(Effect.flatMap(Fiber.join), Effect.asVoid),
  };
});

export type { RuntimeDependency, RuntimeDependencyIssue, RuntimeHeartbeat };
