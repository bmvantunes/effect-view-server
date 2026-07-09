import { runAllFinalizers } from "@effect-view-server/effect-utils";
import { Effect, Ref, Semaphore } from "effect";

type ViewServerRuntimeLifecycleState = {
  readonly closed: boolean;
  readonly grpcIngress?: Effect.Effect<void>;
  readonly grpcLeaseManager?: Effect.Effect<void>;
  readonly kafkaIngress?: Effect.Effect<void>;
  readonly runtimeCore?: Effect.Effect<void>;
  readonly server?: Effect.Effect<void>;
  readonly tcpPublishIngress?: Effect.Effect<void>;
};

export type ViewServerRuntimeLifecycleResource =
  | "grpcIngress"
  | "grpcLeaseManager"
  | "kafkaIngress"
  | "runtimeCore"
  | "server"
  | "tcpPublishIngress";

export type ViewServerRuntimeLifecycle = {
  readonly acquire: <A, E, R>(
    resource: ViewServerRuntimeLifecycleResource,
    acquire: Effect.Effect<A, E, R>,
    close: (value: A) => Effect.Effect<void>,
  ) => Effect.Effect<A, E, R>;
  readonly close: Effect.Effect<void>;
};

const emptyLifecycleState: ViewServerRuntimeLifecycleState = {
  closed: false,
};

const closedLifecycleState: ViewServerRuntimeLifecycleState = {
  closed: true,
};

const ignoreRuntimeStartupCleanupFailure = <R>(
  cleanup: Effect.Effect<void, never, R>,
): Effect.Effect<void, never, R> =>
  cleanup.pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Ignoring runtime startup cleanup failure.", cause),
    ),
  );

const lifecycleStateWithResource = (
  state: ViewServerRuntimeLifecycleState,
  resource: ViewServerRuntimeLifecycleResource,
  close: Effect.Effect<void>,
): ViewServerRuntimeLifecycleState => {
  switch (resource) {
    case "grpcIngress":
      return { ...state, grpcIngress: close };
    case "grpcLeaseManager":
      return { ...state, grpcLeaseManager: close };
    case "kafkaIngress":
      return { ...state, kafkaIngress: close };
    case "runtimeCore":
      return { ...state, runtimeCore: close };
    case "server":
      return { ...state, server: close };
    case "tcpPublishIngress":
      return { ...state, tcpPublishIngress: close };
  }
};

const lifecycleFinalizers = (
  state: ViewServerRuntimeLifecycleState,
): ReadonlyArray<Effect.Effect<void>> => {
  const finalizers: Array<Effect.Effect<void>> = [];
  if (state.tcpPublishIngress !== undefined) {
    finalizers.push(state.tcpPublishIngress);
  }
  if (state.grpcIngress !== undefined) {
    finalizers.push(state.grpcIngress);
  }
  if (state.kafkaIngress !== undefined) {
    finalizers.push(state.kafkaIngress);
  }
  if (state.server !== undefined) {
    finalizers.push(state.server);
  }
  if (state.grpcLeaseManager !== undefined) {
    finalizers.push(state.grpcLeaseManager);
  }
  if (state.runtimeCore !== undefined) {
    finalizers.push(state.runtimeCore);
  }
  return finalizers;
};

const lifecycleStateHasResource = (
  state: ViewServerRuntimeLifecycleState,
  resource: ViewServerRuntimeLifecycleResource,
): boolean => {
  switch (resource) {
    case "grpcIngress":
      return state.grpcIngress !== undefined;
    case "grpcLeaseManager":
      return state.grpcLeaseManager !== undefined;
    case "kafkaIngress":
      return state.kafkaIngress !== undefined;
    case "runtimeCore":
      return state.runtimeCore !== undefined;
    case "server":
      return state.server !== undefined;
    case "tcpPublishIngress":
      return state.tcpPublishIngress !== undefined;
  }
};

const lifecycleStateRejectsAcquire = (
  state: ViewServerRuntimeLifecycleState,
  resource: ViewServerRuntimeLifecycleResource,
): boolean => state.closed || lifecycleStateHasResource(state, resource);

export const makeViewServerRuntimeLifecycle = Effect.fn("ViewServerRuntimeLifecycle.make")(
  function* () {
    const state = yield* Ref.make<ViewServerRuntimeLifecycleState>(emptyLifecycleState);
    const acquireLock = yield* Semaphore.make(1);
    const drain = Effect.fn("ViewServerRuntimeLifecycle.drain")(function* () {
      const finalizers = lifecycleFinalizers(yield* Ref.getAndSet(state, closedLifecycleState));
      yield* runAllFinalizers(finalizers);
    });
    const drainUninterruptibly = Effect.uninterruptible(drain());
    const close = Effect.fn("ViewServerRuntimeLifecycle.close")(function* () {
      yield* acquireLock.withPermit(drainUninterruptibly);
    });
    const acquire: ViewServerRuntimeLifecycle["acquire"] = (
      resource,
      acquireResource,
      closeResource,
    ) => {
      const acquireWithAtomicRegistration = Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(state);
          if (lifecycleStateRejectsAcquire(current, resource)) {
            return yield* Effect.die(
              new Error(`Runtime lifecycle resource already acquired: ${resource}`),
            );
          }
          const value = yield* restore(acquireResource);
          yield* Ref.update(state, (next) =>
            lifecycleStateWithResource(next, resource, closeResource(value)),
          );
          return value;
        }),
      ).pipe(
        Effect.catchCause((cause) =>
          ignoreRuntimeStartupCleanupFailure(drainUninterruptibly).pipe(
            Effect.andThen(Effect.failCause(cause)),
          ),
        ),
        Effect.onInterrupt(() => ignoreRuntimeStartupCleanupFailure(drainUninterruptibly)),
      );
      return acquireLock.withPermit(acquireWithAtomicRegistration);
    };
    return {
      acquire,
      close: close(),
    };
  },
);
