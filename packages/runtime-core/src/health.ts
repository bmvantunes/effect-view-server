import type {
  ColumnLiveViewEngineHealth,
  DecodableTopicDefinitions,
} from "@effect-view-server/column-live-view-engine";
import type { TransportHealth, ViewServerHealth } from "@effect-view-server/config";
import { Clock, Deferred, Effect, Exit, Fiber, Scope, Semaphore, type Duration } from "effect";

type EngineHealthReader<Topics extends DecodableTopicDefinitions> = {
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
};

type RuntimeCoreHealthTiming = {
  readonly nowMillis: number;
  readonly nowNanos: bigint;
  readonly runtimeStartedAtNanos: bigint;
};

type RuntimeCoreHealthInput<Topics extends DecodableTopicDefinitions> = {
  readonly transportHealth?: RuntimeCoreTransportHealth<Topics>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<Topics>;
  readonly timing?: RuntimeCoreHealthTiming;
};

type ReadHealthInput<Topics extends DecodableTopicDefinitions> = {
  readonly runtimeStartedAtNanos: bigint;
  readonly transportHealth: RuntimeCoreTransportHealth<Topics>;
  readonly healthOverlay: RuntimeCoreHealthOverlay<Topics> | undefined;
};

const zeroRuntimeCoreHealthTiming: RuntimeCoreHealthTiming = {
  nowMillis: 0,
  nowNanos: 0n,
  runtimeStartedAtNanos: 0n,
};

const uptimeMillis = (timing: RuntimeCoreHealthTiming): number => {
  const elapsedNanos = timing.nowNanos - timing.runtimeStartedAtNanos;
  return elapsedNanos <= 0n ? 0 : Number(elapsedNanos / 1_000_000n);
};

export const healthFromEngine = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
  input: RuntimeCoreHealthInput<Topics> = {},
): ViewServerHealth<Topics> => {
  const transportHealth = input.transportHealth ?? defaultRuntimeCoreTransportHealth;
  const healthOverlay = input.healthOverlay ?? defaultRuntimeCoreHealthOverlay;
  const timing = input.timing ?? zeroRuntimeCoreHealthTiming;
  return healthOverlay(
    {
      status: engineHealth.status,
      version: engineHealth.version,
      uptimeMs: uptimeMillis(timing),
      engine: { topics: engineHealth.topics },
      transport: transportHealth(engineHealth),
    },
    timing.nowMillis,
  );
};

export type RuntimeCoreTransportHealth<Topics extends DecodableTopicDefinitions> = (
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
) => TransportHealth;

export type RuntimeCoreHealthOverlay<Topics extends DecodableTopicDefinitions> = (
  health: ViewServerHealth<Topics>,
  nowMillis: number,
) => ViewServerHealth<Topics>;

export const defaultRuntimeCoreHealthOverlay = <Topics extends DecodableTopicDefinitions>(
  health: ViewServerHealth<Topics>,
  _nowMillis: number,
): ViewServerHealth<Topics> => health;

export const defaultRuntimeCoreTransportHealth = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
): TransportHealth => ({
  activeClients: 0,
  activeStreams: 0,
  activeSubscriptions: engineHealth.activeSubscriptions,
  messagesPerSecond: 0,
  bytesPerSecond: 0,
  queuedMessages: engineHealth.queuedEvents,
  queuedBytes: 0,
  droppedClients: 0,
  backpressureEvents: engineHealth.backpressureEvents,
  reconnects: 0,
  lastError: null,
});

export const readHealthSnapshot = Effect.fn("ViewServerRuntimeCore.health.readSnapshot")(function* <
  const Topics extends DecodableTopicDefinitions,
>(engine: EngineHealthReader<Topics>, input: ReadHealthInput<Topics>) {
  const nowMillis = yield* Clock.currentTimeMillis;
  const nowNanos = yield* Clock.currentTimeNanos;
  return healthFromEngine(yield* engine.health(), {
    transportHealth: input.transportHealth,
    healthOverlay: input.healthOverlay ?? defaultRuntimeCoreHealthOverlay,
    timing: {
      nowMillis,
      nowNanos,
      runtimeStartedAtNanos: input.runtimeStartedAtNanos,
    },
  });
});

export const makeCoalescedHealthReader = <const Topics extends DecodableTopicDefinitions, E>(
  read: (epoch: number) => Effect.Effect<ViewServerHealth<Topics>, E>,
  currentEpoch: () => number = () => 0,
) => {
  type ActiveRead = {
    readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, E>;
    readonly epoch: number;
  };
  let activeRead: ActiveRead | undefined = undefined;
  const stateLock = Semaphore.makeUnsafe(1);
  type ReadDecision =
    | {
        readonly _tag: "leader";
        readonly active: ActiveRead;
      }
    | {
        readonly _tag: "follower";
        readonly deferred: Deferred.Deferred<ViewServerHealth<Topics>, E>;
      };

  const completeActiveRead = (active: ActiveRead, exit: Exit.Exit<ViewServerHealth<Topics>, E>) =>
    Effect.uninterruptible(
      stateLock.withPermit(
        Effect.gen(function* () {
          yield* Deferred.done(active.deferred, exit);
          yield* Effect.sync(() => {
            if (activeRead === active) {
              activeRead = undefined;
            }
          });
        }),
      ),
    );

  return Effect.fn("ViewServerRuntimeCore.health.readCoalesced")(function* () {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const decision = yield* stateLock.withPermit(
          Effect.gen(function* () {
            const epoch = currentEpoch();
            if (activeRead !== undefined && activeRead.epoch === epoch) {
              return {
                _tag: "follower",
                deferred: activeRead.deferred,
              } satisfies ReadDecision;
            }
            const nextRead = yield* Deferred.make<ViewServerHealth<Topics>, E>();
            const active = {
              deferred: nextRead,
              epoch,
            };
            activeRead = active;
            return {
              _tag: "leader",
              active,
            } satisfies ReadDecision;
          }),
        );
        if (decision._tag === "follower") {
          return yield* restore(Deferred.await(decision.deferred));
        }
        return yield* read(decision.active.epoch).pipe(
          Effect.onExit((exit) => completeActiveRead(decision.active, exit)),
        );
      }),
    );
  });
};

export const makeHealthRefreshScheduler = Effect.fn(
  "ViewServerRuntimeCore.healthRefreshScheduler.make",
)(function* (refresh: Effect.Effect<void>, cadence: Duration.Input = "1 second") {
  const schedulerScope = yield* Scope.make("parallel");
  let scheduled = false;
  let pending = false;
  let closed = false;
  let activeFiber: Fiber.Fiber<void> | undefined = undefined;
  let activeToken: SchedulerRunToken | undefined = undefined;
  let nextToken = 0;
  const stateLock = Semaphore.makeUnsafe(1);
  type SchedulerRunToken = {
    readonly id: number;
  };
  type RequestDecision =
    | { readonly _tag: "closed" }
    | { readonly _tag: "pending" }
    | { readonly _tag: "start"; readonly token: SchedulerRunToken };

  const clearActiveRun = (token: SchedulerRunToken) =>
    stateLock.withPermit(
      Effect.sync(() => {
        if (activeToken === token) {
          activeFiber = undefined;
          activeToken = undefined;
          scheduled = false;
          pending = false;
        }
      }),
    );

  const drainRefreshes = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.drain")(
    function* () {
      let shouldRefresh = true;
      while (shouldRefresh) {
        yield* stateLock.withPermit(
          Effect.sync(() => {
            pending = false;
          }),
        );
        yield* Effect.sleep(cadence);
        yield* refresh;
        shouldRefresh = yield* stateLock.withPermit(
          Effect.sync(() => {
            if (pending) {
              return true;
            }
            activeFiber = undefined;
            activeToken = undefined;
            scheduled = false;
            return false;
          }),
        );
      }
    },
  );

  const requestRefresh = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.request")(
    function* () {
      yield* Effect.uninterruptible(
        stateLock.withPermit(
          Effect.gen(function* () {
            const decision = yield* Effect.sync((): RequestDecision => {
              if (closed) {
                return { _tag: "closed" };
              }
              if (scheduled) {
                pending = true;
                return { _tag: "pending" };
              }
              const token: SchedulerRunToken = { id: nextToken };
              nextToken += 1;
              scheduled = true;
              pending = true;
              activeToken = token;
              return { _tag: "start", token };
            });
            if (decision._tag !== "start") {
              return;
            }
            const { token } = decision;
            const fiber = yield* drainRefreshes().pipe(
              Effect.ensuring(clearActiveRun(token)),
              Effect.forkIn(schedulerScope, { startImmediately: true }),
            );
            yield* Effect.sync(() => {
              activeFiber = fiber;
            });
          }),
        ),
      );
    },
  );

  const close = Effect.fn("ViewServerRuntimeCore.healthRefreshScheduler.close")(function* () {
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const fiber = yield* stateLock.withPermit(
          Effect.sync(() => {
            const current = activeFiber;
            closed = true;
            activeFiber = undefined;
            activeToken = undefined;
            scheduled = false;
            pending = false;
            return current;
          }),
        );
        if (fiber !== undefined) {
          yield* Fiber.interrupt(fiber).pipe(Effect.asVoid);
        }
        yield* Scope.close(schedulerScope, Exit.void);
      }),
    );
  });

  return {
    close: close(),
    request: requestRefresh(),
  };
});
