import type { StatusEvent } from "@effect-view-server/config";
import type { ViewServerLiveEvent, ViewServerLiveSubscription } from "@effect-view-server/client";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import type { ViewServerRuntimeCoreTerminalObserver } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Option, Result, Scope, Semaphore, Stream } from "effect";
import type {
  GrpcLeasedGroupedKeyRetentionObserver,
  GrpcLeasedIdentityError,
  GrpcLeasedIdentityLease,
  GrpcLeasedInternalRowKey,
  GrpcLeasedResultKeyTranslation,
} from "./grpc-leased-identity";

export type GrpcLeasedUpstreamTerminal = {
  readonly message: string;
  readonly healthMessage: string;
};

type UpstreamSubscriptionTerminal = {
  readonly _tag: "Upstream";
  readonly message: string;
};

type EngineSubscriptionTerminal = {
  readonly _tag: "Engine";
  readonly ready: Deferred.Deferred<void>;
  readonly status: StatusEvent;
};

type RuntimeSubscriptionTerminal = {
  readonly _tag: "Runtime";
};

type ClosedSubscriptionTerminal = {
  readonly _tag: "Closed";
};

type SubscriptionTerminal =
  | UpstreamSubscriptionTerminal
  | EngineSubscriptionTerminal
  | RuntimeSubscriptionTerminal
  | ClosedSubscriptionTerminal;

const closedSubscriptionTerminal: ClosedSubscriptionTerminal = {
  _tag: "Closed",
};

const ignoreLeasedSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC subscription close failure.",
);

const grpcLeasedResultKeyFailureTypeId: unique symbol = Symbol(
  "@effect-view-server/runtime/GrpcLeasedResultKeyFailure",
);

type GrpcLeasedResultKeyFailure = {
  readonly [grpcLeasedResultKeyFailureTypeId]: true;
  readonly queryId: string;
  readonly error: GrpcLeasedIdentityError;
};

const resultKeyTranslationFailure = (
  queryId: string,
  error: GrpcLeasedIdentityError,
): GrpcLeasedResultKeyFailure => ({
  [grpcLeasedResultKeyFailureTypeId]: true,
  queryId,
  error,
});

const isResultKeyTranslationFailure = <Row extends object>(
  value: ViewServerLiveEvent<Row> | GrpcLeasedResultKeyFailure,
): value is GrpcLeasedResultKeyFailure => grpcLeasedResultKeyFailureTypeId in value;

const isTerminalStatusEvent = (event: ViewServerLiveEvent<unknown>): event is StatusEvent =>
  event.type === "status" && (event.status === "closed" || event.status === "error");

const externalizeLeasedEvent = <Row extends object>(
  resultKeys: GrpcLeasedResultKeyTranslation<Row>,
  event: ViewServerLiveEvent<Row>,
): ViewServerLiveEvent<Row> | GrpcLeasedResultKeyFailure => {
  if (event.type === "snapshot") {
    const keys = resultKeys.translateSnapshot(event.keys, event.rows);
    if (Result.isFailure(keys)) {
      return resultKeyTranslationFailure(event.queryId, keys.failure);
    }
    return {
      ...event,
      keys: keys.success,
    };
  }
  if (event.type === "delta") {
    const operations = resultKeys.translateDelta(event.operations);
    if (Result.isFailure(operations)) {
      return resultKeyTranslationFailure(event.queryId, operations.failure);
    }
    return {
      ...event,
      operations: operations.success,
    };
  }
  return event;
};

const resultKeyEncodingErrorStatus = (
  topic: string,
  queryId: string,
  error: GrpcLeasedIdentityError,
): StatusEvent => ({
  type: "status",
  topic,
  queryId,
  status: "error",
  code: "RuntimeUnavailable",
  message: error.message,
});

type SubscriptionTerminalRegistration = {
  readonly observer: ViewServerRuntimeCoreTerminalObserver;
  readonly queryId: Deferred.Deferred<string>;
};

const makeSubscriptionTerminalRegistration = Effect.fn(
  "ViewServerRuntime.grpc.leased.subscription.terminalRegistration.make",
)(function* (terminal: Deferred.Deferred<SubscriptionTerminal>) {
  const ready = yield* Deferred.make<void>();
  const queryId = yield* Deferred.make<string>();
  const observer: ViewServerRuntimeCoreTerminalObserver = {
    onQueryRegistered: (registeredQueryId) =>
      Deferred.succeed(queryId, registeredQueryId).pipe(Effect.asVoid),
    onTerminalOccurrence: (status) =>
      Deferred.succeed(terminal, {
        _tag: "Engine",
        ready,
        status,
      }).pipe(Effect.asVoid),
    onTerminalReady: () => Deferred.succeed(ready, undefined).pipe(Effect.asVoid),
  };
  return {
    observer,
    queryId,
  } satisfies SubscriptionTerminalRegistration;
});

export type GrpcLeasedSubscriptionAttachInput<Row extends object> = {
  readonly query: unknown;
  readonly subscription: ViewServerLiveSubscription<Row>;
};

export type GrpcLeasedSubscriptionLease = {
  readonly terminalObserver: ViewServerRuntimeCoreTerminalObserver;
  readonly attach: <Row extends object>(
    input: GrpcLeasedSubscriptionAttachInput<Row>,
  ) => Effect.Effect<ViewServerLiveSubscription<Row>>;
  readonly close: Effect.Effect<void>;
};

export type GrpcLeasedSubscriptionStart<Error> = {
  readonly acquire: Effect.Effect<Effect.Effect<GrpcLeasedUpstreamTerminal>, Error>;
  readonly release: Effect.Effect<void>;
};

export type GrpcLeasedSubscriptionInput<Error> = {
  readonly parentScope: Scope.Scope;
  readonly topic: string;
  readonly identity: GrpcLeasedIdentityLease;
  readonly groupedKeyRetentionObserver?: GrpcLeasedGroupedKeyRetentionObserver;
  readonly cleanupRows: (keys: ReadonlySet<string>) => Effect.Effect<void, Error>;
  readonly onCleanupFailure: (cause: Cause.Cause<Error>) => Effect.Effect<void>;
  readonly onClosed: Effect.Effect<void>;
  readonly onRowsCleared: Effect.Effect<void>;
  readonly onStopping: Effect.Effect<void>;
  readonly onSubscriberAdded: Effect.Effect<void>;
  readonly onSubscriberRemoved: Effect.Effect<void>;
  readonly onUpstreamTerminal: (terminal: GrpcLeasedUpstreamTerminal) => Effect.Effect<void>;
};

type ClientCloseDisposition = "None" | "Initiate" | "Join";

type ActiveClientLease = {
  readonly markClosed: Effect.Effect<boolean>;
  readonly closeChild: Effect.Effect<void>;
  readonly notifyUpstream: (terminal: GrpcLeasedUpstreamTerminal) => Effect.Effect<boolean>;
};

export type GrpcLeasedSubscription<Error> = {
  readonly feedKey: string;
  readonly materializeRoute: GrpcLeasedIdentityLease["materializeRoute"];
  readonly validateRowRoute: GrpcLeasedIdentityLease["validateRowRoute"];
  readonly internalizeRowKey: <Row extends object>(
    row: Row,
  ) => Result.Result<GrpcLeasedInternalRowKey, GrpcLeasedIdentityError>;
  readonly retainedRowCount: () => number;
  readonly acquire: Effect.Effect<Option.Option<GrpcLeasedSubscriptionLease>>;
  readonly start: (input: GrpcLeasedSubscriptionStart<Error>) => Effect.Effect<void, Error>;
  readonly close: Effect.Effect<void>;
};

export const makeGrpcLeasedSubscription = Effect.fn(
  "ViewServerRuntime.grpc.leased.subscription.make",
)(function* <Error>(input: GrpcLeasedSubscriptionInput<Error>) {
  const scope = yield* Scope.fork(input.parentScope, "sequential");
  const lock = yield* Semaphore.make(1);
  const storageKeys = new Set<string>();
  const activeClientLeases = new Set<ActiveClientLease>();
  let subscribers = 0;
  let acceptingSubscribers = true;
  let closing = false;
  let closeCompleted = false;
  const closeExit = yield* Deferred.make<Exit.Exit<void, never>>();

  const cleanupRowsExit = yield* Effect.cached(
    input.cleanupRows(storageKeys).pipe(
      Effect.tap(() =>
        Effect.sync(() => storageKeys.clear()).pipe(Effect.andThen(input.onRowsCleared)),
      ),
      Effect.exit,
    ),
  );

  const finalize = Effect.fn("ViewServerRuntime.grpc.leased.subscription.finalize")(function* () {
    const cleanupExit = yield* cleanupRowsExit;
    if (Exit.isFailure(cleanupExit)) {
      yield* input.onCleanupFailure(cleanupExit.cause);
      return;
    }
    yield* input.onClosed;
  });
  yield* Scope.addFinalizer(scope, finalize());

  const close = (yield* Effect.cached(
    lock
      .withPermit(
        Effect.gen(function* () {
          const shouldStop = acceptingSubscribers && subscribers > 0;
          acceptingSubscribers = false;
          closing = true;
          if (shouldStop) {
            yield* input.onStopping;
          }
          return Array.from(activeClientLeases, (lease) =>
            runAllFinalizers([lease.markClosed, lease.closeChild]),
          );
        }),
      )
      .pipe(
        Effect.flatMap((clientCloses) =>
          runAllFinalizers([...clientCloses, Scope.close(scope, Exit.void)]),
        ),
        Effect.onExit(() =>
          Effect.sync(() => {
            closeCompleted = true;
          }),
        ),
        Effect.exit,
        Effect.flatMap((exit) =>
          Deferred.succeed(closeExit, exit).pipe(
            Effect.andThen(Exit.isFailure(exit) ? Effect.failCause(exit.cause) : Effect.void),
          ),
        ),
      ),
  )).pipe(Effect.uninterruptible);
  const awaitClose = Deferred.await(closeExit).pipe(
    Effect.flatMap((exit) => (Exit.isFailure(exit) ? Effect.failCause(exit.cause) : Effect.void)),
  );
  yield* Scope.addFinalizer(
    input.parentScope,
    Effect.suspend(() => (closeCompleted ? Effect.void : close)),
  );

  const releaseSubscriber = Effect.fn(
    "ViewServerRuntime.grpc.leased.subscription.releaseSubscriber",
  )(function* (
    activeClientLease: ActiveClientLease,
    closeDisposition: Deferred.Deferred<ClientCloseDisposition>,
  ) {
    const disposition = yield* lock.withPermit(
      Effect.gen(function* () {
        activeClientLeases.delete(activeClientLease);
        subscribers -= 1;
        yield* input.onSubscriberRemoved;
        if (closing) {
          return "Join" as const;
        }
        if (subscribers > 0) {
          return "None" as const;
        }
        closing = true;
        acceptingSubscribers = false;
        yield* input.onStopping;
        return "Initiate" as const;
      }),
    );
    yield* Deferred.succeed(closeDisposition, disposition);
  });

  const acquire: Effect.Effect<Option.Option<GrpcLeasedSubscriptionLease>> = Effect.uninterruptible(
    lock.withPermit(
      Effect.gen(function* () {
        if (!acceptingSubscribers) {
          return Option.none<GrpcLeasedSubscriptionLease>();
        }
        const leaseScope = yield* Scope.fork(scope, "sequential");
        const terminal = yield* Deferred.make<SubscriptionTerminal>();
        const terminalRegistration = yield* makeSubscriptionTerminalRegistration(terminal);
        const closeDisposition = yield* Deferred.make<ClientCloseDisposition>();
        const closeChild = (yield* Effect.cached(Scope.close(leaseScope, Exit.void))).pipe(
          Effect.uninterruptible,
        );
        const activeClientLease: ActiveClientLease = {
          markClosed: Deferred.succeed(terminal, closedSubscriptionTerminal),
          closeChild,
          notifyUpstream: (upstream) =>
            Deferred.succeed(terminal, {
              _tag: "Upstream",
              message: upstream.message,
            }),
        };
        subscribers += 1;
        activeClientLeases.add(activeClientLease);
        yield* Scope.addFinalizer(
          leaseScope,
          releaseSubscriber(activeClientLease, closeDisposition),
        );
        yield* input.onSubscriberAdded;

        const closeLease = (yield* Effect.cached(
          runAllFinalizers([
            Deferred.succeed(terminal, closedSubscriptionTerminal),
            closeChild,
            Deferred.await(closeDisposition).pipe(
              Effect.flatMap((disposition) =>
                disposition === "None"
                  ? Effect.void
                  : disposition === "Initiate"
                    ? close
                    : awaitClose,
              ),
            ),
          ]),
        )).pipe(Effect.uninterruptible);
        const closeAfterTerminalClaim = runAllFinalizers([
          closeChild,
          Deferred.await(closeDisposition).pipe(
            Effect.flatMap((disposition) => (disposition === "Initiate" ? close : Effect.void)),
          ),
        ]);

        const attach = <Row extends object>({
          query,
          subscription,
        }: GrpcLeasedSubscriptionAttachInput<Row>): Effect.Effect<
          ViewServerLiveSubscription<Row>
        > =>
          Effect.gen(function* () {
            const resultKeys = input.identity.resultKeys<Row>(
              query,
              input.groupedKeyRetentionObserver,
            );
            yield* Scope.addFinalizer(
              leaseScope,
              subscription.close().pipe(ignoreLeasedSubscriptionCloseFailure),
            );
            yield* Scope.addFinalizer(
              leaseScope,
              Effect.sync(() => resultKeys.clear()),
            );

            const runtimeTerminal: RuntimeSubscriptionTerminal = {
              _tag: "Runtime",
            };
            const claimRuntimeTerminal = (nextTerminal: RuntimeSubscriptionTerminal) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(terminal, nextTerminal);
                return (yield* Deferred.await(terminal)) === nextTerminal;
              });
            const runtimeEvents = subscription.events.pipe(
              Stream.map((event) => externalizeLeasedEvent(resultKeys, event)),
              Stream.filterEffect((translated) => {
                if (isResultKeyTranslationFailure(translated)) {
                  return claimRuntimeTerminal(runtimeTerminal);
                }
                if (isTerminalStatusEvent(translated)) {
                  return Effect.succeed(false);
                }
                return Effect.succeed(true);
              }),
              Stream.takeUntil(isResultKeyTranslationFailure),
              Stream.map((translated) =>
                isResultKeyTranslationFailure(translated)
                  ? resultKeyEncodingErrorStatus(input.topic, translated.queryId, translated.error)
                  : translated,
              ),
            );
            const terminalStatusEvents = Stream.fromEffect(Deferred.await(terminal)).pipe(
              Stream.flatMap((nextTerminal) => {
                if (nextTerminal._tag === "Engine") {
                  return Stream.succeed(nextTerminal.status);
                }
                if (nextTerminal._tag === "Upstream") {
                  return Stream.fromEffect(Deferred.await(terminalRegistration.queryId)).pipe(
                    Stream.map(
                      (queryId): StatusEvent => ({
                        type: "status",
                        topic: input.topic,
                        queryId,
                        status: "error",
                        code: "RuntimeUnavailable",
                        message: nextTerminal.message,
                      }),
                    ),
                  );
                }
                return Stream.empty;
              }),
            );
            const closeAfterTerminal = Deferred.await(terminal).pipe(
              Effect.flatMap((nextTerminal) => {
                if (nextTerminal._tag === "Engine") {
                  return Deferred.await(nextTerminal.ready).pipe(
                    Effect.andThen(closeAfterTerminalClaim),
                  );
                }
                return nextTerminal._tag === "Runtime" || nextTerminal._tag === "Upstream"
                  ? closeAfterTerminalClaim
                  : Effect.void;
              }),
            );
            yield* closeAfterTerminal.pipe(Effect.forkIn(leaseScope, { startImmediately: true }));
            const closeEvents = Deferred.isDone(terminal).pipe(
              Effect.flatMap((terminalDone) =>
                terminalDone
                  ? Deferred.await(terminal).pipe(
                      Effect.flatMap((claimedTerminal) =>
                        claimedTerminal._tag === "Closed"
                          ? runAllFinalizers([activeClientLease.markClosed, closeChild])
                          : closeLease,
                      ),
                    )
                  : closeLease,
              ),
            );
            return {
              events: runtimeEvents.pipe(
                Stream.concat(terminalStatusEvents),
                Stream.takeUntil(isTerminalStatusEvent),
                Stream.ensuring(closeEvents),
              ),
              close: () => closeLease,
            } satisfies ViewServerLiveSubscription<Row>;
          });

        return Option.some({
          terminalObserver: terminalRegistration.observer,
          attach,
          close: closeLease,
        });
      }),
    ),
  );

  const start = Effect.fn("ViewServerRuntime.grpc.leased.subscription.start")(function* (
    startInput: GrpcLeasedSubscriptionStart<Error>,
  ) {
    const release = (yield* Effect.cached(startInput.release)).pipe(Effect.uninterruptible);
    yield* Scope.addFinalizer(scope, release);
    const runUpstream = yield* startInput.acquire;
    const terminate = Effect.fn("ViewServerRuntime.grpc.leased.subscription.terminate")(function* (
      terminal: GrpcLeasedUpstreamTerminal,
    ) {
      const activeClients = yield* lock.withPermit(
        Effect.sync(() => {
          acceptingSubscribers = false;
          closing = true;
          return Array.from(activeClientLeases);
        }).pipe(Effect.when(Effect.sync(() => acceptingSubscribers))),
      );
      yield* Effect.forEach(
        Option.toArray(activeClients),
        (clients) =>
          runAllFinalizers([
            release,
            input.onUpstreamTerminal(terminal),
            Effect.gen(function* () {
              const cleanupExit = yield* cleanupRowsExit;
              if (Exit.isFailure(cleanupExit)) {
                yield* input.onCleanupFailure(cleanupExit.cause);
              }
            }),
            ...clients.map((client) => client.notifyUpstream(terminal)),
            close,
          ]),
        { discard: true },
      );
    });
    yield* runUpstream.pipe(
      Effect.flatMap(terminate),
      Effect.forkIn(scope, { startImmediately: true }),
    );
  });

  const internalizeRowKey = <Row extends object>(
    row: Row,
  ): Result.Result<GrpcLeasedInternalRowKey, GrpcLeasedIdentityError> => {
    const internalKey = input.identity.internalizeRowKey(row);
    if (Result.isSuccess(internalKey)) {
      storageKeys.add(internalKey.success.storageKey);
    }
    return internalKey;
  };

  return {
    feedKey: input.identity.feedKey,
    materializeRoute: input.identity.materializeRoute,
    validateRowRoute: input.identity.validateRowRoute,
    internalizeRowKey,
    retainedRowCount: () => storageKeys.size,
    acquire,
    start,
    close,
  };
});
