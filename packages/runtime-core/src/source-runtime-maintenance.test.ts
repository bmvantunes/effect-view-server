import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import {
  SourceAdapter,
  type SourceApplicationExit,
  type SourceApplicationTransition,
  type SourceDelivery,
  type SourceExecutionFailure,
  type SourceMaintenanceResult,
  type SourceMutation,
  type SourceSettlement,
  type SourceStatus,
  type SourceToolkit,
} from "@effect-view-server/source-adapter";
import {
  SourceAdapterServer,
  type SourceApplicationStateModule,
} from "@effect-view-server/source-adapter/server";
import {
  makeSourceApplicationTransition,
  makeSourceMaintenanceOperation,
} from "@effect-view-server/source-adapter/internal";
import {
  Chunk,
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Result,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import {
  makeRuntimeCoreSourceManager,
  type RuntimeCoreSourceManagerConstructionOptions,
} from "./source-runtime";

const Row = Schema.Struct({
  id: ViewServerId,
  value: Schema.String,
});

const Failure = Schema.TaggedStruct("MaintenanceFixtureFailure", {
  message: Schema.String,
});

const Adapter = SourceAdapter.make({
  identity: { name: "maintenance-fixture" },
  failure: Failure,
  materialized: {
    applicationState: "required",
    metrics: Schema.Struct({
      current: Schema.Boolean,
      successes: Schema.Number,
      failures: Schema.Number,
      stale: Schema.Number,
    }),
    rejectionLocation: Schema.Struct({
      offset: Schema.BigInt,
    }),
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
    }>(),
  },
  leased: undefined,
});

type State = {
  readonly current: boolean;
  readonly successes: number;
  readonly failures: number;
  readonly stale: number;
};

type Command =
  | {
      readonly _tag: "SetCurrent";
      readonly current: boolean;
    }
  | {
      readonly _tag: "Success";
    }
  | {
      readonly _tag: "Failure";
    }
  | {
      readonly _tag: "Stale";
    }
  | {
      readonly _tag: "Throw";
    };

const applicationStateDefect = new Error("injected application state failure");
const currentStateDefect = new Error("injected current-state failure");
const deleteDefect = new Error("injected Delete defect");
const sweepDefect = new Error("injected sweep defect");

type Plan = {
  readonly id: string;
  readonly workId: string;
  readonly current?: "state" | "stale" | "throw";
  readonly success?: "success" | "throw";
  readonly failure?: "failure" | "throw";
  readonly stale?: "stale" | "throw";
};

type DeleteMode = "success" | "failure" | "defect" | "gated-success" | "interrupt";
type SweepMode =
  | "normal"
  | "defect"
  | "interrupt"
  | "invalid-operation"
  | "foreign-operation"
  | "gated-operation";

type Emission =
  | {
      readonly _tag: "Delivery";
      readonly command: Command;
      readonly cancelledMaintenanceWorkIds: ReadonlyArray<string> | undefined;
      readonly mutationCount: 1 | 2;
      readonly settlement: "success" | "failure" | "throw" | "blocked";
      readonly transitionTopic: "rows" | "other";
      readonly transitionLifetime: "bound" | "foreign";
      readonly settled: Deferred.Deferred<SourceApplicationExit> | undefined;
    }
  | {
      readonly _tag: "Rejection";
      readonly offset: bigint;
      readonly rejectedAtNanos: bigint;
      readonly message: string;
      readonly settled: Deferred.Deferred<SourceApplicationExit>;
    }
  | {
      readonly _tag: "Failure";
    };

type MaintenanceControl = {
  readonly lifetimeScope: import("effect").Scope.Scope;
  readonly module: SourceApplicationStateModule<
    string,
    State,
    Command,
    State,
    ReadonlyArray<SourceMaintenanceResult>
  >;
  readonly sweep: () => Effect.Effect<ReadonlyArray<SourceMaintenanceResult>>;
  readonly deliver: (
    command: Command,
    cancelledMaintenanceWorkIds?: ReadonlyArray<string>,
    mutationCount?: 1 | 2,
    transitionTopic?: "rows" | "other",
  ) => Effect.Effect<SourceApplicationExit>;
  readonly emit: (input: {
    readonly command: Command;
    readonly settlement: "success" | "failure" | "throw" | "blocked";
    readonly transitionTopic?: "rows" | "other";
    readonly transitionLifetime?: "bound" | "foreign";
  }) => Effect.Effect<void>;
  readonly reject: (input?: {
    readonly offset?: bigint;
    readonly rejectedAtNanos?: bigint;
    readonly message?: string;
  }) => Effect.Effect<SourceApplicationExit>;
  readonly fail: Effect.Effect<void>;
};

const initialState = (): State =>
  Object.freeze({
    current: true,
    successes: 0,
    failures: 0,
    stale: 0,
  });

const reduce = (state: State, command: Command): State => {
  switch (command._tag) {
    case "SetCurrent": {
      return Object.freeze({
        ...state,
        current: command.current,
      });
    }
    case "Success": {
      return Object.freeze({
        ...state,
        successes: state.successes + 1,
      });
    }
    case "Failure": {
      return Object.freeze({
        ...state,
        failures: state.failures + 1,
      });
    }
    case "Stale": {
      return Object.freeze({
        ...state,
        stale: state.stale + 1,
      });
    }
    case "Throw": {
      throw applicationStateDefect;
    }
  }
};

const mutationFailure = (): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  topic: "rows",
  message: "Injected maintenance Delete failure.",
});

const settlementFailure = (): typeof Failure.Type => ({
  _tag: "MaintenanceFixtureFailure",
  message: "injected settlement Effect failure",
});

const invokeInvalidTransitionDelivery = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, never, Topic>,
  mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>,
  settlement: SourceSettlement<AdapterFailure>,
  transition: SourceApplicationTransition,
): Effect.Effect<SourceDelivery<Row, AdapterFailure>, SourceExecutionFailure<AdapterFailure>> =>
  Effect.suspend<
    SourceDelivery<Row, AdapterFailure>,
    SourceExecutionFailure<AdapterFailure>,
    never
  >(() => Reflect.apply(toolkit.delivery, toolkit, [mutations, settlement, transition]));

const invokeForeignTransitionDelivery = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, never, Topic>,
  mutation: SourceMutation<Row>,
  settlement: SourceSettlement<AdapterFailure>,
  transition: SourceApplicationTransition,
): Effect.Effect<SourceDelivery<Row, AdapterFailure>, SourceExecutionFailure<AdapterFailure>> =>
  Effect.suspend<
    SourceDelivery<Row, AdapterFailure>,
    SourceExecutionFailure<AdapterFailure>,
    never
  >(() => Reflect.apply(toolkit.delivery, toolkit, [mutation, settlement, transition]));

const invokeMaintenance = <Topic extends string>(
  execute: (
    operation: import("@effect-view-server/source-adapter").SourceMaintenanceOperation<Topic>,
  ) => Effect.Effect<SourceMaintenanceResult>,
  operation: unknown,
): Effect.Effect<SourceMaintenanceResult> => Reflect.apply(execute, undefined, [operation]);

const fatalDefect = (exit: Exit.Exit<never, ViewServerRuntimeError>): unknown => {
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Runtime Core fatal signal to fail.");
  }
  return Result.getOrThrow(Cause.findDefect(exit.cause));
};

const fatalFailure = (exit: Exit.Exit<never, ViewServerRuntimeError>): ViewServerRuntimeError => {
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Runtime Core fatal signal to fail.");
  }
  return Option.getOrThrow(Cause.findErrorOption(exit.cause));
};

const maintenanceFailureCause = (
  results: ReadonlyArray<SourceMaintenanceResult>,
): Cause.Cause<unknown> => {
  const result = results[0];
  if (result?._tag !== "Applied" || Exit.isSuccess(result.exit)) {
    throw new Error("Expected one defective maintenance application Exit.");
  }
  return result.exit.cause;
};

const makeHarness = Effect.fn("RuntimeCoreTest.makeMaintenanceHarness")(function* (options?: {
  readonly blockAcquisition?: boolean;
  readonly blockReacquisition?: boolean;
  readonly blockFinalizer?: boolean;
  readonly afterMutationApplication?: Effect.Effect<void>;
  readonly invalidApplicationLifetimeLookup?: boolean;
  readonly retryDelay?: "10 seconds";
  readonly settlementHandoff?: RuntimeCoreSourceManagerConstructionOptions["settlementHandoff"];
}) {
  let plans: ReadonlyArray<Plan> = [];
  let deleteMode: DeleteMode = "success";
  let sweepMode: SweepMode = "normal";
  let sweepInvocations = 0;
  let acquisitions = 0;
  let finalizations = 0;
  let settlementCallbacks = 0;
  let blockAfterAttemptCancellationRequested = false;
  const attemptedDeletes: Array<string> = [];
  const successfulDeletes: Array<string> = [];
  const bindings: Array<{
    readonly topic: string;
    readonly definition: unknown;
    readonly target: unknown;
  }> = [];
  const control = yield* Deferred.make<MaintenanceControl>();
  const deleteStarted = yield* Deferred.make<void>();
  const releaseDelete = yield* Deferred.make<void>();
  const shutdownCancellationRequested = yield* Deferred.make<void>();
  const releaseShutdownCancellation = yield* Deferred.make<void>();
  const settlementStarted = yield* Deferred.make<void>();
  const settlementFinalized = yield* Deferred.make<void>();
  const releaseAcquisition = yield* Deferred.make<void>();
  const releaseFinalizer = yield* Deferred.make<void>();
  const sweepEnumerated = yield* Deferred.make<void>();
  const releaseSweepOperation = yield* Deferred.make<void>();
  const sweepResults = yield* Queue.unbounded<ReadonlyArray<SourceMaintenanceResult>>();
  const cancelledByCommand = new WeakMap<object, ReadonlyArray<string>>();
  const applicationState = SourceAdapterServer.applicationState<
    State,
    Command,
    State,
    ReadonlyArray<SourceMaintenanceResult>
  >({
    sweepIntervalNanos: 1_000_000_000n,
    initialState: (binding) => {
      bindings.push({
        topic: binding.topic,
        definition: binding.definition,
        target: binding.target,
      });
      return initialState();
    },
    reduce,
    cancelledMaintenanceWorkIds: (_state, command) => cancelledByCommand.get(command) ?? [],
    metrics: (state) => state,
    runDueSweep: (input) => {
      sweepInvocations += 1;
      switch (sweepMode) {
        case "defect": {
          return Effect.die(sweepDefect);
        }
        case "interrupt": {
          return Effect.interrupt;
        }
        case "invalid-operation": {
          return Effect.gen(function* () {
            const result = yield* invokeMaintenance(input.execute, {});
            const results = [result];
            yield* Queue.offer(sweepResults, results);
            return results;
          });
        }
        case "foreign-operation": {
          return Effect.gen(function* () {
            const result = yield* invokeMaintenance(
              input.execute,
              makeSourceMaintenanceOperation({
                topic: "rows",
                id: "foreign",
                workId: "foreign:1",
                lifetimeIdentity: Object.freeze({}),
                isCurrent: () => true,
                onSuccess: () => undefined,
                onFailure: () => undefined,
                onStale: () => undefined,
              }),
            );
            const results = [result];
            yield* Queue.offer(sweepResults, results);
            return results;
          });
        }
        case "gated-operation": {
          return Effect.gen(function* () {
            yield* Deferred.succeed(sweepEnumerated, undefined);
            yield* Deferred.await(releaseSweepOperation);
            const result = yield* input.execute(
              input.operation({
                id: "row-expiry",
                workId: "gated:1",
                isCurrent: () => true,
                onSuccess: { _tag: "Success" },
                onFailure: () => ({ _tag: "Failure" }),
                onStale: { _tag: "Stale" },
              }),
            );
            const results = [result];
            yield* Queue.offer(sweepResults, results);
            return results;
          });
        }
        case "normal": {
          return Effect.gen(function* () {
            const scheduled = plans;
            plans = [];
            const results = yield* Effect.forEach(scheduled, (plan) =>
              input.execute(
                input.operation({
                  id: plan.id,
                  workId: plan.workId,
                  isCurrent:
                    plan.current === "throw"
                      ? () => {
                          throw currentStateDefect;
                        }
                      : plan.current === "stale"
                        ? () => false
                        : (state) => state.current,
                  onSuccess: {
                    _tag: plan.success === "throw" ? "Throw" : "Success",
                  },
                  onFailure: () => ({
                    _tag: plan.failure === "throw" ? "Throw" : "Failure",
                  }),
                  onStale: {
                    _tag: plan.stale === "throw" ? "Throw" : "Stale",
                  },
                }),
              ),
            );
            yield* Queue.offer(sweepResults, results);
            return results;
          });
        }
      }
    },
  });
  const layer = SourceAdapterServer.make(Adapter, {
    materialized: {
      applicationState,
      initialLaneIds: () => ["maintenance"],
      acquire: (input) =>
        Effect.gen(function* () {
          acquisitions += 1;
          const events = yield* Queue.unbounded<Emission>();
          const module = applicationState.forLifetime(input.lifetimeScope, input.toolkit.topic);
          const offerDelivery = (emission: Omit<Extract<Emission, { _tag: "Delivery" }>, "_tag">) =>
            Queue.offer(events, {
              _tag: "Delivery",
              ...emission,
            });
          const deliver: MaintenanceControl["deliver"] = (
            command,
            cancelledMaintenanceWorkIds,
            mutationCount = 1,
            transitionTopic = "rows",
          ) =>
            Effect.gen(function* () {
              if (cancelledMaintenanceWorkIds !== undefined) {
                cancelledByCommand.set(command, cancelledMaintenanceWorkIds);
              }
              if (mutationCount === 2) {
                const mutation = yield* input.toolkit.delete("row-1").pipe(Effect.orDie);
                const transition = makeSourceApplicationTransition(
                  "rows",
                  () => undefined,
                  [],
                  Object.freeze({}),
                );
                const invalid = yield* invokeInvalidTransitionDelivery(
                  input.toolkit,
                  Chunk.make(mutation, mutation),
                  () => Effect.void,
                  transition,
                ).pipe(Effect.exit);
                return Exit.isFailure(invalid)
                  ? Exit.fail({
                      _tag: "InvalidSourceDelivery",
                      message:
                        "Source Application Transition requires exactly one nominal Source Mutation.",
                    })
                  : Exit.void;
              }
              const settled = yield* Deferred.make<SourceApplicationExit>();
              yield* offerDelivery({
                command,
                cancelledMaintenanceWorkIds,
                mutationCount,
                settlement: "success",
                transitionTopic,
                transitionLifetime: "bound",
                settled,
              });
              return yield* Deferred.await(settled);
            });
          const emit: MaintenanceControl["emit"] = (emission) =>
            offerDelivery({
              command: emission.command,
              cancelledMaintenanceWorkIds: undefined,
              mutationCount: 1,
              settlement: emission.settlement,
              transitionTopic: emission.transitionTopic ?? "rows",
              transitionLifetime: emission.transitionLifetime ?? "bound",
              settled: undefined,
            });
          const reject: MaintenanceControl["reject"] = (rejection) =>
            Effect.gen(function* () {
              const settled = yield* Deferred.make<SourceApplicationExit>();
              yield* Queue.offer(events, {
                _tag: "Rejection",
                offset: rejection?.offset ?? 1n,
                rejectedAtNanos: rejection?.rejectedAtNanos ?? 1_000_000n,
                message: rejection?.message ?? "settled rejection",
                settled,
              });
              return yield* Deferred.await(settled);
            });
          yield* Deferred.succeed(control, {
            lifetimeScope: input.lifetimeScope,
            module,
            sweep: () =>
              Effect.gen(function* () {
                const result = yield* Queue.take(sweepResults).pipe(Effect.forkChild);
                yield* TestClock.adjust("1 second");
                return yield* Fiber.join(result);
              }),
            deliver,
            emit,
            reject,
            fail: Queue.offer(events, { _tag: "Failure" }),
          });
          if (options?.invalidApplicationLifetimeLookup === true && acquisitions > 1) {
            applicationState.forLifetime(yield* Scope.make(), input.toolkit.topic);
          }
          if (
            options?.blockAcquisition === true ||
            (options?.blockReacquisition === true && acquisitions > 1)
          ) {
            yield* Deferred.await(releaseAcquisition);
          }
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              finalizations += 1;
            }),
          );
          if (options?.blockFinalizer === true) {
            yield* Effect.addFinalizer(() => Deferred.await(releaseFinalizer));
          }
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "maintenance",
              events: Stream.fromQueue(events).pipe(
                Stream.mapEffect((emission) =>
                  Effect.gen(function* () {
                    if (emission._tag === "Failure") {
                      return yield* Effect.fail<SourceExecutionFailure<typeof Failure.Type>>({
                        _tag: "AdapterFailure",
                        failure: {
                          _tag: "MaintenanceFixtureFailure",
                          message: "injected active attempt failure",
                        },
                      });
                    }
                    if (emission._tag === "Rejection") {
                      return yield* input.toolkit.reject({
                        failure: {
                          _tag: "AdapterFailure",
                          failure: {
                            _tag: "MaintenanceFixtureFailure",
                            message: emission.message,
                          },
                        },
                        location: { offset: emission.offset },
                        rejectedAtNanos: emission.rejectedAtNanos,
                        settlement: (exit) =>
                          Deferred.succeed(emission.settled, exit).pipe(Effect.asVoid),
                      });
                    }
                    const settle = (exit: SourceApplicationExit) => {
                      settlementCallbacks += 1;
                      if (emission.settlement === "throw") {
                        throw new Error("injected settlement callback failure");
                      }
                      const completed =
                        emission.settled === undefined
                          ? Effect.void
                          : Deferred.succeed(emission.settled, exit).pipe(Effect.asVoid);
                      if (emission.settlement === "blocked") {
                        return Deferred.succeed(settlementStarted, undefined).pipe(
                          Effect.andThen(Effect.never),
                          Effect.ensuring(
                            Deferred.succeed(settlementFinalized, undefined).pipe(Effect.asVoid),
                          ),
                        );
                      }
                      return emission.settlement === "failure"
                        ? completed.pipe(Effect.andThen(Effect.fail(settlementFailure())))
                        : completed;
                    };
                    const mutation = yield* input.toolkit.delete("row-1");
                    if (
                      emission.transitionTopic !== "rows" ||
                      emission.transitionLifetime === "foreign"
                    ) {
                      return yield* invokeForeignTransitionDelivery(
                        input.toolkit,
                        mutation,
                        settle,
                        makeSourceApplicationTransition(
                          emission.transitionTopic,
                          () => {
                            throw new Error("foreign transition must never execute");
                          },
                          emission.cancelledMaintenanceWorkIds ?? [],
                          Object.freeze({}),
                        ),
                      );
                    }
                    const prepared = yield* module.prepare(emission.command);
                    if (emission.mutationCount === 2) {
                      return yield* invokeInvalidTransitionDelivery(
                        input.toolkit,
                        Chunk.make(mutation, mutation),
                        settle,
                        prepared.transition,
                      );
                    }
                    return yield* input.toolkit.delivery(mutation, settle, prepared.transition);
                  }),
                ),
              ),
            }),
          ]);
        }),
      metrics: (input) =>
        Effect.sync(() => applicationState.forLifetime(input.lifetimeScope, input.topic).metrics()),
      retry:
        options?.retryDelay === "10 seconds"
          ? Schedule.spaced("10 seconds").pipe(Schedule.upTo({ times: 1 }))
          : Schedule.recurs(0),
    },
  });
  const config = defineViewServerConfig({
    topics: {
      rows: {
        schema: Row,
        source: Adapter.materializedSource({
          label: "stateful",
        }),
      },
    },
  });
  const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
    publish: () => Effect.void,
    publishMany: () => Effect.void,
    patch: () => Effect.void,
    delete: (_topic, id) =>
      Effect.suspend(() => {
        attemptedDeletes.push(id);
        switch (deleteMode) {
          case "success": {
            successfulDeletes.push(id);
            return Effect.void;
          }
          case "failure": {
            return Effect.fail(mutationFailure());
          }
          case "defect": {
            return Effect.die(deleteDefect);
          }
          case "gated-success": {
            return Deferred.succeed(deleteStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseDelete)),
              Effect.andThen(
                Effect.sync(() => {
                  successfulDeletes.push(id);
                }),
              ),
            );
          }
          case "interrupt": {
            return Effect.interrupt;
          }
        }
      }),
    reset: () => Effect.void,
    deleteStorageKey: () => Effect.void,
    patchDecodedFields: () => Effect.void,
    publishManyDecodedRows: () => Effect.void,
    publishManyDecodedRowsWithStorageKeys: () => Effect.void,
    publishManyWithStorageKeys: () => Effect.void,
  };
  const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
    ...(options?.afterMutationApplication === undefined
      ? {}
      : { afterMutationApplication: options.afterMutationApplication }),
    afterAttemptCancellationRequested: Effect.suspend(() =>
      blockAfterAttemptCancellationRequested
        ? Deferred.succeed(shutdownCancellationRequested, undefined).pipe(
            Effect.andThen(Deferred.await(releaseShutdownCancellation)),
          )
        : Effect.void,
    ),
    ...(options?.settlementHandoff === undefined
      ? {}
      : { settlementHandoff: options.settlementHandoff }),
  }).pipe(Effect.provide(layer));
  const activeControl = yield* Deferred.await(control);
  const diagnostics = yield* manager.subscribeSourceHealth({ topic: "rows" });
  const aggregateHealth = () =>
    manager.overlayHealth({
      status: "ready",
      version: 1,
      uptimeMs: 0,
      engine: {
        topics: {
          rows: {
            status: "ready",
            rowCount: 0,
            liveRowCount: 0,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
          },
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    });
  const health = () =>
    diagnostics.events.pipe(
      Stream.filter(
        (snapshot) => snapshot.status._tag === "Ready" || snapshot.status._tag === "Degraded",
      ),
      Stream.take(1),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
  const awaitOverlayStatus = (
    tag: SourceStatus<unknown, unknown>["_tag"],
  ): Effect.Effect<SourceStatus<unknown, unknown>> =>
    Effect.suspend(() => {
      const current = aggregateHealth().sources.rows?.status;
      return current?._tag === tag
        ? Effect.succeed(current)
        : Effect.yieldNow.pipe(Effect.andThen(awaitOverlayStatus(tag)));
    });
  if (options?.blockAcquisition !== true) {
    yield* health();
  }
  return {
    applicationState,
    bindings,
    control: activeControl,
    manager,
    health,
    awaitStatus: (tag: string) =>
      diagnostics.events.pipe(
        Stream.filter((snapshot) => snapshot.status._tag === tag),
        Stream.take(1),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      ),
    awaitOverlayStatus,
    aggregateHealth,
    close: diagnostics.close().pipe(Effect.andThen(manager.close)),
    attemptedDeletes,
    successfulDeletes,
    acquisitions: () => acquisitions,
    finalizations: () => finalizations,
    settlementCallbacks: () => settlementCallbacks,
    sweepInvocations: () => sweepInvocations,
    plan: (next: ReadonlyArray<Plan>) => {
      plans = next;
    },
    setDeleteMode: (next: DeleteMode) => {
      deleteMode = next;
    },
    setSweepMode: (next: SweepMode) => {
      sweepMode = next;
    },
    deleteStarted,
    releaseDelete: Deferred.succeed(releaseDelete, undefined).pipe(Effect.asVoid),
    shutdownCancellationRequested,
    releaseShutdownCancellation: Deferred.succeed(releaseShutdownCancellation, undefined).pipe(
      Effect.asVoid,
    ),
    blockAfterAttemptCancellationRequested: () => {
      blockAfterAttemptCancellationRequested = true;
    },
    settlementStarted,
    settlementFinalized,
    sweepEnumerated,
    releaseSweepOperation: Deferred.succeed(releaseSweepOperation, undefined).pipe(Effect.asVoid),
    releaseAcquisition: Deferred.succeed(releaseAcquisition, undefined).pipe(Effect.asVoid),
    releaseFinalizer: Deferred.succeed(releaseFinalizer, undefined).pipe(Effect.asVoid),
  } as const;
});

describe("Runtime Core Source Application State", () => {
  it.effect(
    "keeps the exact Source degradation ledger globally visible, stable, and retryable",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const maintenanceOnly = (degradedAtNanos: bigint) =>
          ({
            _tag: "Degraded",
            attempt: 1n,
            degradedAtNanos,
            reasons: [{ _tag: "AdapterMaintenanceFailure" }],
          }) as const;
        const rejection = (
          degradedAtNanos: bigint,
          offset: bigint,
          rejectedAtNanos: bigint,
          message: string,
          includeMaintenance: boolean,
        ) =>
          ({
            _tag: "Degraded",
            attempt: 1n,
            degradedAtNanos,
            reasons: [
              {
                _tag: "SourceItemRejection",
                latestRejection: {
                  failure: {
                    _tag: "AdapterFailure",
                    failure: {
                      _tag: "MaintenanceFixtureFailure",
                      message,
                    },
                  },
                  location: { offset },
                  rejectedAtNanos,
                },
              },
              ...(includeMaintenance ? ([{ _tag: "AdapterMaintenanceFailure" }] as const) : []),
            ],
          }) as const;

        expect(harness.bindings).toStrictEqual([
          {
            topic: "rows",
            definition: { label: "stateful" },
            target: { _tag: "Materialized" },
          },
        ]);
        expect(harness.acquisitions()).toBe(1);
        expect(
          harness.applicationState.forLifetime(harness.control.lifetimeScope, "rows").metrics(),
        ).toStrictEqual(harness.control.module.metrics());

        harness.setDeleteMode("failure");
        harness.plan([{ id: "row-1", workId: "expiry:a" }]);
        const firstFailure = yield* harness.control.sweep();
        expect(firstFailure.map((result) => result._tag)).toStrictEqual(["Applied"]);
        expect(
          firstFailure.flatMap((result) =>
            result._tag === "Applied" && Exit.isFailure(result.exit)
              ? [Cause.findErrorOption(result.exit.cause)]
              : [],
          ),
        ).toStrictEqual([
          Option.some({
            _tag: "InvalidSourceDelivery",
            message: "Injected maintenance Delete failure.",
          }),
        ]);
        expect(harness.control.module.metrics().failures).toBe(1);
        expect((yield* harness.health()).status).toStrictEqual(maintenanceOnly(1_000_000_000n));

        harness.plan([{ id: "row-1", workId: "expiry:b" }]);
        yield* harness.control.sweep();
        expect((yield* harness.health()).status).toStrictEqual(maintenanceOnly(1_000_000_000n));

        harness.plan([{ id: "row-1", workId: "expiry:b" }]);
        yield* harness.control.sweep();
        expect((yield* harness.health()).status).toStrictEqual(maintenanceOnly(1_000_000_000n));

        const aggregateFailure = harness.aggregateHealth();
        expect({
          aggregate: aggregateFailure.status,
          topic: aggregateFailure.engine.topics.rows.status,
          source: aggregateFailure.sources.rows?.status,
        }).toStrictEqual({
          aggregate: "degraded",
          topic: "degraded",
          source: {
            _tag: "Degraded",
            attempt: 1n,
            degradedAtNanos: 1_000_000_000n,
            reasons: [{ _tag: "AdapterMaintenanceFailure" }],
          },
        });

        harness.setDeleteMode("success");
        expect(
          Exit.isSuccess(
            yield* harness.control.deliver({ _tag: "SetCurrent", current: true }, ["expiry:a"]),
          ),
        ).toBe(true);
        expect((yield* harness.health()).status).toStrictEqual(maintenanceOnly(1_000_000_000n));
        expect(
          Exit.isSuccess(
            yield* harness.control.deliver({ _tag: "SetCurrent", current: true }, ["expiry:b"]),
          ),
        ).toBe(true);
        expect((yield* harness.health()).status).toStrictEqual({
          _tag: "Ready",
          attempt: 1n,
          readyAtNanos: 3_000_000_000n,
        });

        harness.setDeleteMode("failure");
        harness.plan([{ id: "row-1", workId: "expiry:c" }]);
        yield* harness.control.sweep();
        expect((yield* harness.health()).status).toStrictEqual(maintenanceOnly(4_000_000_000n));

        const firstRejection = yield* harness.control.reject({
          offset: 10n,
          rejectedAtNanos: 10_000_000n,
          message: "first settled rejection",
        });
        expect(Exit.isSuccess(firstRejection)).toBe(true);
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 10n, 10_000_000n, "first settled rejection", true),
        );
        const secondRejection = yield* harness.control.reject({
          offset: 11n,
          rejectedAtNanos: 11_000_000n,
          message: "replacement settled rejection",
        });
        expect(Exit.isSuccess(secondRejection)).toBe(true);
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 11n, 11_000_000n, "replacement settled rejection", true),
        );
        harness.plan([{ id: "row-1", workId: "expiry:c" }]);
        yield* harness.control.sweep();
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 11n, 11_000_000n, "replacement settled rejection", true),
        );

        harness.setDeleteMode("interrupt");
        harness.plan([{ id: "row-1", workId: "expiry:c" }]);
        const interruptedRetry = yield* harness.control.sweep();
        expect(interruptedRetry.map((result) => result._tag)).toStrictEqual(["Applied"]);
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 11n, 11_000_000n, "replacement settled rejection", true),
        );

        harness.setDeleteMode("success");
        const transitionExit = yield* harness.control.deliver(
          { _tag: "SetCurrent", current: true },
          ["expiry:c"],
        );
        expect(Exit.isSuccess(transitionExit)).toBe(true);
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 11n, 11_000_000n, "replacement settled rejection", false),
        );
        harness.plan([{ id: "row-1", workId: "expiry:d" }]);
        const successfulMaintenance = yield* harness.control.sweep();
        expect(successfulMaintenance.map((result) => result._tag)).toStrictEqual(["Applied"]);
        expect((yield* harness.health()).status).toStrictEqual(
          rejection(4_000_000_000n, 11n, 11_000_000n, "replacement settled rejection", false),
        );

        const rejectionOnlyAggregate = harness.aggregateHealth();
        expect({
          aggregate: rejectionOnlyAggregate.status,
          topic: rejectionOnlyAggregate.engine.topics.rows.status,
          source: rejectionOnlyAggregate.sources.rows?.status,
        }).toStrictEqual({
          aggregate: "degraded",
          topic: "degraded",
          source: rejection(
            4_000_000_000n,
            11n,
            11_000_000n,
            "replacement settled rejection",
            false,
          ),
        });
        expect(harness.control.module.metrics()).toStrictEqual({
          current: true,
          successes: 1,
          failures: 5,
          stale: 0,
        });
        expect(harness.attemptedDeletes).toStrictEqual([
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
          "row-1",
        ]);
        expect(harness.successfulDeletes).toStrictEqual(["row-1", "row-1", "row-1", "row-1"]);

        yield* harness.close;
        expect(() =>
          harness.applicationState.forLifetime(harness.control.lifetimeScope, "rows"),
        ).toThrow("not bound to this logical lifetime");
      }),
  );

  it.effect("arbitrates stale work and completes admitted expiration during shutdown", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      harness.plan([{ id: "row-1", workId: "stale:1", current: "stale" }]);
      expect((yield* harness.control.sweep()).map((result) => result._tag)).toStrictEqual([
        "Stale",
      ]);
      expect(harness.control.module.metrics().stale).toBe(1);
      expect(harness.attemptedDeletes).toStrictEqual([]);

      harness.setDeleteMode("gated-success");
      harness.plan([{ id: "row-1", workId: "blocked:1" }]);
      const cadence = yield* TestClock.adjust("1 second").pipe(Effect.forkChild);
      yield* Deferred.await(harness.deleteStarted);
      expect((yield* harness.health()).status._tag).toBe("Ready");
      expect(harness.control.module.metrics().failures).toBe(0);
      const close = yield* harness.manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect(close.pollUnsafe()).toBeUndefined();

      yield* harness.releaseDelete;
      yield* Fiber.join(close);
      yield* Fiber.interrupt(cadence).pipe(Effect.asVoid);

      expect({
        state: harness.control.module.metrics(),
        successfulDeletes: harness.successfulDeletes,
      }).toStrictEqual({
        state: {
          current: true,
          successes: 1,
          failures: 0,
          stale: 1,
        },
        successfulDeletes: ["row-1"],
      });
      yield* harness.close;
    }),
  );

  it.effect("rejects invalid transitions without mutating application state", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const exit = yield* harness.control.deliver(
        { _tag: "SetCurrent", current: false },
        undefined,
        2,
      );

      expect(
        Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none(),
      ).toStrictEqual(
        Option.some({
          _tag: "InvalidSourceDelivery",
          message: "Source Application Transition requires exactly one nominal Source Mutation.",
        }),
      );
      expect(harness.control.module.metrics().current).toBe(true);
      expect(harness.successfulDeletes).toStrictEqual([]);
      yield* harness.close;

      const foreignHarness = yield* makeHarness();
      yield* foreignHarness.control.emit({
        command: { _tag: "SetCurrent", current: false },
        settlement: "success",
        transitionTopic: "other",
      });
      const foreignFatal = yield* Effect.exit(foreignHarness.manager.fatal);
      expect(fatalFailure(foreignFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source application transition failed and stopped the complete runtime.",
      });
      expect(fatalDefect(foreignFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source Application Transition is bound to a different Topic or logical lifetime.",
      });
      expect(foreignHarness.control.module.metrics().current).toBe(true);
      expect(foreignHarness.successfulDeletes).toStrictEqual([]);
      yield* foreignHarness.close;

      const foreignLifetimeHarness = yield* makeHarness();
      yield* foreignLifetimeHarness.control.emit({
        command: { _tag: "SetCurrent", current: false },
        settlement: "success",
        transitionLifetime: "foreign",
      });
      const foreignLifetimeFatal = yield* Effect.exit(foreignLifetimeHarness.manager.fatal);
      expect(fatalFailure(foreignLifetimeFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source application transition failed and stopped the complete runtime.",
      });
      expect(fatalDefect(foreignLifetimeFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source Application Transition is bound to a different Topic or logical lifetime.",
      });
      expect(foreignLifetimeHarness.control.module.metrics().current).toBe(true);
      expect(foreignLifetimeHarness.successfulDeletes).toStrictEqual([]);
      yield* foreignLifetimeHarness.close;
    }),
  );

  it.effect("signals complete-runtime fatal errors for maintenance invariant defects", () =>
    Effect.gen(function* () {
      const currentHarness = yield* makeHarness();
      currentHarness.plan([
        {
          id: "row-1",
          workId: "current-defect:1",
          current: "throw",
        },
      ]);
      const currentResult = yield* currentHarness.control.sweep();
      expect(currentResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(currentResult).toJSON()).toStrictEqual(
        Cause.die(currentStateDefect).toJSON(),
      );
      const currentFatal = yield* Effect.exit(currentHarness.manager.fatal);
      expect(fatalFailure(currentFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message:
          "Source maintenance state validation failed fatally and stopped the complete runtime.",
      });
      expect(fatalDefect(currentFatal)).toBe(currentStateDefect);
      yield* currentHarness.close;

      const staleHarness = yield* makeHarness();
      staleHarness.plan([
        {
          id: "row-1",
          workId: "stale-defect:1",
          current: "stale",
          stale: "throw",
        },
      ]);
      const staleResult = yield* staleHarness.control.sweep();
      expect(staleResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(staleResult).toJSON()).toStrictEqual(
        Cause.die(applicationStateDefect).toJSON(),
      );
      const staleFatal = yield* Effect.exit(staleHarness.manager.fatal);
      expect(fatalFailure(staleFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message:
          "Source maintenance stale transition failed fatally and stopped the complete runtime.",
      });
      expect(fatalDefect(staleFatal)).toBe(applicationStateDefect);
      yield* staleHarness.close;

      const successHarness = yield* makeHarness();
      successHarness.plan([
        {
          id: "row-1",
          workId: "success-defect:1",
          success: "throw",
        },
      ]);
      const successResult = yield* successHarness.control.sweep();
      expect(successResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(successResult).toJSON()).toStrictEqual(
        Cause.die(applicationStateDefect).toJSON(),
      );
      const successFatal = yield* Effect.exit(successHarness.manager.fatal);
      expect(fatalFailure(successFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message:
          "Source maintenance success transition failed fatally and stopped the complete runtime.",
      });
      expect(fatalDefect(successFatal)).toBe(applicationStateDefect);
      yield* successHarness.close;

      const failureHarness = yield* makeHarness();
      failureHarness.setDeleteMode("failure");
      failureHarness.plan([
        {
          id: "row-1",
          workId: "failure-defect:1",
          failure: "throw",
        },
      ]);
      const failureResult = yield* failureHarness.control.sweep();
      expect(failureResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(failureResult).toJSON()).toStrictEqual(
        Cause.die(applicationStateDefect).toJSON(),
      );
      const failureFatal = yield* Effect.exit(failureHarness.manager.fatal);
      expect(fatalFailure(failureFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message:
          "Source maintenance failure transition failed fatally and stopped the complete runtime.",
      });
      expect(fatalDefect(failureFatal)).toBe(applicationStateDefect);
      yield* failureHarness.close;

      const combinedFailureHarness = yield* makeHarness();
      combinedFailureHarness.setDeleteMode("defect");
      combinedFailureHarness.plan([
        {
          id: "row-1",
          workId: "combined-failure-defect:1",
          failure: "throw",
        },
      ]);
      const combinedFailureResult = yield* combinedFailureHarness.control.sweep();
      expect(combinedFailureResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(combinedFailureResult).toJSON()).toStrictEqual(
        Cause.combine(Cause.die(applicationStateDefect), Cause.die(deleteDefect)).toJSON(),
      );
      const combinedFailureFatal = yield* Effect.exit(combinedFailureHarness.manager.fatal);
      expect(fatalFailure(combinedFailureFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message:
          "Source maintenance failure transition failed fatally and stopped the complete runtime.",
      });
      const combinedFailureCause = Option.getOrThrow(Exit.getCause(combinedFailureFatal));
      expect(
        combinedFailureCause.reasons.filter(Cause.isDieReason).map(({ defect }) => defect),
      ).toStrictEqual([applicationStateDefect, deleteDefect]);
      yield* combinedFailureHarness.close;

      const deleteHarness = yield* makeHarness();
      deleteHarness.setDeleteMode("defect");
      deleteHarness.plan([{ id: "row-1", workId: "delete-defect:1" }]);
      const deleteResult = yield* deleteHarness.control.sweep();
      expect(deleteResult.map((result) => result._tag)).toStrictEqual(["Applied"]);
      expect(maintenanceFailureCause(deleteResult).toJSON()).toStrictEqual(
        Cause.die(deleteDefect).toJSON(),
      );
      const deleteFatal = yield* Effect.exit(deleteHarness.manager.fatal);
      expect(fatalFailure(deleteFatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source maintenance execution failed fatally and stopped the complete runtime.",
      });
      expect(fatalDefect(deleteFatal)).toBe(deleteDefect);
      yield* deleteHarness.close;

      const invalidSettlementHarness = yield* makeHarness();
      yield* invalidSettlementHarness.control.emit({
        command: { _tag: "Throw" },
        settlement: "throw",
      });
      expect(fatalDefect(yield* Effect.exit(invalidSettlementHarness.manager.fatal))).toBe(
        applicationStateDefect,
      );
      yield* invalidSettlementHarness.close;

      const failedSettlementHarness = yield* makeHarness();
      yield* failedSettlementHarness.control.emit({
        command: { _tag: "Throw" },
        settlement: "failure",
      });
      expect(fatalDefect(yield* Effect.exit(failedSettlementHarness.manager.fatal))).toBe(
        applicationStateDefect,
      );
      yield* Effect.yieldNow;
      yield* failedSettlementHarness.close;
    }),
  );

  it.effect("publishes transition fatality before a handed-off settlement Effect completes", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.control.emit({
        command: { _tag: "Throw" },
        settlement: "blocked",
      });
      yield* Deferred.await(harness.settlementStarted);
      const fatalExit = yield* Effect.exit(harness.manager.fatal);
      expect(
        Exit.isFailure(fatalExit) ? Cause.findErrorOption(fatalExit.cause) : Option.none(),
      ).toStrictEqual(
        Option.some({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source application transition failed and stopped the complete runtime.",
        }),
      );
      expect(fatalDefect(fatalExit)).toBe(applicationStateDefect);
      expect({
        settlementCallbacks: harness.settlementCallbacks(),
        settlementFinalized: yield* Deferred.isDone(harness.settlementFinalized),
        attemptFinalizations: harness.finalizations(),
      }).toStrictEqual({
        settlementCallbacks: 1,
        settlementFinalized: false,
        attemptFinalizations: 0,
      });
      yield* harness.close;
      yield* Deferred.await(harness.settlementFinalized);
      expect({
        settlementCallbacks: harness.settlementCallbacks(),
        attemptFinalizations: harness.finalizations(),
      }).toStrictEqual({
        settlementCallbacks: 1,
        attemptFinalizations: 1,
      });
    }),
  );

  it.effect("hands off a successful callback before publishing a transition-defect fatality", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* harness.control.emit({
        command: { _tag: "Throw" },
        settlement: "success",
      });
      const fatalExit = yield* Effect.exit(harness.manager.fatal);

      expect({
        callbacks: harness.settlementCallbacks(),
        defect: fatalDefect(fatalExit),
        failure: fatalFailure(fatalExit),
      }).toStrictEqual({
        callbacks: 1,
        defect: applicationStateDefect,
        failure: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source application transition failed and stopped the complete runtime.",
        },
      });
      yield* harness.close;
    }),
  );

  it.effect(
    "preserves transition-defect handoff when shutdown wins before settlement-child registration",
    () =>
      Effect.gen(function* () {
        const childForked = yield* Deferred.make<void>();
        const releaseChildRegistration = yield* Deferred.make<void>();
        const callbackApplied = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            afterSettlementChildFork: Deferred.succeed(childForked, undefined).pipe(
              Effect.andThen(Deferred.await(releaseChildRegistration)),
            ),
            afterCallbackApplication: Deferred.succeed(callbackApplied, undefined).pipe(
              Effect.asVoid,
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "success",
        });
        yield* Deferred.await(childForked);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* Deferred.succeed(releaseChildRegistration, undefined);
        yield* Deferred.await(callbackApplied);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
          failure: fatalFailure(fatalExit),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
          failure: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "rows",
            message: "Source application transition failed and stopped the complete runtime.",
          },
        });
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        yield* harness.close;
      }),
  );

  it.effect(
    "preserves transition-defect handoff when shutdown wins after settlement-child registration",
    () =>
      Effect.gen(function* () {
        const childRegistered = yield* Deferred.make<void>();
        const releaseChildRegistration = yield* Deferred.make<void>();
        const callbackApplied = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            afterSettlementChildRegistration: Deferred.succeed(childRegistered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseChildRegistration)),
            ),
            afterCallbackApplication: Deferred.succeed(callbackApplied, undefined).pipe(
              Effect.asVoid,
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "success",
        });
        yield* Deferred.await(childRegistered);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* Deferred.succeed(releaseChildRegistration, undefined);
        yield* Deferred.await(callbackApplied);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
          failure: fatalFailure(fatalExit),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
          failure: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "rows",
            message: "Source application transition failed and stopped the complete runtime.",
          },
        });
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        yield* harness.close;
      }),
  );

  it.effect(
    "preserves a throwing transition settlement when shutdown wins after callback invocation",
    () =>
      Effect.gen(function* () {
        const callbackApplied = yield* Deferred.make<void>();
        const releaseCallback = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            afterCallbackApplication: Deferred.succeed(callbackApplied, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCallback)),
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "throw",
        });
        yield* Deferred.await(callbackApplied);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* Deferred.succeed(releaseCallback, undefined);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
          failure: fatalFailure(fatalExit),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
          failure: {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            topic: "rows",
            message: "Source application transition failed and stopped the complete runtime.",
          },
        });
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        expect(harness.finalizations()).toBe(1);
        yield* harness.close;
      }),
  );

  it.effect(
    "preserves a successful transition settlement when shutdown wins after handoff completion",
    () =>
      Effect.gen(function* () {
        const handoffObserved = yield* Deferred.make<void>();
        const releaseHandoff = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            afterHandoffObserved: Deferred.succeed(handoffObserved, undefined).pipe(
              Effect.andThen(Deferred.await(releaseHandoff)),
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "success",
        });
        yield* Deferred.await(handoffObserved);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* Deferred.succeed(releaseHandoff, undefined);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
        });
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        expect(harness.finalizations()).toBe(1);
        yield* harness.close;
      }),
  );

  it.effect(
    "preserves a failing returned transition settlement when shutdown wins before restore",
    () =>
      Effect.gen(function* () {
        const beforeRestore = yield* Deferred.make<void>();
        const releaseRestore = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            beforeReturnedEffectRestore: Deferred.succeed(beforeRestore, undefined).pipe(
              Effect.andThen(Deferred.await(releaseRestore)),
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "failure",
        });
        yield* Deferred.await(beforeRestore);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* Deferred.succeed(releaseRestore, undefined);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
        });
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        expect(harness.finalizations()).toBe(1);
        yield* harness.close;
      }),
  );

  it.effect(
    "interrupts and joins a never-ending transition settlement after fatal completion wins shutdown",
    () =>
      Effect.gen(function* () {
        const fatalCompleted = yield* Deferred.make<void>();
        const releaseFatal = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          settlementHandoff: {
            afterFatalCompleted: Deferred.succeed(fatalCompleted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFatal)),
            ),
          },
        });
        harness.blockAfterAttemptCancellationRequested();
        yield* harness.control.emit({
          command: { _tag: "Throw" },
          settlement: "blocked",
        });
        yield* Deferred.await(harness.settlementStarted);
        yield* Deferred.await(fatalCompleted);

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect({
          callbacks: harness.settlementCallbacks(),
          defect: fatalDefect(fatalExit),
          settlementFinalized: yield* Deferred.isDone(harness.settlementFinalized),
        }).toStrictEqual({
          callbacks: 1,
          defect: applicationStateDefect,
          settlementFinalized: true,
        });
        yield* Deferred.succeed(releaseFatal, undefined);
        yield* harness.releaseShutdownCancellation;
        yield* Fiber.join(close);
        yield* Deferred.await(harness.settlementFinalized);
        expect(harness.finalizations()).toBe(1);
        yield* harness.close;
      }),
  );

  it.effect(
    "routes an application lifetime lookup defect to the complete runtime fatal signal",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          invalidApplicationLifetimeLookup: true,
          retryDelay: "10 seconds",
        });
        yield* harness.control.fail;
        yield* TestClock.adjust("10 seconds");
        const fatalExit = yield* Effect.exit(harness.manager.fatal);
        expect(fatalFailure(fatalExit)).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source supervisor failed fatally and stopped the complete runtime.",
        });
        expect(fatalDefect(fatalExit)).toStrictEqual(
          new TypeError(
            "Source Application State registration is not bound to this logical lifetime.",
          ),
        );
        yield* harness.close;
      }),
  );

  it.effect(
    "fails the complete runtime when the maintenance supervisor defects or self-interrupts",
    () =>
      Effect.gen(function* () {
        const defect = yield* makeHarness();
        defect.setSweepMode("defect");
        yield* TestClock.adjust("1 second");
        const defectFatal = yield* Effect.exit(defect.manager.fatal);
        expect(fatalFailure(defectFatal)).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source maintenance supervisor failed fatally and closed the complete runtime.",
        });
        expect(fatalDefect(defectFatal)).toBe(sweepDefect);
        yield* defect.close;

        const interrupted = yield* makeHarness();
        interrupted.setSweepMode("interrupt");
        yield* TestClock.adjust("1 second");
        const interruptedFatal = yield* Effect.exit(interrupted.manager.fatal);
        const interruptedFailure = {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source maintenance supervisor failed fatally and closed the complete runtime.",
        } as const;
        expect(fatalFailure(interruptedFatal)).toStrictEqual(interruptedFailure);
        const interruptors = Exit.isFailure(interruptedFatal)
          ? [...Cause.interruptors(interruptedFatal.cause)]
          : [];
        expect(interruptors).toHaveLength(1);
        const interruptedCause = Exit.isFailure(interruptedFatal)
          ? interruptedFatal.cause
          : Cause.empty;
        expect(interruptedCause.toJSON()).toStrictEqual(
          Cause.combine(
            Cause.fail(interruptedFailure),
            Cause.interrupt(Option.getOrThrow(Option.fromUndefinedOr(interruptors[0]))),
          ).toJSON(),
        );
        yield* interrupted.close;
      }),
  );

  it.effect("rechecks active status under the shared lifecycle gate before maintenance", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      harness.setSweepMode("gated-operation");
      const sweep = yield* harness.control.sweep().pipe(Effect.forkChild);
      yield* Deferred.await(harness.sweepEnumerated);

      harness.setDeleteMode("failure");
      const exhausted = yield* harness.awaitStatus("Exhausted").pipe(Effect.forkChild);
      const deliveryExit = yield* harness.control.deliver({
        _tag: "SetCurrent",
        current: true,
      });
      expect(Exit.isFailure(deliveryExit)).toBe(true);
      expect((yield* Fiber.join(exhausted)).status._tag).toBe("Exhausted");

      yield* harness.releaseSweepOperation;
      expect(yield* Fiber.join(sweep)).toStrictEqual([{ _tag: "Inactive" }]);
      const inactiveSweepCount = harness.sweepInvocations();
      harness.plan([{ id: "row-expiry", workId: "exhausted:2" }]);
      yield* TestClock.adjust("3 seconds");
      expect({
        attemptedDeletes: harness.attemptedDeletes,
        metrics: harness.control.module.metrics(),
        sweepInvocations: harness.sweepInvocations(),
      }).toStrictEqual({
        attemptedDeletes: ["row-1"],
        metrics: initialState(),
        sweepInvocations: inactiveSweepCount,
      });
      yield* harness.close;
    }),
  );

  it.effect(
    "completes an Application State transition when cancellation arrives after the engine write",
    () =>
      Effect.gen(function* () {
        const mutationApplied = yield* Deferred.make<void>();
        const releaseMutation = yield* Deferred.make<void>();
        const harness = yield* makeHarness({
          afterMutationApplication: Deferred.succeed(mutationApplied, undefined).pipe(
            Effect.andThen(Deferred.await(releaseMutation)),
          ),
        });
        harness.blockAfterAttemptCancellationRequested();
        const delivery = yield* harness.control
          .deliver({ _tag: "Success" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(mutationApplied);

        expect({
          state: harness.control.module.metrics(),
          successfulDeletes: harness.successfulDeletes,
        }).toStrictEqual({
          state: initialState(),
          successfulDeletes: ["row-1"],
        });

        const close = yield* harness.manager.close.pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(harness.shutdownCancellationRequested);
        yield* harness.releaseShutdownCancellation;
        yield* Effect.yieldNow;
        expect(close.pollUnsafe()).toBeUndefined();

        yield* Deferred.succeed(releaseMutation, undefined);
        yield* Fiber.join(close);
        yield* Fiber.interrupt(delivery).pipe(Effect.asVoid);

        expect({
          state: harness.control.module.metrics(),
          successfulDeletes: harness.successfulDeletes,
        }).toStrictEqual({
          state: {
            current: true,
            successes: 1,
            failures: 0,
            stale: 0,
          },
          successfulDeletes: ["row-1"],
        });
        yield* harness.close;
      }),
  );

  it.effect("records a maintenance outcome when cancellation arrives after its engine write", () =>
    Effect.gen(function* () {
      const mutationApplied = yield* Deferred.make<void>();
      const releaseMutation = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        afterMutationApplication: Deferred.succeed(mutationApplied, undefined).pipe(
          Effect.andThen(Deferred.await(releaseMutation)),
        ),
      });
      harness.plan([{ id: "row-expiry", workId: "shutdown:after-write" }]);
      const cadence = yield* TestClock.adjust("1 second").pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(mutationApplied);

      expect({
        state: harness.control.module.metrics(),
        successfulDeletes: harness.successfulDeletes,
      }).toStrictEqual({
        state: initialState(),
        successfulDeletes: ["row-expiry"],
      });

      harness.blockAfterAttemptCancellationRequested();
      const close = yield* harness.manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(harness.shutdownCancellationRequested);
      yield* harness.releaseShutdownCancellation;
      yield* Effect.yieldNow;
      expect(close.pollUnsafe()).toBeUndefined();

      yield* Deferred.succeed(releaseMutation, undefined);
      yield* Fiber.join(close);
      yield* Fiber.interrupt(cadence).pipe(Effect.asVoid);

      expect({
        state: harness.control.module.metrics(),
        successfulDeletes: harness.successfulDeletes,
      }).toStrictEqual({
        state: {
          current: true,
          successes: 1,
          failures: 0,
          stale: 0,
        },
        successfulDeletes: ["row-expiry"],
      });
      yield* harness.close;
    }),
  );

  it.effect("linearizes admitted maintenance before the visible Stopping transition", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      harness.setDeleteMode("failure");
      harness.plan([{ id: "row-expiry", workId: "shutdown:admitted" }]);
      yield* harness.control.sweep();
      expect((yield* harness.health()).status._tag).toBe("Degraded");
      harness.setDeleteMode("gated-success");
      harness.blockAfterAttemptCancellationRequested();
      harness.plan([{ id: "row-expiry", workId: "shutdown:admitted" }]);
      const sweep = yield* harness.control
        .sweep()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(harness.deleteStarted);

      const close = yield* harness.manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(harness.shutdownCancellationRequested);
      expect(harness.aggregateHealth().sources.rows?.status._tag).toBe("Degraded");

      yield* harness.releaseDelete;
      expect(yield* Fiber.join(sweep)).toStrictEqual([
        {
          _tag: "Applied",
          exit: Exit.void,
        },
      ]);
      yield* harness.releaseShutdownCancellation;
      expect((yield* harness.awaitOverlayStatus("Stopping"))._tag).toBe("Stopping");
      yield* Fiber.join(close);
      expect({
        attemptedDeletes: harness.attemptedDeletes,
        state: harness.control.module.metrics(),
        successfulDeletes: harness.successfulDeletes,
      }).toStrictEqual({
        attemptedDeletes: ["row-expiry", "row-expiry"],
        state: {
          current: true,
          successes: 1,
          failures: 1,
          stale: 0,
        },
        successfulDeletes: ["row-expiry"],
      });
      yield* harness.close;
    }),
  );

  it.effect("lets Stopping win before enumerated maintenance is admitted", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      harness.setSweepMode("gated-operation");
      const sweep = yield* harness.control
        .sweep()
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(harness.sweepEnumerated);

      const close = yield* harness.manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      expect((yield* harness.awaitOverlayStatus("Stopping"))._tag).toBe("Stopping");
      yield* harness.releaseSweepOperation;
      yield* Fiber.join(close);
      yield* Fiber.interrupt(sweep);
      expect({
        attemptedDeletes: harness.attemptedDeletes,
        state: harness.control.module.metrics(),
        successfulDeletes: harness.successfulDeletes,
      }).toStrictEqual({
        attemptedDeletes: [],
        state: initialState(),
        successfulDeletes: [],
      });
      yield* harness.close;
    }),
  );

  it.effect(
    "does not enumerate maintenance while waiting, reacquiring, or stopping across cadences",
    () =>
      Effect.gen(function* () {
        const retrying = yield* makeHarness({
          blockReacquisition: true,
          retryDelay: "10 seconds",
        });
        yield* retrying.control.fail;
        for (let index = 0; index < 10; index++) {
          yield* Effect.yieldNow;
        }
        expect((yield* retrying.awaitOverlayStatus("WaitingToRetry"))._tag).toBe("WaitingToRetry");
        retrying.plan([{ id: "row-1", workId: "waiting:1" }]);
        yield* TestClock.adjust("3 seconds");
        expect({
          attemptedDeletes: retrying.attemptedDeletes,
          state: retrying.control.module.metrics(),
          sweepInvocations: retrying.sweepInvocations(),
        }).toStrictEqual({
          attemptedDeletes: [],
          state: initialState(),
          sweepInvocations: 0,
        });

        const reacquiring = yield* retrying
          .awaitOverlayStatus("Reacquiring")
          .pipe(Effect.forkChild);
        yield* TestClock.adjust("7 seconds");
        expect((yield* Fiber.join(reacquiring))._tag).toBe("Reacquiring");
        yield* TestClock.adjust("3 seconds");
        expect({
          attemptedDeletes: retrying.attemptedDeletes,
          state: retrying.control.module.metrics(),
          sweepInvocations: retrying.sweepInvocations(),
        }).toStrictEqual({
          attemptedDeletes: [],
          state: initialState(),
          sweepInvocations: 0,
        });
        yield* retrying.releaseAcquisition;
        expect((yield* retrying.awaitOverlayStatus("Ready"))._tag).toBe("Ready");
        yield* retrying.close;

        const stopping = yield* makeHarness({ blockFinalizer: true });
        stopping.plan([{ id: "row-1", workId: "stopping:1" }]);
        const closeFiber = yield* stopping.manager.close.pipe(
          Effect.forkDetach({ startImmediately: true }),
        );
        expect((yield* stopping.awaitOverlayStatus("Stopping"))._tag).toBe("Stopping");
        const stoppingSweepCount = stopping.sweepInvocations();
        yield* TestClock.adjust("3 seconds");
        expect({
          attemptedDeletes: stopping.attemptedDeletes,
          state: stopping.control.module.metrics(),
          sweepInvocations: stopping.sweepInvocations(),
        }).toStrictEqual({
          attemptedDeletes: [],
          state: initialState(),
          sweepInvocations: stoppingSweepCount,
        });
        yield* stopping.releaseFinalizer;
        yield* Fiber.join(closeFiber);
        yield* stopping.close;
      }),
  );

  it.effect("rejects invalid, foreign, and inactive maintenance operations", () =>
    Effect.gen(function* () {
      const invalid = yield* makeHarness();
      invalid.setSweepMode("invalid-operation");
      expect(yield* invalid.control.sweep()).toStrictEqual([{ _tag: "Inactive" }]);
      yield* invalid.close;

      const foreign = yield* makeHarness();
      foreign.setSweepMode("foreign-operation");
      expect(yield* foreign.control.sweep()).toStrictEqual([{ _tag: "Inactive" }]);
      expect(yield* Effect.flip(foreign.manager.fatal)).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Source Maintenance Operation is bound to a different Topic or logical lifetime.",
      });
      yield* foreign.close;

      const inactive = yield* makeHarness({ blockAcquisition: true });
      inactive.plan([{ id: "row-1", workId: "inactive:1" }]);
      yield* TestClock.adjust("3 seconds");
      expect({
        attemptedDeletes: inactive.attemptedDeletes,
        state: inactive.control.module.metrics(),
        sweepInvocations: inactive.sweepInvocations(),
      }).toStrictEqual({
        attemptedDeletes: [],
        state: initialState(),
        sweepInvocations: 0,
      });
      yield* inactive.close;
    }),
  );
});
