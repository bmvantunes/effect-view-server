import { expect, it as vitestIt, layer as vitestLayer, type Vitest } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import {
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceRow,
  inspectSourceAdapterPackageConformance,
  validateSourceAdapterPackageConformance,
  type SourceAdapterConformanceAttemptFault,
  type SourceAdapterConformanceDriverValue,
  type SourceAdapterConformanceEventModel,
  type SourceAdapterConformanceSuiteOptions,
  type SourceAdapterConformanceTarget,
  type SourceAdapterConformanceTransportObservation,
  type SourceAdapterPackageInspectionOptions,
} from "@effect-view-server/source-adapter-testing";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Option, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  makeSourceAdapterPackageConformanceCheck,
  makeSourceAdapterPackageConformanceRegistrar,
} from "./package-registration";
import {
  SourceAdapterConformanceRowIdError,
  validateSourceAdapterRuntimeContext,
} from "./runtime-context";
import {
  type HostHealthSnapshot,
  openHealth,
  openQuery,
  requireCallbackBridge,
  requireLeased,
  requireMaterialized,
  rows,
} from "./probes";

export * from "@effect-view-server/source-adapter-testing";

export type SourceAdapterConformanceOptions = SourceAdapterConformanceSuiteOptions;

export type SourceAdapterPackageConformanceOptions = {
  readonly inspection: SourceAdapterPackageInspectionOptions;
  readonly behavioral: SourceAdapterConformanceSuiteOptions;
};

const SourceAdapterConformanceConfigRow = Schema.Struct({
  ...SourceAdapterConformanceRow.fields,
  id: ViewServerId,
});

const materializedTarget = (
  lane: "primary" | "sibling" = "primary",
): Extract<SourceAdapterConformanceTarget, { readonly _tag: "Materialized" }> => ({
  _tag: "Materialized",
  lane,
});

const leasedTarget = (
  route: Readonly<Record<string, unknown>>,
  lane: "primary" | "sibling" = "primary",
): Extract<SourceAdapterConformanceTarget, { readonly _tag: "Leased" }> => ({
  _tag: "Leased",
  route,
  lane,
});

const awaitObservation = Effect.fn("SourceAdapterConformanceHost.transport.awaitObservation")(
  function* (
    driver: SourceAdapterConformanceDriverValue,
    target: SourceAdapterConformanceTarget,
    predicate: (observation: SourceAdapterConformanceTransportObservation) => boolean,
  ) {
    const current = yield* driver.transport.observe(target);
    if (predicate(current)) {
      return current;
    }
    return yield* driver.transport
      .changes(target)
      .pipe(
        Stream.filter(predicate),
        Stream.take(1),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
  },
);

type ConformanceSource =
  | NonNullable<SourceAdapterConformanceDriverValue["materialized"]>["source"]
  | NonNullable<SourceAdapterConformanceDriverValue["leased"]>["source"];

type HostRuntime = {
  readonly liveClient: object;
  readonly close: Effect.Effect<void, unknown>;
};

const manageRuntime = Effect.fn("SourceAdapterConformanceHost.runtime.manage")(function* <
  Runtime extends HostRuntime,
>(driver: SourceAdapterConformanceDriverValue, source: ConformanceSource, runtime: Runtime) {
  const route = source.lifecycle === "leased" ? requireLeased(driver).sameRoute : undefined;
  const leaseSubscription =
    route === undefined
      ? undefined
      : yield* openQuery(runtime.liveClient, {
          routeBy: route,
          select: ["id"],
        });
  const close = yield* Effect.cached(
    Effect.all(
      [...(leaseSubscription === undefined ? [] : [leaseSubscription.close()]), runtime.close],
      { discard: true },
    ),
  );
  yield* Scope.addFinalizer(yield* Effect.scope, close.pipe(Effect.orDie));
  const observeHealth = <Value>(
    observe: (events: Stream.Stream<HostHealthSnapshot, unknown>) => Effect.Effect<Value, unknown>,
  ) =>
    Effect.acquireUseRelease(
      openHealth(runtime.liveClient, route),
      (diagnostics) => observe(diagnostics.events),
      (diagnostics) => diagnostics.close(),
    );
  const nextHealth = observeHealth((events) =>
    events.pipe(Stream.take(1), Stream.runHead, Effect.map(Option.getOrThrow)),
  );
  const awaitStatus = (tag: string) =>
    observeHealth((events) =>
      events.pipe(
        Stream.filter((health) => health.statusTag === tag),
        Stream.take(1),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      ),
    );
  return {
    runtime,
    nextHealth,
    awaitStatus,
    route,
    close,
  };
});

const openRuntime = Effect.fn("SourceAdapterConformanceHost.runtime.open")(function* (
  driver: SourceAdapterConformanceDriverValue,
  source: ConformanceSource,
) {
  const config = defineViewServerConfig({
    topics: {
      rows: {
        schema: SourceAdapterConformanceConfigRow,
        source,
      },
    },
  });
  const runtimeContext = yield* validateSourceAdapterRuntimeContext(driver.runtimeContext);
  const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
    Effect.provideContext(runtimeContext),
  );
  return yield* manageRuntime(driver, source, runtime);
});

const applicationValue = Schema.String.pipe(
  Schema.refine((value): value is string => {
    if (value === "application-defect") {
      throw new Error("conformance application defect");
    }
    if (value === "application-interruption") {
      Option.getOrThrow(Option.fromUndefinedOr(Fiber.getCurrent())).interruptUnsafe();
    }
    return true;
  }),
);

const applicationRow = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  value: applicationValue,
});

const openApplicationRuntime = Effect.fn(
  "SourceAdapterConformanceHost.runtime.openApplicationExit",
)(function* (driver: SourceAdapterConformanceDriverValue, source: ConformanceSource) {
  const config = defineViewServerConfig({
    topics: {
      rows: {
        schema: applicationRow,
        source,
      },
    },
  });
  const runtimeContext = yield* validateSourceAdapterRuntimeContext(driver.runtimeContext);
  const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
    Effect.provideContext(runtimeContext),
  );
  return yield* manageRuntime(driver, source, runtime);
});

type SourceAdapterConformanceLifecycle = "materialized" | "leased";

const requireLifecycle = (
  driver: SourceAdapterConformanceDriverValue,
  lifecycle: SourceAdapterConformanceLifecycle,
) => (lifecycle === "materialized" ? requireMaterialized(driver) : requireLeased(driver));

const lifecycleTarget = (
  driver: SourceAdapterConformanceDriverValue,
  lifecycle: SourceAdapterConformanceLifecycle,
  lane: "primary" | "sibling" = "primary",
): SourceAdapterConformanceTarget =>
  lifecycle === "materialized"
    ? materializedTarget(lane)
    : leasedTarget(requireLeased(driver).sameRoute, lane);

const awaitRows = (
  runtime: HostRuntime,
  route: Readonly<Record<string, unknown>> | undefined,
  expected: ReadonlyArray<string>,
): Effect.Effect<ReadonlyArray<string>, unknown> =>
  Stream.fromEffectRepeat(Effect.yieldNow.pipe(Effect.andThen(rows(runtime, route)))).pipe(
    Stream.filter(
      (actual) =>
        actual.length === expected.length &&
        actual.every((value, index) => value === expected[index]),
    ),
    Stream.take(1),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );

const lifecycleExpectations = (
  driver: SourceAdapterConformanceDriverValue,
  lifecycle: SourceAdapterConformanceLifecycle,
): {
  readonly acquisitionFailure: unknown;
  readonly partialAcquisitionFinalizationCount: bigint;
  readonly streamFailure: unknown;
  readonly settlementFailure: unknown;
  readonly rejectionFailure: (phase: "acquire" | "stream" | "settlement") => unknown;
  readonly updatedMetrics: unknown;
} => {
  return Option.getOrThrow(Option.fromUndefinedOr(driver.expectations[lifecycle]));
};

const expectedRowId = (
  driver: SourceAdapterConformanceDriverValue,
  lifecycle: SourceAdapterConformanceLifecycle,
  lane: "primary" | "sibling",
  localId: string,
): Effect.Effect<string, SourceAdapterConformanceRowIdError> => {
  const rowId = Effect.try({
    try: () =>
      lifecycle === "materialized"
        ? requireMaterializedExpectations(driver).rowId(materializedTarget(lane), localId)
        : requireLeasedExpectations(driver).rowId(
            leasedTarget(requireLeased(driver).sameRoute, lane),
            localId,
          ),
    catch: () =>
      new SourceAdapterConformanceRowIdError({
        message: "Source Adapter conformance rowId expectation must return a string.",
      }),
  });
  return rowId.pipe(
    Effect.filterOrFail(
      (value): value is string => typeof value === "string",
      () =>
        new SourceAdapterConformanceRowIdError({
          message: "Source Adapter conformance rowId expectation must return a string.",
        }),
    ),
  );
};

const expectedRejectionLocation = (
  driver: SourceAdapterConformanceDriverValue,
  lifecycle: SourceAdapterConformanceLifecycle,
  lane: "primary" | "sibling",
  offset: bigint,
): unknown => {
  if (lifecycle === "materialized") {
    return requireMaterializedExpectations(driver).rejectionLocation(
      materializedTarget(lane),
      offset,
    );
  }
  return requireLeasedExpectations(driver).rejectionLocation(
    leasedTarget(requireLeased(driver).sameRoute, lane),
    offset,
  );
};

const requireMaterializedExpectations = (driver: SourceAdapterConformanceDriverValue) => {
  return Option.getOrThrow(Option.fromUndefinedOr(driver.expectations.materialized));
};

const requireLeasedExpectations = (driver: SourceAdapterConformanceDriverValue) => {
  return Option.getOrThrow(Option.fromUndefinedOr(driver.expectations.leased));
};

const expectInvalidSourceDefinition = (health: HostHealthSnapshot): void => {
  expect(health.statusTag).toBe("Exhausted");
  expect(health.lastRuntimeFailureTag).toBe("InvalidSourceDefinition");
};

const registerLifecycleConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
  lifecycle: SourceAdapterConformanceLifecycle,
): void => {
  it.effect("applies ordered lane deliveries and runs sibling lanes concurrently", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const settlements: Array<readonly [string, Exit.Exit<void, unknown>]> = [];
        const releaseFirstPrimarySettlement = yield* Deferred.make<void>();
        const firstPrimarySettlementStarted = yield* Deferred.make<void>();
        const firstPrimarySettled = yield* Deferred.make<void>();
        const secondPrimarySettled = yield* Deferred.make<void>();
        const siblingSettled = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Delivery",
          target: lifecycleTarget(driver, lifecycle),
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "removed", region: "eu", value: "first" },
            },
            {
              _tag: "Upsert",
              row: { id: "primary", region: "eu", value: "second" },
            },
            { _tag: "Delete", id: "removed" },
          ],
          settle: (exit) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(firstPrimarySettlementStarted, undefined);
              yield* Deferred.await(releaseFirstPrimarySettlement);
              settlements.push(["primary-first", exit]);
              yield* Deferred.succeed(firstPrimarySettled, undefined);
            }),
        });
        yield* Deferred.await(firstPrimarySettlementStarted);
        yield* driver.transport.command({
          _tag: "Delivery",
          target: lifecycleTarget(driver, lifecycle),
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "primary-second", region: "eu", value: "ordered" },
            },
          ],
          settle: (exit) =>
            Effect.sync(() => {
              settlements.push(["primary-second", exit]);
            }).pipe(
              Effect.andThen(Deferred.succeed(secondPrimarySettled, undefined)),
              Effect.asVoid,
            ),
        });
        yield* driver.transport.command({
          _tag: "Delivery",
          target: lifecycleTarget(driver, lifecycle, "sibling"),
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "sibling", region: "eu", value: "parallel" },
            },
          ],
          settle: (exit) =>
            Effect.sync(() => {
              settlements.push(["sibling", exit]);
            }).pipe(Effect.andThen(Deferred.succeed(siblingSettled, undefined)), Effect.asVoid),
        });
        yield* Deferred.await(siblingSettled);
        expect(yield* Deferred.isDone(secondPrimarySettled)).toBe(false);
        const primaryRowId = yield* expectedRowId(driver, lifecycle, "primary", "primary");
        const siblingRowId = yield* expectedRowId(driver, lifecycle, "sibling", "sibling");
        expect(yield* rows(opened.runtime, opened.route)).toStrictEqual(
          [primaryRowId, siblingRowId].toSorted(),
        );
        yield* Deferred.succeed(releaseFirstPrimarySettlement, undefined);
        yield* Deferred.await(firstPrimarySettled);
        yield* Deferred.await(secondPrimarySettled);
        const secondPrimaryRowId = yield* expectedRowId(
          driver,
          lifecycle,
          "primary",
          "primary-second",
        );
        expect(yield* rows(opened.runtime, opened.route)).toStrictEqual(
          [primaryRowId, secondPrimaryRowId, siblingRowId].toSorted(),
        );
        expect(settlements).toStrictEqual([
          ["sibling", Exit.void],
          ["primary-first", Exit.void],
          ["primary-second", Exit.void],
        ]);
        expect(yield* driver.transport.observe(lifecycleTarget(driver, lifecycle))).toStrictEqual({
          acquisitions: baseline.acquisitions + 1n,
          finalizations: baseline.finalizations,
          partialAcquisitionFinalizations: baseline.partialAcquisitionFinalizations,
          registrations: 0n,
          callbackFinalizations: 0n,
          finalizerStarted: false,
        });
        yield* opened.close;
        const closed = yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (observation) => observation.finalizations === baseline.finalizations + 1n,
        );
        expect(closed.acquisitions).toBe(closed.finalizations);
      }),
    ),
  );

  it.effect("rolls back a partially acquired attempt before retrying", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        yield* driver.transport.command({
          _tag: "FailNextAcquisition",
          target: lifecycleTarget(driver, lifecycle),
          phase: "acquire",
          afterFirstResource: true,
        });
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        const waiting = yield* opened.awaitStatus("WaitingToRetry");
        expect(waiting.lastExecutionFailure).toStrictEqual({
          _tag: "AdapterFailure",
          failure: lifecycleExpectations(driver, lifecycle).acquisitionFailure,
        });
        yield* TestClock.adjust("1 second");
        yield* opened.awaitStatus("Ready");
        const observation = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        expect({
          acquisitions: observation.acquisitions,
          finalizations: observation.finalizations,
          partialAcquisitionFinalizations: observation.partialAcquisitionFinalizations,
        }).toStrictEqual({
          acquisitions: baseline.acquisitions + 1n,
          finalizations: baseline.finalizations,
          partialAcquisitionFinalizations: baseline.partialAcquisitionFinalizations + 1n,
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("settles success, typed failure, defect, and interruption application exits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const exits: Array<Exit.Exit<void, unknown>> = [];
        const modes = ["success", "typed-failure", "defect", "interruption"] as const;
        for (const [index, testMode] of modes.entries()) {
          const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
          const opened = yield* openApplicationRuntime(driver, definitions.source);
          yield* opened.awaitStatus("Ready");
          yield* awaitObservation(
            driver,
            lifecycleTarget(driver, lifecycle),
            (observation) => observation.acquisitions === baseline.acquisitions + 1n,
          );
          const returnedEffectStarted = yield* Deferred.make<void>();
          const releaseReturnedEffect = yield* Deferred.make<void>();
          const returnedEffectCompleted = yield* Deferred.make<void>();
          const settle = (exit: Exit.Exit<void, unknown>) => {
            exits.push(exit);
            return Deferred.succeed(returnedEffectStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseReturnedEffect)),
              Effect.ensuring(
                Deferred.succeed(returnedEffectCompleted, undefined).pipe(Effect.asVoid),
              ),
            );
          };
          if (testMode === "typed-failure") {
            yield* driver.transport.command({
              _tag: "CorruptLaterMutation",
              target: lifecycleTarget(driver, lifecycle),
              firstRow: {
                id: `application-${index}-first`,
                region: "eu",
                value: "application-exit",
              },
              laterRow: {
                id: `application-${index}-later`,
                region: "eu",
                value: "application-exit",
              },
              field: "region",
              value: 1,
              settle,
            });
          } else if (testMode === "success") {
            yield* driver.transport.command({
              _tag: "Delivery",
              target: lifecycleTarget(driver, lifecycle),
              mutations: [
                {
                  _tag: "Upsert",
                  row: {
                    id: `application-${index}`,
                    region: "eu",
                    value: "application-exit",
                  },
                },
              ],
              settle,
            });
          } else {
            yield* driver.transport.command({
              _tag: "CorruptLaterMutation",
              target: lifecycleTarget(driver, lifecycle),
              firstRow: {
                id: `application-${index}-first`,
                region: "eu",
                value: "application-exit",
              },
              laterRow: {
                id: `application-${index}-later`,
                region: "eu",
                value: "application-exit",
              },
              field: "value",
              value: `application-${testMode}`,
              settle,
            });
          }
          yield* Stream.fromEffectRepeat(Effect.yieldNow).pipe(
            Stream.filter(() => exits.length > index),
            Stream.take(1),
            Stream.runDrain,
          );
          yield* Deferred.await(returnedEffectStarted);
          expect(yield* Deferred.isDone(returnedEffectCompleted)).toBe(testMode === "interruption");
          yield* Deferred.succeed(releaseReturnedEffect, undefined);
          yield* Deferred.await(returnedEffectCompleted);
          yield* opened.close;
        }
        expect(exits.map((exit) => exit._tag)).toStrictEqual([
          "Success",
          "Failure",
          "Failure",
          "Failure",
        ]);
        expect(
          Exit.findErrorOption(Option.getOrThrow(Option.fromUndefinedOr(exits[1]))),
        ).toStrictEqual(
          Option.some({
            _tag: "InvalidTopicRow",
            topic: "rows",
            message: "Source Upsert does not satisfy Topic rows Schema.",
          }),
        );
        const defect = Option.getOrThrow(
          Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(exits[2]))),
        );
        const interruption = Option.getOrThrow(
          Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(exits[3]))),
        );
        expect(defect.reasons.find(Cause.isDieReason)?.defect).toStrictEqual(
          new Error("conformance application defect"),
        );
        expect(Cause.hasInterruptsOnly(interruption)).toBe(true);
      }),
    ),
  );

  it.effect(
    "publishes a transition-defect Runtime Fatal Signal before a returned settlement gate opens",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const driver = yield* SourceAdapterConformanceDriver;
          const definitions = requireLifecycle(driver, lifecycle);
          const target = lifecycleTarget(driver, lifecycle);
          const opened = yield* openApplicationRuntime(driver, definitions.source);
          yield* opened.awaitStatus("Ready");
          const callbackApplied = yield* Deferred.make<void>();
          const releaseReturnedEffect = yield* Deferred.make<void>();
          const returnedEffectCompleted = yield* Deferred.make<void>();
          let callbackApplications = 0;
          yield* driver.transport.command({
            _tag: "TransitionDefect",
            target,
            row: {
              id: "transition-defect",
              region: "eu",
              value: "transition-defect",
            },
            settle: () => {
              callbackApplications += 1;
              return Deferred.succeed(callbackApplied, undefined).pipe(
                Effect.andThen(Deferred.await(releaseReturnedEffect)),
                Effect.ensuring(
                  Deferred.succeed(returnedEffectCompleted, undefined).pipe(Effect.asVoid),
                ),
              );
            },
          });
          yield* Deferred.await(callbackApplied);
          const fatalExit = yield* Effect.exit(opened.runtime.fatal);
          const fatalCause = Option.getOrThrow(Exit.getCause(fatalExit));
          expect({
            callbackApplications,
            returnedEffectCompleted: yield* Deferred.isDone(returnedEffectCompleted),
            fatalFailure: Cause.findErrorOption(fatalCause),
          }).toStrictEqual({
            callbackApplications: 1,
            returnedEffectCompleted: false,
            fatalFailure: Option.some({
              _tag: "ViewServerRuntimeError",
              code: "RuntimeUnavailable",
              topic: "rows",
              message: "Source application transition failed and stopped the complete runtime.",
            }),
          });
          expect(Cause.pretty(fatalCause)).toContain("conformance transition defect");
          yield* Deferred.succeed(releaseReturnedEffect, undefined);
          yield* Deferred.await(returnedEffectCompleted);
          yield* opened.close;
        }),
      ),
  );

  it.effect("restores and owns returned settlement Effects in the Source Attempt Scope", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const callbackApplications: Array<SourceAdapterConformanceLifecycle> = [];
        const returnedEffectStarted = yield* Deferred.make<void>();
        const returnedEffectFinalized = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Delivery",
          target,
          mutations: [
            {
              _tag: "Upsert",
              row: {
                id: "outer-restore",
                region: "eu",
                value: "interruptible",
              },
            },
          ],
          settle: () => {
            callbackApplications.push(lifecycle);
            return Deferred.succeed(returnedEffectStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(returnedEffectFinalized, undefined).pipe(Effect.asVoid),
              ),
            );
          },
        });
        yield* Deferred.await(returnedEffectStarted);
        expect(callbackApplications).toStrictEqual([lifecycle]);

        yield* opened.close;
        yield* Deferred.await(returnedEffectFinalized);
        const closed = yield* awaitObservation(
          driver,
          target,
          (observation) => observation.finalizations === baseline.finalizations + 1n,
        );
        expect({
          callbackApplications,
          acquisitions: closed.acquisitions,
          finalizations: closed.finalizations,
        }).toStrictEqual({
          callbackApplications: [lifecycle],
          acquisitions: baseline.acquisitions + 1n,
          finalizations: baseline.finalizations + 1n,
        });
      }),
    ),
  );

  it.effect("restores and owns rejection settlement Effects in the Source Attempt Scope", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const callbackApplications: Array<SourceAdapterConformanceLifecycle> = [];
        const returnedEffectStarted = yield* Deferred.make<void>();
        const returnedEffectFinalized = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Reject",
          target,
          phase: "stream",
          offset: 91n,
          settle: () => {
            callbackApplications.push(lifecycle);
            return Deferred.succeed(returnedEffectStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(returnedEffectFinalized, undefined).pipe(Effect.asVoid),
              ),
            );
          },
        });
        yield* Deferred.await(returnedEffectStarted);
        expect(callbackApplications).toStrictEqual([lifecycle]);

        yield* opened.close;
        yield* Deferred.await(returnedEffectFinalized);
        const closed = yield* awaitObservation(
          driver,
          target,
          (observation) => observation.finalizations === baseline.finalizations + 1n,
        );
        expect({
          callbackApplications,
          acquisitions: closed.acquisitions,
          finalizations: closed.finalizations,
        }).toStrictEqual({
          callbackApplications: [lifecycle],
          acquisitions: baseline.acquisitions + 1n,
          finalizations: baseline.finalizations + 1n,
        });
      }),
    ),
  );

  it.effect("classifies an interruption-masking returned settlement Effect as non-conformant", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const returnedEffectStarted = yield* Deferred.make<void>();
        const releaseHostileEffect = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Delivery",
          target,
          mutations: [
            {
              _tag: "Upsert",
              row: {
                id: "masked-returned-effect",
                region: "eu",
                value: "non-conformant",
              },
            },
          ],
          settle: () =>
            Effect.uninterruptible(
              Deferred.succeed(returnedEffectStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseHostileEffect)),
              ),
            ),
        });
        yield* Deferred.await(returnedEffectStarted);
        const closeFiber = yield* opened.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Effect.yieldNow;
        expect(closeFiber.pollUnsafe()).toBeUndefined();

        yield* Deferred.succeed(releaseHostileEffect, undefined);
        yield* Fiber.join(closeFiber);
      }),
    ),
  );

  it.effect("classifies synchronous blocking settlement callbacks as non-conformant", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const callbackApplied = yield* Deferred.make<void>();
        const blockingCell = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
        let waitResult: "not-equal" | "ok" | "timed-out" | undefined;
        yield* driver.transport.command({
          _tag: "Delivery",
          target,
          mutations: [
            {
              _tag: "Upsert",
              row: {
                id: "blocking-callback",
                region: "eu",
                value: "non-conformant",
              },
            },
          ],
          settle: () => {
            waitResult = Atomics.wait(blockingCell, 0, 0, 1);
            return Deferred.succeed(callbackApplied, undefined).pipe(Effect.asVoid);
          },
        });
        yield* Deferred.await(callbackApplied);
        expect(waitResult).toBe("timed-out");
        const rowId = yield* expectedRowId(driver, lifecycle, "primary", "blocking-callback");
        expect(yield* rows(opened.runtime, opened.route)).toStrictEqual([rowId]);
        yield* opened.close;
      }),
    ),
  );

  it.effect("rejects every invalid initial attempt shape", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const faults: ReadonlyArray<
          Exclude<SourceAdapterConformanceAttemptFault, "ChangedLaneIds">
        > = ["EmptyLanes", "EmptyLaneId", "DuplicateLaneId", "MissingBufferMetrics"];
        for (const fault of faults) {
          yield* driver.transport.command({
            _tag: "ConfigureNextAttempt",
            target: lifecycleTarget(driver, lifecycle),
            fault,
          });
          yield* driver.transport.command({
            _tag: "ConfigureNextAttempt",
            target: lifecycleTarget(driver, lifecycle),
            fault,
          });
          const opened = yield* openRuntime(driver, definitions.singleRetrySource);
          expectInvalidSourceDefinition(yield* opened.awaitStatus("Exhausted"));
          yield* opened.close;
        }
      }),
    ),
  );

  it.effect("rejects lane identities that change between retry attempts", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.singleRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "ConfigureNextAttempt",
          target: lifecycleTarget(driver, lifecycle),
          fault: "ChangedLaneIds",
        });
        yield* driver.transport.command({
          _tag: "FailLane",
          target: lifecycleTarget(driver, lifecycle),
          phase: "stream",
        });
        expectInvalidSourceDefinition(yield* opened.awaitStatus("Exhausted"));
        yield* opened.close;
      }),
    ),
  );

  it.effect("records rejection before settlement and keeps Degraded sticky after retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        yield* opened.awaitStatus("Ready");
        let statusAtSettlement = "";
        const settled = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Reject",
          target: lifecycleTarget(driver, lifecycle),
          phase: "settlement",
          offset: 1n,
          settle: () =>
            opened.nextHealth.pipe(
              Effect.tap((health) =>
                Effect.sync(() => {
                  statusAtSettlement = health.statusTag;
                }),
              ),
              Effect.andThen(Deferred.succeed(settled, undefined)),
              Effect.andThen(Effect.fail("planned settlement failure")),
            ),
        });
        yield* Deferred.await(settled);
        const waiting = yield* opened.awaitStatus("WaitingToRetry");
        expect(waiting.lastExecutionFailure).toStrictEqual({
          _tag: "AdapterFailure",
          failure: lifecycleExpectations(driver, lifecycle).settlementFailure,
        });
        yield* TestClock.adjust("1 second");
        const observation = yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        const recovered = yield* opened.awaitStatus("Degraded");
        expect({
          statusAtSettlement,
          acquisitions: observation.acquisitions,
          finalizations: observation.finalizations,
          rejectedItemCount: recovered.rejectedItemCount,
          failedSettlementCount: recovered.failedSettlementCount,
          latestRejectionFailure: recovered.latestRejectionFailure,
          latestRejectionLocation: recovered.latestRejectionLocation,
        }).toStrictEqual({
          statusAtSettlement: "Degraded",
          acquisitions: baseline.acquisitions + 2n,
          finalizations: baseline.finalizations + 1n,
          rejectedItemCount: 1n,
          failedSettlementCount: 1n,
          latestRejectionFailure: {
            _tag: "AdapterFailure",
            failure: lifecycleExpectations(driver, lifecycle).rejectionFailure("settlement"),
          },
          latestRejectionLocation: expectedRejectionLocation(driver, lifecycle, "primary", 1n),
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("settles a rejection successfully and continues the same lane", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const rejectionSettled = yield* Deferred.make<Exit.Exit<void, unknown>>();
        yield* driver.transport.command({
          _tag: "Reject",
          target: lifecycleTarget(driver, lifecycle),
          phase: "stream",
          offset: 11n,
          settle: (exit) => Deferred.succeed(rejectionSettled, exit).pipe(Effect.asVoid),
        });
        expect(yield* Deferred.await(rejectionSettled)).toStrictEqual(Exit.void);

        const deliverySettled = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Delivery",
          target: lifecycleTarget(driver, lifecycle),
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "after-rejection", region: "eu", value: "continued" },
            },
          ],
          settle: () => Deferred.succeed(deliverySettled, undefined).pipe(Effect.asVoid),
        });
        yield* Deferred.await(deliverySettled);
        const rowId = yield* expectedRowId(driver, lifecycle, "primary", "after-rejection");
        expect(yield* rows(opened.runtime, opened.route)).toStrictEqual([rowId]);

        const degraded = yield* opened.awaitStatus("Degraded");
        expect({
          rejectedItemCount: degraded.rejectedItemCount,
          failure: degraded.latestRejectionFailure,
          location: degraded.latestRejectionLocation,
          rejectedAtNanos: typeof degraded.latestRejectedAtNanos,
        }).toStrictEqual({
          rejectedItemCount: 1n,
          failure: {
            _tag: "AdapterFailure",
            failure: lifecycleExpectations(driver, lifecycle).rejectionFailure("stream"),
          },
          location: expectedRejectionLocation(driver, lifecycle, "primary", 11n),
          rejectedAtNanos: "bigint",
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("does not roll back an applied delivery when settlement fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        const settled = yield* Deferred.make<void>();
        yield* driver.transport.command({
          _tag: "Delivery",
          target: lifecycleTarget(driver, lifecycle),
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "retained", region: "eu", value: "committed" },
            },
          ],
          settle: () =>
            Deferred.succeed(settled, undefined).pipe(
              Effect.andThen(Effect.fail("planned settlement failure")),
            ),
        });
        yield* Deferred.await(settled);
        yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        const rowId = yield* expectedRowId(driver, lifecycle, "primary", "retained");
        expect(yield* rows(opened.runtime, opened.route)).toStrictEqual([rowId]);
        const health = yield* opened.awaitStatus("Ready");
        expect(health.failedSettlementCount).toBe(1n);
        yield* opened.close;
      }),
    ),
  );

  it.effect("awaits a sibling finalizer before reacquiring and finalizes exactly once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        const opened = yield* openRuntime(driver, definitions.singleRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "BlockNextFinalizer",
          target: lifecycleTarget(driver, lifecycle),
        });
        yield* driver.transport.command({
          _tag: "FailLane",
          target: lifecycleTarget(driver, lifecycle),
          phase: "stream",
        });
        yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.finalizerStarted,
        );
        expect(
          (yield* driver.transport.observe(lifecycleTarget(driver, lifecycle))).acquisitions,
        ).toBe(baseline.acquisitions + 1n);
        yield* driver.transport.command({
          _tag: "ReleaseFinalizer",
          target: lifecycleTarget(driver, lifecycle),
        });
        yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        yield* opened.close;
        const closed = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        expect(closed.finalizations).toBe(closed.acquisitions);
        yield* opened.close;
        expect(
          (yield* driver.transport.observe(lifecycleTarget(driver, lifecycle))).finalizations,
        ).toBe(closed.finalizations);
      }),
    ),
  );

  it.effect("treats unexpected lane completion as attempt failure and stops its sibling", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const baseline = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        const opened = yield* openRuntime(driver, definitions.singleRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "BlockNextFinalizer",
          target: lifecycleTarget(driver, lifecycle),
        });
        yield* driver.transport.command({
          _tag: "CompleteLane",
          target: lifecycleTarget(driver, lifecycle),
        });
        yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.finalizerStarted,
        );
        expect(
          (yield* driver.transport.observe(lifecycleTarget(driver, lifecycle))).acquisitions,
        ).toBe(baseline.acquisitions + 1n);
        yield* driver.transport.command({
          _tag: "ReleaseFinalizer",
          target: lifecycleTarget(driver, lifecycle),
        });
        yield* awaitObservation(
          driver,
          lifecycleTarget(driver, lifecycle),
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        expect((yield* opened.awaitStatus("Ready")).attempt).toBe(2n);
        yield* opened.close;
        const closed = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        expect(closed.finalizations).toBe(closed.acquisitions);
      }),
    ),
  );

  it.effect("preserves retry timing and exposes exact retry state", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const retryStartedAtNanos = yield* Clock.currentTimeNanos;
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "FailLane",
          target: lifecycleTarget(driver, lifecycle),
          phase: "stream",
        });
        const waiting = yield* opened.awaitStatus("WaitingToRetry");
        expect(waiting.lastExecutionFailure).toStrictEqual({
          _tag: "AdapterFailure",
          failure: lifecycleExpectations(driver, lifecycle).streamFailure,
        });
        const before = yield* driver.transport.observe(lifecycleTarget(driver, lifecycle));
        yield* TestClock.adjust("999 millis");
        expect(
          (yield* driver.transport.observe(lifecycleTarget(driver, lifecycle))).acquisitions,
        ).toBe(before.acquisitions);
        yield* TestClock.adjust("1 millis");
        const recovered = yield* opened.awaitStatus("Ready");
        expect({
          attempt: recovered.attempt,
          retryAtNanos: waiting.retryAtNanos,
        }).toStrictEqual({
          attempt: 2n,
          retryAtNanos: retryStartedAtNanos + 1_000_000_000n,
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("samples metrics for the lifetime and exhausts on invalid samples", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.source);
        const initial = yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "updated" });
        yield* TestClock.adjust("999 millis");
        const before = yield* opened.awaitStatus("Ready");
        expect(before.adapterMetrics).toStrictEqual(initial.adapterMetrics);
        yield* TestClock.adjust("1 millis");
        const sampled = yield* opened.awaitStatus("Ready");
        expect(sampled.adapterMetrics).toStrictEqual(
          lifecycleExpectations(driver, lifecycle).updatedMetrics,
        );
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "invalid" });
        const exhausted = yield* opened.awaitStatus("Exhausted").pipe(Effect.forkChild);
        for (let attempt = 0; attempt < 4; attempt++) {
          yield* TestClock.adjust("1 second");
        }
        const health = yield* Fiber.join(exhausted);
        expect(health.statusTag).toBe("Exhausted");
        expect(health.lastRuntimeFailureTag).toBe("InvalidSourceMetrics");
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "reset" });
        yield* opened.close;
      }),
    ),
  );
};

const registerMandatoryLifecycleConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
  lifecycle: SourceAdapterConformanceLifecycle,
): void => {
  it.effect("shared invariant: acquisition failures retry without leaking resources", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        yield* driver.transport.command({
          _tag: "FailNextAcquisition",
          target,
          phase: "acquire",
          afterFirstResource: true,
        });
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        const waiting = yield* opened.awaitStatus("WaitingToRetry");
        expect(waiting.lastExecutionFailure).toStrictEqual({
          _tag: "AdapterFailure",
          failure: lifecycleExpectations(driver, lifecycle).acquisitionFailure,
        });
        const failed = yield* driver.transport.observe(target);
        expect({
          acquisitions: failed.acquisitions,
          finalizations: failed.finalizations,
          partialAcquisitionFinalizations: failed.partialAcquisitionFinalizations,
        }).toStrictEqual({
          acquisitions: baseline.acquisitions,
          finalizations: baseline.finalizations,
          partialAcquisitionFinalizations:
            baseline.partialAcquisitionFinalizations +
            lifecycleExpectations(driver, lifecycle).partialAcquisitionFinalizationCount,
        });
        yield* TestClock.adjust("1 second");
        yield* opened.awaitStatus("Ready");
        expect((yield* driver.transport.observe(target)).acquisitions).toBe(
          baseline.acquisitions + 1n,
        );
        yield* opened.close;
        const closed = yield* awaitObservation(
          driver,
          target,
          (candidate) => candidate.finalizations === baseline.finalizations + 1n,
        );
        expect(closed.acquisitions).toBe(closed.finalizations);
      }),
    ),
  );

  it.effect("shared invariant: Degraded rejection state remains sticky after retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "Reject",
          target,
          phase: "stream",
          offset: 1n,
        });
        const degraded = yield* opened.awaitStatus("Degraded");
        expect({
          rejectedItemCount: degraded.rejectedItemCount,
          latestRejectionFailure: degraded.latestRejectionFailure,
          latestRejectionLocation: degraded.latestRejectionLocation,
        }).toStrictEqual({
          rejectedItemCount: 1n,
          latestRejectionFailure: {
            _tag: "AdapterFailure",
            failure: lifecycleExpectations(driver, lifecycle).rejectionFailure("stream"),
          },
          latestRejectionLocation: expectedRejectionLocation(driver, lifecycle, "primary", 1n),
        });
        yield* driver.transport.command({
          _tag: "FailLane",
          target,
          phase: "stream",
        });
        yield* opened.awaitStatus("WaitingToRetry");
        yield* TestClock.adjust("1 second");
        const recovered = yield* opened.awaitStatus("Degraded");
        expect({
          attempt: recovered.attempt,
          rejectedItemCount: recovered.rejectedItemCount,
          latestRejectionFailure: recovered.latestRejectionFailure,
          latestRejectionLocation: recovered.latestRejectionLocation,
        }).toStrictEqual({
          attempt: 2n,
          rejectedItemCount: 1n,
          latestRejectionFailure: {
            _tag: "AdapterFailure",
            failure: lifecycleExpectations(driver, lifecycle).rejectionFailure("stream"),
          },
          latestRejectionLocation: expectedRejectionLocation(driver, lifecycle, "primary", 1n),
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("shared invariant: retry waits for finalization and finalizes exactly once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        const opened = yield* openRuntime(driver, definitions.singleRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({ _tag: "BlockNextFinalizer", target });
        yield* driver.transport.command({ _tag: "FailLane", target, phase: "stream" });
        yield* awaitObservation(driver, target, (candidate) => candidate.finalizerStarted);
        expect((yield* driver.transport.observe(target)).acquisitions).toBe(
          baseline.acquisitions + 1n,
        );
        yield* driver.transport.command({ _tag: "ReleaseFinalizer", target });
        yield* awaitObservation(
          driver,
          target,
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        yield* opened.close;
        const closed = yield* awaitObservation(
          driver,
          target,
          (candidate) => candidate.finalizations === baseline.finalizations + 2n,
        );
        expect(closed.finalizations).toBe(closed.acquisitions);
        yield* opened.close;
        expect((yield* driver.transport.observe(target)).finalizations).toBe(closed.finalizations);
      }),
    ),
  );

  it.effect("shared invariant: unexpected completion is supervised as attempt failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        const opened = yield* openRuntime(driver, definitions.singleRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({ _tag: "CompleteLane", target });
        yield* awaitObservation(
          driver,
          target,
          (candidate) => candidate.acquisitions === baseline.acquisitions + 2n,
        );
        expect((yield* opened.awaitStatus("Ready")).attempt).toBe(2n);
        yield* driver.transport.command({ _tag: "CompleteLane", target });
        expect((yield* opened.awaitStatus("Exhausted")).statusTag).toBe("Exhausted");
        yield* opened.close;
        const closed = yield* awaitObservation(
          driver,
          target,
          (candidate) => candidate.finalizations === baseline.finalizations + 2n,
        );
        expect(closed.acquisitions).toBe(closed.finalizations);
      }),
    ),
  );

  it.effect("shared invariant: retry timing and retry health are exact", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const retryStartedAtNanos = yield* Clock.currentTimeNanos;
        const opened = yield* openRuntime(driver, definitions.delayedRetrySource);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({ _tag: "FailLane", target, phase: "stream" });
        const waiting = yield* opened.awaitStatus("WaitingToRetry");
        expect(waiting.lastExecutionFailure).toStrictEqual({
          _tag: "AdapterFailure",
          failure: lifecycleExpectations(driver, lifecycle).streamFailure,
        });
        const before = yield* driver.transport.observe(target);
        yield* TestClock.adjust("999 millis");
        expect((yield* driver.transport.observe(target)).acquisitions).toBe(before.acquisitions);
        yield* TestClock.adjust("1 millis");
        const recovered = yield* opened.awaitStatus("Ready");
        expect({
          attempt: recovered.attempt,
          retryAtNanos: waiting.retryAtNanos,
        }).toStrictEqual({
          attempt: 2n,
          retryAtNanos: retryStartedAtNanos + 1_000_000_000n,
        });
        yield* opened.close;
      }),
    ),
  );

  it.effect("shared invariant: adapter metrics publish exactly on the core cadence", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const opened = yield* openRuntime(driver, definitions.source);
        const initial = yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "updated" });
        yield* Effect.yieldNow;
        yield* TestClock.adjust("999 millis");
        const before = yield* opened.awaitStatus("Ready");
        expect(before.adapterMetrics).toStrictEqual(initial.adapterMetrics);
        yield* TestClock.adjust("1 millis");
        const sampled = yield* opened.awaitStatus("Ready");
        expect(sampled.adapterMetrics).toStrictEqual(
          lifecycleExpectations(driver, lifecycle).updatedMetrics,
        );
        yield* driver.transport.command({ _tag: "SetMetrics", sample: "reset" });
        yield* opened.close;
      }),
    ),
  );
};

const registerContinuousUpsertLifecycleConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
  lifecycle: SourceAdapterConformanceLifecycle,
): void => {
  it.effect(`${lifecycle} applies upserts, records rejection, and continues`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definitions = requireLifecycle(driver, lifecycle);
        const target = lifecycleTarget(driver, lifecycle);
        const baseline = yield* driver.transport.observe(target);
        const opened = yield* openRuntime(driver, definitions.source);
        yield* opened.awaitStatus("Ready");
        yield* driver.transport.command({
          _tag: "Delivery",
          target,
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "before-rejection", region: "eu", value: "valid" },
            },
          ],
        });
        expect(yield* awaitRows(opened.runtime, opened.route, ["before-rejection"])).toStrictEqual([
          "before-rejection",
        ]);
        yield* driver.transport.command({
          _tag: "Reject",
          target,
          phase: "stream",
          offset: 2n,
        });
        const degraded = yield* opened.awaitStatus("Degraded");
        expect({
          rejectedItemCount: degraded.rejectedItemCount,
          failure: degraded.latestRejectionFailure,
          location: degraded.latestRejectionLocation,
        }).toStrictEqual({
          rejectedItemCount: 1n,
          failure: {
            _tag: "AdapterFailure",
            failure: lifecycleExpectations(driver, lifecycle).rejectionFailure("stream"),
          },
          location: expectedRejectionLocation(driver, lifecycle, "primary", 2n),
        });
        yield* driver.transport.command({
          _tag: "Delivery",
          target,
          mutations: [
            {
              _tag: "Upsert",
              row: { id: "after-rejection", region: "eu", value: "valid" },
            },
          ],
        });
        expect(
          yield* awaitRows(opened.runtime, opened.route, ["after-rejection", "before-rejection"]),
        ).toStrictEqual(["after-rejection", "before-rejection"]);
        yield* opened.close;
        const closed = yield* awaitObservation(
          driver,
          target,
          (observation) => observation.finalizations === baseline.finalizations + 1n,
        );
        expect(closed.acquisitions).toBe(closed.finalizations);
      }),
    ),
  );
};

const registerLeasedSharingConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
): void => {
  it.effect("keeps diagnostics non-owning and shares only exact leased routes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definition = requireLeased(driver);
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: SourceAdapterConformanceConfigRow,
              source: definition.source,
            },
          },
        });
        const context = yield* driver.runtimeContext;
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideContext(context),
        );
        const closeRuntime = yield* Effect.cached(runtime.close);
        yield* Scope.addFinalizer(yield* Effect.scope, closeRuntime.pipe(Effect.orDie));
        const sameBaseline = yield* driver.transport.observe(leasedTarget(definition.sameRoute));
        const distinctBaseline = yield* driver.transport.observe(
          leasedTarget(definition.distinctRoute),
        );
        const diagnostics = yield* openHealth(runtime.liveClient, definition.sameRoute);
        expect(
          Option.getOrThrow(yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead))
            .statusTag,
        ).toBe("Inactive");
        expect(
          (yield* driver.transport.observe(leasedTarget(definition.sameRoute))).acquisitions,
        ).toBe(sameBaseline.acquisitions);
        const first = yield* openQuery(runtime.liveClient, {
          routeBy: definition.sameRoute,
          select: ["id"],
        });
        const second = yield* openQuery(runtime.liveClient, {
          routeBy: definition.sameRoute,
          select: ["id"],
        });
        const distinct = yield* openQuery(runtime.liveClient, {
          routeBy: definition.distinctRoute,
          select: ["id"],
        });
        expect({
          same: (yield* driver.transport.observe(leasedTarget(definition.sameRoute))).acquisitions,
          distinct: (yield* driver.transport.observe(leasedTarget(definition.distinctRoute)))
            .acquisitions,
        }).toStrictEqual({
          same: sameBaseline.acquisitions + 1n,
          distinct: distinctBaseline.acquisitions + 1n,
        });
        yield* first.close();
        expect(
          (yield* driver.transport.observe(leasedTarget(definition.sameRoute))).finalizations,
        ).toBe(sameBaseline.finalizations);
        yield* diagnostics.close();
        expect(
          (yield* driver.transport.observe(leasedTarget(definition.sameRoute))).finalizations,
        ).toBe(sameBaseline.finalizations);
        yield* second.close();
        yield* distinct.close();
        expect({
          same: (yield* driver.transport.observe(leasedTarget(definition.sameRoute))).finalizations,
          distinct: (yield* driver.transport.observe(leasedTarget(definition.distinctRoute)))
            .finalizations,
        }).toStrictEqual({
          same: sameBaseline.finalizations + 1n,
          distinct: distinctBaseline.finalizations + 1n,
        });
        yield* closeRuntime;
      }),
    ),
  );
};

const registerLeasedCompleteDeliveryConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
): void => {
  it.effect("settles route-incongruent leased deliveries with the application failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = yield* SourceAdapterConformanceDriver;
        const definition = requireLeased(driver);
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: SourceAdapterConformanceConfigRow,
              source: definition.source,
            },
          },
        });
        const context = yield* driver.runtimeContext;
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideContext(context),
        );
        yield* Scope.addFinalizer(yield* Effect.scope, runtime.close.pipe(Effect.orDie));
        const subscription = yield* openQuery(runtime.liveClient, {
          routeBy: definition.sameRoute,
          select: ["id"],
        });
        const settled = yield* Deferred.make<Exit.Exit<void, unknown>>();
        yield* driver.transport.command({
          _tag: "CorruptLaterMutation",
          target: leasedTarget(definition.sameRoute),
          firstRow: { id: "first", region: "eu", value: "valid" },
          laterRow: { id: "second", region: "eu", value: "invalid" },
          field: "region",
          value: "different",
          settle: (exit) => Deferred.succeed(settled, exit).pipe(Effect.asVoid),
        });
        const applicationExit = yield* Deferred.await(settled);
        expect(applicationExit._tag).toBe("Failure");
        expect(Exit.findErrorOption(applicationExit)).toStrictEqual(
          Option.some({
            _tag: "InvalidFeedRoute",
            message: "Source Topic rows row does not match the acquired Feed Route.",
            topic: "rows",
          }),
        );
        yield* subscription.close();
      }),
    ),
  );
};

const registerSelectedLifecycleConformance = (
  it: Vitest.MethodsNonLive<SourceAdapterConformanceDriver>,
  lifecycle: SourceAdapterConformanceLifecycle,
  eventModel: SourceAdapterConformanceEventModel,
): void => {
  registerMandatoryLifecycleConformance(it, lifecycle);
  if (eventModel === "continuous-upserts") {
    registerContinuousUpsertLifecycleConformance(it, lifecycle);
  } else {
    registerLifecycleConformance(it, lifecycle);
    if (lifecycle === "leased") {
      registerLeasedCompleteDeliveryConformance(it);
    }
  }
  if (lifecycle === "leased") {
    registerLeasedSharingConformance(it);
  }
};

export const registerSourceAdapterConformance = (
  options: SourceAdapterConformanceOptions,
): void => {
  if (
    options.eventModel !== undefined &&
    options.eventModel !== "complete-deliveries" &&
    options.eventModel !== "continuous-upserts"
  ) {
    throw new TypeError("Source Adapter conformance requires a supported transport event model.");
  }
  if (options.callbackBridge === true && options.materialized !== true) {
    throw new TypeError(
      "Source Adapter callback conformance requires materialized lifecycle conformance.",
    );
  }
  if (options.materialized !== true && options.leased !== true) {
    throw new TypeError(
      "Source Adapter conformance requires at least one enabled lifecycle capability.",
    );
  }
  vitestLayer(options.layer)(options.name, (it) => {
    const eventModel = options.eventModel ?? "complete-deliveries";
    if (options.materialized === true) {
      registerSelectedLifecycleConformance(it, "materialized", eventModel);
    }
    if (options.leased === true) {
      registerSelectedLifecycleConformance(it, "leased", eventModel);
    }
    if (options.callbackBridge === true) {
      it.effect("uses the adapter's actual callback bridge", () =>
        Effect.scoped(
          Effect.gen(function* () {
            const driver = yield* SourceAdapterConformanceDriver;
            const callbackBridge = requireCallbackBridge(driver);
            const opened = yield* openRuntime(driver, callbackBridge.source);
            yield* opened.awaitStatus("Ready");
            yield* callbackBridge.emitBackpressurable({
              id: "backpressurable",
              region: "eu",
              value: "callback",
            });
            yield* callbackBridge.emitNonPausable({
              id: "non-pausable",
              region: "eu",
              value: "callback",
            });
            const backpressurableRowId = yield* expectedRowId(
              driver,
              "materialized",
              "primary",
              "backpressurable",
            );
            const nonPausableRowId = yield* expectedRowId(
              driver,
              "materialized",
              "primary",
              "non-pausable",
            );
            expect(yield* rows(opened.runtime, opened.route)).toStrictEqual(
              [backpressurableRowId, nonPausableRowId].toSorted(),
            );
            yield* opened.close;
            const observation = yield* driver.transport.observe(materializedTarget());
            expect(observation.registrations).toBe(2n);
            expect(observation.callbackFinalizations).toBe(2n);
          }),
        ),
      );
      it.effect("enforces pressure and overflow through the actual callback bridge", () =>
        Effect.scoped(
          Effect.gen(function* () {
            const driver = yield* SourceAdapterConformanceDriver;
            const callbackBridge = requireCallbackBridge(driver);
            const baseline = yield* driver.transport.observe(materializedTarget());
            yield* callbackBridge.pauseNextConsumer;
            const opened = yield* openRuntime(driver, callbackBridge.source);
            yield* opened.awaitStatus("Ready");
            yield* callbackBridge.offerBackpressurable({
              id: "backpressurable-first",
              region: "eu",
              value: "callback",
            });
            const blocked = yield* callbackBridge
              .offerBackpressurable({
                id: "backpressurable-second",
                region: "eu",
                value: "callback",
              })
              .pipe(Effect.forkChild);
            yield* Effect.yieldNow;
            expect(blocked.pollUnsafe()).toBeUndefined();
            yield* callbackBridge.offerNonPausable({
              id: "non-pausable-first",
              region: "eu",
              value: "callback",
            });
            yield* callbackBridge.offerNonPausable({
              id: "non-pausable-overflow",
              region: "eu",
              value: "callback",
            });
            yield* TestClock.adjust("1 second");
            const health = yield* opened.awaitStatus("Ready");
            expect({
              capacity: callbackBridge.capacity,
              highWaterMark: health.bufferHighWaterMark,
              overflowCount: health.bufferOverflowCount,
            }).toStrictEqual({
              capacity: 1,
              highWaterMark: 1,
              overflowCount: 1n,
            });
            yield* callbackBridge.releaseConsumer;
            yield* Fiber.join(blocked);
            yield* opened.close;
            const closed = yield* driver.transport.observe(materializedTarget());
            expect({
              registrations: closed.registrations,
              callbackFinalizations: closed.callbackFinalizations,
            }).toStrictEqual({
              registrations: baseline.registrations + 2n,
              callbackFinalizations: baseline.callbackFinalizations + 2n,
            });
          }),
        ),
      );
    }
  });
};

const checkSourceAdapterPackageConformance = makeSourceAdapterPackageConformanceCheck(
  inspectSourceAdapterPackageConformance,
  validateSourceAdapterPackageConformance,
);

export const registerSourceAdapterPackageConformance: (
  options: SourceAdapterPackageConformanceOptions,
) => void = makeSourceAdapterPackageConformanceRegistrar(
  vitestIt.effect,
  registerSourceAdapterConformance,
  checkSourceAdapterPackageConformance,
);

export const SourceAdapterConformanceHost = {
  register: registerSourceAdapterConformance,
  registerPackage: registerSourceAdapterPackageConformance,
} as const;
