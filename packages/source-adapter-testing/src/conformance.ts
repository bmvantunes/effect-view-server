import { expect, layer as vitestLayer } from "@effect/vitest";
import type { SourceApplicationExit } from "@effect-view-server/source-adapter";
import { Cause, Context, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";
import { TestClock } from "effect/testing";

export type SourceAdapterConformanceTermination =
  | {
      readonly _tag: "AdapterFailure";
      readonly phase: "acquire" | "stream" | "settlement";
    }
  | {
      readonly _tag: "RuntimeFailure";
      readonly failure:
        | "InvalidSourceDefinition"
        | "InvalidSourceDelivery"
        | "InvalidTopicRow"
        | "InvalidCanonicalId"
        | "InvalidFeedRoute"
        | "InvalidSourceMetrics"
        | "SourceBufferOverflow";
    }
  | {
      readonly _tag: "UnexpectedCompletion";
    };

export type SourceAdapterConformanceMaterializedSnapshot = {
  readonly rows: ReadonlyArray<string>;
  readonly mutationOrder: ReadonlyArray<"Upsert" | "Delete">;
  readonly settlementExits: ReadonlyArray<SourceApplicationExit>;
  readonly status:
    | "Starting"
    | "Ready"
    | "Degraded"
    | "WaitingToRetry"
    | "Reacquiring"
    | "Exhausted"
    | "Stopping";
  readonly rejectionStatusAtSettlement: "Degraded" | null;
  readonly rejectedItemCount: bigint;
  readonly acquisitions: bigint;
  readonly finalizations: bigint;
  readonly adapterMetric: bigint;
  readonly metricReads: bigint;
  readonly failedSettlementCount: bigint;
  readonly lastTermination: SourceAdapterConformanceTermination | null;
  readonly latestRejection: {
    readonly failure: SourceAdapterConformanceTermination;
    readonly safeLocationMatched: boolean;
    readonly rejectedAtNanos: bigint;
    readonly rawPayloadPresent: boolean;
  } | null;
};

export type SourceAdapterConformanceRetryProbe = {
  readonly waiting: {
    readonly termination: SourceAdapterConformanceTermination;
    readonly retryAtNanos: bigint;
  };
  readonly acquisitionsBeforeDelay: bigint;
  readonly acquisitionsBeforeBoundary: bigint;
  readonly reacquiring: {
    readonly previousTermination: SourceAdapterConformanceTermination;
    readonly attempt: bigint;
  };
  readonly recoveredStatus: "Ready" | "Degraded";
  readonly acquisitionsAfterBoundary: bigint;
};

export type SourceAdapterConformanceMaterializedSession = {
  readonly emitOrderedDelivery: Effect.Effect<void, unknown>;
  readonly emitConcurrentSiblingDeliveries: Effect.Effect<void, unknown>;
  readonly emitRejectedItemThenUpsert: Effect.Effect<void, unknown>;
  readonly failCurrentAttempt: Effect.Effect<void, unknown>;
  readonly completeCurrentAttempt: Effect.Effect<void, unknown>;
  readonly failNextAcquisition: Effect.Effect<void, unknown>;
  readonly failDeliverySettlement: Effect.Effect<void, unknown>;
  readonly failRejectionSettlement: Effect.Effect<void, unknown>;
  readonly exhaustAttempts: Effect.Effect<void, unknown>;
  readonly invalidateMetrics: Effect.Effect<void, unknown>;
  readonly exerciseApplicationExits: Effect.Effect<ReadonlyArray<SourceApplicationExit>, unknown>;
  readonly openFinalizationProbe: Effect.Effect<
    SourceAdapterConformanceFinalizationProbe,
    unknown,
    Scope.Scope
  >;
  readonly validateAttemptBoundaries: Effect.Effect<
    SourceAdapterConformanceAttemptValidation,
    unknown,
    Scope.Scope
  >;
  readonly exerciseDelayedRetry: Effect.Effect<SourceAdapterConformanceRetryProbe, unknown>;
  readonly updateAdapterMetric: (value: bigint) => Effect.Effect<void, unknown>;
  readonly awaitAcquisitions: (expected: bigint) => Effect.Effect<void, unknown>;
  readonly awaitStatus: (
    expected: SourceAdapterConformanceMaterializedSnapshot["status"],
  ) => Effect.Effect<void, unknown>;
  readonly inspect: Effect.Effect<SourceAdapterConformanceMaterializedSnapshot, unknown>;
  readonly close: Effect.Effect<void, unknown>;
};

export type SourceAdapterConformanceAttemptValidation = {
  readonly emptyLanesRejected: boolean;
  readonly emptyLaneIdRejected: boolean;
  readonly duplicateLaneIdsRejected: boolean;
  readonly changingLaneIdsRejected: boolean;
  readonly missingBufferMetricsRejected: boolean;
  readonly invalidTransportRowRejected: boolean;
};

export type SourceAdapterConformanceFinalizationProbe = {
  readonly interrupt: Effect.Effect<void, unknown, never>;
  readonly finalizerStarted: Effect.Effect<void, unknown, never>;
  readonly releaseFinalizer: Effect.Effect<void, unknown, never>;
  readonly closeAgain: Effect.Effect<void, unknown, never>;
  readonly finalizationCount: Effect.Effect<bigint, unknown, never>;
};

export type SourceAdapterConformanceDiagnostics = {
  readonly latest: Effect.Effect<"Inactive" | "Active", unknown>;
  readonly close: Effect.Effect<void, unknown>;
};

export type SourceAdapterConformanceLease = {
  readonly close: Effect.Effect<void, unknown>;
};

export type SourceAdapterConformanceLeasedSnapshot = {
  readonly acquisitions: bigint;
  readonly finalizations: bigint;
  readonly active: boolean;
  readonly rows: ReadonlyArray<string>;
  readonly settlementExits: ReadonlyArray<SourceApplicationExit>;
};

export type SourceAdapterConformanceLeasedSession = {
  readonly sameRoute: string;
  readonly distinctRoute: string;
  readonly diagnostics: (
    route: string,
  ) => Effect.Effect<SourceAdapterConformanceDiagnostics, unknown>;
  readonly subscribe: (route: string) => Effect.Effect<SourceAdapterConformanceLease, unknown>;
  readonly seed: (route: string, id: string) => Effect.Effect<void, unknown>;
  readonly emitRouteIncongruentDelivery: (route: string) => Effect.Effect<void, unknown>;
  readonly inspect: (
    route: string,
  ) => Effect.Effect<SourceAdapterConformanceLeasedSnapshot, unknown>;
};

export type SourceAdapterConformanceCallbackBufferSnapshot = {
  readonly capacity: number;
  readonly backpressurableBlockedAtCapacity: boolean;
  readonly backpressurableDeliveryOrder: readonly ["first", "second"];
  readonly backpressurableHighWaterMark: number;
  readonly nonPausableFailure: "SourceBufferOverflow";
  readonly nonPausableOverflowCount: bigint;
  readonly nonPausableHighWaterMark: number;
  readonly registrationCount: bigint;
  readonly finalizationCount: bigint;
};

export type SourceAdapterConformanceSubjectValue = {
  readonly openMaterialized?: Effect.Effect<
    SourceAdapterConformanceMaterializedSession,
    unknown,
    Scope.Scope
  >;
  readonly openLeased?: Effect.Effect<SourceAdapterConformanceLeasedSession, unknown, Scope.Scope>;
  readonly exerciseCallbackBuffer?: Effect.Effect<
    SourceAdapterConformanceCallbackBufferSnapshot,
    unknown,
    Scope.Scope
  >;
};

export class SourceAdapterConformanceSubject extends Context.Service<
  SourceAdapterConformanceSubject,
  SourceAdapterConformanceSubjectValue
>()("@effect-view-server/source-adapter-testing/ConformanceSubject") {}

export type SourceAdapterConformanceSuiteOptions = {
  readonly name: string;
  readonly layer: Layer.Layer<SourceAdapterConformanceSubject, unknown>;
  readonly materialized?: boolean;
  readonly leased?: boolean;
  readonly callbackBuffer?: boolean;
};

export const conformanceMaterializedSession = Effect.fn(
  "SourceAdapterTesting.conformance.requireMaterialized",
)(function* () {
  const subject = yield* SourceAdapterConformanceSubject;
  if (subject.openMaterialized === undefined) {
    return yield* Effect.die(
      new Error("The conformance subject did not provide a Materialized session."),
    );
  }
  return yield* subject.openMaterialized;
});

export const conformanceLeasedSession = Effect.fn("SourceAdapterTesting.conformance.requireLeased")(
  function* () {
    const subject = yield* SourceAdapterConformanceSubject;
    if (subject.openLeased === undefined) {
      return yield* Effect.die(
        new Error("The conformance subject did not provide a Leased session."),
      );
    }
    return yield* subject.openLeased;
  },
);

export const conformanceCallbackBuffer = Effect.fn(
  "SourceAdapterTesting.conformance.requireCallbackBuffer",
)(function* () {
  const subject = yield* SourceAdapterConformanceSubject;
  if (subject.exerciseCallbackBuffer === undefined) {
    return yield* Effect.die(
      new Error("The conformance subject did not provide a callback-buffer exercise."),
    );
  }
  return yield* subject.exerciseCallbackBuffer;
});

export const registerSourceAdapterConformance = (
  options: SourceAdapterConformanceSuiteOptions,
): void => {
  vitestLayer(options.layer)(options.name, (it) => {
    if (options.materialized === true) {
      it.effect("acquires ready and applies lane-local Delivery mutations in order", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          const initial = yield* session.inspect;
          expect({
            status: initial.status,
            acquisitions: initial.acquisitions,
            finalizations: initial.finalizations,
          }).toStrictEqual({
            status: "Ready",
            acquisitions: 1n,
            finalizations: 0n,
          });

          yield* session.emitOrderedDelivery;
          const snapshot = yield* session.inspect;
          expect({
            rows: snapshot.rows,
            mutationOrder: snapshot.mutationOrder,
          }).toStrictEqual({
            rows: [],
            mutationOrder: ["Upsert", "Delete"],
          });
          expect(snapshot.settlementExits).toHaveLength(1);
          expect(snapshot.settlementExits[0]).toStrictEqual(Exit.void);
          yield* session.close;
        }),
      );

      it.effect("runs sibling lanes concurrently while retaining lane-local sequencing", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          yield* session.emitConcurrentSiblingDeliveries;
          expect((yield* session.inspect).rows).toStrictEqual(["primary", "sibling"]);
          yield* session.close;
        }),
      );

      it.effect("rejects invalid lane collections, identities, and buffer metrics", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          expect(yield* session.validateAttemptBoundaries).toStrictEqual({
            emptyLanesRejected: true,
            emptyLaneIdRejected: true,
            duplicateLaneIdsRejected: true,
            changingLaneIdsRejected: true,
            missingBufferMetricsRejected: true,
            invalidTransportRowRejected: true,
          });
          yield* session.close;
        }),
      );

      it.effect("settles every application Exit exactly once", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          const exits = yield* session.exerciseApplicationExits;

          expect(exits.map((exit) => exit._tag)).toStrictEqual([
            "Success",
            "Failure",
            "Failure",
            "Failure",
          ]);
          const failureCause = Option.getOrThrow(
            Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(exits[1]))),
          );
          const defectCause = Option.getOrThrow(
            Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(exits[2]))),
          );
          const interruptionCause = Option.getOrThrow(
            Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(exits[3]))),
          );
          expect(Cause.findErrorOption(failureCause)).toStrictEqual(
            Option.some({
              _tag: "InvalidSourceDelivery",
              message: "Conformance application failure.",
            }),
          );
          expect(defectCause.reasons.find(Cause.isDieReason)?.defect).toBe(
            "conformance application defect",
          );
          expect(Cause.hasInterrupts(interruptionCause)).toBe(true);
          yield* session.close;
        }),
      );

      it.effect("interrupts attempts and awaits one idempotent finalizer", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          const probe = yield* session.openFinalizationProbe;
          let interruptCompleted = false;
          const interruptFiber = yield* probe.interrupt.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                interruptCompleted = true;
              }),
            ),
            Effect.forkChild,
          );
          yield* probe.finalizerStarted;

          yield* Effect.yieldNow;
          expect(interruptCompleted).toBe(false);
          expect(yield* probe.finalizationCount).toBe(1n);

          yield* probe.releaseFinalizer;
          yield* Fiber.join(interruptFiber);
          yield* probe.closeAgain;
          yield* probe.closeAgain;
          expect(yield* probe.finalizationCount).toBe(1n);
          yield* session.close;
        }),
      );

      it.effect(
        "records rejection before settlement, continues, and keeps Degraded sticky across retries",
        () =>
          Effect.gen(function* () {
            const session = yield* conformanceMaterializedSession();
            yield* session.emitRejectedItemThenUpsert;
            const rejected = yield* session.inspect;
            expect({
              rows: rejected.rows,
              status: rejected.status,
              rejectionStatusAtSettlement: rejected.rejectionStatusAtSettlement,
              rejectedItemCount: rejected.rejectedItemCount,
              latestRejection: rejected.latestRejection,
            }).toStrictEqual({
              rows: ["after-rejection"],
              status: "Degraded",
              rejectionStatusAtSettlement: "Degraded",
              rejectedItemCount: 1n,
              latestRejection: {
                failure: {
                  _tag: "AdapterFailure",
                  phase: "stream",
                },
                safeLocationMatched: true,
                rejectedAtNanos: 0n,
                rawPayloadPresent: false,
              },
            });

            yield* session.failCurrentAttempt;
            yield* session.awaitAcquisitions(2n);
            expect((yield* session.inspect).status).toBe("Degraded");

            yield* session.completeCurrentAttempt;
            yield* session.awaitAcquisitions(3n);
            expect((yield* session.inspect).status).toBe("Degraded");

            yield* session.close;
            const closed = yield* session.inspect;
            expect(closed.finalizations).toBe(closed.acquisitions);
          }),
      );

      it.effect("retries exact acquisition failures without overlapping attempts", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          yield* session.failNextAcquisition;
          const recovered = yield* session.inspect;
          expect({
            status: recovered.status,
            acquisitions: recovered.acquisitions,
            finalizations: recovered.finalizations,
          }).toStrictEqual({
            status: "Ready",
            acquisitions: 2n,
            finalizations: 1n,
          });
          yield* session.close;
        }),
      );

      it.effect("retains exact retry state and reacquires only after the scheduled delay", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          const probe = yield* session.exerciseDelayedRetry;
          expect(probe).toStrictEqual({
            waiting: {
              termination: {
                _tag: "AdapterFailure",
                phase: "stream",
              },
              retryAtNanos: 1_000_000_000n,
            },
            acquisitionsBeforeDelay: 1n,
            acquisitionsBeforeBoundary: 1n,
            reacquiring: {
              previousTermination: {
                _tag: "AdapterFailure",
                phase: "stream",
              },
              attempt: 2n,
            },
            recoveredStatus: "Ready",
            acquisitionsAfterBoundary: 2n,
          });
          yield* session.close;
        }),
      );

      it.effect("terminates and reacquires the whole attempt on Delivery settlement failure", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          yield* session.failDeliverySettlement;
          const recovered = yield* session.inspect;
          expect(recovered.settlementExits).toStrictEqual([Exit.void]);
          expect({
            status: recovered.status,
            acquisitions: recovered.acquisitions,
            finalizations: recovered.finalizations,
            failedSettlementCount: recovered.failedSettlementCount,
          }).toStrictEqual({
            status: "Ready",
            acquisitions: 2n,
            finalizations: 1n,
            failedSettlementCount: 1n,
          });
          yield* session.close;
        }),
      );

      it.effect(
        "records rejection before a rejection-settlement failure and reacquires Degraded",
        () =>
          Effect.gen(function* () {
            const session = yield* conformanceMaterializedSession();
            yield* session.failRejectionSettlement;
            const recovered = yield* session.inspect;
            expect(recovered.settlementExits).toStrictEqual([Exit.void]);
            expect({
              status: recovered.status,
              rejectionStatusAtSettlement: recovered.rejectionStatusAtSettlement,
              rejectedItemCount: recovered.rejectedItemCount,
              acquisitions: recovered.acquisitions,
              finalizations: recovered.finalizations,
              failedSettlementCount: recovered.failedSettlementCount,
            }).toStrictEqual({
              status: "Degraded",
              rejectionStatusAtSettlement: "Degraded",
              rejectedItemCount: 1n,
              acquisitions: 2n,
              finalizations: 1n,
              failedSettlementCount: 1n,
            });
            yield* session.close;
          }),
      );

      it.effect("exhausts the declared retry policy after whole-attempt failures", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          yield* session.exhaustAttempts;
          const exhausted = yield* session.inspect;
          expect({
            status: exhausted.status,
            acquisitions: exhausted.acquisitions,
            finalizations: exhausted.finalizations,
            lastTermination: exhausted.lastTermination,
          }).toStrictEqual({
            status: "Exhausted",
            acquisitions: 4n,
            finalizations: 4n,
            lastTermination: {
              _tag: "AdapterFailure",
              phase: "stream",
            },
          });

          const metricReadsAtExhaustion = exhausted.metricReads;
          yield* session.updateAdapterMetric(73n);
          yield* TestClock.adjust("1 second");
          yield* Effect.yieldNow;
          const sampledWhileExhausted = yield* session.inspect;
          expect({
            status: sampledWhileExhausted.status,
            adapterMetric: sampledWhileExhausted.adapterMetric,
            metricReads: sampledWhileExhausted.metricReads,
          }).toStrictEqual({
            status: "Exhausted",
            adapterMetric: 73n,
            metricReads: metricReadsAtExhaustion + 1n,
          });
          yield* session.close;
        }),
      );

      it.effect("samples adapter metrics for the complete logical Source lifetime", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          const initial = yield* session.inspect;
          yield* session.updateAdapterMetric(42n);
          yield* TestClock.adjust("999 millis");
          const beforeCadence = yield* session.inspect;
          expect({
            adapterMetric: beforeCadence.adapterMetric,
            metricReads: beforeCadence.metricReads,
          }).toStrictEqual({
            adapterMetric: initial.adapterMetric,
            metricReads: initial.metricReads,
          });

          yield* TestClock.adjust("1 millis");
          yield* Effect.yieldNow;
          const sampled = yield* session.inspect;
          expect({
            adapterMetric: sampled.adapterMetric,
            metricReads: sampled.metricReads,
          }).toStrictEqual({
            adapterMetric: 42n,
            metricReads: initial.metricReads + 1n,
          });
          yield* session.close;
        }),
      );

      it.effect("terminates every retry attempt when a sampled adapter metric is invalid", () =>
        Effect.gen(function* () {
          const session = yield* conformanceMaterializedSession();
          yield* session.invalidateMetrics;
          const exhaustedFiber = yield* session.awaitStatus("Exhausted").pipe(Effect.forkChild);
          for (let attempt = 1n; attempt <= 4n; attempt++) {
            yield* TestClock.adjust("1 second");
          }
          yield* Fiber.join(exhaustedFiber);
          const exhausted = yield* session.inspect;
          expect({
            status: exhausted.status,
            acquisitions: exhausted.acquisitions,
            finalizations: exhausted.finalizations,
            lastTermination: exhausted.lastTermination,
          }).toStrictEqual({
            status: "Exhausted",
            acquisitions: 1n,
            finalizations: 1n,
            lastTermination: {
              _tag: "RuntimeFailure",
              failure: "InvalidSourceMetrics",
            },
          });
          yield* session.close;
        }),
      );
    }

    if (options.leased === true) {
      it.effect(
        "keeps diagnostics non-owning, shares exact routes, isolates distinct routes, and cleans final releases",
        () =>
          Effect.gen(function* () {
            const session = yield* conformanceLeasedSession();
            const inactiveDiagnostics = yield* session.diagnostics(session.sameRoute);
            expect(yield* inactiveDiagnostics.latest).toBe("Inactive");
            expect((yield* session.inspect(session.sameRoute)).acquisitions).toBe(0n);

            const first = yield* session.subscribe(session.sameRoute);
            const second = yield* session.subscribe(session.sameRoute);
            const distinct = yield* session.subscribe(session.distinctRoute);
            yield* session.seed(session.sameRoute, "same-row");
            yield* session.seed(session.distinctRoute, "distinct-row");
            const activeDiagnostics = yield* session.diagnostics(session.sameRoute);
            expect(yield* activeDiagnostics.latest).toBe("Active");
            expect({
              same: yield* session.inspect(session.sameRoute),
              distinct: yield* session.inspect(session.distinctRoute),
            }).toStrictEqual({
              same: {
                acquisitions: 1n,
                finalizations: 0n,
                active: true,
                rows: ["same-row"],
                settlementExits: [],
              },
              distinct: {
                acquisitions: 1n,
                finalizations: 0n,
                active: true,
                rows: ["distinct-row"],
                settlementExits: [],
              },
            });

            yield* first.close;
            expect((yield* session.inspect(session.sameRoute)).active).toBe(true);
            yield* activeDiagnostics.close;
            expect((yield* session.inspect(session.sameRoute)).active).toBe(true);

            yield* second.close;
            yield* distinct.close;
            expect({
              same: yield* session.inspect(session.sameRoute),
              distinct: yield* session.inspect(session.distinctRoute),
            }).toStrictEqual({
              same: {
                acquisitions: 1n,
                finalizations: 1n,
                active: false,
                rows: [],
                settlementExits: [],
              },
              distinct: {
                acquisitions: 1n,
                finalizations: 1n,
                active: false,
                rows: [],
                settlementExits: [],
              },
            });
          }),
      );

      it.effect(
        "settles route-incongruent Leased rows with the exact application Failure Exit",
        () =>
          Effect.gen(function* () {
            const session = yield* conformanceLeasedSession();
            const lease = yield* session.subscribe(session.sameRoute);
            yield* session.emitRouteIncongruentDelivery(session.sameRoute);
            const snapshot = yield* session.inspect(session.sameRoute);
            expect(snapshot.settlementExits).toHaveLength(1);
            const applicationExit = Option.getOrThrow(
              Option.fromUndefinedOr(snapshot.settlementExits[0]),
            );
            expect(applicationExit._tag).toBe("Failure");
            expect(Exit.findErrorOption(applicationExit)).toStrictEqual(
              Option.some({
                _tag: "InvalidFeedRoute",
                message: "Source Topic rows row does not match the acquired Feed Route.",
                topic: "rows",
              }),
            );
            yield* lease.close;
            const closed = yield* session.inspect(session.sameRoute);
            expect(closed.finalizations).toBe(closed.acquisitions);
          }),
      );
    }

    if (options.callbackBuffer === true) {
      it.effect("bounds callback buffers, preserves pressure, and finalizes registration", () =>
        Effect.gen(function* () {
          const snapshot = yield* conformanceCallbackBuffer();
          expect(snapshot).toStrictEqual({
            capacity: snapshot.capacity,
            backpressurableBlockedAtCapacity: true,
            backpressurableDeliveryOrder: ["first", "second"],
            backpressurableHighWaterMark: snapshot.capacity,
            nonPausableFailure: "SourceBufferOverflow",
            nonPausableOverflowCount: 1n,
            nonPausableHighWaterMark: snapshot.capacity,
            registrationCount: 2n,
            finalizationCount: 2n,
          });
        }),
      );
    }
  });
};

export const SourceAdapterConformance = {
  register: registerSourceAdapterConformance,
  Subject: SourceAdapterConformanceSubject,
} as const;
