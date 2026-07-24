import {
  SourceAdapterConformanceSubject,
  SourceFixture,
  registerSourceAdapterConformance,
  type SourceAdapterConformanceLeasedSession,
  type SourceAdapterConformanceMaterializedSession,
  type SourceAdapterConformanceMaterializedSnapshot,
  type SourceAdapterConformanceTermination,
  type SourceFixtureFailure,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import type { SourceApplicationExit, SourceTermination } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Result,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import { makeRuntimeCoreSourceManager } from "./source-runtime";

const ConformanceRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  value: Schema.String,
});

const materializedTarget: SourceFixtureTarget = {
  _tag: "Materialized",
  lane: "primary",
};

const materializedSiblingTarget: SourceFixtureTarget = {
  _tag: "Materialized",
  lane: "sibling",
};

const leasedTarget = (region: string): SourceFixtureTarget => ({
  _tag: "Leased",
  route: { region },
});

const normalizeTermination = (
  termination: SourceTermination<SourceFixtureFailure>,
): SourceAdapterConformanceTermination => {
  if (termination._tag === "UnexpectedCompletion") {
    return termination;
  }
  if (termination.failure._tag === "AdapterFailure") {
    return {
      _tag: "AdapterFailure",
      phase: termination.failure.failure.phase,
    };
  }
  return {
    _tag: "RuntimeFailure",
    failure: termination.failure.failure._tag,
  };
};

const makeMaterializedSession: Effect.Effect<
  SourceAdapterConformanceMaterializedSession,
  unknown,
  Scope.Scope
> = Effect.gen(function* () {
  const fixture = yield* SourceFixture.make(ConformanceRow);
  const config = defineViewServerConfig({
    topics: {
      rows: {
        schema: ConformanceRow,
        source: fixture.materializedSource({
          label: "conformance-materialized",
          lanes: ["primary", "sibling"],
        }),
      },
    },
  });
  const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(fixture.layer));
  const scope = yield* Effect.scope;
  let closed = false;
  let mutationOrder: ReadonlyArray<"Upsert" | "Delete"> = [];
  const settlementExits: Array<SourceApplicationExit> = [];
  let rejectionStatusAtSettlement: "Degraded" | null = null;
  let cached: SourceAdapterConformanceMaterializedSnapshot = {
    rows: [],
    mutationOrder: [],
    settlementExits: [],
    status: "Starting",
    rejectionStatusAtSettlement: null,
    rejectedItemCount: 0n,
    acquisitions: 0n,
    finalizations: 0n,
    adapterMetric: 0n,
    metricReads: 0n,
    failedSettlementCount: 0n,
    lastTermination: null,
    latestRejection: null,
  };
  const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
  const collectRows = Effect.fn("RuntimeCore.sourceConformance.materialized.rows")(function* () {
    const subscription = yield* runtime.liveClient.subscribe("rows", {
      select: ["id"],
      orderBy: [{ field: "id", direction: "asc" }],
    });
    const snapshot = yield* subscription.events.pipe(
      Stream.filter((event) => event.type === "snapshot"),
      Stream.take(1),
      Stream.runHead,
    );
    yield* subscription.close();
    return Option.getOrThrow(snapshot).rows.map((row) => row.id);
  });
  const readHealth = diagnostics.events.pipe(
    Stream.take(1),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
  const awaitStatus = (expected: SourceAdapterConformanceMaterializedSnapshot["status"]) =>
    diagnostics.events.pipe(
      Stream.filter((health) => health.status._tag === expected),
      Stream.take(1),
      Stream.runDrain,
    );
  const close = yield* Effect.cached(
    Effect.gen(function* () {
      closed = true;
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );
  yield* Scope.addFinalizer(scope, close.pipe(Effect.orDie));
  yield* fixture.controls.awaitActive(materializedTarget);

  const inspect = Effect.gen(function* () {
    const counts = fixture.controls.counts(materializedTarget);
    if (closed) {
      cached = {
        ...cached,
        acquisitions: counts.acquisitions,
        finalizations: counts.finalizations,
      };
      return cached;
    }
    const health = yield* readHealth;
    const lastTermination =
      health.status._tag === "Exhausted"
        ? normalizeTermination(health.status.exhaustion.lastTermination)
        : null;
    const latestRejection =
      health.status._tag === "Degraded"
        ? {
            failure: normalizeTermination({
              _tag: "Failed",
              failure: health.status.latestRejection.failure,
            }),
            safeLocationMatched:
              health.status.latestRejection.location.lane === "fixture" &&
              health.status.latestRejection.location.offset === 1n,
            rejectedAtNanos: health.status.latestRejection.rejectedAtNanos,
            rawPayloadPresent:
              Object.hasOwn(health.status.latestRejection, "payload") ||
              Object.hasOwn(health.status.latestRejection.location, "payload"),
          }
        : cached.latestRejection;
    cached = {
      rows: yield* collectRows(),
      mutationOrder,
      settlementExits: [...settlementExits],
      status: health.status._tag,
      rejectionStatusAtSettlement,
      rejectedItemCount: health.metrics.runtime.rejectedItemCount,
      acquisitions: counts.acquisitions,
      finalizations: counts.finalizations,
      adapterMetric: health.metrics.adapter.observed,
      metricReads: fixture.controls.metricReads(),
      failedSettlementCount: health.metrics.runtime.failedSettlementCount,
      lastTermination,
      latestRejection,
    };
    return cached;
  });

  return {
    emitOrderedDelivery: Effect.gen(function* () {
      const settled = yield* Deferred.make<void>();
      yield* fixture.controls.delivery(
        materializedTarget,
        [
          {
            _tag: "Upsert",
            row: {
              id: "ordered",
              region: "eu",
              value: "created",
            },
          },
          {
            _tag: "Delete",
            id: "ordered",
          },
        ],
        (exit) =>
          Effect.sync(() => {
            mutationOrder = ["Upsert", "Delete"];
            settlementExits.push(exit);
          }).pipe(Effect.andThen(Deferred.succeed(settled, undefined).pipe(Effect.asVoid))),
      );
      yield* Deferred.await(settled);
    }),
    emitConcurrentSiblingDeliveries: Effect.gen(function* () {
      const primarySettlementStarted = yield* Deferred.make<void>();
      const releasePrimarySettlement = yield* Deferred.make<void>();
      const primarySettled = yield* Deferred.make<void>();
      const siblingSettled = yield* Deferred.make<void>();
      yield* fixture.controls.upsert(
        materializedTarget,
        {
          id: "primary",
          region: "eu",
          value: "first-lane",
        },
        () =>
          Deferred.succeed(primarySettlementStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releasePrimarySettlement)),
            Effect.andThen(Deferred.succeed(primarySettled, undefined)),
            Effect.asVoid,
          ),
      );
      yield* Deferred.await(primarySettlementStarted);
      yield* fixture.controls.upsert(
        materializedSiblingTarget,
        {
          id: "sibling",
          region: "us",
          value: "second-lane",
        },
        () => Deferred.succeed(siblingSettled, undefined).pipe(Effect.asVoid),
      );
      yield* Deferred.await(siblingSettled).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releasePrimarySettlement, undefined);
      yield* Deferred.await(primarySettled);
    }),
    emitRejectedItemThenUpsert: Effect.gen(function* () {
      const rejectionSettled = yield* Deferred.make<void>();
      yield* fixture.controls.reject(
        materializedTarget,
        SourceFixture.failure("conformance rejection", "stream"),
        {
          lane: "fixture",
          offset: 1n,
        },
        () =>
          Effect.gen(function* () {
            const health = yield* readHealth.pipe(Effect.orDie);
            rejectionStatusAtSettlement = health.status._tag === "Degraded" ? "Degraded" : null;
            yield* Deferred.succeed(rejectionSettled, undefined);
          }),
      );
      yield* Deferred.await(rejectionSettled);
      const applied = yield* Deferred.make<void>();
      yield* fixture.controls.upsert(
        materializedTarget,
        {
          id: "after-rejection",
          region: "eu",
          value: "accepted",
        },
        () => Deferred.succeed(applied, undefined).pipe(Effect.asVoid),
      );
      yield* Deferred.await(applied);
      yield* fixture.controls.delivery(materializedTarget, [
        {
          _tag: "Delete",
          id: "not-present",
        },
      ]);
    }),
    failCurrentAttempt: Effect.gen(function* () {
      const before = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.fail(
        materializedTarget,
        SourceFixture.failure("conformance stream failure", "stream"),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: before.acquisitions + 1n,
        finalizations: before.finalizations + 1n,
      });
      yield* awaitStatus("Degraded");
    }),
    completeCurrentAttempt: Effect.gen(function* () {
      const before = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.complete(materializedTarget);
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: before.acquisitions + 1n,
        finalizations: before.finalizations + 1n,
      });
      yield* awaitStatus("Degraded");
    }),
    failNextAcquisition: Effect.gen(function* () {
      const before = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.failNextAcquisition(
        materializedTarget,
        SourceFixture.failure("conformance acquisition failure", "acquire"),
      );
      yield* fixture.controls.fail(
        materializedTarget,
        SourceFixture.failure("trigger acquisition retry", "stream"),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: before.acquisitions + 1n,
        finalizations: before.finalizations + 1n,
      });
      yield* awaitStatus("Ready");
    }),
    failDeliverySettlement: Effect.gen(function* () {
      const before = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.upsert(
        materializedTarget,
        {
          id: "settlement-failure",
          region: "eu",
          value: "applied-before-settlement",
        },
        (exit) =>
          Effect.sync(() => {
            settlementExits.push(exit);
          }).pipe(
            Effect.andThen(
              Effect.fail(SourceFixture.failure("delivery settlement failure", "settlement")),
            ),
          ),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: before.acquisitions + 1n,
        finalizations: before.finalizations + 1n,
      });
      yield* awaitStatus("Ready");
    }),
    failRejectionSettlement: Effect.gen(function* () {
      const before = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.reject(
        materializedTarget,
        SourceFixture.failure("rejected source item", "stream"),
        {
          lane: "primary",
          offset: 2n,
        },
        (exit) =>
          Effect.gen(function* () {
            settlementExits.push(exit);
            const health = yield* readHealth.pipe(Effect.orDie);
            rejectionStatusAtSettlement = health.status._tag === "Degraded" ? "Degraded" : null;
            return yield* Effect.fail(
              SourceFixture.failure("rejection settlement failure", "settlement"),
            );
          }),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: before.acquisitions + 1n,
        finalizations: before.finalizations + 1n,
      });
      yield* awaitStatus("Degraded");
    }),
    exhaustAttempts: Effect.gen(function* () {
      for (let retry = 1n; retry <= 3n; retry++) {
        yield* fixture.controls.fail(
          materializedTarget,
          SourceFixture.failure("conformance retry exhaustion", "stream"),
        );
        yield* fixture.controls.awaitCounts(materializedTarget, {
          acquisitions: retry + 1n,
          finalizations: retry,
        });
      }
      yield* fixture.controls.fail(
        materializedTarget,
        SourceFixture.failure("conformance retry exhaustion", "stream"),
      );
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: 4n,
        finalizations: 4n,
      });
      yield* diagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "Exhausted"),
        Stream.take(1),
        Stream.runDrain,
      );
    }),
    invalidateMetrics: fixture.controls.setRawMetricObserved("invalid"),
    exerciseApplicationExits: Effect.gen(function* () {
      const exits: Array<SourceApplicationExit> = [];
      const applications: ReadonlyArray<() => Effect.Effect<void, ViewServerRuntimeError>> = [
        () => Effect.void,
        () =>
          Effect.fail({
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message: "Conformance application failure.",
          }),
        () => Effect.die("conformance application defect"),
        () => Effect.interrupt,
      ];
      for (const [index, application] of applications.entries()) {
        const applicationFixture = yield* SourceFixture.make(ConformanceRow);
        const applicationConfig = defineViewServerConfig({
          topics: {
            rows: {
              schema: ConformanceRow,
              source: applicationFixture.materializedSource({
                label: `conformance-application-exit-${index}`,
              }),
            },
          },
        });
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof applicationConfig.topics> = {
          publish: () => Effect.void,
          publishMany: () => Effect.void,
          patch: () => Effect.void,
          delete: () => Effect.void,
          reset: () => Effect.void,
          deleteStorageKey: () => Effect.void,
          patchDecodedFields: () => Effect.void,
          publishManyDecodedRows: application,
          publishManyDecodedRowsWithStorageKeys: () => Effect.void,
          publishManyWithStorageKeys: () => Effect.void,
        };
        const manager = yield* makeRuntimeCoreSourceManager(applicationConfig, mutations).pipe(
          Effect.provide(applicationFixture.layer),
        );
        const target: SourceFixtureTarget = { _tag: "Materialized" };
        const settled = yield* Deferred.make<void>();
        yield* applicationFixture.controls.awaitActive(target);
        yield* applicationFixture.controls.upsert(
          target,
          {
            id: `application-${index}`,
            region: "eu",
            value: "value",
          },
          (exit) =>
            Effect.sync(() => {
              exits.push(exit);
            }).pipe(Effect.andThen(Deferred.succeed(settled, undefined).pipe(Effect.asVoid))),
        );
        yield* Deferred.await(settled);
        yield* manager.close;
      }
      return exits;
    }),
    openFinalizationProbe: Effect.gen(function* () {
      const finalizationFixture = yield* SourceFixture.make(ConformanceRow);
      const finalizationConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: ConformanceRow,
            source: finalizationFixture.materializedSource({
              label: "conformance-finalization",
            }),
          },
        },
      });
      const adapterContext = yield* Layer.build(finalizationFixture.layer);
      const service = Context.get(adapterContext, finalizationFixture.adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromUndefinedOr(service.materialized));
      const finalizerStarted = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      let finalizationCount = 0n;
      const acquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const attempt = yield* materialized.acquire(input);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              finalizationCount += 1n;
            }).pipe(
              Effect.andThen(Deferred.succeed(finalizerStarted, undefined)),
              Effect.andThen(Deferred.await(releaseFinalizer)),
            ),
          );
          return attempt;
        });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof finalizationConfig.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () => Effect.void,
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.void,
        publishManyDecodedRowsWithStorageKeys: () => Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(finalizationConfig, mutations).pipe(
        Effect.provideService(finalizationFixture.adapter.runtimeService, {
          ...service,
          materialized: new Proxy(materialized, {
            get: (target, property, receiver) =>
              property === "acquire" ? acquire : Reflect.get(target, property, receiver),
          }),
        }),
      );
      yield* finalizationFixture.controls.awaitActive({ _tag: "Materialized" });
      yield* Scope.addFinalizer(
        scope,
        Deferred.succeed(releaseFinalizer, undefined).pipe(
          Effect.andThen(manager.close),
          Effect.asVoid,
        ),
      );
      return {
        interrupt: manager.close,
        finalizerStarted: Deferred.await(finalizerStarted),
        releaseFinalizer: Deferred.succeed(releaseFinalizer, undefined).pipe(Effect.asVoid),
        closeAgain: manager.close,
        finalizationCount: Effect.sync(() => finalizationCount),
      };
    }),
    validateAttemptBoundaries: Effect.gen(function* () {
      const lane = SourceAdapterServer.lane({
        id: "lane",
        events: Stream.never,
      });
      const emptyLanesRejected = Result.isFailure(
        Result.try(() => Reflect.apply(SourceAdapterServer.attempt, undefined, [[]])),
      );
      const emptyLaneIdRejected = Result.isFailure(
        Result.try(() =>
          SourceAdapterServer.lane({
            id: "",
            events: Stream.never,
          }),
        ),
      );
      const duplicateLaneIdsRejected = Result.isFailure(
        Result.try(() => SourceAdapterServer.attempt([lane, lane])),
      );

      const changingFixture = yield* SourceFixture.make(ConformanceRow);
      const changingConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: ConformanceRow,
            source: changingFixture.materializedSource(
              { label: "conformance-changing-lanes" },
              Schedule.recurs(1),
            ),
          },
        },
      });
      const changingContext = yield* Layer.build(changingFixture.layer);
      const changingService = Context.get(changingContext, changingFixture.adapter.runtimeService);
      const changingMaterialized = Option.getOrThrow(
        Option.fromUndefinedOr(changingService.materialized),
      );
      let changingAcquisitions = 0;
      const changingAcquire: typeof changingMaterialized.acquire = () =>
        Effect.gen(function* () {
          changingAcquisitions += 1;
          const failure = yield* changingFixture.adapter
            .failure(SourceFixture.failure("change lane identity", "stream"))
            .pipe(Effect.orDie);
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: changingAcquisitions === 1 ? "first" : "second",
              events: changingAcquisitions === 1 ? Stream.fail(failure) : Stream.never,
            }),
          ]);
        });
      const changingRuntime = yield* makeViewServerRuntimeCore(changingConfig, {}).pipe(
        Effect.provideService(changingFixture.adapter.runtimeService, {
          ...changingService,
          materialized: new Proxy(changingMaterialized, {
            get: (target, property, receiver) =>
              property === "acquire" ? changingAcquire : Reflect.get(target, property, receiver),
          }),
        }),
      );
      const changingDiagnostics = yield* changingRuntime.liveClient.subscribeSourceHealth("rows");
      const changingLaneIdsRejected = Option.isSome(
        yield* changingDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      yield* changingDiagnostics.close();
      yield* changingRuntime.close;

      const missingBufferFixture = yield* SourceFixture.make(ConformanceRow);
      const missingBufferConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: ConformanceRow,
            source: missingBufferFixture.materializedSource(
              { label: "conformance-missing-buffer-metrics" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const missingBufferContext = yield* Layer.build(missingBufferFixture.layer);
      const missingBufferService = Context.get(
        missingBufferContext,
        missingBufferFixture.adapter.runtimeService,
      );
      const missingBufferMaterialized = Option.getOrThrow(
        Option.fromUndefinedOr(missingBufferService.materialized),
      );
      const missingBufferAcquire: typeof missingBufferMaterialized.acquire = (input) =>
        Effect.gen(function* () {
          const attempt = yield* missingBufferMaterialized.acquire(input);
          const missingBufferLane = new Proxy(attempt.lanes[0], {
            get: (target, property, receiver) =>
              property === "bufferMetrics" ? undefined : Reflect.get(target, property, receiver),
          });
          return SourceAdapterServer.attempt([missingBufferLane]);
        });
      const missingBufferRuntime = yield* makeViewServerRuntimeCore(missingBufferConfig, {}).pipe(
        Effect.provideService(missingBufferFixture.adapter.runtimeService, {
          ...missingBufferService,
          materialized: new Proxy(missingBufferMaterialized, {
            get: (target, property, receiver) =>
              property === "acquire"
                ? missingBufferAcquire
                : Reflect.get(target, property, receiver),
          }),
        }),
      );
      const missingBufferDiagnostics =
        yield* missingBufferRuntime.liveClient.subscribeSourceHealth("rows");
      const missingBufferMetricsRejected = Option.isSome(
        yield* missingBufferDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      yield* missingBufferDiagnostics.close();
      yield* missingBufferRuntime.close;

      const beforeInvalidTransportRow = fixture.controls.counts(materializedTarget);
      yield* fixture.controls.upsert(materializedTarget, {
        id: "invalid-transport-row",
      });
      yield* fixture.controls.awaitCounts(materializedTarget, {
        acquisitions: beforeInvalidTransportRow.acquisitions + 1n,
        finalizations: beforeInvalidTransportRow.finalizations + 1n,
      });
      const afterInvalidTransportRow = fixture.controls.counts(materializedTarget);
      const invalidTransportRowRejected =
        afterInvalidTransportRow.acquisitions === beforeInvalidTransportRow.acquisitions + 1n &&
        afterInvalidTransportRow.finalizations === beforeInvalidTransportRow.finalizations + 1n;

      return {
        emptyLanesRejected,
        emptyLaneIdRejected,
        duplicateLaneIdsRejected,
        changingLaneIdsRejected,
        missingBufferMetricsRejected,
        invalidTransportRowRejected,
      };
    }),
    updateAdapterMetric: (value) => fixture.controls.setMetrics({ observed: value }),
    exerciseDelayedRetry: Effect.gen(function* () {
      const delayedFixture = yield* SourceFixture.make(ConformanceRow);
      const delayedConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: ConformanceRow,
            source: delayedFixture.materializedSource(
              {
                label: "conformance-delayed-retry",
                lanes: ["primary"],
              },
              Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
            ),
          },
        },
      });
      const delayedRuntime = yield* makeViewServerRuntimeCore(delayedConfig, {}).pipe(
        Effect.provide(delayedFixture.layer),
      );
      const delayedDiagnostics = yield* delayedRuntime.liveClient.subscribeSourceHealth("rows");
      yield* delayedFixture.controls.awaitActive(materializedTarget);
      const waitingFiber = yield* delayedDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "WaitingToRetry"),
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* delayedFixture.controls.fail(
        materializedTarget,
        SourceFixture.failure("delayed retry", "stream"),
      );
      const waitingHealth = Option.getOrThrow(yield* Fiber.join(waitingFiber));
      if (waitingHealth.status._tag !== "WaitingToRetry") {
        return yield* Effect.die("Expected WaitingToRetry Source Health.");
      }
      const acquisitionsBeforeDelay =
        delayedFixture.controls.counts(materializedTarget).acquisitions;
      const reacquiringFiber = yield* delayedDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "Reacquiring"),
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      const readyFiber = yield* delayedDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "Ready" && health.status.attempt === 2n),
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* TestClock.adjust("999 millis");
      const acquisitionsBeforeBoundary =
        delayedFixture.controls.counts(materializedTarget).acquisitions;
      yield* TestClock.adjust("1 millis");
      const reacquiringHealth = Option.getOrThrow(yield* Fiber.join(reacquiringFiber));
      const readyHealth = Option.getOrThrow(yield* Fiber.join(readyFiber));
      if (reacquiringHealth.status._tag !== "Reacquiring" || readyHealth.status._tag !== "Ready") {
        return yield* Effect.die("Expected Reacquiring and Ready Source Health.");
      }
      const acquisitionsAfterBoundary =
        delayedFixture.controls.counts(materializedTarget).acquisitions;
      yield* delayedDiagnostics.close();
      yield* delayedRuntime.close;
      return {
        waiting: {
          termination: normalizeTermination(waitingHealth.status.termination),
          retryAtNanos: waitingHealth.status.retryAtNanos,
        },
        acquisitionsBeforeDelay,
        acquisitionsBeforeBoundary,
        reacquiring: {
          previousTermination: normalizeTermination(reacquiringHealth.status.previousTermination),
          attempt: reacquiringHealth.status.attempt,
        },
        recoveredStatus: readyHealth.status._tag,
        acquisitionsAfterBoundary,
      };
    }),
    awaitAcquisitions: (expected) =>
      Effect.sync(() => {
        const actual = fixture.controls.counts(materializedTarget).acquisitions;
        if (actual !== expected) {
          throw new Error(`Expected ${expected} acquisitions, received ${actual}.`);
        }
      }),
    awaitStatus,
    inspect,
    close,
  };
});

const makeLeasedSession: Effect.Effect<
  SourceAdapterConformanceLeasedSession,
  unknown,
  Scope.Scope
> = Effect.gen(function* () {
  const fixture = yield* SourceFixture.make(ConformanceRow);
  const config = defineViewServerConfig({
    topics: {
      rows: {
        schema: ConformanceRow,
        source: fixture.leasedSource(["region"], {
          label: "conformance-leased",
        }),
      },
    },
  });
  const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(fixture.layer));
  const scope = yield* Effect.scope;
  const references = new Map<string, number>();
  const settlementExits = new Map<string, Array<SourceApplicationExit>>();
  const closeRuntime = yield* Effect.cached(runtime.close);
  yield* Scope.addFinalizer(scope, closeRuntime);

  const collectRows = (region: string) =>
    Effect.gen(function* () {
      if ((references.get(region) ?? 0) === 0) {
        return [];
      }
      const subscription = yield* runtime.liveClient.subscribe("rows", {
        routeBy: { region },
        select: ["id"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const snapshot = yield* subscription.events.pipe(
        Stream.filter((event) => event.type === "snapshot"),
        Stream.take(1),
        Stream.runHead,
      );
      yield* subscription.close();
      return Option.getOrThrow(snapshot).rows.map((row) => row.id);
    });

  const session: SourceAdapterConformanceLeasedSession = {
    sameRoute: "same",
    distinctRoute: "distinct",
    diagnostics: (region) =>
      Effect.gen(function* () {
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows", {
          region,
        });
        const close = yield* Effect.cached(diagnostics.close());
        yield* Scope.addFinalizer(scope, close.pipe(Effect.orDie));
        return {
          latest: diagnostics.events.pipe(
            Stream.take(1),
            Stream.runHead,
            Effect.map((result) => Option.getOrThrow(result)._tag),
          ),
          close,
        };
      }),
    subscribe: (region) =>
      Effect.gen(function* () {
        const subscription = yield* runtime.liveClient.subscribe("rows", {
          routeBy: { region },
          select: ["id", "region"],
        });
        references.set(region, (references.get(region) ?? 0) + 1);
        const close = yield* Effect.cached(
          Effect.gen(function* () {
            yield* subscription.close();
            const remaining = (references.get(region) ?? 1) - 1;
            references.set(region, remaining);
          }),
        );
        yield* Scope.addFinalizer(scope, close.pipe(Effect.orDie));
        return { close };
      }),
    seed: (region, id) =>
      Effect.gen(function* () {
        const applied = yield* Deferred.make<void>();
        yield* fixture.controls.upsert(
          leasedTarget(region),
          {
            id,
            region,
            value: "seeded",
          },
          () => Deferred.succeed(applied, undefined).pipe(Effect.asVoid),
        );
        yield* Deferred.await(applied);
      }),
    emitRouteIncongruentDelivery: (region) =>
      Effect.gen(function* () {
        const before = fixture.controls.counts(leasedTarget(region));
        const settled = yield* Deferred.make<void>();
        yield* fixture.controls.corruptAfterDecode(
          leasedTarget(region),
          {
            id: "wrong-route",
            region,
            value: "must-fail-application",
          },
          "region",
          `${region}-other`,
          (exit) =>
            Effect.sync(() => {
              const exits = settlementExits.get(region) ?? [];
              exits.push(exit);
              settlementExits.set(region, exits);
            }).pipe(Effect.andThen(Deferred.succeed(settled, undefined).pipe(Effect.asVoid))),
        );
        yield* Deferred.await(settled);
        yield* fixture.controls.awaitCounts(leasedTarget(region), {
          acquisitions: before.acquisitions + 1n,
          finalizations: before.finalizations + 1n,
        });
      }),
    inspect: (region) =>
      Effect.gen(function* () {
        const counts = fixture.controls.counts(leasedTarget(region));
        return {
          acquisitions: counts.acquisitions,
          finalizations: counts.finalizations,
          active: (references.get(region) ?? 0) > 0,
          rows: yield* collectRows(region),
          settlementExits: [...(settlementExits.get(region) ?? [])],
        };
      }),
  };
  return session;
});

const conformanceLayer = Layer.succeed(SourceAdapterConformanceSubject, {
  openMaterialized: makeMaterializedSession,
  openLeased: makeLeasedSession,
});

registerSourceAdapterConformance({
  name: "Runtime Core controllable Materialized fixture conformance",
  layer: conformanceLayer,
  materialized: true,
  leased: false,
});

registerSourceAdapterConformance({
  name: "Runtime Core controllable Leased fixture conformance",
  layer: conformanceLayer,
  materialized: false,
  leased: true,
});

describe("Controllable Source fixture", () => {
  it.effect("rejects commands addressed to an unregistered lane", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(ConformanceRow);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: ConformanceRow,
            source: fixture.materializedSource({
              label: "missing-lane",
              lanes: ["primary"],
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({
        _tag: "Materialized",
        lane: "primary",
      });
      const failure = yield* fixture.controls
        .upsert(
          {
            _tag: "Materialized",
            lane: "missing",
          },
          {
            id: "not-delivered",
            region: "eu",
            value: "not-delivered",
          },
        )
        .pipe(Effect.flip);
      expect(failure).toStrictEqual(
        SourceFixture.failure("Fixture lane missing is not active for this target.", "stream"),
      );
      yield* runtime.close;
    }),
  );
});
