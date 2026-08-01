import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter, type SourceTermination } from "@effect-view-server/source-adapter";
import { makeSourceApplicationTransition } from "@effect-view-server/source-adapter/internal";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceFixture,
  type ControllableSourceFixture,
  type SourceFixtureFailure,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import type { SourceApplicationExit } from "@effect-view-server/source-adapter";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Result,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import { makeRuntimeCoreSourceManager } from "./source-runtime";

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  value: Schema.String,
});

const leasedTarget: SourceFixtureTarget = {
  _tag: "Leased",
  route: { region: "eu" },
};

const mutationFailure = (): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "Injected Source mutation failure.",
});

describe("Runtime Core Source manager lifecycle", () => {
  it.effect("cleans a leased placeholder interrupted before its logical runtime exists", () =>
    Effect.gen(function* () {
      const Failure = Schema.TaggedStruct("PendingLeaseFailure", {
        message: Schema.String,
      });
      const Metrics = Schema.Struct({
        observed: Schema.BigInt,
      });
      const Location = Schema.Struct({
        offset: Schema.BigInt,
      });
      const adapter = SourceAdapter.make({
        identity: { name: "pending-lease" },
        failure: Failure,
        materialized: undefined,
        leased: {
          metrics: Metrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
      });
      const metricsStarted = yield* Deferred.make<void>();
      const layer = SourceAdapterServer.make(adapter, {
        leased: {
          acquire: () => Effect.die(new Error("Acquisition must not start before metrics.")),
          metrics: () =>
            Deferred.succeed(metricsStarted, undefined).pipe(Effect.andThen(Effect.never)),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: adapter.leasedSource(["region"], undefined),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void).pipe(
        Effect.provide(layer),
      );
      const acquisition = yield* manager
        .acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(metricsStarted);
      yield* Fiber.interrupt(acquisition);

      const diagnostics = yield* manager.subscribeSourceHealth({
        topic: "rows",
        routeBy: {
          region: "eu",
        },
      });
      expect(
        Option.getOrThrow(yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead)),
      ).toStrictEqual({
        _tag: "Inactive",
        route: { region: "eu" },
      });
      yield* diagnostics.close();
      yield* manager.close;
    }),
  );

  it.effect("rolls back a manager handoff failure after a materialized Source starts", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "manager-construction-rollback",
            }),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const exit = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        handoff: {
          beforeReturn: fixture.controls
            .awaitActive({ _tag: "Materialized" })
            .pipe(Effect.andThen(Effect.die("manager handoff failed"))),
        },
      }).pipe(Effect.provide(fixture.layer), Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      yield* fixture.controls.awaitCounts(
        { _tag: "Materialized" },
        {
          acquisitions: 1n,
          finalizations: 1n,
        },
      );
    }),
  );

  it.effect("rolls back interruption during leased Source handoff and permits reacquisition", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(["region"], {
              label: "lease-construction-interruption",
            }),
          },
        },
      });
      const retainedStorageKeys = new Set<string>();
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: (_topic, storageKey) =>
          Effect.sync(() => {
            retainedStorageKeys.delete(storageKey);
          }),
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.void,
        publishManyDecodedRowsWithStorageKeys: (_topic, rows) =>
          Effect.sync(() => {
            for (const row of rows) {
              retainedStorageKeys.add(row.storageKey);
            }
          }),
        publishManyWithStorageKeys: () => Effect.void,
      };
      const handoffStarted = yield* Deferred.make<void>();
      let blockLeaseHandoff = true;
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        leaseHandoff: {
          beforeReturn: Effect.suspend(() =>
            blockLeaseHandoff
              ? fixture.controls
                  .awaitActive(leasedTarget)
                  .pipe(
                    Effect.andThen(Deferred.succeed(handoffStarted, undefined)),
                    Effect.andThen(Effect.never),
                  )
              : Effect.void,
          ),
        },
      }).pipe(Effect.provide(fixture.layer));
      const interruptedAcquisition = yield* manager
        .acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(handoffStarted);
      const applied = yield* Deferred.make<boolean>();
      yield* fixture.controls.upsert(
        leasedTarget,
        { id: "interrupted-row", region: "eu", value: "owned" },
        (exit) => Deferred.succeed(applied, Exit.isSuccess(exit)).pipe(Effect.asVoid),
      );
      expect(yield* Deferred.await(applied)).toBe(true);
      expect(retainedStorageKeys.size).toBe(1);
      yield* Fiber.interrupt(interruptedAcquisition);
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 1n,
        finalizations: 1n,
      });
      expect(retainedStorageKeys.size).toBe(0);

      blockLeaseHandoff = false;
      const reacquired = Option.getOrThrow(
        yield* manager.acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        }),
      );
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 2n,
        finalizations: 1n,
      });
      yield* reacquired.release;
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 2n,
        finalizations: 2n,
      });
      yield* manager.close;
    }),
  );

  it.effect("releases a leased Source Health observer interrupted at its return handoff", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(["region"], {
              label: "source-health-handoff-interruption",
            }),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const handoffStarted = yield* Deferred.make<void>();
      const observerClosed = yield* Deferred.make<void>();
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        sourceHealthHandoff: {
          beforeReturn: Deferred.succeed(handoffStarted, undefined).pipe(
            Effect.andThen(Effect.never),
          ),
        },
        afterSourceHealthObservationClose: Deferred.succeed(observerClosed, undefined).pipe(
          Effect.asVoid,
        ),
      }).pipe(Effect.provide(fixture.layer));
      const subscriptionFiber = yield* manager
        .subscribeSourceHealth({ topic: "rows", routeBy: { region: "eu" } })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(handoffStarted);

      yield* Fiber.interrupt(subscriptionFiber);
      yield* Deferred.await(observerClosed);
      expect(fixture.controls.counts(leasedTarget)).toStrictEqual({
        acquisitions: 0n,
        finalizations: 0n,
      });
      yield* manager.close;
    }),
  );

  it.effect("halts active Source Health streams during manager shutdown", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(["region"], {
              label: "source-health-shutdown",
            }),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      let observerCloseCount = 0;
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        afterSourceHealthObservationClose: Effect.sync(() => {
          observerCloseCount += 1;
        }),
      }).pipe(Effect.provide(fixture.layer));
      const diagnostics = yield* manager.subscribeSourceHealth({
        topic: "rows",
        routeBy: { region: "eu" },
      });
      const streamHalted = yield* Deferred.make<void>();
      const streamFiber = yield* diagnostics.events.pipe(
        Stream.runDrain,
        Effect.ensuring(Deferred.succeed(streamHalted, undefined)),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* manager.close;
      yield* Deferred.await(streamHalted);
      yield* Fiber.join(streamFiber);
      yield* diagnostics.close();

      expect(observerCloseCount).toBe(1);
    }),
  );

  it.effect("rejects a nominal Source binding whose captured row Schema is invalid", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const stable = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "missing-schema",
            }),
          },
        },
      });
      const invalidRowDefinition = new Proxy(
        { ...stable.topics.rows },
        {
          get: (target, property, receiver) =>
            property === "schema" ? undefined : Reflect.get(target, property, receiver),
        },
      );
      const config = {
        ...stable,
        topics: {
          ...stable.topics,
          rows: invalidRowDefinition,
        },
      };
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const exit = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("fails a Source mutation if the captured topic graph is replaced", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const stable = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "replaced-topic-graph" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      let hideTopics = false;
      const config = new Proxy(
        { ...stable },
        {
          get: (target, property, receiver) =>
            property === "topics" && hideTopics ? {} : Reflect.get(target, property, receiver),
        },
      );
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof stable.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({
        _tag: "Materialized",
      });
      hideTopics = true;
      const failedApplication = yield* Deferred.make<boolean>();
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "lost", region: "eu", value: "lost" },
        (application) =>
          Deferred.succeed(failedApplication, Exit.isFailure(application)).pipe(Effect.asVoid),
      );

      expect(yield* Deferred.await(failedApplication)).toBe(true);
      yield* manager.close;
    }),
  );

  it.effect("rolls back failed lease mutations and makes every close path idempotent", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(
              ["region"],
              { label: "manager-lifecycle" },
              Schedule.forever,
            ),
          },
        },
      });
      let failPublish = false;
      let failDeleteStorageKey = false;
      let deleteStorageKeyAttempts = 0;
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () => {
          deleteStorageKeyAttempts += 1;
          return failDeleteStorageKey ? Effect.fail(mutationFailure()) : Effect.void;
        },
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.void,
        publishManyDecodedRowsWithStorageKeys: () =>
          failPublish ? Effect.fail(mutationFailure()) : Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );

      const firstDiagnostics = yield* manager.subscribeSourceHealth({
        topic: "rows",
        routeBy: { region: "eu" },
      });
      const secondDiagnostics = yield* manager.subscribeSourceHealth({
        topic: "rows",
        routeBy: { region: "eu" },
      });
      const firstLease = Option.getOrThrow(
        yield* manager.acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        }),
      );
      const secondLease = Option.getOrThrow(
        yield* manager.acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        }),
      );
      const independentlyCleanedLease = Option.getOrThrow(
        yield* manager.acquireLeased("rows", {
          routeBy: { region: "us" },
          select: ["id"],
        }),
      );
      yield* fixture.controls.awaitActive(leasedTarget);

      expect(
        firstLease.partition.matches({ id: "route", region: "eu", value: "match" }, undefined),
      ).toBe(true);
      expect(
        firstLease.partition.matches({ id: "route", region: "us", value: "miss" }, undefined),
      ).toBe(false);
      expect(
        firstLease.partition.matches({ id: "route", region: "eu", value: "owned" }, "not-owned"),
      ).toBe(false);

      const firstApplied = yield* Deferred.make<boolean>();
      yield* fixture.controls.upsert(
        leasedTarget,
        { id: "a", region: "eu", value: "first" },
        (application) =>
          Deferred.succeed(firstApplied, Exit.isSuccess(application)).pipe(Effect.asVoid),
      );
      expect(yield* Deferred.await(firstApplied)).toBe(true);

      failPublish = true;
      yield* fixture.controls.upsert(
        leasedTarget,
        { id: "a", region: "eu", value: "replacement" },
        () =>
          Effect.fail(SourceFixture.failure("settlement failed after application", "settlement")),
      );
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 2n,
        finalizations: 1n,
      });

      const newRowFailed = yield* Deferred.make<boolean>();
      yield* fixture.controls.upsert(
        leasedTarget,
        { id: "b", region: "eu", value: "new" },
        (application) =>
          Deferred.succeed(newRowFailed, Exit.isFailure(application)).pipe(Effect.asVoid),
      );
      expect(yield* Deferred.await(newRowFailed)).toBe(true);
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 3n,
        finalizations: 2n,
      });

      failPublish = false;
      const deleted = yield* Deferred.make<boolean>();
      yield* fixture.controls.delete(leasedTarget, "a", (application) =>
        Deferred.succeed(deleted, Exit.isSuccess(application)).pipe(Effect.asVoid),
      );
      expect(yield* Deferred.await(deleted)).toBe(true);
      const retainedForCleanup = yield* Deferred.make<boolean>();
      yield* fixture.controls.upsert(
        leasedTarget,
        { id: "c", region: "eu", value: "cleanup" },
        (application) =>
          Deferred.succeed(retainedForCleanup, Exit.isSuccess(application)).pipe(Effect.asVoid),
      );
      expect(yield* Deferred.await(retainedForCleanup)).toBe(true);

      yield* firstDiagnostics.close();
      yield* firstDiagnostics.close();
      yield* firstLease.release;
      yield* firstLease.release;

      const beforeFailedCleanup = fixture.controls.counts(leasedTarget);
      const deleteAttemptsBeforeCleanup = deleteStorageKeyAttempts;
      failDeleteStorageKey = true;
      yield* secondLease.release;
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: beforeFailedCleanup.acquisitions,
        finalizations: beforeFailedCleanup.finalizations + 1n,
      });
      expect(deleteStorageKeyAttempts).toBe(deleteAttemptsBeforeCleanup + 1);

      const pendingCleanup = yield* manager
        .acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        })
        .pipe(Effect.exit);
      expect(Exit.findErrorOption(pendingCleanup)).toStrictEqual(
        Option.some({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Source lease row cleanup is pending retry.",
          topic: "rows",
        }),
      );
      expect(deleteStorageKeyAttempts).toBe(deleteAttemptsBeforeCleanup + 2);

      failDeleteStorageKey = false;
      const reacquired = Option.getOrThrow(
        yield* manager.acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        }),
      );
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: beforeFailedCleanup.acquisitions + 1n,
        finalizations: beforeFailedCleanup.finalizations + 1n,
      });
      expect(deleteStorageKeyAttempts).toBe(deleteAttemptsBeforeCleanup + 3);
      yield* reacquired.release;
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: beforeFailedCleanup.acquisitions + 1n,
        finalizations: beforeFailedCleanup.finalizations + 2n,
      });

      failDeleteStorageKey = true;
      yield* manager.close;
      yield* manager.close;
      yield* independentlyCleanedLease.release;
      yield* secondDiagnostics.close();
      yield* secondDiagnostics.close();

      const closedAcquire = yield* manager
        .acquireLeased("rows", {
          routeBy: { region: "eu" },
          select: ["id"],
        })
        .pipe(Effect.exit);
      expect(Exit.isFailure(closedAcquire)).toBe(true);
      const closedHealth = yield* manager
        .subscribeSourceHealth({ topic: "rows", routeBy: { region: "eu" } })
        .pipe(Effect.exit);
      expect(Exit.findErrorOption(closedHealth)).toStrictEqual(
        Option.some({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Runtime Core Source Manager is closed.",
          topic: "rows",
        }),
      );
    }),
  );

  it.effect("serializes materialized Source Health handoff with manager shutdown", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "materialized-source-health-close-race",
            }),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const handoffStarted = yield* Deferred.make<void>();
      const releaseHandoff = yield* Deferred.make<void>();
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        sourceHealthHandoff: {
          beforeReturn: Deferred.succeed(handoffStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseHandoff)),
          ),
        },
      }).pipe(Effect.provide(fixture.layer));
      const subscriptionFiber = yield* manager
        .subscribeSourceHealth({ topic: "rows" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(handoffStarted);
      const closeFiber = yield* manager.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect(closeFiber.pollUnsafe()).toBeUndefined();

      yield* Deferred.succeed(releaseHandoff, undefined);
      const subscription = yield* Fiber.join(subscriptionFiber);
      yield* Fiber.join(closeFiber);
      yield* subscription.close();
      const closedHealth = yield* manager
        .subscribeSourceHealth({ topic: "rows" })
        .pipe(Effect.exit);
      expect(Exit.findErrorOption(closedHealth)).toStrictEqual(
        Option.some({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Runtime Core Source Manager is closed.",
          topic: "rows",
        }),
      );
    }),
  );

  it.effect("settles deliveries with complete success, failure, and defect Exits", () =>
    Effect.gen(function* () {
      const observeExit = Effect.fn("RuntimeCore.sourceManager.observeSettlementExit")(function* (
        application: Effect.Effect<void, ViewServerRuntimeError>,
      ) {
        const fixture = yield* SourceFixture.make(Row);
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource({ label: "settlement-exit" }, Schedule.recurs(0)),
            },
          },
        });
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
          publish: () => Effect.void,
          publishMany: () => Effect.void,
          patch: () => Effect.void,
          delete: () => Effect.void,
          reset: () => Effect.void,
          deleteStorageKey: () => Effect.void,
          patchDecodedFields: () => Effect.void,
          publishManyDecodedRows: () => application,
          publishManyDecodedRowsWithStorageKeys: () => Effect.void,
          publishManyWithStorageKeys: () => Effect.void,
        };
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
          Effect.provide(fixture.layer),
        );
        const observed = yield* Deferred.make<SourceApplicationExit>();
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.upsert(
          { _tag: "Materialized" },
          { id: "settled", region: "eu", value: "value" },
          (exit) => Deferred.succeed(observed, exit).pipe(Effect.asVoid),
        );
        const exit = yield* Deferred.await(observed);
        yield* manager.close;
        return exit;
      });

      const succeeded = yield* observeExit(Effect.void);
      const failed = yield* observeExit(Effect.fail(mutationFailure()));
      const defect = yield* observeExit(Effect.die("application defect"));

      expect(succeeded).toStrictEqual(Exit.void);
      expect(
        Exit.isFailure(failed) ? Cause.findErrorOption(failed.cause) : Option.none(),
      ).toStrictEqual(
        Option.some({
          _tag: "InvalidSourceDelivery",
          message: "Injected Source mutation failure.",
        }),
      );
      expect(
        Exit.isFailure(defect) ? defect.cause.reasons.find(Cause.isDieReason)?.defect : undefined,
      ).toBe("application defect");
    }),
  );

  it.effect(
    "supervises one redacted callback throw for every ordinary application Exit and rejection",
    () =>
      Effect.gen(function* () {
        const applicationDefect = new Error("application defect");
        const observe = Effect.fn("RuntimeCoreSourceManager.test.throwingSettlement")(function* (
          label: string,
          application: Effect.Effect<void, ViewServerRuntimeError>,
          deliver: (
            controls: ControllableSourceFixture["controls"],
            settle: (exit: SourceApplicationExit) => never,
          ) => Effect.Effect<void, SourceFixtureFailure>,
        ) {
          const fixture = yield* SourceFixture.make(Row);
          const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
          const retry = Schedule.spaced("1 second").pipe(
            Schedule.upTo({ times: 1 }),
            Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
            Schedule.tap(({ input: termination }) =>
              Effect.sync(() => {
                scheduleInputs.push(termination);
              }),
            ),
          );
          const config = defineViewServerConfig({
            topics: {
              rows: {
                schema: Row,
                source: fixture.materializedSource({ label }, retry),
              },
            },
          });
          const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
            publish: () => Effect.void,
            publishMany: () => Effect.void,
            patch: () => Effect.void,
            delete: () => Effect.void,
            reset: () => Effect.void,
            deleteStorageKey: () => Effect.void,
            patchDecodedFields: () => Effect.void,
            publishManyDecodedRows: () => application,
            publishManyDecodedRowsWithStorageKeys: () => Effect.void,
            publishManyWithStorageKeys: () => Effect.void,
          };
          const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
            Effect.provide(fixture.layer),
          );
          const diagnostics = yield* manager.subscribeSourceHealth({ topic: "rows" });
          const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
          yield* fixture.controls.awaitActive({ _tag: "Materialized" });
          yield* fixture.controls.failNextAcquisition(
            { _tag: "Materialized" },
            SourceFixture.failure("forced reacquisition failure", "acquire"),
          );
          let callbackCount = 0;
          let observedExit: SourceApplicationExit | undefined;
          const settle = (exit: SourceApplicationExit) => {
            callbackCount += 1;
            observedExit = exit;
            throw new Error(`raw settlement throw ${label}`);
          };
          yield* deliver(fixture.controls, settle);

          const waiting = Option.getOrThrow(
            yield* diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "WaitingToRetry"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          const exactTermination = {
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceSettlement",
                message: "Source Settlement callback threw before returning an Effect",
              },
            },
          } as const;
          const waitingStatus = Option.getOrThrow(
            Option.liftPredicate(waiting.status, (status) => status._tag === "WaitingToRetry"),
          );
          expect(waitingStatus).toStrictEqual({
            _tag: "WaitingToRetry",
            nextAttempt: 2n,
            termination: exactTermination,
            retryAtNanos: waiting.sampledAtNanos + 1_000_000_000n,
          });
          yield* TestClock.adjust("1 second");
          const exhausted = Option.getOrThrow(
            yield* diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Exhausted"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          expect({
            callbackCount,
            status: exhausted.status,
            failedSettlementCount: exhausted.metrics.runtime.failedSettlementCount,
            completedSettlementCount: exhausted.metrics.runtime.completedSettlementCount,
            retryCount: exhausted.metrics.runtime.retryCount,
            scheduleInputs,
            counts: fixture.controls.counts({ _tag: "Materialized" }),
            fatalPending: fatalFiber.pollUnsafe() === undefined,
          }).toStrictEqual({
            callbackCount: 1,
            status: {
              _tag: "Exhausted",
              exhaustion: {
                _tag: "RetryExhausted",
                lastTermination: {
                  _tag: "Failed",
                  failure: {
                    _tag: "AdapterFailure",
                    failure: {
                      _tag: "SourceFixtureFailure",
                      message: "forced reacquisition failure",
                      phase: "acquire",
                    },
                  },
                },
              },
              exhaustedAtNanos: waitingStatus.retryAtNanos,
            },
            failedSettlementCount: 1n,
            completedSettlementCount: 0n,
            retryCount: 1n,
            scheduleInputs: [exactTermination],
            counts: {
              acquisitions: 1n,
              finalizations: 1n,
            },
            fatalPending: true,
          });
          const capturedExit = Option.getOrThrow(Option.fromUndefinedOr(observedExit));
          const capturedSemanticExit = Exit.match(capturedExit, {
            onSuccess: Exit.succeed,
            onFailure: (cause) =>
              Option.match(Cause.findErrorOption(cause), {
                onNone: () => Exit.die(Result.getOrThrow(Cause.findDefect(cause))),
                onSome: Exit.fail,
              }),
          });

          const stillAvailable = yield* manager.subscribeSourceHealth({ topic: "rows" });
          expect(
            Option.getOrThrow(yield* stillAvailable.events.pipe(Stream.take(1), Stream.runHead))
              .status,
          ).toStrictEqual(exhausted.status);
          yield* stillAvailable.close();
          yield* diagnostics.close();
          yield* manager.close;
          return capturedSemanticExit;
        });

        const succeeded = yield* observe(
          "callback-throw-success",
          Effect.void,
          (controls, settle) =>
            controls.upsert(
              { _tag: "Materialized" },
              { id: "callback-throw-success", region: "eu", value: "success" },
              settle,
            ),
        );
        const failed = yield* observe(
          "callback-throw-typed-failure",
          Effect.fail(mutationFailure()),
          (controls, settle) =>
            controls.upsert(
              { _tag: "Materialized" },
              { id: "callback-throw-typed-failure", region: "eu", value: "typed-failure" },
              settle,
            ),
        );
        const defected = yield* observe(
          "callback-throw-defect",
          Effect.die(applicationDefect),
          (controls, settle) =>
            controls.upsert(
              { _tag: "Materialized" },
              { id: "callback-throw-defect", region: "eu", value: "defect" },
              settle,
            ),
        );
        const rejected = yield* observe(
          "callback-throw-rejection",
          Effect.void,
          (controls, settle) =>
            controls.reject(
              { _tag: "Materialized" },
              SourceFixture.failure("rejected item", "stream"),
              { lane: "fixture", offset: 1n },
              settle,
            ),
        );

        expect(succeeded).toStrictEqual(Exit.void);
        expect(failed).toStrictEqual(
          Exit.fail({
            _tag: "InvalidSourceDelivery",
            message: "Injected Source mutation failure.",
          }),
        );
        expect(defected).toStrictEqual(Exit.die(applicationDefect));
        expect(rejected).toStrictEqual(Exit.void);
      }),
  );

  it.effect("routes a non-transition mutation defect through ordinary Source Supervision", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "supervised-mutation-defect" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () => Effect.void,
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.die("injected mutation defect"),
        publishManyDecodedRowsWithStorageKeys: () => Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );
      const diagnostics = yield* manager.subscribeSourceHealth({ topic: "rows" });
      const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "defective", region: "eu", value: "value" },
      );

      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      expect({
        _tag: exhausted.status._tag,
        exhaustion: exhausted.status._tag === "Exhausted" ? exhausted.status.exhaustion : undefined,
      }).toStrictEqual({
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: {
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceDelivery",
                message: "Source mutation application defected and terminated the Source Attempt.",
              },
            },
          },
        },
      });
      expect(fatalFiber.pollUnsafe()).toBeUndefined();
      expect(fixture.controls.counts({ _tag: "Materialized" })).toStrictEqual({
        acquisitions: 1n,
        finalizations: 1n,
      });

      yield* diagnostics.close();
      yield* manager.close;
    }),
  );

  it.effect("settles interrupted applications uninterruptibly and exactly once", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "interrupted-settlement" },
              Schedule.forever,
            ),
          },
        },
      });
      const applicationStarted = yield* Deferred.make<void>();
      const settlementObserved = yield* Deferred.make<SourceApplicationExit>();
      const context = yield* Effect.context<never>();
      let settlementCount = 0;
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () => Effect.void,
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () =>
          Deferred.succeed(applicationStarted, undefined).pipe(Effect.andThen(Effect.never)),
        publishManyDecodedRowsWithStorageKeys: () => Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "interrupted", region: "eu", value: "value" },
        (exit) => {
          settlementCount += 1;
          Effect.runSyncWith(context)(Deferred.succeed(settlementObserved, exit));
          return Effect.never;
        },
      );
      yield* Deferred.await(applicationStarted);
      const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
      const settledExit = yield* Deferred.await(settlementObserved);
      expect(settlementCount).toBe(1);
      yield* Fiber.join(closeFiber);

      expect(settlementCount).toBe(1);
      const failedExit = Option.getOrThrow(Option.liftPredicate(settledExit, Exit.isFailure));
      expect(Cause.hasInterruptsOnly(failedExit.cause)).toBe(true);
    }),
  );

  it.effect(
    "lets shutdown cancellation win after a delivery Exit and before a throwing callback handoff",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "delivery-cancellation-arbitration" },
                retry,
              ),
            },
          },
        });
        const applicationExited = yield* Deferred.make<void>();
        const releaseCallback = yield* Deferred.make<void>();
        const cancellationRequested = yield* Deferred.make<void>();
        const failureCounted = yield* Deferred.make<bigint>();
        let callbackCount = 0;
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
          settlementHandoff: {
            afterApplicationExit: Deferred.succeed(applicationExited, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCallback)),
            ),
            afterFailureCounted: (count) =>
              Deferred.succeed(failureCounted, count).pipe(Effect.asVoid),
          },
          afterAttemptCancellationRequested: Deferred.succeed(
            cancellationRequested,
            undefined,
          ).pipe(Effect.asVoid),
        }).pipe(Effect.provide(fixture.layer));
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.upsert(
          { _tag: "Materialized" },
          { id: "cancelled-delivery", region: "eu", value: "value" },
          () => {
            callbackCount += 1;
            throw new Error("cancelled callback throw");
          },
        );
        yield* Deferred.await(applicationExited);

        const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Deferred.await(cancellationRequested);
        yield* Deferred.succeed(releaseCallback, undefined);
        expect(yield* Deferred.await(failureCounted)).toBe(1n);
        yield* Fiber.join(closeFiber);

        expect({
          callbackCount,
          scheduleInputs,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
        }).toStrictEqual({
          callbackCount: 1,
          scheduleInputs: [],
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
        });
        expect(fatalFiber.pollUnsafe()).toBeUndefined();
      }),
  );

  it.effect(
    "preserves rejection state while cancellation wins before a throwing callback handoff",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "rejection-callback-cancellation-arbitration" },
                retry,
              ),
            },
          },
        });
        const applicationExited = yield* Deferred.make<void>();
        const releaseCallback = yield* Deferred.make<void>();
        const callbackApplied = yield* Deferred.make<void>();
        const releaseHandoff = yield* Deferred.make<void>();
        const cancellationRequested = yield* Deferred.make<void>();
        const failureCounted = yield* Deferred.make<bigint>();
        let callbackCount = 0;
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
          settlementHandoff: {
            afterApplicationExit: Deferred.succeed(applicationExited, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCallback)),
            ),
            afterCallbackApplication: Deferred.succeed(callbackApplied, undefined).pipe(
              Effect.andThen(Deferred.await(releaseHandoff)),
            ),
            afterFailureCounted: (count) =>
              Deferred.succeed(failureCounted, count).pipe(Effect.asVoid),
          },
          afterAttemptCancellationRequested: Deferred.succeed(
            cancellationRequested,
            undefined,
          ).pipe(Effect.asVoid),
        }).pipe(Effect.provide(fixture.layer));
        const diagnostics = yield* manager.subscribeSourceHealth({ topic: "rows" });
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.reject(
          { _tag: "Materialized" },
          SourceFixture.failure("rejected before shutdown", "stream"),
          { lane: "fixture", offset: 17n },
          () => {
            callbackCount += 1;
            throw new Error("cancelled rejection callback throw");
          },
        );
        yield* Deferred.await(applicationExited);
        const degraded = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Degraded"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        expect({
          status: degraded.status,
          rejectedItemCount: degraded.metrics.runtime.rejectedItemCount,
          failedSettlementCount: degraded.metrics.runtime.failedSettlementCount,
          completedSettlementCount: degraded.metrics.runtime.completedSettlementCount,
          retryCount: degraded.metrics.runtime.retryCount,
        }).toStrictEqual({
          status: {
            _tag: "Degraded",
            attempt: 1n,
            degradedAtNanos: degraded.sampledAtNanos,
            reasons: [
              {
                _tag: "SourceItemRejection",
                latestRejection: {
                  failure: {
                    _tag: "AdapterFailure",
                    failure: {
                      _tag: "SourceFixtureFailure",
                      message: "rejected before shutdown",
                      phase: "stream",
                    },
                  },
                  location: {
                    lane: "fixture",
                    offset: 17n,
                  },
                  rejectedAtNanos: degraded.sampledAtNanos,
                },
              },
            ],
          },
          rejectedItemCount: 1n,
          failedSettlementCount: 0n,
          completedSettlementCount: 0n,
          retryCount: 0n,
        });

        const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Deferred.await(cancellationRequested);
        yield* Deferred.succeed(releaseCallback, undefined);
        yield* Deferred.await(callbackApplied);
        expect(yield* Deferred.await(failureCounted)).toBe(1n);
        yield* Deferred.succeed(releaseHandoff, undefined);
        yield* Fiber.join(closeFiber);

        expect({
          callbackCount,
          scheduleInputs,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
          fatalPending: fatalFiber.pollUnsafe() === undefined,
        }).toStrictEqual({
          callbackCount: 1,
          scheduleInputs: [],
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
          fatalPending: true,
        });
        yield* diagnostics.close();
      }),
  );

  it.effect(
    "lets rejection shutdown cancellation suppress the returned settlement Effect and retry",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "rejection-cancellation-arbitration" },
                retry,
              ),
            },
          },
        });
        const callbackApplied = yield* Deferred.make<void>();
        const releaseHandoff = yield* Deferred.make<void>();
        const cancellationRequested = yield* Deferred.make<void>();
        let callbackCount = 0;
        let returnedEffectExecutions = 0;
        let settlementCommits = 0;
        const failedSettlementCounts: Array<bigint> = [];
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
          settlementHandoff: {
            afterCallbackApplication: Deferred.succeed(callbackApplied, undefined).pipe(
              Effect.andThen(Deferred.await(releaseHandoff)),
            ),
            afterFailureCounted: (count) =>
              Effect.sync(() => {
                failedSettlementCounts.push(count);
              }),
          },
          afterAttemptCancellationRequested: Deferred.succeed(
            cancellationRequested,
            undefined,
          ).pipe(Effect.asVoid),
        }).pipe(Effect.provide(fixture.layer));
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.reject(
          { _tag: "Materialized" },
          {
            _tag: "SourceFixtureFailure",
            message: "settled rejection",
            phase: "stream",
          },
          {
            lane: "fixture",
            offset: 1n,
          },
          () => {
            callbackCount += 1;
            return Effect.sync(() => {
              returnedEffectExecutions += 1;
            }).pipe(
              Effect.andThen(
                Effect.fail({
                  _tag: "SourceFixtureFailure" as const,
                  message: "cancelled returned failure",
                  phase: "settlement" as const,
                }),
              ),
              Effect.tap(() =>
                Effect.sync(() => {
                  settlementCommits += 1;
                }),
              ),
            );
          },
        );
        yield* Deferred.await(callbackApplied);

        const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Deferred.await(cancellationRequested);
        yield* Deferred.succeed(releaseHandoff, undefined);
        yield* Fiber.join(closeFiber);

        expect({
          callbackCount,
          failedSettlementCounts,
          returnedEffectExecutions,
          scheduleInputs,
          settlementCommits,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
          fatalPending: fatalFiber.pollUnsafe() === undefined,
        }).toStrictEqual({
          callbackCount: 1,
          failedSettlementCounts: [],
          returnedEffectExecutions: 0,
          scheduleInputs: [],
          settlementCommits: 0,
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
          fatalPending: true,
        });
      }),
  );

  it.effect(
    "interrupts and joins a started rejection settlement Effect in the Source Attempt Scope",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "started-rejection-settlement-ownership" },
                retry,
              ),
            },
          },
        });
        const returnedEffectStarted = yield* Deferred.make<void>();
        const returnedEffectFinalized = yield* Deferred.make<void>();
        let callbackCount = 0;
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
          Effect.provide(fixture.layer),
        );
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.reject(
          { _tag: "Materialized" },
          SourceFixture.failure("owned rejection settlement", "stream"),
          { lane: "fixture", offset: 2n },
          () => {
            callbackCount += 1;
            return Deferred.succeed(returnedEffectStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(returnedEffectFinalized, undefined).pipe(Effect.asVoid),
              ),
            );
          },
        );
        yield* Deferred.await(returnedEffectStarted);
        yield* manager.close;
        yield* Deferred.await(returnedEffectFinalized);

        expect({
          callbackCount,
          scheduleInputs,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
          fatalPending: fatalFiber.pollUnsafe() === undefined,
        }).toStrictEqual({
          callbackCount: 1,
          scheduleInputs: [],
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
          fatalPending: true,
        });
      }),
  );

  it.effect("atomically promotes a returned settlement Effect against shutdown cancellation", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
      const retry = Schedule.spaced("1 second").pipe(
        Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
        Schedule.tap(({ input: termination }) =>
          Effect.sync(() => {
            scheduleInputs.push(termination);
          }),
        ),
      );
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "delivery-returned-effect-cancellation-arbitration" },
              retry,
            ),
          },
        },
      });
      const promotionEntered = yield* Deferred.make<void>();
      const releasePromotion = yield* Deferred.make<void>();
      const cancellationRequested = yield* Deferred.make<void>();
      let callbackCount = 0;
      const failedSettlementCounts: Array<bigint> = [];
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        settlementHandoff: {
          duringReturnedEffectPromotion: Deferred.succeed(promotionEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releasePromotion)),
          ),
          afterFailureCounted: (count) =>
            Effect.sync(() => {
              failedSettlementCounts.push(count);
            }),
        },
        afterAttemptCancellationRequested: Deferred.succeed(cancellationRequested, undefined).pipe(
          Effect.asVoid,
        ),
      }).pipe(Effect.provide(fixture.layer));
      const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "cancelled-delivery-effect", region: "eu", value: "value" },
        () => {
          callbackCount += 1;
          return Effect.never;
        },
      );
      yield* Deferred.await(promotionEntered);

      const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Effect.yieldNow;
      expect({
        cancellationCompleted: yield* Deferred.isDone(cancellationRequested),
        closeCompleted: closeFiber.pollUnsafe() !== undefined,
      }).toStrictEqual({
        cancellationCompleted: false,
        closeCompleted: false,
      });
      yield* Deferred.succeed(releasePromotion, undefined);
      yield* Deferred.await(cancellationRequested);
      yield* Fiber.join(closeFiber);

      expect({
        callbackCount,
        failedSettlementCounts,
        scheduleInputs,
        counts: fixture.controls.counts({ _tag: "Materialized" }),
        fatalPending: fatalFiber.pollUnsafe() === undefined,
      }).toStrictEqual({
        callbackCount: 1,
        failedSettlementCounts: [],
        scheduleInputs: [],
        counts: {
          acquisitions: 1n,
          finalizations: 1n,
        },
        fatalPending: true,
      });
    }),
  );

  it.effect(
    "lets cancellation win after a returned settlement failure but before supervision handoff",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "returned-failure-cancellation-arbitration" },
                retry,
              ),
            },
          },
        });
        const beforeOrdinaryClaim = yield* Deferred.make<void>();
        const releaseOrdinaryClaim = yield* Deferred.make<void>();
        const cancellationRequested = yield* Deferred.make<void>();
        let callbackCount = 0;
        let returnedEffectExecutions = 0;
        const failureCounted = yield* Deferred.make<bigint>();
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
          settlementHandoff: {
            afterFailureCounted: (count) =>
              Deferred.succeed(failureCounted, count).pipe(Effect.asVoid),
            beforeOrdinaryTerminationClaim: Deferred.succeed(beforeOrdinaryClaim, undefined).pipe(
              Effect.andThen(Deferred.await(releaseOrdinaryClaim)),
            ),
          },
          afterAttemptCancellationRequested: Deferred.succeed(
            cancellationRequested,
            undefined,
          ).pipe(Effect.asVoid),
        }).pipe(Effect.provide(fixture.layer));
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.upsert(
          { _tag: "Materialized" },
          { id: "cancelled-returned-failure", region: "eu", value: "value" },
          () => {
            callbackCount += 1;
            return Effect.sync(() => {
              returnedEffectExecutions += 1;
            }).pipe(
              Effect.andThen(
                Effect.fail({
                  _tag: "SourceFixtureFailure" as const,
                  message: "returned failure before cancellation",
                  phase: "settlement" as const,
                }),
              ),
            );
          },
        );
        yield* Deferred.await(beforeOrdinaryClaim);
        expect(yield* Deferred.await(failureCounted)).toBe(1n);

        const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Deferred.await(cancellationRequested);
        yield* Deferred.succeed(releaseOrdinaryClaim, undefined);
        yield* Fiber.join(closeFiber);

        expect({
          callbackCount,
          returnedEffectExecutions,
          scheduleInputs,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
        }).toStrictEqual({
          callbackCount: 1,
          returnedEffectExecutions: 1,
          scheduleInputs: [],
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
        });
        expect(fatalFiber.pollUnsafe()).toBeUndefined();
      }),
  );

  it.effect(
    "lets rejection cancellation win after returned failure without supervision or commit",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const scheduleInputs: Array<SourceTermination<SourceFixtureFailure>> = [];
        const retry = Schedule.spaced("1 second").pipe(
          Schedule.setInputType<SourceTermination<SourceFixtureFailure>>(),
          Schedule.tap(({ input: termination }) =>
            Effect.sync(() => {
              scheduleInputs.push(termination);
            }),
          ),
        );
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "rejection-returned-failure-cancellation-arbitration" },
                retry,
              ),
            },
          },
        });
        const beforeOrdinaryClaim = yield* Deferred.make<void>();
        const releaseOrdinaryClaim = yield* Deferred.make<void>();
        const cancellationRequested = yield* Deferred.make<void>();
        const failureCounted = yield* Deferred.make<bigint>();
        let callbackCount = 0;
        let returnedEffectExecutions = 0;
        let successfulSettlementCommits = 0;
        const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
        const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
          settlementHandoff: {
            afterFailureCounted: (count) =>
              Deferred.succeed(failureCounted, count).pipe(Effect.asVoid),
            beforeOrdinaryTerminationClaim: Deferred.succeed(beforeOrdinaryClaim, undefined).pipe(
              Effect.andThen(Deferred.await(releaseOrdinaryClaim)),
            ),
          },
          afterAttemptCancellationRequested: Deferred.succeed(
            cancellationRequested,
            undefined,
          ).pipe(Effect.asVoid),
        }).pipe(Effect.provide(fixture.layer));
        const diagnostics = yield* manager.subscribeSourceHealth({ topic: "rows" });
        const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
        yield* fixture.controls.awaitActive({ _tag: "Materialized" });
        yield* fixture.controls.reject(
          { _tag: "Materialized" },
          SourceFixture.failure("rejected before returned failure", "stream"),
          { lane: "fixture", offset: 23n },
          () => {
            callbackCount += 1;
            return Effect.sync(() => {
              returnedEffectExecutions += 1;
            }).pipe(
              Effect.andThen(
                Effect.fail(SourceFixture.failure("returned settlement failed", "settlement")),
              ),
              Effect.tap(() =>
                Effect.sync(() => {
                  successfulSettlementCommits += 1;
                }),
              ),
            );
          },
        );
        yield* Deferred.await(beforeOrdinaryClaim);
        expect(yield* Deferred.await(failureCounted)).toBe(1n);
        const degraded = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Degraded"),
            Stream.take(1),
            Stream.runHead,
          ),
        );

        const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
        yield* Deferred.await(cancellationRequested);
        yield* Deferred.succeed(releaseOrdinaryClaim, undefined);
        yield* Fiber.join(closeFiber);

        expect({
          callbackCount,
          returnedEffectExecutions,
          successfulSettlementCommits,
          rejectedItemCount: degraded.metrics.runtime.rejectedItemCount,
          rejectionReason:
            degraded.status._tag === "Degraded" ? degraded.status.reasons : undefined,
          scheduleInputs,
          counts: fixture.controls.counts({ _tag: "Materialized" }),
          fatalPending: fatalFiber.pollUnsafe() === undefined,
        }).toStrictEqual({
          callbackCount: 1,
          returnedEffectExecutions: 1,
          successfulSettlementCommits: 0,
          rejectedItemCount: 1n,
          rejectionReason: [
            {
              _tag: "SourceItemRejection",
              latestRejection: {
                failure: {
                  _tag: "AdapterFailure",
                  failure: {
                    _tag: "SourceFixtureFailure",
                    message: "rejected before returned failure",
                    phase: "stream",
                  },
                },
                location: {
                  lane: "fixture",
                  offset: 23n,
                },
                rejectedAtNanos: degraded.sampledAtNanos,
              },
            },
          ],
          scheduleInputs: [],
          counts: {
            acquisitions: 1n,
            finalizations: 1n,
          },
          fatalPending: true,
        });
        yield* diagnostics.close();
      }),
  );

  it.effect("lets shutdown cancellation win before an ordinary stream-failure handoff", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "stream-failure-cancellation-arbitration" },
              Schedule.forever,
            ),
          },
        },
      });
      const beforeAttemptClaim = yield* Deferred.make<void>();
      const releaseAttemptClaim = yield* Deferred.make<void>();
      const cancellationRequested = yield* Deferred.make<void>();
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        beforeAttemptTerminationClaim: Deferred.succeed(beforeAttemptClaim, undefined).pipe(
          Effect.andThen(Deferred.await(releaseAttemptClaim)),
        ),
        afterAttemptCancellationRequested: Deferred.succeed(cancellationRequested, undefined).pipe(
          Effect.asVoid,
        ),
      }).pipe(Effect.provide(fixture.layer));
      const fatalFiber = yield* manager.fatal.pipe(Effect.exit, Effect.forkChild());
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.fail(
        { _tag: "Materialized" },
        {
          _tag: "SourceFixtureFailure",
          message: "stream failure before cancellation",
          phase: "stream",
        },
      );
      yield* Deferred.await(beforeAttemptClaim);

      const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Deferred.await(cancellationRequested);
      yield* Deferred.succeed(releaseAttemptClaim, undefined);
      yield* Fiber.join(closeFiber);

      expect(fixture.controls.counts({ _tag: "Materialized" })).toStrictEqual({
        acquisitions: 1n,
        finalizations: 1n,
      });
      expect(fatalFiber.pollUnsafe()).toBeUndefined();
    }),
  );

  it.effect("owns a handed-off settlement Effect until the attempt scope closes", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "settlement-attempt-ownership" },
              Schedule.forever,
            ),
          },
        },
      });
      const returnedEffectStarted = yield* Deferred.make<void>();
      const returnedEffectFinalized = yield* Deferred.make<void>();
      let callbackCount = 0;
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.upsert(
        { _tag: "Materialized" },
        { id: "owned-settlement", region: "eu", value: "value" },
        (applicationExit) => {
          callbackCount += 1;
          expect(applicationExit).toStrictEqual(Exit.void);
          return Deferred.succeed(returnedEffectStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Deferred.succeed(returnedEffectFinalized, undefined).pipe(Effect.asVoid),
            ),
          );
        },
      );
      yield* Deferred.await(returnedEffectStarted);
      expect(callbackCount).toBe(1);

      const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Deferred.await(returnedEffectFinalized);
      yield* Fiber.join(closeFiber);
      expect(callbackCount).toBe(1);
    }),
  );

  it.effect("rejects a nominal transition delivered to a stateless logical lifetime", () =>
    Effect.gen(function* () {
      const Failure = Schema.TaggedStruct("StatelessTransitionFailure", {
        message: Schema.String,
      });
      const adapter = SourceAdapter.make({
        identity: { name: "stateless-transition" },
        failure: Failure,
        materialized: {
          metrics: Schema.Struct({ observed: Schema.BigInt }),
          rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const emitTransition = yield* Deferred.make<void>();
      let transitionApplied = false;
      const layer = SourceAdapterServer.make(adapter, {
        materialized: {
          initialLaneIds: () => ["stateless-transition"],
          acquire: (input) =>
            Effect.gen(function* () {
              const mutation = yield* input.toolkit.delete("stateless-transition-row");
              const transition = makeSourceApplicationTransition(
                input.toolkit.topic,
                () => {
                  transitionApplied = true;
                },
                [],
                Object.freeze({}),
              );
              const delivery = yield* input.toolkit.delivery(
                mutation,
                () => Effect.void,
                transition,
              );
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "stateless-transition",
                  events: Stream.fromEffect(
                    Deferred.await(emitTransition).pipe(Effect.as(delivery)),
                  ).pipe(Stream.concat(Stream.never)),
                }),
              ]);
            }),
          metrics: () => Effect.succeed({ observed: 0n }),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: adapter.materializedSource(undefined),
          },
        },
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
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
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(layer),
      );
      yield* Deferred.succeed(emitTransition, undefined);
      const fatalExit = yield* Effect.exit(manager.fatal);
      const fatalCause = Option.getOrThrow(Exit.getCause(fatalExit));

      expect({
        defect: Result.getOrThrow(Cause.findDefect(fatalCause)),
        failure: Option.getOrThrow(Cause.findErrorOption(fatalCause)),
        transitionApplied,
      }).toStrictEqual({
        defect: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message:
            "Source Application Transition is bound to a different Topic or logical lifetime.",
        },
        failure: {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          topic: "rows",
          message: "Source application transition failed and stopped the complete runtime.",
        },
        transitionApplied: false,
      });
      yield* manager.close;
    }),
  );
});
