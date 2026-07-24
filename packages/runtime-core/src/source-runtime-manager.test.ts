import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import {
  SourceFixture,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import type { SourceApplicationExit } from "@effect-view-server/source-adapter";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schedule, Schema } from "effect";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import { makeRuntimeCoreSourceManager } from "./source-runtime";

const Row = Schema.Struct({
  id: Schema.String,
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
      yield* Fiber.interrupt(interruptedAcquisition);
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: 1n,
        finalizations: 1n,
      });

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
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () =>
          failDeleteStorageKey ? Effect.fail(mutationFailure()) : Effect.void,
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.void,
        publishManyDecodedRowsWithStorageKeys: () =>
          failPublish ? Effect.fail(mutationFailure()) : Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations).pipe(
        Effect.provide(fixture.layer),
      );

      const firstDiagnostics = yield* manager.subscribeSourceHealth("rows", { region: "eu" });
      const secondDiagnostics = yield* manager.subscribeSourceHealth("rows", { region: "eu" });
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
      failDeleteStorageKey = true;
      yield* secondLease.release;
      yield* fixture.controls.awaitCounts(leasedTarget, {
        acquisitions: beforeFailedCleanup.acquisitions,
        finalizations: beforeFailedCleanup.finalizations + 1n,
      });

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
      const settlementStarted = yield* Deferred.make<void>();
      const releaseSettlement = yield* Deferred.make<void>();
      let settlementCount = 0;
      let settledExit: SourceApplicationExit | undefined;
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
        (exit) =>
          Effect.sync(() => {
            settlementCount += 1;
            settledExit = exit;
          }).pipe(
            Effect.andThen(Deferred.succeed(settlementStarted, undefined)),
            Effect.andThen(Deferred.await(releaseSettlement)),
          ),
      );
      yield* Deferred.await(applicationStarted);
      const closeFiber = yield* manager.close.pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Deferred.await(settlementStarted);
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(
        Effect.forkDetach({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      expect(settlementCount).toBe(1);
      yield* Deferred.succeed(releaseSettlement, undefined);
      yield* Fiber.join(interruptFiber);

      expect(settlementCount).toBe(1);
      expect(
        settledExit !== undefined &&
          Exit.isFailure(settledExit) &&
          Cause.hasInterrupts(settledExit.cause),
      ).toBe(true);
    }),
  );
});
