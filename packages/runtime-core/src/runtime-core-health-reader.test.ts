import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import {
  healthFromEngine,
  makeCoalescedHealthReader,
  makeHealthRefreshScheduler,
  readHealth,
} from "./health";
import { engineHealth } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("coalesces concurrent health reads while an active same-epoch read is running", () =>
    Effect.gen(function* () {
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return healthFromEngine(engineHealth("ready", readCount));
          }),
        () => 0,
      );

      const first = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(readStarted);
      const second = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseRead, undefined);
      const [firstHealth, secondHealth] = yield* Effect.all(
        [Fiber.join(first), Fiber.join(second)],
        {
          concurrency: 2,
        },
      ).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(1);
      expect(firstHealth.engine.topics.orders.rowCount).toBe(1);
      expect(secondHealth.engine.topics.orders.rowCount).toBe(1);
      const thirdHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(thirdHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("clears the active health read after a failed read so the next read retries", () =>
    Effect.gen(function* () {
      let readCount = 0;
      const healthReads = [
        Effect.fail("boom"),
        Effect.succeed(healthFromEngine(engineHealth("ready", 2))),
      ];
      const coalescedHealth = makeCoalescedHealthReader(() =>
        Effect.gen(function* () {
          const nextRead =
            healthReads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
          readCount += 1;
          return yield* nextRead;
        }),
      );

      const failedHealth = yield* Effect.flip(coalescedHealth());
      expect(failedHealth).toBe("boom");
      const recoveredHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(recoveredHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("does not strand followers when the active health reader is interrupted", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const followerJoinedActiveRead = yield* Deferred.make<void>();
      let epochCheckCount = 0;
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(firstReadStarted, undefined);
            yield* Deferred.await(releaseFirstRead);
            return healthFromEngine(engineHealth("ready", readCount));
          }),
        () => {
          epochCheckCount += 1;
          if (epochCheckCount === 2) {
            Deferred.doneUnsafe(followerJoinedActiveRead, Effect.void);
          }
          return 0;
        },
      );

      const leader = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstReadStarted);
      const follower = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(followerJoinedActiveRead).pipe(
        Effect.timeout("1 second"),
        Effect.onError(() => Deferred.succeed(releaseFirstRead, undefined).pipe(Effect.asVoid)),
      );

      const interruptStarted = yield* Deferred.make<void>();
      const interruptLeader = yield* Effect.gen(function* () {
        yield* Deferred.succeed(interruptStarted, undefined);
        yield* Fiber.interrupt(leader);
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(interruptStarted);
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(interruptLeader);
      const followerHealth = yield* Fiber.join(follower);
      const nextHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(followerHealth.engine.topics.orders.rowCount).toBe(1);
      expect(nextHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("starts a fresh health read after the caller epoch changes", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      let epoch = 0;
      let readCount = 0;
      const healthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return healthFromEngine(engineHealth("ready", 1));
        }),
        Effect.succeed(healthFromEngine(engineHealth("ready", 2))),
      ];
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            const nextRead =
              healthReads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
            readCount += 1;
            return yield* nextRead;
          }),
        () => epoch,
      );

      const staleHealthRead = yield* coalescedHealth().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(firstReadStarted);
      epoch += 1;
      const freshHealth = yield* coalescedHealth();
      yield* Deferred.succeed(releaseFirstRead, undefined);
      const staleHealth = yield* Fiber.join(staleHealthRead);

      expect(readCount).toBe(2);
      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(staleHealth.engine.topics.orders.rowCount).toBe(1);
    }),
  );

  it.effect("does not let obsolete epoch health reads overwrite fresher cached health", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let epoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return engineHealth("ready", 1);
        }),
        Effect.succeed(engineHealth("ready", 2)),
      ];
      const engine = {
        health: () => {
          const nextRead = engineHealthReads[readCount] ?? Effect.succeed(engineHealth("ready", 3));
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };
      const coalescedHealth = makeCoalescedHealthReader(
        (readEpoch) =>
          readHealth(engine, health, {
            shouldInstall: () => epoch === readEpoch,
          }),
        () => epoch,
      );

      const obsoleteHealthRead = yield* coalescedHealth().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(firstReadStarted);
      epoch += 1;
      const freshHealth = yield* coalescedHealth();
      yield* Deferred.succeed(releaseFirstRead, undefined);
      const obsoleteHealth = yield* Fiber.join(obsoleteHealthRead);

      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(obsoleteHealth.engine.topics.orders.rowCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("does not let older scheduled health reads overwrite newer installed health", () =>
    Effect.gen(function* () {
      const scheduledReadStarted = yield* Deferred.make<void>();
      const releaseScheduledRead = yield* Deferred.make<void>();
      const scheduledReadFinished = yield* Deferred.make<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let installEpoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(scheduledReadStarted, undefined);
          yield* Deferred.await(releaseScheduledRead);
          return engineHealth("ready", 1);
        }),
        Effect.succeed(engineHealth("ready", 2)),
      ];
      const engine = {
        health: () => {
          const nextRead = engineHealthReads[readCount] ?? Effect.succeed(engineHealth("ready", 3));
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const readInstallEpoch = installEpoch;
          yield* readHealth(engine, health, {
            shouldInstall: () => installEpoch === readInstallEpoch,
            onInstall: () => {
              installEpoch += 1;
            },
          });
          yield* Deferred.succeed(scheduledReadFinished, undefined);
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(scheduledReadStarted).pipe(Effect.timeout("1 second"));
      const freshHealth = yield* readHealth(engine, health, {
        onInstall: () => {
          installEpoch += 1;
        },
      });
      yield* Deferred.succeed(releaseScheduledRead, undefined);
      yield* Deferred.await(scheduledReadFinished).pipe(Effect.timeout("1 second"));

      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
      yield* scheduler.close;
    }),
  );
});
