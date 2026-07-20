import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Queue } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine, makeHealthRefreshScheduler, readHealth } from "./health";
import { makeViewServerRuntimeCore } from "./index";
import { engineHealth, order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("lets scheduled health refreshes install and follow up while requests continue", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const secondReadStarted = yield* Deferred.make<void>();
      const releaseSecondRead = yield* Deferred.make<void>();
      const refreshCompleted = yield* Queue.unbounded<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let installEpoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return engineHealth("ready", 1);
        }),
        Effect.gen(function* () {
          yield* Deferred.succeed(secondReadStarted, undefined);
          yield* Deferred.await(releaseSecondRead);
          return engineHealth("ready", 2);
        }),
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
          yield* Queue.offer(refreshCompleted, undefined);
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(firstReadStarted).pipe(Effect.timeout("1 second"));
      yield* scheduler.request;
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Queue.take(refreshCompleted).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(secondReadStarted).pipe(Effect.timeout("1 second"));
      expect(health.value.engine.topics.orders.rowCount).toBe(1);
      yield* Deferred.succeed(releaseSecondRead, undefined);
      yield* Queue.take(refreshCompleted).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("maps engine errors into runtime errors", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const invalidTopic = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        runtimeCore.client.publish("missing", order("b", 20)),
      );
      const invalidRow = yield* Effect.flip(
        runtimeCore.client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      );
      const groupedQuery = yield* runtimeCore.client.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const invalidQuery = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown projected fields.
        runtimeCore.client.snapshot("orders", {
          select: ["prcie"],
        }),
      );

      yield* runtimeCore.close;
      const runtimeUnavailable = yield* Effect.flip(
        runtimeCore.client.publish("orders", order("closed", 30)),
      );

      expect(invalidTopic.code).toBe("InvalidTopic");
      expect(invalidRow.code).toBe("InvalidRow");
      expect(groupedQuery.rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(runtimeUnavailable.code).toBe("RuntimeUnavailable");
    }),
  );

  it.effect(
    "queues a trailing health scheduler refresh when requested while refresh is pending",
    () =>
      Effect.gen(function* () {
        const firstStarted = yield* Deferred.make<void>();
        const firstFinished = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        const secondFinished = yield* Deferred.make<void>();
        let refreshCount = 0;
        const refreshSteps = [
          { started: firstStarted, finished: firstFinished },
          { started: secondStarted, finished: secondFinished },
        ] as const;

        const scheduler = yield* makeHealthRefreshScheduler(
          Effect.gen(function* () {
            const refreshStep = refreshSteps[refreshCount] ?? refreshSteps[1];
            yield* Effect.sync(() => {
              refreshCount += 1;
            });
            yield* Deferred.succeed(refreshStep.started, undefined);
            yield* Deferred.await(refreshStep.finished);
          }),
          "0 millis",
        );

        yield* scheduler.request;
        yield* Deferred.await(firstStarted);

        yield* scheduler.request;
        yield* Deferred.succeed(firstFinished, undefined);
        yield* Deferred.await(secondStarted);

        expect(refreshCount).toBe(2);
        yield* Deferred.succeed(secondFinished, undefined);
        yield* scheduler.close;
      }),
  );

  it.effect("closes a sleeping health scheduler refresh fiber", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "1 minute",
      );

      yield* scheduler.request;
      yield* scheduler.close;

      expect(refreshCount).toBe(0);
      yield* scheduler.close;
    }),
  );

  it.effect("clears active health scheduler state when a refresh interrupts itself", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const startedSignals = [firstStarted, secondStarted] as const;
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const started = startedSignals[refreshCount] ?? secondStarted;
          yield* Effect.sync(() => {
            refreshCount += 1;
          });
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.interrupt;
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(firstStarted);
      yield* Effect.yieldNow;
      yield* scheduler.request;
      yield* Deferred.await(secondStarted).pipe(Effect.timeout("1 second"));

      expect(refreshCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("ignores health scheduler refresh requests after close", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "0 millis",
      );

      yield* scheduler.close;
      yield* scheduler.request;

      expect(refreshCount).toBe(0);
    }),
  );
});
