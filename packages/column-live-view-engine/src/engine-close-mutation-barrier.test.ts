import { describe, expect, it } from "@effect/vitest";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Option, Schema } from "effect";
import { createColumnLiveViewEngine, EngineClosedError, InvalidRowError } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";
import { publishTopicStoreRow, type TopicStoreMutationAdmission } from "./topic-store-mutation";
import { TopicStore, topicStoreState } from "./topic-store-state";

const Row = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

const blockingTerminalObserver = (
  terminalStarted: Deferred.Deferred<void>,
  continueTerminal: Deferred.Deferred<void>,
) => ({
  onQueryRegistered: () => Effect.void,
  onTerminalOccurrence: () =>
    Effect.gen(function* () {
      yield* Deferred.succeed(terminalStarted, undefined);
      yield* Deferred.await(continueTerminal);
    }),
  onTerminalReady: () => Effect.void,
});

describe("ColumnLiveViewEngine close mutation barrier", () => {
  it.effect("rejects a mutation when lifecycle closes admission after row preparation", () =>
    Effect.gen(function* () {
      let mutationsAllowed = true;
      const closeAdmission: TopicStoreMutationAdmission = (transaction) =>
        Effect.sync(() => {
          mutationsAllowed = false;
        }).pipe(Effect.andThen(transaction));
      const store = new TopicStore(
        "rows",
        Row,
        "id",
        () => {},
        () => mutationsAllowed,
        closeAdmission,
      );

      const error = yield* publishTopicStoreRow(store, { id: "late", value: 1 }, (topic, message) =>
        InvalidRowError.make({ topic, message }),
      ).pipe(Effect.flip);
      const state = topicStoreState(store);

      expect(error).toBeInstanceOf(EngineClosedError);
      expect(state.storage.rowCount).toBe(0);
      expect(state.storage.version).toBe(0);
    }),
  );

  it.effect("rejects every public and internal mutation entrypoint after close", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          rows: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("rows", { id: "existing", value: 1 });
      yield* engine.close();

      const errors = yield* Effect.forEach(
        [
          engine.publish("rows", { id: "publish", value: 2 }),
          engine.publishMany("rows", [{ id: "publish-many", value: 3 }]),
          engine.patch("rows", "existing", { value: 4 }),
          engine.delete("rows", "existing"),
          engine.publishManyDecodedRows("rows", [{ id: "decoded", value: 5 }]),
          engine.publishManyDecodedRowsWithStorageKeys("rows", [
            {
              storageKey: "decoded-storage-key",
              row: { id: "decoded-storage", value: 6 },
            },
          ]),
          engine.publishManyWithStorageKeys("rows", [
            {
              storageKey: "storage-key",
              row: { id: "storage", value: 7 },
            },
          ]),
          engine.patchDecodedFields("rows", "existing", { value: 8 }),
          engine.reset(),
        ],
        Effect.flip,
      );
      const health = yield* engine.health();

      expect(errors.map((error) => error._tag)).toStrictEqual([
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
        "EngineClosedError",
      ]);
      expect(health.topics.rows.rowCount).toBe(1);
      expect(health.version).toBe(1);
    }),
  );

  it.effect("does not run an interrupted close waiting for active mutations", () =>
    Effect.gen(function* () {
      const mutationStarted = yield* Deferred.make<void>();
      const continueMutation = yield* Deferred.make<void>();
      const blockedMutationClock: Clock.Clock = {
        currentTimeMillisUnsafe: () => 0,
        currentTimeMillis: Effect.gen(function* () {
          yield* Deferred.succeed(mutationStarted, undefined);
          yield* Deferred.await(continueMutation);
          return 0;
        }),
        currentTimeNanosUnsafe: () => 0n,
        currentTimeNanos: Effect.succeed(0n),
        sleep: () => Effect.void,
      };
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          rows: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("rows", { id: "existing", value: 1 });
      const patchFiber = yield* engine
        .patch("rows", "existing", { value: 2 })
        .pipe(
          Effect.provideService(Clock.Clock, blockedMutationClock),
          Effect.forkChild({ startImmediately: true }),
        );
      yield* Deferred.await(mutationStarted);
      const closeFiber = yield* engine.close().pipe(Effect.forkChild({ startImmediately: true }));
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Deferred.succeed(continueMutation, undefined);
      yield* Fiber.join(patchFiber);
      yield* Fiber.join(interruptFiber);
      const closeExit = yield* Fiber.await(closeFiber);
      const closeCause = Exit.getCause(closeExit).pipe(Option.getOrThrow);
      yield* engine.publish("rows", { id: "after-interrupt", value: 1 });
      const health = yield* engine.health();

      expect(Cause.hasInterruptsOnly(closeCause)).toBe(true);
      expect(health.status).toBe("ready");
      expect(health.topics.rows.rowCount).toBe(2);
      expect(health.topics.rows.version).toBe(3);
      expect(health.version).toBe(3);
    }),
  );

  it.effect("admits concurrent mutations on independent topics", () =>
    Effect.gen(function* () {
      const firstMutationStarted = yield* Deferred.make<void>();
      const secondMutationStarted = yield* Deferred.make<void>();
      const continueFirstMutation = yield* Deferred.make<void>();
      const continueSecondMutation = yield* Deferred.make<void>();
      const firstClock: Clock.Clock = {
        currentTimeMillisUnsafe: () => 0,
        currentTimeMillis: Effect.gen(function* () {
          yield* Deferred.succeed(firstMutationStarted, undefined);
          yield* Deferred.await(continueFirstMutation);
          return 0;
        }),
        currentTimeNanosUnsafe: () => 0n,
        currentTimeNanos: Effect.succeed(0n),
        sleep: () => Effect.void,
      };
      const secondClock: Clock.Clock = {
        currentTimeMillisUnsafe: () => 0,
        currentTimeMillis: Effect.gen(function* () {
          yield* Deferred.succeed(secondMutationStarted, undefined);
          yield* Deferred.await(continueSecondMutation);
          return 0;
        }),
        currentTimeNanosUnsafe: () => 0n,
        currentTimeNanos: Effect.succeed(0n),
        sleep: () => Effect.void,
      };
      const engine = yield* createColumnLiveViewEngine({
        topics: {
          first: {
            schema: Row,
            key: "id",
          },
          second: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("first", { id: "first", value: 1 });
      yield* engine.publish("second", { id: "second", value: 2 });

      const firstPatchFiber = yield* engine
        .patch("first", "first", { value: 3 })
        .pipe(
          Effect.provideService(Clock.Clock, firstClock),
          Effect.forkChild({ startImmediately: true }),
        );
      yield* Deferred.await(firstMutationStarted);
      const secondPatchFiber = yield* engine
        .patch("second", "second", { value: 4 })
        .pipe(
          Effect.provideService(Clock.Clock, secondClock),
          Effect.forkChild({ startImmediately: true }),
        );
      yield* Deferred.await(secondMutationStarted);
      const secondMutationActiveAlongsideFirst = secondPatchFiber.pollUnsafe() === undefined;

      yield* Deferred.succeed(continueFirstMutation, undefined);
      yield* Fiber.join(firstPatchFiber);
      const secondMutationStillActive = secondPatchFiber.pollUnsafe() === undefined;
      yield* Deferred.succeed(continueSecondMutation, undefined);
      yield* Fiber.join(secondPatchFiber);
      const firstSnapshot = yield* engine.snapshot("first", { select: ["id", "value"] });
      const secondSnapshot = yield* engine.snapshot("second", { select: ["id", "value"] });

      expect(secondMutationActiveAlongsideFirst).toBe(true);
      expect(secondMutationStillActive).toBe(true);
      expect(firstSnapshot.rows).toStrictEqual([{ id: "first", value: 3 }]);
      expect(secondSnapshot.rows).toStrictEqual([{ id: "second", value: 4 }]);
    }),
  );

  it.effect("does not run an interrupted mutation waiting behind reset", () =>
    Effect.gen(function* () {
      const terminalStarted = yield* Deferred.make<void>();
      const continueTerminal = yield* Deferred.make<void>();
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          first: {
            schema: Row,
            key: "id",
          },
          second: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("first", { id: "first", value: 1 });
      yield* engine.publish("second", { id: "second", value: 2 });
      yield* engine.subscribeRuntimeObserved(
        "second",
        { select: ["id"] },
        blockingTerminalObserver(terminalStarted, continueTerminal),
      );

      const resetFiber = yield* engine.reset().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(terminalStarted);
      const publishFiber = yield* engine
        .publish("first", { id: "interrupted", value: 3 })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const publishWaitingForReset = publishFiber.pollUnsafe() === undefined;
      yield* Fiber.interrupt(publishFiber);
      const publishExit = yield* Fiber.await(publishFiber);
      const publishCause = Exit.getCause(publishExit).pipe(Option.getOrThrow);

      yield* Deferred.succeed(continueTerminal, undefined);
      yield* Fiber.join(resetFiber);
      yield* engine.publish("first", { id: "after-reset", value: 4 });
      const health = yield* engine.health();
      const snapshot = yield* engine.snapshot("first", { select: ["id", "value"] });

      expect(publishWaitingForReset).toBe(true);
      expect(Cause.hasInterruptsOnly(publishCause)).toBe(true);
      expect(health.version).toBe(1);
      expect(health.topics.first.version).toBe(1);
      expect(snapshot.rows).toStrictEqual([{ id: "after-reset", value: 4 }]);
    }),
  );

  it.effect("serializes a write to an already-cleared topic after a multi-topic reset", () =>
    Effect.gen(function* () {
      const terminalStarted = yield* Deferred.make<void>();
      const continueTerminal = yield* Deferred.make<void>();
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          first: {
            schema: Row,
            key: "id",
          },
          second: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("first", { id: "first", value: 1 });
      yield* engine.publish("second", { id: "second", value: 2 });
      yield* engine.subscribeRuntimeObserved(
        "second",
        { select: ["id"] },
        blockingTerminalObserver(terminalStarted, continueTerminal),
      );

      const resetFiber = yield* engine.reset().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(terminalStarted);
      const publishFiber = yield* engine
        .publish("first", { id: "after-reset", value: 3 })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const publishBlockedByReset = publishFiber.pollUnsafe() === undefined;
      const healthWhileResetBlocked = yield* engine.health();

      yield* Deferred.succeed(continueTerminal, undefined);
      yield* Fiber.join(resetFiber);
      yield* Fiber.join(publishFiber);
      const health = yield* engine.health();
      const snapshot = yield* engine.snapshot("first", {
        select: ["id", "value"],
      });

      expect(healthWhileResetBlocked.topics.first.rowCount).toBe(0);
      expect(healthWhileResetBlocked.topics.first.version).toBe(0);
      expect(publishBlockedByReset).toBe(true);
      expect(health.topics.first.rowCount).toBe(1);
      expect(health.topics.first.version).toBe(1);
      expect(health.topics.second.rowCount).toBe(0);
      expect(health.topics.second.version).toBe(0);
      expect(health.version).toBe(1);
      expect(snapshot).toStrictEqual({
        rows: [{ id: "after-reset", value: 3 }],
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 1,
      });
    }),
  );

  it.effect("finishes an admitted reset before a queued close", () =>
    Effect.gen(function* () {
      const terminalStarted = yield* Deferred.make<void>();
      const continueTerminal = yield* Deferred.make<void>();
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          rows: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("rows", { id: "existing", value: 1 });
      yield* engine.subscribeRuntimeObserved(
        "rows",
        { select: ["id"] },
        blockingTerminalObserver(terminalStarted, continueTerminal),
      );

      const resetFiber = yield* engine.reset().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(terminalStarted);
      const closeFiber = yield* engine.close().pipe(Effect.forkChild({ startImmediately: true }));
      const closeQueuedBehindReset = closeFiber.pollUnsafe() === undefined;
      const healthWhileResetBlocked = yield* engine.health();

      yield* Deferred.succeed(continueTerminal, undefined);
      yield* Fiber.join(resetFiber);
      yield* Fiber.join(closeFiber);
      const health = yield* engine.health();

      expect(closeQueuedBehindReset).toBe(true);
      expect(healthWhileResetBlocked.status).toBe("ready");
      expect(health.status).toBe("stopping");
      expect(health.topics.rows.rowCount).toBe(0);
      expect(health.topics.rows.version).toBe(0);
      expect(health.version).toBe(0);
    }),
  );

  it.effect("rejects a reset queued behind an admitted close before reset side effects", () =>
    Effect.gen(function* () {
      const terminalStarted = yield* Deferred.make<void>();
      const continueTerminal = yield* Deferred.make<void>();
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: {
          rows: {
            schema: Row,
            key: "id",
          },
        },
      });
      yield* engine.publish("rows", { id: "existing", value: 1 });
      yield* engine.subscribeRuntimeObserved(
        "rows",
        { select: ["id"] },
        blockingTerminalObserver(terminalStarted, continueTerminal),
      );

      const closeFiber = yield* engine.close().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(terminalStarted);
      const resetFiber = yield* engine.reset().pipe(Effect.forkChild({ startImmediately: true }));
      const resetQueuedBehindClose = resetFiber.pollUnsafe() === undefined;
      const healthWhileCloseBlocked = yield* engine.health();

      yield* Deferred.succeed(continueTerminal, undefined);
      yield* Fiber.join(closeFiber);
      const resetError = yield* Fiber.join(resetFiber).pipe(Effect.flip);
      const health = yield* engine.health();

      expect(resetQueuedBehindClose).toBe(true);
      expect(healthWhileCloseBlocked.status).toBe("stopping");
      expect(resetError).toBeInstanceOf(EngineClosedError);
      expect(health.status).toBe("stopping");
      expect(health.topics.rows.rowCount).toBe(1);
      expect(health.topics.rows.version).toBe(1);
      expect(health.version).toBe(1);
    }),
  );
});
