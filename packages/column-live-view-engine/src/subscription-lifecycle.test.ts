import { describe, expect, it } from "@effect/vitest";
import { type StatusEvent } from "@effect-view-server/config";
import { Deferred, Effect, Exit, Fiber, Option, Stream } from "effect";
import { defaultGroupedIncrementalAdmissionLimits, InvalidRowError } from "./index";
import { TopicRowStorage } from "./topic-row-storage";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeQueryTestInterface,
  activeQueryTestInterfaceForStorage,
  activeQueryTestMetadata,
  activeStoreRawQueryExecutionCount,
  clearStoreRawQueryExecutions,
  releaseMaterializedQueryExecution,
} from "../test-harness/active-query-interface";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import {
  acquireSubscriptionHandoff,
  closeInterruptedAcquiredSubscription,
} from "./subscription-handoff";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { prepareRuntimeGroupedQuery } from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import { makeQueryResultSemantics } from "./query-result-semantics";
import {
  acquireTopicStoreSubscription,
  closeBackpressuredTopicStoreSubscription,
  closeTopicStoreSubscriptions,
  collectTopicStoreHealth,
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  registerTopicStoreSubscription,
  resetTopicStore,
  TopicStore,
} from "./topic-store";
import { expectDefined } from "../test-harness/events";
import { order, Order } from "../test-harness/public-engine";
import { registerTestTopicStoreSubscriber } from "../test-harness/topic-store";

const emptyResultSemantics = makeQueryResultSemantics([]);

describe("Subscription lifecycle ownership", () => {
  it.effect("only closes acquired subscriptions for interrupted exits", () =>
    Effect.gen(function* () {
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      yield* closeInterruptedAcquiredSubscription(Exit.succeed(undefined), subscription);
      yield* closeInterruptedAcquiredSubscription(Exit.interrupt(1), undefined);
      expect(closeCount).toBe(0);

      yield* closeInterruptedAcquiredSubscription(Exit.interrupt(1), subscription);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("closes acquired subscriptions when handoff is interrupted", () =>
    Effect.gen(function* () {
      const acquired = yield* Deferred.make<void>();
      const keepHandoffOpen = yield* Deferred.make<void>();
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      const handoffFiber = yield* Effect.forkChild(
        acquireSubscriptionHandoff(
          (markAcquired) =>
            Effect.gen(function* () {
              yield* markAcquired(subscription);
              return subscription;
            }),
          {
            beforeReturn: Effect.gen(function* () {
              yield* Deferred.succeed(acquired, undefined);
              yield* Deferred.await(keepHandoffOpen);
            }),
          },
        ),
      );

      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(handoffFiber);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("closes acquired subscriptions when acquisition exits interrupted", () =>
    Effect.gen(function* () {
      const acquired = yield* Deferred.make<void>();
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      const handoffFiber = yield* Effect.forkChild(
        acquireSubscriptionHandoff((markAcquired) =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* markAcquired(subscription);
              yield* Deferred.succeed(acquired, undefined);
              yield* Effect.sleep("10 millis");
              return subscription;
            }),
          ),
        ),
      );

      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(handoffFiber);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("closes acquired subscriptions when topic-store acquisition is interrupted", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const acquired = yield* Deferred.make<void>();
      let closeCount = 0;
      const subscription = {
        events: Stream.empty,
        close: () =>
          Effect.sync(() => {
            closeCount += 1;
          }),
      };

      const handoffFiber = yield* Effect.forkChild(
        acquireTopicStoreSubscription(store, (_permit, markAcquired) =>
          Effect.uninterruptible(
            Effect.gen(function* () {
              yield* markAcquired(subscription);
              yield* Deferred.succeed(acquired, undefined);
              yield* Effect.sleep("10 millis");
              return subscription;
            }),
          ),
        ),
      );

      yield* Deferred.await(acquired);
      yield* Fiber.interrupt(handoffFiber);
      expect(closeCount).toBe(1);
    }),
  );

  it.effect("honors explicit topic-store subscription handoff options", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let beforeReturnCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-explicit-handoff-options",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* acquireTopicStoreSubscription(
        store,
        (permit, markAcquired) =>
          Effect.gen(function* () {
            const subscription = {
              close: () => Effect.void,
            };
            yield* registerTopicStoreSubscription(permit, subscriber);
            yield* markAcquired(subscription);
            return subscription;
          }),
        {
          beforeReturn: Effect.sync(() => {
            beforeReturnCount += 1;
          }),
        },
      );

      const health = yield* collectTopicStoreHealth(store, false);
      expect(beforeReturnCount).toBe(1);
      expect(health.activeSubscriptions).toBe(1);
      yield* closeTopicStoreSubscriptions(store);
    }),
  );

  it.effect("records backpressure close only once for already closed subscribers", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let finalizeCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-backpressure-idempotent",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      const finalize = Effect.sync(() => {
        finalizeCount += 1;
      });

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* closeBackpressuredTopicStoreSubscription(store, subscriber, finalize);
      yield* closeBackpressuredTopicStoreSubscription(store, subscriber, finalize);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(finalizeCount).toBe(1);
      expect(subscriber.backpressureEvents).toBe(1);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(1);
    }),
  );

  it.effect("does not notify topic-store subscribers for no-op mutations", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let notifyCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-no-op-mutation-notification",
        notify: () =>
          Effect.sync(() => {
            notifyCount += 1;
          }),
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), invalidRow);
      expect(notifyCount).toBe(1);

      yield* patchTopicStoreRow(store, "1", { price: 10 }, invalidRow);
      yield* deleteTopicStoreRow(store, "missing");
      yield* publishTopicStoreRows(store, [], invalidRow);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), invalidRow);
      yield* publishTopicStoreRows(store, [order("1", "open", 10, 1)], invalidRow);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(notifyCount).toBe(1);
      expect(health.version).toBe(1);
      expect(health.rowCount).toBe(1);
      yield* closeTopicStoreSubscriptions(store);
    }),
  );

  it.effect("collects topic-store throughput and drains subscribers on normal close", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let closeStatusCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-normal-close",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(2),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.sync(() => {
            closeStatusCount += 1;
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      const readyHealth = yield* collectTopicStoreHealth(store, false);
      expect(readyHealth.status).toBe("ready");
      expect(readyHealth.rowCount).toBe(1);
      expect(readyHealth.mutationsPerSecond).toBeGreaterThanOrEqual(0);
      expect(readyHealth.rowsPerSecond).toBeGreaterThanOrEqual(0);
      expect(readyHealth.activeSubscriptions).toBe(1);
      expect(readyHealth.queuedEvents).toBe(2);

      yield* closeTopicStoreSubscriptions(store);

      const closedHealth = yield* collectTopicStoreHealth(store, true);
      expect(closeStatusCount).toBe(1);
      expect(closedHealth.status).toBe("degraded");
      expect(closedHealth.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("acquires topic-store subscriptions through the permit handoff", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let closed = false;
      const subscription = yield* acquireTopicStoreSubscription(store, (permit, markAcquired) =>
        Effect.gen(function* () {
          expect(permit.store).toBe(store);
          const acquired = {
            close: () =>
              Effect.sync(() => {
                closed = true;
              }),
          };
          yield* markAcquired(acquired);
          return acquired;
        }),
      );

      yield* subscription.close();
      expect(closed).toBe(true);
    }),
  );

  it.effect("exposes bounded row-change batches for active query catch-up", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const queryInterface = activeQueryTestInterface(store);
      expect(queryInterface.changesSince(queryInterface.version())).toStrictEqual([]);
      expect(queryInterface.changesSince(-1)).toBeUndefined();
      expect(queryInterface.changesSince(1)).toBeUndefined();

      yield* publishTopicStoreRow(store, order("initial", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      expect(queryInterface.changesSince(0)).toBeUndefined();

      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(
        queryInterface,
        "journal-bounds",
        compiled.plan.resultSemantics,
        () => makeIncrementalGroupedQueryExecution(queryInterface, compiled, () => {}),
      );
      expect(queryInterface.changesSince(queryInterface.version())).toStrictEqual([]);

      yield* publishTopicStoreRow(store, order("first-active", "open", 11, 2), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );
      expect(queryInterface.changesSince(1)).toStrictEqual([
        {
          version: 2,
          changes: [
            {
              key: "first-active",
              previous: undefined,
              next: order("first-active", "open", 11, 2),
            },
          ],
        },
      ]);

      for (let index = 0; index < 1_025; index += 1) {
        yield* publishTopicStoreRow(
          store,
          order(`journal-${index}`, "open", index, index),
          (topic, message) => InvalidRowError.make({ topic, message }),
        );
      }

      expect(queryInterface.version()).toBe(1_027);
      expect(queryInterface.changesSince(0)).toBeUndefined();
      expect(queryInterface.changesSince(queryInterface.version())).toStrictEqual([]);
      yield* releaseMaterializedQueryExecution(queryInterface, "journal-bounds");
      expect(queryInterface.changesSince(queryInterface.version() - 1)).toBeUndefined();
      const cursor = execution.createCursor();
      const unchanged = yield* execution.next("released-journal", cursor);
      expect(Option.isNone(unchanged)).toBe(true);
    }),
  );

  it.effect("clears retained row-change journals on active execution clear and overflow", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id", {
        maxEntries: 4,
        maxVersions: 3,
      });
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      yield* acquireMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(storage),
        "overflow-journal",
        compiled.plan.resultSemantics,
        () =>
          makeIncrementalGroupedQueryExecution(
            activeQueryTestInterfaceForStorage(storage),
            compiled,
            () => {},
          ),
      );
      yield* acquireMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(storage),
        "overflow-journal-second",
        compiled.plan.resultSemantics,
        () =>
          makeIncrementalGroupedQueryExecution(
            activeQueryTestInterfaceForStorage(storage),
            compiled,
            () => {},
          ),
      );
      yield* releaseMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(storage),
        "overflow-journal-second",
      );
      expect(
        activeQueryTestInterfaceForStorage(storage).changesSince(storage.version),
      ).toStrictEqual([]);

      const baseVersion = storage.version;
      storage.setPreparedMany(
        yield* storage.prepareRows(
          Array.from({ length: 5 }, (_value, index) => order(`row-${index}`, "open", index, index)),
          invalidRow,
        ),
      );
      storage.advanceVersion();
      expect(activeQueryTestInterfaceForStorage(storage).changesSince(baseVersion)).toBeUndefined();

      const recoveredVersion = storage.version;
      storage.setPrepared(
        yield* storage.prepareRow(order("after-overflow", "closed", 1, 1), invalidRow),
      );
      storage.advanceVersion();
      expect(
        activeQueryTestInterfaceForStorage(storage).changesSince(recoveredVersion),
      ).toStrictEqual([
        {
          version: recoveredVersion + 1,
          changes: [
            {
              key: "after-overflow",
              previous: undefined,
              next: order("after-overflow", "closed", 1, 1),
            },
          ],
        },
      ]);

      const multiVersionOverflowStart = storage.version;
      for (let batchIndex = 0; batchIndex < 3; batchIndex += 1) {
        storage.setPreparedMany(
          yield* storage.prepareRows(
            Array.from({ length: 2 }, (_value, rowIndex) => {
              const key = `multi-version-${batchIndex}-${rowIndex}`;
              return order(key, "open", rowIndex, rowIndex);
            }),
            invalidRow,
          ),
        );
        storage.advanceVersion();
      }
      expect(
        activeQueryTestInterfaceForStorage(storage).changesSince(multiVersionOverflowStart),
      ).toBeUndefined();

      yield* clearStoreRawQueryExecutions(activeQueryTestInterfaceForStorage(storage));
      expect(
        yield* activeStoreRawQueryExecutionCount(activeQueryTestInterfaceForStorage(storage)),
      ).toBe(0);
      storage.setPrepared(
        yield* storage.prepareRow(order("after-clear", "open", 1, 1), invalidRow),
      );
      const afterClearVersion = storage.version;
      storage.advanceVersion();
      expect(
        activeQueryTestInterfaceForStorage(storage).changesSince(afterClearVersion),
      ).toBeUndefined();

      const fallbackStorage = new TopicRowStorage("orders", Order, "id", {
        maxEntries: 4,
        maxVersions: 3,
      });
      fallbackStorage.setPreparedMany(
        yield* fallbackStorage.prepareRows(
          Array.from({ length: 5 }, (_value, index) =>
            order(`fallback-${index}`, "open", index, index),
          ),
          invalidRow,
        ),
      );
      fallbackStorage.advanceVersion();
      yield* acquireMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(fallbackStorage),
        "fallback-clear",
        compiled.plan.resultSemantics,
        () =>
          makeIncrementalGroupedQueryExecution(
            activeQueryTestInterfaceForStorage(fallbackStorage),
            compiled,
            () => {},
          ),
      );
      yield* clearStoreRawQueryExecutions(activeQueryTestInterfaceForStorage(fallbackStorage));
      expect(
        yield* activeStoreRawQueryExecutionCount(
          activeQueryTestInterfaceForStorage(fallbackStorage),
        ),
      ).toBe(0);
    }),
  );

  it.effect("releases retained row-change journals after grouped fallback demotion", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id", {
        maxEntries: 4,
        maxVersions: 3,
      });
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const compiled = yield* prepareRuntimeGroupedQuery(
        "orders",
        rawQueryCompilerMetadata(Order),
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
      );
      const execution = yield* acquireMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(storage),
        "demoted-grouped-journal",
        compiled.plan.resultSemantics,
        (releaseRetainedChanges) =>
          makeIncrementalGroupedQueryExecution(
            activeQueryTestInterfaceForStorage(storage),
            compiled,
            releaseRetainedChanges,
            {
              ...defaultGroupedIncrementalAdmissionLimits,
              maxMembers: 4,
            },
          ),
      );
      const cursor = execution.createCursor();

      storage.setPreparedMany(
        yield* storage.prepareRows(
          Array.from({ length: 5 }, (_value, index) => order(`row-${index}`, "open", index, index)),
          invalidRow,
        ),
      );
      storage.advanceVersion();
      yield* execution.next("demoted-grouped-journal", cursor);

      const demotedVersion = storage.version;
      storage.setPrepared(
        yield* storage.prepareRow(order("after-demotion", "closed", 1, 1), invalidRow),
      );
      storage.advanceVersion();
      expect(
        activeQueryTestInterfaceForStorage(storage).changesSince(demotedVersion),
      ).toBeUndefined();

      yield* releaseMaterializedQueryExecution(
        activeQueryTestInterfaceForStorage(storage),
        "demoted-grouped-journal",
      );
    }),
  );

  it.effect("drains subscribers and storage on normal topic-store reset", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let resetStatusCount = 0;
      let resetStatus: StatusEvent | undefined;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-normal-reset",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: (event) =>
          Effect.sync(() => {
            resetStatusCount += 1;
            resetStatus = event;
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      yield* resetTopicStore(store);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(resetStatusCount).toBe(1);
      expect(expectDefined(resetStatus)).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-normal-reset",
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine reset.",
      });
      expect(health.status).toBe("ready");
      expect(health.rowCount).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.version).toBe(0);
    }),
  );

  it.effect("does not notify subscribers that were closed after mutation capture", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      let notifyCount = 0;
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-closed-before-notify",
        notify: () =>
          Effect.sync(() => {
            notifyCount += 1;
          }),
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () => Effect.void,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      yield* registerTestTopicStoreSubscriber(store, subscriber);
      subscriber.closed = true;
      yield* publishTopicStoreRow(store, order("1", "open", 10, 1), (topic, message) =>
        InvalidRowError.make({ topic, message }),
      );

      expect(notifyCount).toBe(0);
    }),
  );

  it.effect("interrupted topic-store close still releases subscribers and active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const compiled = yield* prepareRuntimeRawQuery("orders", activeQueryTestMetadata(store), {
        select: ["id"],
      });
      yield* acquireRawQueryExecution(activeQueryTestInterface(store), compiled);

      const closeStarted = yield* Deferred.make<void>();
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-close",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(closeStarted, undefined);
            yield* Effect.sleep("10 millis");
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      yield* registerTestTopicStoreSubscriber(store, subscriber);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);

      const closeFiber = yield* Effect.forkChild(closeTopicStoreSubscriptions(store));
      yield* Deferred.await(closeStarted);
      yield* Fiber.interrupt(closeFiber);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.activeViews).toBe(0);
    }),
  );

  it.effect("interrupted topic-store reset still releases subscribers and active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const compiled = yield* prepareRuntimeRawQuery("orders", activeQueryTestMetadata(store), {
        select: ["id"],
      });
      yield* acquireRawQueryExecution(activeQueryTestInterface(store), compiled);

      const closeStarted = yield* Deferred.make<void>();
      const subscriber: LiveTopicSubscriber = {
        topic: "orders",
        queryId: "query-reset",
        notify: () => Effect.void,
        queuedEvents: Effect.succeed(0),
        end: Effect.void,
        closeWithStatus: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(closeStarted, undefined);
            yield* Effect.sleep("10 millis");
          }),
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };
      yield* registerTestTopicStoreSubscriber(store, subscriber);
      expect(yield* activeStoreRawQueryExecutionCount(activeQueryTestInterface(store))).toBe(1);

      const resetFiber = yield* Effect.forkChild(resetTopicStore(store));
      yield* Deferred.await(closeStarted);
      yield* Fiber.interrupt(resetFiber);

      const health = yield* collectTopicStoreHealth(store, false);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.activeViews).toBe(0);
      expect(health.version).toBe(0);
    }),
  );

  it.effect("materialized active queries ignore unchanged evaluations and unknown releases", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Order, "id", () => {});
      const queryInterface = activeQueryTestInterface(store);
      let evaluationCount = 0;
      const evaluate = () => {
        evaluationCount += 1;
        return {
          rows: [],
          keys: [],
          window: [],
          totalRows: 0,
          version: queryInterface.version(),
        };
      };
      const makeExecution = () => {
        const evaluation = evaluate();
        return {
          diagnostics: () => ({
            fullEvaluationCount: 0,
            patchedEvaluationCount: 0,
          }),
          incremental: false,
          latest: () => evaluation,
        };
      };

      const execution = yield* acquireMaterializedQueryExecution(
        queryInterface,
        "empty-materialized",
        emptyResultSemantics,
        makeExecution,
      );
      const cursor = execution.createCursor();
      const unchanged = yield* execution.next("query-unchanged", cursor);

      expect(Option.isNone(unchanged)).toBe(true);
      expect(evaluationCount).toBe(1);
      yield* acquireMaterializedQueryExecution(
        queryInterface,
        "second-materialized",
        emptyResultSemantics,
        makeExecution,
      );
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(2);
      yield* releaseMaterializedQueryExecution(queryInterface, "empty-materialized");
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(1);
      yield* releaseMaterializedQueryExecution(queryInterface, "missing-materialized");
      yield* releaseMaterializedQueryExecution(queryInterface, "second-materialized");
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);
    }),
  );
});
