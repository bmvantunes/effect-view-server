import { describe, expect, it } from "@effect/vitest";
import { type GroupedQuery } from "@effect-view-server/config";
import { Effect } from "effect";
import {
  createColumnLiveViewEngine,
  defaultGroupedIncrementalAdmissionLimits,
  groupedIncrementalAdmissionLimitsFromConfig,
} from "./index";
import {
  applyDelta,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";
import { makeEngine, order, viewServer } from "../test-harness/public-engine";
import type { OrderRow, Topics } from "../test-harness/public-engine";
import { normalizeDecimalFields } from "../test-harness/rows";

describe("ColumnLiveViewEngine health", () => {
  it.effect("updates health row counts and versions", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();

      const empty = yield* engine.health();
      expect(empty.version).toBe(0);
      expect(empty.topics["orders"].rowCount).toBe(0);
      expect(empty.topics["orders"].version).toBe(0);

      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      yield* engine.patch("orders", "1", { price: 30 });
      yield* engine.delete("orders", "2");

      const mutated = yield* engine.health();
      expect(mutated.version).toBe(3);
      expect(mutated.topics["orders"].rowCount).toBe(1);
      expect(mutated.topics["orders"].version).toBe(3);
      expect(mutated.topics["orders"].lastMutationAt).not.toBeNull();
      expect(mutated.topics["orders"].pendingMutationBatches).toBe(0);

      yield* engine.delete("orders", "missing");
      yield* engine.publishMany("orders", []);

      const afterNoOpMutations = yield* engine.health();
      expect(afterNoOpMutations.version).toBe(mutated.version);
      expect(afterNoOpMutations.topics["orders"].version).toBe(mutated.topics["orders"].version);
      expect(afterNoOpMutations.topics["orders"].rowCount).toBe(mutated.topics["orders"].rowCount);
      expect(afterNoOpMutations.topics["orders"].lastMutationAt).toBe(
        mutated.topics["orders"].lastMutationAt,
      );
      expect(afterNoOpMutations.topics["orders"].pendingMutationBatches).toBe(0);

      yield* engine.reset();

      const reset = yield* engine.health();
      expect(reset.version).toBe(0);
      expect(reset.topics["orders"].rowCount).toBe(0);
      expect(reset.topics["orders"].version).toBe(0);
    }),
  );

  it.effect("reads health state when the returned Effect is executed", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const delayedMutatedHealth = engine.health();

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const mutated = yield* delayedMutatedHealth;
      expect(mutated.status).toBe("ready");
      expect(mutated.version).toBe(1);
      expect(mutated.topics["orders"].rowCount).toBe(1);
      expect(mutated.topics["orders"].version).toBe(1);

      const delayedClosedHealth = engine.health();
      yield* engine.close();

      const closed = yield* delayedClosedHealth;
      expect(closed.status).toBe("stopping");
      expect(closed.topics["orders"].status).toBe("degraded");
    }),
  );

  it.effect("creates stores only for own topic definitions", () =>
    Effect.gen(function* () {
      const topicsWithInheritedDefinition: Record<string, Topics["orders"]> = Object.create({
        inherited: viewServer.topics.orders,
      });
      topicsWithInheritedDefinition["orders"] = viewServer.topics.orders;

      const engine = yield* createColumnLiveViewEngine({
        topics: topicsWithInheritedDefinition,
      });
      const health = yield* engine.health();
      expect(Object.keys(health.topics)).toStrictEqual(["orders"]);

      const inherited = yield* Effect.flip(engine.snapshot("inherited", { select: ["id"] }));
      expect(inherited).toMatchObject({
        _tag: "InvalidTopicError",
        topic: "inherited",
      });
    }),
  );

  it.effect("falls back to the default queue capacity when configured capacity is invalid", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 0,
      });
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      yield* subscription.close();

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it("normalizes grouped incremental admission limits from config", () => {
    expect(groupedIncrementalAdmissionLimitsFromConfig(undefined)).toStrictEqual(
      defaultGroupedIncrementalAdmissionLimits,
    );
    expect(
      groupedIncrementalAdmissionLimitsFromConfig({
        maxGroups: Number.NaN,
        maxMembers: 0,
        maxMembersPerGroup: -1,
        maxRetainedValueEntries: 2,
      }),
    ).toStrictEqual({
      ...defaultGroupedIncrementalAdmissionLimits,
      maxRetainedValueEntries: 2,
    });
  });

  it.effect("reports active grouped execution mode counts in health", () =>
    Effect.gen(function* () {
      const admittedEngine = yield* createColumnLiveViewEngine({
        groupedIncrementalAdmissionLimits: {
          maxGroups: 10,
          maxMembers: 10,
          maxMembersPerGroup: 10,
          maxRetainedValueEntries: 10,
        },
        topics: viewServer.topics,
      });
      yield* admittedEngine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "closed", 20, 2),
      ]);
      const admittedSubscription = yield* admittedEngine.subscribe("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });

      const admitted = yield* admittedEngine.health();

      expect(admitted.topics["orders"].activeSubscriptions).toBe(1);
      expect(admitted.topics["orders"].activeViews).toBe(1);
      expect(admitted.topics["orders"].activeFallbackGroupedViews).toBe(0);
      expect(admitted.topics["orders"].activeIncrementalGroupedViews).toBe(1);
      yield* admittedSubscription.close();

      const fallbackEngine = yield* createColumnLiveViewEngine({
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
          maxMembers: 10,
          maxMembersPerGroup: 10,
          maxRetainedValueEntries: 10,
        },
        topics: viewServer.topics,
      });
      yield* fallbackEngine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "closed", 20, 2),
      ]);
      const fallbackSubscription = yield* fallbackEngine.subscribe("orders", {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      });

      const fallback = yield* fallbackEngine.health();

      expect(fallback.topics["orders"].activeSubscriptions).toBe(1);
      expect(fallback.topics["orders"].activeViews).toBe(1);
      expect(fallback.topics["orders"].activeFallbackGroupedViews).toBe(1);
      expect(fallback.topics["orders"].activeIncrementalGroupedViews).toBe(0);
      yield* fallbackSubscription.close();

      const retainedValueFallbackEngine = yield* createColumnLiveViewEngine({
        groupedIncrementalAdmissionLimits: {
          maxGroups: 10,
          maxMembers: 10,
          maxMembersPerGroup: 10,
          maxRetainedValueEntries: 1,
        },
        topics: viewServer.topics,
      });
      yield* retainedValueFallbackEngine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "closed", 20, 2),
      ]);
      const retainedValueFallbackSubscription = yield* retainedValueFallbackEngine.subscribe(
        "orders",
        {
          groupBy: ["status"],
          aggregates: {
            minimumPrice: { aggFunc: "min", field: "price" },
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
          limit: 10,
        } satisfies GroupedQuery<OrderRow>,
      );

      const retainedValueFallback = yield* retainedValueFallbackEngine.health();

      expect(retainedValueFallback.topics["orders"].activeSubscriptions).toBe(1);
      expect(retainedValueFallback.topics["orders"].activeViews).toBe(1);
      expect(retainedValueFallback.topics["orders"].activeFallbackGroupedViews).toBe(1);
      expect(retainedValueFallback.topics["orders"].activeIncrementalGroupedViews).toBe(0);
      yield* retainedValueFallbackSubscription.close();
    }),
  );

  it.effect("demotes active grouped execution when writes exceed admission limits", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        groupedIncrementalAdmissionLimits: {
          maxGroups: 10,
          maxMembers: 2,
          maxMembersPerGroup: 10,
          maxRetainedValueEntries: 1_000,
        },
        topics: viewServer.topics,
      });
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
        limit: 10,
      } satisfies GroupedQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      const initialState = stateFromSnapshot(initial);

      const admitted = yield* engine.health();
      expect(admitted.topics["orders"].activeFallbackGroupedViews).toBe(0);
      expect(admitted.topics["orders"].activeIncrementalGroupedViews).toBe(1);

      yield* engine.publish("orders", order("3", "open", 5, 3));
      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      const expectedRows = [
        {
          rowCount: 1n,
          status: "closed",
          totalPrice: "20",
        },
        {
          rowCount: 2n,
          status: "open",
          totalPrice: "15",
        },
      ];
      const convergedState = applyDelta(initialState, delta);
      expect(normalizeDecimalFields(convergedState.rows)).toStrictEqual(expectedRows);
      const snapshot = yield* engine.snapshot("orders", query);
      expect(normalizeDecimalFields(snapshot.rows)).toStrictEqual(expectedRows);

      const demoted = yield* engine.health();
      expect(demoted.topics["orders"].activeFallbackGroupedViews).toBe(1);
      expect(demoted.topics["orders"].activeIncrementalGroupedViews).toBe(0);
      expect(demoted.topics["orders"].groupedFullEvaluationCount).toBe(1);
      expect(demoted.topics["orders"].groupedPatchedEvaluationCount).toBe(0);

      yield* subscription.close();
    }),
  );

  it.effect(
    "demotes active grouped execution when retained value entries exceed admission limits",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          groupedIncrementalAdmissionLimits: {
            maxGroups: 10,
            maxMembers: 10,
            maxMembersPerGroup: 10,
            maxRetainedValueEntries: 2,
          },
          topics: viewServer.topics,
        });
        yield* engine.publishMany("orders", [
          order("1", "open", 10, 1),
          order("2", "closed", 20, 2),
        ]);
        const query = {
          groupBy: ["status"],
          aggregates: {
            minimumPrice: { aggFunc: "min", field: "price" },
            rowCount: { aggFunc: "count" },
          },
          orderBy: [{ field: "status", direction: "asc" }],
          limit: 10,
        } satisfies GroupedQuery<OrderRow>;
        const subscription = yield* engine.subscribe("orders", query);
        const read = yield* makeEventReader(subscription);
        const initial = firstEvent(yield* read(1));
        expectSnapshotEvent(initial);
        const initialState = stateFromSnapshot(initial);

        const admitted = yield* engine.health();
        expect(admitted.topics["orders"].activeFallbackGroupedViews).toBe(0);
        expect(admitted.topics["orders"].activeIncrementalGroupedViews).toBe(1);

        yield* engine.publish("orders", order("3", "open", 5, 3));
        const delta = firstEvent(yield* read(1));
        expectDeltaEvent(delta);
        const expectedRows = [
          {
            minimumPrice: 20,
            rowCount: 1n,
            status: "closed",
          },
          {
            minimumPrice: 5,
            rowCount: 2n,
            status: "open",
          },
        ];
        const convergedState = applyDelta(initialState, delta);
        expect(convergedState.rows).toStrictEqual(expectedRows);
        const snapshot = yield* engine.snapshot("orders", query);
        expect(snapshot.rows).toStrictEqual(expectedRows);

        const demoted = yield* engine.health();
        expect(demoted.topics["orders"].activeFallbackGroupedViews).toBe(1);
        expect(demoted.topics["orders"].activeIncrementalGroupedViews).toBe(0);
        expect(demoted.topics["orders"].groupedFullEvaluationCount).toBe(1);
        expect(demoted.topics["orders"].groupedPatchedEvaluationCount).toBe(0);

        yield* subscription.close();
      }),
  );

  it.effect("subscribes through the runtime-validated entrypoint", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribeRuntime("orders", { select: ["id"] });
      yield* subscription.close();

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
    }),
  );
});
