import { describe, expect, it } from "@effect/vitest";
import { type RawQuery } from "@effect-view-server/config";
import { Effect } from "effect";
import { createColumnLiveViewEngine, EngineClosedError } from "./index";
import {
  collectEvents,
  expectDeltaConverges,
  expectDeltaEvent,
  expectSnapshotEvent,
  expectSnapshotRows,
  expectStatusEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
  takeEvents,
} from "../test-harness/events";
import {
  instrument,
  instrumentSelect,
  makeEngine,
  order,
  orderSelect,
  viewServer,
} from "../test-harness/public-engine";
import type { OrderRow } from "../test-harness/public-engine";
import { rowIds } from "../test-harness/rows";

describe("ColumnLiveViewEngine subscriptions", () => {
  it.effect("emits the initial snapshot before live deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        where: {
          status: "open",
        },
      });
      const events = yield* takeEvents(subscription, 1);

      expectSnapshotRows(firstEvent(events), [order("1", "open", 10, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("emits snapshot keys for projected subscriptions without selected id select", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("b", "open", 10, 1), order("a", "open", 10, 1)]);

      const subscription = yield* engine.subscribe("orders", {
        select: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      const snapshot = firstEvent(initialEvents);
      expectSnapshotEvent(snapshot);
      expect(snapshot.keys).toStrictEqual(["a", "b"]);
      expect(snapshot.rows).toStrictEqual([
        { customerId: "customer-a", status: "open" },
        { customerId: "customer-b", status: "open" },
      ]);

      let state = stateFromSnapshot(snapshot);
      yield* engine.delete("orders", "a");
      const deleteEvents = yield* take(1);
      state = expectDeltaConverges(state, firstEvent(deleteEvents), [
        { customerId: "customer-b", status: "open" },
      ]);
      expect(state.keys).toStrictEqual(["b"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits projected rows in subscription delta operations", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));

      const subscription = yield* engine.subscribe("orders", {
        select: ["customerId", "status"],
      });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "a", { status: "closed", price: 99 });
      const firstDelta = firstEvent(yield* take(1));
      expectDeltaEvent(firstDelta);
      expect(firstDelta.operations).toStrictEqual([
        {
          type: "update",
          key: "a",
          row: {
            customerId: "customer-a",
            status: "closed",
          },
          index: 0,
        },
      ]);
      yield* engine.publish("orders", order("b", "open", 20, 2));
      const secondDelta = firstEvent(yield* take(1));
      expectDeltaEvent(secondDelta);
      expect(secondDelta.operations).toStrictEqual([
        {
          type: "insert",
          key: "b",
          row: {
            customerId: "customer-b",
            status: "open",
          },
          index: 1,
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("reports queued events for active subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(1);
      expect(health.queuedEvents).toBe(2);
      expect(health.topics["orders"].queuedEvents).toBe(2);

      yield* subscription.close();
    }),
  );

  it.effect("reports current queued events after subscribers consume snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const firstSubscription = yield* engine.subscribe("orders", { select: ["id"] });
      const secondSubscription = yield* engine.subscribe("orders", { select: ["id"] });
      const takeFirst = yield* makeEventReader(firstSubscription);
      const takeSecond = yield* makeEventReader(secondSubscription);

      yield* takeFirst(1);
      yield* takeSecond(1);

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(2);
      expect(health.queuedEvents).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(1);
      expect(health.topics["orders"].activeSubscriptions).toBe(2);
      expect(health.topics["orders"].queuedEvents).toBe(0);

      yield* firstSubscription.close();
      yield* secondSubscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect("keeps shared active-query projections separate across deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "open", 20, 2)]);

      const baseQuery = {
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "asc" }],
      } satisfies Omit<RawQuery<OrderRow>, "select">;
      const idSubscription = yield* engine.subscribe("orders", {
        ...baseQuery,
        select: ["id"],
      });
      const priceSubscription = yield* engine.subscribe("orders", {
        ...baseQuery,
        select: ["id", "price"],
      });
      const takeId = yield* makeEventReader(idSubscription);
      const takePrice = yield* makeEventReader(priceSubscription);
      let idState = stateFromSnapshot(firstEvent(yield* takeId(1)));
      let priceState = stateFromSnapshot(firstEvent(yield* takePrice(1)));

      const shared = yield* engine.health();
      expect(shared.topics["orders"].activeViews).toBe(1);

      yield* engine.patch("orders", "a", { price: 30 });
      idState = expectDeltaConverges(idState, firstEvent(yield* takeId(1)), [
        { id: "b" },
        { id: "a" },
      ]);
      priceState = expectDeltaConverges(priceState, firstEvent(yield* takePrice(1)), [
        { id: "b", price: 20 },
        { id: "a", price: 30 },
      ]);

      yield* idSubscription.close();
      yield* priceSubscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect("emits publish, patch, and delete deltas that converge to fresh snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);

      const query = {
        select: orderSelect,
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "asc" }],
      } satisfies RawQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.publish("orders", order("3", "open", 5, 3));
      const publishEvents = yield* take(1);
      const afterPublish = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(publishEvents), afterPublish.rows);

      yield* engine.patch("orders", "1", { price: 30 });
      const patchEvents = yield* take(1);
      const afterPatch = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(patchEvents), afterPatch.rows);

      yield* engine.delete("orders", "3");
      const deleteEvents = yield* take(1);
      const afterDelete = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, firstEvent(deleteEvents), afterDelete.rows);

      expect(state.rows).toStrictEqual([order("1", "open", 30, 1)]);
      yield* subscription.close();
    }),
  );

  it.effect("serializes concurrent publishes before notifying subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* Effect.all(
        ["c", "a", "b"].map((id, index) =>
          engine.publish("orders", order(id, "open", 10 + index, index)),
        ),
        { concurrency: "unbounded" },
      );

      yield* take(3);
      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(rowIds(fresh.rows)).toStrictEqual(["a", "b", "c"]);
      expect(fresh.version).toBe(3);
      yield* subscription.close();
    }),
  );

  it.effect("serializes mixed concurrent writes before notifying subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "open", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* Effect.all(
        [
          engine.patch("orders", "a", { price: 30 }),
          engine.delete("orders", "b"),
          engine.publish("orders", order("c", "closed", 40, 3)),
        ],
        { concurrency: "unbounded" },
      );

      yield* take(1);
      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.version).toBe(4);
      expect(rowIds(fresh.rows)).toStrictEqual(["a", "c"]);
      expect(fresh.rows).toStrictEqual([order("a", "open", 30, 1), order("c", "closed", 40, 3)]);
      yield* subscription.close();
    }),
  );

  it.effect("applies disjoint patches cumulatively against the latest row state", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));

      yield* engine.patch("orders", "a", { status: "closed" });
      yield* engine.patch("orders", "a", { price: 99 });

      const fresh = yield* engine.snapshot("orders", { select: orderSelect });
      expect(fresh.version).toBe(3);
      expect(fresh.rows).toStrictEqual([order("a", "closed", 99, 1)]);
    }),
  );

  it.effect("idempotent subscription close removes active subscribers from health", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const active = yield* engine.health();
      expect(active.topics["orders"].activeSubscriptions).toBe(1);
      expect(active.activeSubscriptions).toBe(1);

      yield* subscription.close();
      yield* subscription.close();

      const closed = yield* engine.health();
      expect(closed.topics["orders"].activeSubscriptions).toBe(0);
      expect(closed.topics["orders"].activeViews).toBe(0);
      expect(closed.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("does not record backpressure when explicit close races with publish", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscriptions = yield* Effect.all(
        Array.from({ length: 32 }, () => engine.subscribe("orders", { select: ["id"] })),
        { concurrency: "unbounded" },
      );

      yield* Effect.all(
        [
          Effect.all(
            subscriptions.map((subscription) => subscription.close()),
            { concurrency: "unbounded" },
          ),
          Effect.all(
            Array.from({ length: 32 }, (_, index) =>
              engine.publish("orders", order(`race-${index}`, "open", index, index)),
            ),
            { concurrency: "unbounded" },
          ),
        ],
        { concurrency: "unbounded" },
      );

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(0);
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].backpressureEvents).toBe(0);
    }),
  );

  it.effect("stream finalization releases active subscribers", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });

      const events = yield* takeEvents(subscription, 1);
      expect(events.map((event) => event.type)).toStrictEqual(["snapshot"]);

      const health = yield* engine.health();
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("emits closed status before ending subscriptions when engine closes", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("a", "open", 10, 1));
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);

      const snapshot = yield* take(1);
      expectSnapshotRows(firstEvent(snapshot), [{ id: "a" }]);

      yield* engine.close();

      const closedEvents = yield* take(1);
      const closed = firstEvent(closedEvents);
      expectStatusEvent(closed);
      expect(closed).toMatchObject({
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine closed.",
      });

      const health = yield* engine.health();
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect(
    "delivers engine closed status when the subscription queue already contains a snapshot",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          topics: viewServer.topics,
          subscriptionQueueCapacity: 1,
        });
        yield* engine.publish("orders", order("a", "open", 10, 1));
        const subscription = yield* engine.subscribe("orders", { select: ["id"] });

        yield* engine.close();

        const events = yield* collectEvents(subscription);
        expect(events).toStrictEqual([
          {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "closed",
            code: "SubscriptionClosed",
            message: "Subscription closed because the engine closed.",
          },
        ]);
        const health = yield* engine.health();
        expect(health.topics["orders"].activeSubscriptions).toBe(0);
        expect(health.topics["orders"].activeViews).toBe(0);
        expect(health.activeSubscriptions).toBe(0);
      }),
  );

  it.effect("does not register subscriptions after a concurrent engine close", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const subscribeAll = Effect.all(
        Array.from({ length: 64 }, () =>
          engine.subscribe("orders", { select: ["id"] }).pipe(Effect.result),
        ),
        { concurrency: "unbounded" },
      );

      yield* Effect.all([subscribeAll, engine.close()], { concurrency: "unbounded" });

      const health = yield* engine.health();
      expect(health.status).toBe("stopping");
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("does not emit deltas for invisible updates or no-op visible patches", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "closed", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        where: {
          status: "open",
        },
      });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "2", { price: 25 });
      yield* engine.patch("orders", "1", { price: 10 });
      yield* subscription.close();

      const remaining = yield* collectEvents(subscription);
      expect(remaining).toStrictEqual([]);
    }),
  );

  it.effect("freezes subscription query semantics at subscribe time", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const openStatus: OrderRow["status"] = "open";

      const query: {
        select: typeof orderSelect;
        where: {
          status: OrderRow["status"];
        };
      } = {
        select: orderSelect,
        where: {
          status: openStatus,
        },
      };
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      query.where.status = "closed";
      yield* engine.publish("orders", order("2", "open", 20, 2));

      const events = yield* take(1);
      const event = firstEvent(events);
      state = expectDeltaConverges(state, event, [
        order("1", "open", 10, 1),
        order("2", "open", 20, 2),
      ]);
      expect(state.keys).toStrictEqual(["1", "2"]);
      yield* subscription.close();
    }),
  );

  it.effect("does not let consumer snapshot mutations corrupt subscription cursors", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("instruments", instrument("1", "xnys", 1, ["equity", "us"]));

      const subscription = yield* engine.subscribe("instruments", {
        select: instrumentSelect,
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      const initial = firstEvent(initialEvents);
      expectSnapshotEvent(initial);
      Object.assign(Object(initial.rows[0]).metadata.risk, { tier: 999 });
      Object(initial.rows[0]).tags.push("mutated-client-row");

      yield* engine.publish("instruments", instrument("2", "xlon", 2, ["equity", "uk"]));
      const events = yield* take(1);
      const event = firstEvent(events);
      expectDeltaEvent(event);
      expect(event.operations).toStrictEqual([
        {
          type: "insert",
          key: "2",
          row: instrument("2", "xlon", 2, ["equity", "uk"]),
          index: 1,
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("emits move and update operations for sort movement without full-window churn", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("1", "open", 10, 1), order("2", "open", 20, 2)]);
      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.patch("orders", "1", { price: 30 });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "move",
            key: "1",
            fromIndex: 0,
            toIndex: 1,
          },
          {
            type: "update",
            key: "1",
            row: order("1", "open", 30, 1),
            index: 1,
          },
        ],
        totalRows: 2,
      });
      state = expectDeltaConverges(state, event, [
        order("2", "open", 20, 2),
        order("1", "open", 30, 1),
      ]);
      expect(state.keys).toStrictEqual(["2", "1"]);
      yield* subscription.close();
    }),
  );

  it.effect("falls back to multi-move deltas when a batch reorders more than one row", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);
      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      let state = stateFromSnapshot(firstEvent(yield* take(1)));

      yield* engine.publishMany("orders", [order("1", "open", 30, 1), order("3", "open", 10, 3)]);
      const event = firstEvent(yield* take(1));
      expect(event).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "move",
            key: "3",
            fromIndex: 2,
            toIndex: 0,
          },
          {
            type: "update",
            key: "3",
            row: order("3", "open", 10, 3),
            index: 0,
          },
          {
            type: "move",
            key: "2",
            fromIndex: 2,
            toIndex: 1,
          },
          {
            type: "update",
            key: "1",
            row: order("1", "open", 30, 1),
            index: 2,
          },
        ],
        totalRows: 3,
      });
      state = expectDeltaConverges(state, event, [
        order("3", "open", 10, 3),
        order("2", "open", 20, 2),
        order("1", "open", 30, 1),
      ]);
      expect(state.keys).toStrictEqual(["3", "2", "1"]);
      yield* subscription.close();
    }),
  );

  it.effect("keeps subscription deltas indexed by configured row-key tiebreaks", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("c", "open", 10, 1),
        order("a", "open", 10, 1),
        order("b", "open", 10, 1),
      ]);
      const subscription = yield* engine.subscribe("orders", {
        select: orderSelect,
        orderBy: [{ field: "price", direction: "asc" }],
      });
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));
      expect(state.keys).toStrictEqual(["a", "b", "c"]);

      yield* engine.patch("orders", "b", { customerId: "customer-b-updated" });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "update",
            key: "b",
            row: { ...order("b", "open", 10, 1), customerId: "customer-b-updated" },
            index: 1,
          },
        ],
      });
      state = expectDeltaConverges(state, event, [
        order("a", "open", 10, 1),
        { ...order("b", "open", 10, 1), customerId: "customer-b-updated" },
        order("c", "open", 10, 1),
      ]);
      expect(state.keys).toStrictEqual(["a", "b", "c"]);
      yield* subscription.close();
    }),
  );

  it.effect("emits an update when an optional field appears on a visible row", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const query = {
        select: [...orderSelect, "note"],
        where: {
          status: "open",
        },
      } satisfies RawQuery<OrderRow> & {
        readonly select: readonly [
          "id",
          "customerId",
          "status",
          "price",
          "region",
          "updatedAt",
          "note",
        ];
        readonly where: {
          readonly status: "open";
        };
      };
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.patch("orders", "1", { note: "newly-visible" });
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "update",
            key: "1",
            row: {
              ...order("1", "open", 10, 1),
              note: "newly-visible",
            },
            index: 0,
          },
        ],
      });

      const fresh = yield* engine.snapshot("orders", query);
      state = expectDeltaConverges(state, event, fresh.rows);
      expect(state.rows).toStrictEqual([
        {
          ...order("1", "open", 10, 1),
          note: "newly-visible",
        },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("removes a deleted visible row and inserts the next row entering the window", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [
        order("1", "open", 10, 1),
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);
      const query = {
        select: orderSelect,
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 2,
      } satisfies RawQuery<OrderRow>;
      const subscription = yield* engine.subscribe("orders", query);
      const take = yield* makeEventReader(subscription);
      const initialEvents = yield* take(1);
      let state = stateFromSnapshot(firstEvent(initialEvents));

      yield* engine.delete("orders", "1");
      const events = yield* take(1);
      const event = firstEvent(events);
      expect(event).toMatchObject({
        type: "delta",
        operations: [
          {
            type: "remove",
            key: "1",
          },
          {
            type: "insert",
            key: "3",
            row: order("3", "open", 30, 3),
            index: 1,
          },
        ],
      });
      state = expectDeltaConverges(state, event, [
        order("2", "open", 20, 2),
        order("3", "open", 30, 3),
      ]);
      expect(state.keys).toStrictEqual(["2", "3"]);
      yield* subscription.close();
    }),
  );

  it.effect("closes a subscriber and records health counters when its bounded queue is full", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: 1,
      });
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });

      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.backpressureEvents).toBe(1);
      expect(health.topics["orders"].activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
      expect(health.topics["orders"].backpressureEvents).toBe(1);
      expect(health.topics["orders"].maxQueueDepth).toBe(1);

      const events = yield* collectEvents(subscription);
      expect(events.map((event) => event.type)).toStrictEqual(["status"]);
      expect(events[0]).toMatchObject({
        type: "status",
        code: "BackpressureExceeded",
      });
    }),
  );

  it.effect("falls back to default subscription capacity for invalid config capacity", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: viewServer.topics,
        subscriptionQueueCapacity: Number.NaN,
      });
      const subscription = yield* engine.subscribe("orders", { select: orderSelect });
      yield* engine.publish("orders", order("1", "open", 10, 1));

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(1);
      expect(health.backpressureEvents).toBe(0);
      yield* subscription.close();
    }),
  );

  it.effect("reset closes subscriptions instead of emitting lower-version deltas", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publish("orders", order("1", "open", 10, 1));
      const subscription = yield* engine.subscribe("orders", { select: ["id"] });
      const take = yield* makeEventReader(subscription);
      yield* take(1);

      yield* engine.patch("orders", "1", { price: 20 });
      yield* engine.publish("orders", order("2", "open", 5, 2));
      yield* engine.reset();

      const closedRead = yield* take(1);
      expect(closedRead).toMatchObject([
        {
          type: "status",
          status: "closed",
          code: "SubscriptionClosed",
        },
      ]);

      const health = yield* engine.health();
      expect(health.version).toBe(0);
      expect(health.activeSubscriptions).toBe(0);
      expect(health.topics["orders"].activeViews).toBe(0);
    }),
  );

  it.effect(
    "delivers reset closed status when the subscription queue already contains a snapshot",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          topics: viewServer.topics,
          subscriptionQueueCapacity: 1,
        });
        yield* engine.publish("orders", order("1", "open", 10, 1));
        const subscription = yield* engine.subscribe("orders", { select: ["id"] });

        yield* engine.reset();

        const events = yield* collectEvents(subscription);
        expect(events).toStrictEqual([
          {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "closed",
            code: "SubscriptionClosed",
            message: "Subscription closed because the engine reset.",
          },
        ]);
        const health = yield* engine.health();
        expect(health.version).toBe(0);
        expect(health.activeSubscriptions).toBe(0);
        expect(health.topics["orders"].activeViews).toBe(0);
      }),
  );

  it.effect("rejects reset after the engine is closed", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.close();

      const error = yield* Effect.flip(engine.reset());

      expect(error).toBeInstanceOf(EngineClosedError);
      expect(error._tag).toBe("EngineClosedError");
    }),
  );
});
