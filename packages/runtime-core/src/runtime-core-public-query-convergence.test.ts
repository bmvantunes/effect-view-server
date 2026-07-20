import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("runs the shared runtime core and live client", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* runtimeCore.client.publishMany("orders", [order("b", 20), order("a", 10)]);
      yield* runtimeCore.client.publish("orders", order("c", 30));
      yield* runtimeCore.client.patch("orders", "c", { price: 5 });
      yield* runtimeCore.client.delete("orders", "a");

      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      expect(snapshot.rows).toStrictEqual([
        { id: "c", price: 5 },
        { id: "b", price: 20 },
      ]);

      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.rowCount).toBe(2);
      const refreshedHealth = yield* runtimeCore.refreshHealth;
      expect(refreshedHealth.engine.topics.orders.rowCount).toBe(2);

      yield* subscription.close();
      yield* runtimeCore.client.reset();
      const resetHealth = yield* runtimeCore.client.health();
      expect(resetHealth.engine.topics.orders.rowCount).toBe(0);
      yield* runtimeCore.close;
    }),
  );
});
