import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { createViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("exposes route-bypassing internals only through the internal factory", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      const subscription = yield* runtimeCore.internalLiveClient.subscribeInternal("orders", {
        select: ["id"],
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

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("supports the synchronous runtime core constructor", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer, { subscriptionQueueCapacity: 1 });
      expect("set" in runtimeCore.liveClient.health).toBe(false);
      expect(runtimeCore.liveClient.health.value.status).toBe("ready");

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect(health.transport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.effect("supports the synchronous runtime core constructor defaults", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer);

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* runtimeCore.close;
    }),
  );
});
