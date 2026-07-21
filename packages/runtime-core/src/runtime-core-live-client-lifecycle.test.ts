import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./test-support/runtime-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect(
    "keeps canonical filtered subscriptions alive across the acquisition health refresh",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
        const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
          where: [{ field: "id", type: "equals", filter: "missing" }],
          orderBy: [{ field: "price", direction: "asc" }],
          select: ["id", "price"],
          limit: 10,
        });

        const health = yield* runtimeCore.client.health();
        expect(health.engine.topics.orders.activeSubscriptions).toBe(1);

        yield* subscription.close();
        yield* runtimeCore.close;
      }),
  );

  it.effect("releases an event consumer without explicit close and refreshes health", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.internalClient.publish("orders", order("a", 10));
      const subscription = yield* runtimeCore.liveClient.subscribeRuntime("orders", {
        select: ["id"],
      });

      yield* runtimeCore.refreshHealth;
      expect(runtimeCore.liveClient.health.value.engine.topics.orders.activeSubscriptions).toBe(1);
      yield* subscription.events.pipe(Stream.take(1), Stream.runDrain);
      yield* runtimeCore.refreshHealth;
      expect(runtimeCore.liveClient.health.value.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* runtimeCore.close;
    }),
  );
});
