import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./test-support/runtime-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect(
    "rejects hostile source-free Source Health inputs through the typed error channel",
    () =>
      Effect.gen(function* () {
        const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
        const subscribeHostile = <Input>(input: Input): Effect.Effect<unknown, unknown> =>
          Reflect.apply(runtimeCore.liveClient.subscribeSourceHealth, runtimeCore.liveClient, [
            input,
          ]);
        const hostile = new Proxy(
          {},
          {
            ownKeys: () => {
              throw new Error("hostile ownKeys");
            },
          },
        );
        const errors = yield* Effect.all([
          Effect.flip(subscribeHostile(null)),
          Effect.flip(subscribeHostile(undefined)),
          Effect.flip(subscribeHostile(hostile)),
        ]);

        expect(errors).toStrictEqual(
          Array.from({ length: 3 }, () => ({
            _tag: "ViewServerRuntimeError",
            code: "InvalidQuery",
            topic: "<invalid>",
            message: "Source Health input must be one exact { topic, routeBy? } object.",
          })),
        );
        expect(yield* Effect.flip(subscribeHostile({ topic: "orders" }))).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          topic: "orders",
          message: "Topic orders has no canonical Source Definition.",
        });

        yield* runtimeCore.close;
      }),
  );

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
