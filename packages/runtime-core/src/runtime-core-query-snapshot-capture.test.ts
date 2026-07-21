import { describe, expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { makeViewServerRuntimeCoreInternal } from "./internal";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./test-support/runtime-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("captures ordinary subscription queries when subscribe is called", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.client.publishMany("orders", [
        order("open-low", 10),
        order("open-high", 20),
        { ...order("closed", 30), status: "closed" },
      ]);
      const query = {
        where: [{ field: "status", type: "equals", filter: "open" }],
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 0,
        limit: 1,
      } satisfies {
        where: [{ field: "status"; type: "equals"; filter: "open" | "closed" }];
        select: ["id", "price"];
        orderBy: [{ field: "price"; direction: "asc" | "desc" }];
        offset: number;
        limit: number;
      };
      const subscribe = runtimeCore.liveClient.subscribe("orders", query);
      expect(Reflect.set(query.where[0], "filter", "closed")).toBe(true);
      expect(Reflect.set(query.orderBy[0], "direction", "desc")).toBe(true);
      expect(Reflect.set(query, "offset", 1)).toBe(true);
      expect(Reflect.set(query, "limit", 2)).toBe(true);

      const subscription = yield* subscribe;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["open-low"],
        rows: [{ id: "open-low", price: 10 }],
        totalRows: 2,
      });

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("captures ordinary runtime subscription queries when subscribeRuntime is called", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.client.publishMany("orders", [
        order("open-low", 10),
        order("open-high", 20),
        { ...order("closed", 30), status: "closed" },
      ]);
      const query = {
        where: [{ field: "status", type: "equals", filter: "open" }],
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 0,
        limit: 1,
      } satisfies {
        where: [{ field: "status"; type: "equals"; filter: "open" | "closed" }];
        select: ["id", "price"];
        orderBy: [{ field: "price"; direction: "asc" | "desc" }];
        offset: number;
        limit: number;
      };
      const subscribe = runtimeCore.liveClient.subscribeRuntime("orders", query);
      expect(Reflect.set(query.where[0], "filter", "closed")).toBe(true);
      expect(Reflect.set(query.orderBy[0], "direction", "desc")).toBe(true);
      expect(Reflect.set(query, "offset", 1)).toBe(true);
      expect(Reflect.set(query, "limit", 2)).toBe(true);

      const subscription = yield* subscribe;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["open-low"],
        rows: [{ id: "open-low", price: 10 }],
        totalRows: 2,
      });

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("captures routed internal runtime queries when the private seam is called", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(viewServer, {});
      yield* runtimeCore.client.publishMany("orders", [
        order("open-low", 10),
        order("open-high", 20),
        { ...order("closed", 30), status: "closed" },
      ]);
      const query = {
        where: [{ field: "status", type: "equals", filter: "open" }],
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        offset: 0,
        limit: 1,
      } satisfies {
        where: [{ field: "status"; type: "equals"; filter: "open" | "closed" }];
        select: ["id", "price"];
        orderBy: [{ field: "price"; direction: "asc" | "desc" }];
        offset: number;
        limit: number;
      };
      const subscribe = runtimeCore.internalLiveClient.subscribeRuntimeRoutedInternal(
        "orders",
        query,
      );
      expect(Reflect.set(query.where[0], "filter", "closed")).toBe(true);
      expect(Reflect.set(query.orderBy[0], "direction", "desc")).toBe(true);
      expect(Reflect.set(query, "offset", 1)).toBe(true);
      expect(Reflect.set(query, "limit", 2)).toBe(true);

      const subscription = yield* subscribe;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["open-low"],
        rows: [{ id: "open-low", price: 10 }],
        totalRows: 2,
      });

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );
});
