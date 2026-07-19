import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema, Stream } from "effect";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreQueryPartition,
  type ViewServerRuntimeCoreTerminalObserver,
} from "./internal";

const config = defineViewServerConfig({
  topics: {
    orders: {
      schema: Schema.Struct({
        id: Schema.String,
        region: Schema.String,
      }),
      key: "id",
    },
  },
});

const usaPartition: ViewServerRuntimeCoreQueryPartition = Object.freeze({
  key: "test-route:usa",
  matches: (_row, storageKey) => storageKey === "usa",
  ownedStorageKeys: () => ["usa"],
});

const observer: ViewServerRuntimeCoreTerminalObserver = {
  onQueryRegistered: () => Effect.void,
  onTerminalOccurrence: () => Effect.void,
  onTerminalReady: () => Effect.void,
};

describe("runtime-core query partitions", () => {
  it.effect("forwards typed and runtime observed partitions to the engine", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCoreInternal(config, {});
      yield* runtimeCore.internalClient.publishMany("orders", [
        { id: "usa", region: "usa" },
        { id: "eu", region: "eu" },
      ]);

      const typed = yield* runtimeCore.internalLiveClient.subscribeObservedInternal(
        "orders",
        { select: ["id", "region"] },
        observer,
        usaPartition,
      );
      const runtime = yield* runtimeCore.internalLiveClient.subscribeRuntimeObservedInternal(
        "orders",
        { select: ["id", "region"] },
        observer,
        usaPartition,
      );
      const typedEvents = yield* typed.events.pipe(Stream.take(1), Stream.runCollect);
      const runtimeEvents = yield* runtime.events.pipe(Stream.take(1), Stream.runCollect);

      expect(typedEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["usa"],
        rows: [{ id: "usa", region: "usa" }],
        totalRows: 1,
      });
      expect(runtimeEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: ["usa"],
        rows: [{ id: "usa", region: "usa" }],
        totalRows: 1,
      });

      yield* runtimeCore.internalClient.deleteStorageKey("orders", "usa", usaPartition.key);

      yield* typed.close();
      yield* runtime.close();
      yield* runtimeCore.close;
    }),
  );
});
