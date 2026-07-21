import { describe, expect, it } from "@effect/vitest";
import {
  createColumnLiveViewEngineInternal,
  type ColumnLiveViewEngineQueryPartition,
  type ColumnLiveViewTerminalObserver,
} from "./internal";
import { Effect } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import { firstEvent, makeEventReader, takeEvents } from "../test-harness/events";
import { order, position, viewServer } from "../test-harness/public-engine";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";

const usaPartition: ColumnLiveViewEngineQueryPartition = Object.freeze({
  key: "test-route:usa",
  matches: (_row, storageKey) => storageKey === "usa",
  ownedStorageKeys: () => ["usa"],
});

const observer: ColumnLiveViewTerminalObserver = {
  onQueryRegistered: () => Effect.void,
  onTerminalOccurrence: () => Effect.void,
  onTerminalReady: () => Effect.void,
};

describe("ColumnLiveViewEngine query partitions", () => {
  it.effect("keeps targeted raw and grouped live-write work linear across route counts", () =>
    Effect.gen(function* () {
      for (const routeCount of [1, 25]) {
        const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
        const regions = Array.from({ length: routeCount }, (_value, index) => `region-${index}`);
        let partitionMatchCalls = 0;
        const subscriptions = yield* Effect.forEach(regions, (region) => {
          const partition: ColumnLiveViewEngineQueryPartition = Object.freeze({
            key: `partition:${region}`,
            matches: (_row, storageKey) => {
              partitionMatchCalls += 1;
              return storageKey === region;
            },
            ownedStorageKeys: () => [region],
          });
          return Effect.all([
            engine.subscribeRuntimePartitioned("orders", { select: ["id", "region"] }, partition),
            engine.subscribeRuntimePartitioned(
              "orders",
              {
                groupBy: ["status"],
                aggregates: { rowCount: { aggFunc: "count" } },
              },
              partition,
            ),
          ]);
        }).pipe(Effect.map((pairs) => pairs.flat()));
        const readers = yield* Effect.forEach(subscriptions, makeEventReader);
        yield* Effect.forEach(readers, (read) => read(1), { discard: true });

        expect(partitionMatchCalls).toBe(0);

        yield* Effect.forEach(
          regions,
          (region, index) =>
            engine.publishManyDecodedRowsWithStorageKeys(
              "orders",
              [
                {
                  storageKey: region,
                  row: order(region, "open", index, index, region),
                },
              ],
              `partition:${region}`,
            ),
          { discard: true },
        );
        yield* Effect.forEach(readers, (read) => read(1), { discard: true });

        expect(partitionMatchCalls).toBe(routeCount * 2);

        yield* Effect.forEach(
          regions,
          (region) => engine.deleteStorageKey("orders", region, `partition:${region}`),
          { discard: true },
        );
        yield* Effect.forEach(readers, (read) => read(1), { discard: true });

        expect(partitionMatchCalls).toBe(routeCount * 4);

        yield* Effect.forEach(subscriptions, (subscription) => subscription.close(), {
          discard: true,
        });
        yield* engine.close();
      }
    }),
  );

  it.effect("keeps storage ownership authoritative in the partition predicate plan", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeRawQuery(
        "orders",
        rawQueryCompilerMetadata(viewServer.topics.orders.schema),
        { select: ["id"] },
        usaPartition,
      );

      expect(compiled.plan.predicate.plan).toStrictEqual({
        filters: [],
        callbackRequired: true,
        callbackSkippable: false,
      });
      expect(compiled.plan.predicate.matches({ id: "usa", region: "usa" }, "usa")).toBe(true);
      expect(compiled.plan.predicate.matches({ id: "eu", region: "eu" }, "eu")).toBe(false);
    }),
  );

  it.effect("uses storage ownership when a decoded scalar differs from the acquisition route", () =>
    Effect.gen(function* () {
      let partitionMatchCalls = 0;
      const transformedRoutePartition: ColumnLiveViewEngineQueryPartition = Object.freeze({
        key: "test-route:transformed-region",
        matches: (_row, storageKey) => {
          partitionMatchCalls += 1;
          return storageKey === "route:owned";
        },
        ownedStorageKeys: () => ["route:owned"],
      });
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      yield* engine.publishManyDecodedRowsWithStorageKeys("orders", [
        { storageKey: "route:owned", row: order("owned", "open", 10, 1, "usa") },
        { storageKey: "route:other", row: order("other", "open", 20, 2, "USA") },
      ]);

      const subscription = yield* engine.subscribeRuntimePartitioned(
        "orders",
        {
          select: ["id", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        },
        transformedRoutePartition,
      );
      const snapshot = firstEvent(yield* takeEvents(subscription, 1));

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["route:owned"],
        rows: [{ id: "owned", region: "usa" }],
        totalRows: 1,
      });
      expect(partitionMatchCalls).toBe(1);

      partitionMatchCalls = 0;
      const grouped = yield* engine.subscribeRuntimePartitioned(
        "orders",
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
        transformedRoutePartition,
      );
      const groupedSnapshot = firstEvent(yield* takeEvents(grouped, 1));

      expect(groupedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [JSON.stringify([["status", JSON.stringify(["present", '"open"'])]])],
        rows: [{ status: "open", rowCount: 1n }],
        totalRows: 1,
      });
      expect(partitionMatchCalls).toBe(1);

      yield* subscription.close();
      yield* grouped.close();
      yield* engine.close();
    }),
  );

  it.effect("applies partitions to runtime raw and grouped subscription snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      yield* engine.publishMany("orders", [
        order("usa", "open", 10, 1, "usa"),
        order("eu", "open", 20, 2, "eu"),
      ]);

      const raw = yield* engine.subscribeRuntimePartitioned(
        "orders",
        {
          select: ["id", "region"],
          orderBy: [{ field: "id", direction: "asc" }],
        },
        usaPartition,
      );
      const grouped = yield* engine.subscribeRuntimeObservedPartitioned(
        "orders",
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
          where: [{ field: "price", type: "greaterThan", filter: 0 }],
        },
        usaPartition,
        observer,
      );
      const groupedWithoutWhere = yield* engine.subscribeRuntimeObservedPartitioned(
        "orders",
        {
          groupBy: ["status"],
          aggregates: { rowCount: { aggFunc: "count" } },
        },
        usaPartition,
        observer,
      );

      const rawSnapshot = firstEvent(yield* takeEvents(raw, 1));
      const groupedSnapshot = firstEvent(yield* takeEvents(grouped, 1));
      const groupedWithoutWhereSnapshot = firstEvent(yield* takeEvents(groupedWithoutWhere, 1));

      expect(rawSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["usa"],
        rows: [{ id: "usa", region: "usa" }],
        totalRows: 1,
      });
      expect(groupedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-1",
        version: 1,
        keys: [JSON.stringify([["status", JSON.stringify(["present", '"open"'])]])],
        rows: [{ status: "open", rowCount: 1n }],
        totalRows: 1,
      });
      expect(groupedWithoutWhereSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-2",
        version: 1,
        keys: [JSON.stringify([["status", JSON.stringify(["present", '"open"'])]])],
        rows: [{ status: "open", rowCount: 1n }],
        totalRows: 1,
      });

      yield* raw.close();
      yield* grouped.close();
      yield* groupedWithoutWhere.close();
      yield* engine.close();
    }),
  );

  it.effect("uses storage ownership when BigDecimal schema decoding normalizes scale", () =>
    Effect.gen(function* () {
      const expected = BigDecimal.fromStringUnsafe("1.0");
      const exactDecimalPartition: ColumnLiveViewEngineQueryPartition = Object.freeze({
        key: "test-route:decimal-scale",
        matches: (row, storageKey) => {
          if (storageKey !== undefined) {
            return storageKey === "route:scale-one";
          }
          const value = Reflect.get(row, "price");
          return (
            BigDecimal.isBigDecimal(value) &&
            value.value === expected.value &&
            Object.is(value.scale, expected.scale)
          );
        },
        ownedStorageKeys: () => ["route:scale-one"],
      });
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      yield* engine.publishManyDecodedRowsWithStorageKeys("positions", [
        { storageKey: "route:scale-one", row: position("scale-one", "ABC", 1n, "1.0") },
        { storageKey: "route:scale-zero", row: position("scale-zero", "ABC", 1n, "1") },
      ]);

      const subscription = yield* engine.subscribeRuntimePartitioned(
        "positions",
        {
          select: ["id"],
          orderBy: [{ field: "id", direction: "asc" }],
        },
        exactDecimalPartition,
      );
      const snapshot = firstEvent(yield* takeEvents(subscription, 1));

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "positions",
        queryId: "query-0",
        version: 1,
        keys: ["route:scale-one"],
        rows: [{ id: "scale-one" }],
        totalRows: 1,
      });

      yield* subscription.close();
      yield* engine.close();
    }),
  );
});
