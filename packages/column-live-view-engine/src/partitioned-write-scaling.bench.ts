// Import Vitest directly so @effect/vitest's eager test-runtime module graph does not
// distort the heap, JIT, and GC behavior this benchmark is measuring.
import { bench, describe } from "vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  createColumnLiveViewEngineInternal,
  type ColumnLiveViewEngineQueryPartition,
} from "./internal";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.String,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const benchmarkRouteCounts = [1, 25] as const;

const runPartitionedWriteSample = (routeCount: number): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      const regions = Array.from({ length: routeCount }, (_value, index) => `region-${index}`);
      const subscriptions = yield* Effect.forEach(regions, (region) => {
        const partition: ColumnLiveViewEngineQueryPartition = Object.freeze({
          key: `partition:${region}`,
          matches: (_row, storageKey) => storageKey === region,
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

      yield* Effect.forEach(
        regions,
        (region) =>
          engine.publishManyDecodedRowsWithStorageKeys(
            "orders",
            [
              {
                storageKey: region,
                row: { id: region, status: "open", region },
              },
            ],
            `partition:${region}`,
          ),
        { discard: true },
      );

      yield* Effect.forEach(subscriptions, (subscription) => subscription.close(), {
        discard: true,
      });
      yield* engine.close();
    }),
  );

describe("partitioned raw and grouped live-write scaling", () => {
  for (const routeCount of benchmarkRouteCounts) {
    bench(
      `${routeCount} active route${routeCount === 1 ? "" : "s"}`,
      () => runPartitionedWriteSample(routeCount),
      {
        iterations: 5,
        time: 0,
        warmupIterations: 0,
        warmupTime: 0,
      },
    );
  }
});
