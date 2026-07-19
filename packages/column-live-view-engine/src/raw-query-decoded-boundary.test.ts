import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema, Stream } from "effect";
import { InvalidQueryError } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";

const Order = Schema.Struct({
  id: Schema.String,
  amount: Schema.BigIntFromString,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

describe("Raw query decoded value boundary", () => {
  it.effect("accepts decoded operands and rejects encoded schema representations", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({ topics: viewServer.topics });
      yield* engine.publishManyDecodedRows("orders", [{ id: "one", amount: 1n }]);

      const subscription = yield* engine.subscribeRuntime("orders", {
        select: ["id", "amount"],
        where: [{ field: "amount", type: "equals", filter: 1n }],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const encodedError = yield* Effect.flip(
        engine.subscribeRuntime("orders", {
          select: ["id"],
          where: [{ field: "amount", type: "equals", filter: "1" }],
        }),
      );
      const rawSnapshot = yield* engine.snapshotRuntime("orders", {
        select: ["id"],
        where: [{ field: "amount", type: "equals", filter: 1n }],
      });
      const groupedSnapshot = yield* engine.snapshotRuntime("orders", {
        groupBy: ["id"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const cyclicQuery: Record<string, unknown> = { select: ["id"] };
      cyclicQuery["cycle"] = cyclicQuery;
      const cyclicError = yield* Effect.flip(engine.subscribeRuntime("orders", cyclicQuery));
      const hostileThrownValue = {
        toString: () => {
          throw new Error("hostile toString must never run");
        },
      };
      const hostileQuery = new Proxy(
        { select: ["id"] },
        {
          ownKeys: () => {
            throw hostileThrownValue;
          },
        },
      );
      const hostileError = yield* Effect.flip(engine.subscribeRuntime("orders", hostileQuery));

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["one"],
          rows: [{ id: "one", amount: 1n }],
          totalRows: 1,
        },
      ]);
      expect(encodedError).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Raw query where field amount does not satisfy its configured schema.",
        }),
      );
      expect(rawSnapshot).toStrictEqual({
        rows: [{ id: "one" }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(groupedSnapshot).toStrictEqual({
        rows: [{ id: "one", rowCount: 1n }],
        totalRows: 1,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(cyclicError.message).toBe("Query input could not be snapshotted at subscribe.");
      expect(hostileError).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Query input could not be snapshotted at subscribe.",
        }),
      );
      yield* subscription.close();
      yield* engine.close();
    }),
  );
});
