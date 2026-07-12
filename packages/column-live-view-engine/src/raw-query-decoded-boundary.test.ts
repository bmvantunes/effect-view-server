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
        where: { amount: { eq: 1n } },
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const encodedError = yield* Effect.flip(
        engine.subscribeRuntime("orders", {
          select: ["id"],
          where: { amount: { eq: "1" } },
        }),
      );

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
      yield* subscription.close();
      yield* engine.close();
    }),
  );
});
