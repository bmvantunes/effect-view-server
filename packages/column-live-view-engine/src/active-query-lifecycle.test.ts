import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  acquireRawQueryExecution,
  activeQueryTestInterface,
  activeQueryTestMetadata,
  activeStoreRawQueryExecutionCount,
  releaseRawQueryExecution,
} from "../test-harness/active-query-interface";
import { prepareRuntimeRawQuery } from "./raw-query-compiler";
import { publishTopicStoreRow, resetTopicStore, TopicStore } from "./topic-store";

const invalidRow = (_topic: string, message: string): Error => new Error(message);
describe("column-live-view-engine Active Query lifecycle", () => {
  it.effect("releases retained raw changes when a topic reset clears active queries", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "raw-insert-reset-release",
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          score: Schema.Number,
        }),
        "id",
        () => {},
      );
      yield* publishTopicStoreRow(store, { id: "a", status: "open", score: 1 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "b", status: "open", score: 2 }, invalidRow);
      yield* publishTopicStoreRow(store, { id: "c", status: "open", score: 3 }, invalidRow);

      const queryInterface = activeQueryTestInterface(store);
      const compiled = yield* prepareRuntimeRawQuery(
        "raw-insert-reset-release",
        activeQueryTestMetadata(store),
        {
          select: ["id"],
          where: [{ field: "status", type: "equals", filter: "open" }],
          orderBy: [{ field: "score", direction: "desc" }],
          limit: 2,
        },
      );

      yield* acquireRawQueryExecution(queryInterface, compiled);
      yield* publishTopicStoreRow(store, { id: "d", status: "open", score: 4 }, invalidRow);
      expect(queryInterface.changesSince(3)).toBeDefined();

      yield* resetTopicStore(store);
      expect(yield* activeStoreRawQueryExecutionCount(queryInterface)).toBe(0);

      yield* publishTopicStoreRow(store, { id: "e", status: "open", score: 5 }, invalidRow);
      expect(queryInterface.changesSince(0)).toBeUndefined();
    }),
  );

  it.effect("no-ops release when execution cache does not contain a query", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "numbers",
        Schema.Struct({ id: Schema.String, score: Schema.Number }),
        "id",
        () => {},
      );
      const compiled = yield* prepareRuntimeRawQuery("numbers", activeQueryTestMetadata(store), {
        select: ["id"],
        where: [{ field: "score", type: "equals", filter: 1 }],
      });

      yield* releaseRawQueryExecution(activeQueryTestInterface(store), compiled);
    }),
  );
});
