import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine } from "./index";

it.effect(
  "keeps retained rows queryable through key-index growth, replacement and slot compaction",
  () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: { rows: { schema: Schema.Struct({ id: ViewServerId, value: Schema.Number }) } },
      });
      const engine = yield* createColumnLiveViewEngine({ topics: config.topics });
      for (let start = 0; start < 70_000; start += 1000) {
        yield* engine.publishMany(
          "rows",
          Array.from({ length: 1000 }, (_, offset) => ({
            id: `row-${start + offset}`,
            value: start + offset,
          })),
        );
      }
      const loaded = yield* engine.snapshot("rows", { select: ["id", "value"], limit: 1 });
      expect(loaded.totalRows).toBe(70_000);
      yield* engine.publish("rows", { id: "row-1", value: -1 });
      yield* engine.delete("rows", "row-0");
      yield* engine.patch("rows", "row-69999", { value: -2 });
      const replaced = yield* engine.snapshot("rows", {
        select: ["id", "value"],
        where: [{ field: "id", type: "equals", filter: "row-1" }],
        limit: 1,
      });
      expect(replaced.rows).toStrictEqual([{ id: "row-1", value: -1 }]);
      const moved = yield* engine.snapshot("rows", {
        select: ["id", "value"],
        where: [{ field: "id", type: "equals", filter: "row-69999" }],
        limit: 1,
      });
      expect(moved.rows).toStrictEqual([{ id: "row-69999", value: -2 }]);
      const retained = yield* engine.snapshot("rows", { select: ["id"], limit: 1 });
      expect(retained.totalRows).toBe(69_999);
      yield* engine.reset();
      const empty = yield* engine.snapshot("rows", { select: ["id"], limit: 1 });
      expect(empty.rows).toStrictEqual([]);
      expect(empty.totalRows).toBe(0);
      yield* engine.close();
    }),
);
