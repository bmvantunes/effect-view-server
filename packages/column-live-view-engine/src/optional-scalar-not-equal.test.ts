import { defineViewServerConfig } from "@effect-view-server/config";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, Stream } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { createColumnLiveViewEngine } from "./index";

const OptionalScalarRow = Schema.Struct({
  id: Schema.String,
  number: Schema.optionalKey(Schema.Number),
  bigint: Schema.optionalKey(Schema.BigInt),
  amount: Schema.Union([Schema.BigDecimal, Schema.Undefined]),
  active: Schema.optionalKey(Schema.Boolean),
  nullable: Schema.optionalKey(Schema.Union([Schema.Null, Schema.Number])),
});

const viewServer = defineViewServerConfig({
  topics: {
    optionalScalars: {
      schema: OptionalScalarRow,
      key: "id",
    },
  },
});

const expectedRows = [{ id: "different" }, { id: "missing" }];

describe("optional scalar notEqual semantics", () => {
  it.effect("keeps notEqual as the exact complement of equals across optimized paths", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({ topics: viewServer.topics });
      yield* engine.publishMany("optionalScalars", [
        { id: "missing", amount: undefined },
        {
          id: "matching",
          number: 1,
          bigint: 1n,
          amount: fromStringUnsafe("1"),
          active: true,
          nullable: null,
        },
        {
          id: "different",
          number: 2,
          bigint: 2n,
          amount: fromStringUnsafe("2"),
          active: false,
          nullable: 2,
        },
      ]);

      const number = yield* engine.snapshot("optionalScalars", {
        select: ["id"],
        where: [{ field: "number", type: "notEqual", filter: 1 }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const bigint = yield* engine.snapshot("optionalScalars", {
        select: ["id"],
        where: [{ field: "bigint", type: "notEqual", filter: 1n }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const amount = yield* engine.snapshot("optionalScalars", {
        select: ["id"],
        where: [{ field: "amount", type: "notEqual", filter: fromStringUnsafe("1") }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const active = yield* engine.snapshot("optionalScalars", {
        select: ["id"],
        where: [{ field: "active", type: "notEqual", filter: true }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const nullable = yield* engine.snapshot("optionalScalars", {
        select: ["id"],
        where: [{ field: "nullable", type: "notEqual", filter: null }],
        orderBy: [{ field: "id", direction: "asc" }],
      });

      expect(number.rows).toStrictEqual(expectedRows);
      expect(bigint.rows).toStrictEqual(expectedRows);
      expect(amount.rows).toStrictEqual(expectedRows);
      expect(active.rows).toStrictEqual(expectedRows);
      expect(nullable.rows).toStrictEqual(expectedRows);

      const subscription = yield* engine.subscribe("optionalScalars", {
        select: ["id"],
        where: [{ field: "number", type: "notEqual", filter: 1 }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "optionalScalars",
          queryId: "query-0",
          version: 1,
          keys: ["different", "missing"],
          rows: expectedRows,
          totalRows: 2,
        },
      ]);

      const grouped = yield* engine.snapshot("optionalScalars", {
        groupBy: ["id"],
        aggregates: { rowCount: { aggFunc: "count" } },
        where: [{ field: "number", type: "notEqual", filter: 1 }],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      expect(grouped.rows).toStrictEqual([
        { id: "different", rowCount: 1n },
        { id: "missing", rowCount: 1n },
      ]);
      expect(grouped.totalRows).toBe(2);

      yield* subscription.close();
      yield* engine.close();
    }),
  );
});
