import { fromStringUnsafe } from "effect/BigDecimal";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { acquireRawQueryExecution, releaseRawQueryExecution } from "./active-query";
import { prepareRawQuery } from "./raw-query-compiler";
import { publishTopicStoreRow, TopicStore } from "./topic-store";

const invalidRow = (_topic: string, message: string): Error => new Error(message);

describe("column-live-view-engine active query execution", () => {
  it.effect("reuses execution state for identical compiled raw queries", () =>
    Effect.gen(function* () {
      const rowSchema = Schema.Struct({
        id: Schema.String,
        status: Schema.String,
        score: Schema.Number,
        count: Schema.BigInt,
        value: Schema.BigDecimal,
      });
      const store = new TopicStore("scores", rowSchema, "id", () => {});

      yield* publishTopicStoreRow(
        store,
        {
          id: "1",
          status: "open",
          score: 10,
          count: 1n,
          value: fromStringUnsafe("1.00"),
        },
        invalidRow,
      );
      yield* publishTopicStoreRow(
        store,
        {
          id: "2",
          status: "closed",
          score: 20,
          count: 2n,
          value: fromStringUnsafe("2.00"),
        },
        invalidRow,
      );

      const compiled = yield* prepareRawQuery("scores", store.rawQueryMetadata, {
        select: ["id", "score", "count", "value"],
        where: {
          status: "open",
        },
        orderBy: [
          {
            field: "score",
            direction: "desc",
          },
        ],
      });

      const firstExecution = yield* acquireRawQueryExecution(store, compiled);
      const secondExecution = yield* acquireRawQueryExecution(store, compiled);
      expect(firstExecution).toBe(secondExecution);

      const firstCursor = firstExecution.createCursor();
      const secondCursor = secondExecution.createCursor();

      const initialFirst = firstExecution.initial("query-a");
      const initialSecond = secondExecution.initial("query-b");
      expect(initialFirst.rows).toStrictEqual(initialSecond.rows);
      expect(initialFirst.totalRows).toBe(1);
      expect(initialFirst.keys).toStrictEqual(["1"]);

      const beforePublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const beforePublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(beforePublishFirst._tag).toBe("None");
      expect(beforePublishSecond._tag).toBe("None");

      yield* publishTopicStoreRow(
        store,
        {
          id: "3",
          status: "open",
          score: 5,
          count: 3n,
          value: fromStringUnsafe("3.00"),
        },
        invalidRow,
      );

      const afterPublishFirst = yield* firstExecution.next("query-a", firstCursor);
      const afterPublishSecond = yield* secondExecution.next("query-b", secondCursor);
      expect(afterPublishFirst._tag).toBe("Some");
      expect(afterPublishSecond._tag).toBe("Some");

      yield* releaseRawQueryExecution(store, compiled);
      const afterRefcountDecrement = yield* acquireRawQueryExecution(store, compiled);
      expect(afterRefcountDecrement).toBe(firstExecution);

      yield* releaseRawQueryExecution(store, compiled);
      yield* releaseRawQueryExecution(store, compiled);

      const afterRefcountExhausted = yield* acquireRawQueryExecution(store, compiled);
      expect(afterRefcountExhausted).not.toBe(firstExecution);
      yield* releaseRawQueryExecution(store, compiled);
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
      const compiled = yield* prepareRawQuery("numbers", store.rawQueryMetadata, {
        select: ["id"],
        where: {
          score: 1,
        },
      });

      yield* releaseRawQueryExecution(store, compiled);
    }),
  );

  it.effect("covers query cache key paths for numeric, bigint, and BigDecimal filters", () =>
    Effect.gen(function* () {
      const numericStore = new TopicStore(
        "numbers",
        Schema.Struct({ id: Schema.String, score: Schema.Number }),
        "id",
        () => {},
      );
      const bigintStore = new TopicStore(
        "bigints",
        Schema.Struct({ id: Schema.String, amount: Schema.BigInt }),
        "id",
        () => {},
      );
      const decimalStore = new TopicStore(
        "decimals",
        Schema.Struct({ id: Schema.String, price: Schema.BigDecimal }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(numericStore, { id: "a", score: 10 }, invalidRow);
      yield* publishTopicStoreRow(bigintStore, { id: "b", amount: 5n }, invalidRow);
      yield* publishTopicStoreRow(
        decimalStore,
        { id: "c", price: fromStringUnsafe("1.23") },
        invalidRow,
      );

      const infFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: Number.POSITIVE_INFINITY,
        },
      });
      const nanFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: Number.NaN,
        },
      });
      const zeroFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        where: {
          score: 10,
        },
      });
      const offsetFilter = yield* prepareRawQuery("numbers", numericStore.rawQueryMetadata, {
        select: ["id", "score"],
        offset: 1,
      });
      const bigIntFilter = yield* prepareRawQuery("bigints", bigintStore.rawQueryMetadata, {
        select: ["id", "amount"],
        where: {
          amount: 3n,
        },
      });
      const decimalFilter = yield* prepareRawQuery("decimals", decimalStore.rawQueryMetadata, {
        select: ["id", "price"],
        where: {
          price: fromStringUnsafe("1.23"),
        },
      });

      const infExecution = yield* acquireRawQueryExecution(numericStore, infFilter);
      const nanExecution = yield* acquireRawQueryExecution(numericStore, nanFilter);
      const zeroExecution = yield* acquireRawQueryExecution(numericStore, zeroFilter);
      const offsetExecution = yield* acquireRawQueryExecution(numericStore, offsetFilter);
      const bigintExecution = yield* acquireRawQueryExecution(bigintStore, bigIntFilter);
      const decimalExecution = yield* acquireRawQueryExecution(decimalStore, decimalFilter);

      const infCursor = infExecution.createCursor();
      const nanCursor = nanExecution.createCursor();
      const zeroCursor = zeroExecution.createCursor();
      const offsetCursor = offsetExecution.createCursor();
      const bigintCursor = bigintExecution.createCursor();
      const decimalCursor = decimalExecution.createCursor();

      expect(infExecution.initial("q").totalRows).toBe(0);
      expect(nanExecution.initial("q").totalRows).toBe(0);
      expect(zeroExecution.initial("q").totalRows).toBe(1);
      expect(offsetExecution.initial("q").totalRows).toBe(1);
      expect(bigintExecution.initial("q").totalRows).toBe(0);
      expect(decimalExecution.initial("q").totalRows).toBe(1);

      expect(infExecution).not.toBe(nanExecution);
      expect(infExecution).not.toBe(zeroExecution);
      expect(nanExecution).not.toBe(zeroExecution);

      expect((yield* infExecution.next("q", infCursor))._tag).toBe("None");
      expect((yield* nanExecution.next("q", nanCursor))._tag).toBe("None");
      expect((yield* zeroExecution.next("q", zeroCursor))._tag).toBe("None");
      expect((yield* offsetExecution.next("q", offsetCursor))._tag).toBe("None");
      expect((yield* bigintExecution.next("q", bigintCursor))._tag).toBe("None");
      expect((yield* decimalExecution.next("q", decimalCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(numericStore, infFilter);
      yield* releaseRawQueryExecution(numericStore, nanFilter);
      yield* releaseRawQueryExecution(numericStore, zeroFilter);
      yield* releaseRawQueryExecution(numericStore, offsetFilter);
      yield* releaseRawQueryExecution(bigintStore, bigIntFilter);
      yield* releaseRawQueryExecution(decimalStore, decimalFilter);
    }),
  );

  it.effect("covers cache keys for nullish, array, object, and boolean filter values", () =>
    Effect.gen(function* () {
      const eventStore = new TopicStore(
        "events",
        Schema.Struct({
          id: Schema.String,
          label: Schema.String,
          tags: Schema.Array(Schema.String),
          metadata: Schema.Struct({
            kind: Schema.String,
            scope: Schema.String,
          }),
          active: Schema.Boolean,
        }),
        "id",
        () => {},
      );

      yield* publishTopicStoreRow(
        eventStore,
        {
          id: "a",
          label: "foo",
          tags: ["open", "closed"],
          metadata: { kind: "test", scope: "global" },
          active: true,
        },
        invalidRow,
      );

      const undefinedFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "label"],
        where: {
          label: undefined,
        },
      });
      const arrayFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "tags"],
        where: {
          tags: ["open", "closed"],
        },
      });
      const objectFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "metadata"],
        where: {
          metadata: { kind: "test", scope: "global" },
        },
      });
      const booleanFilter = yield* prepareRawQuery("events", eventStore.rawQueryMetadata, {
        select: ["id", "active"],
        where: {
          active: true,
        },
      });

      const undefinedExecution = yield* acquireRawQueryExecution(eventStore, undefinedFilter);
      const arrayExecution = yield* acquireRawQueryExecution(eventStore, arrayFilter);
      const objectExecution = yield* acquireRawQueryExecution(eventStore, objectFilter);
      const booleanExecution = yield* acquireRawQueryExecution(eventStore, booleanFilter);

      expect(undefinedExecution.initial("query").totalRows).toBe(0);
      expect(arrayExecution.initial("query").totalRows).toBe(1);
      expect(objectExecution.initial("query").totalRows).toBe(1);
      expect(booleanExecution.initial("query").totalRows).toBe(1);

      expect(undefinedExecution).not.toBe(arrayExecution);
      expect(arrayExecution).not.toBe(objectExecution);
      expect(objectExecution).not.toBe(booleanExecution);

      const undefinedCursor = undefinedExecution.createCursor();
      const arrayCursor = arrayExecution.createCursor();
      const objectCursor = objectExecution.createCursor();
      const booleanCursor = booleanExecution.createCursor();

      expect((yield* undefinedExecution.next("query", undefinedCursor))._tag).toBe("None");
      expect((yield* arrayExecution.next("query", arrayCursor))._tag).toBe("None");
      expect((yield* objectExecution.next("query", objectCursor))._tag).toBe("None");
      expect((yield* booleanExecution.next("query", booleanCursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(eventStore, undefinedFilter);
      yield* releaseRawQueryExecution(eventStore, arrayFilter);
      yield* releaseRawQueryExecution(eventStore, objectFilter);
      yield* releaseRawQueryExecution(eventStore, booleanFilter);
    }),
  );

  it.effect("covers query cache encoding for non-plain object filter values", () =>
    Effect.gen(function* () {
      const store = new TopicStore(
        "special",
        Schema.Struct({
          id: Schema.String,
          payload: Schema.Struct({
            value: Schema.BigInt,
            label: Schema.String,
          }),
        }),
        "id",
        () => {},
      );
      const queryPayload = Object.create(null) as Record<string, unknown>;
      queryPayload["value"] = 1n;
      queryPayload["label"] = "special";

      const compiled = yield* prepareRawQuery("special", store.rawQueryMetadata, {
        select: ["id", "payload"],
        where: {
          payload: queryPayload,
        },
      });

      const execution = yield* acquireRawQueryExecution(store, compiled);
      const cursor = execution.createCursor();

      expect(execution.initial("query").rows).toStrictEqual([]);
      expect((yield* execution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(store, compiled);
      yield* releaseRawQueryExecution(store, compiled);
    }),
  );

  it.effect("covers cache key encoding for non-serializable filter values", () =>
    Effect.gen(function* () {
      const nonSerializable = () => "token";
      const store = new TopicStore(
        "special-non-serializable",
        Schema.Struct({
          id: Schema.String,
          marker: Schema.Unknown,
        }),
        "id",
        () => {},
      );

      const prepared = yield* prepareRawQuery("special-non-serializable", store.rawQueryMetadata, {
        select: ["id", "marker"],
        where: {
          marker: nonSerializable,
        },
      });
      const execution = yield* acquireRawQueryExecution(store, prepared);
      const cursor = execution.createCursor();

      expect(execution.initial("query").totalRows).toBe(0);
      expect((yield* execution.next("query", cursor))._tag).toBe("None");

      yield* releaseRawQueryExecution(store, prepared);
    }),
  );
});
