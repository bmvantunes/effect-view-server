import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
} from "../test-harness/events";
import { createColumnLiveViewEngine } from "./index";
import { createColumnLiveViewEngineInternal } from "./internal";

const OptionalUndefinedRow = Schema.Struct({
  id: Schema.String,
  desk: Schema.optionalKey(Schema.Union([Schema.String, Schema.Undefined])),
});

type OptionalUndefinedRow = typeof OptionalUndefinedRow.Type;

const optionalUndefinedViewServer = defineViewServerConfig({
  topics: {
    optionalUndefinedRows: {
      schema: OptionalUndefinedRow,
      key: "id",
    },
  },
});

const groupedByOptionalUndefined = {
  groupBy: ["desk"],
  aggregates: {
    rowCount: { aggFunc: "count" },
  },
  orderBy: [{ field: "desk", direction: "asc" }],
} satisfies GroupedQuery<OptionalUndefinedRow>;

describe("ColumnLiveViewEngine optional undefined presence semantics", () => {
  it.effect("preserves missing versus present undefined across publish and patch", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: optionalUndefinedViewServer.topics,
      });
      yield* engine.publishMany("optionalUndefinedRows", [{ id: "one" }, { id: "two" }]);

      const subscription = yield* engine.subscribe(
        "optionalUndefinedRows",
        groupedByOptionalUndefined,
      );
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      expect(initial).toStrictEqual({
        type: "snapshot",
        topic: "optionalUndefinedRows",
        queryId: initial.queryId,
        version: 1,
        keys: ['[["desk","[\\"missing\\"]"]]'],
        rows: [{ rowCount: 2n }],
        totalRows: 1,
      });

      yield* engine.publish("optionalUndefinedRows", {
        id: "one",
        desk: undefined,
      });
      const published = firstEvent(yield* read(1));
      expectDeltaEvent(published);
      expect(published).toStrictEqual({
        type: "delta",
        topic: "optionalUndefinedRows",
        queryId: initial.queryId,
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: '[["desk","[\\"missing\\"]"]]',
            row: { rowCount: 1n },
            index: 0,
          },
          {
            type: "insert",
            key: '[["desk","[\\"present\\",\\"null\\"]"]]',
            row: { desk: undefined, rowCount: 1n },
            index: 1,
          },
        ],
        totalRows: 2,
      });

      const afterPublish = yield* engine.snapshot("optionalUndefinedRows", {
        select: ["id", "desk"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      expect(afterPublish.rows).toStrictEqual([{ id: "one", desk: undefined }, { id: "two" }]);
      expect(Object.keys(afterPublish.rows[0] ?? {})).toStrictEqual(["id", "desk"]);
      expect(Object.keys(afterPublish.rows[1] ?? {})).toStrictEqual(["id"]);

      yield* engine.patch("optionalUndefinedRows", "two", { desk: undefined });
      const patched = firstEvent(yield* read(1));
      expectDeltaEvent(patched);
      expect(patched).toStrictEqual({
        type: "delta",
        topic: "optionalUndefinedRows",
        queryId: initial.queryId,
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "remove",
            key: '[["desk","[\\"missing\\"]"]]',
          },
          {
            type: "update",
            key: '[["desk","[\\"present\\",\\"null\\"]"]]',
            row: { desk: undefined, rowCount: 2n },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      const afterPatch = yield* engine.snapshot("optionalUndefinedRows", {
        select: ["id", "desk"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      expect(afterPatch.rows).toStrictEqual([
        { id: "one", desk: undefined },
        { id: "two", desk: undefined },
      ]);
      expect(Object.keys(afterPatch.rows[0] ?? {})).toStrictEqual(["id", "desk"]);
      expect(Object.keys(afterPatch.rows[1] ?? {})).toStrictEqual(["id", "desk"]);
      yield* subscription.close();
    }),
  );

  it.effect("preserves presence through decoded patches and reverse replacements", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngineInternal({
        topics: optionalUndefinedViewServer.topics,
      });
      yield* engine.publishManyDecodedRows("optionalUndefinedRows", [
        { id: "one", desk: undefined },
        { id: "two" },
      ]);

      yield* engine.patchDecodedFields("optionalUndefinedRows", "two", { desk: undefined });
      let snapshot = yield* engine.snapshot("optionalUndefinedRows", {
        select: ["id", "desk"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      expect(snapshot.version).toBe(2);
      expect(snapshot.rows).toStrictEqual([
        { id: "one", desk: undefined },
        { id: "two", desk: undefined },
      ]);
      expect(Object.keys(snapshot.rows[0] ?? {})).toStrictEqual(["id", "desk"]);
      expect(Object.keys(snapshot.rows[1] ?? {})).toStrictEqual(["id", "desk"]);

      yield* engine.publishManyDecodedRows("optionalUndefinedRows", [{ id: "one" }]);
      snapshot = yield* engine.snapshot("optionalUndefinedRows", {
        select: ["id", "desk"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      expect(snapshot.version).toBe(3);
      expect(snapshot.rows).toStrictEqual([{ id: "one" }, { id: "two", desk: undefined }]);
      expect(Object.keys(snapshot.rows[0] ?? {})).toStrictEqual(["id"]);
      expect(Object.keys(snapshot.rows[1] ?? {})).toStrictEqual(["id", "desk"]);
      yield* engine.close();
    }),
  );
});
