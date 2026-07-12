import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine } from "./index";
import {
  expectDeltaEvent,
  expectDefined,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
} from "../test-harness/events";

const OptionalAggregateRow = Schema.Struct({
  id: Schema.String,
  team: Schema.String,
  desk: Schema.optionalKey(Schema.String),
});

type OptionalAggregateRow = typeof OptionalAggregateRow.Type;

const optionalAggregateViewServer = defineViewServerConfig({
  topics: {
    optionalAggregateRows: {
      schema: OptionalAggregateRow,
      key: "id",
    },
  },
});

const optionalAggregateQuery = {
  groupBy: ["team"],
  aggregates: {
    distinctDesks: { aggFunc: "countDistinct", field: "desk" },
    minDesk: { aggFunc: "min", field: "desk" },
    maxDesk: { aggFunc: "max", field: "desk" },
  },
  orderBy: [{ field: "team", direction: "asc" }],
} satisfies GroupedQuery<OptionalAggregateRow>;

describe("ColumnLiveViewEngine grouped optional aggregate semantics", () => {
  it.effect("preserves omitted optional aggregate values in full-scan results", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: optionalAggregateViewServer.topics,
      });
      yield* engine.publishMany("optionalAggregateRows", [
        { id: "missing-1", team: "alpha" },
        { id: "missing-2", team: "alpha" },
        { id: "a", team: "alpha", desk: "a" },
        { id: "b", team: "alpha", desk: "b" },
        { id: "beta-missing", team: "beta" },
      ]);

      const snapshot = yield* engine.snapshot("optionalAggregateRows", {
        groupBy: ["team"],
        aggregates: {
          distinctDesks: { aggFunc: "countDistinct", field: "desk" },
          minDesk: { aggFunc: "min", field: "desk" },
          maxDesk: { aggFunc: "max", field: "desk" },
        },
        orderBy: [
          { aggregate: "minDesk", direction: "asc" },
          { field: "team", direction: "asc" },
        ],
        limit: 2,
      });

      expect(snapshot.rows).toStrictEqual([
        {
          team: "alpha",
          distinctDesks: 3n,
          minDesk: undefined,
          maxDesk: "b",
        },
        {
          team: "beta",
          distinctDesks: 1n,
          minDesk: undefined,
          maxDesk: undefined,
        },
      ]);
      expect(Object.keys(snapshot.rows[0] ?? {})).toStrictEqual([
        "team",
        "distinctDesks",
        "minDesk",
        "maxDesk",
      ]);
    }),
  );

  it.effect(
    "keeps optional distinct and extrema exact across incremental inserts, patches, deletes, and recomputes",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          topics: optionalAggregateViewServer.topics,
        });
        yield* engine.publishMany("optionalAggregateRows", [
          { id: "missing-1", team: "alpha" },
          { id: "missing-2", team: "alpha" },
          { id: "a", team: "alpha", desk: "a" },
          { id: "b", team: "alpha", desk: "b" },
          { id: "beta-missing", team: "beta" },
        ]);

        const subscription = yield* engine.subscribe(
          "optionalAggregateRows",
          optionalAggregateQuery,
        );
        const read = yield* makeEventReader(subscription);
        const snapshot = firstEvent(yield* read(1));
        expectSnapshotEvent(snapshot);
        expect(snapshot.rows).toStrictEqual([
          {
            team: "alpha",
            distinctDesks: 3n,
            minDesk: undefined,
            maxDesk: "b",
          },
          {
            team: "beta",
            distinctDesks: 1n,
            minDesk: undefined,
            maxDesk: undefined,
          },
        ]);
        const alphaKey = expectDefined(snapshot.keys[0]);
        const betaKey = expectDefined(snapshot.keys[1]);
        const queryId = snapshot.queryId;

        yield* engine.patch("optionalAggregateRows", "missing-1", { desk: "c" });
        const missingToPresent = firstEvent(yield* read(1));
        expectDeltaEvent(missingToPresent);
        expect(missingToPresent).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: alphaKey,
              row: {
                team: "alpha",
                distinctDesks: 4n,
                minDesk: undefined,
                maxDesk: "c",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        });

        yield* engine.delete("optionalAggregateRows", "missing-2");
        const removeLastMissing = firstEvent(yield* read(1));
        expectDeltaEvent(removeLastMissing);
        expect(removeLastMissing).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 2,
          toVersion: 3,
          operations: [
            {
              type: "update",
              key: alphaKey,
              row: {
                team: "alpha",
                distinctDesks: 3n,
                minDesk: "a",
                maxDesk: "c",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        });

        yield* engine.patch("optionalAggregateRows", "missing-1", { desk: "aa" });
        const patchCurrentMaximum = firstEvent(yield* read(1));
        expectDeltaEvent(patchCurrentMaximum);
        expect(patchCurrentMaximum).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 3,
          toVersion: 4,
          operations: [
            {
              type: "update",
              key: alphaKey,
              row: {
                team: "alpha",
                distinctDesks: 3n,
                minDesk: "a",
                maxDesk: "b",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        });

        yield* engine.delete("optionalAggregateRows", "b");
        const deleteCurrentMaximum = firstEvent(yield* read(1));
        expectDeltaEvent(deleteCurrentMaximum);
        expect(deleteCurrentMaximum).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 4,
          toVersion: 5,
          operations: [
            {
              type: "update",
              key: alphaKey,
              row: {
                team: "alpha",
                distinctDesks: 2n,
                minDesk: "a",
                maxDesk: "aa",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        });

        yield* engine.publish("optionalAggregateRows", { id: "a", team: "alpha" });
        const presentToMissing = firstEvent(yield* read(1));
        expectDeltaEvent(presentToMissing);
        expect(presentToMissing).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 5,
          toVersion: 6,
          operations: [
            {
              type: "update",
              key: alphaKey,
              row: {
                team: "alpha",
                distinctDesks: 2n,
                minDesk: undefined,
                maxDesk: "aa",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        });

        yield* engine.publish("optionalAggregateRows", {
          id: "beta-present",
          team: "beta",
          desk: "z",
        });
        const insertPresent = firstEvent(yield* read(1));
        expectDeltaEvent(insertPresent);
        expect(insertPresent).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 6,
          toVersion: 7,
          operations: [
            {
              type: "update",
              key: betaKey,
              row: {
                team: "beta",
                distinctDesks: 2n,
                minDesk: undefined,
                maxDesk: "z",
              },
              index: 1,
            },
          ],
          totalRows: 2,
        });

        yield* engine.delete("optionalAggregateRows", "beta-missing");
        const deleteMissingExtremum = firstEvent(yield* read(1));
        expectDeltaEvent(deleteMissingExtremum);
        expect(deleteMissingExtremum).toStrictEqual({
          type: "delta",
          topic: "optionalAggregateRows",
          queryId,
          fromVersion: 7,
          toVersion: 8,
          operations: [
            {
              type: "update",
              key: betaKey,
              row: {
                team: "beta",
                distinctDesks: 1n,
                minDesk: "z",
                maxDesk: "z",
              },
              index: 1,
            },
          ],
          totalRows: 2,
        });

        yield* subscription.close();
      }),
  );
});
