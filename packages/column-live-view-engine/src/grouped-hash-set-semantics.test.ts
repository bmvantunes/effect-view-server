import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import * as HashSet from "effect/HashSet";
import { createColumnLiveViewEngine } from "./index";
import {
  applyDelta,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";

const Labels = viewSchema.HashSet(Schema.String);

const HashSetRow = Schema.Struct({
  id: Schema.String,
  desk: Schema.String,
  labels: Labels,
});

type HashSetRow = typeof HashSetRow.Type;

type Labels = typeof Labels.Type;

const hashSetViewServer = defineViewServerConfig({
  topics: {
    hashSetRows: {
      schema: HashSetRow,
      key: "id",
    },
  },
});

const byLabels = {
  groupBy: ["labels"],
  aggregates: { rowCount: { aggFunc: "count" } },
} satisfies GroupedQuery<HashSetRow>;

const distinctLabels = {
  groupBy: ["desk"],
  aggregates: {
    distinctLabels: { aggFunc: "countDistinct", field: "labels" },
  },
} satisfies GroupedQuery<HashSetRow>;

const subsetLabels = HashSet.make("blue", "green");
const equivalentSubsetLabels = HashSet.make("green", "blue");
const supersetLabels = HashSet.make("blue", "green", "red");

const hashSetRow = (id: string, labels: Labels): HashSetRow => ({
  id,
  desk: "equities",
  labels,
});

const sortedLabels = (labels: Labels): ReadonlyArray<string> => Array.from(labels).toSorted();

const normalizedRawRows = (rows: ReadonlyArray<{ readonly id: string; readonly labels: Labels }>) =>
  rows.map((row) => ({
    id: row.id,
    labels: sortedLabels(row.labels),
  }));

const normalizedGroupedRows = (
  rows: ReadonlyArray<{ readonly labels: Labels; readonly rowCount: bigint }>,
) =>
  rows
    .map((row) => ({
      labels: sortedLabels(row.labels),
      rowCount: row.rowCount,
    }))
    .toSorted((left, right) => left.labels.length - right.labels.length);

const initialRawRows = [
  { id: "1", labels: ["blue", "green"] },
  { id: "2", labels: ["blue", "green", "red"] },
];

const supersetRawRows = [
  { id: "1", labels: ["blue", "green", "red"] },
  { id: "2", labels: ["blue", "green", "red"] },
];

const initialGroupedRows = [
  { labels: ["blue", "green"], rowCount: 1n },
  { labels: ["blue", "green", "red"], rowCount: 1n },
];

const supersetGroupedRows = [{ labels: ["blue", "green", "red"], rowCount: 2n }];

describe("ColumnLiveViewEngine grouped HashSet semantics", () => {
  it.effect(
    "keeps subset and superset HashSets distinct across predicates, groups, and replacements",
    () =>
      Effect.gen(function* () {
        const engine = yield* createColumnLiveViewEngine({
          topics: hashSetViewServer.topics,
        });
        yield* engine.publishMany("hashSetRows", [
          hashSetRow("1", subsetLabels),
          hashSetRow("2", supersetLabels),
        ]);

        const subsetSnapshot = yield* engine.snapshot("hashSetRows", {
          select: ["id"],
          where: { labels: { eq: subsetLabels } },
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const supersetSnapshot = yield* engine.snapshot("hashSetRows", {
          select: ["id"],
          where: { labels: { eq: supersetLabels } },
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const groupedSnapshot = yield* engine.snapshot("hashSetRows", byLabels);
        const distinctSnapshot = yield* engine.snapshot("hashSetRows", distinctLabels);

        expect({
          subset: subsetSnapshot.rows,
          superset: supersetSnapshot.rows,
          grouped: normalizedGroupedRows(groupedSnapshot.rows),
          distinct: distinctSnapshot.rows,
        }).toStrictEqual({
          subset: [{ id: "1" }],
          superset: [{ id: "2" }],
          grouped: initialGroupedRows,
          distinct: [{ desk: "equities", distinctLabels: 2n }],
        });

        const rawSubscription = yield* engine.subscribe("hashSetRows", {
          select: ["id", "labels"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const subsetSubscription = yield* engine.subscribe("hashSetRows", {
          select: ["id"],
          where: { labels: { eq: subsetLabels } },
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const groupedSubscription = yield* engine.subscribe("hashSetRows", byLabels);
        const distinctSubscription = yield* engine.subscribe("hashSetRows", distinctLabels);
        const readRaw = yield* makeEventReader(rawSubscription);
        const readSubset = yield* makeEventReader(subsetSubscription);
        const readGrouped = yield* makeEventReader(groupedSubscription);
        const readDistinct = yield* makeEventReader(distinctSubscription);
        const rawInitial = firstEvent(yield* readRaw(1));
        const subsetInitial = firstEvent(yield* readSubset(1));
        const groupedInitial = firstEvent(yield* readGrouped(1));
        const distinctInitial = firstEvent(yield* readDistinct(1));
        expectSnapshotEvent(rawInitial);
        expectSnapshotEvent(subsetInitial);
        expectSnapshotEvent(groupedInitial);
        expectSnapshotEvent(distinctInitial);
        let rawState = stateFromSnapshot(rawInitial);
        let subsetState = stateFromSnapshot(subsetInitial);
        let groupedState = stateFromSnapshot(groupedInitial);
        let distinctState = stateFromSnapshot(distinctInitial);

        yield* engine.publish("hashSetRows", hashSetRow("1", equivalentSubsetLabels));

        const afterEquivalent = yield* engine.snapshot("hashSetRows", {
          select: ["id", "labels"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const equivalentHealth = yield* engine.health();
        expect({
          snapshotVersion: afterEquivalent.version,
          healthVersion: equivalentHealth.version,
          queuedEvents: equivalentHealth.queuedEvents,
          rows: normalizedRawRows(afterEquivalent.rows),
        }).toStrictEqual({
          snapshotVersion: 1,
          healthVersion: 1,
          queuedEvents: 0,
          rows: initialRawRows,
        });

        yield* engine.publish("hashSetRows", hashSetRow("1", supersetLabels));
        const rawToSuperset = firstEvent(yield* readRaw(1));
        const subsetToSuperset = firstEvent(yield* readSubset(1));
        const groupedToSuperset = firstEvent(yield* readGrouped(1));
        const distinctToSuperset = firstEvent(yield* readDistinct(1));
        expectDeltaEvent(rawToSuperset);
        expectDeltaEvent(subsetToSuperset);
        expectDeltaEvent(groupedToSuperset);
        expectDeltaEvent(distinctToSuperset);
        rawState = applyDelta(rawState, rawToSuperset);
        subsetState = applyDelta(subsetState, subsetToSuperset);
        groupedState = applyDelta(groupedState, groupedToSuperset);
        distinctState = applyDelta(distinctState, distinctToSuperset);
        const rawAfterSuperset = yield* engine.snapshot("hashSetRows", {
          select: ["id", "labels"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const subsetAfterSuperset = yield* engine.snapshot("hashSetRows", {
          select: ["id"],
          where: { labels: { eq: subsetLabels } },
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const groupedAfterSuperset = yield* engine.snapshot("hashSetRows", byLabels);
        const distinctAfterSuperset = yield* engine.snapshot("hashSetRows", distinctLabels);

        expect(rawToSuperset).toStrictEqual({
          type: "delta",
          topic: "hashSetRows",
          queryId: rawInitial.queryId,
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "1",
              row: { id: "1", labels: supersetLabels },
              index: 0,
            },
          ],
          totalRows: 2,
        });
        expect(subsetToSuperset).toStrictEqual({
          type: "delta",
          topic: "hashSetRows",
          queryId: subsetInitial.queryId,
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "1" }],
          totalRows: 0,
        });
        expect({
          raw: normalizedRawRows(rawState.rows),
          rawFresh: normalizedRawRows(rawAfterSuperset.rows),
          subset: subsetState.rows,
          subsetFresh: subsetAfterSuperset.rows,
          grouped: normalizedGroupedRows(groupedState.rows),
          groupedFresh: normalizedGroupedRows(groupedAfterSuperset.rows),
          distinct: distinctState.rows,
          distinctFresh: distinctAfterSuperset.rows,
        }).toStrictEqual({
          raw: supersetRawRows,
          rawFresh: supersetRawRows,
          subset: [],
          subsetFresh: [],
          grouped: supersetGroupedRows,
          groupedFresh: supersetGroupedRows,
          distinct: [{ desk: "equities", distinctLabels: 1n }],
          distinctFresh: [{ desk: "equities", distinctLabels: 1n }],
        });

        yield* engine.publish("hashSetRows", hashSetRow("1", subsetLabels));
        const rawToSubset = firstEvent(yield* readRaw(1));
        const subsetToSubset = firstEvent(yield* readSubset(1));
        const groupedToSubset = firstEvent(yield* readGrouped(1));
        const distinctToSubset = firstEvent(yield* readDistinct(1));
        expectDeltaEvent(rawToSubset);
        expectDeltaEvent(subsetToSubset);
        expectDeltaEvent(groupedToSubset);
        expectDeltaEvent(distinctToSubset);
        rawState = applyDelta(rawState, rawToSubset);
        subsetState = applyDelta(subsetState, subsetToSubset);
        groupedState = applyDelta(groupedState, groupedToSubset);
        distinctState = applyDelta(distinctState, distinctToSubset);
        const rawAfterSubset = yield* engine.snapshot("hashSetRows", {
          select: ["id", "labels"],
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const subsetAfterSubset = yield* engine.snapshot("hashSetRows", {
          select: ["id"],
          where: { labels: { eq: subsetLabels } },
          orderBy: [{ field: "id", direction: "asc" }],
        });
        const groupedAfterSubset = yield* engine.snapshot("hashSetRows", byLabels);
        const distinctAfterSubset = yield* engine.snapshot("hashSetRows", distinctLabels);

        expect(rawToSubset).toStrictEqual({
          type: "delta",
          topic: "hashSetRows",
          queryId: rawInitial.queryId,
          fromVersion: 2,
          toVersion: 3,
          operations: [
            {
              type: "update",
              key: "1",
              row: { id: "1", labels: subsetLabels },
              index: 0,
            },
          ],
          totalRows: 2,
        });
        expect(subsetToSubset).toStrictEqual({
          type: "delta",
          topic: "hashSetRows",
          queryId: subsetInitial.queryId,
          fromVersion: 2,
          toVersion: 3,
          operations: [
            {
              type: "insert",
              key: "1",
              row: { id: "1" },
              index: 0,
            },
          ],
          totalRows: 1,
        });
        expect({
          raw: normalizedRawRows(rawState.rows),
          rawFresh: normalizedRawRows(rawAfterSubset.rows),
          subset: subsetState.rows,
          subsetFresh: subsetAfterSubset.rows,
          grouped: normalizedGroupedRows(groupedState.rows),
          groupedFresh: normalizedGroupedRows(groupedAfterSubset.rows),
          distinct: distinctState.rows,
          distinctFresh: distinctAfterSubset.rows,
        }).toStrictEqual({
          raw: initialRawRows,
          rawFresh: initialRawRows,
          subset: [{ id: "1" }],
          subsetFresh: [{ id: "1" }],
          grouped: initialGroupedRows,
          groupedFresh: initialGroupedRows,
          distinct: [{ desk: "equities", distinctLabels: 2n }],
          distinctFresh: [{ desk: "equities", distinctLabels: 2n }],
        });

        yield* rawSubscription.close();
        yield* subsetSubscription.close();
        yield* groupedSubscription.close();
        yield* distinctSubscription.close();
      }),
  );
});
