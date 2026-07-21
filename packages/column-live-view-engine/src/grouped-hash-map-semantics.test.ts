import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery, viewSchema } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import * as HashMap from "effect/HashMap";
import { createColumnLiveViewEngine } from "./index";
import {
  applyDelta,
  expectDefined,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";

const collisionLeft = "8ocpIaaa";
const collisionRight = "GpcpIaaa";

const Attributes = viewSchema.HashMap(Schema.String, Schema.String);

const HashMapRow = Schema.Struct({
  id: Schema.String,
  desk: Schema.String,
  attributes: Attributes,
});

type HashMapRow = typeof HashMapRow.Type;

type Attributes = typeof Attributes.Type;

const hashMapViewServer = defineViewServerConfig({
  topics: {
    hashMapRows: {
      schema: HashMapRow,
      key: "id",
    },
  },
});

const byAttributes = {
  groupBy: ["attributes"],
  aggregates: { rowCount: { aggFunc: "count" } },
} satisfies GroupedQuery<HashMapRow>;

const distinctAttributes = {
  groupBy: ["desk"],
  aggregates: {
    distinctAttributes: { aggFunc: "countDistinct", field: "attributes" },
  },
} satisfies GroupedQuery<HashMapRow>;

const subsetAttributes = HashMap.make(["desk", "equities"], ["region", "emea"]);
const equivalentSubsetAttributes = HashMap.make(["region", "emea"], ["desk", "equities"]);
const supersetAttributes = HashMap.make(["desk", "equities"], ["region", "emea"], ["tier", "one"]);

const hashMapRow = (id: string, attributes: Attributes): HashMapRow => ({
  id,
  desk: "equities",
  attributes,
});

const sortedEntries = (
  attributes: Attributes,
): ReadonlyArray<readonly [key: string, value: string]> =>
  HashMap.toEntries(attributes).toSorted(
    ([left], [right]) => Number(left > right) - Number(left < right),
  );

const normalizedRawRows = (
  rows: ReadonlyArray<{ readonly id: string; readonly attributes: Attributes }>,
) =>
  rows.map((row) => ({
    id: row.id,
    attributes: sortedEntries(row.attributes),
  }));

const normalizedGroupedRows = (
  rows: ReadonlyArray<{ readonly attributes: Attributes; readonly rowCount: bigint }>,
) =>
  rows
    .map((row) => ({
      attributes: sortedEntries(row.attributes),
      rowCount: row.rowCount,
    }))
    .toSorted((left, right) => left.attributes.length - right.attributes.length);

describe("ColumnLiveViewEngine grouped HashMap semantics", () => {
  it.effect("uses order-neutral HashMap identity for grouping and countDistinct", () =>
    Effect.gen(function* () {
      const first = HashMap.make([collisionLeft, "one"], [collisionRight, "two"]);
      const second = HashMap.make([collisionRight, "two"], [collisionLeft, "one"]);
      expect(Schema.toEquivalence(Attributes)(first, second)).toBe(true);
      expect(Schema.encodeUnknownSync(Schema.toCodecJson(Attributes))(first)).not.toStrictEqual(
        Schema.encodeUnknownSync(Schema.toCodecJson(Attributes))(second),
      );

      const engine = yield* createColumnLiveViewEngine({
        topics: hashMapViewServer.topics,
      });
      yield* engine.publishMany("hashMapRows", [
        { id: "1", desk: "equities", attributes: first },
        { id: "2", desk: "equities", attributes: second },
      ]);

      const grouped = yield* engine.snapshot("hashMapRows", byAttributes);
      const distinct = yield* engine.snapshot("hashMapRows", distinctAttributes);
      const subscription = yield* engine.subscribe("hashMapRows", byAttributes);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      const groupedKey = initial.keys[0]!;

      yield* engine.publish("hashMapRows", {
        id: "3",
        desk: "equities",
        attributes: first,
      });
      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);

      expect(
        grouped.rows.map((row) => ({
          attributes: HashMap.toEntries(row.attributes).toSorted(
            ([left], [right]) => Number(left > right) - Number(left < right),
          ),
          rowCount: row.rowCount,
        })),
      ).toStrictEqual([
        {
          attributes: [
            [collisionLeft, "one"],
            [collisionRight, "two"],
          ],
          rowCount: 2n,
        },
      ]);
      expect(distinct.rows).toStrictEqual([{ desk: "equities", distinctAttributes: 1n }]);
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "hashMapRows",
        queryId: initial.queryId,
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: groupedKey,
            index: 0,
            row: {
              attributes: first,
              rowCount: 3n,
            },
          },
        ],
        totalRows: 1,
      });
      yield* subscription.close();
    }),
  );

  it.effect("keeps subset and superset HashMaps distinct across groups and replacements", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({
        topics: hashMapViewServer.topics,
      });
      yield* engine.publishMany("hashMapRows", [
        hashMapRow("1", subsetAttributes),
        hashMapRow("2", supersetAttributes),
      ]);

      const groupedSnapshot = yield* engine.snapshot("hashMapRows", byAttributes);
      const distinctSnapshot = yield* engine.snapshot("hashMapRows", distinctAttributes);

      expect({
        grouped: normalizedGroupedRows(groupedSnapshot.rows),
        distinct: distinctSnapshot.rows,
      }).toStrictEqual({
        grouped: [
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
            rowCount: 1n,
          },
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
            rowCount: 1n,
          },
        ],
        distinct: [{ desk: "equities", distinctAttributes: 2n }],
      });

      const rawSubscription = yield* engine.subscribe("hashMapRows", {
        select: ["id", "attributes"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const groupedSubscription = yield* engine.subscribe("hashMapRows", byAttributes);
      const distinctSubscription = yield* engine.subscribe("hashMapRows", distinctAttributes);
      const readRaw = yield* makeEventReader(rawSubscription);
      const readGrouped = yield* makeEventReader(groupedSubscription);
      const readDistinct = yield* makeEventReader(distinctSubscription);
      const rawInitial = firstEvent(yield* readRaw(1));
      const groupedInitial = firstEvent(yield* readGrouped(1));
      const distinctInitial = firstEvent(yield* readDistinct(1));
      expectSnapshotEvent(rawInitial);
      expectSnapshotEvent(groupedInitial);
      expectSnapshotEvent(distinctInitial);
      let rawState = stateFromSnapshot(rawInitial);
      let groupedState = stateFromSnapshot(groupedInitial);
      let distinctState = stateFromSnapshot(distinctInitial);

      yield* engine.publish("hashMapRows", hashMapRow("1", equivalentSubsetAttributes));

      const afterEquivalent = yield* engine.snapshot("hashMapRows", {
        select: ["id", "attributes"],
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
        rows: [
          {
            id: "1",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
          },
          {
            id: "2",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
        ],
      });

      yield* engine.publish("hashMapRows", hashMapRow("1", supersetAttributes));
      const rawToSuperset = firstEvent(yield* readRaw(1));
      const groupedToSuperset = firstEvent(yield* readGrouped(1));
      const distinctToSuperset = firstEvent(yield* readDistinct(1));
      expectDeltaEvent(rawToSuperset);
      expectDeltaEvent(groupedToSuperset);
      expectDeltaEvent(distinctToSuperset);
      rawState = applyDelta(rawState, rawToSuperset);
      groupedState = applyDelta(groupedState, groupedToSuperset);
      distinctState = applyDelta(distinctState, distinctToSuperset);
      const rawAfterSuperset = yield* engine.snapshot("hashMapRows", {
        select: ["id", "attributes"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const groupedAfterSuperset = yield* engine.snapshot("hashMapRows", byAttributes);
      const distinctAfterSuperset = yield* engine.snapshot("hashMapRows", distinctAttributes);

      expect(rawToSuperset).toStrictEqual({
        type: "delta",
        topic: "hashMapRows",
        queryId: rawInitial.queryId,
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "1",
            row: { id: "1", attributes: supersetAttributes },
            index: 0,
          },
        ],
        totalRows: 2,
      });
      expect({
        raw: normalizedRawRows(rawState.rows),
        rawFresh: normalizedRawRows(rawAfterSuperset.rows),
        grouped: normalizedGroupedRows(groupedState.rows),
        groupedFresh: normalizedGroupedRows(groupedAfterSuperset.rows),
        distinct: distinctState.rows,
        distinctFresh: distinctAfterSuperset.rows,
      }).toStrictEqual({
        raw: [
          {
            id: "1",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
          {
            id: "2",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
        ],
        rawFresh: [
          {
            id: "1",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
          {
            id: "2",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
        ],
        grouped: [
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
            rowCount: 2n,
          },
        ],
        groupedFresh: [
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
            rowCount: 2n,
          },
        ],
        distinct: [{ desk: "equities", distinctAttributes: 1n }],
        distinctFresh: [{ desk: "equities", distinctAttributes: 1n }],
      });

      yield* engine.publish("hashMapRows", hashMapRow("1", subsetAttributes));
      const rawToSubset = firstEvent(yield* readRaw(1));
      const groupedToSubset = firstEvent(yield* readGrouped(1));
      const distinctToSubset = firstEvent(yield* readDistinct(1));
      expectDeltaEvent(rawToSubset);
      expectDeltaEvent(groupedToSubset);
      expectDeltaEvent(distinctToSubset);
      rawState = applyDelta(rawState, rawToSubset);
      groupedState = applyDelta(groupedState, groupedToSubset);
      distinctState = applyDelta(distinctState, distinctToSubset);
      const rawAfterSubset = yield* engine.snapshot("hashMapRows", {
        select: ["id", "attributes"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const groupedAfterSubset = yield* engine.snapshot("hashMapRows", byAttributes);
      const distinctAfterSubset = yield* engine.snapshot("hashMapRows", distinctAttributes);

      expect(rawToSubset).toStrictEqual({
        type: "delta",
        topic: "hashMapRows",
        queryId: rawInitial.queryId,
        fromVersion: 2,
        toVersion: 3,
        operations: [
          {
            type: "update",
            key: "1",
            row: { id: "1", attributes: subsetAttributes },
            index: 0,
          },
        ],
        totalRows: 2,
      });
      expect({
        raw: normalizedRawRows(rawState.rows),
        rawFresh: normalizedRawRows(rawAfterSubset.rows),
        grouped: normalizedGroupedRows(groupedState.rows),
        groupedFresh: normalizedGroupedRows(groupedAfterSubset.rows),
        distinct: distinctState.rows,
        distinctFresh: distinctAfterSubset.rows,
      }).toStrictEqual({
        raw: [
          {
            id: "1",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
          },
          {
            id: "2",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
        ],
        rawFresh: [
          {
            id: "1",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
          },
          {
            id: "2",
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
          },
        ],
        grouped: [
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
            rowCount: 1n,
          },
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
            rowCount: 1n,
          },
        ],
        groupedFresh: [
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
            ],
            rowCount: 1n,
          },
          {
            attributes: [
              ["desk", "equities"],
              ["region", "emea"],
              ["tier", "one"],
            ],
            rowCount: 1n,
          },
        ],
        distinct: [{ desk: "equities", distinctAttributes: 2n }],
        distinctFresh: [{ desk: "equities", distinctAttributes: 2n }],
      });

      expect(expectDefined(rawState.rows[0]).id).toBe("1");
      expect(expectDefined(groupedState.rows[0]).rowCount).toBe(1n);
      yield* rawSubscription.close();
      yield* groupedSubscription.close();
      yield* distinctSubscription.close();
    }),
  );
});
