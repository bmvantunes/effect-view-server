import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type GroupedQuery, viewSchema } from "@effect-view-server/config";
import { BigDecimal, Effect, Schema } from "effect";
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

class Venue extends Schema.Class<Venue>("Venue")({
  code: Schema.String,
  aliases: Schema.mutable(Schema.Array(Schema.String)),
}) {}
viewSchema.admitClass(Venue);

class Sentinel extends Schema.Class<Sentinel>("Sentinel")({
  code: Schema.String,
  labels: Schema.mutable(Schema.Array(Schema.String)),
}) {}
viewSchema.admitClass(Sentinel);

const GroupedSemanticRow = Schema.Struct({
  id: Schema.String,
  venue: Venue,
  sentinel: Sentinel,
  amount: Schema.BigDecimal,
  desk: Schema.optionalKey(Schema.String),
});

type GroupedSemanticRow = typeof GroupedSemanticRow.Type;

type GroupedSemanticResult = {
  readonly venue: Venue;
  readonly rowCount: bigint;
  readonly distinctSentinels: bigint;
  readonly totalAmount: BigDecimal.BigDecimal;
  readonly minSentinel: Sentinel;
  readonly maxSentinel: Sentinel;
};

const groupedSemanticViewServer = defineViewServerConfig({
  topics: {
    groupedSemanticRows: {
      schema: GroupedSemanticRow,
      key: "id",
    },
  },
});

const groupedSemanticQuery = {
  groupBy: ["venue"],
  aggregates: {
    rowCount: { aggFunc: "count" },
    distinctSentinels: { aggFunc: "countDistinct", field: "sentinel" },
    totalAmount: { aggFunc: "sum", field: "amount" },
    minSentinel: { aggFunc: "min", field: "sentinel" },
    maxSentinel: { aggFunc: "max", field: "sentinel" },
  },
  orderBy: [{ field: "venue", direction: "asc" }],
} satisfies GroupedQuery<GroupedSemanticRow>;

const groupedOptionalQuery = {
  groupBy: ["desk"],
  aggregates: {
    rowCount: { aggFunc: "count" },
  },
} satisfies GroupedQuery<GroupedSemanticRow>;

const venue = (code: string, alias: string): Venue =>
  Venue.make({
    code,
    aliases: [alias],
  });

const sentinel = (code: string): Sentinel =>
  Sentinel.make({
    code,
    labels: [`label-${code}`],
  });

const groupedRow = (
  id: string,
  venueCode: string,
  venueAlias: string,
  sentinelCode: string,
  amount: string,
): GroupedSemanticRow => ({
  id,
  venue: venue(venueCode, venueAlias),
  sentinel: sentinel(sentinelCode),
  amount: BigDecimal.fromStringUnsafe(amount),
});

const expectGroupedSemanticResult = (
  row: GroupedSemanticResult | undefined,
  expected: {
    readonly venueCode: string;
    readonly venueAlias: string;
    readonly rowCount: bigint;
    readonly distinctSentinels: bigint;
    readonly totalAmount: string;
    readonly minSentinel: string;
    readonly maxSentinel: string;
  },
): GroupedSemanticResult => {
  const grouped = expectDefined(row);
  expect(grouped.venue).toBeInstanceOf(Venue);
  expect(grouped.minSentinel).toBeInstanceOf(Sentinel);
  expect(grouped.maxSentinel).toBeInstanceOf(Sentinel);
  expect(BigDecimal.isBigDecimal(grouped.totalAmount)).toBe(true);
  expect({
    venueCode: grouped.venue.code,
    venueAliases: grouped.venue.aliases,
    rowCount: grouped.rowCount,
    distinctSentinels: grouped.distinctSentinels,
    totalAmount: BigDecimal.format(grouped.totalAmount),
    minSentinel: {
      code: grouped.minSentinel.code,
      labels: grouped.minSentinel.labels,
    },
    maxSentinel: {
      code: grouped.maxSentinel.code,
      labels: grouped.maxSentinel.labels,
    },
  }).toStrictEqual({
    venueCode: expected.venueCode,
    venueAliases: [expected.venueAlias],
    rowCount: expected.rowCount,
    distinctSentinels: expected.distinctSentinels,
    totalAmount: expected.totalAmount,
    minSentinel: {
      code: expected.minSentinel,
      labels: [`label-${expected.minSentinel}`],
    },
    maxSentinel: {
      code: expected.maxSentinel,
      labels: [`label-${expected.maxSentinel}`],
    },
  });
  return grouped;
};

const expectGroupedSemanticRows = (
  rows: ReadonlyArray<GroupedSemanticResult>,
  xTotalAmount: string,
): void => {
  expect(rows.length).toBe(2);
  expectGroupedSemanticResult(rows[0], {
    venueCode: "XNAS",
    venueAlias: "primary",
    rowCount: 2n,
    distinctSentinels: 2n,
    totalAmount: xTotalAmount,
    minSentinel: "a",
    maxSentinel: "z",
  });
  expectGroupedSemanticResult(rows[1], {
    venueCode: "XNYS",
    venueAlias: "secondary",
    rowCount: 1n,
    distinctSentinels: 1n,
    totalAmount: "3",
    minSentinel: "m",
    maxSentinel: "m",
  });
};

const makeGroupedSemanticEngine = () =>
  createColumnLiveViewEngine({
    topics: groupedSemanticViewServer.topics,
  });

const groupedSemanticFixture = (): ReadonlyArray<GroupedSemanticRow> => [
  groupedRow("row-1", "XNAS", "primary", "z", "1"),
  groupedRow("row-2", "XNAS", "primary", "a", "2"),
  groupedRow("row-3", "XNYS", "secondary", "m", "3"),
];

describe("ColumnLiveViewEngine grouped projection value semantics", () => {
  it.effect("keeps a missing optional group field omitted from the result row", () =>
    Effect.gen(function* () {
      const engine = yield* makeGroupedSemanticEngine();
      yield* engine.publishMany("groupedSemanticRows", groupedSemanticFixture());

      const snapshot = yield* engine.snapshot("groupedSemanticRows", groupedOptionalQuery);

      expect(snapshot.rows).toStrictEqual([{ rowCount: 3n }]);
      expect(Object.hasOwn(expectDefined(snapshot.rows[0]), "desk")).toBe(false);

      const subscription = yield* engine.subscribe("groupedSemanticRows", groupedOptionalQuery);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      expect(initial.rows).toStrictEqual([{ rowCount: 3n }]);
      expect(Object.hasOwn(expectDefined(initial.rows[0]), "desk")).toBe(false);
      yield* subscription.close();
    }),
  );

  it.effect("groups and aggregates schema class values while isolating one-shot results", () =>
    Effect.gen(function* () {
      const engine = yield* makeGroupedSemanticEngine();
      yield* engine.publishMany("groupedSemanticRows", groupedSemanticFixture());

      const snapshot = yield* engine.snapshot("groupedSemanticRows", groupedSemanticQuery);
      expect(snapshot.totalRows).toBe(2);
      expectGroupedSemanticRows(snapshot.rows, "3");

      const xGroup = expectDefined(snapshot.rows[0]);
      xGroup.venue.aliases.push("mutated-snapshot");
      xGroup.minSentinel.labels.push("mutated-min");

      const fresh = yield* engine.snapshot("groupedSemanticRows", groupedSemanticQuery);
      expectGroupedSemanticRows(fresh.rows, "3");
    }),
  );

  it.effect("isolates grouped subscription snapshots, cursors, and update delta rows", () =>
    Effect.gen(function* () {
      const engine = yield* makeGroupedSemanticEngine();
      yield* engine.publishMany("groupedSemanticRows", groupedSemanticFixture());

      const firstSubscription = yield* engine.subscribe(
        "groupedSemanticRows",
        groupedSemanticQuery,
      );
      const secondSubscription = yield* engine.subscribe(
        "groupedSemanticRows",
        groupedSemanticQuery,
      );
      const readFirst = yield* makeEventReader(firstSubscription);
      const readSecond = yield* makeEventReader(secondSubscription);
      const firstSnapshot = firstEvent(yield* readFirst(1));
      const secondSnapshot = firstEvent(yield* readSecond(1));
      expectSnapshotEvent(firstSnapshot);
      expectSnapshotEvent(secondSnapshot);
      let firstState = stateFromSnapshot(firstSnapshot);
      let secondState = stateFromSnapshot(secondSnapshot);
      expectGroupedSemanticRows(firstState.rows, "3");

      const firstXGroup = expectDefined(firstState.rows[0]);
      firstXGroup.venue.aliases.push("mutated-first-subscriber");
      firstXGroup.maxSentinel.labels.push("mutated-max");
      expectGroupedSemanticRows(secondState.rows, "3");

      const xGroupKey = expectDefined(firstSnapshot.keys[0]);
      yield* engine.patch("groupedSemanticRows", "row-1", {
        amount: BigDecimal.fromStringUnsafe("4"),
      });

      const firstDelta = firstEvent(yield* readFirst(1));
      expectDeltaEvent(firstDelta);
      expect(firstDelta.operations).toStrictEqual([
        {
          type: "update",
          key: xGroupKey,
          row: {
            venue: venue("XNAS", "primary"),
            rowCount: 2n,
            distinctSentinels: 2n,
            totalAmount: BigDecimal.fromStringUnsafe("6"),
            minSentinel: sentinel("a"),
            maxSentinel: sentinel("z"),
          },
          index: 0,
        },
      ]);
      firstState = applyDelta(firstState, firstDelta);
      expectGroupedSemanticRows(firstState.rows, "6");
      expectDefined(firstState.rows[0]).venue.aliases.push("mutated-delta");

      const secondDelta = firstEvent(yield* readSecond(1));
      expectDeltaEvent(secondDelta);
      expect(secondDelta.operations.length).toBe(1);
      expect(secondDelta.operations[0]?.type).toBe("update");
      secondState = applyDelta(secondState, secondDelta);
      expectGroupedSemanticRows(secondState.rows, "6");
      expectGroupedSemanticRows(
        (yield* engine.snapshot("groupedSemanticRows", groupedSemanticQuery)).rows,
        "6",
      );

      yield* firstSubscription.close();
      yield* secondSubscription.close();
    }),
  );
});
