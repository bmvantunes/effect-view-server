import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import { BigDecimal, Chunk, Effect, HashMap, HashSet, Option, Schema } from "effect";
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

class Profile extends Schema.Class<Profile>("Profile")({
  code: Schema.String,
  aliases: Schema.mutable(Schema.Array(Schema.String)),
}) {}
viewSchema.admitClass(Profile);

const SemanticRow = Schema.Struct({
  id: Schema.String,
  profile: Profile,
  backupProfile: viewSchema.Option(Profile),
  tags: viewSchema.Chunk(Schema.String),
  labels: viewSchema.HashSet(Schema.String),
  assignments: viewSchema.HashMap(Schema.String, Schema.BigInt),
  price: Schema.BigDecimal,
  note: Schema.optionalKey(Schema.String),
  requiredMaybe: Schema.Union([Schema.String, Schema.Undefined]),
});

type SemanticRow = typeof SemanticRow.Type;

const semanticViewServer = defineViewServerConfig({
  topics: {
    semanticRows: {
      schema: SemanticRow,
      key: "id",
    },
  },
});

const semanticQuery = {
  select: [
    "id",
    "profile",
    "backupProfile",
    "tags",
    "labels",
    "assignments",
    "price",
    "note",
    "requiredMaybe",
  ],
  orderBy: [{ field: "id", direction: "asc" }],
} satisfies {
  readonly select: readonly [
    "id",
    "profile",
    "backupProfile",
    "tags",
    "labels",
    "assignments",
    "price",
    "note",
    "requiredMaybe",
  ];
  readonly orderBy: readonly [{ readonly field: "id"; readonly direction: "asc" }];
};

const semanticOrderQuery = {
  select: ["id", "profile"],
  orderBy: [{ field: "profile", direction: "asc" }],
} satisfies {
  readonly select: readonly ["id", "profile"];
  readonly orderBy: readonly [{ readonly field: "profile"; readonly direction: "asc" }];
};

const semanticRow = (id: string, revision: number): SemanticRow => ({
  id,
  profile: Profile.make({
    code: `profile-${revision}`,
    aliases: [`alias-${revision}`],
  }),
  backupProfile: Option.some(
    Profile.make({
      code: `backup-${revision}`,
      aliases: [`backup-alias-${revision}`],
    }),
  ),
  tags: Chunk.make(`tag-${revision}`, "shared"),
  labels: HashSet.make(`label-${revision}`, "shared"),
  assignments: HashMap.make(["desk", BigInt(revision)]),
  price: BigDecimal.fromStringUnsafe(`${revision}.25`),
  requiredMaybe: undefined,
});

const semanticallyOrderedRow = (id: string, profileCode: string): SemanticRow => ({
  ...semanticRow(id, 1),
  profile: Profile.make({
    code: profileCode,
    aliases: [],
  }),
});

const semanticOrderProjection = (
  rows: ReadonlyArray<{ readonly id: string; readonly profile: Profile }>,
): ReadonlyArray<{ readonly id: string; readonly profileCode: string }> =>
  rows.map((row) => ({
    id: row.id,
    profileCode: row.profile.code,
  }));

const expectSemanticProjection = (
  row: SemanticRow | undefined,
  id: string,
  revision: number,
): SemanticRow => {
  const projected = expectDefined(row);
  expect(projected.profile).toBeInstanceOf(Profile);
  expect(Option.isSome(projected.backupProfile)).toBe(true);
  expect(Option.getOrThrow(projected.backupProfile)).toBeInstanceOf(Profile);
  expect(Chunk.isChunk(projected.tags)).toBe(true);
  expect(HashSet.isHashSet(projected.labels)).toBe(true);
  expect(HashMap.isHashMap(projected.assignments)).toBe(true);
  expect(BigDecimal.isBigDecimal(projected.price)).toBe(true);
  expect({
    id: projected.id,
    profile: {
      code: projected.profile.code,
      aliases: projected.profile.aliases,
    },
    backupProfile: {
      code: Option.getOrThrow(projected.backupProfile).code,
      aliases: Option.getOrThrow(projected.backupProfile).aliases,
    },
    tags: Array.from(projected.tags),
    labels: Array.from(projected.labels).toSorted(),
    assignments: HashMap.toEntries(projected.assignments),
    price: BigDecimal.format(projected.price),
    requiredMaybe: projected.requiredMaybe,
  }).toStrictEqual({
    id,
    profile: {
      code: `profile-${revision}`,
      aliases: [`alias-${revision}`],
    },
    backupProfile: {
      code: `backup-${revision}`,
      aliases: [`backup-alias-${revision}`],
    },
    tags: [`tag-${revision}`, "shared"],
    labels: [`label-${revision}`, "shared"].toSorted(),
    assignments: [["desk", BigInt(revision)]],
    price: `${revision}.25`,
    requiredMaybe: undefined,
  });
  expect(Object.hasOwn(projected, "note")).toBe(false);
  expect(Object.prototype.propertyIsEnumerable.call(projected, "note")).toBe(false);
  expect(Object.hasOwn(projected, "requiredMaybe")).toBe(true);
  expect(Object.prototype.propertyIsEnumerable.call(projected, "requiredMaybe")).toBe(true);
  return projected;
};

const makeSemanticEngine = () =>
  createColumnLiveViewEngine({
    topics: semanticViewServer.topics,
  });

describe("ColumnLiveViewEngine raw projection value semantics", () => {
  it.effect("orders raw snapshots and subscription moves by configured structured semantics", () =>
    Effect.gen(function* () {
      const engine = yield* makeSemanticEngine();
      yield* engine.publishMany("semanticRows", [
        semanticallyOrderedRow("a", "z"),
        semanticallyOrderedRow("b", "a"),
      ]);

      const oneShot = yield* engine.snapshot("semanticRows", semanticOrderQuery);
      expect(semanticOrderProjection(oneShot.rows)).toStrictEqual([
        { id: "b", profileCode: "a" },
        { id: "a", profileCode: "z" },
      ]);

      const subscription = yield* engine.subscribe("semanticRows", semanticOrderQuery);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      let state = stateFromSnapshot(initial);
      expect(semanticOrderProjection(state.rows)).toStrictEqual([
        { id: "b", profileCode: "a" },
        { id: "a", profileCode: "z" },
      ]);

      yield* engine.publish("semanticRows", semanticallyOrderedRow("a", "0"));
      const moved = firstEvent(yield* read(1));
      expectDeltaEvent(moved);
      expect(moved.operations).toStrictEqual([
        {
          type: "move",
          key: "a",
          fromIndex: 1,
          toIndex: 0,
        },
        {
          type: "update",
          key: "a",
          row: {
            id: "a",
            profile: Profile.make({ code: "0", aliases: [] }),
          },
          index: 0,
        },
      ]);
      state = applyDelta(state, moved);
      expect(semanticOrderProjection(state.rows)).toStrictEqual([
        { id: "a", profileCode: "0" },
        { id: "b", profileCode: "a" },
      ]);

      const converged = yield* engine.snapshot("semanticRows", semanticOrderQuery);
      expect(semanticOrderProjection(converged.rows)).toStrictEqual([
        { id: "a", profileCode: "0" },
        { id: "b", profileCode: "a" },
      ]);
      yield* subscription.close();
    }),
  );

  it.effect("materializes schema values and exact optional presence in one-shot snapshots", () =>
    Effect.gen(function* () {
      const engine = yield* makeSemanticEngine();
      const source = semanticRow("row-1", 1);
      yield* engine.publish("semanticRows", source);

      const snapshot = yield* engine.snapshot("semanticRows", semanticQuery);
      const projected = expectSemanticProjection(snapshot.rows[0], "row-1", 1);

      projected.profile.aliases.push("mutated-snapshot");
      Option.getOrThrow(projected.backupProfile).aliases.push("mutated-backup");

      const fresh = yield* engine.snapshot("semanticRows", semanticQuery);
      expectSemanticProjection(fresh.rows[0], "row-1", 1);
      expect(source.profile.aliases).toStrictEqual(["alias-1"]);
      expect(Option.getOrThrow(source.backupProfile).aliases).toStrictEqual(["backup-alias-1"]);
    }),
  );

  it.effect("isolates subscription snapshots plus insert and update delta rows", () =>
    Effect.gen(function* () {
      const engine = yield* makeSemanticEngine();
      yield* engine.publish("semanticRows", semanticRow("row-1", 1));

      const firstSubscription = yield* engine.subscribe("semanticRows", semanticQuery);
      const secondSubscription = yield* engine.subscribe("semanticRows", semanticQuery);
      const readFirst = yield* makeEventReader(firstSubscription);
      const readSecond = yield* makeEventReader(secondSubscription);
      const firstSnapshot = firstEvent(yield* readFirst(1));
      const secondSnapshot = firstEvent(yield* readSecond(1));
      expectSnapshotEvent(firstSnapshot);
      expectSnapshotEvent(secondSnapshot);
      let firstState = stateFromSnapshot(firstSnapshot);
      let secondState = stateFromSnapshot(secondSnapshot);

      const firstInitial = expectSemanticProjection(firstState.rows[0], "row-1", 1);
      firstInitial.profile.aliases.push("mutated-first-subscriber");
      expectSemanticProjection(secondState.rows[0], "row-1", 1);
      expectSemanticProjection(
        (yield* engine.snapshot("semanticRows", semanticQuery)).rows[0],
        "row-1",
        1,
      );

      yield* engine.publish("semanticRows", semanticRow("row-2", 2));
      const firstInsert = firstEvent(yield* readFirst(1));
      expectDeltaEvent(firstInsert);
      expect(firstInsert.operations.length).toBe(1);
      expect(firstInsert.operations[0]?.type).toBe("insert");
      firstState = applyDelta(firstState, firstInsert);
      const firstInserted = expectSemanticProjection(
        firstState.rows.find((row) => row.id === "row-2"),
        "row-2",
        2,
      );
      firstInserted.profile.aliases.push("mutated-insert");

      const secondInsert = firstEvent(yield* readSecond(1));
      expectDeltaEvent(secondInsert);
      expect(secondInsert.operations.length).toBe(1);
      expect(secondInsert.operations[0]?.type).toBe("insert");
      secondState = applyDelta(secondState, secondInsert);
      expectSemanticProjection(
        secondState.rows.find((row) => row.id === "row-2"),
        "row-2",
        2,
      );
      expectSemanticProjection(
        (yield* engine.snapshot("semanticRows", semanticQuery)).rows.find(
          (row) => row.id === "row-2",
        ),
        "row-2",
        2,
      );

      yield* engine.publish("semanticRows", semanticRow("row-1", 3));
      const firstUpdate = firstEvent(yield* readFirst(1));
      expectDeltaEvent(firstUpdate);
      expect(firstUpdate.operations.length).toBe(1);
      expect(firstUpdate.operations[0]?.type).toBe("update");
      firstState = applyDelta(firstState, firstUpdate);
      const firstUpdated = expectSemanticProjection(
        firstState.rows.find((row) => row.id === "row-1"),
        "row-1",
        3,
      );
      firstUpdated.profile.aliases.push("mutated-update");

      const secondUpdate = firstEvent(yield* readSecond(1));
      expectDeltaEvent(secondUpdate);
      expect(secondUpdate.operations.length).toBe(1);
      expect(secondUpdate.operations[0]?.type).toBe("update");
      secondState = applyDelta(secondState, secondUpdate);
      expectSemanticProjection(
        secondState.rows.find((row) => row.id === "row-1"),
        "row-1",
        3,
      );
      expectSemanticProjection(
        (yield* engine.snapshot("semanticRows", semanticQuery)).rows.find(
          (row) => row.id === "row-1",
        ),
        "row-1",
        3,
      );

      yield* firstSubscription.close();
      yield* secondSubscription.close();
    }),
  );

  it.effect("treats separately instantiated schema-equal rows as no-op publishes", () =>
    Effect.gen(function* () {
      const engine = yield* makeSemanticEngine();
      yield* engine.publish("semanticRows", semanticRow("row-1", 1));
      const subscription = yield* engine.subscribe("semanticRows", semanticQuery);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      expect(initial.version).toBe(1);

      yield* engine.publish("semanticRows", semanticRow("row-1", 1));

      const afterEquivalent = yield* engine.snapshot("semanticRows", semanticQuery);
      const equivalentHealth = yield* engine.health();
      expect(afterEquivalent.version).toBe(1);
      expect(equivalentHealth.version).toBe(1);
      expect(equivalentHealth.queuedEvents).toBe(0);

      yield* engine.publish("semanticRows", semanticRow("row-1", 2));
      const changed = firstEvent(yield* read(1));
      expectDeltaEvent(changed);
      expect(changed.fromVersion).toBe(1);
      expect(changed.toVersion).toBe(2);
      expect(changed.operations.length).toBe(1);
      expect(changed.operations[0]?.type).toBe("update");
      expectSemanticProjection(
        (yield* engine.snapshot("semanticRows", semanticQuery)).rows[0],
        "row-1",
        2,
      );

      yield* subscription.close();
    }),
  );
});
