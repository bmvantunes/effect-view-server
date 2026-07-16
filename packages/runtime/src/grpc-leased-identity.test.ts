import { describe, expect, it } from "@effect/vitest";
import { viewSchema } from "@effect-view-server/config";
import { Effect, HashMap, HashSet, Result, Schema } from "effect";
import {
  makeGrpcLeasedIdentityContract,
  type GrpcLeasedIdentityError,
} from "./grpc-leased-identity";

const IdentityRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  desk: Schema.Struct({ name: Schema.String }),
});

const GroupedIdentityRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  text: Schema.String,
  maybe: Schema.optionalKey(Schema.Union([Schema.String, Schema.Undefined])),
  maybeArray: Schema.optionalKey(Schema.Array(Schema.String)),
  opaque: Schema.Unknown,
  hashMap: viewSchema.HashMap(Schema.String, Schema.String),
  hashSet: viewSchema.HashSet(Schema.String),
});

const failure = <Success>(
  result: Result.Result<Success, GrpcLeasedIdentityError>,
): GrpcLeasedIdentityError | undefined =>
  Result.match(result, {
    onFailure: (error) => error,
    onSuccess: () => undefined,
  });

const missingPresenceKey = JSON.stringify(["missing"]);

const presentPresenceKey = (canonicalKey: string): string =>
  JSON.stringify(["present", canonicalKey]);

const groupedPublicKey = (
  entries: ReadonlyArray<readonly [field: string, presenceKey: string]>,
): string => JSON.stringify(entries);

const makeGroupedLease = Effect.gen(function* () {
  const contract = yield* Effect.fromResult(
    makeGrpcLeasedIdentityContract({
      topic: "orders",
      feedName: "orders",
      routeBy: ["region"],
      schema: GroupedIdentityRow,
      keyField: "id",
    }),
  );
  return yield* Effect.fromResult(contract.leaseFromQuery({ where: { region: { eq: "usa" } } }));
});

describe("leased gRPC identity contract", () => {
  it.effect("round-trips one canonical route and collision-resistant raw Row Keys", () =>
    Effect.gen(function* () {
      const contract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders/active",
          feedName: "orders/active",
          routeBy: ["region", "desk"],
          schema: IdentityRow,
          keyField: "id",
        }),
      );
      const lease = yield* Effect.fromResult(
        contract.leaseFromQuery({
          where: {
            region: { eq: "us&a=1/%" },
            desk: { eq: { name: "equities" } },
          },
        }),
      );
      const firstRoute = lease.materializeRoute();
      const secondRoute = lease.materializeRoute();
      const firstRow = { id: 'a","b', region: "us&a=1/%", desk: { name: "equities" } };
      const secondRow = { id: 'a],["b', region: "us&a=1/%", desk: { name: "equities" } };
      const firstRowKey = yield* Effect.fromResult(lease.internalizeRowKey(firstRow));
      const secondRowKey = yield* Effect.fromResult(lease.internalizeRowKey(secondRow));
      const rawKeys = lease.resultKeys<typeof firstRow>({ select: ["id"] });
      const malformedInternalKeys = yield* Effect.forEach(
        [
          "not-an-internal-row-key",
          "null",
          "{}",
          "[]",
          JSON.stringify(["other", lease.feedKey, "key"]),
          JSON.stringify(["leased-row", "other-feed", "key"]),
          JSON.stringify(["leased-row", lease.feedKey, 1]),
        ],
        (key) => Effect.fromResult(rawKeys.translateSnapshot([key], [firstRow])).pipe(Effect.flip),
      );
      const invalidRawDelta = yield* Effect.fromResult(
        rawKeys.translateDelta([{ type: "remove", key: "not-an-internal-row-key" }]),
      ).pipe(Effect.flip);
      const invalidRawSnapshot = yield* Effect.fromResult(
        rawKeys.translateSnapshot(["not-an-internal-row-key"], [firstRow]),
      ).pipe(Effect.flip);
      const routeMismatch = yield* Effect.fromResult(
        lease.validateRowRoute({ ...firstRow, region: "europe" }),
      ).pipe(Effect.flip);
      const invalidRowRoute = yield* Effect.fromResult(
        lease.validateRowRoute({ ...firstRow, region: 1 }),
      ).pipe(Effect.flip);
      let hostileRouteReads = 0;
      const hostileRouteMismatch = yield* Effect.fromResult(
        lease.validateRowRoute(
          new Proxy(firstRow, {
            get(target, property, receiver) {
              if (property === "region") {
                hostileRouteReads += 1;
                if (hostileRouteReads > 1) {
                  throw new Error("route field was read twice");
                }
                return "europe";
              }
              return Reflect.get(target, property, receiver);
            },
          }),
        ),
      ).pipe(Effect.flip);
      const hostileRouteRead = yield* Effect.fromResult(
        lease.validateRowRoute(
          new Proxy(firstRow, {
            get(target, property, receiver) {
              if (property === "region") {
                throw new Error("route field reflection failed");
              }
              return Reflect.get(target, property, receiver);
            },
          }),
        ),
      ).pipe(Effect.flip);
      const invalidPublicKey = yield* Effect.fromResult(
        lease.internalizeRowKey({ ...firstRow, id: 1 }),
      ).pipe(Effect.flip);
      const hostilePublicKey = yield* Effect.fromResult(
        lease.internalizeRowKey(
          new Proxy(firstRow, {
            get() {
              throw new Error("row key reflection failed");
            },
          }),
        ),
      ).pipe(Effect.flip);

      expect({
        feedKey: lease.feedKey,
        firstRoute,
        secondRoute,
        validatedRow: yield* Effect.fromResult(lease.validateRowRoute(firstRow)),
        snapshotKeys: yield* Effect.fromResult(
          rawKeys.translateSnapshot(
            [firstRowKey.storageKey, secondRowKey.storageKey],
            [firstRow, secondRow],
          ),
        ),
        delta: yield* Effect.fromResult(
          rawKeys.translateDelta([
            { type: "insert", key: firstRowKey.storageKey, row: firstRow, index: 0 },
            { type: "move", key: secondRowKey.storageKey, fromIndex: 1, toIndex: 0 },
            { type: "remove", key: firstRowKey.storageKey },
          ]),
        ),
        malformedInternalKeyKinds: malformedInternalKeys.map((error) => error.kind),
        invalidRawDeltaKind: invalidRawDelta.kind,
        invalidRawSnapshotKind: invalidRawSnapshot.kind,
        routeMismatchKind: routeMismatch.kind,
        invalidRowRouteKind: invalidRowRoute.kind,
        hostileRouteMismatchKind: hostileRouteMismatch.kind,
        hostileRouteReadKind: hostileRouteRead.kind,
        hostileRouteReads,
        invalidPublicKeyKind: invalidPublicKey.kind,
        hostilePublicKeyKind: hostilePublicKey.kind,
        distinctStorageKeys: firstRowKey.storageKey !== secondRowKey.storageKey,
      }).toStrictEqual({
        feedKey:
          "orders%2Factive/orders%2Factive/leased/region=%22us%26a%3D1%2F%25%22&desk=%7B%22name%22%3A%22equities%22%7D",
        firstRoute: { region: "us&a=1/%", desk: { name: "equities" } },
        secondRoute: { region: "us&a=1/%", desk: { name: "equities" } },
        validatedRow: firstRow,
        snapshotKeys: ['a","b', 'a],["b'],
        delta: [
          { type: "insert", key: 'a","b', row: firstRow, index: 0 },
          { type: "move", key: 'a],["b', fromIndex: 1, toIndex: 0 },
          { type: "remove", key: 'a","b' },
        ],
        malformedInternalKeyKinds: [
          "RowKey",
          "RowKey",
          "RowKey",
          "RowKey",
          "RowKey",
          "RowKey",
          "RowKey",
        ],
        invalidRawDeltaKind: "RowKey",
        invalidRawSnapshotKind: "RowKey",
        routeMismatchKind: "RouteMismatch",
        invalidRowRouteKind: "RouteMismatch",
        hostileRouteMismatchKind: "RouteMismatch",
        hostileRouteReadKind: "RouteMismatch",
        hostileRouteReads: 1,
        invalidPublicKeyKind: "RowKey",
        hostilePublicKeyKind: "RowKey",
        distinctStorageKeys: true,
      });
      expect(firstRoute).not.toBe(secondRoute);
      expect(firstRoute["desk"]).not.toBe(secondRoute["desk"]);
      rawKeys.clear();
    }),
  );

  it("returns typed configuration and route identity failures", () => {
    const hostileRouteBy = new Proxy(["region"], {
      get(target, property, receiver) {
        if (property === Symbol.iterator) {
          throw new Error("route metadata failed");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const hostileRegion = new Proxy(Schema.String, {
      get(target, property, receiver) {
        if (property === "ast") {
          throw new Error("route codec failed");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const HostileIdentityRow = new Proxy(IdentityRow, {
      get(target, property, receiver) {
        if (property === "fields") {
          return { ...IdentityRow.fields, region: hostileRegion };
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const UnreadableFieldsIdentityRow = new Proxy(IdentityRow, {
      get(target, property, receiver) {
        if (property === "fields") {
          throw new Error("schema fields failed");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const contract = Result.getOrThrow(
      makeGrpcLeasedIdentityContract({
        topic: "orders",
        feedName: "orders",
        routeBy: ["region"],
        schema: IdentityRow,
        keyField: "id",
      }),
    );
    const loneSurrogateLease = Result.getOrThrow(
      Result.getOrThrow(
        makeGrpcLeasedIdentityContract({
          topic: "\ud800",
          feedName: "orders",
          routeBy: ["region"],
          schema: IdentityRow,
          keyField: "id",
        }),
      ).leaseFromQuery({ where: { region: { eq: "usa" } } }),
    );

    expect({
      loneSurrogateFeedKey: loneSurrogateLease.feedKey,
      unreadable: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: hostileRouteBy,
          schema: IdentityRow,
          keyField: "id",
        }),
      )?.kind,
      empty: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: [],
          schema: IdentityRow,
          keyField: "id",
        }),
      )?.kind,
      duplicate: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region", "region"],
          schema: IdentityRow,
          keyField: "id",
        }),
      )?.kind,
      missing: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["missing"],
          schema: IdentityRow,
          keyField: "id",
        }),
      )?.kind,
      hostileCodec: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: HostileIdentityRow,
          keyField: "id",
        }),
      )?.kind,
      unreadableFields: failure(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: UnreadableFieldsIdentityRow,
          keyField: "id",
        }),
      )?.kind,
      missingWhere: failure(contract.leaseFromQuery({}))?.kind,
      nonExact: failure(contract.leaseFromQuery({ where: { region: { startsWith: "u" } } }))?.kind,
      invalidValue: failure(contract.leaseFromQuery({ where: { region: { eq: 1 } } }))?.kind,
    }).toStrictEqual({
      loneSurrogateFeedKey: "json:%22%5Cud800%22/orders/leased/region=%22usa%22",
      unreadable: "Configuration",
      empty: "Configuration",
      duplicate: "Configuration",
      missing: "Configuration",
      hostileCodec: "Configuration",
      unreadableFields: "Configuration",
      missingWhere: "Route",
      nonExact: "Route",
      invalidValue: "Route",
    });
  });

  it.effect("derives deterministic grouped keys from the canonical field codecs", () =>
    Effect.gen(function* () {
      const lease = yield* makeGroupedLease;
      const missingAndUndefined = lease.resultKeys({ groupBy: ["text", "maybe"] });
      const missing = yield* Effect.fromResult(
        missingAndUndefined.translateSnapshot(["missing"], [{ text: "route" }]),
      );
      const presentUndefined = yield* Effect.fromResult(
        missingAndUndefined.translateSnapshot(["undefined"], [{ text: "route", maybe: undefined }]),
      );
      const presenceCollision = lease.resultKeys({ groupBy: ["maybeArray"] });
      const presenceKeys = yield* Effect.fromResult(
        presenceCollision.translateSnapshot(
          ["missing", "present"],
          [{}, { maybeArray: ["missing"] }],
        ),
      );
      const collisionLeft = "8ocpIaaa";
      const collisionRight = "GpcpIaaa";
      const semanticCollision = lease.resultKeys({ groupBy: ["hashMap", "hashSet"] });
      const semanticKeys = yield* Effect.fromResult(
        semanticCollision.translateSnapshot(
          ["left", "right"],
          [
            {
              hashMap: HashMap.make([collisionLeft, "left"], [collisionRight, "right"]),
              hashSet: HashSet.make(collisionLeft, collisionRight),
            },
            {
              hashMap: HashMap.make([collisionRight, "right"], [collisionLeft, "left"]),
              hashSet: HashSet.make(collisionRight, collisionLeft),
            },
          ],
        ),
      );

      expect({ missing, presentUndefined, presenceKeys, semanticKeys }).toStrictEqual({
        missing: [
          groupedPublicKey([
            ["text", presentPresenceKey('"route"')],
            ["maybe", missingPresenceKey],
          ]),
        ],
        presentUndefined: [
          groupedPublicKey([
            ["text", presentPresenceKey('"route"')],
            ["maybe", presentPresenceKey("null")],
          ]),
        ],
        presenceKeys: [
          groupedPublicKey([["maybeArray", missingPresenceKey]]),
          groupedPublicKey([["maybeArray", presentPresenceKey('["missing"]')]]),
        ],
        semanticKeys: [
          groupedPublicKey([
            [
              "hashMap",
              presentPresenceKey(`[["${collisionLeft}","left"],["${collisionRight}","right"]]`),
            ],
            ["hashSet", presentPresenceKey(`["${collisionLeft}","${collisionRight}"]`)],
          ]),
          groupedPublicKey([
            [
              "hashMap",
              presentPresenceKey(`[["${collisionLeft}","left"],["${collisionRight}","right"]]`),
            ],
            ["hashSet", presentPresenceKey(`["${collisionLeft}","${collisionRight}"]`)],
          ]),
        ],
      });
    }),
  );

  it.effect("translates grouped snapshots and deltas transactionally", () =>
    Effect.gen(function* () {
      const lease = yield* makeGroupedLease;
      let retainedEntryCount = -1;
      const translations = lease.resultKeys<{ readonly text: unknown }>(
        { groupBy: ["text"] },
        (retention) => {
          retainedEntryCount = retention.retainedEntryCount();
        },
      );
      const initial = yield* Effect.fromResult(
        translations.translateSnapshot(
          ["internal-a", "internal-b"],
          [{ text: "a" }, { text: "b" }],
        ),
      );
      const missingRow = yield* Effect.fromResult(
        translations.translateSnapshot(["missing-row"], []),
      ).pipe(Effect.flip);
      const invalidSnapshot = yield* Effect.fromResult(
        translations.translateSnapshot(["invalid"], [{ text: 1 }]),
      ).pipe(Effect.flip);
      const move = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "move", key: "internal-b", fromIndex: 1, toIndex: 0 },
        ]),
      );
      const committed = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "update", key: "internal-a", row: { text: "a2" }, index: 0 },
          { type: "move", key: "internal-a", fromIndex: 0, toIndex: 1 },
          { type: "remove", key: "internal-b" },
        ]),
      );
      const rolledBack = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "remove", key: "internal-a" },
          { type: "insert", key: "invalid", row: { text: 1 }, index: 0 },
        ]),
      ).pipe(Effect.flip);
      const retainedAfterRollback = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "move", key: "internal-a", fromIndex: 0, toIndex: 1 },
        ]),
      );
      const replacement = yield* Effect.fromResult(
        translations.translateSnapshot(["replacement"], [{ text: "replacement" }]),
      );
      const replacedKey = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "move", key: "internal-a", fromIndex: 0, toIndex: 1 },
        ]),
      ).pipe(Effect.flip);
      translations.clear();
      const clearedMove = yield* Effect.fromResult(
        translations.translateDelta([
          { type: "move", key: "replacement", fromIndex: 0, toIndex: 1 },
        ]),
      ).pipe(Effect.flip);

      expect({
        initial,
        retainedEntryCount,
        missingRow: missingRow.kind,
        invalidSnapshot: invalidSnapshot.kind,
        move,
        committed,
        rolledBack: rolledBack.kind,
        retainedAfterRollback,
        replacement,
        replacedKey: replacedKey.kind,
        clearedMove: clearedMove.kind,
      }).toStrictEqual({
        initial: [
          groupedPublicKey([["text", presentPresenceKey('"a"')]]),
          groupedPublicKey([["text", presentPresenceKey('"b"')]]),
        ],
        retainedEntryCount: 0,
        missingRow: "ResultKey",
        invalidSnapshot: "ResultKey",
        move: [
          {
            type: "move",
            key: groupedPublicKey([["text", presentPresenceKey('"b"')]]),
            fromIndex: 1,
            toIndex: 0,
          },
        ],
        committed: [
          {
            type: "update",
            key: groupedPublicKey([["text", presentPresenceKey('"a2"')]]),
            row: { text: "a2" },
            index: 0,
          },
          {
            type: "move",
            key: groupedPublicKey([["text", presentPresenceKey('"a2"')]]),
            fromIndex: 0,
            toIndex: 1,
          },
          {
            type: "remove",
            key: groupedPublicKey([["text", presentPresenceKey('"b"')]]),
          },
        ],
        rolledBack: "ResultKey",
        retainedAfterRollback: [
          {
            type: "move",
            key: groupedPublicKey([["text", presentPresenceKey('"a2"')]]),
            fromIndex: 0,
            toIndex: 1,
          },
        ],
        replacement: [groupedPublicKey([["text", presentPresenceKey('"replacement"')]])],
        replacedKey: "ResultKey",
        clearedMove: "ResultKey",
      });
    }),
  );

  it.effect("returns one typed grouped-key error for invalid grouping identities", () =>
    Effect.gen(function* () {
      const lease = yield* makeGroupedLease;
      const missingField = lease.resultKeys({ groupBy: ["missing"] });
      const nonStringField = lease.resultKeys({ groupBy: [1] });
      const invalidValue = lease.resultKeys({ groupBy: ["opaque"] });
      const hostileField = new Proxy(Schema.String, {
        get(target, property, receiver) {
          if (property === "ast") {
            throw new Error("grouped codec failed");
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const HostileGroupedIdentityRow = new Proxy(GroupedIdentityRow, {
        get(target, property, receiver) {
          if (property === "fields") {
            return { ...GroupedIdentityRow.fields, hostile: hostileField };
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const unreadableGroupedFields = new Proxy(GroupedIdentityRow.fields, {
        get(target, property, receiver) {
          if (property === "unreadable") {
            throw new Error("grouped schema fields failed");
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const UnreadableGroupedIdentityRow = new Proxy(GroupedIdentityRow, {
        get(target, property, receiver) {
          if (property === "fields") {
            return unreadableGroupedFields;
          }
          return Reflect.get(target, property, receiver);
        },
      });
      const hostileContract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: HostileGroupedIdentityRow,
          keyField: "id",
        }),
      );
      const hostileLease = yield* Effect.fromResult(
        hostileContract.leaseFromQuery({ where: { region: { eq: "usa" } } }),
      );
      const hostileGrouping = hostileLease.resultKeys({ groupBy: ["hostile"] });
      const unreadableContract = yield* Effect.fromResult(
        makeGrpcLeasedIdentityContract({
          topic: "orders",
          feedName: "orders",
          routeBy: ["region"],
          schema: UnreadableGroupedIdentityRow,
          keyField: "id",
        }),
      );
      const unreadableLease = yield* Effect.fromResult(
        unreadableContract.leaseFromQuery({ where: { region: { eq: "usa" } } }),
      );
      const unreadableGrouping = unreadableLease.resultKeys({ groupBy: ["unreadable"] });
      const missingFieldError = yield* Effect.fromResult(
        missingField.translateSnapshot(["key"], [{}]),
      ).pipe(Effect.flip);
      const nonStringFieldError = yield* Effect.fromResult(nonStringField.translateDelta([])).pipe(
        Effect.flip,
      );
      const invalidValueError = yield* Effect.fromResult(
        invalidValue.translateSnapshot(["key"], [{ opaque: new Map([["desk", "equities"]]) }]),
      ).pipe(Effect.flip);
      const hostileGroupingError = yield* Effect.fromResult(
        hostileGrouping.translateSnapshot(["key"], [{}]),
      ).pipe(Effect.flip);
      const unreadableGroupingError = yield* Effect.fromResult(
        unreadableGrouping.translateSnapshot(["key"], [{}]),
      ).pipe(Effect.flip);

      expect({
        missingField: missingFieldError.kind,
        nonStringField: nonStringFieldError.kind,
        invalidValue: invalidValueError.kind,
        hostileGrouping: hostileGroupingError.kind,
        unreadableGrouping: unreadableGroupingError.kind,
      }).toStrictEqual({
        missingField: "ResultKey",
        nonStringField: "ResultKey",
        invalidValue: "ResultKey",
        hostileGrouping: "ResultKey",
        unreadableGrouping: "ResultKey",
      });
      missingField.clear();
      nonStringField.clear();
    }),
  );
});
