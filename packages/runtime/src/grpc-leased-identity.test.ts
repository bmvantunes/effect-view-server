import { describe, expect, it } from "@effect/vitest";
import { viewSchema } from "@effect-view-server/config";
import { Effect, HashMap, HashSet, Result, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  makeGrpcLeasedIdentityContract,
  type GrpcLeasedIdentityContract,
  type GrpcLeasedIdentityError,
} from "./grpc-leased-identity";

const IdentityRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  desk: Schema.String,
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

const leaseFromQuery = (contract: GrpcLeasedIdentityContract, query: unknown) =>
  Result.map(contract.resolveQueryRoute(query), contract.leaseFromRoute);

const missingPresenceKey = JSON.stringify(["missing"]);

const presentPresenceKey = (canonicalKey: string): string =>
  JSON.stringify(["present", canonicalKey]);

const groupedPublicKey = (
  entries: ReadonlyArray<readonly [field: string, presenceKey: string]>,
): string => JSON.stringify(entries);

const withObjectPrototypeValue = <Value, Error, Requirements>(
  field: string,
  value: unknown,
  effect: Effect.Effect<Value, Error, Requirements>,
): Effect.Effect<Value, Error, Requirements> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      Reflect.set(Object.prototype, field, value);
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        Reflect.deleteProperty(Object.prototype, field);
      }),
  );

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
  return yield* Effect.fromResult(leaseFromQuery(contract, { routeBy: { region: "usa" } }));
});

describe("leased gRPC identity contract", () => {
  it.effect("ignores inherited route and grouping query fields", () =>
    withObjectPrototypeValue(
      "routeBy",
      { region: "usa" },
      Effect.gen(function* () {
        const contract = yield* Effect.fromResult(
          makeGrpcLeasedIdentityContract({
            topic: "orders",
            feedName: "orders",
            routeBy: ["region"],
            schema: IdentityRow,
            keyField: "id",
          }),
        );
        expect(failure(contract.resolveQueryRoute({}))?.kind).toBe("Route");
        expect(failure(contract.resolveQueryRoute(null))?.kind).toBe("Route");
        expect(
          failure(
            contract.resolveQueryRoute(
              new Proxy(
                {},
                {
                  getOwnPropertyDescriptor() {
                    throw new Error("query fields cannot be inspected");
                  },
                },
              ),
            ),
          )?.kind,
        ).toBe("Route");

        const lease = yield* Effect.fromResult(
          leaseFromQuery(contract, { routeBy: { region: "usa" } }),
        );
        const row = { id: "order-1", region: "usa", desk: "equities" };
        const internal = yield* Effect.fromResult(lease.internalizeRowKey(row));
        const translated = yield* withObjectPrototypeValue(
          "groupBy",
          ["region"],
          Effect.fromResult(
            lease
              .resultKeys<typeof row>({ select: ["id"] })
              .translateSnapshot([internal.storageKey], [row]),
          ),
        );
        const nullQueryTranslated = yield* Effect.fromResult(
          lease.resultKeys<typeof row>(null).translateSnapshot([internal.storageKey], [row]),
        );

        expect(translated).toStrictEqual(["order-1"]);
        expect(nullQueryTranslated).toStrictEqual(["order-1"]);
      }),
    ),
  );

  it("compiles a total exact-route engine partition predicate", () => {
    const contract = Result.getOrThrow(
      makeGrpcLeasedIdentityContract({
        topic: "orders",
        feedName: "orders",
        routeBy: ["region"],
        schema: IdentityRow,
        keyField: "id",
      }),
    );
    const lease = Result.getOrThrow(leaseFromQuery(contract, { routeBy: { region: "UsÁ" } }));
    const reacquired = Result.getOrThrow(leaseFromQuery(contract, { routeBy: { region: "UsÁ" } }));
    let hostileReads = 0;
    const hostileRow = new Proxy(
      { region: "UsÁ" },
      {
        get() {
          hostileReads += 1;
          throw new Error("route field cannot be read");
        },
      },
    );

    expect({
      exact: lease.enginePartition.matches({ region: "UsÁ" }),
      differentExactValue: lease.enginePartition.matches({ region: "usá" }),
      scalarOutsideSchema: lease.enginePartition.matches({ region: 1 }),
      nonScalar: lease.enginePartition.matches({ region: { value: "UsÁ" } }),
      unreadable: lease.enginePartition.matches(hostileRow),
      hostileReads,
      sameFeed: reacquired.feedKey === lease.feedKey,
      separateExecution: reacquired.enginePartition.key !== lease.enginePartition.key,
    }).toStrictEqual({
      exact: true,
      differentExactValue: false,
      scalarOutsideSchema: false,
      nonScalar: false,
      unreadable: false,
      hostileReads: 1,
      sameFeed: true,
      separateExecution: true,
    });
  });

  it("rejects malformed BigDecimals before deriving a leased feed key", () => {
    const BigDecimalRouteRow = Schema.Struct({
      id: Schema.String,
      amount: Schema.BigDecimal,
    });
    const contract = Result.getOrThrow(
      makeGrpcLeasedIdentityContract({
        topic: "orders",
        feedName: "orders",
        routeBy: ["amount"],
        schema: BigDecimalRouteRow,
        keyField: "id",
      }),
    );
    const infinite = leaseFromQuery(contract, {
      routeBy: { amount: BigDecimal.make(1n, Number.POSITIVE_INFINITY) },
    });
    const notANumber = leaseFromQuery(contract, {
      routeBy: { amount: BigDecimal.make(1n, Number.NaN) },
    });
    const fractional = leaseFromQuery(contract, {
      routeBy: { amount: BigDecimal.make(1n, 1.5) },
    });
    const firstCodecCollision = leaseFromQuery(contract, {
      routeBy: { amount: BigDecimal.make(111n, Number.MIN_SAFE_INTEGER) },
    });
    const secondCodecCollision = leaseFromQuery(contract, {
      routeBy: { amount: BigDecimal.make(111n, Number.MIN_SAFE_INTEGER + 1) },
    });

    expect({
      infinite: failure(infinite)?.kind,
      notANumber: failure(notANumber)?.kind,
      fractional: failure(fractional)?.kind,
      firstCodecCollision: failure(firstCodecCollision)?.kind,
      secondCodecCollision: failure(secondCodecCollision)?.kind,
    }).toStrictEqual({
      infinite: "Route",
      notANumber: "Route",
      fractional: "Route",
      firstCodecCollision: "Route",
      secondCodecCollision: "Route",
    });
    expect(Result.isSuccess(infinite)).toBe(false);
    expect(Result.isSuccess(notANumber)).toBe(false);
    expect(Result.isSuccess(fractional)).toBe(false);
    expect(Result.isSuccess(firstCodecCollision)).toBe(false);
    expect(Result.isSuccess(secondCodecCollision)).toBe(false);
  });

  it("preserves negative-zero BigDecimal scales in exact route identity", () => {
    const BigDecimalRouteRow = Schema.Struct({
      id: Schema.String,
      amount: Schema.BigDecimal,
    });
    const contract = Result.getOrThrow(
      makeGrpcLeasedIdentityContract({
        topic: "orders",
        feedName: "orders",
        routeBy: ["amount"],
        schema: BigDecimalRouteRow,
        keyField: "id",
      }),
    );
    const zeroScale = Result.getOrThrow(
      leaseFromQuery(contract, { routeBy: { amount: BigDecimal.make(1n, 0) } }),
    );
    const negativeZeroScale = Result.getOrThrow(
      leaseFromQuery(contract, { routeBy: { amount: BigDecimal.make(1n, -0) } }),
    );
    const materialized = negativeZeroScale.materializeRoute();
    const materializedAmount = materialized["amount"];
    let hostileInspectionCount = 0;
    const trustedDecodedAmount = new Proxy(BigDecimal.make(1n, -0), {
      getOwnPropertyDescriptor(target, property) {
        hostileInspectionCount += 1;
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
      getPrototypeOf(target) {
        hostileInspectionCount += 1;
        return Reflect.getPrototypeOf(target);
      },
    });
    const unreadableAmount = new Proxy(BigDecimal.make(1n, -0), {
      get(target, property, receiver) {
        if (property === "value") {
          throw new Error("BigDecimal coefficient cannot be read");
        }
        return Reflect.get(target, property, receiver);
      },
    });

    expect(zeroScale.feedKey).not.toBe(negativeZeroScale.feedKey);
    expect(
      BigDecimal.isBigDecimal(materializedAmount) && Object.is(materializedAmount.scale, -0),
    ).toBe(true);
    expect(
      Result.isSuccess(
        negativeZeroScale.validateRowRoute({ id: "negative", amount: BigDecimal.make(1n, -0) }),
      ),
    ).toBe(true);
    expect(
      Result.isFailure(
        negativeZeroScale.validateRowRoute({ id: "positive", amount: BigDecimal.make(1n, 0) }),
      ),
    ).toBe(true);
    expect(
      negativeZeroScale.enginePartition.matches({
        id: "negative",
        amount: BigDecimal.make(1n, -0),
      }),
    ).toBe(true);
    expect(
      negativeZeroScale.enginePartition.matches({ id: "positive", amount: BigDecimal.make(1n, 0) }),
    ).toBe(false);
    expect(
      Result.isSuccess(
        negativeZeroScale.validateRowRoute({ id: "trusted", amount: trustedDecodedAmount }),
      ),
    ).toBe(true);
    expect(
      negativeZeroScale.enginePartition.matches({ id: "trusted", amount: trustedDecodedAmount }),
    ).toBe(true);
    expect(
      failure(negativeZeroScale.validateRowRoute({ id: "unreadable", amount: unreadableAmount }))
        ?.kind,
    ).toBe("RouteMismatch");
    expect(hostileInspectionCount).toBe(0);
  });

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
        leaseFromQuery(contract, {
          routeBy: { region: "UsÁ&a=1/%", desk: "Equíties" },
        }),
      );
      const firstRoute = lease.materializeRoute();
      const secondRoute = lease.materializeRoute();
      const firstRow = { id: 'a","b', region: "UsÁ&a=1/%", desk: "Equíties" };
      const secondRow = { id: 'a],["b', region: "UsÁ&a=1/%", desk: "Equíties" };
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
      let accessorRouteReads = 0;
      const accessorRoute = { id: firstRow.id, desk: firstRow.desk };
      Object.defineProperty(accessorRoute, "region", {
        enumerable: true,
        get() {
          accessorRouteReads += 1;
          return firstRow.region;
        },
      });
      const accessorRouteMismatch = yield* Effect.fromResult(
        lease.validateRowRoute(accessorRoute),
      ).pipe(Effect.flip);
      const inheritedRouteMismatch = yield* Effect.fromResult(
        lease.validateRowRoute(
          Object.assign(Object.create({ region: firstRow.region }), {
            id: firstRow.id,
            desk: firstRow.desk,
          }),
        ),
      ).pipe(Effect.flip);
      const unreadableRouteDescriptor = yield* Effect.fromResult(
        lease.validateRowRoute(
          new Proxy(firstRow, {
            getOwnPropertyDescriptor(target, property) {
              if (property === "region") {
                throw new Error("route field descriptor failed");
              }
              return Reflect.getOwnPropertyDescriptor(target, property);
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
        accessorRouteMismatchKind: accessorRouteMismatch.kind,
        inheritedRouteMismatchKind: inheritedRouteMismatch.kind,
        unreadableRouteDescriptorKind: unreadableRouteDescriptor.kind,
        accessorRouteReads,
        invalidPublicKeyKind: invalidPublicKey.kind,
        hostilePublicKeyKind: hostilePublicKey.kind,
        distinctStorageKeys: firstRowKey.storageKey !== secondRowKey.storageKey,
      }).toStrictEqual({
        feedKey:
          "orders%2Factive/orders%2Factive/leased/region=%5B%22string%22%2C%22Us%C3%81%26a%3D1%2F%25%22%5D&desk=%5B%22string%22%2C%22Equ%C3%ADties%22%5D",
        firstRoute: { region: "UsÁ&a=1/%", desk: "Equíties" },
        secondRoute: { region: "UsÁ&a=1/%", desk: "Equíties" },
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
        accessorRouteMismatchKind: "RouteMismatch",
        inheritedRouteMismatchKind: "RouteMismatch",
        unreadableRouteDescriptorKind: "RouteMismatch",
        accessorRouteReads: 0,
        invalidPublicKeyKind: "RowKey",
        hostilePublicKeyKind: "RowKey",
        distinctStorageKeys: true,
      });
      expect(firstRoute).not.toBe(secondRoute);
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
    const loneSurrogateContract = Result.getOrThrow(
      makeGrpcLeasedIdentityContract({
        topic: "\ud800",
        feedName: "orders",
        routeBy: ["region"],
        schema: IdentityRow,
        keyField: "id",
      }),
    );
    const loneSurrogateLease = Result.getOrThrow(
      leaseFromQuery(loneSurrogateContract, { routeBy: { region: "usa" } }),
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
      missingRoute: failure(contract.resolveQueryRoute({}))?.kind,
      extraRoute: failure(
        contract.resolveQueryRoute({ routeBy: { region: "usa", desk: "equities" } }),
      )?.kind,
      invalidValue: failure(contract.resolveQueryRoute({ routeBy: { region: { value: "usa" } } }))
        ?.kind,
    }).toStrictEqual({
      loneSurrogateFeedKey:
        "json:%22%5Cud800%22/orders/leased/region=%5B%22string%22%2C%22usa%22%5D",
      unreadable: "Configuration",
      empty: "Configuration",
      duplicate: "Configuration",
      missing: "Configuration",
      hostileCodec: "Configuration",
      unreadableFields: "Configuration",
      missingRoute: "Route",
      extraRoute: "Route",
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
        leaseFromQuery(hostileContract, { routeBy: { region: "usa" } }),
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
        leaseFromQuery(unreadableContract, { routeBy: { region: "usa" } }),
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
