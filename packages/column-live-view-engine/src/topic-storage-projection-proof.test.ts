import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { InvalidRowError } from "./index";
import { evaluateRawQuery } from "./active-query";
import { prepareGroupedQuery } from "./grouped-query-compiler";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import { makeQueryResultSemantics } from "./query-result-semantics";
import { TopicRowStorage } from "./topic-row-storage";
import { bindTopicStorageProjection } from "./topic-storage-projection";
import { order, Order } from "../test-harness/public-engine";

describe("Topic Storage projection proof", () => {
  it.effect("projects raw snapshots from carried storage slots", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      for (const row of [
        order("stored-slot-0", "open", 10, 1),
        order("stored-slot-1", "open", 15, 2),
        order("projected-from-slot-2", "open", 20, 3),
      ]) {
        storage.setPrepared(yield* storage.prepareRow(row, invalidRow));
      }
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      let slotForKeyCalls = 0;
      const evaluation = evaluateRawQuery(
        {
          ...storage.readModel,
          scanRawWindow: () => ({
            keys: ["projected-from-slot-2"],
            window: [
              {
                key: "projected-from-slot-2",
                row: order("row-object-should-not-project", "open", 1, 1),
                slot: 2,
              },
            ],
            totalRows: 1,
          }),
          slotForKey: () => {
            slotForKeyCalls += 1;
            return 99;
          },
          version: () => 1,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: ["projected-from-slot-2"],
        rows: [
          {
            id: "projected-from-slot-2",
            price: 20,
          },
        ],
        totalRows: 1,
        version: 1,
        window: [
          {
            key: "projected-from-slot-2",
            row: {
              id: "projected-from-slot-2",
              price: 20,
            },
          },
        ],
      });
      expect(slotForKeyCalls).toBe(0);
    }),
  );

  it.effect("falls back to key lookup when a carried storage slot is stale", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      for (const row of [
        order("stale-slot", "open", 10, 1),
        order("stored-slot-1", "open", 15, 2),
        order("stored-slot-2", "open", 20, 3),
        order("moved-row", "open", 30, 4),
      ]) {
        storage.setPrepared(yield* storage.prepareRow(row, invalidRow));
      }
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      let slotForKeyCalls = 0;
      const evaluation = evaluateRawQuery(
        {
          ...storage.readModel,
          scanRawWindow: () => ({
            keys: ["moved-row"],
            window: [
              {
                key: "moved-row",
                row: order("row-object-should-not-project", "open", 1, 1),
                slot: 0,
              },
            ],
            totalRows: 1,
          }),
          slotForKey: (key) => {
            slotForKeyCalls += 1;
            return storage.readModel.slotForKey?.(key);
          },
          version: () => 2,
        },
        compiled,
      );

      expect(evaluation).toStrictEqual({
        keys: ["moved-row"],
        rows: [
          {
            id: "moved-row",
            price: 30,
          },
        ],
        totalRows: 1,
        version: 2,
        window: [
          {
            key: "moved-row",
            row: {
              id: "moved-row",
              price: 30,
            },
          },
        ],
      });
      expect(slotForKeyCalls).toBe(1);
    }),
  );

  it.effect("rejects hostile structural scan rows instead of treating their slot as proof", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      const wrongType = order("wrong-type", "open", 10, 1);
      Reflect.set(wrongType, "id", 1);
      const missingRequired = order("missing-required", "open", 20, 2);
      Reflect.deleteProperty(missingRequired, "id");
      const evaluateHostileRow = (row: typeof wrongType) =>
        evaluateRawQuery(
          {
            scanRawWindow: () => ({
              keys: ["hostile"],
              window: [{ key: "hostile", row, slot: 0 }],
              totalRows: 1,
            }),
            version: () => 1,
          },
          compiled,
        );

      expect(() => evaluateHostileRow(wrongType)).toThrowError(
        "Projected Query Result Row does not satisfy its compiled proof.",
      );
      expect(() => evaluateHostileRow(missingRequired)).toThrowError(
        "Projected Query Result Row does not satisfy its compiled proof.",
      );
    }),
  );

  it.effect("rejects a forged runtime storage projection capability", () =>
    Effect.gen(function* () {
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      const forgedStore = {
        keyAtSlot: () => "hostile",
        scanRawWindow: () => ({
          keys: ["hostile"],
          window: [
            {
              key: "hostile",
              row: order("valid-fallback", "open", 10, 1),
              slot: 0,
            },
          ],
          totalRows: 1,
        }),
        version: () => 1,
      };
      Reflect.defineProperty(forgedStore, "storageProjection", {
        enumerable: true,
        value: Object.freeze({
          project: () => ({
            consume: () => ({ id: 1, price: "wrong" }),
          }),
        }),
      });

      expect(() => evaluateRawQuery(forgedStore, compiled)).toThrowError(
        "Topic Storage projection capability is not authentic.",
      );
    }),
  );

  it.effect("rejects an authentic storage projection capability from an incompatible schema", () =>
    Effect.gen(function* () {
      const StringPriceOrder = Schema.Struct({
        id: Schema.String,
        price: Schema.String,
      });
      const incompatibleStorage = new TopicRowStorage(
        "string-price-orders",
        StringPriceOrder,
        "id",
      );
      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });

      expect(() =>
        evaluateRawQuery(
          {
            ...incompatibleStorage.readModel,
            keyAtSlot: () => "hostile",
            scanRawWindow: () => ({
              keys: ["hostile"],
              window: [
                {
                  key: "hostile",
                  row: order("valid-fallback", "open", 10, 1),
                  slot: 0,
                },
              ],
              totalRows: 1,
            }),
          },
          compiled,
        ),
      ).toThrowError("Topic Storage projection schema does not match its compiled proof.");
    }),
  );

  it.effect("keeps grouped aggregate aliases outside the raw storage projection capability", () =>
    Effect.gen(function* () {
      const AliasCollision = Schema.Struct({
        id: Schema.String,
        price: Schema.Number,
      });
      const storage = new TopicRowStorage("alias-collision", AliasCollision, "id");
      const grouped = yield* prepareGroupedQuery(
        "alias-collision",
        rawQueryCompilerMetadata(AliasCollision),
        {
          groupBy: ["id"],
          aggregates: {
            price: { aggFunc: "count" },
          },
        },
      );

      expect(
        Reflect.get(grouped.plan.resultSemantics, "topicStorageProjectionProof"),
      ).toBeUndefined();
      expect(Reflect.get(grouped.plan.resultSemantics, "projectTopicStorageRow")).toBeUndefined();
      expect(Object.isFrozen(storage.readModel.storageProjection)).toBe(true);
    }),
  );

  it.effect("binds a concrete storage projection to one authentic compiled proof", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("stored", "open", 10, 1), invalidRow));
      const first = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      const proof = first.plan.resultSemantics.topicStorageProjectionProof;
      const session = bindTopicStorageProjection(storage.readModel.storageProjection, proof);

      expect(Object.isFrozen(storage.readModel.storageProjection)).toBe(true);
      expect(Object.isFrozen(Object.getPrototypeOf(storage.readModel.storageProjection))).toBe(
        true,
      );
      expect(Object.isFrozen(session)).toBe(true);
      expect(Object.isFrozen(Object.getPrototypeOf(session))).toBe(true);
      expect(Object.isFrozen(proof)).toBe(true);
      expect(Object.isFrozen(Object.getPrototypeOf(proof))).toBe(true);
      expect(Reflect.ownKeys(proof)).toStrictEqual([]);
      const projected = session.projectResultRow(0);
      expect(Object.keys(projected)).toStrictEqual(["id", "price"]);
      expect(projected).toStrictEqual({ id: "stored", price: 10 });
      expect(Reflect.apply(session.projectResultRow, Object.freeze({}), [0])).toStrictEqual({
        id: "stored",
        price: 10,
      });
      expect(() =>
        Reflect.construct(
          Reflect.get(Object.getPrototypeOf(storage.readModel.storageProjection), "constructor"),
          [],
        ),
      ).toThrowError("Topic Storage projection construction is private.");
      expect(() =>
        Reflect.construct(Reflect.get(Object.getPrototypeOf(session), "constructor"), []),
      ).toThrowError("Topic Storage projection construction is private.");
      expect(() =>
        Reflect.construct(Reflect.get(Object.getPrototypeOf(proof), "constructor"), []),
      ).toThrowError("Query Result Topic Storage projection proof construction is private.");
      expect(() =>
        Reflect.apply(Reflect.get(Object.getPrototypeOf(proof), "bind"), proof, [
          Object.freeze({}),
          Object.freeze(["id", "price"]),
        ]),
      ).toThrowError("Query Result Topic Storage projection proof binding is private.");
      expect(() =>
        Reflect.apply(
          Reflect.get(Object.getPrototypeOf(storage.readModel.storageProjection), "bind"),
          Object.freeze({}),
          [first.plan.resultSemantics.topicStorageProjectionProof],
        ),
      ).toThrowError(TypeError);
    }),
  );

  it.effect("rejects forged proofs and keeps result projectors owned by their session", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("stored", "open", 10, 1), invalidRow));
      const idQuery = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id"],
      });
      const priceQuery = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["price"],
      });
      const idProof = idQuery.plan.resultSemantics.topicStorageProjectionProof;
      const idSession = bindTopicStorageProjection(storage.readModel.storageProjection, idProof);
      const priceSession = bindTopicStorageProjection(
        storage.readModel.storageProjection,
        priceQuery.plan.resultSemantics.topicStorageProjectionProof,
      );
      const proxyProof = new Proxy(idProof, {});
      const spreadProof = {
        matchesValueSemantics: () => true,
        selectedFields: Object.freeze(["price"]),
      };

      expect(() =>
        bindTopicStorageProjection(storage.readModel.storageProjection, proxyProof),
      ).toThrowError("Query Result Topic Storage projection proof is not authentic.");
      expect(() =>
        Reflect.apply(bindTopicStorageProjection, undefined, [
          storage.readModel.storageProjection,
          spreadProof,
        ]),
      ).toThrowError("Query Result Topic Storage projection proof is not authentic.");
      expect(Reflect.apply(idSession.projectResultRow, priceSession, [0])).toStrictEqual({
        id: "stored",
      });
    }),
  );

  it.effect("rejects missing proof provenance and corrupted required storage projections", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const prepared = yield* storage.prepareRow(order("stored", "open", 10, 1), invalidRow);
      expect(Reflect.deleteProperty(prepared.row, "id")).toBe(false);
      storage.setPrepared(prepared);
      expect(() =>
        Reflect.apply(Reflect.get(storage, "setPrepared"), storage, [
          Object.freeze({
            key: "forged",
            row: Object.freeze(order("forged", "open", 10, 1)),
            source: "row",
          }),
        ]),
      ).toThrowError("Prepared Topic Row is not authentic.");
      const untyped = makeQueryResultSemantics([
        {
          field: "id",
          required: true,
          semantics: storage.valueSemantics.field("id"),
        },
      ]);
      expect(Reflect.get(untyped, "topicStorageProjectionProof")).toBeUndefined();
      expect(Reflect.get(untyped, "projectTopicStorageRow")).toBeUndefined();

      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price"],
      });
      const storedEntry = Reflect.get(storage, "slots")[0];
      Reflect.set(storedEntry, "row", Object.freeze({ price: 10 }));
      expect(() =>
        bindTopicStorageProjection(
          storage.readModel.storageProjection,
          compiled.plan.resultSemantics.topicStorageProjectionProof,
        ).projectResultRow(0),
      ).toThrowError("Projected Query Result Row does not satisfy its compiled proof.");
    }),
  );

  it.effect("keeps stored optional-field presence immutable after preparation", () =>
    Effect.gen(function* () {
      const OptionalOrder = Schema.Struct({
        id: Schema.String,
        note: Schema.optionalKey(Schema.String),
      });
      const storage = new TopicRowStorage("optional-orders", OptionalOrder, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const prepared = yield* storage.prepareRow({ id: "stored" }, invalidRow);
      storage.setPrepared(prepared);

      expect(
        Reflect.defineProperty(prepared.row, "note", {
          configurable: true,
          enumerable: true,
          value: undefined,
          writable: true,
        }),
      ).toBe(false);
      const compiled = yield* prepareRuntimeRawQuery(
        "optional-orders",
        rawQueryCompilerMetadata(OptionalOrder),
        { select: ["id", "note"] },
      );

      expect(evaluateRawQuery(storage.readModel, compiled).rows).toStrictEqual([{ id: "stored" }]);
    }),
  );

  it.effect("projects a __proto__ field as owned row data", () =>
    Effect.gen(function* () {
      const PrototypeFieldOrder = Schema.Struct({
        id: Schema.String,
        ["__proto__"]: Schema.String,
      });
      const storage = new TopicRowStorage("prototype-field-orders", PrototypeFieldOrder, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(
        yield* storage.prepareRow({ id: "stored", ["__proto__"]: "plain-data" }, invalidRow),
      );
      const compiled = yield* prepareRuntimeRawQuery(
        "prototype-field-orders",
        rawQueryCompilerMetadata(PrototypeFieldOrder),
        { select: ["id", "__proto__"] },
      );

      const projected = evaluateRawQuery(storage.readModel, compiled).rows[0]!;
      expect(Object.getPrototypeOf(projected)).toBe(Object.prototype);
      expect(Object.prototype.propertyIsEnumerable.call(projected, "__proto__")).toBe(true);
      expect(projected).toStrictEqual({ id: "stored", ["__proto__"]: "plain-data" });
    }),
  );

  it.effect("rejects prepared rows from a different storage context atomically", () =>
    Effect.gen(function* () {
      const SharedOrder = Schema.Struct({
        alternateId: Schema.String,
        id: Schema.String,
        price: Schema.Finite,
      });
      const StringPrice = Schema.Struct({ id: Schema.String, price: Schema.String });
      const source = new TopicRowStorage("source-orders", SharedOrder, "id");
      const target = new TopicRowStorage("target-orders", SharedOrder, "alternateId");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const foreign = yield* source.prepareRow(
        { alternateId: "foreign-alternate", id: "foreign", price: 1 },
        invalidRow,
      );
      const local = yield* target.prepareRow(
        { alternateId: "local-alternate", id: "local", price: 1 },
        invalidRow,
      );
      const targetPreparationContext = Reflect.get(target, "rowPreparation");

      expect(Object.isFrozen(targetPreparationContext)).toBe(true);
      expect(Reflect.set(targetPreparationContext, "schema", StringPrice)).toBe(false);
      const invalidLocal = yield* Effect.flip(
        target.prepareRow(
          { alternateId: "invalid-local-alternate", id: "invalid-local", price: "wrong" },
          invalidRow,
        ),
      );
      expect(invalidLocal._tag).toBe("InvalidRowError");
      expect(() => target.setPrepared(foreign)).toThrowError(
        "Prepared Topic Row is not authentic.",
      );
      expect(() => target.setPreparedMany([local, foreign])).toThrowError(
        "Prepared Topic Row is not authentic.",
      );
      expect(target.rowCount).toBe(0);
    }),
  );

  it.effect("rejects structured column values corrupted after storage", () =>
    Effect.gen(function* () {
      const StructuredOrder = Schema.Struct({
        id: Schema.String,
        payload: Schema.Struct({ count: Schema.Number }),
      });
      const storage = new TopicRowStorage("structured-orders", StructuredOrder, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      const prepared = yield* storage.prepareRow(
        { id: "stored", payload: { count: 1 } },
        invalidRow,
      );
      storage.setPrepared(prepared);
      expect(Reflect.set(Reflect.get(prepared.row, "payload"), "count", "wrong")).toBe(true);
      const compiled = yield* prepareRuntimeRawQuery(
        "structured-orders",
        rawQueryCompilerMetadata(StructuredOrder),
        { select: ["id", "payload"] },
      );

      expect(() => evaluateRawQuery(storage.readModel, compiled)).toThrowError(
        "Projected Query Result Row does not satisfy its compiled proof.",
      );
    }),
  );

  it.effect("projects raw snapshots correctly after storage delete compacts slots", () =>
    Effect.gen(function* () {
      const storage = new TopicRowStorage("orders", Order, "id");
      const invalidRow = (topic: string, message: string) =>
        InvalidRowError.make({ topic, message });
      storage.setPrepared(yield* storage.prepareRow(order("deleted", "open", 10, 1), invalidRow));
      storage.advanceVersion();
      storage.setPrepared(yield* storage.prepareRow(order("retained", "open", 20, 2), invalidRow));
      storage.advanceVersion();
      storage.setPrepared(yield* storage.prepareRow(order("moved", "open", 30, 3), invalidRow));
      storage.advanceVersion();

      storage.delete("retained");
      storage.advanceVersion();

      const compiled = yield* prepareRuntimeRawQuery("orders", rawQueryCompilerMetadata(Order), {
        select: ["id", "price", "updatedAt"],
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 2,
      });
      const evaluation = evaluateRawQuery(storage.readModel, compiled);

      expect(evaluation).toStrictEqual({
        keys: ["moved", "deleted"],
        rows: [
          {
            id: "moved",
            price: 30,
            updatedAt: 3,
          },
          {
            id: "deleted",
            price: 10,
            updatedAt: 1,
          },
        ],
        totalRows: 2,
        version: 4,
        window: [
          {
            key: "moved",
            row: {
              id: "moved",
              price: 30,
              updatedAt: 3,
            },
          },
          {
            key: "deleted",
            row: {
              id: "deleted",
              price: 10,
              updatedAt: 1,
            },
          },
        ],
      });
    }),
  );
});
