import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import {
  applyDelta,
  expectDeltaEvent,
  expectSnapshotEvent,
  firstEvent,
  makeEventReader,
  stateFromSnapshot,
} from "../test-harness/events";
import { makeEngine, order, Order } from "../test-harness/public-engine";
import { normalizeDecimalFields } from "../test-harness/rows";
import { InvalidQueryError } from "./index";
import { prepareGroupedQuery } from "./grouped-query-compiler";
import { decodeTypedGroupedQuery } from "./grouped-query-decoder";
import {
  prepareRawQuery,
  prepareRuntimeRawQuery,
  rawQueryCompilerMetadata,
  rawQueryCompilerMetadataMatchesSchema,
} from "./raw-query-compiler";
import { decodeTypedRawQuery } from "./raw-query-decoder";
import { groupedQueryResultSemantics, rawQueryResultSemantics } from "./query-result-semantics";
import { topicRowValueSemanticsMatchesSchema } from "./topic-row-value-semantics";

describe("ColumnLiveViewEngine query result proof", () => {
  it.effect("keeps raw snapshot and delta projection exact and convergent", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "closed", 20, 2)]);
      const query = {
        select: ["id", "status"],
        orderBy: [{ field: "id", direction: "asc" }],
      } satisfies {
        readonly select: readonly ["id", "status"];
        readonly orderBy: readonly [{ readonly field: "id"; readonly direction: "asc" }];
      };

      const initialSnapshot = yield* engine.snapshot("orders", query);
      expect(initialSnapshot).toStrictEqual({
        rows: [
          { id: "a", status: "open" },
          { id: "b", status: "closed" },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 1,
      });

      const subscription = yield* engine.subscribe("orders", query);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      expect(initial).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a", "b"],
        rows: [
          { id: "a", status: "open" },
          { id: "b", status: "closed" },
        ],
        totalRows: 2,
      });
      let state = stateFromSnapshot(initial);

      yield* engine.patch("orders", "a", { status: "closed", price: 99 });
      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "a",
            row: { id: "a", status: "closed" },
            index: 0,
          },
        ],
        totalRows: 2,
      });
      state = applyDelta(state, delta);

      const freshSnapshot = yield* engine.snapshot("orders", query);
      expect(freshSnapshot).toStrictEqual({
        rows: [
          { id: "a", status: "closed" },
          { id: "b", status: "closed" },
        ],
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 2,
      });
      expect(state).toStrictEqual({
        keys: ["a", "b"],
        rows: freshSnapshot.rows,
      });

      yield* subscription.close();
      yield* engine.close();
    }),
  );

  it.effect("keeps grouped snapshot and delta projection exact and convergent", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      yield* engine.publishMany("orders", [order("a", "open", 10, 1), order("b", "closed", 20, 2)]);
      const query = {
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        orderBy: [{ field: "status", direction: "asc" }],
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
          readonly totalPrice: { readonly aggFunc: "sum"; readonly field: "price" };
        };
        readonly orderBy: readonly [{ readonly field: "status"; readonly direction: "asc" }];
      };
      const initialRows = [
        { status: "closed", rowCount: 1n, totalPrice: "20" },
        { status: "open", rowCount: 1n, totalPrice: "10" },
      ];

      const initialSnapshot = yield* engine.snapshot("orders", query);
      expect({
        ...initialSnapshot,
        rows: normalizeDecimalFields(initialSnapshot.rows),
      }).toStrictEqual({
        rows: initialRows,
        status: "ready",
        statusCode: "Ready",
        totalRows: 2,
        version: 1,
      });

      const subscription = yield* engine.subscribeRuntime("orders", query);
      const read = yield* makeEventReader(subscription);
      const initial = firstEvent(yield* read(1));
      expectSnapshotEvent(initial);
      expect(normalizeDecimalFields(initial.rows)).toStrictEqual(initialRows);
      expect(initial.totalRows).toBe(2);
      expect(initial.version).toBe(1);
      let state = stateFromSnapshot(initial);

      yield* engine.patch("orders", "a", { status: "closed", price: 15 });
      const delta = firstEvent(yield* read(1));
      expectDeltaEvent(delta);
      expect(delta.fromVersion).toBe(1);
      expect(delta.toVersion).toBe(2);
      expect(delta.totalRows).toBe(1);
      state = applyDelta(state, delta);

      const freshSnapshot = yield* engine.snapshot("orders", query);
      const convergedRows = [{ status: "closed", rowCount: 2n, totalPrice: "35" }];
      expect({
        ...freshSnapshot,
        rows: normalizeDecimalFields(freshSnapshot.rows),
      }).toStrictEqual({
        rows: convergedRows,
        status: "ready",
        statusCode: "Ready",
        totalRows: 1,
        version: 2,
      });
      expect(normalizeDecimalFields(state.rows)).toStrictEqual(convergedRows);

      yield* subscription.close();
      yield* engine.close();
    }),
  );

  it.effect("rejects a mixed runtime query before acquiring active state", () =>
    Effect.gen(function* () {
      const engine = yield* makeEngine();
      const mixedQuery: unknown = {
        select: ["id"],
        groupBy: ["status"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      };

      const error = yield* Effect.flip(engine.subscribeRuntime("orders", mixedQuery));
      expect(error).toStrictEqual(
        InvalidQueryError.make({
          topic: "orders",
          message: "Grouped query must not include select.",
        }),
      );

      const health = yield* engine.health();
      expect(health.activeSubscriptions).toBe(0);
      expect(health.topics.orders.activeSubscriptions).toBe(0);
      expect(health.topics.orders.activeViews).toBe(0);

      yield* engine.close();
    }),
  );

  it.effect("rejects metadata whose public schema does not match its hidden provenance", () =>
    Effect.gen(function* () {
      const numericSchema = Schema.Struct({ id: Schema.Number });
      const stringSchema = Schema.Struct({ id: Schema.String });
      const forgedMetadata = {
        ...rawQueryCompilerMetadata(numericSchema),
        schema: stringSchema,
      };

      expect(rawQueryCompilerMetadataMatchesSchema(forgedMetadata, stringSchema)).toBe(false);
      const error = yield* Effect.flip(
        // @ts-expect-error this intentionally exercises a hostile JavaScript metadata object.
        prepareRuntimeRawQuery("forged", forgedMetadata, { select: ["id"] }),
      );
      expect(error).toStrictEqual(
        InvalidQueryError.make({
          topic: "forged",
          message: "Query compiler metadata schema does not match its provenance.",
        }),
      );
    }),
  );

  it.effect("rejects metadata whose value semantics come from another same-row schema", () =>
    Effect.gen(function* () {
      const finiteSchema = Schema.Struct({ id: Schema.Finite });
      const numberSchema = Schema.Struct({ id: Schema.Number });
      const finiteMetadata = rawQueryCompilerMetadata(finiteSchema);
      const numberMetadata = rawQueryCompilerMetadata(numberSchema);
      const forgedSemantics = {
        ...finiteMetadata.valueSemantics,
        field: () => numberMetadata.valueSemantics.field("id"),
      };
      const forgedMetadata = {
        ...finiteMetadata,
        valueSemantics: forgedSemantics,
      };

      expect(topicRowValueSemanticsMatchesSchema(forgedSemantics, finiteSchema)).toBe(false);
      expect(rawQueryCompilerMetadataMatchesSchema(forgedMetadata, finiteSchema)).toBe(false);
      const error = yield* Effect.flip(
        prepareRuntimeRawQuery("forged", forgedMetadata, { select: ["id"] }),
      );
      expect(error).toStrictEqual(
        InvalidQueryError.make({
          topic: "forged",
          message: "Query compiler metadata schema does not match its provenance.",
        }),
      );
    }),
  );

  it.effect("rejects mismatched or unauthenticated typed query witnesses", () =>
    Effect.gen(function* () {
      const numericSchema = Schema.Struct({ id: Schema.Number });
      const stringSchema = Schema.Struct({ id: Schema.String });
      const numericMetadata = rawQueryCompilerMetadata(numericSchema);
      const stringMetadata = rawQueryCompilerMetadata(stringSchema);
      const numericRawQuery = { select: ["id"] } as const;
      const stringRawQuery = { select: ["id"] } as const;
      const numericGroupedQuery = {
        groupBy: ["id"],
        aggregates: { rowCount: { aggFunc: "count" } },
      } as const;
      const stringGroupedQuery = {
        groupBy: ["id"],
        aggregates: { rowCount: { aggFunc: "count" } },
      } as const;
      const numericRawWitness = yield* decodeTypedRawQuery(
        "numeric",
        numericMetadata,
        numericRawQuery,
      );
      const numericGroupedWitness = yield* decodeTypedGroupedQuery(
        "numeric",
        numericMetadata,
        numericGroupedQuery,
      );
      const numericRawSemantics = rawQueryResultSemantics(
        numericMetadata.valueSemantics,
        numericRawWitness,
      );

      expect(numericRawSemantics.narrowProjectedRow({ id: 1 })).toStrictEqual({ id: 1 });

      expect(() =>
        rawQueryResultSemantics<typeof stringSchema.Type, typeof stringRawQuery>(
          stringMetadata.valueSemantics,
          // @ts-expect-error a numeric witness cannot prove string result semantics.
          numericRawWitness,
        ),
      ).toThrowError(
        new TypeError("Typed raw query proof does not match its Topic Row Value Semantics."),
      );
      expect(() =>
        groupedQueryResultSemantics<typeof stringSchema.Type, typeof stringGroupedQuery>(
          stringMetadata.valueSemantics,
          // @ts-expect-error a numeric witness cannot prove string grouped result semantics.
          numericGroupedWitness,
        ),
      ).toThrowError(
        new TypeError("Typed grouped query proof does not match its Topic Row Value Semantics."),
      );
      expect(() =>
        rawQueryResultSemantics<typeof stringSchema.Type, typeof stringRawQuery>(
          stringMetadata.valueSemantics,
          // @ts-expect-error a structural query is not an authenticated decoded witness.
          stringRawQuery,
        ),
      ).toThrowError(
        new TypeError("Typed raw query proof does not match its Topic Row Value Semantics."),
      );
      expect(() =>
        groupedQueryResultSemantics<typeof stringSchema.Type, typeof stringGroupedQuery>(
          stringMetadata.valueSemantics,
          // @ts-expect-error a structural grouped query is not an authenticated decoded witness.
          stringGroupedQuery,
        ),
      ).toThrowError(
        new TypeError("Typed grouped query proof does not match its Topic Row Value Semantics."),
      );
    }),
  );

  it.effect("rejects undefined extrema for required aggregate fields", () =>
    Effect.gen(function* () {
      const RequiredMetric = Schema.Struct({
        group: Schema.String,
        value: Schema.Number,
      });
      const compiled = yield* prepareGroupedQuery(
        "requiredMetrics",
        rawQueryCompilerMetadata(RequiredMetric),
        {
          groupBy: ["group"],
          aggregates: {
            minValue: { aggFunc: "min", field: "value" },
            maxValue: { aggFunc: "max", field: "value" },
          },
        },
      );
      const proof = compiled.plan.resultSemantics;

      expect(() =>
        proof.narrowProjectedRow({ group: "a", minValue: undefined, maxValue: 2 }),
      ).toThrowError(
        new TypeError("Projected Query Result Row does not satisfy its compiled proof."),
      );
      expect(() =>
        proof.narrowProjectedRow({ group: "a", minValue: 1, maxValue: undefined }),
      ).toThrowError(
        new TypeError("Projected Query Result Row does not satisfy its compiled proof."),
      );
      expect(proof.narrowProjectedRow({ group: "a", minValue: 1, maxValue: 2 })).toStrictEqual({
        group: "a",
        minValue: 1,
        maxValue: 2,
      });
    }),
  );

  it("freezes every public object in the schema-backed proof graph", () => {
    const stringMetadata = rawQueryCompilerMetadata(Schema.Struct({ id: Schema.String }));
    const numberMetadata = rawQueryCompilerMetadata(Schema.Struct({ id: Schema.Number }));
    const stringFieldSemantics = stringMetadata.valueSemantics.field("id");
    const numberFieldSemantics = numberMetadata.valueSemantics.field("id");
    const stringFieldMetadata = Option.getOrThrow(
      Option.fromNullishOr(stringMetadata.fieldMetadata.get("id")),
    );
    const numberRangeKinds = Option.getOrThrow(
      Option.fromNullishOr(numberMetadata.rangeValueKinds.get("id")),
    );

    expect(Object.isFrozen(stringMetadata)).toBe(true);
    expect(Object.isFrozen(stringMetadata.fieldNames)).toBe(true);
    expect(Object.isFrozen(stringMetadata.fieldOrder)).toBe(true);
    expect(Object.isFrozen(stringMetadata.fieldMetadata)).toBe(true);
    expect(Object.isFrozen(stringFieldMetadata)).toBe(true);
    expect(Object.isFrozen(numberMetadata.rangeValueKinds)).toBe(true);
    expect(Object.isFrozen(numberRangeKinds)).toBe(true);
    expect(Object.isFrozen(stringMetadata.valueSemantics)).toBe(true);
    expect(Object.isFrozen(stringFieldSemantics)).toBe(true);
    expect(Object.isFrozen(stringMetadata.valueSemantics.fieldNames)).toBe(true);
    expect(() => Set.prototype.add.call(stringMetadata.fieldNames, "missing")).toThrowError(
      TypeError,
    );
    expect(() =>
      Map.prototype.set.call(
        stringMetadata.fieldMetadata,
        "id",
        numberMetadata.fieldMetadata.get("id"),
      ),
    ).toThrowError(TypeError);
    expect(() => Set.prototype.add.call(numberRangeKinds, "bigint")).toThrowError(TypeError);
    expect(() => Array.prototype.push.call(stringMetadata.fieldOrder, "missing")).toThrowError(
      TypeError,
    );
    expect(() => Object.assign(stringFieldMetadata, { sumResultKind: "bigint" })).toThrowError(
      TypeError,
    );
    expect(() => Object.assign(stringMetadata, numberMetadata)).toThrowError(TypeError);
    expect(() =>
      Object.assign(stringMetadata.valueSemantics, {
        field: () => numberFieldSemantics,
      }),
    ).toThrowError(TypeError);
    expect(() => Object.assign(stringFieldSemantics, numberFieldSemantics)).toThrowError(TypeError);
  });

  it.effect("captures typed raw and grouped query accessors exactly once", () =>
    Effect.gen(function* () {
      const metadata = rawQueryCompilerMetadata(Order);
      let rawSelectReads = 0;
      const rawQuery = {
        get select(): readonly ["id"] | readonly ["id", "status"] {
          rawSelectReads += 1;
          return rawSelectReads === 1 ? ["id"] : ["id", "status"];
        },
      };

      const raw = yield* prepareRawQuery("orders", metadata, rawQuery);
      expect(rawSelectReads).toBe(1);
      expect(raw.plan.selectedFields).toStrictEqual(["id"]);
      expect(raw.plan.project(order("a", "open", 10, 1))).toStrictEqual({ id: "a" });
      expect(raw.plan.resultSemantics.narrowProjectedRow({ id: "a" })).toStrictEqual({ id: "a" });

      let groupedKeyReads = 0;
      const groupedQuery = {
        get groupBy(): readonly ["status"] | readonly ["status", "region"] {
          groupedKeyReads += 1;
          return groupedKeyReads === 1 ? ["status"] : ["status", "region"];
        },
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"] | readonly ["status", "region"];
        readonly aggregates: {
          readonly rowCount: { readonly aggFunc: "count" };
        };
      };

      const grouped = yield* prepareGroupedQuery("orders", metadata, groupedQuery);
      expect(groupedKeyReads).toBe(1);
      expect(grouped.plan.groupBy).toStrictEqual(["status"]);
      expect(
        grouped.plan.resultSemantics.projectRow({ status: "open", rowCount: 1n }),
      ).toStrictEqual({ status: "open", rowCount: 1n });

      let arrayPropertyReads = 0;
      let arrayEntryDescriptorReads = 0;
      const inspectOnlyArray = <Values extends Array<unknown>>(values: Values): Values =>
        new Proxy(values, {
          get: () => {
            arrayPropertyReads += 1;
            throw new Error("decoded query arrays must not be read through properties");
          },
          getOwnPropertyDescriptor: (target, key) => {
            if (key === "0") {
              arrayEntryDescriptorReads += 1;
            }
            return Reflect.getOwnPropertyDescriptor(target, key);
          },
        });
      const rawSelect = inspectOnlyArray<["id"]>(["id"]);
      const rawOrderBy = inspectOnlyArray<[{ field: "id"; direction: "asc" }]>([
        { field: "id", direction: "asc" },
      ]);
      const groupedFields = inspectOnlyArray<["status"]>(["status"]);
      const groupedOrderBy = inspectOnlyArray<[{ aggregate: "rowCount"; direction: "desc" }]>([
        { aggregate: "rowCount", direction: "desc" },
      ]);

      const inspectedRaw = yield* prepareRawQuery("orders", metadata, {
        select: rawSelect,
        orderBy: rawOrderBy,
      });
      const inspectedGrouped = yield* prepareGroupedQuery("orders", metadata, {
        groupBy: groupedFields,
        aggregates: { rowCount: { aggFunc: "count" } },
        orderBy: groupedOrderBy,
      });

      expect(inspectedRaw.plan.selectedFields).toStrictEqual(["id"]);
      expect(inspectedRaw.plan.orderBy).toStrictEqual([{ field: "id", direction: "asc" }]);
      expect(inspectedGrouped.plan.groupBy).toStrictEqual(["status"]);
      expect(inspectedGrouped.plan.orderBy).toStrictEqual([
        { aggregate: "rowCount", direction: "desc" },
      ]);
      expect(arrayPropertyReads).toBe(0);
      expect(arrayEntryDescriptorReads).toBe(4);
    }),
  );
});

it.effect("freezes raw and grouped compiled proof carriers", () =>
  Effect.gen(function* () {
    const stringSchema = Schema.Struct({ id: Schema.String });
    const numberSchema = Schema.Struct({ id: Schema.Number });
    const stringMetadata = rawQueryCompilerMetadata(stringSchema);
    const numberMetadata = rawQueryCompilerMetadata(numberSchema);
    const stringRaw = yield* prepareRawQuery("strings", stringMetadata, { select: ["id"] });
    const numberRaw = yield* prepareRawQuery("numbers", numberMetadata, { select: ["id"] });
    const stringGrouped = yield* prepareGroupedQuery("strings", stringMetadata, {
      groupBy: ["id"],
      aggregates: { rowCount: { aggFunc: "count" } },
      where: [{ field: "id", type: "equals", filter: "none" }],
    });
    const numberGrouped = yield* prepareGroupedQuery("numbers", numberMetadata, {
      groupBy: ["id"],
      aggregates: { rowCount: { aggFunc: "count" } },
    });
    const emptyGroupedEvaluation = stringGrouped.evaluate({
      changesSince: () => [],
      scanRows: () => {},
      version: () => 0,
    });

    expect(Object.isFrozen(stringRaw.plan)).toBe(true);
    expect(Object.isFrozen(stringRaw)).toBe(true);
    expect(Object.isFrozen(stringGrouped.plan)).toBe(true);
    expect(Object.isFrozen(stringGrouped)).toBe(true);
    expect(Object.isFrozen(stringRaw.plan.resultSemantics)).toBe(true);
    expect(Object.isFrozen(stringGrouped.plan.resultSemantics)).toBe(true);
    expect(emptyGroupedEvaluation).toStrictEqual({
      keys: [],
      rows: [],
      totalRows: 0,
      version: 0,
      window: [],
    });
    expect(Object.isFrozen(stringRaw.plan.resultSemantics.topicStorageProjectionProof)).toBe(true);
    expect(
      Reflect.ownKeys(stringRaw.plan.resultSemantics.topicStorageProjectionProof),
    ).toStrictEqual([]);
    expect(
      Object.isFrozen(
        Object.getPrototypeOf(stringRaw.plan.resultSemantics.topicStorageProjectionProof),
      ),
    ).toBe(true);
    expect(() => Object.assign(stringRaw.plan, numberRaw.plan)).toThrowError(TypeError);
    expect(() => Object.assign(stringRaw, numberRaw)).toThrowError(TypeError);
    expect(() => Object.assign(stringGrouped.plan, numberGrouped.plan)).toThrowError(TypeError);
    expect(() => Object.assign(stringGrouped, numberGrouped)).toThrowError(TypeError);
    expect(() =>
      Object.assign(stringRaw.plan.resultSemantics, numberRaw.plan.resultSemantics),
    ).toThrowError(TypeError);
    expect(() =>
      Object.assign(stringGrouped.plan.resultSemantics, numberGrouped.plan.resultSemantics),
    ).toThrowError(TypeError);
    expect(
      Reflect.defineProperty(stringRaw.plan.resultSemantics.topicStorageProjectionProof, "forged", {
        configurable: true,
        enumerable: true,
        value: numberRaw.plan.resultSemantics.topicStorageProjectionProof,
        writable: true,
      }),
    ).toBe(false);
    expect(() =>
      Object.assign(
        Object.getPrototypeOf(stringRaw.plan.resultSemantics.topicStorageProjectionProof),
        {
          matchesValueSemantics: () => true,
        },
      ),
    ).toThrowError(TypeError);
    expect(() =>
      Reflect.construct(
        Reflect.get(
          Object.getPrototypeOf(stringRaw.plan.resultSemantics.topicStorageProjectionProof),
          "constructor",
        ),
        [],
      ),
    ).toThrowError("Query Result Topic Storage projection proof construction is private.");
  }),
);
