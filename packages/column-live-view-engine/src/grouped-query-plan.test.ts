import { describe, expect, it } from "@effect/vitest";
import { Option, Schema } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { makeGroupedQueryPlan } from "./grouped-query-plan";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";

const GroupKeyRow = Schema.Struct({
  status: Schema.String,
  price: Schema.Number,
  decimalPrice: Schema.BigDecimal,
  payload: Schema.Record(Schema.String, Schema.String),
  venue: Schema.String,
  negativeZero: Schema.Number,
  quantity: Schema.BigInt,
  active: Schema.Boolean,
  closed: Schema.Boolean,
  missing: Schema.Null,
  note: Schema.Union([Schema.String, Schema.Undefined]),
  collision: Schema.optionalKey(Schema.Array(Schema.String)),
});

const valueSemantics = rawQueryCompilerMetadata(GroupKeyRow).valueSemantics;

describe("Grouped query planning", () => {
  it("uses canonical field tokens for scalar grouped keys", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: [
          "status",
          "price",
          "negativeZero",
          "quantity",
          "active",
          "closed",
          "missing",
          "note",
        ],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "scalar-key-predicate",
    );
    const row = {
      status: "open",
      price: 10,
      negativeZero: -0,
      quantity: 5n,
      active: true,
      closed: false,
      missing: null,
      note: undefined,
    };

    expect(plan.groupKey(row)).toBe(plan.groupKey({ ...row, negativeZero: 0 }));
    expect(plan.groupKey(row)).not.toBe(plan.groupKey({ ...row, price: 11 }));
  });

  it("normalizes BigDecimal grouped key identity through its canonical codec", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["status", "decimalPrice", "venue"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "big-decimal-key-predicate",
    );

    expect(
      plan.groupKey({
        status: "open",
        decimalPrice: fromStringUnsafe("1.50"),
        venue: "xnys",
      }),
    ).toBe(
      plan.groupKey({
        status: "open",
        decimalPrice: fromStringUnsafe("1.5"),
        venue: "xnys",
      }),
    );
  });

  it("uses key-order-neutral canonical tokens for structured grouped values", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["status", "payload", "venue"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "structured-key-predicate",
    );

    expect(
      plan.groupKey({
        status: "open",
        payload: { second: "2", first: "1" },
        venue: "xnys",
      }),
    ).toBe(
      plan.groupKey({
        status: "open",
        payload: { first: "1", second: "2" },
        venue: "xnys",
      }),
    );
  });

  it("reads each grouped key field once", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["status", "payload", "venue"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "single-read-predicate",
    );
    let statusReads = 0;
    let payloadReads = 0;
    let venueReads = 0;
    const row = Object.defineProperties(
      {},
      {
        status: {
          enumerable: true,
          get() {
            statusReads += 1;
            return "open";
          },
        },
        payload: {
          enumerable: true,
          get() {
            payloadReads += 1;
            return { venue: "xnys" };
          },
        },
        venue: {
          enumerable: true,
          get() {
            venueReads += 1;
            return "xnys";
          },
        },
      },
    );

    plan.groupKey(row);

    expect(statusReads).toBe(1);
    expect(payloadReads).toBe(1);
    expect(venueReads).toBe(1);
  });

  it("caches canonical tokens for engine-owned structured values", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["status", "payload"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "canonical-cache-predicate",
    );
    let ownKeyReads = 0;
    const payload = new Proxy(
      { venue: "xnys" },
      {
        ownKeys(target) {
          ownKeyReads += 1;
          return Reflect.ownKeys(target);
        },
      },
    );
    const row = {
      status: "open",
      payload,
    };

    plan.groupKey(row);
    const readsAfterFirstKey = ownKeyReads;
    plan.groupKey(row);

    expect(readsAfterFirstKey).toBeGreaterThan(0);
    expect(ownKeyReads).toBe(readsAfterFirstKey);
  });

  it("distinguishes a missing aggregate input from a present undefined value", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["status"],
        aggregates: {
          distinctNotes: { aggFunc: "countDistinct", field: "note" },
        },
      },
      valueSemantics,
      "aggregate-input-predicate",
    );
    const aggregatePlan = Option.getOrThrow(
      Option.fromNullishOr(plan.aggregatePlans.find((candidate) => candidate.kind === "field")),
    );
    const missing = aggregatePlan.input.read({});
    const presentUndefined = aggregatePlan.input.read({ note: undefined });
    const secondPresentUndefined = aggregatePlan.input.read({ note: undefined });
    const presentValue = aggregatePlan.input.read({ note: "note" });

    expect(missing).toStrictEqual({ _tag: "Missing" });
    expect(presentUndefined).toStrictEqual({ _tag: "Present", value: undefined });
    expect(aggregatePlan.input.canonicalKey(missing)).not.toBe(
      aggregatePlan.input.canonicalKey(presentUndefined),
    );
    expect(aggregatePlan.input.equivalent(missing, presentUndefined)).toBe(false);
    expect(aggregatePlan.input.equivalent(presentUndefined, secondPresentUndefined)).toBe(true);
    expect(aggregatePlan.input.compare(missing, presentUndefined)).toBe(-1);
    expect(aggregatePlan.input.compare(presentUndefined, missing)).toBe(1);
    expect(aggregatePlan.input.compare(presentUndefined, secondPresentUndefined)).toBe(0);
    expect(aggregatePlan.input.compare(presentUndefined, presentValue)).toBe(-1);
  });

  it("distinguishes a missing grouped field from a present value shaped like the token", () => {
    const plan = makeGroupedQueryPlan<object>(
      {
        groupBy: ["collision"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      },
      valueSemantics,
      "presence-token-collision-predicate",
    );

    expect(plan.groupKey({})).not.toBe(plan.groupKey({ collision: ["missing"] }));
  });
});
