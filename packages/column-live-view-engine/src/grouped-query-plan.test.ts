import { describe, expect, it } from "@effect/vitest";
import { fromStringUnsafe } from "effect/BigDecimal";
import { stableQueryValueString } from "./raw-query-compiler";
import { makeGroupedQueryPlan } from "./grouped-query-plan";

describe("Grouped query planning", () => {
  it("keeps scalar grouped key strings compatible with stable query value strings", () => {
    const plan = makeGroupedQueryPlan<object>({
      groupBy: [
        "status",
        "price",
        "notANumber",
        "positiveInfinity",
        "negativeInfinity",
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
    });
    const row = {
      status: "open",
      price: 10,
      notANumber: Number.NaN,
      positiveInfinity: Number.POSITIVE_INFINITY,
      negativeInfinity: Number.NEGATIVE_INFINITY,
      negativeZero: -0,
      quantity: 5n,
      active: true,
      closed: false,
      missing: null,
      note: undefined,
    };

    expect(plan.groupKey(row)).toBe(
      stableQueryValueString([
        ["status", "open"],
        ["price", 10],
        ["notANumber", Number.NaN],
        ["positiveInfinity", Number.POSITIVE_INFINITY],
        ["negativeInfinity", Number.NEGATIVE_INFINITY],
        ["negativeZero", -0],
        ["quantity", 5n],
        ["active", true],
        ["closed", false],
        ["missing", null],
        ["note", undefined],
      ]),
    );
  });

  it("keeps BigDecimal grouped key fallback compatible with stable query value strings", () => {
    const plan = makeGroupedQueryPlan<object>({
      groupBy: ["status", "decimalPrice", "venue"],
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    });
    const decimalPrice = fromStringUnsafe("1.50");
    const row = {
      status: "open",
      decimalPrice,
      venue: "xnys",
    };

    expect(plan.groupKey(row)).toBe(
      stableQueryValueString([
        ["status", "open"],
        ["decimalPrice", decimalPrice],
        ["venue", "xnys"],
      ]),
    );
  });

  it("keeps structured grouped key fallback compatible with stable query value strings", () => {
    const plan = makeGroupedQueryPlan<object>({
      groupBy: ["status", "payload", "venue"],
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    });
    const payload = new Map([["venue", "xnys"]]);
    const row = {
      status: "open",
      payload,
      venue: "xnys",
    };

    expect(plan.groupKey(row)).toBe(
      stableQueryValueString([
        ["status", "open"],
        ["payload", payload],
        ["venue", "xnys"],
      ]),
    );
  });

  it("reads each grouped key field once when fallback is needed", () => {
    const plan = makeGroupedQueryPlan<object>({
      groupBy: ["status", "payload", "venue"],
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    });
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
            return new Map([["venue", "xnys"]]);
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

  it("does not add extra object probes before grouped key fallback", () => {
    const plan = makeGroupedQueryPlan<object>({
      groupBy: ["status", "payload"],
      aggregates: {
        rowCount: { aggFunc: "count" },
      },
    });
    let hasProbeCount = 0;
    const payload = new Proxy(
      {},
      {
        has(target, key) {
          hasProbeCount += 1;
          return key in target;
        },
      },
    );
    stableQueryValueString([
      ["status", "open"],
      ["payload", payload],
    ]);
    const baselineHasProbeCount = hasProbeCount;
    hasProbeCount = 0;

    plan.groupKey({
      status: "open",
      payload,
    });

    expect(hasProbeCount).toBe(baselineHasProbeCount);
  });
});
