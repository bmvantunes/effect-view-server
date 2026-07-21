import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import { ownViewServerQuerySnapshot, snapshotViewServerQuery } from "./query-input-snapshot";

describe("query input snapshots", () => {
  it("owns recursive values while preserving sharing and exact scalars", () => {
    const shared = { field: "label", type: "equals", filter: "ÁbC" };
    const amount = BigDecimal.make(1230n, 3);
    const query = {
      routeBy: { region: "ÁbC", zero: -0, amount },
      where: [shared, { type: "NOT", condition: shared }],
      scalars: [null, true, 1, 1n],
    };
    const snapshot = snapshotViewServerQuery(query);
    query.routeBy.region = "changed";

    expect(snapshot.routeBy).toStrictEqual({ region: "ÁbC", zero: -0, amount });
    expect(Object.is(snapshot.routeBy.zero, -0)).toBe(true);
    expect(snapshot.routeBy.amount).not.toBe(amount);
    expect(snapshot.where[0]).toBe(Reflect.get(Object(snapshot.where[1]), "condition"));
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(snapshotViewServerQuery(snapshot)).toBe(snapshot);

    const derived = ownViewServerQuerySnapshot(Object.freeze({ select: snapshot.scalars }));
    expect(snapshotViewServerQuery(derived)).toBe(derived);
    expect(() => ownViewServerQuerySnapshot({ select: ["id"] })).toThrow(
      "Owned query snapshots must be frozen.",
    );
  });

  it("owns a stateful BigDecimal from one descriptor capture", () => {
    let coefficientDescriptorReads = 0;
    let scaleDescriptorReads = 0;
    const amount = new Proxy(BigDecimal.make(123n, 2), {
      getOwnPropertyDescriptor: (target, key) => {
        if (key === "value") {
          coefficientDescriptorReads += 1;
          if (coefficientDescriptorReads > 1) {
            throw new Error("coefficient descriptor was read twice");
          }
        }
        if (key === "scale") {
          scaleDescriptorReads += 1;
          if (scaleDescriptorReads > 1) {
            throw new Error("scale descriptor was read twice");
          }
        }
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });

    const snapshot = snapshotViewServerQuery({ routeBy: { amount }, values: [amount, amount] });

    expect(snapshot).toStrictEqual({
      routeBy: { amount: BigDecimal.make(123n, 2) },
      values: [BigDecimal.make(123n, 2), BigDecimal.make(123n, 2)],
    });
    expect(snapshot.values[0]).toBe(snapshot.routeBy.amount);
    expect(snapshot.values[1]).toBe(snapshot.routeBy.amount);
    expect(coefficientDescriptorReads).toBe(1);
    expect(scaleDescriptorReads).toBe(1);
  });

  it("rejects cycles and unsupported scalar or object values", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic["self"] = cyclic;
    let hostileBrandChecks = 0;
    const hostileBrand = new Proxy(
      {},
      {
        has: () => {
          hostileBrandChecks += 1;
          throw new Error("brand reflection failed");
        },
      },
    );
    const hostileBigDecimal = new Proxy(BigDecimal.make(123n, 2), {
      getOwnPropertyDescriptor: () => {
        throw new Error("BigDecimal descriptor reflection failed");
      },
    });
    let forgedBigDecimalReads = 0;
    const forgedBigDecimal: object = Object.create(Object.getPrototypeOf(BigDecimal.make(123n, 2)));
    const forgedFields: ReadonlyArray<readonly [string, unknown]> = [
      ["value", 123n],
      ["scale", 2],
    ];
    for (const [key, result] of forgedFields) {
      Object.defineProperty(forgedBigDecimal, key, {
        enumerable: true,
        get: () => {
          forgedBigDecimalReads += 1;
          return result;
        },
      });
    }

    expect(() => snapshotViewServerQuery({ extension: cyclic })).toThrow(
      "Query input contains a cycle.",
    );
    expect(() => snapshotViewServerQuery({ extension: undefined })).toThrow(
      "Query input contains an unsupported value.",
    );
    expect(() => snapshotViewServerQuery({ extension: () => true })).toThrow(
      "Query input contains an unsupported value.",
    );
    expect(() => snapshotViewServerQuery({ extension: Symbol("x") })).toThrow(
      "Query input contains an unsupported value.",
    );
    expect(() => snapshotViewServerQuery({ extension: Number.NaN })).toThrow(
      "Query input numbers must be finite.",
    );
    expect(() => snapshotViewServerQuery({ extension: new Date(0) })).toThrow(
      "Query input contains an unsupported object value.",
    );
    expect(snapshotViewServerQuery({ extension: hostileBrand })).toStrictEqual({ extension: {} });
    expect(hostileBrandChecks).toBe(0);
    expect(() => snapshotViewServerQuery({ extension: hostileBigDecimal })).toThrow(
      "Query input contains an unsupported object value.",
    );
    expect(() => snapshotViewServerQuery({ extension: forgedBigDecimal })).toThrow(
      "Query input contains an unsupported object value.",
    );
    expect(forgedBigDecimalReads).toBe(0);
    for (const scale of [Number.POSITIVE_INFINITY, Number.NaN, 1.5]) {
      expect(() =>
        snapshotViewServerQuery({ routeBy: { amount: BigDecimal.make(123n, scale) } }),
      ).toThrow("Query input contains an unsupported object value.");
    }
    expect(() => snapshotViewServerQuery([])).toThrow(
      "Query input snapshot must remain a plain object.",
    );
  });

  it("rejects hostile record and array ownership shapes without invoking accessors", () => {
    let reads = 0;
    let arrayReads = 0;
    let extraArrayReads = 0;
    const accessor: Record<string, unknown> = {};
    Object.defineProperty(accessor, "value", {
      enumerable: true,
      get: () => {
        reads += 1;
        return "computed";
      },
    });
    const symbolic: Record<string, unknown> = {};
    Object.defineProperty(symbolic, Symbol("x"), { enumerable: true, value: true });
    const hidden: Record<string, unknown> = {};
    Object.defineProperty(hidden, "value", { enumerable: false, value: true });
    const missingDescriptor = new Proxy(
      {},
      {
        ownKeys: () => ["ghost"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    const sparse: Array<unknown> = [];
    sparse.length = 1;
    const arrayAccessor: Array<unknown> = [undefined];
    Object.defineProperty(arrayAccessor, "0", { enumerable: true, get: () => "computed" });
    const hiddenArrayValue: Array<unknown> = [undefined];
    Object.defineProperty(hiddenArrayValue, "0", { enumerable: false, value: "hidden" });
    const extended: Array<unknown> = [];
    Reflect.set(extended, "extra", true);
    const extraArrayAccessor: Array<unknown> = [];
    Object.defineProperty(extraArrayAccessor, "extra", {
      enumerable: true,
      get: () => {
        extraArrayReads += 1;
        return true;
      },
    });
    const symbolicArray: Array<unknown> = [];
    Object.defineProperty(symbolicArray, Symbol("x"), { enumerable: true, value: true });
    const proxiedArray = new Proxy(["plain"], {
      get: () => {
        arrayReads += 1;
        throw new Error("array property reads must not run");
      },
    });
    class CustomArray extends Array<unknown> {}

    expect(() => snapshotViewServerQuery({ extension: accessor })).toThrow(
      "Query input fields must be own enumerable data properties.",
    );
    expect(() => snapshotViewServerQuery({ extension: symbolic })).toThrow(
      "Query input must not contain symbol properties.",
    );
    expect(() => snapshotViewServerQuery({ extension: hidden })).toThrow(
      "Query input fields must be own enumerable data properties.",
    );
    expect(() => snapshotViewServerQuery({ extension: missingDescriptor })).toThrow(
      "Query input fields must be own enumerable data properties.",
    );
    expect(() => snapshotViewServerQuery({ extension: sparse })).toThrow(
      "Query input arrays must be dense data arrays.",
    );
    expect(() => snapshotViewServerQuery({ extension: arrayAccessor })).toThrow(
      "Query input arrays must be dense data arrays.",
    );
    expect(() => snapshotViewServerQuery({ extension: hiddenArrayValue })).toThrow(
      "Query input arrays must be dense data arrays.",
    );
    const extendedSnapshot = snapshotViewServerQuery({ extension: extended });
    expect(Reflect.get(extendedSnapshot.extension, "extra")).toBe(true);
    expect(Object.isFrozen(extendedSnapshot.extension)).toBe(true);
    expect(() => snapshotViewServerQuery({ extension: extraArrayAccessor })).toThrow(
      "Query input arrays must contain enumerable data properties.",
    );
    expect(() => snapshotViewServerQuery({ extension: new CustomArray() })).toThrow(
      "Query input arrays must be plain arrays.",
    );
    expect(() => snapshotViewServerQuery({ extension: symbolicArray })).toThrow(
      "Query input arrays must be plain arrays.",
    );
    expect(snapshotViewServerQuery({ extension: proxiedArray })).toStrictEqual({
      extension: ["plain"],
    });
    expect(reads).toBe(0);
    expect(arrayReads).toBe(0);
    expect(extraArrayReads).toBe(0);
  });
});
