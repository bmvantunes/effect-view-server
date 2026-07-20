import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import {
  compareTrustedWireSafeBigDecimal,
  compareWireSafeBigDecimal,
  inspectWireSafeBigDecimal,
  isTrustedWireSafeBigDecimal,
  isWireSafeBigDecimal,
  trustedWireSafeBigDecimalSemanticKey,
  wireSafeBigDecimalSemanticKey,
} from "./wire-safe-big-decimal";

describe("wire-safe BigDecimal", () => {
  it("requires an Effect decimal that round-trips through the JSON codec", () => {
    const invalidCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidCoefficient, "value", { value: 123 });

    expect([
      isWireSafeBigDecimal(BigDecimal.make(123n, 2)),
      isWireSafeBigDecimal(BigDecimal.make(123n, -2)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.POSITIVE_INFINITY)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.NaN)),
      isWireSafeBigDecimal(BigDecimal.make(123n, 1.5)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.MAX_SAFE_INTEGER + 1)),
      isWireSafeBigDecimal(BigDecimal.make(1n, Number.MIN_SAFE_INTEGER)),
      isWireSafeBigDecimal(BigDecimal.make(10n, Number.MIN_SAFE_INTEGER)),
      isWireSafeBigDecimal(BigDecimal.make(111n, Number.MIN_SAFE_INTEGER)),
      isWireSafeBigDecimal(BigDecimal.make(111n, Number.MIN_SAFE_INTEGER + 1)),
      isWireSafeBigDecimal(invalidCoefficient),
      isWireSafeBigDecimal({ value: 123n, scale: 2 }),
    ]).toStrictEqual([
      true,
      true,
      false,
      false,
      false,
      false,
      true,
      false,
      false,
      false,
      false,
      false,
    ]);
  });

  it("accepts a cross-bundle Effect prototype with data-backed fields", () => {
    const foreignPrototype: object = {};
    Object.defineProperty(foreignPrototype, "~effect/BigDecimal", {
      enumerable: true,
      value: "~effect/BigDecimal",
    });
    const foreignBigDecimal: object = Object.create(foreignPrototype);
    Object.defineProperties(foreignBigDecimal, {
      value: { enumerable: true, value: 123n },
      scale: { enumerable: true, value: 2 },
    });

    expect(isWireSafeBigDecimal(foreignBigDecimal)).toBe(true);
    expect(inspectWireSafeBigDecimal(foreignBigDecimal)).toStrictEqual({
      _tag: "Success",
      source: foreignBigDecimal,
      coefficient: 123n,
      scale: 2,
      semanticKey: '["123","2"]',
    });
  });

  it("rejects hostile branded-value reflection without invoking accessors", () => {
    let forgedReads = 0;
    const forgedBrand: Record<string, unknown> = {
      "~effect/BigDecimal": "~effect/BigDecimal",
    };
    Object.defineProperty(forgedBrand, "value", {
      enumerable: true,
      get: () => {
        forgedReads += 1;
        return 123n;
      },
    });
    Object.defineProperty(forgedBrand, "scale", {
      enumerable: true,
      get: () => {
        forgedReads += 1;
        return 2;
      },
    });
    const accessorForgery: object = Object.create(Object.getPrototypeOf(BigDecimal.make(123n, 2)));
    Object.defineProperty(accessorForgery, "value", {
      enumerable: true,
      get: () => {
        forgedReads += 1;
        return 123n;
      },
    });
    Object.defineProperty(accessorForgery, "scale", {
      enumerable: true,
      get: () => {
        forgedReads += 1;
        return 2;
      },
    });
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    const brandFailure = new Proxy(
      {},
      {
        has: () => {
          throw new Error("brand reflection failed");
        },
      },
    );
    const descriptorFailure = new Proxy(BigDecimal.make(123n, 2), {
      getOwnPropertyDescriptor: () => {
        throw new Error("descriptor reflection failed");
      },
    });
    const invalidCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidCoefficient, "value", { value: 123 });
    const nullPrototype = Object.create(null);
    const accessorBrandPrototype: object = {};
    Object.defineProperty(accessorBrandPrototype, "~effect/BigDecimal", {
      get: () => {
        forgedReads += 1;
        return "~effect/BigDecimal";
      },
    });
    const accessorBrand: object = Object.create(accessorBrandPrototype);
    const wrongBrandPrototype = { "~effect/BigDecimal": "not-big-decimal" };
    const wrongBrand: object = Object.create(wrongBrandPrototype);

    expect([
      isWireSafeBigDecimal(revoked.proxy),
      isWireSafeBigDecimal(brandFailure),
      isWireSafeBigDecimal(descriptorFailure),
      isWireSafeBigDecimal(forgedBrand),
      isWireSafeBigDecimal(accessorForgery),
      isWireSafeBigDecimal(nullPrototype),
      isWireSafeBigDecimal(accessorBrand),
      isWireSafeBigDecimal(wrongBrand),
    ]).toStrictEqual([false, false, false, false, false, false, false, false]);
    expect(inspectWireSafeBigDecimal(revoked.proxy)).toStrictEqual({
      _tag: "ReflectionFailure",
    });
    expect(inspectWireSafeBigDecimal({ value: 123n, scale: 2 })).toStrictEqual({
      _tag: "NotBigDecimal",
    });
    expect(inspectWireSafeBigDecimal(forgedBrand)).toStrictEqual({
      _tag: "NotBigDecimal",
    });
    expect(inspectWireSafeBigDecimal(accessorForgery)).toStrictEqual({
      _tag: "UnsafeBigDecimal",
    });
    expect(inspectWireSafeBigDecimal(BigDecimal.make(123n, Number.NaN))).toStrictEqual({
      _tag: "UnsafeBigDecimal",
    });
    expect(inspectWireSafeBigDecimal(invalidCoefficient)).toStrictEqual({
      _tag: "UnsafeBigDecimal",
    });
    const valid = BigDecimal.make(123n, 2);
    expect(inspectWireSafeBigDecimal(valid)).toStrictEqual({
      _tag: "Success",
      source: valid,
      coefficient: 123n,
      scale: 2,
      semanticKey: '["123","2"]',
    });
    expect(forgedReads).toBe(0);
  });

  it("compares from one captured descriptor of each BigDecimal field", () => {
    let coefficientDescriptorReads = 0;
    let scaleDescriptorReads = 0;
    const value = new Proxy(BigDecimal.make(123n, 2), {
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
    const comparisonFailure = new Proxy(BigDecimal.make(123n, 2), {
      getOwnPropertyDescriptor: () => {
        throw new Error("comparison reflection failed");
      },
    });
    const invalidLeftCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidLeftCoefficient, "value", { value: 123 });
    const invalidRightCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidRightCoefficient, "value", { value: 123 });

    expect(compareWireSafeBigDecimal(value, BigDecimal.make(123n, 2))).toBe(0);
    expect(coefficientDescriptorReads).toBe(1);
    expect(scaleDescriptorReads).toBe(1);
    expect(compareWireSafeBigDecimal(comparisonFailure, BigDecimal.make(123n, 2))).toBeUndefined();
    expect([
      compareWireSafeBigDecimal({}, BigDecimal.make(123n, 2)),
      compareWireSafeBigDecimal(BigDecimal.make(123n, 2), {}),
      compareWireSafeBigDecimal(invalidLeftCoefficient, BigDecimal.make(123n, 2)),
      compareWireSafeBigDecimal(BigDecimal.make(123n, 2), invalidRightCoefficient),
      compareWireSafeBigDecimal(BigDecimal.make(123n, 2), BigDecimal.make(123n, Number.NaN)),
    ]).toStrictEqual([undefined, undefined, undefined, undefined, undefined]);
  });

  it("keeps trusted engine guards and comparisons allocation-free", () => {
    let coefficientReads = 0;
    let scaleReads = 0;
    const value = new Proxy(BigDecimal.make(123n, 2), {
      get: (target, key, receiver) => {
        if (key === "value") {
          coefficientReads += 1;
        }
        if (key === "scale") {
          scaleReads += 1;
        }
        return Reflect.get(target, key, receiver);
      },
    });
    const reflectionFailure = new Proxy(BigDecimal.make(123n, 2), {
      get: () => {
        throw new Error("trusted value reflection failed");
      },
    });
    const invalidLeftCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidLeftCoefficient, "value", { value: 123 });
    const invalidRightCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidRightCoefficient, "value", { value: 123 });
    const invalidScaleType = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidScaleType, "scale", { value: "2" });

    expect(isTrustedWireSafeBigDecimal(value)).toBe(true);
    expect(coefficientReads).toBe(1);
    expect(scaleReads).toBe(1);
    expect([
      isTrustedWireSafeBigDecimal({}),
      isTrustedWireSafeBigDecimal(reflectionFailure),
      isTrustedWireSafeBigDecimal(invalidLeftCoefficient),
      isTrustedWireSafeBigDecimal(invalidScaleType),
      isTrustedWireSafeBigDecimal(BigDecimal.make(123n, Number.NaN)),
    ]).toStrictEqual([false, false, false, false, false]);
    expect(compareTrustedWireSafeBigDecimal(value, BigDecimal.make(123n, 2))).toBe(0);
    expect(coefficientReads).toBe(2);
    expect(scaleReads).toBe(2);
    expect([
      compareTrustedWireSafeBigDecimal(reflectionFailure, BigDecimal.make(123n, 2)),
      compareTrustedWireSafeBigDecimal(invalidLeftCoefficient, BigDecimal.make(123n, 2)),
      compareTrustedWireSafeBigDecimal(BigDecimal.make(123n, 2), invalidRightCoefficient),
      compareTrustedWireSafeBigDecimal(BigDecimal.make(123n, 2), BigDecimal.make(123n, Number.NaN)),
    ]).toStrictEqual([undefined, undefined, undefined, undefined]);
  });

  it("compares extreme scales without constructing powers of ten", () => {
    const tiny = BigDecimal.make(1n, Number.MAX_SAFE_INTEGER);
    const lessTiny = BigDecimal.make(1n, Number.MAX_SAFE_INTEGER - 1);
    const huge = BigDecimal.make(1n, Number.MIN_SAFE_INTEGER);
    const lessHuge = BigDecimal.make(1n, Number.MIN_SAFE_INTEGER + 1);

    expect([
      compareWireSafeBigDecimal(tiny, lessTiny),
      compareWireSafeBigDecimal(lessTiny, tiny),
      compareWireSafeBigDecimal(huge, lessHuge),
      compareWireSafeBigDecimal(lessHuge, huge),
      compareWireSafeBigDecimal(BigDecimal.make(-1n, Number.MIN_SAFE_INTEGER), huge),
      compareWireSafeBigDecimal(BigDecimal.make(-1n, Number.MAX_SAFE_INTEGER), tiny),
    ]).toStrictEqual([-1, 1, 1, -1, -1, -1]);
  });

  it("creates injective semantic keys only for codec-round-trippable decimals", () => {
    const firstCollision = BigDecimal.make(111n, Number.MIN_SAFE_INTEGER);
    const secondCollision = BigDecimal.make(111n, Number.MIN_SAFE_INTEGER + 1);
    const negativeTrailingZeroBoundary = BigDecimal.make(-10n, Number.MIN_SAFE_INTEGER + 1);
    const invalidCoefficient = BigDecimal.make(1n, 0);
    Object.defineProperty(invalidCoefficient, "value", { value: 1 });
    const invalidScaleType = BigDecimal.make(1n, 0);
    Object.defineProperty(invalidScaleType, "scale", { value: "0" });
    const reflectionFailure = new Proxy(BigDecimal.make(1n, 0), {
      get: () => {
        throw new Error("semantic key reflection failed");
      },
    });

    expect([
      wireSafeBigDecimalSemanticKey(BigDecimal.make(150n, 2)),
      wireSafeBigDecimalSemanticKey(BigDecimal.make(15n, 1)),
      trustedWireSafeBigDecimalSemanticKey(BigDecimal.make(150n, 2)),
      wireSafeBigDecimalSemanticKey(firstCollision),
      wireSafeBigDecimalSemanticKey(secondCollision),
      trustedWireSafeBigDecimalSemanticKey(firstCollision),
      trustedWireSafeBigDecimalSemanticKey(BigDecimal.make(10n, Number.MIN_SAFE_INTEGER)),
      trustedWireSafeBigDecimalSemanticKey(invalidCoefficient),
      trustedWireSafeBigDecimalSemanticKey(invalidScaleType),
      trustedWireSafeBigDecimalSemanticKey(reflectionFailure),
    ]).toStrictEqual([
      '["15","1"]',
      '["15","1"]',
      '["15","1"]',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
    ]);
    expect(wireSafeBigDecimalSemanticKey(negativeTrailingZeroBoundary)).toBe(
      '["-1","-9007199254740991"]',
    );
  });

  it("compares equal values and coefficients with different decimal widths", () => {
    expect([
      compareWireSafeBigDecimal(
        BigDecimal.make(0n, Number.MAX_SAFE_INTEGER),
        BigDecimal.make(0n, 0),
      ),
      compareWireSafeBigDecimal(BigDecimal.make(12n, 1), BigDecimal.make(120n, 2)),
      compareWireSafeBigDecimal(BigDecimal.make(1201n, 3), BigDecimal.make(12n, 1)),
      compareWireSafeBigDecimal(BigDecimal.make(-1201n, 3), BigDecimal.make(-12n, 1)),
      compareWireSafeBigDecimal(BigDecimal.make(1n, 0), BigDecimal.make(1n, -0)),
      compareWireSafeBigDecimal(BigDecimal.make(1n, Number.NaN), BigDecimal.make(1n, 0)),
    ]).toStrictEqual([0, 0, 1, -1, 0, undefined]);
  });

  it("agrees with Effect ordering for ordinary scales", () => {
    const values = [
      BigDecimal.make(-1201n, 3),
      BigDecimal.make(-12n, 1),
      BigDecimal.make(-1n, -2),
      BigDecimal.make(0n, 9),
      BigDecimal.make(1n, 4),
      BigDecimal.make(12n, 1),
      BigDecimal.make(1201n, 3),
      BigDecimal.make(1n, -2),
    ];

    for (const left of values) {
      for (const right of values) {
        expect(compareWireSafeBigDecimal(left, right)).toBe(BigDecimal.Order(left, right));
        expect(compareTrustedWireSafeBigDecimal(left, right)).toBe(BigDecimal.Order(left, right));
      }
    }
  });
});
