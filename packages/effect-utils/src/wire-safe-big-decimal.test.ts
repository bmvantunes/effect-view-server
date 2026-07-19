import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import { compareWireSafeBigDecimal, isWireSafeBigDecimal } from "./wire-safe-big-decimal";

describe("wire-safe BigDecimal", () => {
  it("requires the Effect brand, a bigint coefficient, and a safe integer scale", () => {
    const invalidCoefficient = BigDecimal.make(123n, 2);
    Object.defineProperty(invalidCoefficient, "value", { value: 123 });

    expect([
      isWireSafeBigDecimal(BigDecimal.make(123n, 2)),
      isWireSafeBigDecimal(BigDecimal.make(123n, -2)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.POSITIVE_INFINITY)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.NaN)),
      isWireSafeBigDecimal(BigDecimal.make(123n, 1.5)),
      isWireSafeBigDecimal(BigDecimal.make(123n, Number.MAX_SAFE_INTEGER + 1)),
      isWireSafeBigDecimal(invalidCoefficient),
      isWireSafeBigDecimal({ value: 123n, scale: 2 }),
    ]).toStrictEqual([true, true, false, false, false, false, false, false]);
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
      }
    }
  });
});
