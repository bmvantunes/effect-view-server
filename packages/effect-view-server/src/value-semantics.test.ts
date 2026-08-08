import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as ValueSemantics from "./value-semantics";

describe("public value semantics", () => {
  it("exposes only the wire-safe BigDecimal runtime contract", () => {
    expect(Object.keys(ValueSemantics).sort()).toStrictEqual([
      "compareTrustedWireSafeBigDecimal",
      "compareWireSafeBigDecimal",
      "inspectWireSafeBigDecimal",
      "isWireSafeBigDecimal",
      "wireSafeBigDecimalSemanticKey",
    ]);
  });

  it("preserves exact scaled equality and allocation-safe extreme ordering", () => {
    const scaled = BigDecimal.make(150n, 2);
    const canonical = BigDecimal.make(15n, 1);
    const tiny = BigDecimal.make(1n, Number.MAX_SAFE_INTEGER);
    const lessTiny = BigDecimal.make(1n, Number.MAX_SAFE_INTEGER - 1);

    expect(ValueSemantics.compareWireSafeBigDecimal(scaled, canonical)).toBe(0);
    expect(ValueSemantics.wireSafeBigDecimalSemanticKey(scaled)).toBe(
      ValueSemantics.wireSafeBigDecimalSemanticKey(canonical),
    );
    expect(ValueSemantics.compareTrustedWireSafeBigDecimal(tiny, lessTiny)).toBe(-1);
  });

  it("rejects invalid and hostile values at the public boundary", () => {
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();

    expect(ValueSemantics.inspectWireSafeBigDecimal(revoked.proxy)).toStrictEqual({
      _tag: "ReflectionFailure",
    });
    expect(ValueSemantics.isWireSafeBigDecimal({ value: 1n, scale: 0 })).toBe(false);
    expect(ValueSemantics.compareWireSafeBigDecimal(BigDecimal.make(1n, 0), {})).toBeUndefined();
  });
});
