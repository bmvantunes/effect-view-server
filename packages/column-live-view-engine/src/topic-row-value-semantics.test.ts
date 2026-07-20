import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Schema } from "effect";
import { makeTopicRowValueSemantics } from "./topic-row-value-semantics";

describe("Topic Row value semantics", () => {
  it("rejects an unknown field requested outside validated query preparation", () => {
    const semantics = makeTopicRowValueSemantics(
      Schema.Struct({
        id: Schema.String,
      }),
    );

    expect(() => semantics.field("missing")).toThrowError("Unknown Topic Row field: missing.");
  });

  it("keeps malformed non-Struct schema metadata defensive", () => {
    // @ts-expect-error hostile callers can still pass a non-Struct schema.
    const semantics = makeTopicRowValueSemantics(Schema.String);

    expect(semantics.fieldNames).toStrictEqual([]);
    expect(() => semantics.field("missing")).toThrowError("Unknown Topic Row field: missing.");
  });

  it("reads an accessor field descriptor once before rejecting it", () => {
    const semantics = makeTopicRowValueSemantics(
      Schema.Struct({
        id: Schema.String,
      }),
    );
    let descriptorReads = 0;
    const accessorRow = {};
    Object.defineProperty(accessorRow, "id", {
      enumerable: true,
      get() {
        return "order-1";
      },
    });
    const row = new Proxy(accessorRow, {
      getOwnPropertyDescriptor(target, property) {
        if (property === "id") {
          descriptorReads += 1;
        }
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });

    expect(() => semantics.materializeRow(row)).toThrowError(
      "Topic Row field id must be an own data property.",
    );
    expect(descriptorReads).toBe(1);
  });

  it("compares absent optional objects before nested BigDecimal canonicalization", () => {
    const semantics = makeTopicRowValueSemantics(
      Schema.Struct({
        id: Schema.String,
        meta: Schema.optionalKey(Schema.Struct({ amount: Schema.BigDecimal })),
      }),
    );

    expect(semantics.equivalentField("meta", undefined, undefined)).toBe(true);
    expect(
      semantics.equivalentField("meta", undefined, {
        amount: BigDecimal.fromStringUnsafe("1.0"),
      }),
    ).toBe(false);
    expect(
      semantics.equivalentField(
        "meta",
        { amount: BigDecimal.fromStringUnsafe("1.0") },
        { amount: BigDecimal.fromStringUnsafe("1.00") },
      ),
    ).toBe(true);
  });
});
