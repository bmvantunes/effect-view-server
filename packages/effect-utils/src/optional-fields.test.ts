import { describe, expect, it } from "@effect/vitest";
import { conditionalFields, definedFields } from "./optional-fields";

describe("optional field builders", () => {
  it("includes conditional fields only for a true condition", () => {
    expect(conditionalFields(true, () => ({ included: true }))).toStrictEqual({ included: true });
    expect(conditionalFields(false, () => ({ omitted: true }))).toStrictEqual({});
    expect(
      conditionalFields(false, () => {
        throw new Error("must stay lazy");
      }),
    ).toStrictEqual({});
  });

  it("narrows a defined value inside the factory", () => {
    expect(definedFields("value", (value) => ({ value }))).toStrictEqual({ value: "value" });
    expect(
      definedFields(undefined, () => {
        throw new Error("must stay lazy");
      }),
    ).toStrictEqual({});
  });

  it("omits undefined fields returned by the factory", () => {
    const maybeValue: string | undefined = undefined;
    expect(definedFields("present", () => ({ value: maybeValue }))).toStrictEqual({});
  });

  it("does not evaluate non-enumerable accessors", () => {
    const source = Object.defineProperty({}, "ignored", {
      enumerable: false,
      get: () => {
        throw new Error("must stay inaccessible");
      },
    });

    expect(definedFields("present", () => source)).toStrictEqual({});
  });
});
