import { describe, expect, it } from "@effect/vitest";
import { definedFields } from "./optional-fields";

describe("optional field builders", () => {
  it("preserves only defined fields", () => {
    expect(definedFields(undefined, () => ({ omitted: true }))).toStrictEqual({});
    expect(
      definedFields("value", () => ({
        omitted: undefined,
        present: "value",
      })),
    ).toStrictEqual({ present: "value" });
  });

  it("ignores accessors and non-enumerable properties", () => {
    const source = Object.defineProperties(
      { present: "value", omitted: undefined },
      {
        accessor: { enumerable: true, get: () => "ignored" },
        hidden: { enumerable: false, value: "ignored" },
      },
    );

    expect(definedFields("input", () => source)).toStrictEqual({ present: "value" });
  });
});
