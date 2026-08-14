import { describe, expect, it } from "@effect/vitest";
import { definedFields } from "./optional-fields";

describe("optional field builders", () => {
  it("builds only defined enumerable data fields", () => {
    const source = Object.defineProperties(
      {
        present: "value",
        omitted: undefined,
      },
      {
        accessor: {
          enumerable: true,
          get: () => "ignored",
        },
        hidden: {
          enumerable: false,
          value: "ignored",
        },
      },
    );

    expect(definedFields("input", () => source)).toStrictEqual({ present: "value" });
  });

  it("keeps the factory lazy for undefined values", () => {
    expect(
      definedFields(undefined, () => {
        throw new Error("must stay lazy");
      }),
    ).toStrictEqual({});
  });
});
