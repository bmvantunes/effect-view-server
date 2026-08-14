import { describe, expectTypeOf, it } from "@effect/vitest";
import { conditionalFields, definedFields } from "./optional-fields";

declare const maybeCount: number | undefined;
declare const maybeLabel: string | undefined;

describe("optional field helpers", () => {
  it("infers value and field contracts without explicit type arguments", () => {
    const defined = definedFields(maybeCount, (count) => {
      expectTypeOf(count).toEqualTypeOf<number>();
      return { count };
    });
    const conditional = conditionalFields(true, () => ({ label: "ready" as const }));

    expectTypeOf(defined).toEqualTypeOf<{ count?: number }>();
    expectTypeOf(conditional).toEqualTypeOf<{ label?: "ready" }>();
  });

  it("preserves field types while making conditional output optional", () => {
    const defined = definedFields<number, { readonly count: number }>(1, (count) => ({ count }));
    const conditional = conditionalFields<{ readonly label: string }>(true, () => ({
      label: "ready",
    }));

    expectTypeOf(defined).toEqualTypeOf<{ readonly count?: number }>();
    expectTypeOf(conditional).toEqualTypeOf<{ readonly label?: string }>();
  });

  it("keeps undefined values out of the advertised field type", () => {
    const defined = definedFields(1, () => ({ label: maybeLabel }));

    expectTypeOf(defined).toEqualTypeOf<{ label?: string }>();
  });

  it("rejects non-object field contracts", () => {
    // @ts-expect-error field helpers require object-shaped contracts.
    definedFields(1, () => "invalid");

    // @ts-expect-error field helpers require object-shaped contracts.
    definedFields<number, string>(1, () => "invalid");
    // @ts-expect-error conditional field helpers require object-shaped contracts.
    conditionalFields(true, () => "invalid");
  });
});
