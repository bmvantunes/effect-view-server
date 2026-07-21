import { describe, expectTypeOf, it } from "@effect/vitest";
import { trustDecodedRuntimeQuery, type ValidatedRuntimeQuery } from "./validated-runtime-query";

describe("validated runtime query type contract", () => {
  it("brands records while preserving their inferred query type", () => {
    const query = { select: ["id"], extension: { enabled: true } };
    const validated = trustDecodedRuntimeQuery(query);

    expectTypeOf(validated).toEqualTypeOf<typeof query & ValidatedRuntimeQuery>();
    expectTypeOf(validated.select).toEqualTypeOf<Array<string>>();
    expectTypeOf(validated.extension).toEqualTypeOf<{ enabled: boolean }>();

    // @ts-expect-error decoded runtime queries must be records
    trustDecodedRuntimeQuery("not-a-query");
  });
});
