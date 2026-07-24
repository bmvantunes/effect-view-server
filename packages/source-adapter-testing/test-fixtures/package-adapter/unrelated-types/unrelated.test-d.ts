import { expectTypeOf } from "@effect/vitest";

expectTypeOf<string>().toEqualTypeOf<string>();

// @ts-expect-error unrelated negative evidence must not satisfy package conformance.
const invalid: string = 1;

void invalid;
