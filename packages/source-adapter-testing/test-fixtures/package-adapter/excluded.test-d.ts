import { expectTypeOf } from "@effect/vitest";

expectTypeOf("excluded").toEqualTypeOf<string>();
expectTypeOf("excluded").toEqualTypeOf<string>();

// @ts-expect-error excluded evidence must never count.
const invalid: string = 1;
// @ts-expect-error excluded evidence must never count.
const alsoInvalid: string = 2;

void invalid;
void alsoInvalid;
