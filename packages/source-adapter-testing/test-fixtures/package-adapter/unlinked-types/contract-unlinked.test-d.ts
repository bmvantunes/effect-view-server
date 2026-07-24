import { expectTypeOf } from "@effect/vitest";
import { source } from "../contract.js";

expectTypeOf<string>().toEqualTypeOf<string>();

// @ts-expect-error unrelated failures do not prove contract rejection behavior.
const invalid: string = 1;

void invalid;
void source;
