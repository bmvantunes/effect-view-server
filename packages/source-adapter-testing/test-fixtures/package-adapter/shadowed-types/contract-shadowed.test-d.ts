import { expectTypeOf } from "@effect/vitest";
import { source as contractSource } from "../contract.js";

void contractSource;

contractSource({ stream: "@ts-expect-error" });

{
  contractSource({ stream: "orders" });

  const source = (value: number): number => value;

  expectTypeOf(source(1)).toEqualTypeOf<number>();

  // @ts-expect-error a shadowed local binding must not count as contract rejection evidence.
  source("invalid");
}

{
  contractSource({ stream: "orders" });

  // @ts-expect-error an unrelated error in the same statement must not count as contract evidence.
  const invalidNumber: number = "invalid";

  // @ts-ignore a non-expect directive must not count as contract evidence.
  const ignoredNumber: number = "ignored";

  void invalidNumber;
  void ignoredNumber;
}
