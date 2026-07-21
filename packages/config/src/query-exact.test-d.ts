import { describe, expectTypeOf, it } from "@effect/vitest";
import type { RejectArrayExtraKeys, ValidateExactArray } from "./query-exact";

describe("exact query arrays", () => {
  it("accepts ordinary mutable and readonly arrays and rejects optional entries", () => {
    expectTypeOf<ValidateExactArray<Array<"a">>>().not.toBeNever();
    expectTypeOf<ValidateExactArray<["a"]>>().not.toBeNever();
    expectTypeOf<ValidateExactArray<ReadonlyArray<"a">>>().not.toBeNever();
    expectTypeOf<keyof RejectArrayExtraKeys<readonly ["a"]>>().toBeNever();
    expectTypeOf<ValidateExactArray<readonly ["a"]>>().not.toBeNever();
    expectTypeOf<ValidateExactArray<readonly ["a"?]>>().toBeNever();
  });

  it("rejects own overrides of mutable and shared array prototype methods", () => {
    const pushSource = ["a"] satisfies Array<"a">;
    const pushDecorated = Object.assign(pushSource, { push: pushSource.push });
    const sortDecorated = Object.assign(["a"] satisfies ["a"], {
      sort: (): ["a"] => ["a"],
    });
    const mutableMapSource = ["a"] satisfies Array<"a">;
    const mutableMapDecorated = Object.assign(mutableMapSource, {
      map: mutableMapSource.map.bind(mutableMapSource),
    });
    const readonlyMapSource: ReadonlyArray<"a"> = ["a"];
    const readonlyMapDecorated = Object.assign(readonlyMapSource, {
      map: readonlyMapSource.map.bind(readonlyMapSource),
    });
    const lengthSource = ["a"] satisfies Array<"a">;
    const narrowedLengthWithoutTupleEntries = Object.assign(lengthSource, { length: 2 as const });

    expectTypeOf<ValidateExactArray<typeof pushDecorated>>().toBeNever();
    expectTypeOf<ValidateExactArray<typeof sortDecorated>>().toBeNever();
    expectTypeOf<ValidateExactArray<typeof mutableMapDecorated>>().toBeNever();
    expectTypeOf<ValidateExactArray<typeof readonlyMapDecorated>>().toBeNever();
    expectTypeOf<ValidateExactArray<typeof narrowedLengthWithoutTupleEntries>>().toBeNever();
  });

  it("rejects decorated tuple union members", () => {
    type DecoratedUnion = readonly ["a"] | (readonly ["a"] & { readonly metadata: true });

    expectTypeOf<ValidateExactArray<DecoratedUnion>>().toBeNever();
  });
});
