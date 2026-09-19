import { expectTypeOf, it } from "@effect/vitest";
import { StringKeyIndex } from "./string-key-index";

it("keeps index values typed and distinguishes missing lookups", () => {
  const index = new StringKeyIndex<{ readonly slot: number }>();
  index.set("row", { slot: 1 });
  expectTypeOf(index.get("row")).toEqualTypeOf<{ readonly slot: number } | undefined>();
  expectTypeOf(index.has("row")).toEqualTypeOf<boolean>();
  expectTypeOf(index.delete("row")).toEqualTypeOf<boolean>();
  expectTypeOf(index.size).toEqualTypeOf<number>();
  // @ts-expect-error Values must preserve the declared slot type.
  index.set("row", { slot: "invalid" });
  // @ts-expect-error This index accepts only string keys.
  index.get(1);
});
