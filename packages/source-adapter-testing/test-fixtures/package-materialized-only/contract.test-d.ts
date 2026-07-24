import { expectTypeOf } from "@effect/vitest";
import { source } from "./contract.js";

expectTypeOf(source({ stream: "orders" }).lifecycle).toEqualTypeOf<"materialized">();

source({
  // @ts-expect-error package contract rejects invalid source options.
  stream: 1,
});
