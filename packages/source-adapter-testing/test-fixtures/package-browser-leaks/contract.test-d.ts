import { expectTypeOf } from "@effect/vitest";
import { leasedSource, source } from "./contract.js";

expectTypeOf(
  source({
    stream: "orders",
  }).lifecycle,
).toEqualTypeOf<"materialized">();

expectTypeOf(
  leasedSource(["region"], {
    stream: "orders",
  }).lifecycle,
).toEqualTypeOf<"leased">();

source({
  // @ts-expect-error package contract rejects invalid source options.
  stream: 1,
});
