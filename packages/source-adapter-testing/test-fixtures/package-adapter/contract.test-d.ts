import { expectTypeOf } from "@effect/vitest";
import * as Contract from "./contract.js";
import { leasedSource, source } from "./contract.js";

expectTypeOf<typeof Contract.source>().not.toBeAny();
expectTypeOf("unlinked-within-contract").toEqualTypeOf<string>();

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

leasedSource(["region"], {
  // @ts-expect-error package Leased contract rejects invalid source options.
  stream: 1,
});

void Promise.resolve(
  source({
    // @ts-expect-error nested calls still associate rejection evidence with the contract call.
    stream: 1,
  }),
);
