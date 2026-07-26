import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect } from "effect";
import { awaitTestCondition } from "./test-support";

describe("gRPC test polling support", () => {
  it.effect("returns immediately when the default-budget condition is already ready", () =>
    Effect.gen(function* () {
      let reads = 0;

      yield* awaitTestCondition(
        () => "an immediately ready condition",
        () => {
          reads += 1;
          return true;
        },
      );

      expect(reads).toBe(1);
    }),
  );

  it.live("uses the default bounded backoff until the condition becomes ready", () =>
    Effect.gen(function* () {
      let reads = 0;

      yield* awaitTestCondition(
        () => `two condition reads; last observed ${reads}`,
        () => {
          reads += 1;
          return reads === 2;
        },
        2,
      );

      expect(reads).toBe(2);
    }),
  );

  it.effect("dies with the last labeled observation when the budget is exhausted", () =>
    Effect.gen(function* () {
      const observed = yield* awaitTestCondition(
        () => "an exhausted condition; last observed false",
        () => false,
        0,
        Effect.void,
      ).pipe(
        Effect.matchCause({
          onFailure: Cause.squash,
          onSuccess: () => undefined,
        }),
      );

      expect(observed).toStrictEqual(
        new TypeError("Timed out waiting for an exhausted condition; last observed false."),
      );
    }),
  );

  it.effect("treats a negative initial budget as already exhausted", () =>
    Effect.gen(function* () {
      const observed = yield* awaitTestCondition(
        () => "a negative-budget condition",
        () => false,
        -1,
        Effect.void,
      ).pipe(
        Effect.matchCause({
          onFailure: Cause.squash,
          onSuccess: () => undefined,
        }),
      );

      expect(observed).toStrictEqual(
        new TypeError("Timed out waiting for a negative-budget condition."),
      );
    }),
  );
});
