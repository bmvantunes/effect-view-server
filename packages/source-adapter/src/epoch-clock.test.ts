import { describe, expect, it } from "@effect/vitest";
import { Clock, Effect } from "effect";
import { currentEpochNanos, epochNanosFromWallMillis } from "./epoch-clock";

describe("epoch clock", () => {
  it("converts safe non-negative wall-clock milliseconds to epoch nanoseconds", () => {
    expect(epochNanosFromWallMillis(1_234)).toBe(1_234_000_000n);
  });

  it("rejects negative, non-integral, and unsafe wall-clock values", () => {
    for (const invalid of [
      -1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(() => epochNanosFromWallMillis(invalid)).toThrow(
        "Effect Clock returned an invalid wall-clock millisecond value.",
      );
    }
  });

  it.effect(
    "reads independently advanceable epoch wall time without consulting monotonic time",
    () => {
      let wallMillis = 2_000;
      let monotonicNanos = 7n;
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => wallMillis,
        currentTimeMillis: Effect.sync(() => wallMillis),
        currentTimeNanosUnsafe: () => monotonicNanos,
        currentTimeNanos: Effect.sync(() => monotonicNanos),
        monotonicTimeNanosUnsafe: () => monotonicNanos,
        monotonicTimeNanos: Effect.sync(() => monotonicNanos),
        sleep: () => Effect.void,
      };
      return Effect.gen(function* () {
        expect(yield* currentEpochNanos).toBe(2_000_000_000n);
        monotonicNanos = 9_000_000_000n;
        expect(yield* currentEpochNanos).toBe(2_000_000_000n);
        wallMillis = 3_500;
        expect(yield* currentEpochNanos).toBe(3_500_000_000n);
        expect(yield* Clock.currentTimeNanos).toBe(9_000_000_000n);
      }).pipe(Effect.provideService(Clock.Clock, clock));
    },
  );
});
