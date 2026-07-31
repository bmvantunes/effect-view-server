import { Clock, Effect } from "effect";

export const epochNanosFromWallMillis = (wallMillis: number): bigint => {
  if (!Number.isSafeInteger(wallMillis) || wallMillis < 0) {
    throw new TypeError("Effect Clock returned an invalid wall-clock millisecond value.");
  }
  return BigInt(wallMillis) * 1_000_000n;
};

export const currentEpochNanos = Clock.currentTimeMillis.pipe(Effect.map(epochNanosFromWallMillis));
