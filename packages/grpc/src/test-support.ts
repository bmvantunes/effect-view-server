import { Effect } from "effect";

export const awaitTestCondition = (
  label: () => string,
  predicate: () => boolean,
  remaining = 10_000,
  backoff: Effect.Effect<void> = Effect.sleep("5 millis"),
): Effect.Effect<void> =>
  Effect.suspend(() =>
    predicate()
      ? Effect.void
      : remaining === 0
        ? Effect.die(new TypeError(`Timed out waiting for ${label()}.`))
        : backoff.pipe(
            Effect.andThen(awaitTestCondition(label, predicate, remaining - 1, backoff)),
          ),
  );
