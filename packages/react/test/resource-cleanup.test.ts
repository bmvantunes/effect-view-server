import { expect, it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { closeRemoteTestResources } from "./resource-cleanup";

it.effect("attempts every remote test resource close after an earlier failure", () =>
  Effect.gen(function* () {
    const closed: Array<string> = [];
    const result = yield* Effect.exit(
      closeRemoteTestResources([
        Effect.sync(() => {
          closed.push("first");
        }).pipe(Effect.andThen(Effect.fail("close failed"))),
        Effect.sync(() => {
          closed.push("second");
        }),
        Effect.sync(() => {
          closed.push("third");
        }).pipe(Effect.andThen(Effect.fail("another close failed"))),
      ]),
    );
    yield* closeRemoteTestResources([
      Effect.sync(() => {
        closed.push("successful");
      }),
    ]);

    expect(Exit.isFailure(result)).toBe(true);
    expect(closed).toStrictEqual(["first", "second", "third", "successful"]);
  }),
);
