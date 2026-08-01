import { Cause, Effect, Exit } from "effect";

export const closeRemoteTestResources = Effect.fn("React.test.resources.close")(function* <Error>(
  resources: ReadonlyArray<Effect.Effect<void, Error>>,
) {
  let failure: Cause.Cause<Error> | undefined;
  for (const close of resources) {
    const exit = yield* Effect.exit(close);
    if (Exit.isFailure(exit)) {
      failure = failure === undefined ? exit.cause : Cause.combine(failure, exit.cause);
    }
  }
  if (failure !== undefined) {
    return yield* Effect.failCause(failure);
  }
});
