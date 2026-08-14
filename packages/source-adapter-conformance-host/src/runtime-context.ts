import { Context, Effect, Schema, Scope } from "effect";

export class SourceAdapterRuntimeContextError extends Schema.TaggedError<SourceAdapterRuntimeContextError>()(
  "SourceAdapterRuntimeContextError",
  {
    message: Schema.String,
  },
) {}

export class SourceAdapterConformanceRowIdError extends Schema.TaggedError<SourceAdapterConformanceRowIdError>()(
  "SourceAdapterConformanceRowIdError",
  {
    message: Schema.String,
  },
) {}

export const validateSourceAdapterRuntimeContext = <Error>(
  runtimeContext: Effect.Effect<unknown, Error, Scope.Scope>,
): Effect.Effect<Context.Context<unknown>, Error | SourceAdapterRuntimeContextError, Scope.Scope> =>
  runtimeContext.pipe(
    Effect.filterOrFail(
      (value): value is Context.Context<unknown> => Context.isContext(value),
      () =>
        new SourceAdapterRuntimeContextError({
          message: "Source Adapter conformance runtime context is not a Context.",
        }),
    ),
  );
