import { describe, expect, it } from "@effect/vitest";
import { Context, Effect } from "effect";
import {
  SourceAdapterRuntimeContextError,
  validateSourceAdapterRuntimeContext,
} from "./runtime-context";

describe("Source Adapter runtime context validation", () => {
  it.effect("accepts an Effect Context", () =>
    Effect.gen(function* () {
      const context = yield* validateSourceAdapterRuntimeContext(Effect.succeed(Context.empty()));
      expect(Context.isContext(context)).toBe(true);
    }),
  );

  it.effect("rejects an Effect that produces a non-Context value", () =>
    Effect.gen(function* () {
      const error = yield* validateSourceAdapterRuntimeContext(Effect.succeed(42)).pipe(
        Effect.flip,
      );
      expect(error).toBeInstanceOf(SourceAdapterRuntimeContextError);
      expect(error).toHaveProperty(
        "message",
        "Source Adapter conformance runtime context is not a Context.",
      );
    }),
  );
});
