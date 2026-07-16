import { Effect, Exit } from "effect";

export type RuntimeCoreResourceHandoffOptions = {
  readonly beforeReturn?: Effect.Effect<void>;
};

export const acquireRuntimeCoreResourceHandoff = <Value, Error, Requirements>(
  acquire: (
    markAcquired: (finalizer: Effect.Effect<void>) => Effect.Effect<void>,
  ) => Effect.Effect<Value, Error, Requirements>,
  options: RuntimeCoreResourceHandoffOptions = {},
): Effect.Effect<Value, Error, Requirements> =>
  Effect.suspend(() => {
    let acquiredFinalizer: Effect.Effect<void> | undefined;
    const markAcquired = (finalizer: Effect.Effect<void>) =>
      Effect.sync(() => {
        acquiredFinalizer = finalizer;
      });

    return acquire(markAcquired).pipe(
      Effect.tap(() => options.beforeReturn ?? Effect.void),
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) || acquiredFinalizer === undefined ? Effect.void : acquiredFinalizer,
      ),
    );
  });
