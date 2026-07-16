import { Effect, Exit } from "effect";

type RuntimeCoreClosableResource = {
  readonly close: () => Effect.Effect<void>;
};

export type RuntimeCoreResourceHandoffOptions = {
  readonly beforeReturn?: Effect.Effect<void>;
};

export const acquireRuntimeCoreResourceHandoff = <
  Resource extends RuntimeCoreClosableResource,
  Value,
  Error,
  Requirements,
>(
  acquire: (
    markAcquired: (resource: Resource) => Effect.Effect<void>,
  ) => Effect.Effect<Value, Error, Requirements>,
  options: RuntimeCoreResourceHandoffOptions = {},
): Effect.Effect<Value, Error, Requirements> =>
  Effect.suspend(() => {
    let acquiredResource: RuntimeCoreClosableResource | undefined;
    const markAcquired = (resource: Resource) =>
      Effect.sync(() => {
        acquiredResource = resource;
      });

    return acquire(markAcquired).pipe(
      Effect.tap(() => options.beforeReturn ?? Effect.void),
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) || acquiredResource === undefined
          ? Effect.void
          : acquiredResource.close(),
      ),
    );
  });
