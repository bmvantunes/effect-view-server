import { Cause, Effect, Exit } from "effect";

export {
  materializeStrictJson,
  StrictJsonMaterializationError,
  type StrictJsonMaterializationReason,
} from "./strict-json-materialization";
export { makeSchemaJsonIdentity, type SchemaJsonIdentity } from "./schema-json-identity";
export {
  compileGroupedKeyIdentity,
  type CompiledGroupedKeyIdentity,
  type GroupedKeyIdentityField,
} from "./grouped-key-identity";
export {
  missingSchemaValuePresenceToken,
  presentSchemaValuePresenceToken,
  schemaValuePresenceKey,
  type SchemaValuePresenceToken,
} from "./schema-value-presence";

const isNonTypedFailureReason = <E>(
  reason: Cause.Reason<E>,
): reason is Cause.Die | Cause.Interrupt =>
  Cause.isDieReason(reason) || Cause.isInterruptReason(reason);

export const ignoreLoggedTypedFailuresPreserveNonTypedFailures =
  (message: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<void, never, R> =>
    effect.pipe(
      Effect.asVoid,
      Effect.catchCause((cause) => {
        const typedFailures = cause.reasons.filter(Cause.isFailReason);
        const nonTypedFailures = cause.reasons.filter(isNonTypedFailureReason);
        const preservedCause = Cause.fromReasons<never>(nonTypedFailures);
        const logTypedFailures =
          typedFailures.length === 0
            ? Effect.void
            : Effect.logWarning(message, Cause.fromReasons(typedFailures));

        return nonTypedFailures.length === 0
          ? logTypedFailures
          : logTypedFailures.pipe(Effect.andThen(Effect.failCause(preservedCause)));
      }),
    );

export const runAllFinalizers = <E, R>(
  finalizers: ReadonlyArray<Effect.Effect<unknown, E, R>>,
): Effect.Effect<void, E, R> =>
  Effect.uninterruptible(
    Effect.forEach(finalizers, Effect.exit).pipe(
      Effect.andThen((exits) => {
        const combinedExit = Exit.asVoidAll(exits);
        return Exit.isSuccess(combinedExit) ? Effect.void : Effect.failCause(combinedExit.cause);
      }),
    ),
  );
