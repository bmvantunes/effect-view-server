import { Effect, Result, Schema } from "effect";
import type { RowSchema } from "./query-core";

const decodedRowMakeOptions = {
  parseOptions: { onExcessProperty: "error" },
} as const;

class DecodedRowSnapshotError extends Schema.TaggedErrorClass<DecodedRowSnapshotError>()(
  "DecodedRowSnapshotError",
  { message: Schema.String },
) {}

const decodedRowSnapshotError = (message: string): DecodedRowSnapshotError =>
  DecodedRowSnapshotError.make({ message });

const snapshotDecodedRow = (
  schema: RowSchema,
  row: unknown,
): Result.Result<Record<string, unknown>, DecodedRowSnapshotError> => {
  if (typeof row !== "object" || row === null) {
    return Result.fail(decodedRowSnapshotError("Decoded row must be an object."));
  }
  const keys = Result.try({
    try: () => Reflect.ownKeys(row),
    catch: () => decodedRowSnapshotError("Could not inspect decoded row fields."),
  });
  if (Result.isFailure(keys)) {
    return Result.fail(keys.failure);
  }
  const snapshot: Record<string, unknown> = {};
  for (const key of keys.success) {
    if (typeof key !== "string" || !Object.hasOwn(schema.fields, key)) {
      return Result.fail(
        decodedRowSnapshotError(`Decoded row contains unknown field: ${String(key)}.`),
      );
    }
    const descriptor = Result.try({
      try: () => Object.getOwnPropertyDescriptor(row, key),
      catch: () => decodedRowSnapshotError(`Could not inspect decoded row field: ${key}.`),
    });
    if (Result.isFailure(descriptor)) {
      return Result.fail(descriptor.failure);
    }
    if (descriptor.success === undefined || !("value" in descriptor.success)) {
      return Result.fail(
        decodedRowSnapshotError(`Decoded row field must be a data property: ${key}.`),
      );
    }
    if (descriptor.success.enumerable !== true) {
      return Result.fail(decodedRowSnapshotError(`Decoded row field must be enumerable: ${key}.`));
    }
    Object.defineProperty(snapshot, key, {
      configurable: true,
      enumerable: true,
      value: descriptor.success.value,
      writable: true,
    });
  }
  return Result.succeed(snapshot);
};

export const validateDecodedRow = <S extends RowSchema>(schema: S, row: unknown) => {
  return Effect.fromResult(snapshotDecodedRow(schema, row)).pipe(
    Effect.flatMap((snapshot) => schema.makeEffect(snapshot, decodedRowMakeOptions)),
  );
};
