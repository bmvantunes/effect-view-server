import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { validateDecodedRow } from "./decoded-row-validation";
import { viewSchema } from "./view-schema";

const DecodedRow = Schema.Struct({
  id: Schema.String,
  amount: Schema.BigIntFromString,
});

const NestedDecodedRow = Schema.Struct({
  id: Schema.String,
  nested: Schema.Struct({
    amount: Schema.BigIntFromString,
  }),
});

class ConstructorNormalizedRow extends Schema.Class<ConstructorNormalizedRow>(
  "ConstructorNormalizedRow",
)({
  id: Schema.String,
  amount: Schema.BigIntFromString,
}) {}
viewSchema.admitClass(ConstructorNormalizedRow);
const constructorNormalizedRowMakeEffect =
  ConstructorNormalizedRow.makeEffect.bind(ConstructorNormalizedRow);
const makeConstructorNormalizedRow: typeof ConstructorNormalizedRow.makeEffect = (input, options) =>
  constructorNormalizedRowMakeEffect(input, options).pipe(
    Effect.map(
      (row) => new ConstructorNormalizedRow({ id: row.id.toUpperCase(), amount: row.amount }),
    ),
  );
Object.defineProperty(ConstructorNormalizedRow, "makeEffect", {
  value: makeConstructorNormalizedRow,
});

describe("decoded row validation", () => {
  it.effect("validates an owned data-property snapshot", () =>
    Effect.gen(function* () {
      const first = yield* validateDecodedRow(DecodedRow, { id: "first", amount: 1n });
      const second = yield* validateDecodedRow(DecodedRow, { id: "second", amount: 2n });

      expect({ first, second }).toStrictEqual({
        first: { id: "first", amount: 1n },
        second: { id: "second", amount: 2n },
      });
    }),
  );

  it.effect("preserves root Class constructor normalization", () =>
    Effect.gen(function* () {
      const validated = yield* validateDecodedRow(ConstructorNormalizedRow, {
        id: "normalized",
        amount: 1n,
      });

      expect(validated).toBeInstanceOf(ConstructorNormalizedRow);
      expect(validated).toStrictEqual(
        new ConstructorNormalizedRow({ id: "NORMALIZED", amount: 1n }),
      );
    }),
  );

  it.effect("reads a nested stateful accessor once before reconstructing an owned graph", () =>
    Effect.gen(function* () {
      let accessorReads = 0;
      const nested = {
        get amount() {
          accessorReads += 1;
          return accessorReads === 1 ? 1n : 2n;
        },
      };

      const validated = yield* validateDecodedRow(NestedDecodedRow, {
        id: "nested",
        nested,
      });

      expect(accessorReads).toBe(1);
      expect(validated).toStrictEqual({ id: "nested", nested: { amount: 1n } });
      expect(validated.nested === nested).toBe(false);
    }),
  );

  it.effect("rejects excess properties inside nested decoded values", () =>
    Effect.gen(function* () {
      const error = yield* validateDecodedRow(NestedDecodedRow, {
        id: "nested",
        nested: { amount: 1n, extra: true },
      }).pipe(Effect.flip);

      expect(String(error)).toBe(
        'SchemaError(Unexpected key with value true\n  at ["nested"]["extra"])',
      );
    }),
  );

  it.effect("rejects hostile descriptors without reading a stateful accessor", () =>
    Effect.gen(function* () {
      let accessorReads = 0;
      const accessorRow = { id: "accessor" };
      Object.defineProperty(accessorRow, "amount", {
        enumerable: true,
        get() {
          accessorReads += 1;
          return accessorReads === 1 ? 1n : 2n;
        },
      });
      const nonEnumerableRow = { id: "non-enumerable" };
      Object.defineProperty(nonEnumerableRow, "amount", {
        enumerable: false,
        value: 1n,
      });
      const symbol = Symbol("unknown");
      const ownKeysFailure = new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error("ownKeys exploded");
          },
        },
      );
      const descriptorFailure = new Proxy(
        {},
        {
          ownKeys: () => ["id"],
          getOwnPropertyDescriptor: () => {
            throw new Error("descriptor exploded");
          },
        },
      );
      const missingDescriptor = new Proxy(
        {},
        {
          ownKeys: () => ["id"],
          getOwnPropertyDescriptor: () => undefined,
        },
      );

      const errors = yield* Effect.forEach(
        [
          null,
          ownKeysFailure,
          { [symbol]: 1 },
          { unknown: 1 },
          descriptorFailure,
          missingDescriptor,
          accessorRow,
          nonEnumerableRow,
        ],
        (row) => validateDecodedRow(DecodedRow, row).pipe(Effect.flip),
      );

      expect(accessorReads).toBe(0);
      expect(errors.map(String)).toStrictEqual([
        "DecodedRowSnapshotError: Decoded row must be an object.",
        "DecodedRowSnapshotError: Could not inspect decoded row fields.",
        "DecodedRowSnapshotError: Decoded row contains unknown field: Symbol(unknown).",
        "DecodedRowSnapshotError: Decoded row contains unknown field: unknown.",
        "DecodedRowSnapshotError: Could not inspect decoded row field: id.",
        "DecodedRowSnapshotError: Decoded row field must be a data property: id.",
        "DecodedRowSnapshotError: Decoded row field must be a data property: amount.",
        "DecodedRowSnapshotError: Decoded row field must be enumerable: amount.",
      ]);
    }),
  );
});
