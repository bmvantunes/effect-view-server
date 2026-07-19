import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaGetter } from "effect";
import { decodeRawQuery } from "./raw-query-decoder";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";

describe("raw query decoder", () => {
  it.effect("validates decoded scalar operands without re-encoding them", () =>
    Effect.gen(function* () {
      let encodeCalls = 0;
      const CountedString = Schema.String.pipe(
        Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.transform((value) => value),
          encode: SchemaGetter.transform((value) => {
            encodeCalls += 1;
            return value;
          }),
        }),
      );
      const Row = Schema.Struct({ value: CountedString });

      const decoded = yield* decodeRawQuery("rows", rawQueryCompilerMetadata(Row), {
        select: ["value"],
        where: [{ field: "value", type: "equals", filter: "alpha" }],
      });

      expect(decoded.select).toStrictEqual(["value"]);
      expect(decoded.where).toMatchObject({
        _tag: "condition",
        field: "value",
        type: "equals",
        filter: "alpha",
        caseSensitive: false,
        accentSensitive: false,
      });
      expect(encodeCalls).toBe(0);
    }),
  );

  it.effect("turns unexpected filter reflection failures into a stable query error", () =>
    Effect.gen(function* () {
      const Row = Schema.Struct({ value: Schema.String });
      const hostileWhere = new Proxy<Array<unknown>>([], {
        getPrototypeOf: () => {
          throw new Error("hostile prototype");
        },
      });
      const error = yield* Effect.flip(
        decodeRawQuery("rows", rawQueryCompilerMetadata(Row), {
          select: ["value"],
          where: hostileWhere,
        }),
      );

      expect(error.message).toBe("Raw query where contains an unsupported query value.");
    }),
  );
});
