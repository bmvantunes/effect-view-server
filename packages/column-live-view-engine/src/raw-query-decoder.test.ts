import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaGetter } from "effect";
import { decodeRawQuery } from "./raw-query-decoder";
import { rawQueryCompilerMetadata } from "./raw-query-metadata";

describe("raw query decoder", () => {
  it.effect("materializes each where operand through its field schema exactly once", () =>
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
        where: { value: { eq: "alpha" } },
      });

      expect(decoded).toStrictEqual({
        select: ["value"],
        where: { value: { eq: "alpha" } },
      });
      expect(encodeCalls).toBe(1);
    }),
  );
});
