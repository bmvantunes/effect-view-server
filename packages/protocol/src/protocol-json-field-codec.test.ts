import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaGetter } from "effect";
import { encodeJsonFieldValue } from "./protocol-json-field-codec";

const EncodingFailure = Schema.String.pipe(
  Schema.encodeTo(Schema.String, {
    decode: SchemaGetter.passthrough(),
    encode: SchemaGetter.forbidden(() => "encoding is intentionally unavailable"),
  }),
);

describe("JSON field codec", () => {
  it.effect("reports an encoding failure for a value accepted by the schema", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        encodeJsonFieldValue(EncodingFailure, "value", {
          invalid: () => "invalid",
          notJsonSafe: () => "not-json-safe",
        }),
      );

      expect(error).toBe("invalid");
    }),
  );
});
