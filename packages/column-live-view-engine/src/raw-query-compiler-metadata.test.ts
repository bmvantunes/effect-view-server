import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine, InvalidRowError } from "./index";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";

describe("Raw query compiler metadata", () => {
  it.effect("rejects malformed engine schemas while metadata inspection remains defensive", () =>
    Effect.gen(function* () {
      const malformedFieldSchemaConfig = {
        topics: {
          loose: {
            schema: {
              fields: {
                id: "not-a-schema",
                label: { ast: "not-a-schema-ast" },
              },
            },
            key: "id",
          },
        },
      };
      const configError = yield* Effect.flip(
        // @ts-expect-error invalid configs can still reach runtime through untyped callers.
        createColumnLiveViewEngine(malformedFieldSchemaConfig),
      );

      expect(configError).toBeInstanceOf(InvalidRowError);
      expect(configError).toMatchObject({
        _tag: "InvalidRowError",
        topic: "loose",
        message: "Topic row schema must be an Effect Schema Struct.",
      });

      const metadata = rawQueryCompilerMetadata({
        // @ts-expect-error hostile schema metadata can contain malformed field entries.
        fields: {
          id: "not-a-schema",
          invalidAst: { ast: "not-an-effect-ast" },
          plainAst: { ast: Schema.String.ast },
          price: Schema.Number,
        },
      });
      expect(metadata.fieldOrder).toStrictEqual(["id", "invalidAst", "plainAst", "price"]);
      expect(metadata.fieldNames.has("id")).toBe(true);
      expect(metadata.exactScalarEqualityFieldNames.has("invalidAst")).toBe(false);
      expect(metadata.exactScalarEqualityFieldNames.has("plainAst")).toBe(true);
      expect(metadata.numericFieldNames.has("id")).toBe(false);
      expect(metadata.numericFieldNames.has("price")).toBe(true);

      // @ts-expect-error hostile callers can still pass a non-Struct schema.
      const nonStructMetadata = rawQueryCompilerMetadata(Schema.String);
      expect({
        fields: [...nonStructMetadata.fieldNames],
        ranges: [...nonStructMetadata.rangeValueKinds],
        strings: [...nonStructMetadata.stringFieldNames],
        structured: [...nonStructMetadata.structuredFieldNames],
        structuredObjects: [...nonStructMetadata.structuredObjectFieldNames],
      }).toStrictEqual({
        fields: [],
        ranges: [],
        strings: [],
        structured: [],
        structuredObjects: [],
      });
    }),
  );
});
