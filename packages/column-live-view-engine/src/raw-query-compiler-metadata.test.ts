import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { createColumnLiveViewEngine } from "./index";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";

describe("Raw query compiler metadata", () => {
  it.effect("keeps runtime guards for malformed schema field metadata", () =>
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
      // @ts-expect-error invalid configs can still reach runtime through untyped callers.
      const engine = yield* createColumnLiveViewEngine(malformedFieldSchemaConfig);
      const query: object = { select: ["id"] };

      const snapshot = yield* engine.snapshot(
        "loose",
        // @ts-expect-error hostile untyped runtime query is still handled by runtime guards.
        query,
      );

      expect(snapshot).toMatchObject({
        rows: [],
        totalRows: 0,
      });

      const metadata = rawQueryCompilerMetadata({
        // @ts-expect-error hostile schema metadata can contain malformed field entries.
        fields: {
          id: "not-a-schema",
          price: Schema.Number,
        },
      });
      expect(metadata.fieldOrder).toStrictEqual(["id", "price"]);
      expect(metadata.fieldNames.has("id")).toBe(true);
      expect(metadata.numericFieldNames.has("id")).toBe(false);
      expect(metadata.numericFieldNames.has("price")).toBe(true);

      const invalidNumericAggregateQuery: object = {
        groupBy: ["label"],
        aggregates: {
          totalId: { aggFunc: "sum", field: "id" },
        },
      };
      const invalidNumericAggregate = yield* Effect.flip(
        engine.snapshot(
          "loose",
          // @ts-expect-error malformed schema metadata makes the query shape untyped.
          invalidNumericAggregateQuery,
        ),
      );
      expect(invalidNumericAggregate).toMatchObject({
        _tag: "InvalidQueryError",
        topic: "loose",
        message: "Grouped query aggregate totalId must reference a numeric field.",
      });
    }),
  );
});
