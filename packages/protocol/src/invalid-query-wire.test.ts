import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  viewServerDecodeGroupedQuery,
  viewServerDecodeLiveQuery,
  viewServerDecodeRawQuery,
  viewServerDecodeTopic,
  viewServerEncodeGroupedQuery,
  viewServerEncodeRawQuery,
} from "./index";

import { viewServer } from "../test-harness/protocol";

describe("Invalid query wire inputs", () => {
  it.effect("rejects invalid topics, query shapes, and filters", () =>
    Effect.gen(function* () {
      const missingTopic = yield* Effect.flip(viewServerDecodeTopic(viewServer, "missing"));

      expect(missingTopic.code).toBe("InvalidTopic");

      const invalidEncodeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still encode unknown topics.
        viewServerEncodeRawQuery(viewServer, "missing", { select: ["id"] }),
      );

      expect(invalidEncodeTopic.code).toBe("InvalidTopic");

      const queryCases = [
        [{ select: [] }, "Query select must include at least one field"],
        [{ select: ["id"], offset: -1 }, "Query offset must be a non-negative integer"],
        [
          { select: ["id"], offset: Number.MAX_SAFE_INTEGER + 1 },
          "Query offset must be a non-negative integer",
        ],
        [{ select: ["id"], limit: -1 }, "Query limit must be a non-negative integer"],
        [
          { select: ["id"], limit: Number.MAX_SAFE_INTEGER + 1 },
          "Query limit must be a non-negative integer",
        ],
        [{ select: ["missing"] }, "Query references an unknown field for topic: orders"],
        [
          { select: ["id"], where: { missing: "x" } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { select: ["id"], orderBy: [{ field: "missing", direction: "asc" }] },
          "Query references an unknown field for topic: orders",
        ],
      ] as const;

      for (const [query, message] of queryCases) {
        const encodeError = yield* Effect.flip(
          viewServerEncodeRawQuery(viewServer, "orders", query),
        );
        expect(encodeError.code).toBe("InvalidQuery");
        expect(encodeError.message).toBe(message);

        const decodeError = yield* Effect.flip(
          viewServerDecodeRawQuery(viewServer, "orders", query),
        );
        expect(decodeError.code).toBe("InvalidQuery");
        expect(decodeError.message).toBe(message);
      }

      const extraKey = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", { select: ["id"], whre: {} }),
      );

      expect(extraKey.code).toBe("InvalidQuery");

      const decodeExtraKey = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", { select: ["id"], whre: {} }),
      );

      expect(decodeExtraKey.code).toBe("InvalidQuery");

      const malformedGroupedEncode = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          typo: true,
        }),
      );

      expect(malformedGroupedEncode.code).toBe("InvalidQuery");

      const malformedGroupedDecode = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          typo: true,
        }),
      );

      expect(malformedGroupedDecode.code).toBe("InvalidQuery");

      const groupedQueryCases = [
        [
          { groupBy: [], aggregates: { rowCount: { aggFunc: "count" } } },
          "Grouped query groupBy must include at least one field",
        ],
        [
          { groupBy: ["id"], aggregates: {} },
          "Grouped query aggregates must include at least one aggregate",
        ],
        [
          { groupBy: ["missing"], aggregates: { rowCount: { aggFunc: "count" } } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { groupBy: ["id"], aggregates: { id: { aggFunc: "count" } } },
          "Aggregate alias collides with groupBy field: id",
        ],
        [
          { groupBy: ["id"], aggregates: { constructor: { aggFunc: "count" } } },
          "Grouped aggregate alias is not allowed: constructor",
        ],
        [
          { groupBy: ["id"], aggregates: { total: { aggFunc: "sum", field: "missing" } } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { groupBy: ["id"], aggregates: { total: { aggFunc: "sum", field: "id" } } },
          "Grouped aggregate total must reference a numeric field",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            where: { missing: "x" },
          },
          "Query references an unknown field for topic: orders",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "price", direction: "asc" }],
          },
          "Grouped orderBy field is not in groupBy: price",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ aggregate: "missing", direction: "asc" }],
          },
          "Grouped orderBy aggregate is not defined: missing",
        ],
        [
          { groupBy: ["id"], aggregates: { rowCount: { aggFunc: "count" } }, offset: -1 },
          "Query offset must be a non-negative integer",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            offset: Number.MAX_SAFE_INTEGER + 1,
          },
          "Query offset must be a non-negative integer",
        ],
        [
          { groupBy: ["id"], aggregates: { rowCount: { aggFunc: "count" } }, limit: -1 },
          "Query limit must be a non-negative integer",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            limit: Number.MAX_SAFE_INTEGER + 1,
          },
          "Query limit must be a non-negative integer",
        ],
      ] as const;

      for (const [query, message] of groupedQueryCases) {
        const encodeError = yield* Effect.flip(
          viewServerEncodeGroupedQuery(viewServer, "orders", query),
        );
        expect(encodeError.code).toBe("InvalidQuery");
        expect(encodeError.message).toBe(message);

        const decodeError = yield* Effect.flip(
          viewServerDecodeGroupedQuery(viewServer, "orders", query),
        );
        expect(decodeError.code).toBe("InvalidQuery");
        expect(decodeError.message).toBe(message);
      }

      const malformedSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                id: { ast: "not-a-schema-ast" },
              },
            },
            key: "id",
          },
        },
      };

      const malformedSchemaNumericField = yield* Effect.flip(
        // @ts-expect-error hostile config can have malformed field schemas.
        viewServerEncodeGroupedQuery(malformedSchemaConfig, "broken", {
          groupBy: ["id"],
          aggregates: { total: { aggFunc: "sum", field: "id" } },
        }),
      );

      expect(malformedSchemaNumericField.message).toBe(
        "Grouped aggregate total must reference a numeric field",
      );

      const malformedPrimitiveSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                id: "not-a-schema",
              },
            },
            key: "id",
          },
        },
      };

      const primitiveSchemaNumericField = yield* Effect.flip(
        // @ts-expect-error hostile config can have primitive field schemas.
        viewServerEncodeGroupedQuery(malformedPrimitiveSchemaConfig, "broken", {
          groupBy: ["id"],
          aggregates: { total: { aggFunc: "sum", field: "id" } },
        }),
      );

      expect(primitiveSchemaNumericField.message).toBe(
        "Grouped aggregate total must reference a numeric field",
      );

      const invalidGroupedEncodeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still encode unknown topics.
        viewServerEncodeGroupedQuery(viewServer, "missing", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
        }),
      );

      expect(invalidGroupedEncodeTopic.code).toBe("InvalidTopic");

      const liveGroupedDecodeError = yield* Effect.flip(
        viewServerDecodeLiveQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          orderBy: [{ aggregate: "missing", direction: "asc" }],
        }),
      );

      expect(liveGroupedDecodeError.message).toBe(
        "Grouped orderBy aggregate is not defined: missing",
      );

      const invalidFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );

      expect(invalidFilter.code).toBe("InvalidQuery");

      expect(invalidFilter.message).toBe('Invalid filter for price: Expected number, got "nope"');

      const invalidEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { startsWith: 1 } },
        }),
      );

      expect(invalidEncodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidStringStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );

      expect(invalidStringStartsWith.message).toBe("Invalid filter for id: expected string");

      const invalidDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { startsWith: 1 } },
        }),
      );

      expect(invalidDecodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidDecodedStringStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );

      expect(invalidDecodedStringStartsWith.message).toBe("Invalid filter for id: expected string");

      const trimmedViewServer = defineViewServerConfig({
        topics: {
          trimmed: {
            schema: Schema.Struct({
              id: Schema.Trim,
            }),
            key: "id",
          },
        },
      });

      const encodedTrimmedStartsWith = yield* viewServerEncodeRawQuery(
        trimmedViewServer,
        "trimmed",
        {
          select: ["id"],
          where: { id: { startsWith: "  abc  " } },
        },
      );

      expect(encodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "  abc  " } },
      });

      const decodedTrimmedStartsWith = yield* viewServerDecodeRawQuery(
        trimmedViewServer,
        "trimmed",
        {
          select: ["id"],
          where: { id: { startsWith: "  abc  " } },
        },
      );

      expect(decodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "  abc  " } },
      });

      const badJsonStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );

      expect(badJsonStartsWith.message).toBe("Invalid filter for id: expected string");

      const refinedStringViewServer = defineViewServerConfig({
        topics: {
          refined: {
            schema: Schema.Struct({
              id: Schema.String.check(Schema.isMinLength(2)),
            }),
            key: "id",
          },
        },
      });

      const encodedRefinedStartsWith = yield* viewServerEncodeRawQuery(
        refinedStringViewServer,
        "refined",
        {
          select: ["id"],
          where: { id: { startsWith: "x" } },
        },
      );

      expect(encodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "x" } },
      });

      const decodedRefinedStartsWith = yield* viewServerDecodeRawQuery(
        refinedStringViewServer,
        "refined",
        {
          select: ["id"],
          where: { id: { startsWith: "x" } },
        },
      );

      expect(decodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "x" } },
      });

      const encodedLiteralStartsWith = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["status"],
        where: { status: { startsWith: "op" } },
      });

      expect(encodedLiteralStartsWith).toStrictEqual({
        select: ["status"],
        where: { status: { startsWith: "op" } },
      });

      const structuredEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              startsWith: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );

      expect(structuredEncodeStartsWith.message).toBe(
        "Filter metadata does not support startsWith",
      );

      const structuredDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              startsWith: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );

      expect(structuredDecodeStartsWith.message).toBe(
        "Filter metadata does not support startsWith",
      );

      const structuredEncodeRange = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              gt: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );

      expect(structuredEncodeRange.message).toBe(
        "Filter metadata does not support range operators",
      );

      const structuredDecodeRange = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              gt: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );

      expect(structuredDecodeRange.message).toBe(
        "Filter metadata does not support range operators",
      );

      const invalidRangeOperator = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { gt: "a" } },
        }),
      );

      expect(invalidRangeOperator.message).toBe("Filter id does not support range operators");

      const nonJsonFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { eq: "x" } },
        }),
      );

      expect(nonJsonFilter.message).toBe(
        "Filter id is not JSON-safe: Expected JSON value, got Symbol(not-json)",
      );

      const badDecodedField = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );

      expect(badDecodedField.code).toBe("InvalidQuery");
    }),
  );
});
