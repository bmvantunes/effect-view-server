import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  viewServerDecodeGroupedQuery,
  viewServerDecodeLiveQuery,
  viewServerDecodeRawQuery,
  viewServerDecodeTopic,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveQuery,
  viewServerEncodeRawQuery,
} from "./index";

import { BadJsonField, viewServer } from "../test-harness/protocol";

describe("Invalid query wire inputs", () => {
  it.effect("rejects non-record and hostile query shells without invoking accessors", () =>
    Effect.gen(function* () {
      const nullQuery = yield* Effect.flip(viewServerEncodeRawQuery(viewServer, "orders", null));
      expect(nullQuery.code).toBe("InvalidQuery");

      const hiddenQuery = { select: ["id"] };
      Object.defineProperty(hiddenQuery, "hidden", { enumerable: false, value: true });
      const accessorQuery = {};
      Object.defineProperty(accessorQuery, "select", {
        enumerable: true,
        get: () => ["id"],
      });
      const symbolicQuery = { select: ["id"] };
      Object.defineProperty(symbolicQuery, Symbol("metadata"), {
        enumerable: true,
        value: true,
      });
      const disappearingDescriptorQuery = new Proxy(
        { select: ["id"] },
        {
          getOwnPropertyDescriptor: () => undefined,
          ownKeys: () => ["select"],
        },
      );
      const revokedQuery = Proxy.revocable({ select: ["id"] }, {});
      revokedQuery.revoke();
      const prototypeFailureQuery = new Proxy(
        { select: ["id"] },
        {
          getPrototypeOf: () => {
            throw new Error("query prototype reflection failed");
          },
        },
      );
      const keysFailureQuery = new Proxy(
        { select: ["id"] },
        {
          ownKeys: () => {
            throw new Error("query key reflection failed");
          },
        },
      );
      const descriptorFailureQuery = new Proxy(
        { select: ["id"] },
        {
          getOwnPropertyDescriptor: () => {
            throw new Error("query descriptor reflection failed");
          },
        },
      );

      for (const query of [
        hiddenQuery,
        accessorQuery,
        symbolicQuery,
        disappearingDescriptorQuery,
        revokedQuery.proxy,
        prototypeFailureQuery,
        keysFailureQuery,
        descriptorFailureQuery,
      ]) {
        const error = yield* Effect.flip(viewServerEncodeRawQuery(viewServer, "orders", query));
        expect(error).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          message: "Query input could not be inspected",
          topic: "orders",
        });
      }
    }),
  );

  it.effect("captures each public live-query shell field once", () =>
    Effect.gen(function* () {
      const makeStatefulQuery = () => {
        let selectReads = 0;
        let whereReads = 0;
        const where: ReadonlyArray<unknown> = [];
        const query = new Proxy(
          { select: ["id"], where },
          {
            getOwnPropertyDescriptor: (target, key) => {
              const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
              if (descriptor === undefined) {
                return undefined;
              }
              if (key === "select") {
                selectReads += 1;
                return {
                  ...descriptor,
                  value: selectReads === 1 ? ["id"] : ["missing"],
                };
              }
              if (key === "where") {
                whereReads += 1;
                return {
                  ...descriptor,
                  value:
                    whereReads === 1
                      ? []
                      : [{ field: "missing", type: "equals", filter: "changed" }],
                };
              }
              return descriptor;
            },
          },
        );
        return {
          query,
          selectReads: () => selectReads,
          whereReads: () => whereReads,
        };
      };
      const encodeInput = makeStatefulQuery();
      const decodeInput = makeStatefulQuery();

      const encoded = yield* viewServerEncodeLiveQuery(viewServer, "orders", encodeInput.query);
      const decoded = yield* viewServerDecodeLiveQuery(viewServer, "orders", decodeInput.query);

      expect(encoded).toStrictEqual({ select: ["id"], where: [] });
      expect(decoded).toStrictEqual({ select: ["id"], where: [] });
      expect(encodeInput.selectReads()).toBe(1);
      expect(encodeInput.whereReads()).toBe(1);
      expect(decodeInput.selectReads()).toBe(1);
      expect(decodeInput.whereReads()).toBe(1);
    }),
  );

  it.effect("rejects decorated query arrays before schema decoding", () =>
    Effect.gen(function* () {
      const decoratedSelect = ["id"];
      const decoratedWhere: Array<unknown> = [];
      const decoratedGroupBy = ["id"];
      const decoratedOrderBy = [{ field: "id", direction: "asc" }];
      for (const value of [decoratedSelect, decoratedWhere, decoratedGroupBy, decoratedOrderBy]) {
        Object.defineProperty(value, "metadata", { enumerable: true, value: true });
      }

      const selectEncode = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", { select: decoratedSelect }),
      );
      const selectDecode = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", { select: decoratedSelect }),
      );
      const whereEncode = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: decoratedWhere,
        }),
      );
      const whereDecode = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: decoratedWhere,
        }),
      );
      const orderByEncode = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          orderBy: decoratedOrderBy,
        }),
      );
      const orderByDecode = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          orderBy: decoratedOrderBy,
        }),
      );
      const groupByEncode = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", {
          groupBy: decoratedGroupBy,
          aggregates: { rowCount: { aggFunc: "count" } },
        }),
      );
      const groupByDecode = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", {
          groupBy: decoratedGroupBy,
          aggregates: { rowCount: { aggFunc: "count" } },
        }),
      );

      expect(selectEncode.message).toBe(
        "Query select must be a dense array without extra properties",
      );
      expect(selectDecode.message).toBe(
        "Query select must be a dense array without extra properties",
      );
      expect(whereEncode.message).toBe(
        "Query where must be a dense array without extra properties",
      );
      expect(whereDecode.message).toBe(
        "Query where must be a dense array without extra properties",
      );
      expect(orderByEncode.message).toBe(
        "Query orderBy must be a dense array without extra properties",
      );
      expect(orderByDecode.message).toBe(
        "Query orderBy must be a dense array without extra properties",
      );
      expect(groupByEncode.message).toBe(
        "Query groupBy must be a dense array without extra properties",
      );
      expect(groupByDecode.message).toBe(
        "Query groupBy must be a dense array without extra properties",
      );
    }),
  );

  it.effect("rejects invalid topics, query shapes, and filters", () =>
    Effect.gen(function* () {
      const missingTopic = yield* Effect.flip(viewServerDecodeTopic(viewServer, "missing"));

      expect(missingTopic.code).toBe("InvalidTopic");

      const invalidEncodeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still encode unknown topics.
        viewServerEncodeRawQuery(viewServer, "missing", { select: ["id"] }),
      );

      expect(invalidEncodeTopic.code).toBe("InvalidTopic");

      const queryCases: ReadonlyArray<readonly [unknown, string]> = [
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
          { select: ["id"], where: [{ field: "missing", type: "equals", filter: "x" }] },
          "Query references an unknown or non-filterable field: missing",
        ],
        [
          { select: ["id"], orderBy: [{ field: "missing", direction: "asc" }] },
          "Query references an unknown field for topic: orders",
        ],
      ];

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

      const prototypeKeyQuery = { select: ["id"] };
      Object.defineProperty(prototypeKeyQuery, "__proto__", {
        enumerable: true,
        value: null,
      });
      const prototypeKeyEncode = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", prototypeKeyQuery),
      );
      const prototypeKeyDecode = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", prototypeKeyQuery),
      );

      expect(prototypeKeyEncode.code).toBe("InvalidQuery");
      expect(prototypeKeyDecode.code).toBe("InvalidQuery");
    }),
  );

  it.effect("rejects invalid grouped query shapes", () =>
    Effect.gen(function* () {
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

      const groupedQueryCases: ReadonlyArray<readonly [unknown, string]> = [
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
            where: [{ field: "missing", type: "equals", filter: "x" }],
          },
          "Query references an unknown or non-filterable field: missing",
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
      ];

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
    }),
  );

  it.effect("rejects invalid filter operators and values", () =>
    Effect.gen(function* () {
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
          where: [{ field: "price", type: "greaterThan", filter: "nope" }],
        }),
      );

      expect(invalidFilter.code).toBe("InvalidQuery");

      expect(invalidFilter.message).toBe('Invalid filter for price: Expected number, got "nope"');

      const invalidEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "price", type: "startsWith", filter: 1 }],
        }),
      );

      expect(invalidEncodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidStringStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "id", type: "startsWith", filter: 1 }],
        }),
      );

      expect(invalidStringStartsWith.message).toBe(
        "Filter condition id startsWith requires a string",
      );

      const invalidDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "price", type: "startsWith", filter: 1 }],
        }),
      );

      expect(invalidDecodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidDecodedStringStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "id", type: "startsWith", filter: 1 }],
        }),
      );

      expect(invalidDecodedStringStartsWith.message).toBe(
        "Filter condition id startsWith requires a string",
      );

      const numericTextOptions = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "price",
              type: "equals",
              filter: 1,
              caseSensitive: true,
            },
          ],
        }),
      );
      const nonBooleanTextOption = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "id",
              type: "equals",
              filter: "a",
              accentSensitive: "yes",
            },
          ],
        }),
      );
      const inValues = ["a"];
      Object.defineProperty(inValues, "extra", {
        configurable: true,
        enumerable: true,
        value: "b",
        writable: true,
      });
      const decoratedIn = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "id", type: "in", filter: inValues }],
        }),
      );

      expect(numericTextOptions.message).toBe("Filter condition price has invalid keys");
      expect(nonBooleanTextOption.message).toBe(
        "Filter condition id accentSensitive must be a boolean",
      );
      expect(decoratedIn.message).toBe("Filter condition id in must be an array");
    }),
  );

  it.effect("preserves text-search operands for supported string schemas", () =>
    Effect.gen(function* () {
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
          where: [{ field: "id", type: "startsWith", filter: "  abc  " }],
        },
      );

      expect(encodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: [{ field: "id", type: "startsWith", filter: "  abc  " }],
      });

      const decodedTrimmedStartsWith = yield* viewServerDecodeRawQuery(
        trimmedViewServer,
        "trimmed",
        {
          select: ["id"],
          where: [{ field: "id", type: "startsWith", filter: "  abc  " }],
        },
      );

      expect(decodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: [{ field: "id", type: "startsWith", filter: "  abc  " }],
      });
    }),
  );

  it.effect("rejects non-string text-search operands", () =>
    Effect.gen(function* () {
      const badJsonStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: [{ field: "id", type: "startsWith", filter: 1 }],
        }),
      );

      expect(badJsonStartsWith.message).toBe("Filter condition id startsWith requires a string");
    }),
  );

  it.effect("preserves text-search operands for refined strings", () =>
    Effect.gen(function* () {
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
          where: [{ field: "id", type: "startsWith", filter: "x" }],
        },
      );

      expect(encodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: [{ field: "id", type: "startsWith", filter: "x" }],
      });

      const decodedRefinedStartsWith = yield* viewServerDecodeRawQuery(
        refinedStringViewServer,
        "refined",
        {
          select: ["id"],
          where: [{ field: "id", type: "startsWith", filter: "x" }],
        },
      );

      expect(decodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: [{ field: "id", type: "startsWith", filter: "x" }],
      });
    }),
  );

  it.effect("accepts text search for literal string fields", () =>
    Effect.gen(function* () {
      const encodedLiteralStartsWith = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["status"],
        where: [{ field: "status", type: "startsWith", filter: "op" }],
      });

      expect(encodedLiteralStartsWith).toStrictEqual({
        select: ["status"],
        where: [{ field: "status", type: "startsWith", filter: "op" }],
      });
    }),
  );

  it.effect("rejects structured and non-JSON filter operands", () =>
    Effect.gen(function* () {
      const structuredEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "metadata",
              type: "startsWith",
              filter: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          ],
        }),
      );

      expect(structuredEncodeStartsWith.message).toBe(
        "Query references an unknown or non-filterable field: metadata",
      );

      const structuredDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "metadata",
              type: "startsWith",
              filter: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          ],
        }),
      );

      expect(structuredDecodeStartsWith.message).toBe(
        "Query references an unknown or non-filterable field: metadata",
      );

      const structuredEncodeRange = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "metadata",
              type: "greaterThan",
              filter: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          ],
        }),
      );

      expect(structuredEncodeRange.message).toBe(
        "Query references an unknown or non-filterable field: metadata",
      );

      const structuredDecodeRange = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [
            {
              field: "metadata",
              type: "greaterThan",
              filter: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          ],
        }),
      );

      expect(structuredDecodeRange.message).toBe(
        "Query references an unknown or non-filterable field: metadata",
      );

      const invalidRangeOperator = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "id", type: "greaterThan", filter: "a" }],
        }),
      );

      expect(invalidRangeOperator.message).toBe("Filter id does not support range operators");

      const hostileFilterSchema = Schema.Struct({ id: Schema.String });
      const hostileFilterViewServer = defineViewServerConfig({
        topics: {
          badjson: {
            schema: hostileFilterSchema,
            key: "id",
          },
        },
      });
      Object.defineProperty(hostileFilterSchema.fields, "id", {
        configurable: true,
        enumerable: true,
        value: BadJsonField,
        writable: true,
      });
      const unsafeHostileFilterViewServer = {
        ...hostileFilterViewServer,
        topics: {
          badjson: {
            ...hostileFilterViewServer.topics.badjson,
            schema: hostileFilterSchema,
          },
        },
      };
      const nonJsonFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(unsafeHostileFilterViewServer, "badjson", {
          select: ["id"],
          where: [{ field: "id", type: "equals", filter: "x" }],
        }),
      );

      expect(nonJsonFilter.message).toBe(
        'Filter id is not JSON-safe: Unsupported JSON value type "symbol" at $.',
      );

      const badDecodedField = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: [{ field: "price", type: "greaterThan", filter: "nope" }],
        }),
      );

      expect(badDecodedField.code).toBe("InvalidQuery");
    }),
  );
});
