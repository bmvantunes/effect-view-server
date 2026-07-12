import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  viewServerDecodeLiveEvent,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveEvent,
} from "./index";

import { viewServer } from "../test-harness/protocol";

describe("Invalid grouped row wire inputs", () => {
  it.effect("rejects invalid grouped row and aggregate payloads", () =>
    Effect.gen(function* () {
      const groupedQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          averagePrice: { aggFunc: "avg", field: "price" },
        },
      });

      const missingGroupedField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [{ rowCount: 1n, averagePrice: BigDecimal.fromStringUnsafe("1.5") }],
          totalRows: 1,
        }),
      );

      expect(missingGroupedField.message).toBe("Missing grouped row field for topic orders: id");

      const missingGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", averagePrice: BigDecimal.fromStringUnsafe("1.5") }],
          totalRows: 1,
        }),
      );

      expect(missingGroupedAggregate.message).toBe(
        "Missing grouped aggregate for topic orders: rowCount",
      );

      const missingGroupedAggregateDefinition = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              // @ts-expect-error hostile query payload can omit an aggregate definition value.
              missing: undefined,
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", missing: 1n }],
            totalRows: 1,
          },
        ),
      );

      expect(missingGroupedAggregateDefinition.message).toBe(
        "Missing grouped aggregate definition for topic orders: missing",
      );

      const unexpectedGroupedField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: 1n,
              averagePrice: BigDecimal.fromStringUnsafe("1.5"),
              extra: true,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(unexpectedGroupedField.message).toBe(
        "Unexpected grouped row field for topic orders: extra",
      );

      const nonJsonGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: Symbol("bad"),
              averagePrice: BigDecimal.fromStringUnsafe("1.5"),
            },
          ],
          totalRows: 1,
        }),
      );

      expect(nonJsonGroupedAggregate.message).toBe("Aggregate rowCount must be a bigint.");

      const invalidBigDecimalGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: 1n,
              averagePrice: 1,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidBigDecimalGroupedAggregate.message).toBe(
        "Aggregate averagePrice must be a BigDecimal.",
      );

      const invalidGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: 10,
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(invalidGroupedField.message).toBe("Invalid field id: Expected string, got 10");

      const missingDecodedGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(missingDecodedGroupedField.message).toBe(
        "Missing grouped row field for topic orders: id",
      );

      const missingDecodedGroupedAggregate = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(missingDecodedGroupedAggregate.message).toBe(
        "Missing grouped aggregate for topic orders: rowCount",
      );

      const missingDecodedGroupedAggregateDefinition = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              // @ts-expect-error hostile query payload can omit an aggregate definition value.
              missing: undefined,
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                missing: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(missingDecodedGroupedAggregateDefinition.message).toBe(
        "Missing grouped aggregate definition for topic orders: missing",
      );

      const unexpectedDecodedGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
                extra: true,
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(unexpectedDecodedGroupedField.message).toBe(
        "Unexpected grouped row field for topic orders: extra",
      );

      const nonJsonDecodedGroupedAggregate = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "nope", value: "bad" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(nonJsonDecodedGroupedAggregate.message).toBe(
        "Aggregate rowCount must be a View Server aggregate envelope.",
      );

      const numericBigIntEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: 1 },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(numericBigIntEnvelope.message).toBe(
        "Aggregate rowCount must be a View Server aggregate envelope.",
      );

      const invalidGroupedBigInt = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "not-a-bigint" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(invalidGroupedBigInt.message).toBe("Aggregate rowCount must be a bigint envelope.");

      const invalidGroupedBigDecimal = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "nope" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(invalidGroupedBigDecimal.message).toMatch(/Invalid aggregate averagePrice/);

      const numericBigDecimalEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: 1 },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(numericBigDecimalEnvelope.message).toBe(
        "Aggregate averagePrice must be a View Server aggregate envelope.",
      );

      const wrongGroupedBigDecimalEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(wrongGroupedBigDecimalEnvelope.message).toBe(
        "Aggregate averagePrice must be a BigDecimal envelope.",
      );

      const groupedMinQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          minPrice: { aggFunc: "min", field: "price" },
        },
      });

      const invalidEncodedMin = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedMinQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-min",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", minPrice: "not-a-number" }],
          totalRows: 1,
        }),
      );

      expect(invalidEncodedMin.message).toBe(
        'Invalid field minPrice: Expected number, got "not-a-number"',
      );

      const wrongJsonEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedMinQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-min",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                minPrice: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(wrongJsonEnvelope.message).toBe(
        "Aggregate minPrice must be a JSON aggregate envelope.",
      );

      const invalidJsonAggregateValue = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedMinQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-min",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                minPrice: { _viewServerAggregate: "json", value: "not-a-number" },
              },
            ],
            totalRows: 1,
          },
        ),
      );

      expect(invalidJsonAggregateValue.message).toBe(
        'Invalid field minPrice: Expected "Infinity" | "-Infinity" | "NaN", got "not-a-number"',
      );

      const missingAggregateSourceField = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              badPrice: { aggFunc: "min", field: "missing" },
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-missing-field",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", badPrice: 1 }],
            totalRows: 1,
          },
        ),
      );

      expect(missingAggregateSourceField.message).toBe(
        "Aggregate references unknown field for topic orders: missing",
      );

      const malformedEventSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                group: Schema.String,
                value: "not-a-schema",
              },
            },
            key: "group",
          },
        },
      };

      const malformedEventSchemaSnapshot = yield* viewServerEncodeLiveEvent(
        // @ts-expect-error hostile config can have malformed aggregate field schemas.
        malformedEventSchemaConfig,
        "broken",
        {
          groupBy: ["group"],
          aggregates: {
            totalValue: { aggFunc: "sum", field: "value" },
          },
        },
        {
          type: "snapshot",
          topic: "broken",
          queryId: "grouped-malformed-schema",
          version: 1,
          keys: ["a"],
          rows: [
            {
              group: "a",
              totalValue: BigDecimal.fromStringUnsafe("1"),
            },
          ],
          totalRows: 1,
        },
      );

      expect(malformedEventSchemaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "broken",
        queryId: "grouped-malformed-schema",
        version: 1,
        keys: ["a"],
        rows: [
          {
            group: "a",
            totalValue: { _viewServerAggregate: "bigdecimal", value: "1" },
          },
        ],
        totalRows: 1,
      });

      const malformedEventAstSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                group: Schema.String,
                value: { ast: "not-a-schema-ast" },
              },
            },
            key: "group",
          },
        },
      };

      const malformedEventAstSchemaSnapshot = yield* viewServerEncodeLiveEvent(
        // @ts-expect-error hostile config can have malformed aggregate field schema ASTs.
        malformedEventAstSchemaConfig,
        "broken",
        {
          groupBy: ["group"],
          aggregates: {
            totalValue: { aggFunc: "sum", field: "value" },
          },
        },
        {
          type: "snapshot",
          topic: "broken",
          queryId: "grouped-malformed-schema-ast",
          version: 1,
          keys: ["a"],
          rows: [
            {
              group: "a",
              totalValue: BigDecimal.fromStringUnsafe("1"),
            },
          ],
          totalRows: 1,
        },
      );

      expect(malformedEventAstSchemaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "broken",
        queryId: "grouped-malformed-schema-ast",
        version: 1,
        keys: ["a"],
        rows: [
          {
            group: "a",
            totalValue: { _viewServerAggregate: "bigdecimal", value: "1" },
          },
        ],
        totalRows: 1,
      });
    }),
  );
});
