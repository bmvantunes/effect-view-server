import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  viewServerDecodeLiveEvent,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveEvent,
} from "./index";

import { BadJsonField, Order, viewServer } from "../test-harness/protocol";

describe("Invalid raw row wire inputs", () => {
  it.effect("rejects invalid raw row payloads", () =>
    Effect.gen(function* () {
      const idQuery = { select: ["id"] };

      const priceQuery = { select: ["price"] };

      const wrongEncodeTopic = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
          type: "status",
          topic: "badjson",
          queryId: "query-0",
          status: "ready",
          code: "Ready",
        }),
      );

      expect(wrongEncodeTopic.code).toBe("InvalidRow");

      const wrongDecodeTopic = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          idQuery,
          {
            type: "status",
            topic: "badjson",
            queryId: "query-0",
            status: "ready",
            code: "Ready",
          },
        ),
      );

      expect(wrongDecodeTopic.code).toBe("InvalidRow");

      const missingField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ price: 10 }],
          totalRows: 1,
        }),
      );

      expect(missingField.message).toBe("Missing row field for topic orders: id");

      const extraEncodeField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", price: 10 }],
          totalRows: 1,
        }),
      );

      expect(extraEncodeField.message).toBe("Unexpected row field for topic orders: price");

      const invalidEncodeFieldType = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", priceQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ price: "nope" }],
          totalRows: 1,
        }),
      );

      expect(invalidEncodeFieldType.code).toBe("InvalidRow");

      expect(invalidEncodeFieldType.message).toBe(
        'Invalid field price: Expected number, got "nope"',
      );

      const extraField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          idQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", price: 10 }],
            totalRows: 1,
          },
        ),
      );

      expect(extraField.message).toBe("Unexpected row field for topic orders: price");

      const missingDecodeField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          priceQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a" }],
            totalRows: 1,
          },
        ),
      );

      expect(missingDecodeField.message).toBe("Missing row field for topic orders: price");

      const invalidFieldType = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          priceQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ price: "nope" }],
            totalRows: 1,
          },
        ),
      );

      expect(invalidFieldType.code).toBe("InvalidRow");

      expect(invalidFieldType.message).toBe(
        'Invalid field price: Expected "Infinity" | "-Infinity" | "NaN", got "nope"',
      );

      const hostileRowSchema = Schema.Struct({ id: Schema.String });
      const hostileRowViewServer = defineViewServerConfig({
        topics: {
          badjson: {
            schema: hostileRowSchema,
            key: "id",
          },
        },
      });
      Object.defineProperty(hostileRowSchema.fields, "id", {
        configurable: true,
        enumerable: true,
        value: BadJsonField,
        writable: true,
      });
      const unsafeHostileRowViewServer = {
        ...hostileRowViewServer,
        topics: {
          badjson: {
            ...hostileRowViewServer.topics.badjson,
            schema: hostileRowSchema,
          },
        },
      };
      const nonJsonRow = yield* Effect.flip(
        viewServerEncodeLiveEvent(unsafeHostileRowViewServer, "badjson", idQuery, {
          type: "snapshot",
          topic: "badjson",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a" }],
          totalRows: 1,
        }),
      );

      expect(nonJsonRow.message).toBe(
        'Field id is not JSON-safe: Unsupported JSON value type "symbol" at $.',
      );

      const BadJsonAggregateRow = Schema.Struct({
        id: Schema.String,
        value: Schema.Number,
      });

      const badJsonAggregateViewServer = defineViewServerConfig({
        topics: {
          badAggregate: {
            schema: BadJsonAggregateRow,
            key: "id",
          },
        },
      });

      const badJsonAggregateQuery = yield* viewServerEncodeGroupedQuery(
        badJsonAggregateViewServer,
        "badAggregate",
        {
          groupBy: ["id"],
          aggregates: {
            badValue: { aggFunc: "min", field: "value" },
          },
        },
      );
      Object.defineProperty(BadJsonAggregateRow.fields, "value", {
        configurable: true,
        enumerable: true,
        value: BadJsonField,
        writable: true,
      });
      const unsafeBadJsonAggregateViewServer = {
        ...badJsonAggregateViewServer,
        topics: {
          badAggregate: {
            ...badJsonAggregateViewServer.topics.badAggregate,
            schema: BadJsonAggregateRow,
          },
        },
      };

      const nonJsonAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          unsafeBadJsonAggregateViewServer,
          "badAggregate",
          badJsonAggregateQuery,
          {
            type: "snapshot",
            topic: "badAggregate",
            queryId: "grouped-bad-aggregate",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", badValue: "not-json-safe" }],
            totalRows: 1,
          },
        ),
      );

      expect(nonJsonAggregate.code).toBe("InvalidRow");

      expect(nonJsonAggregate.message).toBe(
        'Field badValue is not JSON-safe: Unsupported JSON value type "symbol" at $.',
      );
    }),
  );
});
