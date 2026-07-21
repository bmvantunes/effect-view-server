import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  compileViewServerLiveEventCodec,
  viewServerDecodeGroupedQuery,
  viewServerDecodeLiveEvent,
  viewServerDecodeLiveQuery,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveEvent,
  viewServerEncodeLiveQuery,
} from "./index";

import {
  formatDecodedDecimal,
  nonOwnTopicRowFields,
  unknownTopicRowFieldError,
  viewServer,
} from "../test-harness/protocol";

describe("Grouped live wire codec", () => {
  it.effect("compiles and reuses one grouped row contract across live events", () =>
    Effect.gen(function* () {
      const aggregateDefinitions = {
        rowCount: { aggFunc: "count" as const },
      };
      const query = {
        aggregates: aggregateDefinitions,
        groupBy: ["id"],
      };
      const codec = compileViewServerLiveEventCodec<
        typeof viewServer.topics,
        "orders",
        { readonly id: string; readonly rowCount: bigint }
      >(viewServer, "orders", query);
      query.groupBy.push("status");
      Object.defineProperty(query.aggregates, "newCount", {
        configurable: true,
        enumerable: true,
        value: { aggFunc: "count" },
      });
      Object.defineProperty(query.aggregates.rowCount, "aggFunc", {
        configurable: true,
        enumerable: true,
        value: "sum",
      });
      Object.defineProperty(query.aggregates.rowCount, "field", {
        configurable: true,
        enumerable: true,
        value: "price",
      });

      const first = yield* codec.encode({
        type: "snapshot",
        topic: "orders",
        queryId: "compiled-grouped",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", rowCount: 1n }],
        totalRows: 1,
      });
      const second = yield* codec.encode({
        type: "delta",
        topic: "orders",
        queryId: "compiled-grouped",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "a", row: { id: "a", rowCount: 2n }, index: 0 }],
        totalRows: 1,
      });
      const decodedFirst = yield* codec.decodeTrusted(first);
      const decodedSecond = yield* codec.decodeTrusted(second);

      expect(decodedFirst).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "compiled-grouped",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", rowCount: 1n }],
        totalRows: 1,
      });
      expect(decodedSecond).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "compiled-grouped",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "a", row: { id: "a", rowCount: 2n }, index: 0 }],
        totalRows: 1,
      });
    }),
  );

  it.effect("encodes and decodes grouped query and grouped live event operations", () =>
    Effect.gen(function* () {
      const groupedQuery = {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minPrice: { aggFunc: "min", field: "price" },
          maxPrice: { aggFunc: "max", field: "price" },
          distinctPrice: { aggFunc: "countDistinct", field: "price" },
        },
        where: [
          { field: "id", type: "startsWith", filter: "a" },
          { field: "price", type: "in", filter: [10, 11] },
          { field: "price", type: "greaterThanOrEqual", filter: 10 },
        ],
        orderBy: [
          { field: "id", direction: "asc" },
          { aggregate: "totalPrice", direction: "desc" },
        ],
        offset: 0,
        limit: 10,
      };

      const encodedGrouped = yield* viewServerEncodeGroupedQuery(
        viewServer,
        "orders",
        groupedQuery,
      );
      expect(encodedGrouped).toStrictEqual(groupedQuery);

      const minimalGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      };
      const encodedMinimalGrouped = yield* viewServerEncodeGroupedQuery(
        viewServer,
        "orders",
        minimalGroupedQuery,
      );
      expect(encodedMinimalGrouped).toStrictEqual(minimalGroupedQuery);

      const encodedDecimalGrouped = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          totalDecimalPrice: { aggFunc: "sum", field: "decimalPrice" },
        },
      });
      expect(encodedDecimalGrouped).toStrictEqual({
        groupBy: ["id"],
        aggregates: {
          totalDecimalPrice: { aggFunc: "sum", field: "decimalPrice" },
        },
      });

      const optionalGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          totalOptionalPrice: { aggFunc: "sum", field: "optionalPrice" },
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      };
      const optionalGroupedEncodeError = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(optionalGroupedEncodeError.code).toBe("InvalidQuery");
      expect(optionalGroupedEncodeError.message).toBe(
        "Grouped aggregate totalOptionalPrice must reference a numeric field",
      );
      const optionalGroupedDecodeError = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(optionalGroupedDecodeError.code).toBe("InvalidQuery");
      expect(optionalGroupedDecodeError.message).toBe(
        "Grouped aggregate totalOptionalPrice must reference a numeric field",
      );

      const optionalBigIntGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      };
      const optionalBigIntGroupedError = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", optionalBigIntGroupedQuery),
      );
      expect(optionalBigIntGroupedError.code).toBe("InvalidQuery");
      expect(optionalBigIntGroupedError.message).toBe(
        "Grouped aggregate totalOptionalQuantity must reference a numeric field",
      );

      const decodedGrouped = yield* viewServerDecodeGroupedQuery(
        viewServer,
        "orders",
        encodedGrouped,
      );
      expect(decodedGrouped).toStrictEqual(groupedQuery);

      const decodedMinimalGrouped = yield* viewServerDecodeGroupedQuery(
        viewServer,
        "orders",
        encodedMinimalGrouped,
      );
      expect(decodedMinimalGrouped).toStrictEqual(minimalGroupedQuery);

      const decodedOptionalBigIntGroupedError = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", optionalBigIntGroupedQuery),
      );
      expect(decodedOptionalBigIntGroupedError.code).toBe("InvalidQuery");
      expect(decodedOptionalBigIntGroupedError.message).toBe(
        "Grouped aggregate totalOptionalQuantity must reference a numeric field",
      );

      const encodedLiveGrouped = yield* viewServerEncodeLiveQuery(
        viewServer,
        "orders",
        groupedQuery,
      );
      expect(encodedLiveGrouped).toStrictEqual(groupedQuery);

      const encodedLiveRaw = yield* viewServerEncodeLiveQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(encodedLiveRaw).toStrictEqual({ select: ["id"] });

      const decodedLiveGrouped = yield* viewServerDecodeLiveQuery(
        viewServer,
        "orders",
        encodedLiveGrouped,
      );
      expect(decodedLiveGrouped).toStrictEqual(groupedQuery);

      const decodedLiveRaw = yield* viewServerDecodeLiveQuery(viewServer, "orders", encodedLiveRaw);
      expect(decodedLiveRaw).toStrictEqual({ select: ["id"] });

      const invalidOptionalLiveQuery = yield* Effect.flip(
        viewServerEncodeLiveQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(invalidOptionalLiveQuery.code).toBe("InvalidQuery");

      const groupedRow = {
        id: "a",
        rowCount: 2n,
        totalPrice: BigDecimal.fromStringUnsafe("21"),
        averagePrice: BigDecimal.fromStringUnsafe("10.5"),
        minPrice: 10,
        maxPrice: 11,
        distinctPrice: 2n,
      };

      const groupedSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        encodedGrouped,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );
      expect(groupedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-0",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            rowCount: { _viewServerAggregate: "bigint", value: "2" },
            totalPrice: { _viewServerAggregate: "bigdecimal", value: "21" },
            averagePrice: { _viewServerAggregate: "bigdecimal", value: "10.5" },
            minPrice: { _viewServerAggregate: "json", value: 10 },
            maxPrice: { _viewServerAggregate: "json", value: 11 },
            distinctPrice: { _viewServerAggregate: "bigint", value: "2" },
          },
        ],
        totalRows: 1,
      });

      const decodedGroupedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof groupedRow
      >(viewServer, "orders", encodedGrouped, groupedSnapshot);
      const decodedGroupedSnapshotRows =
        decodedGroupedSnapshot.type === "snapshot"
          ? decodedGroupedSnapshot.rows.map((row) => ({
              ...row,
              totalPrice: formatDecodedDecimal(row.totalPrice),
              averagePrice: formatDecodedDecimal(row.averagePrice),
            }))
          : [];
      expect({
        ...decodedGroupedSnapshot,
        rows: decodedGroupedSnapshotRows,
      }).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-0",
        version: 1,
        keys: ["a"],
        rows: [{ ...groupedRow, totalPrice: "21", averagePrice: "10.5" }],
        totalRows: 1,
      });

      const invalidMinSnapshot = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", encodedGrouped, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-undefined",
          version: 1,
          keys: ["a"],
          rows: [{ ...groupedRow, minPrice: undefined }],
          totalRows: 1,
        }),
      );
      expect(invalidMinSnapshot).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        topic: "orders",
        message:
          "Invalid field minPrice: aggregate min cannot be undefined because price is required.",
      });
      const invalidMinEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent(viewServer, "orders", encodedGrouped, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-undefined-envelope",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: { _viewServerAggregate: "bigint", value: "2" },
              totalPrice: { _viewServerAggregate: "bigdecimal", value: "21" },
              averagePrice: { _viewServerAggregate: "bigdecimal", value: "10.5" },
              minPrice: { _viewServerAggregate: "undefined" },
              maxPrice: { _viewServerAggregate: "json", value: 11 },
              distinctPrice: { _viewServerAggregate: "bigint", value: "2" },
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidMinEnvelope).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidRow",
        topic: "orders",
        message:
          "Invalid field minPrice: aggregate min cannot be undefined because price is required.",
      });

      const optionalMinQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          minUnset: { aggFunc: "min", field: "unset" },
        },
      });
      const optionalMinSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        optionalMinQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-unset-min",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", minUnset: undefined }],
          totalRows: 1,
        },
      );
      expect(optionalMinSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-unset-min",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            minUnset: { _viewServerAggregate: "undefined" },
          },
        ],
        totalRows: 1,
      });
      const decodedOptionalMinSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        { readonly id: string; readonly minUnset: undefined }
      >(viewServer, "orders", optionalMinQuery, optionalMinSnapshot);
      expect(decodedOptionalMinSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-unset-min",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", minUnset: undefined }],
        totalRows: 1,
      });

      const objectAggregateQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          firstMetadata: { aggFunc: "min", field: "metadata" },
        },
      });
      const objectAggregateSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        objectAggregateQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-object",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              firstMetadata: {
                _viewServerScalar: "bigint",
                value: "not-protocol",
              },
            },
          ],
          totalRows: 1,
        },
      );
      expect(objectAggregateSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-object",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            firstMetadata: {
              _viewServerAggregate: "json",
              value: {
                _viewServerScalar: "bigint",
                value: "not-protocol",
              },
            },
          },
        ],
        totalRows: 1,
      });
      const decodedObjectAggregateSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        {
          readonly id: string;
          readonly firstMetadata: { readonly _viewServerScalar: string; readonly value: string };
        }
      >(viewServer, "orders", objectAggregateQuery, objectAggregateSnapshot);
      expect(decodedObjectAggregateSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-object",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            firstMetadata: {
              _viewServerScalar: "bigint",
              value: "not-protocol",
            },
          },
        ],
        totalRows: 1,
      });

      const bigIntSumQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          totalQuantity: { aggFunc: "sum", field: "quantity" },
        },
      });
      const bigIntSumSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        bigIntSumQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-bigint-sum",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", totalQuantity: 3n }],
          totalRows: 1,
        },
      );
      expect(bigIntSumSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-bigint-sum",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            totalQuantity: { _viewServerAggregate: "bigint", value: "3" },
          },
        ],
        totalRows: 1,
      });
      const decodedBigIntSumSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        {
          readonly id: string;
          readonly totalQuantity: bigint;
        }
      >(viewServer, "orders", bigIntSumQuery, bigIntSumSnapshot);
      expect(decodedBigIntSumSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-bigint-sum",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", totalQuantity: 3n }],
        totalRows: 1,
      });

      const groupedDelta = yield* viewServerEncodeLiveEvent(viewServer, "orders", encodedGrouped, {
        type: "delta",
        topic: "orders",
        queryId: "grouped-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row: groupedRow, index: 0 },
          {
            type: "update",
            key: "b",
            row: { ...groupedRow, id: "b", rowCount: 3n },
            index: 1,
          },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });

      const decodedGroupedDelta = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof groupedRow
      >(viewServer, "orders", encodedGrouped, groupedDelta);
      const decodedGroupedDeltaOperations =
        decodedGroupedDelta.type === "delta"
          ? decodedGroupedDelta.operations.map((operation) =>
              operation.type === "insert" || operation.type === "update"
                ? {
                    ...operation,
                    row: {
                      ...operation.row,
                      totalPrice: formatDecodedDecimal(operation.row.totalPrice),
                      averagePrice: formatDecodedDecimal(operation.row.averagePrice),
                    },
                  }
                : operation,
            )
          : [];
      expect({
        ...decodedGroupedDelta,
        operations: decodedGroupedDeltaOperations,
      }).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "grouped-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "a",
            row: { ...groupedRow, totalPrice: "21", averagePrice: "10.5" },
            index: 0,
          },
          {
            type: "update",
            key: "b",
            row: { ...groupedRow, id: "b", rowCount: 3n, totalPrice: "21", averagePrice: "10.5" },
            index: 1,
          },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });
    }),
  );

  it.effect("rejects non-own Topic Row fields in grouped groupBy encoding and decoding", () =>
    Effect.gen(function* () {
      for (const field of nonOwnTopicRowFields) {
        const query = {
          groupBy: [field],
          aggregates: { rowCount: { aggFunc: "count" } },
        };
        const encodeError = yield* Effect.flip(
          viewServerEncodeGroupedQuery(viewServer, "orders", query),
        );
        expect(encodeError).toStrictEqual(unknownTopicRowFieldError);

        const decodeError = yield* Effect.flip(
          viewServerDecodeGroupedQuery(viewServer, "orders", query),
        );
        expect(decodeError).toStrictEqual(unknownTopicRowFieldError);
      }
    }),
  );

  it.effect("rejects non-own Topic Row fields in grouped aggregate encoding and decoding", () =>
    Effect.gen(function* () {
      for (const field of nonOwnTopicRowFields) {
        const query = {
          groupBy: ["id"],
          aggregates: { total: { aggFunc: "sum", field } },
        };
        const encodeError = yield* Effect.flip(
          viewServerEncodeGroupedQuery(viewServer, "orders", query),
        );
        expect(encodeError).toStrictEqual(unknownTopicRowFieldError);

        const decodeError = yield* Effect.flip(
          viewServerDecodeGroupedQuery(viewServer, "orders", query),
        );
        expect(decodeError).toStrictEqual(unknownTopicRowFieldError);
      }
    }),
  );
});
