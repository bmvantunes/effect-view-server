import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  defineViewServerLiveEventQuery,
  viewServerDecodeLiveEvent,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveEvent,
} from "./index";

const OpaqueRow = Schema.Struct({
  id: Schema.String,
  payload: Schema.ObjectKeyword,
});

const opaqueViewServer = defineViewServerConfig({
  topics: {
    opaque: {
      schema: OpaqueRow,
      key: "id",
    },
  },
});

const rawQuery = defineViewServerLiveEventQuery(opaqueViewServer, "opaque", {
  select: ["id", "payload"],
});

const expectedOpaqueValueError = {
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message: "Field payload is not JSON-safe: Expected a plain data record or dense array at $.",
  topic: "opaque",
} as const;

describe("Opaque row wire values", () => {
  it.effect("rejects opaque Map values in raw Snapshots and insert/update Deltas", () =>
    Effect.gen(function* () {
      const payload = new Map([["venue", "xnys"]]);
      const snapshotError = yield* Effect.flip(
        viewServerEncodeLiveEvent(opaqueViewServer, "opaque", rawQuery, {
          type: "snapshot",
          topic: "opaque",
          queryId: "opaque-snapshot",
          version: 1,
          keys: ["1"],
          rows: [{ id: "1", payload }],
          totalRows: 1,
        }),
      );
      expect(snapshotError).toStrictEqual(expectedOpaqueValueError);

      const insertError = yield* Effect.flip(
        viewServerEncodeLiveEvent(opaqueViewServer, "opaque", rawQuery, {
          type: "delta",
          topic: "opaque",
          queryId: "opaque-insert",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "insert", key: "1", row: { id: "1", payload }, index: 0 }],
          totalRows: 1,
        }),
      );
      expect(insertError).toStrictEqual(expectedOpaqueValueError);

      const updateError = yield* Effect.flip(
        viewServerEncodeLiveEvent(opaqueViewServer, "opaque", rawQuery, {
          type: "delta",
          topic: "opaque",
          queryId: "opaque-update",
          fromVersion: 2,
          toVersion: 3,
          operations: [{ type: "update", key: "1", row: { id: "1", payload }, index: 0 }],
          totalRows: 1,
        }),
      );
      expect(updateError).toStrictEqual(expectedOpaqueValueError);
    }),
  );

  it.effect("rejects hostile opaque Map values supplied directly to decode", () =>
    Effect.gen(function* () {
      const hostileEvent = {
        type: "snapshot",
        topic: "opaque",
        queryId: "hostile-direct-decode",
        version: 1,
        keys: ["1"],
        rows: [{ id: "1", payload: new Map([["venue", "xnys"]]) }],
        totalRows: 1,
      };
      const error = yield* Effect.flip(
        viewServerDecodeLiveEvent(
          opaqueViewServer,
          "opaque",
          rawQuery,
          // @ts-expect-error hostile callers can bypass the public JSON wire type.
          hostileEvent,
        ),
      );

      expect(error).toStrictEqual({
        ...expectedOpaqueValueError,
        message: "Invalid event: Expected a plain data record or dense array at $.rows[0].payload.",
      });
    }),
  );

  it.effect("rejects opaque Map aggregate values in grouped Snapshots and update Deltas", () =>
    Effect.gen(function* () {
      const groupedQuery = defineViewServerLiveEventQuery(opaqueViewServer, "opaque", {
        groupBy: ["id"],
        aggregates: {
          firstPayload: { aggFunc: "min", field: "payload" },
        },
      });
      yield* viewServerEncodeGroupedQuery(opaqueViewServer, "opaque", groupedQuery);
      const row = {
        id: "1",
        firstPayload: new Map([["venue", "xnys"]]),
      };
      const groupedError = {
        ...expectedOpaqueValueError,
        message:
          "Field firstPayload is not JSON-safe: Expected a plain data record or dense array at $.",
      };

      const snapshotError = yield* Effect.flip(
        viewServerEncodeLiveEvent(opaqueViewServer, "opaque", groupedQuery, {
          type: "snapshot",
          topic: "opaque",
          queryId: "opaque-grouped-snapshot",
          version: 1,
          keys: ["1"],
          rows: [row],
          totalRows: 1,
        }),
      );
      expect(snapshotError).toStrictEqual(groupedError);

      const updateError = yield* Effect.flip(
        viewServerEncodeLiveEvent(opaqueViewServer, "opaque", groupedQuery, {
          type: "delta",
          topic: "opaque",
          queryId: "opaque-grouped-update",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "update", key: "1", row, index: 0 }],
          totalRows: 1,
        }),
      );
      expect(updateError).toStrictEqual(groupedError);

      const hostileGroupedEvent = {
        type: "snapshot",
        topic: "opaque",
        queryId: "opaque-grouped-direct-decode",
        version: 1,
        keys: ["1"],
        rows: [
          {
            id: "1",
            firstPayload: {
              _viewServerAggregate: "json",
              value: new Map([["venue", "xnys"]]),
            },
          },
        ],
        totalRows: 1,
      };
      const directDecodeError = yield* Effect.flip(
        viewServerDecodeLiveEvent(
          opaqueViewServer,
          "opaque",
          groupedQuery,
          // @ts-expect-error hostile callers can bypass the public JSON wire type.
          hostileGroupedEvent,
        ),
      );
      expect(directDecodeError).toStrictEqual({
        ...expectedOpaqueValueError,
        message:
          "Invalid event: Expected a plain data record or dense array at $.rows[0].firstPayload.value.",
      });
    }),
  );

  it.effect("round-trips plain JSON objects accepted by an opaque field schema", () =>
    Effect.gen(function* () {
      const encoded = yield* viewServerEncodeLiveEvent(opaqueViewServer, "opaque", rawQuery, {
        type: "snapshot",
        topic: "opaque",
        queryId: "plain-object",
        version: 1,
        keys: ["1"],
        rows: [{ id: "1", payload: { venue: "xnys", nested: [1, true, null] } }],
        totalRows: 1,
      });
      expect(encoded).toStrictEqual({
        type: "snapshot",
        topic: "opaque",
        queryId: "plain-object",
        version: 1,
        keys: ["1"],
        rows: [{ id: "1", payload: { venue: "xnys", nested: [1, true, null] } }],
        totalRows: 1,
      });

      const decoded = yield* viewServerDecodeLiveEvent(
        opaqueViewServer,
        "opaque",
        rawQuery,
        encoded,
      );
      expect(decoded).toStrictEqual(encoded);
    }),
  );
});
