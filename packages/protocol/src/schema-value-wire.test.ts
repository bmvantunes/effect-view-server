import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, viewSchema } from "@effect-view-server/config";
import * as BigDecimal from "effect/BigDecimal";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import { Effect, Schema } from "effect";
import {
  ViewServerWireEventSchema,
  viewServerDecodeLiveEvent,
  viewServerDecodeRawQuery,
  viewServerEncodeGroupedQuery,
  viewServerEncodeLiveEvent,
  viewServerEncodeRawQuery,
} from "./index";
import type { ViewServerProtocolEvent } from "./protocol-event-codec";

class Venue extends Schema.Class<Venue>("Venue")({
  code: Schema.String,
}) {}
viewSchema.admitClass(Venue);

const SemanticRow = Schema.Struct({
  id: Schema.String,
  venue: Venue,
  optionalQuantity: viewSchema.Option(Schema.BigInt),
  tags: viewSchema.Chunk(Schema.String),
  quantities: viewSchema.HashMap(Schema.String, Schema.BigInt),
  amount: Schema.BigDecimal,
  note: Schema.optionalKey(Schema.String),
});

const semanticViewServer = defineViewServerConfig({
  topics: {
    semantic: {
      schema: SemanticRow,
      key: "id",
    },
  },
});

const semanticWireRow = {
  id: "1",
  venue: { code: "XNYS" },
  optionalQuantity: { _tag: "Some", value: "9007199254740993" },
  tags: ["live", "typed"],
  quantities: [["desk-a", "9007199254740995"]],
  amount: "1234567890.123456789",
};

const summarizeSemanticRow = (row: typeof SemanticRow.Type) => ({
  id: row.id,
  venue: row.venue.code,
  venueIsClass: row.venue instanceof Venue,
  optionalQuantity: Option.getOrNull(row.optionalQuantity),
  tags: Array.from(row.tags),
  quantities: HashMap.toEntries(row.quantities),
  amount: BigDecimal.format(row.amount),
  hasNote: Object.hasOwn(row, "note"),
});

const semanticRowSummary = {
  id: "1",
  venue: "XNYS",
  venueIsClass: true,
  optionalQuantity: 9007199254740993n,
  tags: ["live", "typed"],
  quantities: [["desk-a", 9007199254740995n]],
  amount: "1234567890.123456789",
  hasNote: false,
};

const decodeTransportedEvent = Effect.fn("ViewServerProtocol.test.transport")(function* (
  value: unknown,
) {
  const jsonText = yield* Schema.encodeUnknownEffect(Schema.UnknownFromJsonString)(value);
  const parsed = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(jsonText);
  return yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(parsed);
});

const requireSnapshotEvent = Effect.fn("ViewServerProtocol.test.requireSnapshot")(function* <Row>(
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.type !== "snapshot") {
    return yield* Effect.die("Expected a Snapshot event.");
  }
  return event;
});

const requireDeltaEvent = Effect.fn("ViewServerProtocol.test.requireDelta")(function* <Row>(
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.type !== "delta") {
    return yield* Effect.die("Expected a Delta event.");
  }
  return event;
});

describe("Schema value wire semantics", () => {
  it.effect("round-trips Schema.Class equality filters through canonical JSON", () =>
    Effect.gen(function* () {
      const venue = Venue.make({ code: "XNYS" });
      const encoded = yield* viewServerEncodeRawQuery(semanticViewServer, "semantic", {
        select: ["id"],
        where: {
          venue: { eq: venue },
        },
      });
      expect(encoded).toStrictEqual({
        select: ["id"],
        where: {
          venue: { eq: { code: "XNYS" } },
        },
      });

      const jsonText = yield* Schema.encodeUnknownEffect(Schema.UnknownFromJsonString)(encoded);
      const transported = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(jsonText);
      const decoded = yield* viewServerDecodeRawQuery(semanticViewServer, "semantic", transported);
      const decodedVenue = Reflect.get(Object(decoded.where?.["venue"]), "eq");

      expect(decoded.where).toStrictEqual({
        venue: { eq: venue },
      });
      expect(decodedVenue).toBeInstanceOf(Venue);
    }),
  );

  it.effect(
    "round-trips canonical values and omitted optional fields in raw Snapshot and Delta",
    () =>
      Effect.gen(function* () {
        const row = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(SemanticRow))(
          semanticWireRow,
        );
        const query = {
          select: ["id", "venue", "optionalQuantity", "tags", "quantities", "amount", "note"],
        };
        const snapshot = yield* viewServerEncodeLiveEvent(semanticViewServer, "semantic", query, {
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-snapshot",
          version: 1,
          keys: ["1"],
          rows: [row],
          totalRows: 1,
        });
        expect(snapshot).toStrictEqual({
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-snapshot",
          version: 1,
          keys: ["1"],
          rows: [semanticWireRow],
          totalRows: 1,
        });

        const transportedSnapshot = yield* decodeTransportedEvent(snapshot);
        const decodedSnapshot = yield* viewServerDecodeLiveEvent<
          typeof semanticViewServer.topics,
          "semantic",
          typeof SemanticRow.Type
        >(semanticViewServer, "semantic", query, transportedSnapshot);
        const decodedSnapshotEvent = yield* requireSnapshotEvent(decodedSnapshot);
        const summarizedSnapshot = {
          ...decodedSnapshotEvent,
          rows: decodedSnapshotEvent.rows.map(summarizeSemanticRow),
        };
        expect(summarizedSnapshot).toStrictEqual({
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-snapshot",
          version: 1,
          keys: ["1"],
          rows: [semanticRowSummary],
          totalRows: 1,
        });

        const delta = yield* viewServerEncodeLiveEvent(semanticViewServer, "semantic", query, {
          type: "delta",
          topic: "semantic",
          queryId: "semantic-delta",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            { type: "insert", key: "1", row, index: 0 },
            { type: "update", key: "1", row, index: 0 },
          ],
          totalRows: 1,
        });
        expect(delta).toStrictEqual({
          type: "delta",
          topic: "semantic",
          queryId: "semantic-delta",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            { type: "insert", key: "1", row: semanticWireRow, index: 0 },
            { type: "update", key: "1", row: semanticWireRow, index: 0 },
          ],
          totalRows: 1,
        });

        const transportedDelta = yield* decodeTransportedEvent(delta);
        const decodedDelta = yield* viewServerDecodeLiveEvent<
          typeof semanticViewServer.topics,
          "semantic",
          typeof SemanticRow.Type
        >(semanticViewServer, "semantic", query, transportedDelta);
        const decodedDeltaEvent = yield* requireDeltaEvent(decodedDelta);
        const summarizedDelta = {
          ...decodedDeltaEvent,
          operations: decodedDeltaEvent.operations.map((operation) =>
            operation.type === "insert" || operation.type === "update"
              ? { ...operation, row: summarizeSemanticRow(operation.row) }
              : operation,
          ),
        };
        expect(summarizedDelta).toStrictEqual({
          type: "delta",
          topic: "semantic",
          queryId: "semantic-delta",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            { type: "insert", key: "1", row: semanticRowSummary, index: 0 },
            { type: "update", key: "1", row: semanticRowSummary, index: 0 },
          ],
          totalRows: 1,
        });

        expect(summarizeSemanticRow(row)).toStrictEqual(semanticRowSummary);
      }),
  );

  it.effect("round-trips Schema.Class group fields in grouped Snapshot and update Delta rows", () =>
    Effect.gen(function* () {
      const venue = Venue.make({ code: "XNYS" });
      const groupedQuery = yield* viewServerEncodeGroupedQuery(semanticViewServer, "semantic", {
        groupBy: ["venue"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      const groupedRow = { venue, rowCount: 1n };
      const groupedWireRow = {
        venue: { code: "XNYS" },
        rowCount: { _viewServerAggregate: "bigint", value: "1" },
      };
      const snapshot = yield* viewServerEncodeLiveEvent(
        semanticViewServer,
        "semantic",
        groupedQuery,
        {
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-grouped-snapshot",
          version: 1,
          keys: ["venue-xnys"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-grouped-snapshot",
        version: 1,
        keys: ["venue-xnys"],
        rows: [groupedWireRow],
        totalRows: 1,
      });

      const decodedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(snapshot));
      expect(decodedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-grouped-snapshot",
        version: 1,
        keys: ["venue-xnys"],
        rows: [groupedRow],
        totalRows: 1,
      });

      const delta = yield* viewServerEncodeLiveEvent(semanticViewServer, "semantic", groupedQuery, {
        type: "delta",
        topic: "semantic",
        queryId: "semantic-grouped-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "venue-xnys", row: groupedRow, index: 0 }],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "semantic",
        queryId: "semantic-grouped-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "venue-xnys", row: groupedWireRow, index: 0 }],
        totalRows: 1,
      });

      const decodedDelta = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(delta));
      expect(decodedDelta).toStrictEqual({
        type: "delta",
        topic: "semantic",
        queryId: "semantic-grouped-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "venue-xnys", row: groupedRow, index: 0 }],
        totalRows: 1,
      });
    }),
  );

  it.effect("round-trips Schema.Class min and max aggregate envelopes", () =>
    Effect.gen(function* () {
      const groupedQuery = yield* viewServerEncodeGroupedQuery(semanticViewServer, "semantic", {
        groupBy: ["id"],
        aggregates: {
          firstVenue: { aggFunc: "min", field: "venue" },
          lastVenue: { aggFunc: "max", field: "venue" },
        },
      });
      const groupedRow = {
        id: "1",
        firstVenue: Venue.make({ code: "XNYS" }),
        lastVenue: Venue.make({ code: "XNAS" }),
      };
      const encoded = yield* viewServerEncodeLiveEvent(
        semanticViewServer,
        "semantic",
        groupedQuery,
        {
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-class-aggregates",
          version: 1,
          keys: ["1"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );
      expect(encoded).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-class-aggregates",
        version: 1,
        keys: ["1"],
        rows: [
          {
            id: "1",
            firstVenue: { _viewServerAggregate: "json", value: { code: "XNYS" } },
            lastVenue: { _viewServerAggregate: "json", value: { code: "XNAS" } },
          },
        ],
        totalRows: 1,
      });

      const decoded = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(encoded));
      const snapshot = yield* requireSnapshotEvent(decoded);
      expect(snapshot.rows[0]?.firstVenue).toBeInstanceOf(Venue);
      expect(snapshot.rows[0]?.lastVenue).toBeInstanceOf(Venue);
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-class-aggregates",
        version: 1,
        keys: ["1"],
        rows: [groupedRow],
        totalRows: 1,
      });
    }),
  );

  it.effect("round-trips all-missing optional min and max values in Snapshot and Delta", () =>
    Effect.gen(function* () {
      const groupedQuery = yield* viewServerEncodeGroupedQuery(semanticViewServer, "semantic", {
        groupBy: ["id"],
        aggregates: {
          firstNote: { aggFunc: "min", field: "note" },
          lastNote: { aggFunc: "max", field: "note" },
        },
      });
      const groupedRow = {
        id: "1",
        firstNote: undefined,
        lastNote: undefined,
      };
      const wireRow = {
        id: "1",
        firstNote: { _viewServerAggregate: "undefined" },
        lastNote: { _viewServerAggregate: "undefined" },
      };
      const snapshot = yield* viewServerEncodeLiveEvent(
        semanticViewServer,
        "semantic",
        groupedQuery,
        {
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-optional-extrema-snapshot",
          version: 1,
          keys: ["1"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-optional-extrema-snapshot",
        version: 1,
        keys: ["1"],
        rows: [wireRow],
        totalRows: 1,
      });
      const decodedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(snapshot));
      expect(decodedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-optional-extrema-snapshot",
        version: 1,
        keys: ["1"],
        rows: [groupedRow],
        totalRows: 1,
      });

      const delta = yield* viewServerEncodeLiveEvent(semanticViewServer, "semantic", groupedQuery, {
        type: "delta",
        topic: "semantic",
        queryId: "semantic-optional-extrema-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "1", row: groupedRow, index: 0 }],
        totalRows: 1,
      });
      expect(delta).toStrictEqual({
        type: "delta",
        topic: "semantic",
        queryId: "semantic-optional-extrema-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "1", row: wireRow, index: 0 }],
        totalRows: 1,
      });
      const decodedDelta = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(delta));
      expect(decodedDelta).toStrictEqual({
        type: "delta",
        topic: "semantic",
        queryId: "semantic-optional-extrema-delta",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "1", row: groupedRow, index: 0 }],
        totalRows: 1,
      });
    }),
  );

  it.effect("round-trips an omitted optional grouped field", () =>
    Effect.gen(function* () {
      const groupedQuery = yield* viewServerEncodeGroupedQuery(semanticViewServer, "semantic", {
        groupBy: ["note"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      });
      const groupedRow = { rowCount: 1n };
      const snapshot = yield* viewServerEncodeLiveEvent(
        semanticViewServer,
        "semantic",
        groupedQuery,
        {
          type: "snapshot",
          topic: "semantic",
          queryId: "semantic-optional-group",
          version: 1,
          keys: ["missing-note"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );

      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-optional-group",
        version: 1,
        keys: ["missing-note"],
        rows: [{ rowCount: { _viewServerAggregate: "bigint", value: "1" } }],
        totalRows: 1,
      });

      const decoded = yield* viewServerDecodeLiveEvent<
        typeof semanticViewServer.topics,
        "semantic",
        typeof groupedRow
      >(semanticViewServer, "semantic", groupedQuery, yield* decodeTransportedEvent(snapshot));
      expect(decoded).toStrictEqual({
        type: "snapshot",
        topic: "semantic",
        queryId: "semantic-optional-group",
        version: 1,
        keys: ["missing-note"],
        rows: [groupedRow],
        totalRows: 1,
      });
    }),
  );
});
