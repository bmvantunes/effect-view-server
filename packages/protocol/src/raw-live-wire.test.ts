import { describe, expect, it } from "@effect/vitest";
import { VIEW_SERVER_HEALTH_TOPIC } from "@effect-view-server/config";
import { Effect, Schema } from "effect";
import {
  compileViewServerLiveEventCodec,
  ViewServerTrustedWireEventSchema,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeLiveEvent,
  viewServerDecodeRawQuery,
  viewServerDecodeTrustedLiveEvent,
  viewServerDecodeTopic,
  viewServerEncodeLiveEvent,
  viewServerEncodeRawQuery,
} from "./index";

import {
  nonOwnTopicRowFields,
  Order,
  unknownTopicRowFieldError,
  viewServer,
  wireHealth,
} from "../test-harness/protocol";

describe("Raw live wire codec", () => {
  it.effect("compiles and reuses one raw row contract across live events", () =>
    Effect.gen(function* () {
      const query = {
        select: ["id"],
      };
      const codec = compileViewServerLiveEventCodec<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", query);
      query.select.push("price");

      const snapshot = yield* codec.encode({
        type: "snapshot",
        topic: "orders",
        queryId: "compiled-raw",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });
      const delta = yield* codec.encode({
        type: "delta",
        topic: "orders",
        queryId: "compiled-raw",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "update", key: "a", row: { id: "a" }, index: 0 }],
        totalRows: 1,
      });
      const decodedSnapshot = yield* codec.decodeTrusted(snapshot);
      const decodedDelta = yield* codec.decodeTrusted(delta);

      expect(decodedSnapshot).toStrictEqual(snapshot);
      expect(decodedDelta).toStrictEqual(delta);
    }),
  );

  it.effect("encodes and decodes live wire codec operations", () =>
    Effect.gen(function* () {
      const topic = yield* viewServerDecodeTopic(viewServer, "orders");
      expect(topic).toBe("orders");

      const richWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id", "price"],
        where: [
          { field: "id", type: "in", filter: ["a", "b"] },
          { field: "id", type: "startsWith", filter: "a" },
          { field: "price", type: "greaterThan", filter: 1 },
        ],
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 0,
        limit: 10,
      });
      expect(richWireQuery).toStrictEqual({
        select: ["id", "price"],
        where: [
          { field: "id", type: "in", filter: ["a", "b"] },
          { field: "id", type: "startsWith", filter: "a" },
          { field: "price", type: "greaterThan", filter: 1 },
        ],
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 0,
        limit: 10,
      });

      const scalarWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: 10 }],
      });
      expect(scalarWireQuery).toStrictEqual({
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: 10 }],
      });
      const minimalWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(minimalWireQuery).toStrictEqual({ select: ["id"] });

      const decodedQuery = yield* viewServerDecodeRawQuery(viewServer, "orders", richWireQuery);
      expect(decodedQuery).toStrictEqual(richWireQuery);
      const healthQuery = yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, {
        select: ["id"],
      });
      expect(healthQuery).toStrictEqual({ select: ["id"] });
      const decodedNoWhere = yield* viewServerDecodeRawQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(decodedNoWhere).toStrictEqual({ select: ["id"] });
      const decodedScalarWhere = yield* viewServerDecodeRawQuery(viewServer, "orders", {
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: 10 }],
      });
      expect(decodedScalarWhere).toStrictEqual({
        select: ["id"],
        where: [{ field: "price", type: "equals", filter: 10 }],
      });

      const idQuery = { select: ["id"] };
      const statusEvent = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });
      expect(statusEvent).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });

      const malformedStatusEncode = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
          type: "status",
          topic: "orders",
          queryId: "query-0",
          status: "ready",
          // @ts-expect-error ready status events can only use the Ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedStatusEncode.message).toMatch(/Invalid event/);

      const snapshot = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });

      const delta = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row: { id: "a" }, index: 0 },
          { type: "update", key: "b", row: { id: "b" }, index: 1 },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });
      expect(delta.type).toBe("delta");

      const decodedStatus = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof Order.Type
      >(viewServer, "orders", idQuery, statusEvent);
      expect(decodedStatus).toStrictEqual(statusEvent);

      const malformedStatusDecode = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          idQuery,
          {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "ready",
            // @ts-expect-error hostile wire status can use an invalid ready code.
            code: "InvalidRow",
          },
        ),
      );
      expect(malformedStatusDecode.message).toMatch(/Invalid event/);

      const decodedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, snapshot);
      expect(decodedSnapshot).toStrictEqual(snapshot);

      const decodedDelta = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, delta);
      expect(decodedDelta).toStrictEqual(delta);

      const trustedSnapshot = yield* Schema.decodeUnknownEffect(ViewServerTrustedWireEventSchema)(
        snapshot,
      );
      const decodedTrustedSnapshot = yield* viewServerDecodeTrustedLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, trustedSnapshot);
      expect(decodedTrustedSnapshot).toStrictEqual(snapshot);

      const decodedHealth = yield* viewServerDecodeHealth(viewServer, wireHealth);
      expect(decodedHealth.status).toBe("ready");
    }),
  );

  it.effect("rejects non-own Topic Row fields in raw select encoding and decoding", () =>
    Effect.gen(function* () {
      for (const field of nonOwnTopicRowFields) {
        const encodeError = yield* Effect.flip(
          viewServerEncodeRawQuery(viewServer, "orders", { select: [field] }),
        );
        expect(encodeError).toStrictEqual(unknownTopicRowFieldError);

        const decodeError = yield* Effect.flip(
          viewServerDecodeRawQuery(viewServer, "orders", { select: [field] }),
        );
        expect(decodeError).toStrictEqual(unknownTopicRowFieldError);
      }
    }),
  );

  it.effect("rejects non-own Topic Row fields in raw where encoding and decoding", () =>
    Effect.gen(function* () {
      for (const field of nonOwnTopicRowFields) {
        const query = {
          select: ["id"],
          where: [{ field, type: "equals", filter: "x" }],
        };
        const unknownFilterFieldError = {
          _tag: "ViewServerRuntimeError",
          code: "InvalidQuery",
          message: `Query references an unknown or non-filterable field: ${field}`,
          topic: "orders",
        };
        const encodeError = yield* Effect.flip(
          viewServerEncodeRawQuery(viewServer, "orders", query),
        );
        expect(encodeError).toStrictEqual(unknownFilterFieldError);

        const decodeError = yield* Effect.flip(
          viewServerDecodeRawQuery(viewServer, "orders", query),
        );
        expect(decodeError).toStrictEqual(unknownFilterFieldError);
      }
    }),
  );

  it.effect("rejects non-own Topic Row fields in raw orderBy encoding and decoding", () =>
    Effect.gen(function* () {
      for (const field of nonOwnTopicRowFields) {
        const query = {
          select: ["id"],
          orderBy: [{ field, direction: "asc" }],
        };
        const encodeError = yield* Effect.flip(
          viewServerEncodeRawQuery(viewServer, "orders", query),
        );
        expect(encodeError).toStrictEqual(unknownTopicRowFieldError);

        const decodeError = yield* Effect.flip(
          viewServerDecodeRawQuery(viewServer, "orders", query),
        );
        expect(decodeError).toStrictEqual(unknownTopicRowFieldError);
      }
    }),
  );
});
