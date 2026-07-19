import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import {
  ViewServerBackpressureErrorSchema,
  ViewServerHealthSchema,
  ViewServerRpcErrorSchema,
  ViewServerRpcs,
  ViewServerRuntimeErrorSchema,
  ViewServerSubscribePayloadSchema,
  ViewServerTransportErrorSchema,
  ViewServerWireEventSchema,
  ViewServerWireGroupedQuerySchema,
  ViewServerWireRawQuerySchema,
  ViewServerWireRowSchema,
} from "./index";

import { kafkaStartFromHealth, wireHealth } from "../test-harness/protocol";

describe("Public wire schemas", () => {
  it.effect("decodes the public wire schemas", () =>
    Effect.gen(function* () {
      const row = yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)({
        id: "a",
        quantity: "10",
      });
      expect(row).toStrictEqual({
        id: "a",
        quantity: "10",
      });

      const query = yield* Schema.decodeUnknownEffect(ViewServerWireRawQuerySchema)({
        select: ["id", "quantity"],
        where: [{ field: "quantity", type: "greaterThanOrEqual", filter: "10" }],
        orderBy: [{ field: "quantity", direction: "asc" }],
        offset: 0,
        limit: 10,
      });
      expect(query).toStrictEqual({
        select: ["id", "quantity"],
        where: [{ field: "quantity", type: "greaterThanOrEqual", filter: "10" }],
        orderBy: [{ field: "quantity", direction: "asc" }],
        offset: 0,
        limit: 10,
      });

      const groupedQuery = yield* Schema.decodeUnknownEffect(ViewServerWireGroupedQuerySchema)({
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
        offset: 0,
        limit: 10,
      });
      expect(groupedQuery).toStrictEqual({
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: [{ field: "price", type: "greaterThanOrEqual", filter: 10 }],
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
        offset: 0,
        limit: 10,
      });

      const health = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)({
        status: "ready",
        version: 1,
        uptimeMs: 10,
        engine: {
          topics: {
            orders: {
              status: "degraded",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 1,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
              activeSubscriptions: 1,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
            },
          },
        },
        transport: {
          activeClients: 1,
          activeStreams: 1,
          activeSubscriptions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          queuedMessages: 0,
          queuedBytes: 0,
          droppedClients: 0,
          backpressureEvents: 0,
          reconnects: 0,
          lastError: null,
        },
      });
      expect(health.engine.topics["orders"]?.rowCount).toBe(1);

      const largeLag = 9_007_199_254_740_993n;
      const lagHealth = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)({
        status: "ready",
        version: 1,
        uptimeMs: 10,
        engine: {
          topics: {
            orders: {
              status: "ready",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 1,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
              activeSubscriptions: 1,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
            },
          },
        },
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            orders: {
              status: "ready",
              sourceTopic: "orders-source",
              viewServerTopic: "orders",
              regions: {
                usa: {
                  connected: true,
                  assignedPartitions: 1,
                  messagesPerSecond: 2,
                  bytesPerSecond: 2,
                  decodedMessagesPerSecond: 0,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  publishFailuresPerSecond: 1,
                  commitFailuresPerSecond: 1,
                  processingFailuresPerSecond: 2,
                  lastMessageAt: 123,
                  lastCommitAt: null,
                  consumerLagMessages: largeLag.toString(),
                  lagSampledAt: null,
                  committedOffset: null,
                  lastError: "commit failed",
                },
              },
            },
          },
        },
        transport: {
          activeClients: 1,
          activeStreams: 1,
          activeSubscriptions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          queuedMessages: 0,
          queuedBytes: 0,
          droppedClients: 0,
          backpressureEvents: 0,
          reconnects: 0,
          lastError: null,
        },
      });
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.consumerLagMessages).toBe(largeLag);
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.processingFailuresPerSecond).toBe(
        2,
      );
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.publishFailuresPerSecond).toBe(1);
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.commitFailuresPerSecond).toBe(1);
      const encodedLagHealth = yield* Schema.encodeUnknownEffect(ViewServerHealthSchema)(lagHealth);
      expect(lagHealth.kafka?.startFrom).toStrictEqual(kafkaStartFromHealth);
      expect(encodedLagHealth.kafka?.topics["orders"]?.regions["usa"]?.consumerLagMessages).toBe(
        largeLag.toString(),
      );
      expect(encodedLagHealth.kafka?.startFrom).toStrictEqual(kafkaStartFromHealth);
      const impossibleKafkaStartFromHealth = yield* Effect.flip(
        Schema.decodeUnknownEffect(ViewServerHealthSchema)({
          ...wireHealth,
          kafka: {
            startFrom: {
              consumerGroupId: "view-server-invalid-latest-fail",
              fallbackMode: "fail",
              mode: "latest",
            },
            regions: {},
            topics: {},
          },
        }),
      );
      expect(String(impossibleKafkaStartFromHealth)).toContain("fallbackMode");

      const snapshot = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [row],
        totalRows: 1,
      });
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [row],
        totalRows: 1,
      });

      const delta = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row, index: 0 },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "b" },
        ],
        totalRows: 1,
      });
      expect(delta.type).toBe("delta");

      const ready = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });
      expect(ready.type).toBe("status");

      const stale = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "stale",
        code: "SnapshotStale",
      });
      expect(stale.type).toBe("status");

      const closed = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "SubscriptionClosed",
      });
      expect(closed.type).toBe("status");

      const error = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "InvalidQuery",
      });
      expect(error.type).toBe("status");

      const encodedSubscribePayload = yield* Schema.encodeUnknownEffect(
        ViewServerSubscribePayloadSchema,
      )({
        topic: "orders",
        query,
      });
      const subscribePayload = yield* Schema.decodeUnknownEffect(ViewServerSubscribePayloadSchema)(
        encodedSubscribePayload,
      );
      expect(subscribePayload.topic).toBe("orders");
      expect(subscribePayload.query).toStrictEqual(query);

      const backpressure = yield* Schema.decodeUnknownEffect(ViewServerBackpressureErrorSchema)({
        _tag: "ViewServerBackpressureError",
        code: "BackpressureExceeded",
        message: "queue full",
      });
      expect(backpressure.code).toBe("BackpressureExceeded");

      const runtime = yield* Schema.decodeUnknownEffect(ViewServerRuntimeErrorSchema)({
        _tag: "ViewServerRuntimeError",
        code: "InvalidTopic",
        message: "unknown",
      });
      expect(runtime.code).toBe("InvalidTopic");

      const transport = yield* Schema.decodeUnknownEffect(ViewServerTransportErrorSchema)({
        _tag: "ViewServerTransportError",
        code: "TransportError",
        message: "socket closed",
      });
      expect(transport.code).toBe("TransportError");

      const rpcError = yield* Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "bad query",
      });
      expect(rpcError.code).toBe("InvalidQuery");

      expect(typeof ViewServerRpcs).toBe("function");
    }),
  );
});
