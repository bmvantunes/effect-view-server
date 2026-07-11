import { describe, expect, it } from "@effect/vitest";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
} from "@effect-view-server/config";
import { Effect } from "effect";
import {
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
} from "./index";

import { viewServer } from "../test-harness/protocol";

describe("Health wire codec", () => {
  it.effect("encodes and decodes pushed health wire codec operations", () =>
    Effect.gen(function* () {
      const summaryRow: ViewServerHealthSummaryRow<typeof viewServer.topics> = {
        id: "summary",
        status: "degraded",
        runtimeStatus: "degraded",
        connectionStatus: "connected",
        unhealthyTopics: ["orders"],
        updatedAtNanos: 123n,
        maxKafkaLag: 45n,
      };

      const summaryStatus = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
        type: "status",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        status: "ready",
        code: "Ready",
      });
      expect(summaryStatus).toStrictEqual({
        type: "status",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        status: "ready",
        code: "Ready",
      });

      const decodedSummaryStatus = yield* viewServerDecodeHealthSummaryEvent(
        viewServer,
        summaryStatus,
      );
      expect(decodedSummaryStatus).toStrictEqual(summaryStatus);

      const summarySnapshot = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [summaryRow],
        totalRows: 1,
      });
      expect(summarySnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "degraded",
            runtimeStatus: "degraded",
            connectionStatus: "connected",
            unhealthyTopics: ["orders"],
            updatedAtNanos: "123",
            maxKafkaLag: "45",
          },
        ],
        totalRows: 1,
      });

      const decodedSummary = yield* viewServerDecodeHealthSummaryEvent(viewServer, summarySnapshot);
      expect(decodedSummary).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [summaryRow],
        totalRows: 1,
      });

      const disconnectedSummary = yield* viewServerDecodeHealthSummaryEvent(viewServer, {
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "disconnected",
            runtimeStatus: "ready",
            connectionStatus: "disconnected",
            unhealthyTopics: [],
            updatedAtNanos: "1",
            maxKafkaLag: null,
          },
        ],
        totalRows: 1,
      });
      expect(disconnectedSummary).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "disconnected",
            runtimeStatus: "ready",
            connectionStatus: "disconnected",
            unhealthyTopics: [],
            updatedAtNanos: 1n,
            maxKafkaLag: null,
          },
        ],
        totalRows: 1,
      });

      const summaryDelta = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
        type: "delta",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "summary",
            row: { ...summaryRow, unhealthyTopics: [] },
            index: 0,
          },
          { type: "move", key: "summary", fromIndex: 0, toIndex: 0 },
        ],
        totalRows: 1,
      });
      const decodedSummaryDelta = yield* viewServerDecodeHealthSummaryEvent(
        viewServer,
        summaryDelta,
      );
      expect(decodedSummaryDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "summary",
            row: { ...summaryRow, unhealthyTopics: [] },
            index: 0,
          },
          { type: "move", key: "summary", fromIndex: 0, toIndex: 0 },
        ],
        totalRows: 1,
      });

      const healthTopicRow: ViewServerHealthTopicRow<"orders"> = {
        id: "orders",
        status: "ready",
        rowCount: 10,
        liveRowCount: 9,
        deletedRowCount: 1,
        version: 10,
        lastMutationAt: null,
        mutationsPerSecond: 2,
        rowsPerSecond: 3,
        pendingMutationBatches: 0,
        activeFallbackGroupedViews: 0,
        activeIncrementalGroupedViews: 0,
        activeViews: 1,
        groupedFullEvaluationCount: 0,
        groupedPatchedEvaluationCount: 0,
        activeSubscriptions: 2,
        queuedEvents: 3,
        maxQueueDepth: 4,
        backpressureEvents: 5,
        memoryBytes: 6,
        tombstoneCount: 1,
        compactionPending: false,
        kafkaLag: 7n,
        updatedAtNanos: 456n,
      };
      const badJsonHealthTopicRow: ViewServerHealthTopicRow<"badjson"> = {
        ...healthTopicRow,
        id: "badjson",
        kafkaLag: null,
        rowCount: 0,
        liveRowCount: 0,
        version: 0,
      };

      const topicSnapshot = yield* viewServerEncodeHealthTopicEvent(viewServer, {
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [healthTopicRow, badJsonHealthTopicRow],
        totalRows: 2,
      });
      expect(topicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [
          {
            id: "orders",
            status: "ready",
            rowCount: 10,
            liveRowCount: 9,
            deletedRowCount: 1,
            version: 10,
            lastMutationAt: null,
            mutationsPerSecond: 2,
            rowsPerSecond: 3,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 1,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 2,
            queuedEvents: 3,
            maxQueueDepth: 4,
            backpressureEvents: 5,
            memoryBytes: 6,
            tombstoneCount: 1,
            compactionPending: false,
            kafkaLag: "7",
            updatedAtNanos: "456",
          },
          {
            ...badJsonHealthTopicRow,
            updatedAtNanos: "456",
          },
        ],
        totalRows: 2,
      });

      const topicDelta = yield* viewServerEncodeHealthTopicEvent(viewServer, {
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
      });
      expect(topicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "orders",
            row: { ...healthTopicRow, rowCount: 11, kafkaLag: "7", updatedAtNanos: "456" },
            index: 0,
          },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
      });

      const decodedTopicSnapshot = yield* viewServerDecodeHealthTopicEvent(
        viewServer,
        topicSnapshot,
      );
      expect(decodedTopicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [healthTopicRow, badJsonHealthTopicRow],
        totalRows: 2,
      });

      const decodedTopicDelta = yield* viewServerDecodeHealthTopicEvent(viewServer, topicDelta);
      expect(decodedTopicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
      });
    }),
  );
});
