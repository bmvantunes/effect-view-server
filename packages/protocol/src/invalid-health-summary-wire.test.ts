import { describe, expect, it } from "@effect/vitest";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@effect-view-server/config";
import { Effect } from "effect";
import {
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
} from "./index";

import { viewServer } from "../test-harness/protocol";

describe("Invalid health summary wire inputs", () => {
  it.effect("rejects inconsistent health summary events", () =>
    Effect.gen(function* () {
      const wrongSummaryEncodeTopic = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "status",
          // @ts-expect-error hostile callers can pass the wrong system topic.
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-summary",
          status: "ready",
          code: "Ready",
        }),
      );

      expect(wrongSummaryEncodeTopic.message).toBe(
        "Received event for __view_server_health while subscribed to __view_server_health_summary",
      );

      const malformedSummaryStatus = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          status: "ready",
          // @ts-expect-error ready summary status can only use the Ready code.
          code: "InvalidRow",
        }),
      );

      expect(malformedSummaryStatus.message).toMatch(/Invalid event/);

      const malformedDecodedSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          status: "ready",
          // @ts-expect-error hostile wire status can use an invalid ready code.
          code: "InvalidRow",
        }),
      );

      expect(malformedDecodedSummaryStatus.message).toMatch(/Invalid system event/);

      const wrongTopicDecodeTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-detail",
          status: "ready",
          code: "Ready",
        }),
      );

      expect(wrongTopicDecodeTopic.message).toBe(
        "Received event for __view_server_health_summary while subscribed to __view_server_health",
      );

      const validTopicStatus = yield* viewServerDecodeHealthTopicEvent(viewServer, {
        type: "status",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        status: "ready",
        code: "Ready",
      });

      expect(validTopicStatus).toStrictEqual({
        type: "status",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        status: "ready",
        code: "Ready",
      });

      const malformedTopicStatus = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          status: "ready",
          // @ts-expect-error ready detail status can only use the Ready code.
          code: "InvalidRow",
        }),
      );

      expect(malformedTopicStatus.message).toMatch(/Invalid event/);

      const malformedDecodedTopicStatus = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          status: "ready",
          // @ts-expect-error hostile wire status can use an invalid ready code.
          code: "InvalidRow",
        }),
      );

      expect(malformedDecodedTopicStatus.message).toMatch(/Invalid system event/);

      const invalidHealthSummaryRow = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: 1,
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryRow.message).toMatch(/Invalid system row/);

      const missingSummaryTopics = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [{ id: "summary", updatedAtNanos: 1 }],
          totalRows: 1,
        }),
      );

      expect(missingSummaryTopics.message).toMatch(/Invalid system row/);

      const missingDeltaSummaryTopics = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "update", key: "summary", row: { id: "summary" }, index: 0 }],
          totalRows: 1,
        }),
      );

      expect(missingDeltaSummaryTopics.message).toMatch(/Invalid system row/);

      const unknownSummaryTopic = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
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
              unhealthyTopics: ["missing"],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(unknownSummaryTopic.message).toBe("Health payload references unknown topic: missing");

      const wrongSummaryKey = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["not-summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(wrongSummaryKey.message).toBe("Health summary keys must be exactly: summary");

      const wrongSummaryRowCount = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [],
          totalRows: 0,
        }),
      );

      expect(wrongSummaryRowCount.message).toBe("Health summary must contain exactly one row");

      const wrongSummaryTotalRows = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 2,
        }),
      );

      expect(wrongSummaryTotalRows.message).toBe("Health summary must contain exactly one row");

      const inconsistentSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "degraded",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(inconsistentSummaryStatus.message).toBe(
        "Health summary status does not match runtime/connection status: ready != degraded",
      );

      const connectedSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "connected",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(connectedSummaryStatus.message).toMatch(/Invalid system row/);

      const wrongSummaryDeltaKey = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "remove",
              key: "not-summary",
            },
          ],
          totalRows: 1,
        }),
      );

      expect(wrongSummaryDeltaKey.message).toBe("Health summary delta key must be: summary");

      const mismatchedSummaryDeltaRow = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "summary",
              row: {
                id: "not-summary",
                status: "ready",
                runtimeStatus: "ready",
                connectionStatus: "connected",
                unhealthyTopics: [],
                updatedAtNanos: "1",
                maxKafkaLag: "0",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(mismatchedSummaryDeltaRow.message).toBe(
        "Health summary delta key does not match row id: summary != not-summary",
      );

      const unknownDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson", "missing"],
          rows: [
            {
              id: "orders",
              status: "ready",
              rowCount: 0,
              liveRowCount: 0,
              deletedRowCount: 0,
              version: 0,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 0,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
              activeSubscriptions: 0,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
            {
              id: "badjson",
              status: "ready",
              rowCount: 0,
              liveRowCount: 0,
              deletedRowCount: 0,
              version: 0,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 0,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
              activeSubscriptions: 0,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
            {
              id: "missing",
              status: "ready",
              rowCount: 0,
              liveRowCount: 0,
              deletedRowCount: 0,
              version: 0,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 0,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
              activeSubscriptions: 0,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
              kafkaLag: 0,
              updatedAtNanos: 1,
            },
          ],
          totalRows: 2,
        }),
      );

      expect(unknownDetailTopic.message).toBe("Health payload references unknown topic: missing");
    }),
  );
});
