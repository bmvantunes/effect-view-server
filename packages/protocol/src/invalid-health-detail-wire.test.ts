import { describe, expect, it } from "@effect/vitest";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthTopicRow,
} from "@effect-view-server/config";
import { Effect } from "effect";
import {
  viewServerDecodeHealthQuery,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
} from "./index";

import { viewServer } from "../test-harness/protocol";

describe("Invalid health detail wire inputs", () => {
  it.effect("rejects inconsistent health detail events", () =>
    Effect.gen(function* () {
      const partialDetailTopicSnapshot = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders"],
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
          ],
          totalRows: 2,
        }),
      );

      expect(partialDetailTopicSnapshot.message).toBe(
        "Health topic snapshot keys is missing topic: badjson",
      );

      const wrongDetailTopicSnapshotTotalRows = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
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
          ],
          totalRows: 999,
        }),
      );

      expect(wrongDetailTopicSnapshotTotalRows.message).toBe(
        "Health topic snapshot totalRows must equal configured topic count: 999 != 2",
      );

      const duplicateDetailTopicKeys = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "orders"],
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
          ],
          totalRows: 2,
        }),
      );

      expect(duplicateDetailTopicKeys.message).toBe(
        "Health topic snapshot keys contains duplicate topic: orders",
      );

      const mismatchedDetailTopicKey = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
          rows: [
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
          ],
          totalRows: 2,
        }),
      );

      expect(mismatchedDetailTopicKey.message).toBe(
        "Health topic snapshot key does not match row id: orders != badjson",
      );

      const nonStringDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
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
              id: 1,
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
          ],
          totalRows: 2,
        }),
      );

      expect(nonStringDetailTopic.message).toMatch(/Invalid system row/);

      const nonStringDeltaDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
                id: 1,
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
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );

      expect(nonStringDeltaDetailTopic.message).toMatch(/Invalid system row/);

      const mismatchedDeltaDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
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
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );

      expect(mismatchedDeltaDetailTopic.message).toBe(
        "Health topic delta key does not match row id: orders != badjson",
      );

      const wrongDetailTopicDeltaTotalRows = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "orders" }],
          totalRows: 1,
        }),
      );

      expect(wrongDetailTopicDeltaTotalRows.message).toBe(
        "Health topic delta totalRows must equal configured topic count: 1 != 2",
      );

      const malformedHealthQuery = yield* Effect.flip(
        viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, { select: ["rowCount"] }),
      );

      expect(malformedHealthQuery.message).toBe("Health query select must be exactly: id");

      const extraHealthQueryKey = yield* Effect.flip(
        viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, {
          select: ["id"],
          limit: 1,
        }),
      );

      expect(extraHealthQueryKey.code).toBe("InvalidQuery");

      const invalidHealthSummaryEncodeRow = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
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
              // @ts-expect-error hostile callers can pass invalid system row values.
              updatedAtNanos: "1",
              // @ts-expect-error hostile callers can pass invalid system row values.
              maxKafkaLag: 1,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryEncodeRow.message).toMatch(/Invalid system row/);

      const missingHealthSummaryEncodeTopics = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            // @ts-expect-error hostile callers can omit required system row values.
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(missingHealthSummaryEncodeTopics.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryEncodeKey = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          // @ts-expect-error hostile callers can pass invalid summary snapshot keys.
          keys: ["not-summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryEncodeKey.message).toBe(
        "Health summary keys must be exactly: summary",
      );

      const unknownHealthSummaryEncodeTopic = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
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
              // @ts-expect-error hostile callers can pass unknown unhealthy topics.
              unhealthyTopics: ["missing"],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(unknownHealthSummaryEncodeTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const invalidHealthSummaryEncodeRowId = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              // @ts-expect-error hostile callers can pass invalid system row ids.
              id: "not-summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryEncodeRowId.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryEncodeVersion = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          // @ts-expect-error hostile callers can pass malformed snapshot metadata.
          version: "not-a-number",
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryEncodeVersion.message).toMatch(/Invalid event/);

      const invalidHealthSummaryEncodeDeltaKey = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "move",
              // @ts-expect-error hostile callers can pass invalid summary delta keys.
              key: "not-summary",
              fromIndex: 0,
              toIndex: 0,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryEncodeDeltaKey.message).toBe(
        "Health summary delta key must be: summary",
      );

      const mismatchedHealthSummaryEncodeDeltaRow = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
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
                // @ts-expect-error hostile callers can pass invalid system row ids.
                id: "not-summary",
                status: "ready",
                runtimeStatus: "ready",
                connectionStatus: "connected",
                unhealthyTopics: [],
                updatedAtNanos: 1n,
                maxKafkaLag: 0n,
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(mismatchedHealthSummaryEncodeDeltaRow.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryDecodeRemove = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "summary" }],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryDecodeRemove.message).toBe(
        "Health summary delta cannot remove summary",
      );

      const invalidHealthSummaryDecodeInsert = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: "summary",
              row: {
                id: "summary",
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

      expect(invalidHealthSummaryDecodeInsert.message).toBe(
        "Health summary delta cannot insert summary",
      );

      const invalidHealthSummaryDecodeIndex = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "move", key: "summary", fromIndex: 0, toIndex: -1 }],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryDecodeIndex.message).toBe(
        "Health summary move to index must be 0: -1",
      );

      const invalidHealthSummaryDecodeDeltaTopic = yield* Effect.flip(
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
                id: "summary",
                status: "degraded",
                runtimeStatus: "degraded",
                connectionStatus: "connected",
                unhealthyTopics: ["missing"],
                updatedAtNanos: "1",
                maxKafkaLag: "0",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );

      expect(invalidHealthSummaryDecodeDeltaTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const encodedHealthTopicRow: ViewServerHealthTopicRow<"orders"> = {
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
        kafkaLag: 0n,
        updatedAtNanos: 1n,
      };

      const partialHealthTopicEncodeSnapshot = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders"],
          rows: [encodedHealthTopicRow],
          totalRows: 2,
        }),
      );

      expect(partialHealthTopicEncodeSnapshot.message).toBe(
        "Health topic snapshot keys is missing topic: badjson",
      );

      const invalidHealthTopicEncodeVersion = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          // @ts-expect-error hostile callers can pass malformed snapshot metadata.
          version: "not-a-number",
          keys: ["orders", "badjson"],
          rows: [encodedHealthTopicRow, { ...encodedHealthTopicRow, id: "badjson" }],
          totalRows: 2,
        }),
      );

      expect(invalidHealthTopicEncodeVersion.message).toMatch(/Invalid event/);

      const mismatchedHealthTopicEncodeSnapshot = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["badjson", "orders"],
          rows: [encodedHealthTopicRow, { ...encodedHealthTopicRow, id: "badjson" }],
          totalRows: 2,
        }),
      );

      expect(mismatchedHealthTopicEncodeSnapshot.message).toBe(
        "Health topic snapshot key does not match row id: badjson != orders",
      );

      const mismatchedHealthTopicEncodeDelta = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
                ...encodedHealthTopicRow,
                // @ts-expect-error hostile callers can pass a valid but mismatched topic row id.
                id: "badjson",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );

      expect(mismatchedHealthTopicEncodeDelta.message).toBe(
        "Health topic delta key does not match row id: orders != badjson",
      );

      const invalidHealthTopicDecodeRemove = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "orders" }],
          totalRows: 2,
        }),
      );

      expect(invalidHealthTopicDecodeRemove.message).toBe(
        "Health topic delta cannot remove configured topic: orders",
      );

      const invalidHealthTopicDecodeInsert = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: "orders",
              row: {
                ...encodedHealthTopicRow,
                kafkaLag: "0",
                updatedAtNanos: "1",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );

      expect(invalidHealthTopicDecodeInsert.message).toBe(
        "Health topic delta cannot insert configured topic: orders",
      );

      const invalidHealthTopicDecodeIndex = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "move", key: "orders", fromIndex: 99, toIndex: 0 }],
          totalRows: 2,
        }),
      );

      expect(invalidHealthTopicDecodeIndex.message).toBe(
        "Health topic move from index must be within configured topic count: 99",
      );
    }),
  );
});
