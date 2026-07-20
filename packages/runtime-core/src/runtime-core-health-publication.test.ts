import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";
import { makeViewServerRuntimeCore } from "./index";
import { order, viewServer } from "./runtime-core-test-fixtures";

describe("@effect-view-server/runtime-core", () => {
  it.effect("pushes summary and detailed health snapshots", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "0 millis",
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const detailFiber = yield* detail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtimeCore.client.publish("orders", order("a", 10));

      const summaryEvents = yield* Fiber.join(summaryFiber);
      const detailEvents = yield* Fiber.join(detailFiber);
      expect(Array.from(summaryEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: null,
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
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
              updatedAtNanos: expect.anything(),
              maxKafkaLag: null,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
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
              kafkaLag: null,
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 1,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "ready",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: expect.anything(),
              mutationsPerSecond: 1,
              rowsPerSecond: 1,
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
              kafkaLag: null,
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* summary.close();
      yield* summary.close();
      yield* detail.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("applies health overlays to pushed health subscriptions", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health) => ({
          ...health,
          status: "degraded",
          kafka: {
            startFrom: {
              consumerGroupId: "view-server-test",
              fallbackMode: "earliest",
              mode: "committed",
            },
            regions: {
              local: {
                status: "connected",
                brokers: "localhost:9092",
                lastConnectedAt: 1_000,
                lastError: null,
              },
            },
            topics: {
              sourceOrders: {
                status: "stalled",
                sourceTopic: "orders-source",
                viewServerTopic: "orders",
                regions: {
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    publishFailuresPerSecond: 0,
                    commitFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: 7n,
                    lagSampledAt: null,
                    committedOffset: "3",
                    lastError: "lagging",
                  },
                },
              },
            },
          },
        }),
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryEvents = yield* summary.events.pipe(Stream.take(1), Stream.runCollect);
      const detailEvents = yield* detail.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(summaryEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "degraded",
              runtimeStatus: "degraded",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 7n,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "degraded",
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
              kafkaLag: 7n,
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* summary.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );
});
