import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerReservedTopicNames,
  viewServerTopicNameIsReserved,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
  type GrpcTopicFeedsHealth,
  type KafkaMessageMetadata,
  type KafkaStartFromHealth,
  type KafkaTopicHealth,
  type KafkaTopicRegionHealth,
  type LiveSubscription,
  type LiveTransportAdapter,
  type SnapshotEvent,
  type StatusEvent,
  type TopicRuntimeHealth,
  type ViewServerHealth,
  type ViewServerHealthDetails,
  type ViewServerHealthSummary,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
  type ViewServerTransportError,
} from "./index";

import {
  kafkaCommittedFailStartFromHealth,
  kafkaEarliestStartFromHealth,
  kafkaLatestStartFromHealth,
  kafkaStartFromHealth,
  sourceTopicHealth,
} from "../test-harness/health";
import { viewServer } from "../test-harness/live-query";

describe("Health contracts", () => {
  it("types only normalized start policy combinations", () => {
    expectTypeOf<{
      readonly consumerGroupId: "view-server-invalid-latest-fail";
      readonly fallbackMode: "fail";
      readonly mode: "latest";
    }>().not.toMatchTypeOf<KafkaStartFromHealth>();
    expect(kafkaLatestStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-latest",
      fallbackMode: "latest",
      mode: "latest",
    });
    expect(kafkaEarliestStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-earliest",
      fallbackMode: "earliest",
      mode: "earliest",
    });
    expect(kafkaCommittedFailStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-committed",
      fallbackMode: "fail",
      mode: "committed",
    });
  });

  it("exposes health and transport contracts", () => {
    const snapshot: SnapshotEvent<{ readonly id: string }> = {
      type: "snapshot",
      topic: "orders",
      queryId: "query-1",
      version: 1,
      keys: ["order-1"],
      rows: [{ id: "order-1" }],
      totalRows: 1,
    };

    const metadata: KafkaMessageMetadata<"usa"> = {
      sourceTopic: "orders",
      sourceRegion: "usa",
      partition: 0,
      offset: "1",
      timestamp: null,
      headers: {},
    };

    const topicHealth: TopicRuntimeHealth = {
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
    };

    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: topicHealth,
          trades: topicHealth,
          positions: topicHealth,
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };

    const backpressure: StatusEvent = {
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    };

    expect(snapshot.rows[0]).toStrictEqual({
      id: "order-1",
    });
    expect(metadata.sourceRegion).toBe("usa");
    expect(health.engine.topics["orders"].rowCount).toBe(1);
    expect(backpressure).toStrictEqual({
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    });
    expectTypeOf<LiveTransportAdapter>().toHaveProperty("subscribe");
    expectTypeOf<Effect.Success<ReturnType<LiveTransportAdapter["subscribe"]>>>().toEqualTypeOf<
      LiveSubscription<unknown>
    >();
    expectTypeOf<
      Effect.Error<ReturnType<LiveTransportAdapter["subscribe"]>>
    >().toEqualTypeOf<ViewServerTransportError>();
  });

  it("derives pushed health summary and detailed rows from runtime health", () => {
    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "degraded",
      version: 7,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: sourceTopicHealth("ready", 10),
          trades: sourceTopicHealth("degraded", 20),
          positions: sourceTopicHealth("starting", 30),
        },
      },
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          sourceOrders: {
            status: "degraded",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 10,
                bytesPerSecond: 100,
                decodedMessagesPerSecond: 10,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 5n,
                lagSampledAt: null,
                committedOffset: "5",
                lastError: "decode failed",
              },
              london: {
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
                consumerLagMessages: 3n,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceOrdersUnknownLag: {
            status: "ready",
            sourceTopic: "orders-source-unknown-lag",
            viewServerTopic: "orders",
            regions: {
              usa: {
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
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceTrades: {
            status: "stalled",
            sourceTopic: "trades-source",
            viewServerTopic: "trades",
            regions: {
              usa: {
                connected: false,
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
                consumerLagMessages: 11n,
                lagSampledAt: null,
                committedOffset: "9",
                lastError: "stalled",
              },
            },
          },
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };

    const summary = viewServerHealthSummaryFromHealth(health, 123n);
    const summaryRow = viewServerHealthSummaryRowFromHealth(health, 123n);
    const rows = viewServerHealthTopicRowsFromHealth(health, 123n);
    const healthWithoutKafka: ViewServerHealth<typeof viewServer.topics> = {
      status: health.status,
      version: health.version,
      uptimeMs: health.uptimeMs,
      engine: health.engine,
      transport: health.transport,
    };
    const grpcOnlyHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "degraded",
      version: 9,
      uptimeMs: 300,
      engine: {
        topics: {
          orders: sourceTopicHealth("ready", 10),
          trades: sourceTopicHealth("ready", 20),
          positions: sourceTopicHealth("ready", 30),
        },
      },
      grpc: {
        clients: {
          ordersClient: {
            status: "connected",
            baseUrl: "http://localhost:8080",
            activeFeeds: 3,
            lastConnectedAt: null,
            lastError: null,
          },
        },
        feeds: {
          orders: {
            materialized: {
              ordersFeed: {
                status: "ready",
                lifecycle: "materialized",
                feedName: "ordersFeed",
                feedKey: "ordersFeed",
                topic: "orders",
                subscriberCount: 0,
                rowCount: 10,
                messagesPerSecond: 1,
                rowsPerSecond: 1,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                reconnects: 0,
                lastMessageAt: null,
                lastError: null,
              },
            },
            leased: {},
          },
          trades: {
            materialized: {},
            leased: {
              tradesFeed: {
                status: "starting",
                lifecycle: "leased",
                feedName: "tradesFeed",
                feedKey: "tradesFeed:region=usa",
                topic: "trades",
                subscriberCount: 1,
                rowCount: 0,
                messagesPerSecond: 0,
                rowsPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                reconnects: 0,
                lastMessageAt: null,
                lastError: null,
              },
            },
          },
          positions: {
            materialized: {
              positionsFeed: {
                status: "degraded",
                lifecycle: "materialized",
                feedName: "positionsFeed",
                feedKey: "positionsFeed",
                topic: "positions",
                subscriberCount: 0,
                rowCount: 0,
                messagesPerSecond: 0,
                rowsPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 1,
                publishFailuresPerSecond: 0,
                reconnects: 1,
                lastMessageAt: null,
                lastError: "mapping failed",
              },
            },
            leased: {},
          },
        },
      },
      transport: health.transport,
    };
    const kafkaStartingHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "starting",
      version: 8,
      uptimeMs: 200,
      engine: {
        topics: {
          orders: sourceTopicHealth("ready", 10),
          trades: sourceTopicHealth("ready", 20),
          positions: sourceTopicHealth("ready", 30),
        },
      },
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          sourceOrdersReady: {
            status: "ready",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
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
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceTradesStarting: {
            status: "starting",
            sourceTopic: "trades-source",
            viewServerTopic: "trades",
            regions: {
              usa: {
                connected: false,
                assignedPartitions: 0,
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
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
        },
      },
      transport: health.transport,
    };
    const orphanKafkaHealth: ViewServerHealth<typeof viewServer.topics> = {
      ...healthWithoutKafka,
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          orphanSource: {
            status: "ready",
            sourceTopic: "orphan-source",
            viewServerTopic: "orphan",
            regions: {
              usa: {
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
                consumerLagMessages: 99n,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
        },
      },
    };
    const stoppingRows = viewServerHealthTopicRowsFromHealth(
      {
        ...health,
        status: "stopping",
      },
      456n,
    );

    expect(summary).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["orders", "trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(summaryRow).toStrictEqual({
      id: "summary",
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["orders", "trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(rows).toStrictEqual([
      {
        id: "orders",
        status: "degraded",
        rowCount: 10,
        liveRowCount: 10,
        deletedRowCount: 0,
        version: 10,
        lastMutationAt: null,
        mutationsPerSecond: 10,
        rowsPerSecond: 10,
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
        updatedAtNanos: 123n,
      },
      {
        id: "trades",
        status: "degraded",
        rowCount: 20,
        liveRowCount: 20,
        deletedRowCount: 0,
        version: 20,
        lastMutationAt: null,
        mutationsPerSecond: 20,
        rowsPerSecond: 20,
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
        kafkaLag: 11n,
        updatedAtNanos: 123n,
      },
      {
        id: "positions",
        status: "starting",
        rowCount: 30,
        liveRowCount: 30,
        deletedRowCount: 0,
        version: 30,
        lastMutationAt: null,
        mutationsPerSecond: 30,
        rowsPerSecond: 30,
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
        updatedAtNanos: 123n,
      },
    ]);
    expect(viewServerHealthSummaryFromHealth(healthWithoutKafka, 123n).maxKafkaLag).toBe(null);
    expect(viewServerHealthSummaryFromHealth(orphanKafkaHealth, 123n).maxKafkaLag).toBe(null);
    expect(viewServerHealthTopicRowsFromHealth(orphanKafkaHealth, 123n)).toStrictEqual(
      viewServerHealthTopicRowsFromHealth(healthWithoutKafka, 123n),
    );
    expect(
      viewServerHealthTopicRowsFromHealth(healthWithoutKafka, 123n).map((row) => [
        row.id,
        row.status,
      ]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "degraded"],
      ["positions", "starting"],
    ]);
    expect(
      viewServerHealthTopicRowsFromHealth(kafkaStartingHealth, 123n).map((row) => [
        row.id,
        row.status,
      ]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "starting"],
      ["positions", "ready"],
    ]);
    expect(viewServerHealthSummaryFromHealth(kafkaStartingHealth, 123n)).toStrictEqual({
      status: "starting",
      runtimeStatus: "starting",
      connectionStatus: "connected",
      unhealthyTopics: ["trades"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(viewServerHealthSummaryFromHealth(grpcOnlyHealth, 123n)).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(
      viewServerHealthTopicRowsFromHealth(grpcOnlyHealth, 123n).map((row) => [row.id, row.status]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "starting"],
      ["positions", "degraded"],
    ]);
    expect(stoppingRows.map((row) => row.status)).toStrictEqual([
      "stopping",
      "stopping",
      "stopping",
    ]);
    expect({
      summary: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      detailed: VIEW_SERVER_HEALTH_TOPIC,
      detailedIsReserved: viewServerTopicNameIsReserved(VIEW_SERVER_HEALTH_TOPIC),
      ordersIsReserved: viewServerTopicNameIsReserved("orders"),
      all: viewServerReservedTopicNames,
    }).toStrictEqual({
      summary: "__view_server_health_summary",
      detailed: "__view_server_health",
      detailedIsReserved: true,
      ordersIsReserved: false,
      all: [VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC],
    });
    expectTypeOf(summary).toEqualTypeOf<ViewServerHealthSummary<typeof viewServer.topics>>();
    expectTypeOf(summary.maxKafkaLag).toEqualTypeOf<bigint | null>();
    expectTypeOf(summaryRow).toEqualTypeOf<ViewServerHealthSummaryRow<typeof viewServer.topics>>();
    expectTypeOf(summaryRow.maxKafkaLag).toEqualTypeOf<bigint | null>();
    expectTypeOf(rows[0]).toEqualTypeOf<
      ViewServerHealthTopicRow<"orders" | "trades" | "positions"> | undefined
    >();
    expectTypeOf(rows[0]?.kafkaLag).toEqualTypeOf<bigint | null | undefined>();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.orders).toEqualTypeOf<
      GrpcTopicFeedsHealth<"orders"> | undefined
    >();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.trades).toEqualTypeOf<
      GrpcTopicFeedsHealth<"trades"> | undefined
    >();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.positions).toEqualTypeOf<
      GrpcTopicFeedsHealth<"positions"> | undefined
    >();
    expectTypeOf<ViewServerHealthDetails<"orders">["status"]>().toEqualTypeOf<
      "ready" | "degraded" | "starting" | "stopping" | "connecting" | "disconnected"
    >();
  });

  it("derives Kafka lag only from Kafka sources mapped to engine topics", () => {
    const baseHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: sourceTopicHealth("ready", 10),
          trades: sourceTopicHealth("ready", 20),
          positions: sourceTopicHealth("ready", 30),
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };
    const regionHealth = (consumerLagMessages: bigint | null): KafkaTopicRegionHealth => ({
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
      consumerLagMessages,
      lagSampledAt: null,
      committedOffset: null,
      lastError: null,
    });
    const healthWithKafkaTopics = (
      topics: NonNullable<ViewServerHealth<typeof viewServer.topics>["kafka"]>["topics"],
    ): ViewServerHealth<typeof viewServer.topics> => ({
      ...baseHealth,
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics,
      },
    });
    const topicHealth = (
      viewServerTopic: string,
      consumerLagMessages: bigint | null,
    ): KafkaTopicHealth => ({
      status: "ready",
      sourceTopic: `${viewServerTopic}-source-${String(consumerLagMessages)}`,
      viewServerTopic,
      regions: {
        usa: regionHealth(consumerLagMessages),
      },
    });
    const lagSummary = (health: ViewServerHealth<typeof viewServer.topics>) => ({
      maxKafkaLag: viewServerHealthSummaryFromHealth(health, 123n).maxKafkaLag,
      topicLags: viewServerHealthTopicRowsFromHealth(health, 123n).map((row) => [
        row.id,
        row.kafkaLag,
      ]),
    });

    expect(lagSummary(baseHealth)).toStrictEqual({
      maxKafkaLag: null,
      topicLags: [
        ["orders", null],
        ["trades", null],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersZero: topicHealth("orders", 0n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 0n,
      topicLags: [
        ["orders", 0n],
        ["trades", null],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersLow: topicHealth("orders", 2n),
          ordersHigh: topicHealth("orders", 8n),
          trades: topicHealth("trades", 5n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 8n,
      topicLags: [
        ["orders", 8n],
        ["trades", 5n],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersKnown: topicHealth("orders", 0n),
          ordersUnknown: topicHealth("orders", null),
          trades: topicHealth("trades", 5n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: null,
      topicLags: [
        ["orders", null],
        ["trades", 5n],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          orders: topicHealth("orders", 4n),
          orphanKnown: topicHealth("orphan", 99n),
          orphanUnknown: topicHealth("orphan", null),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 4n,
      topicLags: [
        ["orders", 4n],
        ["trades", null],
        ["positions", null],
      ],
    });
  });
});
