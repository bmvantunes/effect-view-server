import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
  viewServerReservedTopicNames,
  viewServerTopicNameIsReserved,
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

import { sourceTopicHealth } from "../test-harness/health";
import { viewServer } from "../test-harness/live-query";

const transportHealth = {
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
} as const;

describe("Health contracts", () => {
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
    const topicHealth: TopicRuntimeHealth = sourceTopicHealth("ready", 1);
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
      sources: {},
      transport: transportHealth,
    };
    const backpressure: StatusEvent = {
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    };

    expect(snapshot.rows[0]).toStrictEqual({ id: "order-1" });
    expect(health.engine.topics.orders.rowCount).toBe(1);
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

  it("derives canonical pushed health summary and detailed rows", () => {
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
      sources: {},
      transport: transportHealth,
    };

    expect(viewServerHealthSummaryFromHealth(health, 123n)).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
    });
    expect(viewServerHealthSummaryRowFromHealth(health, 123n)).toStrictEqual({
      id: "summary",
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
    });
    expect(viewServerHealthTopicRowsFromHealth(health, 123n)).toStrictEqual([
      {
        id: "orders",
        ...sourceTopicHealth("ready", 10),
        updatedAtNanos: 123n,
      },
      {
        id: "trades",
        ...sourceTopicHealth("degraded", 20),
        updatedAtNanos: 123n,
      },
      {
        id: "positions",
        ...sourceTopicHealth("starting", 30),
        updatedAtNanos: 123n,
      },
    ]);

    const stoppingHealth: ViewServerHealth<typeof viewServer.topics> = {
      ...health,
      status: "stopping",
    };
    expect(
      viewServerHealthTopicRowsFromHealth(stoppingHealth, 124n).map((row) => row.status),
    ).toStrictEqual(["stopping", "stopping", "stopping"]);
  });

  it("reserves the two health system topics and preserves exact public row types", () => {
    expect(viewServerReservedTopicNames).toStrictEqual([
      VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      VIEW_SERVER_HEALTH_TOPIC,
    ]);
    expect([
      viewServerTopicNameIsReserved(VIEW_SERVER_HEALTH_SUMMARY_TOPIC),
      viewServerTopicNameIsReserved(VIEW_SERVER_HEALTH_TOPIC),
      viewServerTopicNameIsReserved("orders"),
    ]).toStrictEqual([true, true, false]);
    expectTypeOf<
      ViewServerHealthSummary<typeof viewServer.topics>["unhealthyTopics"]
    >().toEqualTypeOf<ReadonlyArray<"orders" | "trades" | "positions">>();
    expectTypeOf<
      ViewServerHealthSummaryRow<typeof viewServer.topics>["id"]
    >().toEqualTypeOf<"summary">();
    expectTypeOf<ViewServerHealthTopicRow<"orders">["id"]>().toEqualTypeOf<"orders">();
    expectTypeOf<ViewServerHealthDetails<"orders">["rows"]>().toEqualTypeOf<
      ReadonlyArray<ViewServerHealthTopicRow<"orders">>
    >();
  });
});
