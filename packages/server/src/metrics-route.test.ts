import { describe, expect, it } from "@effect/vitest";
import type { ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect } from "effect";
import { makeViewServerWebSocketServer } from "./index";
import {
  bearerAuth,
  createServerTestRuntime,
  degradedServerHealth,
  fetchJson,
  fetchText,
  fetchTextWithAuthorization,
  order,
  viewServer,
} from "../test-harness/server";

describe("Real View Server metrics route", () => {
  it.live("serves GET /metrics beside the websocket RPC endpoint", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);

      yield* inMemory.client.publish("orders", order("a", 10));

      const metrics = yield* fetchText(server.metricsUrl);
      const lines = metrics.text.trimEnd().split("\n");

      expect(metrics.response.status).toBe(200);
      expect(metrics.response.headers.get("content-type")).toBe(
        "text/plain; version=0.0.4; charset=utf-8",
      );
      expect(lines).toContain("# TYPE view_server_runtime_status gauge");
      expect(lines).toContain("# TYPE view_server_runtime_version gauge");
      expect(lines).toContain("# TYPE view_server_transport_backpressure_events gauge");
      expect(lines).toContain("# TYPE view_server_engine_topic_grouped_evaluations gauge");
      expect(lines).toContain("# TYPE view_server_engine_topic_backpressure_events gauge");
      expect(lines).toContain("# TYPE view_server_grpc_feed_reconnects gauge");
      expect(lines).toContain('view_server_runtime_status{status="ready"} 1');
      expect(lines).toContain('view_server_engine_topic_rows{topic="orders",state="total"} 1');
      expect(lines).toContain('view_server_engine_topic_rows{topic="orders",state="live"} 1');
      expect(lines).toContain("view_server_transport_active_clients 0");

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("returns fallback metrics when runtime health fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const healthError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "health unavailable",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.fail(healthError),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

      const metrics = yield* fetchText(server.metricsUrl);

      expect(metrics.response.status).toBe(200);
      expect(metrics.text).toBe("view_server_metrics_error 1\n");

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("renders degraded Kafka and gRPC health metrics", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const degradedHealth = degradedServerHealth(baseHealth);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () => Effect.succeed(degradedHealth),
        },
      });
      yield* Effect.addFinalizer(() => server.close);

      const metrics = yield* fetchText(server.metricsUrl);
      const lines = metrics.text.trimEnd().split("\n");

      expect(metrics.response.status).toBe(200);
      expect(lines).toContain(
        'view_server_kafka_region_connected{region="usa",sourceTopic="source_orders",viewServerTopic="orders"} 1',
      );
      expect(lines).toContain(
        'view_server_kafka_bytes_per_second{region="london",sourceTopic="source_orders",viewServerTopic="orders"} 70',
      );
      expect(lines).toContain(
        'view_server_kafka_processing_failures_per_second{region="london",sourceTopic="source_orders",viewServerTopic="orders"} 5',
      );
      expect(lines).toContain(
        'view_server_kafka_consumer_lag_messages{region="usa",sourceTopic="source_orders",viewServerTopic="orders"} 42',
      );
      expect(lines).toContain(
        'view_server_kafka_region_connected{region="london",sourceTopic="source_orders",viewServerTopic="orders"} 0',
      );
      expect(
        lines.filter((line) =>
          line.startsWith(
            'view_server_kafka_consumer_lag_messages{region="london",sourceTopic="source_orders",viewServerTopic="orders"}',
          ),
        ),
      ).toStrictEqual([]);
      expect(lines).toContain(
        'view_server_grpc_feed_rows{lifecycle="materialized",topic="orders",feed="ordersFeed"} 5',
      );
      expect(lines).toContain(
        'view_server_grpc_client_active_feeds{client="ordersClient",baseUrl="http://127.0.0.1:8080"} 3',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_rows{lifecycle="leased",topic="orders",feed="ordersLease"} 10',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_subscribers{lifecycle="leased",topic="orders",feed="ordersLease"} 3',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_messages_per_second{lifecycle="leased",topic="orders",feed="ordersLease"} 10',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_rows_per_second{lifecycle="leased",topic="orders",feed="ordersLease"} 8',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_mapping_failures_per_second{lifecycle="leased",topic="orders",feed="ordersLease"} 2',
      );
      expect(lines).toContain(
        'view_server_grpc_feed_reconnects{lifecycle="leased",topic="orders",feed="ordersLease"} 4',
      );
      expect(
        lines.filter((line) =>
          line.startsWith('view_server_grpc_feed_rows{lifecycle="leased",topic="orders"'),
        ),
      ).toStrictEqual([
        'view_server_grpc_feed_rows{lifecycle="leased",topic="orders",feed="ordersLease"} 10',
      ]);
      expect(
        lines.filter((line) =>
          /^view_server_grpc_feed_[a-z_]+\{[^}]*feed="ordersLease:strategy=[^"]+"[^}]*\} -?\d+(?:\.\d+)?$/.test(
            line,
          ),
        ),
      ).toStrictEqual([]);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("requires auth for GET /metrics when an auth validator is configured", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        auth: bearerAuth,
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);

      const deniedMetrics = yield* fetchJson(server.metricsUrl);
      const acceptedMetrics = yield* fetchTextWithAuthorization(
        server.metricsUrl,
        "Bearer view-server-test",
      );

      expect(deniedMetrics.response.status).toBe(401);
      expect(deniedMetrics.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });
      expect(acceptedMetrics.response.status).toBe(200);
      expect(acceptedMetrics.text.trimEnd().split("\n")).toContain(
        "# TYPE view_server_runtime_status gauge",
      );

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );
});
