import { describe, expect, it } from "@effect/vitest";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerHealth,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { Effect, Schema } from "effect";
import { makeViewServerWebSocketServer } from "./index";
import { viewServerHealthMetrics } from "./metrics-route";
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

const SourceFailure = Schema.TaggedStruct("MetricsSourceFailure", {
  message: Schema.String,
});
const SourceAdapterMetrics = Schema.Struct({
  opaqueSequence: Schema.BigInt,
});
const SourceRejectionLocation = Schema.Struct({
  offset: Schema.BigInt,
});
const sourceAdapter = SourceAdapter.make({
  identity: {
    name: 'source"adapter\\name',
    version: "1",
  },
  failure: SourceFailure,
  materialized: {
    metrics: SourceAdapterMetrics,
    rejectionLocation: SourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
  leased: {
    metrics: SourceAdapterMetrics,
    rejectionLocation: SourceRejectionLocation,
    definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
  },
});
const sourceMetricsConfig = defineViewServerConfig({
  topics: {
    manual: {
      schema: Schema.Struct({ id: ViewServerId, price: Schema.Number }),
    },
    materialized: {
      schema: Schema.Struct({ id: ViewServerId, price: Schema.Number }),
      source: sourceAdapter.materializedSource({ label: "materialized" }),
    },
    leased: {
      schema: Schema.Struct({ id: ViewServerId, price: Schema.Number }),
      source: sourceAdapter.leasedSource(["id"], { label: "leased" }),
    },
  },
});

const materializedSourceHealth = {
  adapter: sourceAdapter.identity,
  target: {
    _tag: "Materialized",
  },
  status: {
    _tag: "Ready",
    attempt: 1n,
    readyAtNanos: 2n,
  },
  metrics: {
    runtime: {
      startedAtNanos: 3n,
      lastAttemptStartedAtNanos: 4n,
      lastDeliveryAtNanos: null,
      lastRejectionAtNanos: null,
      lastAppliedMutationAtNanos: null,
      lastTerminationAtNanos: null,
      currentAttempt: 1n,
      retryCount: 1n,
      receivedDeliveryCount: 2n,
      rejectedItemCount: 3n,
      attemptedMutationCount: 4n,
      appliedUpsertCount: 5n,
      appliedDeleteCount: 6n,
      failedMutationCount: 7n,
      completedSettlementCount: 8n,
      failedSettlementCount: 9n,
      retainedRowCount: 10,
      lanes: [
        {
          id: "materialized",
          buffer: {
            _tag: "Bounded",
            capacity: 16,
            depth: 2,
            highWaterMark: 4,
            overflowCount: 11n,
          },
        },
      ],
    },
    adapter: {
      opaqueSequence: 999n,
    },
  },
  sampledAtNanos: 12n,
} as const;

const firstLeasedSourceHealth = {
  adapter: sourceAdapter.identity,
  target: {
    _tag: "Leased",
    route: {
      id: "first",
    },
  },
  status: {
    _tag: "Ready",
    attempt: 2n,
    readyAtNanos: 20n,
  },
  metrics: {
    runtime: {
      ...materializedSourceHealth.metrics.runtime,
      startedAtNanos: 20n,
      lastAttemptStartedAtNanos: 10n,
      lastDeliveryAtNanos: 10n,
      lastRejectionAtNanos: 10n,
      lastAppliedMutationAtNanos: 10n,
      lastTerminationAtNanos: 10n,
      currentAttempt: 2n,
      retainedRowCount: 20,
      lanes: [
        {
          id: "first",
          buffer: {
            _tag: "Unbuffered",
          },
        },
      ],
    },
    adapter: {
      opaqueSequence: 1_000n,
    },
  },
  sampledAtNanos: 21n,
} as const;

const secondLeasedSourceHealth = {
  adapter: sourceAdapter.identity,
  target: {
    _tag: "Leased",
    route: {
      id: "second",
    },
  },
  status: {
    _tag: "Ready",
    attempt: 1n,
    readyAtNanos: 22n,
  },
  metrics: {
    runtime: {
      ...materializedSourceHealth.metrics.runtime,
      startedAtNanos: 10n,
      lastAttemptStartedAtNanos: 20n,
      lastDeliveryAtNanos: null,
      lastRejectionAtNanos: 9n,
      lastAppliedMutationAtNanos: 11n,
      lastTerminationAtNanos: 10n,
      currentAttempt: 1n,
      retainedRowCount: 30,
      lanes: [
        {
          id: "second",
          buffer: {
            _tag: "Bounded",
            capacity: 32,
            depth: 3,
            highWaterMark: 5,
            overflowCount: 12n,
          },
        },
      ],
    },
    adapter: {
      opaqueSequence: 1_001n,
    },
  },
  sampledAtNanos: 23n,
} as const;

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

  it.live("renders degraded canonical runtime and engine health metrics", () =>
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
      expect(lines).toContain('view_server_runtime_status{status="degraded"} 1');
      expect(lines).toContain('view_server_engine_topic_rows{topic="orders",state="total"} 0');
      expect(lines.some((line) => line.startsWith("view_server_kafka_"))).toBe(false);
      expect(lines.some((line) => line.startsWith("view_server_grpc_"))).toBe(false);

      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.effect("projects fixed low-cardinality Source metrics and aggregates leased instances", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const topic = baseHealth.engine.topics.orders;
      const health: ViewServerHealth<typeof sourceMetricsConfig.topics> = {
        ...baseHealth,
        engine: {
          topics: {
            manual: topic,
            materialized: topic,
            leased: topic,
          },
        },
        sources: {
          materialized: materializedSourceHealth,
          leased: [firstLeasedSourceHealth, secondLeasedSourceHealth],
        },
      };

      const lines = viewServerHealthMetrics(sourceMetricsConfig, health).trimEnd().split("\n");

      expect(lines).toContain(
        'view_server_source_active_instances{topic="materialized",adapter="source\\"adapter\\\\name",lifecycle="materialized"} 1',
      );
      expect(lines).toContain(
        'view_server_source_active_instances{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 2',
      );
      expect(lines).toContain(
        'view_server_source_status{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased",status="ready"} 2',
      );
      expect(lines).toContain(
        'view_server_source_started_at_nanos{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 20',
      );
      expect(lines).toContain(
        'view_server_source_last_attempt_started_at_nanos{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 20',
      );
      expect(lines).toContain(
        'view_server_source_last_delivery_at_nanos{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 10',
      );
      expect(lines).toContain(
        'view_server_source_last_rejection_at_nanos{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 10',
      );
      expect(lines).toContain(
        'view_server_source_last_applied_mutation_at_nanos{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 11',
      );
      expect(lines).toContain(
        'view_server_source_current_attempt{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 2',
      );
      expect(lines).toContain(
        'view_server_source_retained_rows{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 50',
      );
      expect(lines).toContain(
        'view_server_source_delivery_lanes{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 2',
      );
      expect(lines).toContain(
        'view_server_source_bounded_buffer_lanes{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 1',
      );
      expect(lines).toContain(
        'view_server_source_buffer_overflows_total{topic="leased",adapter="source\\"adapter\\\\name",lifecycle="leased"} 12',
      );
      expect(lines.some((line) => line.includes("opaqueSequence"))).toBe(false);
      expect(lines.some((line) => line.includes('id="first"'))).toBe(false);

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
