import type {
  TopicDefinitions,
  ViewServerHealth,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import {
  ViewServerSourceRuntimeMetricsSchema,
  viewServerEncodeHealth,
} from "@effect-view-server/protocol";
import { Effect, Result, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { validateViewServerHttpRequest, viewServerAuthErrorResponse } from "./auth";
import type { ViewServerWebSocketServerInput } from "./server-types";

const metricContentType = "text/plain; version=0.0.4; charset=utf-8";

const escapeLabelValue = (value: string): string =>
  value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll('"', '\\"');

const labelsText = (labels: Readonly<Record<string, string>>): string => {
  const entries = Object.entries(labels);
  if (entries.length === 0) {
    return "";
  }
  return `{${entries.map(([key, value]) => `${key}="${escapeLabelValue(value)}"`).join(",")}}`;
};

const metricLine = (
  name: string,
  value: number | bigint,
  labels: Readonly<Record<string, string>> = {},
): string => `${name}${labelsText(labels)} ${value.toString()}`;

const booleanMetric = (value: boolean): number => (value ? 1 : 0);

const statusMetric = (status: string, expected: string): number =>
  booleanMetric(status === expected);

const compactLines = (lines: ReadonlyArray<string | undefined>): string =>
  `${lines.filter((line) => line !== undefined).join("\n")}\n`;

const nullableMetricLine = (
  name: string,
  value: bigint | null,
  labels: Readonly<Record<string, string>>,
): string | undefined => (value === null ? undefined : metricLine(name, value, labels));

type SourceMetricStatus =
  | "Starting"
  | "Ready"
  | "Degraded"
  | "WaitingToRetry"
  | "Reacquiring"
  | "Exhausted"
  | "Stopping";

const SourceMetricHealthSchema = Schema.Struct({
  status: Schema.Struct({
    _tag: Schema.Literals([
      "Starting",
      "Ready",
      "Degraded",
      "WaitingToRetry",
      "Reacquiring",
      "Exhausted",
      "Stopping",
    ]),
  }),
  metrics: Schema.Struct({
    runtime: ViewServerSourceRuntimeMetricsSchema,
  }),
});
type SourceMetricHealth = typeof SourceMetricHealthSchema.Type;

const validatedSourceMetricHealth = (candidate: unknown): SourceMetricHealth | undefined => {
  const decoded = Schema.decodeUnknownResult(SourceMetricHealthSchema)(candidate);
  return Result.isSuccess(decoded) ? decoded.success : undefined;
};

type SourceMetricTotals = {
  readonly statuses: Record<SourceMetricStatus, number>;
  activeInstances: number;
  startedAtNanos: bigint | null;
  lastAttemptStartedAtNanos: bigint | null;
  lastDeliveryAtNanos: bigint | null;
  lastRejectionAtNanos: bigint | null;
  lastAppliedMutationAtNanos: bigint | null;
  lastTerminationAtNanos: bigint | null;
  currentAttempt: bigint;
  retryCount: bigint;
  receivedDeliveryCount: bigint;
  rejectedItemCount: bigint;
  attemptedMutationCount: bigint;
  appliedUpsertCount: bigint;
  appliedDeleteCount: bigint;
  failedMutationCount: bigint;
  completedSettlementCount: bigint;
  failedSettlementCount: bigint;
  retainedRowCount: number;
  laneCount: number;
  boundedLaneCount: number;
  bufferCapacity: number;
  bufferDepth: number;
  bufferHighWaterMark: number;
  bufferOverflowCount: bigint;
};

const emptySourceMetricTotals = (): SourceMetricTotals => ({
  statuses: {
    Starting: 0,
    Ready: 0,
    Degraded: 0,
    WaitingToRetry: 0,
    Reacquiring: 0,
    Exhausted: 0,
    Stopping: 0,
  },
  activeInstances: 0,
  startedAtNanos: null,
  lastAttemptStartedAtNanos: null,
  lastDeliveryAtNanos: null,
  lastRejectionAtNanos: null,
  lastAppliedMutationAtNanos: null,
  lastTerminationAtNanos: null,
  currentAttempt: 0n,
  retryCount: 0n,
  receivedDeliveryCount: 0n,
  rejectedItemCount: 0n,
  attemptedMutationCount: 0n,
  appliedUpsertCount: 0n,
  appliedDeleteCount: 0n,
  failedMutationCount: 0n,
  completedSettlementCount: 0n,
  failedSettlementCount: 0n,
  retainedRowCount: 0,
  laneCount: 0,
  boundedLaneCount: 0,
  bufferCapacity: 0,
  bufferDepth: 0,
  bufferHighWaterMark: 0,
  bufferOverflowCount: 0n,
});

const maxNullableBigInt = (current: bigint | null, next: bigint | null): bigint | null =>
  next === null ? current : current === null || next > current ? next : current;

const addSourceMetricHealth = (totals: SourceMetricTotals, health: SourceMetricHealth): void => {
  const runtime = health.metrics.runtime;
  totals.activeInstances += 1;
  totals.statuses[health.status._tag] += 1;
  totals.startedAtNanos = maxNullableBigInt(totals.startedAtNanos, runtime.startedAtNanos);
  totals.lastAttemptStartedAtNanos = maxNullableBigInt(
    totals.lastAttemptStartedAtNanos,
    runtime.lastAttemptStartedAtNanos,
  );
  totals.lastDeliveryAtNanos = maxNullableBigInt(
    totals.lastDeliveryAtNanos,
    runtime.lastDeliveryAtNanos,
  );
  totals.lastRejectionAtNanos = maxNullableBigInt(
    totals.lastRejectionAtNanos,
    runtime.lastRejectionAtNanos,
  );
  totals.lastAppliedMutationAtNanos = maxNullableBigInt(
    totals.lastAppliedMutationAtNanos,
    runtime.lastAppliedMutationAtNanos,
  );
  totals.lastTerminationAtNanos = maxNullableBigInt(
    totals.lastTerminationAtNanos,
    runtime.lastTerminationAtNanos,
  );
  totals.currentAttempt =
    runtime.currentAttempt > totals.currentAttempt ? runtime.currentAttempt : totals.currentAttempt;
  totals.retryCount += runtime.retryCount;
  totals.receivedDeliveryCount += runtime.receivedDeliveryCount;
  totals.rejectedItemCount += runtime.rejectedItemCount;
  totals.attemptedMutationCount += runtime.attemptedMutationCount;
  totals.appliedUpsertCount += runtime.appliedUpsertCount;
  totals.appliedDeleteCount += runtime.appliedDeleteCount;
  totals.failedMutationCount += runtime.failedMutationCount;
  totals.completedSettlementCount += runtime.completedSettlementCount;
  totals.failedSettlementCount += runtime.failedSettlementCount;
  totals.retainedRowCount += runtime.retainedRowCount;
  totals.laneCount += runtime.lanes.length;
  for (const lane of runtime.lanes) {
    if (lane.buffer._tag === "Bounded") {
      totals.boundedLaneCount += 1;
      totals.bufferCapacity += lane.buffer.capacity;
      totals.bufferDepth += lane.buffer.depth;
      totals.bufferHighWaterMark += lane.buffer.highWaterMark;
      totals.bufferOverflowCount += lane.buffer.overflowCount;
    }
  }
};

const sourceMetricStatusLabels: Readonly<Record<SourceMetricStatus, string>> = {
  Starting: "starting",
  Ready: "ready",
  Degraded: "degraded",
  WaitingToRetry: "waiting_to_retry",
  Reacquiring: "reacquiring",
  Exhausted: "exhausted",
  Stopping: "stopping",
};

const sourceMetricStatuses: ReadonlyArray<SourceMetricStatus> = [
  "Starting",
  "Ready",
  "Degraded",
  "WaitingToRetry",
  "Reacquiring",
  "Exhausted",
  "Stopping",
];

export const viewServerHealthMetrics = <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  health: ViewServerHealth<Topics>,
): string => {
  const lines: Array<string | undefined> = [
    "# HELP view_server_runtime_status Runtime status as one-hot labels.",
    "# TYPE view_server_runtime_status gauge",
    metricLine("view_server_runtime_status", statusMetric(health.status, "ready"), {
      status: "ready",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "starting"), {
      status: "starting",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "degraded"), {
      status: "degraded",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "stopping"), {
      status: "stopping",
    }),
    "# HELP view_server_runtime_version Runtime health version.",
    "# TYPE view_server_runtime_version gauge",
    metricLine("view_server_runtime_version", health.version),
    "# HELP view_server_runtime_uptime_millis Runtime uptime in milliseconds.",
    "# TYPE view_server_runtime_uptime_millis gauge",
    metricLine("view_server_runtime_uptime_millis", health.uptimeMs),
    "# HELP view_server_transport_active_clients Active transport clients.",
    "# TYPE view_server_transport_active_clients gauge",
    metricLine("view_server_transport_active_clients", health.transport.activeClients),
    "# HELP view_server_transport_active_streams Active transport streams.",
    "# TYPE view_server_transport_active_streams gauge",
    metricLine("view_server_transport_active_streams", health.transport.activeStreams),
    "# HELP view_server_transport_active_subscriptions Active transport subscriptions.",
    "# TYPE view_server_transport_active_subscriptions gauge",
    metricLine("view_server_transport_active_subscriptions", health.transport.activeSubscriptions),
    "# HELP view_server_transport_queued_messages Queued transport messages.",
    "# TYPE view_server_transport_queued_messages gauge",
    metricLine("view_server_transport_queued_messages", health.transport.queuedMessages),
    "# HELP view_server_transport_queued_bytes Queued transport bytes.",
    "# TYPE view_server_transport_queued_bytes gauge",
    metricLine("view_server_transport_queued_bytes", health.transport.queuedBytes),
    "# HELP view_server_transport_messages_per_second Transport messages per second.",
    "# TYPE view_server_transport_messages_per_second gauge",
    metricLine("view_server_transport_messages_per_second", health.transport.messagesPerSecond),
    "# HELP view_server_transport_bytes_per_second Transport bytes per second.",
    "# TYPE view_server_transport_bytes_per_second gauge",
    metricLine("view_server_transport_bytes_per_second", health.transport.bytesPerSecond),
    "# HELP view_server_transport_dropped_clients Dropped transport clients.",
    "# TYPE view_server_transport_dropped_clients counter",
    metricLine("view_server_transport_dropped_clients", health.transport.droppedClients),
    "# HELP view_server_transport_backpressure_events Transport backpressure events.",
    "# TYPE view_server_transport_backpressure_events gauge",
    metricLine("view_server_transport_backpressure_events", health.transport.backpressureEvents),
    "# HELP view_server_transport_reconnects Transport reconnects.",
    "# TYPE view_server_transport_reconnects counter",
    metricLine("view_server_transport_reconnects", health.transport.reconnects),
    "# HELP view_server_engine_topic_rows Engine topic row counts.",
    "# TYPE view_server_engine_topic_rows gauge",
    "# HELP view_server_engine_topic_version Engine topic version.",
    "# TYPE view_server_engine_topic_version gauge",
    "# HELP view_server_engine_topic_pending_mutation_batches Pending mutation batches by topic.",
    "# TYPE view_server_engine_topic_pending_mutation_batches gauge",
    "# HELP view_server_engine_topic_active_views Active raw views by topic.",
    "# TYPE view_server_engine_topic_active_views gauge",
    "# HELP view_server_engine_topic_active_grouped_views Active grouped views by topic and mode.",
    "# TYPE view_server_engine_topic_active_grouped_views gauge",
    "# HELP view_server_engine_topic_grouped_evaluations Active grouped evaluation count by topic and mode.",
    "# TYPE view_server_engine_topic_grouped_evaluations gauge",
    "# HELP view_server_engine_topic_active_subscriptions Active subscriptions by topic.",
    "# TYPE view_server_engine_topic_active_subscriptions gauge",
    "# HELP view_server_engine_topic_queued_events Queued events by topic.",
    "# TYPE view_server_engine_topic_queued_events gauge",
    "# HELP view_server_engine_topic_max_queue_depth Maximum queue depth by topic.",
    "# TYPE view_server_engine_topic_max_queue_depth gauge",
    "# HELP view_server_engine_topic_backpressure_events Backpressure events by topic.",
    "# TYPE view_server_engine_topic_backpressure_events gauge",
    "# HELP view_server_engine_topic_memory_bytes Estimated memory bytes by topic.",
    "# TYPE view_server_engine_topic_memory_bytes gauge",
    "# HELP view_server_engine_topic_tombstones Tombstone count by topic.",
    "# TYPE view_server_engine_topic_tombstones gauge",
    "# HELP view_server_engine_topic_compaction_pending Topic compaction pending flag.",
    "# TYPE view_server_engine_topic_compaction_pending gauge",
    "# HELP view_server_engine_topic_mutations_per_second Mutations per second by topic.",
    "# TYPE view_server_engine_topic_mutations_per_second gauge",
    "# HELP view_server_engine_topic_rows_per_second Rows per second by topic.",
    "# TYPE view_server_engine_topic_rows_per_second gauge",
    "# HELP view_server_source_active_instances Active logical source instances by topic.",
    "# TYPE view_server_source_active_instances gauge",
    "# HELP view_server_source_status Active logical source instances by lifecycle status.",
    "# TYPE view_server_source_status gauge",
    "# HELP view_server_source_started_at_nanos Latest logical source start time in epoch nanoseconds.",
    "# TYPE view_server_source_started_at_nanos gauge",
    "# HELP view_server_source_last_attempt_started_at_nanos Latest attempt start time in epoch nanoseconds.",
    "# TYPE view_server_source_last_attempt_started_at_nanos gauge",
    "# HELP view_server_source_last_delivery_at_nanos Latest delivery time in epoch nanoseconds.",
    "# TYPE view_server_source_last_delivery_at_nanos gauge",
    "# HELP view_server_source_last_rejection_at_nanos Latest item rejection time in epoch nanoseconds.",
    "# TYPE view_server_source_last_rejection_at_nanos gauge",
    "# HELP view_server_source_last_applied_mutation_at_nanos Latest applied mutation time in epoch nanoseconds.",
    "# TYPE view_server_source_last_applied_mutation_at_nanos gauge",
    "# HELP view_server_source_last_termination_at_nanos Latest attempt termination time in epoch nanoseconds.",
    "# TYPE view_server_source_last_termination_at_nanos gauge",
    "# HELP view_server_source_current_attempt Highest current attempt number.",
    "# TYPE view_server_source_current_attempt gauge",
    "# HELP view_server_source_active_retries Source retries across active logical source instances.",
    "# TYPE view_server_source_active_retries gauge",
    "# HELP view_server_source_active_received_deliveries Received deliveries across active logical source instances.",
    "# TYPE view_server_source_active_received_deliveries gauge",
    "# HELP view_server_source_active_rejected_items Rejected source items across active logical source instances.",
    "# TYPE view_server_source_active_rejected_items gauge",
    "# HELP view_server_source_active_attempted_mutations Attempted source mutations across active logical source instances.",
    "# TYPE view_server_source_active_attempted_mutations gauge",
    "# HELP view_server_source_active_applied_upserts Applied source upserts across active logical source instances.",
    "# TYPE view_server_source_active_applied_upserts gauge",
    "# HELP view_server_source_active_applied_deletes Applied source deletes across active logical source instances.",
    "# TYPE view_server_source_active_applied_deletes gauge",
    "# HELP view_server_source_active_failed_mutations Failed source mutations across active logical source instances.",
    "# TYPE view_server_source_active_failed_mutations gauge",
    "# HELP view_server_source_active_completed_settlements Completed source settlements across active logical source instances.",
    "# TYPE view_server_source_active_completed_settlements gauge",
    "# HELP view_server_source_active_failed_settlements Failed source settlements across active logical source instances.",
    "# TYPE view_server_source_active_failed_settlements gauge",
    "# HELP view_server_source_retained_rows Retained source-owned rows across active logical source instances.",
    "# TYPE view_server_source_retained_rows gauge",
    "# HELP view_server_source_delivery_lanes Active source delivery lanes.",
    "# TYPE view_server_source_delivery_lanes gauge",
    "# HELP view_server_source_bounded_buffer_lanes Active bounded source buffer lanes.",
    "# TYPE view_server_source_bounded_buffer_lanes gauge",
    "# HELP view_server_source_buffer_capacity Total bounded source buffer capacity.",
    "# TYPE view_server_source_buffer_capacity gauge",
    "# HELP view_server_source_buffer_depth Total bounded source buffer depth.",
    "# TYPE view_server_source_buffer_depth gauge",
    "# HELP view_server_source_buffer_high_water_mark Total bounded source buffer high-water mark.",
    "# TYPE view_server_source_buffer_high_water_mark gauge",
    "# HELP view_server_source_active_buffer_overflows Source buffer overflows across active logical source instances.",
    "# TYPE view_server_source_active_buffer_overflows gauge",
  ];

  for (const [topicName, topic] of Object.entries(health.engine.topics)) {
    const labels = { topic: topicName };
    lines.push(
      metricLine("view_server_engine_topic_rows", topic.rowCount, {
        ...labels,
        state: "total",
      }),
      metricLine("view_server_engine_topic_rows", topic.liveRowCount, {
        ...labels,
        state: "live",
      }),
      metricLine("view_server_engine_topic_rows", topic.deletedRowCount, {
        ...labels,
        state: "deleted",
      }),
      metricLine("view_server_engine_topic_version", topic.version, labels),
      metricLine(
        "view_server_engine_topic_pending_mutation_batches",
        topic.pendingMutationBatches,
        labels,
      ),
      metricLine("view_server_engine_topic_active_views", topic.activeViews, labels),
      metricLine(
        "view_server_engine_topic_active_grouped_views",
        topic.activeFallbackGroupedViews,
        {
          ...labels,
          mode: "fallback",
        },
      ),
      metricLine(
        "view_server_engine_topic_active_grouped_views",
        topic.activeIncrementalGroupedViews,
        {
          ...labels,
          mode: "incremental",
        },
      ),
      metricLine("view_server_engine_topic_grouped_evaluations", topic.groupedFullEvaluationCount, {
        ...labels,
        mode: "full",
      }),
      metricLine(
        "view_server_engine_topic_grouped_evaluations",
        topic.groupedPatchedEvaluationCount,
        {
          ...labels,
          mode: "patched",
        },
      ),
      metricLine(
        "view_server_engine_topic_active_subscriptions",
        topic.activeSubscriptions,
        labels,
      ),
      metricLine("view_server_engine_topic_queued_events", topic.queuedEvents, labels),
      metricLine("view_server_engine_topic_max_queue_depth", topic.maxQueueDepth, labels),
      metricLine("view_server_engine_topic_backpressure_events", topic.backpressureEvents, labels),
      metricLine("view_server_engine_topic_memory_bytes", topic.memoryBytes, labels),
      metricLine("view_server_engine_topic_tombstones", topic.tombstoneCount, labels),
      metricLine(
        "view_server_engine_topic_compaction_pending",
        booleanMetric(topic.compactionPending),
        labels,
      ),
      metricLine("view_server_engine_topic_mutations_per_second", topic.mutationsPerSecond, labels),
      metricLine("view_server_engine_topic_rows_per_second", topic.rowsPerSecond, labels),
    );
  }

  for (const [topicName, definition] of Object.entries(config.topics)) {
    const source = definition.source;
    if (source === undefined) {
      continue;
    }
    if (!Object.hasOwn(health.sources, topicName)) {
      continue;
    }
    const candidate: unknown = Reflect.get(health.sources, topicName);
    const totals = emptySourceMetricTotals();
    for (const value of Array.isArray(candidate) ? candidate : [candidate]) {
      const sourceHealth = validatedSourceMetricHealth(value);
      if (sourceHealth !== undefined) {
        addSourceMetricHealth(totals, sourceHealth);
      }
    }
    const labels = {
      topic: topicName,
      adapter: source.identity.name,
      lifecycle: source.lifecycle,
    };
    lines.push(
      metricLine("view_server_source_active_instances", totals.activeInstances, labels),
      ...sourceMetricStatuses.map((status) =>
        metricLine("view_server_source_status", totals.statuses[status], {
          ...labels,
          status: sourceMetricStatusLabels[status],
        }),
      ),
      nullableMetricLine("view_server_source_started_at_nanos", totals.startedAtNanos, labels),
      nullableMetricLine(
        "view_server_source_last_attempt_started_at_nanos",
        totals.lastAttemptStartedAtNanos,
        labels,
      ),
      nullableMetricLine(
        "view_server_source_last_delivery_at_nanos",
        totals.lastDeliveryAtNanos,
        labels,
      ),
      nullableMetricLine(
        "view_server_source_last_rejection_at_nanos",
        totals.lastRejectionAtNanos,
        labels,
      ),
      nullableMetricLine(
        "view_server_source_last_applied_mutation_at_nanos",
        totals.lastAppliedMutationAtNanos,
        labels,
      ),
      nullableMetricLine(
        "view_server_source_last_termination_at_nanos",
        totals.lastTerminationAtNanos,
        labels,
      ),
      metricLine("view_server_source_current_attempt", totals.currentAttempt, labels),
      metricLine("view_server_source_active_retries", totals.retryCount, labels),
      metricLine(
        "view_server_source_active_received_deliveries",
        totals.receivedDeliveryCount,
        labels,
      ),
      metricLine("view_server_source_active_rejected_items", totals.rejectedItemCount, labels),
      metricLine(
        "view_server_source_active_attempted_mutations",
        totals.attemptedMutationCount,
        labels,
      ),
      metricLine("view_server_source_active_applied_upserts", totals.appliedUpsertCount, labels),
      metricLine("view_server_source_active_applied_deletes", totals.appliedDeleteCount, labels),
      metricLine("view_server_source_active_failed_mutations", totals.failedMutationCount, labels),
      metricLine(
        "view_server_source_active_completed_settlements",
        totals.completedSettlementCount,
        labels,
      ),
      metricLine(
        "view_server_source_active_failed_settlements",
        totals.failedSettlementCount,
        labels,
      ),
      metricLine("view_server_source_retained_rows", totals.retainedRowCount, labels),
      metricLine("view_server_source_delivery_lanes", totals.laneCount, labels),
      metricLine("view_server_source_bounded_buffer_lanes", totals.boundedLaneCount, labels),
      metricLine("view_server_source_buffer_capacity", totals.bufferCapacity, labels),
      metricLine("view_server_source_buffer_depth", totals.bufferDepth, labels),
      metricLine("view_server_source_buffer_high_water_mark", totals.bufferHighWaterMark, labels),
      metricLine("view_server_source_active_buffer_overflows", totals.bufferOverflowCount, labels),
    );
  }

  return compactLines(lines);
};

const metricsResponse = (status: number, body: string): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(body, {
    status,
    contentType: metricContentType,
  });

export const makeViewServerMetricsRoute = <const Topics extends TopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* validateViewServerHttpRequest(input.auth, request).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(viewServerAuthErrorResponse(error)),
          onSuccess: () =>
            Effect.gen(function* () {
              const health = yield* input.runtime.health();
              yield* viewServerEncodeHealth(config, health);
              return health;
            }).pipe(
              Effect.map((health) => metricsResponse(200, viewServerHealthMetrics(config, health))),
              Effect.catchCause(() =>
                Effect.succeed(
                  metricsResponse(200, compactLines([metricLine("view_server_metrics_error", 1)])),
                ),
              ),
            ),
        }),
      );
    }),
  );
