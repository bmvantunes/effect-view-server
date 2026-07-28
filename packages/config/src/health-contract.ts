import type {
  SourceDefinitionAny,
  SourceDefinitionLifecycle,
  SourceHealthForDefinition,
} from "@effect-view-server/source-adapter";
import type { LiveQueryResult, TopicRow } from "./topic-contract";

export type RuntimeStatus = "ready" | "degraded" | "starting" | "stopping";
export type TopicHealthStatus = "ready" | "degraded" | "starting";
export type ViewServerHealthConnectionStatus = "connecting" | "connected" | "disconnected";
export type ViewServerHealthStatus =
  | RuntimeStatus
  | Exclude<ViewServerHealthConnectionStatus, "connected">;

export const VIEW_SERVER_HEALTH_SUMMARY_TOPIC = "__view_server_health_summary";
export const VIEW_SERVER_HEALTH_TOPIC = "__view_server_health";

export type ViewServerSystemTopicName =
  | typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
  | typeof VIEW_SERVER_HEALTH_TOPIC;

export const viewServerReservedTopicNames: ReadonlyArray<ViewServerSystemTopicName> = [
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
];

export const viewServerTopicNameIsReserved = (topic: string): topic is ViewServerSystemTopicName =>
  viewServerReservedTopicNames.some((reservedTopic) => reservedTopic === topic);

export type TopicRuntimeHealth = {
  readonly status: TopicHealthStatus;
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

export type TransportHealth = {
  readonly activeClients: number;
  readonly activeStreams: number;
  readonly activeSubscriptions: number;
  readonly messagesPerSecond: number;
  readonly bytesPerSecond: number;
  readonly queuedMessages: number;
  readonly queuedBytes: number;
  readonly droppedClients: number;
  readonly backpressureEvents: number;
  readonly reconnects: number;
  readonly lastError: string | null;
};

type TopicSourceDefinition<
  Topics extends object,
  Topic extends keyof Topics,
> = Topics[Topic] extends {
  readonly source: infer Definition extends SourceDefinitionAny;
}
  ? Definition
  : never;

type TopicSourceHealthValue<
  Definition extends SourceDefinitionAny,
  Row extends object,
> = Definition extends SourceDefinitionAny
  ? SourceDefinitionLifecycle<Definition> extends "leased"
    ? ReadonlyArray<SourceHealthForDefinition<Definition, Row>>
    : SourceHealthForDefinition<Definition, Row>
  : never;

export type ViewServerSourceHealth<Topics extends object = Record<string, object>> = {
  readonly [Topic in keyof Topics as Topics[Topic] extends {
    readonly source: SourceDefinitionAny;
  }
    ? "materialized" extends SourceDefinitionLifecycle<TopicSourceDefinition<Topics, Topic>>
      ? never
      : Topic
    : never]: TopicSourceHealthValue<
    TopicSourceDefinition<Topics, Topic>,
    Extract<TopicRow<Topics, Topic>, object>
  >;
} & {
  readonly [Topic in keyof Topics as Topics[Topic] extends {
    readonly source: SourceDefinitionAny;
  }
    ? "materialized" extends SourceDefinitionLifecycle<TopicSourceDefinition<Topics, Topic>>
      ? Topic
      : never
    : never]?: TopicSourceHealthValue<
    TopicSourceDefinition<Topics, Topic>,
    Extract<TopicRow<Topics, Topic>, object>
  >;
};

export type ViewServerHealth<Topics extends object = Record<string, object>> = {
  readonly status: RuntimeStatus;
  readonly version: number;
  readonly uptimeMs: number;
  readonly engine: {
    readonly topics: {
      readonly [Topic in Extract<keyof Topics, string>]: TopicRuntimeHealth;
    };
  };
  readonly sources: ViewServerSourceHealth<Topics>;
  readonly transport: TransportHealth;
};

export type ViewServerHealthSummary<Topics extends object = Record<string, object>> = {
  readonly status: ViewServerHealthStatus;
  readonly runtimeStatus: RuntimeStatus;
  readonly connectionStatus: ViewServerHealthConnectionStatus;
  readonly unhealthyTopics: ReadonlyArray<Extract<keyof Topics, string>>;
  readonly updatedAtNanos: bigint;
};

export type ViewServerHealthSummaryRow<Topics extends object = Record<string, object>> =
  ViewServerHealthSummary<Topics> & {
    readonly id: "summary";
  };

export type ViewServerHealthTopicRow<Topic extends string = string> = {
  readonly id: Topic;
  readonly status: TopicHealthStatus | "stopping";
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
  readonly updatedAtNanos: bigint;
};

export type ViewServerHealthDetails<Topic extends string = string> = Omit<
  LiveQueryResult<ViewServerHealthTopicRow<Topic>>,
  "status"
> & {
  readonly runtimeStatus: RuntimeStatus;
  readonly connectionStatus: ViewServerHealthConnectionStatus;
  readonly status: ViewServerHealthStatus;
};

function typedTopicNames<Topics extends object>(
  topics: ReadonlyArray<string>,
): ReadonlyArray<Extract<keyof Topics, string>>;
function typedTopicNames(topics: ReadonlyArray<string>): ReadonlyArray<string> {
  return topics;
}

function typedHealthTopicRows<Topic extends string>(
  rows: ReadonlyArray<ViewServerHealthTopicRow<string>>,
): ReadonlyArray<ViewServerHealthTopicRow<Topic>>;
function typedHealthTopicRows(
  rows: ReadonlyArray<ViewServerHealthTopicRow<string>>,
): ReadonlyArray<ViewServerHealthTopicRow<string>> {
  return rows;
}

const topicIsUnhealthy = (topic: TopicRuntimeHealth): boolean => topic.status !== "ready";

export const viewServerHealthSummaryFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ViewServerHealthSummary<Topics> => {
  const topicHealthByName: Readonly<Record<string, TopicRuntimeHealth>> = health.engine.topics;
  const unhealthyTopics = Object.entries(topicHealthByName)
    .filter(([, topic]) => topicIsUnhealthy(topic))
    .map(([topic]) => topic);
  return {
    status: health.status,
    runtimeStatus: health.status,
    connectionStatus: "connected",
    unhealthyTopics: typedTopicNames<Topics>(unhealthyTopics),
    updatedAtNanos,
  };
};

export const viewServerHealthSummaryRowFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ViewServerHealthSummaryRow<Topics> => ({
  id: "summary",
  ...viewServerHealthSummaryFromHealth(health, updatedAtNanos),
});

export const viewServerHealthTopicRowsFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ReadonlyArray<ViewServerHealthTopicRow<Extract<keyof Topics, string>>> => {
  const topicHealthByName: Readonly<Record<string, TopicRuntimeHealth>> = health.engine.topics;
  const rows: Array<ViewServerHealthTopicRow<string>> = [];
  for (const [id, topic] of Object.entries(topicHealthByName)) {
    const status: TopicHealthStatus | "stopping" =
      health.status === "stopping" ? "stopping" : topic.status;
    rows.push({
      id,
      status,
      rowCount: topic.rowCount,
      liveRowCount: topic.liveRowCount,
      deletedRowCount: topic.deletedRowCount,
      version: topic.version,
      lastMutationAt: topic.lastMutationAt,
      mutationsPerSecond: topic.mutationsPerSecond,
      rowsPerSecond: topic.rowsPerSecond,
      pendingMutationBatches: topic.pendingMutationBatches,
      activeFallbackGroupedViews: topic.activeFallbackGroupedViews,
      activeIncrementalGroupedViews: topic.activeIncrementalGroupedViews,
      activeViews: topic.activeViews,
      groupedFullEvaluationCount: topic.groupedFullEvaluationCount,
      groupedPatchedEvaluationCount: topic.groupedPatchedEvaluationCount,
      activeSubscriptions: topic.activeSubscriptions,
      queuedEvents: topic.queuedEvents,
      maxQueueDepth: topic.maxQueueDepth,
      backpressureEvents: topic.backpressureEvents,
      memoryBytes: topic.memoryBytes,
      tombstoneCount: topic.tombstoneCount,
      compactionPending: topic.compactionPending,
      updatedAtNanos,
    });
  }
  return typedHealthTopicRows<Extract<keyof Topics, string>>(rows);
};
