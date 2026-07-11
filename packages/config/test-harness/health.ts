import type { KafkaStartFromHealth, TopicRuntimeHealth } from "../src/index";

export const sourceTopicHealth = (
  status: TopicRuntimeHealth["status"],
  rowCount: number,
): TopicRuntimeHealth => ({
  status,
  rowCount,
  liveRowCount: rowCount,
  deletedRowCount: 0,
  version: rowCount,
  lastMutationAt: null,
  mutationsPerSecond: rowCount,
  rowsPerSecond: rowCount,
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
});

export const kafkaStartFromHealth = {
  consumerGroupId: "view-server-test",
  fallbackMode: "earliest",
  mode: "committed",
} as const;

export const kafkaLatestStartFromHealth = {
  consumerGroupId: "view-server-latest",
  fallbackMode: "latest",
  mode: "latest",
} satisfies KafkaStartFromHealth;

export const kafkaEarliestStartFromHealth = {
  consumerGroupId: "view-server-earliest",
  fallbackMode: "earliest",
  mode: "earliest",
} satisfies KafkaStartFromHealth;

export const kafkaCommittedFailStartFromHealth = {
  consumerGroupId: "view-server-committed",
  fallbackMode: "fail",
  mode: "committed",
} satisfies KafkaStartFromHealth;
