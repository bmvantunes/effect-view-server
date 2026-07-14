import {
  groupedAggregateTask,
  groupedKeyWidthTask,
  groupedWriteTask,
  queryDeltaOperationsTask,
  rawActiveRetainedDeltaTask,
  rawLiveFanoutTask,
  rawPredicateIndexTask,
  rawSnapshotTask,
  rawWriteTask,
  reactInMemoryTask,
  runtimeGrpcLeasedTask,
  runtimeGrpcMaterializedTask,
  runtimeKafkaIngestTask,
  runtimeKafkaSustainedFirehoseTask,
  runtimeWebSocketFirehoseTask,
} from "./benchmark-baseline-task-catalog.mjs";

const commonEngineSmokeEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "1",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const stableP99MinimumSampleCount = "1000";
const stableMemoryRssMetric = "process-peak-over-initial-current";

const engineReadSmokeEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: stableP99MinimumSampleCount,
  VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC: stableMemoryRssMetric,
  VIEW_SERVER_ENGINE_BENCH_TIMED_READ_MINIMUM_SAMPLES: stableP99MinimumSampleCount,
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "250",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "100",
};

const engineMixedReadSmokeEnv = {
  ...engineReadSmokeEnv,
  VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS: "5",
};

const rawWriteSmokeEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const rawReadWriteReadEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: stableP99MinimumSampleCount,
  VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC: stableMemoryRssMetric,
  VIEW_SERVER_ENGINE_BENCH_TIMED_READ_MINIMUM_SAMPLES: stableP99MinimumSampleCount,
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "250",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "100",
};

const rawReadWriteMixedReadEnv = {
  ...rawReadWriteReadEnv,
  VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS: "20",
};

const rawReadWriteWriteEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "20",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const commonReactSmokeEnv = {
  VIEW_SERVER_REACT_BENCH_ITERATIONS: "5",
  VIEW_SERVER_REACT_BENCH_TIME_MS: "1",
  VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS: "0",
};

const commonRuntimeKafkaSmokeEnv = {
  VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "localhost:9092",
  VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "3",
  VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "1",
  VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
};

const retainedDeltaSmokeEnv = {
  ...commonEngineSmokeEnv,
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
};

const retainedDeltaReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "100",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const retainedDeltaNoopWideReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT: "1000",
};

const retainedDeltaMoveDownReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "49",
};

const retainedDeltaReplacementBatchReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "24",
};

const retainedDeltaReplacementBatchWideReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_REPLACEMENT_BATCH_SIZE: "64",
  VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT: "1000",
};

const retainedDeltaVisibleDeleteBatchWideReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "4",
  VIEW_SERVER_ENGINE_BENCH_REPLACEMENT_BATCH_SIZE: "16",
  VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT: "1000",
};

const groupedReadReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const groupedWriteReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "incremental",
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
};

const groupedAdmissionReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "incremental",
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "32",
};

const forcedGroupedFallbackAdmissionEnv = {
  VIEW_SERVER_ENGINE_BENCH_EXPECTED_GROUPED_ADMISSION: "fallback",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_GROUPS: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS_PER_GROUP: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_RETAINED_VALUE_ENTRIES: "1",
};

export const profiles = new Map([
  [
    "smoke",
    [
      rawSnapshotTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...engineMixedReadSmokeEnv,
      }),
      rawPredicateIndexTask(1_000, engineReadSmokeEnv),
      rawWriteTask("base", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "100",
        ...rawWriteSmokeEnv,
      }),
      rawWriteTask("indexed", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "100",
        ...rawWriteSmokeEnv,
      }),
      rawLiveFanoutTask("same-window", 1_000, 5, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("ten-window", 1_000, 5, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      groupedAggregateTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...engineMixedReadSmokeEnv,
      }),
      groupedKeyWidthTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...engineReadSmokeEnv,
      }),
      queryDeltaOperationsTask("head-replacement-batch", 1_000, 16, commonEngineSmokeEnv),
      groupedWriteTask("incremental", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
        ...retainedDeltaSmokeEnv,
      }),
      rawActiveRetainedDeltaTask("noop", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-update", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-move-down", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-replacement-batch", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("predicate-enter", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("visible-delete", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("exhausted-lookahead", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("count-only", 101, retainedDeltaSmokeEnv),
      reactInMemoryTask("chromium", 20, {
        VIEW_SERVER_REACT_BENCH_BATCH_SIZE: "10",
        ...commonReactSmokeEnv,
      }),
    ],
  ],
  [
    "kafka-ingest",
    [
      runtimeKafkaIngestTask(250, {
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BURST_MULTIPLIER: "4",
        ...commonRuntimeKafkaSmokeEnv,
      }),
    ],
  ],
  [
    "kafka-sustained-firehose",
    [
      runtimeKafkaSustainedFirehoseTask(250, 4, {
        ...commonRuntimeKafkaSmokeEnv,
      }),
    ],
  ],
  [
    "grpc-materialized",
    [
      runtimeGrpcMaterializedTask(1_000, 256, {
        VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "5",
        VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
      }),
    ],
  ],
  [
    "grpc-leased",
    [
      runtimeGrpcLeasedTask(50, 25, 500, {
        VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "5",
        VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
      }),
    ],
  ],
  [
    "grpc-leased-retained",
    [
      runtimeGrpcLeasedTask(50, 25, 50_000, {
        VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "5",
        VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
      }),
    ],
  ],
  [
    "websocket-firehose",
    [
      runtimeWebSocketFirehoseTask("same-window", 1_000, 10, {
        VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "5",
        VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "1",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
      }),
      runtimeWebSocketFirehoseTask("ten-window", 1_000, 10, {
        VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "5",
        VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "1",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
        VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
      }),
    ],
  ],
  [
    "active-query-sharing",
    [
      rawLiveFanoutTask("same-window", 10_000, 50, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("ten-window", 10_000, 50, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("unique-window", 10_000, 50, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("unique-shape", 10_000, 50, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...commonEngineSmokeEnv,
      }),
    ],
  ],
  [
    "raw-read-write",
    [
      rawSnapshotTask(100_000, rawReadWriteMixedReadEnv),
      rawPredicateIndexTask(100_000, rawReadWriteReadEnv),
      rawWriteTask("base", 100_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...rawReadWriteWriteEnv,
      }),
      rawWriteTask("indexed", 100_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "1000",
        ...rawReadWriteWriteEnv,
      }),
    ],
  ],
  [
    "grouped-admission",
    [
      groupedWriteTask("incremental", 100_000, groupedAdmissionReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, groupedAdmissionReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, {
        ...groupedAdmissionReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "128",
      }),
      groupedWriteTask("incremental", 100_000, {
        ...groupedAdmissionReleaseEnv,
        ...forcedGroupedFallbackAdmissionEnv,
        VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX: "forced-fallback-admission",
      }),
      groupedWriteTask("fallback", 100_000, {
        ...groupedAdmissionReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX: "broad-fallback",
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "fallback",
      }),
    ],
  ],
  [
    "grouped-order-neutral",
    [
      groupedWriteTask("incremental", 100_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
      groupedWriteTask("incremental", 1_000_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
      groupedWriteTask("incremental", 5_000_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
    ],
  ],
  [
    "release",
    [
      rawSnapshotTask(100_000),
      rawSnapshotTask(1_000_000),
      rawSnapshotTask(10_000_000),
      rawPredicateIndexTask(100_000),
      rawPredicateIndexTask(1_000_000),
      rawPredicateIndexTask(10_000_000),
      rawWriteTask("base", 100_000),
      rawWriteTask("indexed", 100_000),
      rawWriteTask("base", 1_000_000),
      rawWriteTask("indexed", 1_000_000),
      rawWriteTask("base", 10_000_000),
      rawWriteTask("indexed", 10_000_000),
      rawLiveFanoutTask("same-window", 100_000, 50),
      rawLiveFanoutTask("ten-window", 100_000, 50),
      rawLiveFanoutTask("same-window", 1_000_000, 250),
      rawLiveFanoutTask("ten-window", 1_000_000, 250),
      groupedAggregateTask(100_000, groupedReadReleaseEnv),
      groupedAggregateTask(1_000_000, groupedReadReleaseEnv),
      groupedAggregateTask(5_000_000, groupedReadReleaseEnv),
      groupedKeyWidthTask(100_000, groupedReadReleaseEnv),
      groupedKeyWidthTask(1_000_000, groupedReadReleaseEnv),
      queryDeltaOperationsTask("head-replacement-batch", 10_000, 64),
      queryDeltaOperationsTask("middle-replacement-batch", 10_000, 64),
      queryDeltaOperationsTask("tail-replacement-batch", 10_000, 64),
      groupedWriteTask("incremental", 100_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 5_000_000, groupedWriteReleaseEnv),
      rawActiveRetainedDeltaTask("noop", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("noop", 100_000, retainedDeltaNoopWideReleaseEnv),
      rawActiveRetainedDeltaTask("match-update", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask(
        "match-move-down",
        100_000,
        retainedDeltaMoveDownReleaseEnv,
      ),
      rawActiveRetainedDeltaTask(
        "match-replacement-batch",
        100_000,
        retainedDeltaReplacementBatchReleaseEnv,
      ),
      rawActiveRetainedDeltaTask(
        "match-replacement-batch",
        100_000,
        retainedDeltaReplacementBatchWideReleaseEnv,
      ),
      rawActiveRetainedDeltaTask(
        "visible-delete-batch",
        100_000,
        retainedDeltaVisibleDeleteBatchWideReleaseEnv,
      ),
      rawActiveRetainedDeltaTask("predicate-enter", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("visible-delete", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("exhausted-lookahead", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("count-only", 100_000, retainedDeltaReleaseEnv),
      reactInMemoryTask("chromium", 10_000),
      reactInMemoryTask("firefox", 10_000),
      reactInMemoryTask("webkit", 10_000),
    ],
  ],
]);

export const repeatableReportOnlyProfiles = new Set(["grpc-leased-retained"]);
