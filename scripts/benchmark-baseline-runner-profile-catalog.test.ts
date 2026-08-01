import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import config from "../vite.config";
import { profiles } from "./benchmark-baseline-profiles.mjs";
import {
  groupedWriteTask,
  kafkaSourceAdapterBrokerTask,
  kafkaSourceAdapterTask,
  rawLargeMembershipTask,
  runtimeGrpcSourceAdapterTask,
} from "./benchmark-baseline-task-catalog.mjs";

describe("benchmark baseline runner", () => {
  it("defines the Kafka Source Adapter multi-partition baseline", () => {
    const sourceTask = kafkaSourceAdapterTask(64, 64);
    const overriddenRetentionTask = kafkaSourceAdapterTask(64, 64, {
      VIEW_SERVER_KAFKA_RETENTION_BENCH_ROWS: "25",
    });
    const brokerTask = kafkaSourceAdapterBrokerTask(64);
    const sourceProfile = profiles.get("kafka-source-adapter") ?? [];

    expect({
      benchmarkScope: sourceTask.expectedBenchmarkScope,
      iterations: sourceTask.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_ITERATIONS"],
      expectedMutationCount: sourceTask.expectedMutationCount,
      minimumSampleCount: sourceTask.minimumSampleCount,
      outputJsonPath: sourceTask.packageOutputJsonPath,
      partitions: sourceTask.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_PARTITIONS"],
      retentionRows: sourceTask.env["VIEW_SERVER_KAFKA_RETENTION_BENCH_ROWS"],
      rowCount: sourceTask.expectedRowCount,
      rows: sourceTask.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_ROWS"],
      task: sourceTask.args,
    }).toStrictEqual({
      benchmarkScope: "kafka-source-adapter",
      expectedMutationCount: 184_986,
      iterations: undefined,
      minimumSampleCount: 5,
      outputJsonPath: ".artifacts/source-lanes-64rows-64partitions.json",
      partitions: "64",
      retentionRows: "10000",
      rowCount: 64,
      rows: "64",
      task: ["run", "--no-cache", "kafka#bench:source-lanes"],
    });
    expect({
      expectedMutationCount: overriddenRetentionTask.expectedMutationCount,
      retentionRows:
        overriddenRetentionTask.env["VIEW_SERVER_KAFKA_RETENTION_BENCH_ROWS"],
    }).toStrictEqual({
      expectedMutationCount: 5_436,
      retentionRows: "25",
    });
    expect(
      sourceProfile.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations:
          task.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_ITERATIONS"] ??
          task.env["VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_ITERATIONS"],
        minimumSampleCount: task.minimumSampleCount,
        expectedMutationCount: task.expectedMutationCount,
        partitions: task.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_PARTITIONS"],
        rowCount: task.expectedRowCount,
        rows:
          task.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_ROWS"] ??
          task.env["VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_ROWS"],
        task: task.args,
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "kafka-source-adapter",
        expectedMutationCount: 335_994,
        iterations: "5",
        minimumSampleCount: 5,
        partitions: "64",
        rowCount: 2_000,
        rows: "2000",
        task: ["run", "--no-cache", "kafka#bench:source-lanes"],
      },
      {
        benchmarkScope: "kafka-source-adapter-broker",
        expectedMutationCount: undefined,
        iterations: "5",
        minimumSampleCount: 5,
        partitions: undefined,
        rowCount: 250,
        rows: "250",
        task: ["run", "--no-cache", "bench:kafka-source-broker"],
      },
    ]);
    expect({
      benchmarkScope: brokerTask.expectedBenchmarkScope,
      outputJsonPath: brokerTask.packageOutputJsonPath,
      rowCount: brokerTask.expectedRowCount,
      rows: brokerTask.env["VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_ROWS"],
      task: brokerTask.args,
    }).toStrictEqual({
      benchmarkScope: "kafka-source-adapter-broker",
      outputJsonPath: ".artifacts/source-broker-64rows.json",
      rowCount: 64,
      rows: "64",
      task: ["run", "--no-cache", "bench:kafka-source-broker"],
    });
    expect(config.run?.tasks?.["bench:kafka-source-broker"]).toStrictEqual({
      command: "node scripts/run-kafka-source-broker-bench.mjs",
      dependsOn: ["build:effect-declarations:runtime-core"],
    });

    const sourceBenchmark = readFileSync("packages/kafka/src/source-lanes.bench.ts", "utf8");
    expect(sourceBenchmark).toContain("${laneBatchSize}-record mixed JSON/protobuf burst");
    expect(sourceBenchmark).toContain("sustained JSON/protobuf ingestion");
    expect(sourceBenchmark).toContain("kafka.protobuf(OrderValueSchema)");
  });

  it("guards large membership performance in the focused active-query-sharing profile", () => {
    const smokeMembershipTasks = (profiles.get("smoke") ?? []).filter(
      (task) => task.expectedBenchmarkScope === "engine-raw-large-membership",
    );
    const membershipTasks = (profiles.get("active-query-sharing") ?? []).filter(
      (task) => task.expectedBenchmarkScope === "engine-raw-large-membership",
    );

    expect(smokeMembershipTasks).toStrictEqual([]);
    expect(
      membershipTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        minimumSampleCount: task.minimumSampleCount,
        outputJsonPath: task.packageOutputJsonPath,
        rawLargeMembershipParameters: task.expectedRawLargeMembershipParameters,
        rowCount: task.expectedRowCount,
        task: task.args,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-raw-large-membership",
        iterations: "5",
        minimumSampleCount: 5,
        outputJsonPath: ".artifacts/raw-large-membership-50000candidates-100000rows.json",
        rawLargeMembershipParameters: {
          candidateCount: 50_000,
          partitionCount: 25,
          preparedPlanCompilationCount: 1,
          subscriberCount: 32,
        },
        rowCount: 100_000,
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-large-membership"],
        timeMs: "0",
      },
    ]);
  });

  it("defines the large membership task with deterministic defaults", () => {
    const membershipTask = rawLargeMembershipTask();

    expect({
      benchmarkScope: membershipTask.expectedBenchmarkScope,
      minimumSampleCount: membershipTask.minimumSampleCount,
      outputJsonPath: membershipTask.packageOutputJsonPath,
      rawLargeMembershipParameters: membershipTask.expectedRawLargeMembershipParameters,
      rowCount: membershipTask.expectedRowCount,
      task: membershipTask.args,
    }).toStrictEqual({
      benchmarkScope: "engine-raw-large-membership",
      minimumSampleCount: 5,
      outputJsonPath: ".artifacts/raw-large-membership-50000candidates-100000rows.json",
      rawLargeMembershipParameters: {
        candidateCount: 50_000,
        partitionCount: 25,
        preparedPlanCompilationCount: 1,
        subscriberCount: 32,
      },
      rowCount: 100_000,
      task: ["run", "--no-cache", "column-live-view-engine#bench:raw-large-membership"],
    });
  });

  it("defines active-query sharing fanout tasks", () => {
    const activeQuerySharingProfile = profiles.get("active-query-sharing") ?? [];
    expect(
      activeQuerySharingProfile.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        taskLabel: task.label,
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-raw-large-membership",
        taskLabel: "raw large membership 50000 candidates 100000 rows",
      },
      {
        benchmarkScope: "engine-raw-live-fanout",
        taskLabel: "raw live fanout same-window 10000 rows 50 subscribers",
      },
      {
        benchmarkScope: "engine-raw-live-fanout",
        taskLabel: "raw live fanout ten-window 10000 rows 50 subscribers",
      },
      {
        benchmarkScope: "engine-raw-live-fanout",
        taskLabel: "raw live fanout unique-window 10000 rows 50 subscribers",
      },
      {
        benchmarkScope: "engine-raw-live-fanout",
        taskLabel: "raw live fanout unique-shape 10000 rows 50 subscribers",
      },
    ]);

    const activeQuerySharingTasks = activeQuerySharingProfile.filter(
      (task) => task.expectedBenchmarkScope === "engine-raw-live-fanout",
    );

    expect(
      activeQuerySharingTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        fanoutCase: task.env["VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        subscriberCount: task.env["VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "same-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-same-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "ten-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-ten-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "unique-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-unique-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "unique-shape",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-unique-shape-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
    ]);
  });

  it("defines the WebSocket firehose runtime benchmark tasks", () => {
    const webSocketFirehoseTasks = profiles.get("websocket-firehose") ?? [];

    expect(
      webSocketFirehoseTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkCase: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_CASE"],
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_ROWS"],
        subscriberCount: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_SUBSCRIBERS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkCase: "same-window",
        benchmarkScope: "runtime-websocket-firehose",
        iterations: "5",
        outputJsonPath: ".artifacts/websocket-firehose-same-window-1000rows-10subs.json",
        rowCount: "1000",
        subscriberCount: "10",
        task: ["run", "--no-cache", "runtime#bench:websocket-firehose"],
        timeMs: "1",
      },
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkCase: "ten-window",
        benchmarkScope: "runtime-websocket-firehose",
        iterations: "5",
        outputJsonPath: ".artifacts/websocket-firehose-ten-window-1000rows-10subs.json",
        rowCount: "1000",
        subscriberCount: "10",
        task: ["run", "--no-cache", "runtime#bench:websocket-firehose"],
        timeMs: "1",
      },
    ]);
  });

  it("defines the gRPC Source Adapter benchmark task", () => {
    const sourceAdapterTasks = profiles.get("grpc-source-adapter") ?? [];
    const directTask = runtimeGrpcSourceAdapterTask(1_000, 50, 50_000, {
      VIEW_SERVER_RUNTIME_BENCH_ITERATIONS: "7",
      VIEW_SERVER_RUNTIME_BENCH_TIME_MS: "0",
      VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS: "0",
      VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS: "0",
    });

    expect(sourceAdapterTasks).toStrictEqual([directTask]);
    expect(
      sourceAdapterTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        batchSize: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        expectedMutationCount: task.expectedMutationCount,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        minimumSampleCount: task.minimumSampleCount,
        outputJsonPath: task.packageOutputJsonPath,
        routeCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_ROUTE_COUNT"],
        retainedRowCount:
          task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_RETAINED_ROWS"],
        rowCount: task.expectedRowCount,
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        batchSize: "1000",
        benchmarkScope: "runtime-grpc-source-adapter",
        expectedMutationCount: 64_007,
        iterations: "7",
        minimumSampleCount: 7,
        outputJsonPath:
          ".artifacts/grpc-source-adapter-1000batch-50routes-50000retained.json",
        routeCount: "50",
        retainedRowCount: "50000",
        rowCount: 1_000,
        task: ["run", "--no-cache", "bench:grpc-source-adapter"],
        timeMs: "0",
      },
    ]);

    const sourceBenchmark = readFileSync("packages/grpc/src/grpc.bench.ts", "utf8");
    expect(sourceBenchmark).toContain("preserves Materialized batch, Leased route");
    expect(sourceBenchmark).toContain("${retainedRowCount}-row retained-capacity");
  });

  it("defines isolated grouped order-neutral tasks without changing dual grouped-write artifacts", () => {
    const groupedOrderNeutralTasks = profiles.get("grouped-order-neutral") ?? [];
    const releaseGroupedWriteTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("grouped write "),
    );
    const smokeGroupedWriteTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("grouped write "),
    );

    expect(
      groupedOrderNeutralTasks.map((task) => ({
        explicitGc: task.env["VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC"],
        measurementProtocol: task.expectedMeasurementProtocol,
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
        postGcEventLoopTurns:
          task.env["VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS"],
        readerProfile: task.env["VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE"],
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-order-neutral-100000rows-1mutations.json",
        primingAppendBatches: undefined,
        postGcEventLoopTurns: undefined,
        readerProfile: "order-neutral",
        rowCount: "100000",
      },
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-1000000rows-1mutations.json",
        primingAppendBatches: undefined,
        postGcEventLoopTurns: undefined,
        readerProfile: "order-neutral",
        rowCount: "1000000",
      },
      {
        explicitGc: "1",
        measurementProtocol: {
          memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
          postGcEventLoopTurns: 8,
          priming: "append-delete-restore-before-sampling",
        },
        nodeOptions: "--expose-gc",
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-5000000rows-1mutations.json",
        primingAppendBatches: "1",
        postGcEventLoopTurns: "8",
        readerProfile: "order-neutral",
        rowCount: "5000000",
      },
    ]);
    expect(
      smokeGroupedWriteTasks.map((task) => ({
        explicitGc: task.env["VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC"],
        measurementProtocol: task.expectedMeasurementProtocol,
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-1000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
    ]);
    expect(
      releaseGroupedWriteTasks.map((task) => ({
        explicitGc: task.env["VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC"],
        measurementProtocol: task.expectedMeasurementProtocol,
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-100000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-1000000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
      {
        explicitGc: undefined,
        measurementProtocol: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-5000000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
    ]);
  });

  it("derives the supported grouped measurement protocol", () => {
    const commonGroupedEnvironment = {
      VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
      VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
    };

    expect(
      groupedWriteTask("incremental", 1, {
        ...commonGroupedEnvironment,
        VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES: "1",
      }).expectedMeasurementProtocol,
    ).toStrictEqual({
      priming: "append-delete-restore-before-sampling",
    });
  });

  it("rejects a grouped explicit-GC task without append priming", () => {
    expect(() =>
      groupedWriteTask("incremental", 1, {
        VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC: "1",
        VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
      }),
    ).toThrow(/^Grouped write explicit GC requires append priming to be enabled\.$/u);
  });

  it("requires exactly eight post-GC event-loop turns for grouped explicit-GC tasks", () => {
    expect(() =>
      groupedWriteTask("incremental", 1, {
        VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC: "1",
        VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
        VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES: "1",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
      }),
    ).toThrow(/^Grouped write explicit GC requires post-GC event-loop turns\.$/u);
    expect(() =>
      groupedWriteTask("incremental", 1, {
        VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC: "1",
        VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
        VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS: "7",
        VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES: "1",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
      }),
    ).toThrow(/^Grouped write post-GC event-loop turns must be 8\.$/u);
    expect(() =>
      groupedWriteTask("incremental", 1, {
        VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
        VIEW_SERVER_ENGINE_BENCH_POST_GC_EVENT_LOOP_TURNS: "8",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
      }),
    ).toThrow(/^Grouped write post-GC event-loop turns require explicit GC\.$/u);
  });

  it("defines grouped key width smoke and release tasks", () => {
    const smokeGroupedKeyWidthTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("grouped key width "),
    );
    const releaseGroupedKeyWidthTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("grouped key width "),
    );

    expect(
      smokeGroupedKeyWidthTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "1000",
        outputJsonPath: ".artifacts/grouped-key-width-1000rows.json",
        rowCount: "1000",
        timeMs: "250",
      },
    ]);
    expect(
      releaseGroupedKeyWidthTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "3",
        outputJsonPath: ".artifacts/grouped-key-width-100000rows.json",
        rowCount: "100000",
        timeMs: "0",
      },
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "3",
        outputJsonPath: ".artifacts/grouped-key-width-1000000rows.json",
        rowCount: "1000000",
        timeMs: "0",
      },
    ]);
  });

  it("defines query delta operation smoke and release tasks", () => {
    const smokeDeltaTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("query delta operations "),
    );
    const releaseDeltaTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("query delta operations "),
    );

    expect(
      smokeDeltaTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE"],
        operationCount: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "head-replacement-batch",
        operationCount: "16",
        outputJsonPath:
          ".artifacts/query-delta-operations-head-replacement-batch-1000rows-32ops.json",
        rowCount: "1000",
      },
    ]);
    expect(
      releaseDeltaTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE"],
        operationCount: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "head-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-head-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "middle-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-middle-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "tail-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-tail-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
    ]);
  });

  it("defines retained delta move cases for smoke and release baseline gates", () => {
    const smokeRetainedDeltaTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("raw active retained delta "),
    );
    const releaseRetainedDeltaTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("raw active retained delta "),
    );

    expect(
      smokeRetainedDeltaTasks.map((task) => ({
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        caseName: "noop",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-noop-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-update",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-update-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-move-down",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-move-down-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-replacement-batch",
        iterations: "5",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "predicate-enter",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-predicate-enter-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "visible-delete",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-visible-delete-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "exhausted-lookahead",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-exhausted-lookahead-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "count-only",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-count-only-101rows.json",
        rowCount: "101",
      },
    ]);
    expect(
      releaseRetainedDeltaTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_REPLACEMENT_BATCH_SIZE"],
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        windowLimit: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT"],
      })),
    ).toStrictEqual([
      {
        batchSize: undefined,
        caseName: "noop",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-noop-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "noop",
        iterations: "100",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-noop-100000rows-1000limit-2batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: undefined,
        caseName: "match-update",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-update-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "match-move-down",
        iterations: "49",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-move-down-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "match-replacement-batch",
        iterations: "24",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: "64",
        caseName: "match-replacement-batch",
        iterations: "5",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-100000rows-1000limit-64batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: "16",
        caseName: "visible-delete-batch",
        iterations: "4",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-visible-delete-batch-100000rows-1000limit-16batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: undefined,
        caseName: "predicate-enter",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-predicate-enter-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "visible-delete",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-visible-delete-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "exhausted-lookahead",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-exhausted-lookahead-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "count-only",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-count-only-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
    ]);
  });
});
