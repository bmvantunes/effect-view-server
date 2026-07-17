import { describe, expect, it } from "@effect/vitest";
import { profiles } from "./benchmark-baseline-profiles.mjs";

describe("benchmark baseline runner", () => {
  it("defines active-query sharing fanout tasks", () => {
    const activeQuerySharingTasks = profiles.get("active-query-sharing") ?? [];

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

  it("defines the Kafka ingest runtime benchmark task", () => {
    const kafkaIngestTasks = profiles.get("kafka-ingest") ?? [];

    expect(
      kafkaIngestTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkScope: task.expectedBenchmarkScope,
        broker: task.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE"],
        task: task.args,
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        broker: "localhost:9092",
        outputJsonPath: ".artifacts/kafka-ingest-250rows.json",
        rowCount: "250",
        task: ["run", "--no-cache", "runtime#bench:kafka-ingest"],
      },
    ]);
  });

  it("defines the Kafka sustained firehose runtime benchmark task", () => {
    const kafkaSustainedFirehoseTasks = profiles.get("kafka-sustained-firehose") ?? [];

    expect(
      kafkaSustainedFirehoseTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkMode: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE"],
        benchmarkScope: task.expectedBenchmarkScope,
        broker: task.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE"],
        sustainedBatches: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES"],
        task: task.args,
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkMode: "sustained-firehose",
        benchmarkScope: "runtime-kafka-sustained-firehose",
        broker: "localhost:9092",
        outputJsonPath: ".artifacts/kafka-sustained-firehose-250rows-4batches.json",
        rowCount: "250",
        sustainedBatches: "4",
        task: ["run", "--no-cache", "runtime#bench:kafka-ingest"],
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

  it("defines the gRPC runtime benchmark tasks", () => {
    const materializedTasks = profiles.get("grpc-materialized") ?? [];
    const leasedTasks = profiles.get("grpc-leased") ?? [];
    const retainedTasks = profiles.get("grpc-leased-retained") ?? [];

    expect({
      leased: leasedTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        explicitGc: task.env["VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC"],
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        retainedRows: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_RETAINED_ROWS"],
        routeCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROUTE_COUNT"],
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROWS_PER_FEED"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
      retained: retainedTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        explicitGc: task.env["VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC"],
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        retainedRows: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_RETAINED_ROWS"],
        routeCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROUTE_COUNT"],
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROWS_PER_FEED"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
      materialized: materializedTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        batchSize: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        explicitGc: task.env["VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC"],
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_GRPC_SEED_ROWS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
    }).toStrictEqual({
      leased: [
        {
          artifactKind: "runtime-benchmark-summary",
          benchmarkScope: "runtime-grpc-leased",
          iterations: "5",
          explicitGc: "1",
          nodeOptions: "--expose-gc",
          outputJsonPath: ".artifacts/grpc-leased-50rows-25routes-500retained.json",
          retainedRows: "500",
          routeCount: "25",
          rowCount: "50",
          task: ["run", "--no-cache", "runtime#bench:grpc-leased"],
          timeMs: "0",
        },
      ],
      retained: [
        {
          artifactKind: "runtime-benchmark-summary",
          benchmarkScope: "runtime-grpc-leased",
          iterations: "5",
          explicitGc: "1",
          nodeOptions: "--expose-gc",
          outputJsonPath: ".artifacts/grpc-leased-50rows-25routes-50000retained.json",
          retainedRows: "50000",
          routeCount: "25",
          rowCount: "50",
          task: ["run", "--no-cache", "runtime#bench:grpc-leased"],
          timeMs: "0",
        },
      ],
      materialized: [
        {
          artifactKind: "runtime-benchmark-summary",
          batchSize: "256",
          benchmarkScope: "runtime-grpc-materialized",
          iterations: "5",
          explicitGc: "1",
          nodeOptions: "--expose-gc",
          outputJsonPath: ".artifacts/grpc-materialized-1000seed-256batch.json",
          rowCount: "1000",
          task: ["run", "--no-cache", "runtime#bench:grpc-materialized"],
          timeMs: "0",
        },
      ],
    });
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
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
        readerProfile: task.env["VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE"],
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-order-neutral-100000rows-1mutations.json",
        primingAppendBatches: undefined,
        readerProfile: "order-neutral",
        rowCount: "100000",
      },
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-1000000rows-1mutations.json",
        primingAppendBatches: undefined,
        readerProfile: "order-neutral",
        rowCount: "1000000",
      },
      {
        explicitGc: "1",
        nodeOptions: "--expose-gc",
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-5000000rows-1mutations.json",
        primingAppendBatches: "1",
        readerProfile: "order-neutral",
        rowCount: "5000000",
      },
    ]);
    expect(
      smokeGroupedWriteTasks.map((task) => ({
        explicitGc: task.env["VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC"],
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-1000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
    ]);
    expect(
      releaseGroupedWriteTasks.map((task) => ({
        explicitGc: task.env["VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC"],
        nodeOptions: task.env["NODE_OPTIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        primingAppendBatches:
          task.env["VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES"],
      })),
    ).toStrictEqual([
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-100000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-1000000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
      {
        explicitGc: undefined,
        nodeOptions: undefined,
        outputJsonPath: ".artifacts/grouped-write-incremental-5000000rows-1mutations.json",
        primingAppendBatches: undefined,
      },
    ]);
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
