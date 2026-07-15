import { describe, expect, it } from "@effect/vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  benchmarkThresholdsForProfile,
  buildBenchmarkBaseline,
  comparableBenchmarksFromVitestOutput,
  defaultBenchmarkThresholds,
  grpcRuntimeBenchmarkThresholds,
  grpcRetainedRuntimeBenchmarkThresholds,
  groupedOrderNeutralBenchmarkThresholds,
  kafkaIngestBenchmarkThresholds,
  kafkaSustainedFirehoseBenchmarkThresholds,
  rawReadWriteBenchmarkThresholds,
  readBenchmarkBaseline,
  readBenchmarkObservation,
  validateBenchmarkBaseline,
  websocketFirehoseBenchmarkThresholds,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";

import {
  vitestOutput,
  summary,
  runtimeHealth,
  runtimeKafkaIngestLanes,
  runtimeThroughputMutationCount,
  runtimeThroughputHealth,
  runtimeThroughputKafkaIngestLanes,
  comparableRuntimeThroughputKafkaIngestLanes,
  runtimeThroughput,
  comparableRuntimeThroughputCases,
  rawRuntimeMetrics,
  runtimeMetrics,
  comparableNonKafkaRuntimeThroughputCases,
  runtimeGrpcLeasedSample,
  runtimeGrpcLeasedOperationCaseFor,
  runtimeGrpcLeasedOperationCase,
  runtimeGrpcLeasedReuseOperationCase,
  runtimeGrpcLeasedOperationCases,
  runtimeGrpcMaterializedParameters,
  runtimeGrpcMaterializedOperationCase,
  runtimeGrpcMaterializedOperationCases,
  observation,
  grpcLeasedObservationFor,
  grpcLeasedObservation,
  completeGrpcLeasedObservation,
  replaceGrpcMaterializedOperationCase,
  runtimeGrpcMaterializedObservation,
  runtimeGrpcMaterializedVitestOutput,
  taskPaths,
  browserTaskPaths,
  runtimeTaskPaths,
} from "./benchmark-baseline-test-fixtures.ts";

describe("benchmark baseline artifacts", () => {
  it("extracts comparable benchmark metrics from Vitest output", () => {
    expect(comparableBenchmarksFromVitestOutput(vitestOutput)).toStrictEqual([
      {
        maxMs: 3,
        meanMs: 2,
        minMs: 1,
        groupName: "src/example.bench.ts > example benchmark group",
        name: "case a",
        p99Ms: 3,
        sampleCount: 7,
      },
    ]);
  });

  it("uses profile-specific baseline thresholds", () => {
    expect({
      groupedOrderNeutral: benchmarkThresholdsForProfile("grouped-order-neutral"),
      grpcLeased: benchmarkThresholdsForProfile("grpc-leased"),
      grpcLeasedRetained: benchmarkThresholdsForProfile("grpc-leased-retained"),
      grpcMaterialized: benchmarkThresholdsForProfile("grpc-materialized"),
      kafkaIngest: benchmarkThresholdsForProfile("kafka-ingest"),
      kafkaSustainedFirehose: benchmarkThresholdsForProfile("kafka-sustained-firehose"),
      rawReadWrite: benchmarkThresholdsForProfile("raw-read-write"),
      smoke: benchmarkThresholdsForProfile("smoke"),
      websocketFirehose: benchmarkThresholdsForProfile("websocket-firehose"),
      kafkaIngestBaseline: buildBenchmarkBaseline("kafka-ingest", [observation]).thresholds,
      kafkaSustainedFirehoseBaseline: buildBenchmarkBaseline("kafka-sustained-firehose", [
        observation,
      ]).thresholds,
      rawReadWriteBaseline: buildBenchmarkBaseline("raw-read-write", [observation]).thresholds,
      smokeBaseline: buildBenchmarkBaseline("smoke", [observation]).thresholds,
      websocketFirehoseBaseline: buildBenchmarkBaseline("websocket-firehose", [observation])
        .thresholds,
      orderNeutralBaseline: buildBenchmarkBaseline("grouped-order-neutral", [observation])
        .thresholds,
      grpcLeasedBaseline: buildBenchmarkBaseline("grpc-leased", [observation]).thresholds,
      grpcLeasedRetainedBaseline: buildBenchmarkBaseline("grpc-leased-retained", [observation])
        .thresholds,
      grpcMaterializedBaseline: buildBenchmarkBaseline("grpc-materialized", [observation])
        .thresholds,
    }).toStrictEqual({
      groupedOrderNeutral: groupedOrderNeutralBenchmarkThresholds,
      grpcLeased: grpcRuntimeBenchmarkThresholds,
      grpcLeasedRetained: grpcRetainedRuntimeBenchmarkThresholds,
      grpcMaterialized: grpcRuntimeBenchmarkThresholds,
      kafkaIngest: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehose: kafkaSustainedFirehoseBenchmarkThresholds,
      rawReadWrite: rawReadWriteBenchmarkThresholds,
      smoke: defaultBenchmarkThresholds,
      websocketFirehose: websocketFirehoseBenchmarkThresholds,
      kafkaIngestBaseline: kafkaIngestBenchmarkThresholds,
      kafkaSustainedFirehoseBaseline: kafkaSustainedFirehoseBenchmarkThresholds,
      rawReadWriteBaseline: rawReadWriteBenchmarkThresholds,
      smokeBaseline: defaultBenchmarkThresholds,
      websocketFirehoseBaseline: websocketFirehoseBenchmarkThresholds,
      orderNeutralBaseline: groupedOrderNeutralBenchmarkThresholds,
      grpcLeasedBaseline: grpcRuntimeBenchmarkThresholds,
      grpcLeasedRetainedBaseline: grpcRetainedRuntimeBenchmarkThresholds,
      grpcMaterializedBaseline: grpcRuntimeBenchmarkThresholds,
    });
  });

  it("reads benchmark observations from summary and Vitest artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify(summary)}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      outputJsonPath,
      summaryPath,
    });
  });

  it("reads non-gRPC observations with descriptive benchmark case metadata", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-descriptive-case-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const benchmarkCases = [
      "publish matching row through runtime client and observe through live client",
    ];
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        benchmarkCases,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      benchmarkCases,
      outputJsonPath,
      summaryPath,
    });
  });

  it("reads active-query sharing structural counters from summary artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        activeViewCountBeforeCleanup: 1,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      activeViewCountBeforeCleanup: 1,
      outputJsonPath,
      summaryPath,
    });
  });

  it("reads gRPC benchmark parameters from summary artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const grpcParameters = {
      retainedRows: 500,
      routeCount: 25,
      rowsPerFeed: 50,
    };
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        benchmarkCases: completeGrpcLeasedObservation.benchmarkCases,
        benchmarkScope: "runtime-grpc-leased",
        cases: runtimeGrpcLeasedOperationCases,
        grpcParameters,
        mutationCount: completeGrpcLeasedObservation.mutationCount,
        seedMutationCount: completeGrpcLeasedObservation.seedMutationCount,
      })}\n`,
    );
    writeFileSync(
      outputJsonPath,
      `${JSON.stringify({
        ...vitestOutput,
        files: vitestOutput.files.map((file) => ({
          ...file,
          groups: file.groups.map((group) => ({
            ...group,
            benchmarks: runtimeGrpcLeasedOperationCases.map((operationCase) => ({
              ...group.benchmarks[0],
              name: operationCase.name,
              sampleCount: operationCase.sampleCount,
            })),
          })),
        })),
      })}\n`,
    );

    expect(
      readBenchmarkObservation({
        ...taskPaths(summaryPath, outputJsonPath),
        expectedBenchmarkScope: "runtime-grpc-leased",
      }),
    ).toStrictEqual({
      ...completeGrpcLeasedObservation,
      benchmarkScope: "runtime-grpc-leased",
      grpcParameters,
      outputJsonPath,
      runtimeOperationCases: runtimeGrpcLeasedOperationCases,
      summaryPath,
    });

    expect(
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...completeGrpcLeasedObservation,
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters,
            outputJsonPath,
            runtimeOperationCases: runtimeGrpcLeasedOperationCases,
            summaryPath,
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toStrictEqual({
      artifactKind: "view-server-benchmark-baseline",
      profile: "grpc-leased",
      tasks: [
        {
          ...completeGrpcLeasedObservation,
          benchmarkScope: "runtime-grpc-leased",
          grpcParameters,
          outputJsonPath,
          runtimeOperationCases: runtimeGrpcLeasedOperationCases,
          summaryPath,
        },
      ],
      thresholds: grpcRuntimeBenchmarkThresholds,
    });
  });

  it("reads materialized gRPC operation cases from summary artifacts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const grpcParameters = runtimeGrpcMaterializedParameters;
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        benchmarkCases: runtimeGrpcMaterializedObservation.benchmarkCases,
        benchmarkScope: "runtime-grpc-materialized",
        cases: runtimeGrpcMaterializedOperationCases,
        grpcParameters,
        mutationCount: runtimeGrpcMaterializedObservation.mutationCount,
        seedMutationCount: runtimeGrpcMaterializedObservation.seedMutationCount,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(runtimeGrpcMaterializedVitestOutput)}\n`);

    expect(
      readBenchmarkObservation({
        ...taskPaths(summaryPath, outputJsonPath),
        expectedBenchmarkScope: "runtime-grpc-materialized",
      }),
    ).toStrictEqual({
      ...runtimeGrpcMaterializedObservation,
      benchmarkScope: "runtime-grpc-materialized",
      grpcParameters,
      outputJsonPath,
      runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
      summaryPath,
    });
  });

  it("reads browser benchmark observations without process memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "react-browser-benchmark-summary",
        benchmarkScope: "react-in-memory-live-query",
        browser: {
          browser: "chromium",
          provider: "playwright",
        },
        groupedWriteAdmission: undefined,
        memory: {},
        seedBatchSize: 10,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(browserTaskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      artifactKind: "react-browser-benchmark-summary",
      benchmarkScope: "react-in-memory-live-query",
      browser: {
        browser: "chromium",
        provider: "playwright",
      },
      groupedWriteAdmission: undefined,
      memoryRssTotalDeltaBytes: undefined,
      outputJsonPath,
      seedBatchSize: 10,
      summaryPath,
    });
  });

  it("reads non-Kafka throughput observations without Kafka mutation reconciliation", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      outputJsonPath,
      summaryPath,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    });
  });

  it("reads runtime benchmark observations with process memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeThroughputHealth,
        kafka: {
          ingestLanes: runtimeThroughputKafkaIngestLanes,
        },
        mutationCount: runtimeThroughputMutationCount,
        runtimeMetrics: rawRuntimeMetrics,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation(runtimeTaskPaths(summaryPath, outputJsonPath)),
    ).toStrictEqual({
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      outputJsonPath,
      runtimeMetrics,
      summaryPath,
      throughputCases: comparableRuntimeThroughputCases,
    });
  });

  it("reads non-Kafka runtime benchmark observations without Kafka health", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-websocket-firehose",
        groupedWriteAdmission: undefined,
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
            },
          },
          transport: {
            activeClients: 0,
            activeStreams: 0,
          },
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(
      readBenchmarkObservation({
        ...runtimeTaskPaths(summaryPath, outputJsonPath),
        expectedBenchmarkScope: "runtime-websocket-firehose",
      }),
    ).toStrictEqual({
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-websocket-firehose",
      groupedWriteAdmission: undefined,
      outputJsonPath,
      summaryPath,
    });
  });

  it("accepts duplicate throughput benchmark names across groups with matching sample counts", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-groups-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const duplicateGroupVitestOutput = {
      files: [
        {
          groups: [
            {
              fullName: "src/example-a.bench.ts > first group",
              benchmarks: [
                {
                  max: 3,
                  mean: 2,
                  min: 1,
                  name: "case a",
                  p99: 3,
                  sampleCount: 7,
                },
              ],
            },
            {
              fullName: "src/example-b.bench.ts > second group",
              benchmarks: [
                {
                  max: 4,
                  mean: 3,
                  min: 2,
                  name: "case a",
                  p99: 4,
                  sampleCount: 7,
                },
              ],
            },
          ],
        },
      ],
    };
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(duplicateGroupVitestOutput)}\n`);

    expect(readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toStrictEqual({
      ...observation,
      benchmarks: [
        {
          groupName: "src/example-a.bench.ts > first group",
          maxMs: 3,
          meanMs: 2,
          minMs: 1,
          name: "case a",
          p99Ms: 3,
          sampleCount: 7,
        },
        {
          groupName: "src/example-b.bench.ts > second group",
          maxMs: 4,
          meanMs: 3,
          minMs: 2,
          name: "case a",
          p99Ms: 4,
          sampleCount: 7,
        },
      ],
      outputJsonPath,
      summaryPath,
      throughputCases: comparableNonKafkaRuntimeThroughputCases,
    });
  });

  it("rejects ambiguous throughput benchmark sample counts across groups", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-groups-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    const ambiguousGroupVitestOutput = {
      files: [
        {
          groups: [
            {
              fullName: "src/example-a.bench.ts > first group",
              benchmarks: [
                {
                  max: 3,
                  mean: 2,
                  min: 1,
                  name: "case a",
                  p99: 3,
                  sampleCount: 7,
                },
              ],
            },
            {
              fullName: "src/example-b.bench.ts > second group",
              benchmarks: [
                {
                  max: 4,
                  mean: 3,
                  min: 2,
                  name: "case a",
                  p99: 4,
                  sampleCount: 8,
                },
              ],
            },
          ],
        },
      ],
    };
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(ambiguousGroupVitestOutput)}\n`);

    expect(() => readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath))).toThrow(
      `Benchmark artifact field ${summaryPath}.throughput.cases.benchmarks contains ambiguous benchmark sampleCount values for case a.`,
    );
  });

  it("rejects runtime benchmark observations with malformed throughput", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-throughput-"));
    const invalidSourceSummaryPath = join(directory, "invalid-source.summary.json");
    const invalidRowsSummaryPath = join(directory, "invalid-rows.summary.json");
    const invalidTotalRowsSummaryPath = join(directory, "invalid-total-rows.summary.json");
    const invalidPositiveRateSummaryPath = join(directory, "invalid-positive-rate.summary.json");
    const invalidAggregateSummaryPath = join(directory, "invalid-aggregate.summary.json");
    const invalidMinRowsSummaryPath = join(directory, "invalid-min-rows.summary.json");
    const invalidMaxTotalSummaryPath = join(directory, "invalid-max-total.summary.json");
    const invalidProducerTimerSummaryPath = join(directory, "invalid-producer-timer.summary.json");
    const invalidConvergenceTimerSummaryPath = join(
      directory,
      "invalid-convergence-timer.summary.json",
    );
    const invalidReadTimerMaximumSummaryPath = join(
      directory,
      "invalid-read-timer-maximum.summary.json",
    );
    const invalidReadTimerTotalSummaryPath = join(
      directory,
      "invalid-read-timer-total.summary.json",
    );
    const mismatchedNameSummaryPath = join(directory, "mismatched-name.summary.json");
    const mismatchedSampleCountSummaryPath = join(
      directory,
      "mismatched-sample-count.summary.json",
    );
    const extraThroughputCaseSummaryPath = join(directory, "extra-throughput-case.summary.json");
    const mismatchedMutationTotalSummaryPath = join(
      directory,
      "mismatched-mutation-total.summary.json",
    );
    const missingThroughputSummaryPath = join(directory, "missing-throughput.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      missingThroughputSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      invalidSourceSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          source: "other-timer",
        },
      })}\n`,
    );
    writeFileSync(
      invalidRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              producedRowsPerSample: 0,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidTotalRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              totalProducedRows: 699,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidAggregateSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              aggregateRowsPerSecond: 999,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidPositiveRateSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              aggregateRowsPerSecond: 0,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidMinRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanRowsPerSecond: 900,
              minRowsPerSecond: 901,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidMaxTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxTotalMs: 99,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidProducerTimerSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanProducerSendMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidConvergenceTimerSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              meanConvergenceMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidReadTimerMaximumSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxReadSnapshotMs: 4,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      invalidReadTimerTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              maxReadSnapshotMs: 102,
              meanReadSnapshotMs: 101,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedNameSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              name: "case b",
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedSampleCountSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            {
              ...runtimeThroughput.cases[0],
              sampleCount: 6,
              totalProducedRows: 600,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      extraThroughputCaseSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: {
          ...runtimeThroughput,
          cases: [
            runtimeThroughput.cases[0],
            {
              ...runtimeThroughput.cases[0],
              name: "case b",
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedMutationTotalSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        groupedWriteAdmission: undefined,
        health: runtimeHealth,
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(missingThroughputSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${missingThroughputSummaryPath}.throughput is required for runtime-kafka-ingest.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidSourceSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidSourceSummaryPath}.throughput.source must be benchmark-operation-timers.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidRowsSummaryPath}.throughput.cases[0].producedRowsPerSample must be a positive integer.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidTotalRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidTotalRowsSummaryPath}.throughput.cases[0].totalProducedRows must equal producedRowsPerSample * sampleCount (700).`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidAggregateSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidAggregateSummaryPath}.throughput.cases[0].aggregateRowsPerSecond must match producedRowsPerSample * 1000 / meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidPositiveRateSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidPositiveRateSummaryPath}.throughput.cases[0].aggregateRowsPerSecond must be a positive finite number.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidMinRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidMinRowsSummaryPath}.throughput.cases[0].minRowsPerSecond must be less than or equal to meanRowsPerSecond.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidMaxTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidMaxTotalSummaryPath}.throughput.cases[0].meanTotalMs must be less than or equal to maxTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidProducerTimerSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidProducerTimerSummaryPath}.throughput.cases[0].meanProducerSendMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(
        runtimeTaskPaths(invalidConvergenceTimerSummaryPath, outputJsonPath),
      ),
    ).toThrow(
      `Benchmark artifact field ${invalidConvergenceTimerSummaryPath}.throughput.cases[0].meanConvergenceMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(
        runtimeTaskPaths(invalidReadTimerMaximumSummaryPath, outputJsonPath),
      ),
    ).toThrow(
      `Benchmark artifact field ${invalidReadTimerMaximumSummaryPath}.throughput.cases[0].meanReadSnapshotMs must be less than or equal to maxReadSnapshotMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(invalidReadTimerTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${invalidReadTimerTotalSummaryPath}.throughput.cases[0].meanReadSnapshotMs must be less than or equal to meanTotalMs.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedNameSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedNameSummaryPath}.throughput.cases is missing throughput case case a.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedSampleCountSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedSampleCountSummaryPath}.throughput.cases.case a.sampleCount must equal benchmark sampleCount 7 but was 6.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraThroughputCaseSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraThroughputCaseSummaryPath}.throughput.cases contains throughput case without matching benchmark: case b.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedMutationTotalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedMutationTotalSummaryPath}.throughput.cases totalProducedRows must equal mutationCount 100 but was 700.`,
    );
  });

  it("rejects incomplete runtime benchmark health", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-runtime-health-"));
    const duplicateLaneSummaryPath = join(directory, "duplicate-lane.summary.json");
    const malformedOffsetSummaryPath = join(directory, "malformed-offset.summary.json");
    const mismatchedProducedRowsSummaryPath = join(directory, "mismatched-produced-rows.summary.json");
    const extraRowsSummaryPath = join(directory, "extra-rows.summary.json");
    const extraOffsetsSummaryPath = join(directory, "extra-offsets.summary.json");
    const missingViewServerTopicSummaryPath = join(
      directory,
      "missing-view-server-topic.summary.json",
    );
    const staleRowsSummaryPath = join(directory, "stale-rows.summary.json");
    const staleSecondLaneRowsSummaryPath = join(directory, "stale-second-lane-rows.summary.json");
    const staleOffsetsSummaryPath = join(directory, "stale-offsets.summary.json");
    const wrongViewServerTopicSummaryPath = join(directory, "wrong-view-server-topic.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      duplicateLaneSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
              trades: {
                rowCount: 100,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "orders",
              },
              sourceTrades: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              internalTopic: "orders",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceOrders",
              sourceTopicAlias: "unique-topic-per-run:orders",
            },
            {
              internalTopic: "trades",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceTrades",
              sourceTopicAlias: "unique-topic-per-run:trades",
            },
          ],
        },
        mutationCount: 200,
        topics: ["orders", "trades"],
      })}\n`,
    );
    writeFileSync(
      malformedOffsetSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "not-an-offset",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      missingViewServerTopicSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      mismatchedProducedRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 99,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "99",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              ...runtimeKafkaIngestLanes[0],
              producedRows: 99,
            },
          ],
        },
      })}\n`,
    );
    writeFileSync(
      extraRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          engine: {
            topics: {
              orders: {
                rowCount: 101,
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      extraOffsetsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "101",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      staleRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          engine: {
            topics: {
              orders: {
                rowCount: 99,
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      staleSecondLaneRowsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          engine: {
            topics: {
              orders: {
                rowCount: 100,
              },
              trades: {
                rowCount: 0,
              },
            },
          },
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "orders",
              },
              sourceTrades: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: [
            {
              internalTopic: "orders",
              lane: "orders",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceOrders",
              sourceTopicAlias: "unique-topic-per-run:orders",
            },
            {
              internalTopic: "trades",
              lane: "trades",
              producedRows: 100,
              region: "local",
              sourceTopic: "sourceTrades",
              sourceTopicAlias: "unique-topic-per-run:trades",
            },
          ],
        },
        mutationCount: 200,
        topics: ["orders", "trades"],
      })}\n`,
    );
    writeFileSync(
      staleOffsetsSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "99",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(
      wrongViewServerTopicSummaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "100",
                  },
                },
                viewServerTopic: "trades",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(duplicateLaneSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${duplicateLaneSummaryPath}.kafka.ingestLanes contains duplicate lane orders in lanes orders and orders.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(malformedOffsetSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${malformedOffsetSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must be a non-negative integer string.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(missingViewServerTopicSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${missingViewServerTopicSummaryPath}.health.kafka.topics.sourceOrders.viewServerTopic must be a non-empty string.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(mismatchedProducedRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${mismatchedProducedRowsSummaryPath}.kafka.ingestLanes producedRows total must equal mutationCount 100 but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleRowsSummaryPath}.health.engine.topics.orders.rowCount must equal producedRows 100 for Kafka ingest lane orders but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraRowsSummaryPath}.health.engine.topics.orders.rowCount must equal producedRows 100 for Kafka ingest lane orders but was 101.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleSecondLaneRowsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleSecondLaneRowsSummaryPath}.health.engine.topics.trades.rowCount must equal producedRows 100 for Kafka ingest lane trades but was 0.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(staleOffsetsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${staleOffsetsSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must equal producedRows 100 for Kafka ingest lane orders but was 99.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(wrongViewServerTopicSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${wrongViewServerTopicSummaryPath}.health.kafka.topics.sourceOrders.viewServerTopic must equal internalTopic orders for Kafka ingest lane orders but was trades.`,
    );
    expect(() =>
      readBenchmarkObservation(runtimeTaskPaths(extraOffsetsSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${extraOffsetsSummaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must equal producedRows 100 for Kafka ingest lane orders but was 101.`,
    );
  });

  it("rejects engine benchmark observations with missing RSS memory data", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({ ...summary, memory: { totalDelta: {} } })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.memory.totalDelta.rssBytes is required for engine-benchmark-summary.`,
    );
  });

  it("rejects benchmark observations with unknown artifact kinds", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-artifact-kind-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({ ...summary, artifactKind: "engine-benchmak-summary" })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.artifactKind must be engine-benchmark-summary, react-browser-benchmark-summary, or runtime-benchmark-summary.`,
    );
  });

  it("rejects benchmark observations with too few samples", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-sample-count-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify(summary)}\n`);
    writeFileSync(
      outputJsonPath,
      `${JSON.stringify({
        files: [
          {
            groups: [
              {
                fullName: "src/example.bench.ts > example benchmark group",
                benchmarks: [
                  {
                    ...vitestOutput.files[0].groups[0].benchmarks[0],
                    sampleCount: 1,
                  },
                ],
              },
            ],
          },
        ],
      })}\n`,
    );

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      "task a / src/example.bench.ts > example benchmark group / case a: sampleCount must be at least 5 but was 1.",
    );
  });

  it("rejects benchmark observations that drift from expected task metadata", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-task-metadata-"));
    const artifactKindSummaryPath = join(directory, "artifact-kind.summary.json");
    const benchmarkScopeSummaryPath = join(directory, "benchmark-scope.summary.json");
    const rowCountSummaryPath = join(directory, "row-count.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);
    writeFileSync(
      artifactKindSummaryPath,
      `${JSON.stringify({ ...summary, artifactKind: "react-browser-benchmark-summary" })}\n`,
    );
    writeFileSync(
      benchmarkScopeSummaryPath,
      `${JSON.stringify({ ...summary, benchmarkScope: "other-scope" })}\n`,
    );
    writeFileSync(rowCountSummaryPath, `${JSON.stringify({ ...summary, rowCount: 101 })}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(artifactKindSummaryPath, outputJsonPath)),
    ).toThrow(
      "task a: artifactKind changed from engine-benchmark-summary to react-browser-benchmark-summary.",
    );
    expect(() =>
      readBenchmarkObservation(taskPaths(benchmarkScopeSummaryPath, outputJsonPath)),
    ).toThrow("task a: benchmarkScope changed from engine-raw-snapshot to other-scope.");
    expect(() =>
      readBenchmarkObservation(taskPaths(rowCountSummaryPath, outputJsonPath)),
    ).toThrow("task a: rowCount changed from 100 to 101.");
  });

  it("rejects summaries that point at a different Vitest output artifact", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-output-path-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        latency: {
          outputJsonPath: "other.json",
          source: "vitest-output-json",
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.latency.outputJsonPath changed from actual.json to other.json.`,
    );
  });

  it("roundtrips committed baseline manifests", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-baseline-"));
    const baselinePath = join(directory, "baseline.json");
    const baseline = buildBenchmarkBaseline("smoke", [observation]);

    writeBenchmarkBaseline(baselinePath, baseline);

    expect(readBenchmarkBaseline(baselinePath)).toStrictEqual(baseline);
  });

  it("validates baseline manifests with the default diagnostic path", () => {
    const baseline = buildBenchmarkBaseline("smoke", [observation]);

    expect(validateBenchmarkBaseline(baseline)).toStrictEqual(baseline);
  });

  it("accepts descriptive benchmark case metadata for non-gRPC baseline manifests", () => {
    const descriptiveCaseBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        benchmarkCases: ["publish matching row through runtime client and observe through live client"],
      },
    ]);

    expect(validateBenchmarkBaseline(descriptiveCaseBaseline)).toStrictEqual(descriptiveCaseBaseline);
  });

  it("rejects baseline writes with nonzero invariant counters", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-baseline-counter-"));
    const baselinePath = join(directory, "baseline.json");
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        cleanupLeakCount: 1,
      },
    ]);

    expect(() => writeBenchmarkBaseline(baselinePath, baseline)).toThrow(
      `Benchmark baseline ${baselinePath} is not writable:\ntask a: cleanupLeakCount must stay 0 but was 1.`,
    );
  });

  it("rejects runtime operation cases that do not match benchmark cases", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [
              runtimeGrpcLeasedReuseOperationCase,
            ],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases contains runtime operation case without matching benchmarkCase: gRPC leased same-route reuse.",
    );

    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservationFor([
              runtimeGrpcLeasedOperationCase,
              runtimeGrpcLeasedReuseOperationCase,
            ]),
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [runtimeGrpcLeasedOperationCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases is missing runtime operation case for benchmarkCase: gRPC leased same-route reuse.",
    );
  });

  it("rejects gRPC runtime benchmark cases that do not match Vitest benchmarks", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarkCases: [runtimeGrpcLeasedReuseOperationCase.name],
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [
              runtimeGrpcLeasedReuseOperationCase,
            ],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].benchmarkCases contains benchmarkCase without matching Vitest benchmark: gRPC leased same-route reuse.",
    );

    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarkCases: [],
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [runtimeGrpcLeasedOperationCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].benchmarkCases is missing benchmarkCase for Vitest benchmark: gRPC leased first subscriber.",
    );
  });

  it("rejects under-sampled runtime operation cases", () => {
    const underSampledCase = runtimeGrpcLeasedOperationCaseFor(runtimeGrpcLeasedSample, 4);
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservationFor([underSampledCase]),
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [underSampledCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases.gRPC leased first subscriber.sampleCount must be at least 5 but was 4.",
    );
  });

  it("rejects runtime operation sample counts that differ from Vitest benchmark samples", () => {
    const sixSampleCase = runtimeGrpcLeasedOperationCaseFor(runtimeGrpcLeasedSample, 6);
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservationFor([sixSampleCase]),
            benchmarks: grpcLeasedObservation.benchmarks,
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [sixSampleCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases.gRPC leased first subscriber.sampleCount must equal Vitest benchmark sampleCount 7 but was 6.",
    );
  });

  it("rejects duplicate Vitest benchmark names for runtime operation cases", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarks: [
              grpcLeasedObservation.benchmarks[0],
              {
                ...grpcLeasedObservation.benchmarks[0],
                sampleCount: 8,
              },
            ],
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [runtimeGrpcLeasedOperationCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].benchmarks contains duplicate benchmark name for runtime operation case: gRPC leased first subscriber.",
    );
  });

  it("rejects duplicate Vitest benchmark names with matching sample counts for runtime operation cases", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarks: [
              grpcLeasedObservation.benchmarks[0],
              {
                ...grpcLeasedObservation.benchmarks[0],
                groupName: "src/example.bench.ts > another benchmark group",
              },
            ],
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [runtimeGrpcLeasedOperationCase],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].benchmarks contains duplicate benchmark name for runtime operation case: gRPC leased first subscriber.",
    );
  });

  it("rejects duplicate runtime operation case names", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: [
              runtimeGrpcLeasedOperationCase,
              runtimeGrpcLeasedOperationCase,
            ],
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases contains duplicate runtime operation case: gRPC leased first subscriber.",
    );
  });

  it("rejects malformed materialized gRPC runtime operation timing fields", () => {
    const baseline = {
      artifactKind: "view-server-benchmark-baseline",
      profile: "grpc-materialized",
      tasks: [
        {
          ...runtimeGrpcMaterializedObservation,
          grpcParameters: runtimeGrpcMaterializedParameters,
          runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
        },
      ],
      thresholds: grpcRuntimeBenchmarkThresholds,
    };

    expect(() =>
      validateBenchmarkBaseline({
        ...baseline,
        tasks: [
          {
            ...baseline.tasks[0],
            runtimeOperationCases: replaceGrpcMaterializedOperationCase(
              {
                ...runtimeGrpcMaterializedOperationCase,
                maxHealthOverlayMs: 1,
                meanHealthOverlayMs: 2,
              },
            ),
          },
        ],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].meanHealthOverlayMs must be less than or equal to maxHealthOverlayMs.",
    );
    expect(() =>
      validateBenchmarkBaseline({
        ...baseline,
        tasks: [
          {
            ...baseline.tasks[0],
            runtimeOperationCases: replaceGrpcMaterializedOperationCase(
              {
                ...runtimeGrpcMaterializedOperationCase,
                maxSnapshotMs: 1,
                meanSnapshotMs: 2,
              },
            ),
          },
        ],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].meanSnapshotMs must be less than or equal to maxSnapshotMs.",
    );
    expect(() =>
      validateBenchmarkBaseline({
        ...baseline,
        tasks: [
          {
            ...baseline.tasks[0],
            runtimeOperationCases: replaceGrpcMaterializedOperationCase(
              {
                ...runtimeGrpcMaterializedOperationCase,
                maxStreamConvergenceMs: 1,
                meanStreamConvergenceMs: 2,
              },
            ),
          },
        ],
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases[0].meanStreamConvergenceMs must be less than or equal to maxStreamConvergenceMs.",
    );
  });

  it("requires gRPC runtime operation cases in summaries and baselines", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-observation-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        benchmarkScope: "runtime-grpc-leased",
        grpcParameters: {
          retainedRows: 500,
          routeCount: 25,
          rowsPerFeed: 50,
        },
        seedMutationCount: 0,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation({
        ...taskPaths(summaryPath, outputJsonPath),
        expectedBenchmarkScope: "runtime-grpc-leased",
      }),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.cases is required for runtime-grpc-leased.`,
    );
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-leased",
        tasks: [
          {
            ...grpcLeasedObservation,
            benchmarkScope: "runtime-grpc-leased",
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases is required for runtime-grpc-leased.",
    );
  });

  it("rejects runtime operation cases outside gRPC runtime scopes", () => {
    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "smoke",
        tasks: [
          {
            ...observation,
            runtimeOperationCases: [runtimeGrpcLeasedOperationCase],
          },
        ],
        thresholds: defaultBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeOperationCases is only supported for gRPC runtime scopes.",
    );
  });

  it("rejects malformed gRPC benchmark parameters", () => {
    expect(
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-materialized",
        tasks: [
          {
            ...runtimeGrpcMaterializedObservation,
            grpcParameters: runtimeGrpcMaterializedParameters,
            runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toStrictEqual({
      artifactKind: "view-server-benchmark-baseline",
      profile: "grpc-materialized",
      tasks: [
        {
          ...runtimeGrpcMaterializedObservation,
          grpcParameters: runtimeGrpcMaterializedParameters,
          runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
        },
      ],
      thresholds: grpcRuntimeBenchmarkThresholds,
    });

    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "grpc-materialized",
        tasks: [
          {
            ...runtimeGrpcMaterializedObservation,
            grpcParameters: {
              retainedRows: 500,
              routeCount: 25,
              rowsPerFeed: 50,
            },
            runtimeOperationCases: runtimeGrpcMaterializedOperationCases,
          },
        ],
        thresholds: grpcRuntimeBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].grpcParameters must contain exactly these keys: batchSize, seedRows.",
    );

    expect(() =>
      validateBenchmarkBaseline({
        artifactKind: "view-server-benchmark-baseline",
        profile: "smoke",
        tasks: [
          {
            ...observation,
            grpcParameters: {
              batchSize: 256,
              seedRows: 1000,
            },
          },
        ],
        thresholds: defaultBenchmarkThresholds,
      }),
    ).toThrow(
      "Benchmark artifact field baseline.tasks[0].grpcParameters is only supported for gRPC runtime benchmark scopes.",
    );
  });

  it("requires throughput cases in committed Kafka baselines", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
    };
    const baseline = buildBenchmarkBaseline("kafka-ingest", [kafkaObservation]);
    const completeBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: comparableRuntimeThroughputCases,
      },
    ]);
    const emptyThroughputBaseline = {
      ...completeBaseline,
      tasks: [
        {
          ...completeBaseline.tasks[0],
          throughputCases: [],
        },
      ],
    };
    const renamedThroughputBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            name: "case b",
          },
        ],
      },
    ]);
    const mismatchedSampleCountBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            sampleCount: 6,
            totalProducedRows: 600,
          },
        ],
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is required for runtime-kafka-ingest.",
    );
    expect(() => validateBenchmarkBaseline(emptyThroughputBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases must be a non-empty array.",
    );
    expect(() => validateBenchmarkBaseline(renamedThroughputBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is missing throughput case case a.",
    );
    expect(() => validateBenchmarkBaseline(mismatchedSampleCountBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases.case a.sampleCount must equal benchmark sampleCount 7 but was 6.",
    );
  });

  it("requires throughput cases in committed Kafka sustained firehose baselines", () => {
    const kafkaObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-sustained-firehose",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
    };
    const baseline = buildBenchmarkBaseline("kafka-sustained-firehose", [kafkaObservation]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases is required for runtime-kafka-sustained-firehose.",
    );
  });

  it("preserves Kafka lag precision in runtime metrics", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics,
      },
    ]);

    expect(validateBenchmarkBaseline(baseline).tasks[0].runtimeMetrics).toStrictEqual(
      runtimeMetrics,
    );
  });

  it("normalizes safe numeric Kafka lag runtime metrics", () => {
    const safeNumericLagBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          kafkaLag: {
            maxConsumerLagMessages: 5,
            sampledRegionCount: 1,
            totalConsumerLagMessages: 5,
          },
        },
      },
    ]);

    expect(validateBenchmarkBaseline(safeNumericLagBaseline).tasks[0].runtimeMetrics).toStrictEqual({
      ...runtimeMetrics,
      kafkaLag: {
        maxConsumerLagMessages: "5",
        sampledRegionCount: 1,
        totalConsumerLagMessages: "5",
      },
    });
  });

  it("rejects impossible runtime metric durations", () => {
    const negativeEventLoopDelayBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            ...runtimeMetrics.eventLoopDelay,
            maxMs: -1,
          },
        },
      },
    ]);
    const inconsistentEventLoopMeanBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 4,
            meanMs: 5,
            p99Ms: 3,
          },
        },
      },
    ]);
    const inconsistentEventLoopP99Baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          eventLoopDelay: {
            maxMs: 4,
            meanMs: 2,
            p99Ms: 5,
          },
        },
      },
    ]);
    const inconsistentHealthPollingBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          healthPolling: {
            ...runtimeMetrics.healthPolling,
            maxMs: 8,
            totalMs: 7,
          },
        },
      },
    ]);
    const kafkaThroughputObservation = {
      ...observation,
      artifactKind: "runtime-benchmark-summary",
      benchmarkScope: "runtime-kafka-ingest",
      groupedWriteAdmission: undefined,
      kafkaIngestLanes: comparableRuntimeThroughputKafkaIngestLanes,
      mutationCount: runtimeThroughputMutationCount,
      throughputCases: comparableRuntimeThroughputCases,
    };
    const inconsistentCommitMeanBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxCommitObservedMs: 10,
            meanCommitObservedMs: 11,
          },
        ],
      },
    ]);
    const impossibleCommitMaxBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            maxCommitObservedMs: 101,
          },
        ],
      },
    ]);
    const impossibleCommitMeanBaseline = buildBenchmarkBaseline("kafka-ingest", [
      {
        ...kafkaThroughputObservation,
        throughputCases: [
          {
            ...comparableRuntimeThroughputCases[0],
            aggregateRowsPerSecond: 100_000 / 79,
            maxCommitObservedMs: 90,
            meanCommitObservedMs: 80,
            meanRowsPerSecond: 100_000 / 79,
            meanTotalMs: 79,
          },
        ],
      },
    ]);

    expect(() => validateBenchmarkBaseline(negativeEventLoopDelayBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs must be a non-negative finite number.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentEventLoopMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.meanMs must be less than or equal to baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentEventLoopP99Baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.eventLoopDelay.p99Ms must be less than or equal to baseline.tasks[0].runtimeMetrics.eventLoopDelay.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentHealthPollingBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.healthPolling.totalMs must be greater than or equal to baseline.tasks[0].runtimeMetrics.healthPolling.maxMs.",
    );
    expect(() => validateBenchmarkBaseline(inconsistentCommitMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].meanCommitObservedMs must be less than or equal to maxCommitObservedMs.",
    );
    expect(() => validateBenchmarkBaseline(impossibleCommitMaxBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].maxCommitObservedMs must be less than or equal to maxTotalMs.",
    );
    expect(() => validateBenchmarkBaseline(impossibleCommitMeanBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].throughputCases[0].meanCommitObservedMs must be less than or equal to meanTotalMs.",
    );
  });

  it("rejects unsafe numeric Kafka lag runtime metrics", () => {
    const unsafeNumericLagBaseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        runtimeMetrics: {
          ...runtimeMetrics,
          kafkaLag: {
            maxConsumerLagMessages: 9_007_199_254_740_992,
            sampledRegionCount: 1,
            totalConsumerLagMessages: "0",
          },
        },
      },
    ]);

    expect(() => validateBenchmarkBaseline(unsafeNumericLagBaseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].runtimeMetrics.kafkaLag.maxConsumerLagMessages must be a safe non-negative integer.",
    );
  });

  it("rejects unsafe Kafka committed offset strings", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-runtime-health-"));
    const summaryPath = join(directory, "unsafe-committed-offset.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        health: {
          ...runtimeHealth,
          kafka: {
            topics: {
              sourceOrders: {
                regions: {
                  local: {
                    committedOffset: "9007199254740993",
                  },
                },
                viewServerTopic: "orders",
              },
            },
          },
        },
        kafka: {
          ingestLanes: runtimeKafkaIngestLanes,
        },
        throughput: runtimeThroughput,
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() => readBenchmarkObservation(runtimeTaskPaths(summaryPath, outputJsonPath))).toThrow(
      `Benchmark artifact field ${summaryPath}.health.kafka.topics.sourceOrders.regions.local.committedOffset must be a safe integer string.`,
    );
  });

  it("rejects missing actual RSS data for engine baselines", () => {
    const withoutMemory = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        memoryRssTotalDeltaBytes: undefined,
      },
    ]);

    expect(() => validateBenchmarkBaseline(withoutMemory, "actual")).toThrow(
      "Benchmark artifact field actual.tasks[0].memoryRssTotalDeltaBytes is required for engine-benchmark-summary.",
    );
  });

  it("rejects malformed Vitest output", () => {
    expect(() => comparableBenchmarksFromVitestOutput({ files: {} })).toThrow(
      "Benchmark artifact field vitestOutput.files must be an array.",
    );
  });

  it("rejects malformed benchmark summaries", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(summaryPath, `${JSON.stringify({ ...summary, rowCount: "100" })}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(`Benchmark artifact field ${summaryPath}.rowCount must be a finite number.`);
  });

  it("rejects malformed benchmark mutation counters", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-mutations-"));
    const negativeSummaryPath = join(directory, "negative.summary.json");
    const fractionalSummaryPath = join(directory, "fractional.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(negativeSummaryPath, `${JSON.stringify({ ...summary, mutationCount: -1 })}\n`);
    writeFileSync(fractionalSummaryPath, `${JSON.stringify({ ...summary, mutationCount: 1.5 })}\n`);
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(negativeSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${negativeSummaryPath}.mutationCount must be a non-negative integer.`,
    );
    expect(() =>
      readBenchmarkObservation(taskPaths(fractionalSummaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${fractionalSummaryPath}.mutationCount must be a non-negative integer.`,
    );
  });

  it("rejects malformed benchmark memory summaries", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-memory-"));
    const summaryPath = join(directory, "actual.summary.json");
    const outputJsonPath = join(directory, "actual.json");
    writeFileSync(
      summaryPath,
      `${JSON.stringify({
        ...summary,
        memory: {
          totalDelta: {
            rssBytes: "1024",
          },
        },
      })}\n`,
    );
    writeFileSync(outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() =>
      readBenchmarkObservation(taskPaths(summaryPath, outputJsonPath)),
    ).toThrow(
      `Benchmark artifact field ${summaryPath}.memory.totalDelta.rssBytes must be a finite number.`,
    );
  });

  it("rejects malformed benchmark names", () => {
    expect(() =>
      comparableBenchmarksFromVitestOutput({
        files: [
          {
            groups: [
              {
                fullName: "src/example.bench.ts > example benchmark group",
                benchmarks: [
                  {
                    ...vitestOutput.files[0].groups[0].benchmarks[0],
                    name: "",
                  },
                ],
              },
            ],
          },
        ],
      }),
    ).toThrow("Benchmark artifact field benchmark.name must be a non-empty string.");
  });

  it("rejects malformed baseline manifests", () => {
    const directory = mkdtempSync(join(tmpdir(), "view-server-benchmark-malformed-baseline-"));
    const baselinePath = join(directory, "baseline.json");
    writeFileSync(baselinePath, "[]\n");

    expect(() => readBenchmarkBaseline(baselinePath)).toThrow(
      `Benchmark artifact field ${baselinePath} must be an object.`,
    );
  });

  it("rejects baseline manifests with unknown artifact kinds", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      artifactKind: "benchmark-baseline",
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.artifactKind must be view-server-benchmark-baseline.",
    );
  });

  it("rejects non-positive benchmark sample requirements", () => {
    const baseline = buildBenchmarkBaseline("smoke", [
      {
        ...observation,
        minimumSampleCount: 0,
      },
    ]);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks[0].minimumSampleCount must be a positive integer.",
    );
  });

  it("rejects editable baseline threshold drift", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        ...defaultBenchmarkThresholds,
        latencyMean: {
          maxAbsoluteDeltaMs: 5000,
          maxRatio: 8000,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds must match code-owned profile thresholds.",
    );
  });

  it("rejects stale extra baseline threshold keys", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        ...defaultBenchmarkThresholds,
        commitObservedMean: {
          maxAbsoluteDeltaMs: 10,
          maxRatio: 2,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds must contain exactly these keys: latencyMean, latencyP99, memoryRssTotalDelta, throughputAggregateRowsPerSecond.",
    );
  });

  it("rejects stale extra nested baseline threshold keys", () => {
    const baseline = {
      ...buildBenchmarkBaseline("smoke", [observation]),
      thresholds: {
        ...defaultBenchmarkThresholds,
        latencyMean: {
          ...defaultBenchmarkThresholds.latencyMean,
          staleKey: 123,
        },
      },
    };

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.thresholds.latencyMean must contain exactly these keys: maxAbsoluteDeltaMs, maxRatio.",
    );
  });

  it("rejects empty baseline task manifests", () => {
    const baseline = buildBenchmarkBaseline("smoke", []);

    expect(() => validateBenchmarkBaseline(baseline)).toThrow(
      "Benchmark artifact field baseline.tasks must be a non-empty array.",
    );
  });

});
