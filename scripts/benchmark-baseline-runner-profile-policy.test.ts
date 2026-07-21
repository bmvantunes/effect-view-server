import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import { profiles } from "./benchmark-baseline-profiles.mjs";
import { rawPredicateIndexTask } from "./benchmark-baseline-task-catalog.mjs";

describe("benchmark baseline runner", () => {
  it("keeps the focused active-query-sharing gate in pull request CI", () => {
    const ciWorkflow = readFileSync(".github/workflows/ci.yml", "utf8");

    expect(ciWorkflow).toContain(
      "      - name: Active query sharing benchmark regression gate\n" +
        "        run: vp run -w bench:baseline:active-query-sharing\n",
    );
  });

  it("requires timed-read and peak-RSS runner policy together", () => {
    expect(() =>
      rawPredicateIndexTask(100, {
        VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC: "process-peak-over-initial-current",
      }),
    ).toThrow("Peak RSS measurement requires timed read sampling.");
    expect(() =>
      rawPredicateIndexTask(100, {
        VIEW_SERVER_ENGINE_BENCH_TIMED_READ_MINIMUM_SAMPLES: "1000",
      }),
    ).toThrow("Timed read sampling requires process-peak-over-initial-current RSS measurement.");
  });

  it("keeps targeted grouped baseline scripts in compare mode by default", () => {
    const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;

    expect({
      activeQuerySharing: scripts["bench:baseline:active-query-sharing"],
      activeQuerySharingUpdate: scripts["bench:baseline:active-query-sharing:update"],
      groupedAdmission: scripts["bench:baseline:grouped-admission"],
      groupedAdmissionUpdate: scripts["bench:baseline:grouped-admission:update"],
      groupedOrderNeutral: scripts["bench:baseline:grouped-order-neutral"],
      groupedOrderNeutralUpdate: scripts["bench:baseline:grouped-order-neutral:update"],
      grpcGate: scripts["grpc:gate"],
      grpcLeased: scripts["bench:baseline:grpc-leased"],
      grpcLeasedRetained: scripts["bench:baseline:grpc-leased-retained"],
      grpcLeasedRetainedRepeat: scripts["bench:baseline:grpc-leased-retained:repeat"],
      grpcLeasedRetainedUpdate: scripts["bench:baseline:grpc-leased-retained:update"],
      grpcLeasedUpdate: scripts["bench:baseline:grpc-leased:update"],
      grpcMaterialized: scripts["bench:baseline:grpc-materialized"],
      grpcMaterializedUpdate: scripts["bench:baseline:grpc-materialized:update"],
      kafkaIngest: scripts["bench:baseline:kafka-ingest"],
      kafkaIngestUpdate: scripts["bench:baseline:kafka-ingest:update"],
      kafkaSustainedFirehose: scripts["bench:baseline:kafka-sustained-firehose"],
      kafkaSustainedFirehoseUpdate: scripts["bench:baseline:kafka-sustained-firehose:update"],
      preGrpcGate: scripts["pre-grpc:gate"],
      rawReadWrite: scripts["bench:baseline:raw-read-write"],
      rawReadWriteUpdate: scripts["bench:baseline:raw-read-write:update"],
      release: scripts["bench:baseline:release"],
      webSocketFirehose: scripts["bench:baseline:websocket-firehose"],
      webSocketFirehoseUpdate: scripts["bench:baseline:websocket-firehose:update"],
    }).toStrictEqual({
      activeQuerySharing:
        "node scripts/run-benchmark-baseline.mjs --profile=active-query-sharing",
      activeQuerySharingUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=active-query-sharing --update-baseline",
      groupedAdmission: "node scripts/run-benchmark-baseline.mjs --profile=grouped-admission",
      groupedAdmissionUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grouped-admission --update-baseline",
      groupedOrderNeutral: "node scripts/run-benchmark-baseline.mjs --profile=grouped-order-neutral",
      groupedOrderNeutralUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grouped-order-neutral --update-baseline",
      grpcGate:
        "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w ready && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-materialized && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-leased && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-leased-retained",
      grpcLeased: "node scripts/run-benchmark-baseline.mjs --profile=grpc-leased",
      grpcLeasedRetained: "node scripts/run-benchmark-baseline.mjs --profile=grpc-leased-retained",
      grpcLeasedRetainedRepeat:
        "node scripts/run-benchmark-baseline.mjs --profile=grpc-leased-retained --repeat=3 --no-compare",
      grpcLeasedRetainedUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grpc-leased-retained --update-baseline",
      grpcLeasedUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grpc-leased --update-baseline",
      grpcMaterialized: "node scripts/run-benchmark-baseline.mjs --profile=grpc-materialized",
      grpcMaterializedUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grpc-materialized --update-baseline",
      kafkaIngest: "node scripts/run-benchmark-baseline.mjs --profile=kafka-ingest",
      kafkaIngestUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-ingest --update-baseline",
      kafkaSustainedFirehose:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-sustained-firehose",
      kafkaSustainedFirehoseUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-sustained-firehose --update-baseline",
      preGrpcGate:
        "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w ready && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:smoke && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:raw-read-write && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:active-query-sharing && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grouped-admission && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grouped-order-neutral && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:websocket-firehose && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:kafka-ingest && VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:kafka-sustained-firehose",
      rawReadWrite: "node scripts/run-benchmark-baseline.mjs --profile=raw-read-write",
      rawReadWriteUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=raw-read-write --update-baseline",
      release:
        "NODE_OPTIONS=--max-old-space-size=12288 node scripts/run-benchmark-baseline.mjs --profile=release --no-compare",
      webSocketFirehose: "node scripts/run-benchmark-baseline.mjs --profile=websocket-firehose",
      webSocketFirehoseUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=websocket-firehose --update-baseline",
    });
  });

  it("keeps the pre-gRPC gate bounded and covering strict compare-mode benchmark gates", () => {
    const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;
    const preGrpcGateSteps = scripts["pre-grpc:gate"].split(" && ");
    const preGrpcBenchmarkGates = preGrpcGateSteps
      .slice(1)
      .map((step: string) => step.replace("VP_RUN_CONCURRENCY_LIMIT=1 vp run -w ", ""));
    const strictPreGrpcCompareBenchmarkGates = Object.entries(scripts)
      .filter(([name, command]) =>
        name.startsWith("bench:baseline:") &&
        !name.endsWith(":update") &&
        name !== "bench:baseline:grpc-materialized" &&
        name !== "bench:baseline:grpc-leased" &&
        name !== "bench:baseline:grpc-leased-retained" &&
        command === command.replace(" --no-compare", ""),
      )
      .map(([name]) => name)
      .sort();

    expect(preGrpcGateSteps).toStrictEqual([
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w ready",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:smoke",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:raw-read-write",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:active-query-sharing",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grouped-admission",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grouped-order-neutral",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:websocket-firehose",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:kafka-ingest",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:kafka-sustained-firehose",
    ]);
    expect(preGrpcBenchmarkGates.toSorted()).toStrictEqual(strictPreGrpcCompareBenchmarkGates);
    expect(preGrpcBenchmarkGates).not.toContain("bench:baseline:release");
    expect(preGrpcBenchmarkGates).not.toContain("bench:baseline:grpc-materialized");
    expect(preGrpcBenchmarkGates).not.toContain("bench:baseline:grpc-leased");
    expect(preGrpcBenchmarkGates).not.toContain("bench:baseline:grpc-leased-retained");
  });

  it("keeps the gRPC gate bounded and scoped to gRPC runtime baselines", () => {
    const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;

    expect(scripts["grpc:gate"].split(" && ")).toStrictEqual([
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w ready",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-materialized",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-leased",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:grpc-leased-retained",
    ]);
  });

  it("keeps the release-candidate capacity gate bounded, serial, and complete", () => {
    const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;

    expect(scripts["release-candidate:capacity"].split(" && ")).toStrictEqual([
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w examples:test",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w examples:build",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w pre-grpc:gate",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w grpc:gate",
      "VP_RUN_CONCURRENCY_LIMIT=1 vp run -w bench:baseline:release",
    ]);
  });

  it("defines raw read and write performance gate tasks", () => {
    const rawReadWriteTasks = profiles.get("raw-read-write") ?? [];

    expect(
      rawReadWriteTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        expectedMutationCount: task.expectedMutationCount,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        minimumSampleCount: task.minimumSampleCount,
        mutationIterations: task.env["VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        samplingPolicy: task.samplingPolicy,
        task: task.args,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
        warmupIterations: task.env["VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS"],
        warmupTimeMs: task.env["VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS"],
        writeMode: task.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"],
      })),
    ).toStrictEqual([
      {
        batchSize: undefined,
        benchmarkScope: "engine-raw-snapshot",
        expectedMutationCount: 100020,
        iterations: "1000",
        minimumSampleCount: 20,
        mutationIterations: "20",
        outputJsonPath: ".artifacts/raw-snapshot-100000rows.json",
        rowCount: "100000",
        samplingPolicy: {
          iterationBoundCases: [
            {
              name: "live subscription delta after publish",
              sampleCount: 20,
              timeMs: 0,
              warmupIterations: 0,
              warmupTimeMs: 0,
            },
          ],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-snapshot"],
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
        writeMode: undefined,
      },
      {
        batchSize: undefined,
        benchmarkScope: "engine-raw-predicate-index",
        expectedMutationCount: undefined,
        iterations: "1000",
        minimumSampleCount: 1000,
        mutationIterations: undefined,
        outputJsonPath: ".artifacts/raw-predicate-index-100000rows.json",
        rowCount: "100000",
        samplingPolicy: {
          iterationBoundCases: [],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-predicate-index"],
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
        writeMode: undefined,
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-write",
        expectedMutationCount: undefined,
        iterations: "20",
        minimumSampleCount: 20,
        mutationIterations: undefined,
        outputJsonPath: ".artifacts/raw-write-base-100000rows.json",
        rowCount: "100000",
        samplingPolicy: undefined,
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-write"],
        timeMs: "0",
        warmupIterations: "0",
        warmupTimeMs: "0",
        writeMode: "base",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-write",
        expectedMutationCount: undefined,
        iterations: "20",
        minimumSampleCount: 20,
        mutationIterations: undefined,
        outputJsonPath: ".artifacts/raw-write-indexed-100000rows.json",
        rowCount: "100000",
        samplingPolicy: undefined,
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-write"],
        timeMs: "0",
        warmupIterations: "0",
        warmupTimeMs: "0",
        writeMode: "indexed",
      },
    ]);
  });

  it("keeps release read tasks outside the smoke sampling policy", () => {
    const releaseReadScopes = new Set([
      "engine-grouped-aggregate",
      "engine-grouped-key-width",
      "engine-raw-predicate-index",
      "engine-raw-snapshot",
    ]);
    const releaseReadTasks = (profiles.get("release") ?? []).filter((task) =>
      releaseReadScopes.has(task.expectedBenchmarkScope),
    );

    expect(
      releaseReadTasks.map((task) => ({
        expectedMutationCount: task.expectedMutationCount,
        label: task.label,
        samplingPolicy: task.samplingPolicy,
        timedReadMinimumSamples:
          task.env["VIEW_SERVER_ENGINE_BENCH_TIMED_READ_MINIMUM_SAMPLES"],
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        expectedMutationCount: undefined,
        label: "raw snapshot 100000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "raw snapshot 1000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "raw snapshot 10000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "raw predicate index 100000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "raw predicate index 1000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "raw predicate index 10000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: undefined,
      },
      {
        expectedMutationCount: undefined,
        label: "grouped aggregate 100000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: "0",
      },
      {
        expectedMutationCount: undefined,
        label: "grouped aggregate 1000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: "0",
      },
      {
        expectedMutationCount: undefined,
        label: "grouped aggregate 5000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: "0",
      },
      {
        expectedMutationCount: undefined,
        label: "grouped key width 100000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: "0",
      },
      {
        expectedMutationCount: undefined,
        label: "grouped key width 1000000 rows",
        samplingPolicy: undefined,
        timedReadMinimumSamples: undefined,
        timeMs: "0",
      },
    ]);
  });

  it("defines exact raw write smoke tasks", () => {
    const rawWriteSmokeTasks = (profiles.get("smoke") ?? []).filter(
      (task) => task.expectedBenchmarkScope === "engine-raw-write",
    );

    expect(
      rawWriteSmokeTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
        writeMode: task.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"],
      })),
    ).toStrictEqual([
      {
        batchSize: "100",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-write-base-1000rows.json",
        timeMs: "0",
        writeMode: "base",
      },
      {
        batchSize: "100",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-write-indexed-1000rows.json",
        timeMs: "0",
        writeMode: "indexed",
      },
    ]);
  });

  it("stabilizes smoke read sampling without changing mutation task sampling", () => {
    const smokeTasks = profiles.get("smoke") ?? [];
    const taskByScope = new Map(
      smokeTasks.map((task) => [
        `${task.expectedBenchmarkScope}:${task.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"] ?? task.env["VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE"] ?? "default"}`,
        task,
      ]),
    );
    const samplingFor = (key: string) => {
      const task = taskByScope.get(key);
      return {
        expectedMutationCount: task?.expectedMutationCount,
        iterations: task?.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        minimumSampleCount: task?.minimumSampleCount,
        mutationIterations: task?.env["VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS"],
        samplingPolicy: task?.samplingPolicy,
        timeMs: task?.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
        warmupIterations: task?.env["VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS"],
        warmupTimeMs: task?.env["VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS"],
      };
    };

    expect({
      groupedAggregate: samplingFor("engine-grouped-aggregate:default"),
      groupedKeyWidth: samplingFor("engine-grouped-key-width:default"),
      rawPredicateIndex: samplingFor("engine-raw-predicate-index:default"),
      rawSnapshot: samplingFor("engine-raw-snapshot:default"),
      rawWrite: samplingFor("engine-raw-write:base"),
      sameWindowFanout: samplingFor("engine-raw-live-fanout:same-window"),
    }).toStrictEqual({
      groupedAggregate: {
        expectedMutationCount: 1005,
        iterations: "1000",
        minimumSampleCount: 5,
        mutationIterations: "5",
        samplingPolicy: {
          iterationBoundCases: [
            {
              name: "live grouped aggregate delta after publish",
              sampleCount: 5,
              timeMs: 0,
              warmupIterations: 0,
              warmupTimeMs: 0,
            },
          ],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
      },
      groupedKeyWidth: {
        expectedMutationCount: undefined,
        iterations: "1000",
        minimumSampleCount: 1000,
        mutationIterations: undefined,
        samplingPolicy: {
          iterationBoundCases: [],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
      },
      rawPredicateIndex: {
        expectedMutationCount: undefined,
        iterations: "1000",
        minimumSampleCount: 1000,
        mutationIterations: undefined,
        samplingPolicy: {
          iterationBoundCases: [],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
      },
      rawSnapshot: {
        expectedMutationCount: 1005,
        iterations: "1000",
        minimumSampleCount: 5,
        mutationIterations: "5",
        samplingPolicy: {
          iterationBoundCases: [
            {
              name: "live subscription delta after publish",
              sampleCount: 5,
              timeMs: 0,
              warmupIterations: 0,
              warmupTimeMs: 0,
            },
          ],
          memoryRssMetric: "process-peak-over-initial-current",
          measured: {
            minimumSampleCount: 1000,
            timeMs: 250,
            warmupIterations: 5,
            warmupTimeMs: 100,
          },
        },
        timeMs: "250",
        warmupIterations: "5",
        warmupTimeMs: "100",
      },
      rawWrite: {
        expectedMutationCount: undefined,
        iterations: "5",
        minimumSampleCount: 5,
        mutationIterations: undefined,
        samplingPolicy: undefined,
        timeMs: "0",
        warmupIterations: "0",
        warmupTimeMs: "0",
      },
      sameWindowFanout: {
        expectedMutationCount: undefined,
        iterations: "5",
        minimumSampleCount: 5,
        mutationIterations: undefined,
        samplingPolicy: undefined,
        timeMs: "1",
        warmupIterations: "0",
        warmupTimeMs: "0",
      },
    });
  });
});
