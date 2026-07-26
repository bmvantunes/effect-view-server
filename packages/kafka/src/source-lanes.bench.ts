// Import Vitest directly so the Effect test-runtime graph does not distort
// Kafka Source Lane hot-path measurements.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Deferred, Effect, Exit, Queue, Schema, Scope, Stream } from "effect";
import { kafka, type KafkaMessageMetadata } from "./contract";
import { kafkaNodeInternals } from "./node-internal";
import { makeKafkaServerLayer, type KafkaServerRecord, type KafkaServerRegion } from "./server";

const benchmarkInteger = (name: string, fallback: number): number => {
  const configured = process.env[name];
  if (configured === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(configured, 10);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative safe integer.`);
  }
  return parsed;
};

const benchmarkOptions = {
  iterations: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_ITERATIONS", 5),
  time: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_TIME_MS", 0),
  warmupIterations: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_WARMUP_ITERATIONS", 1),
  warmupTime: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_WARMUP_TIME_MS", 0),
};
const laneBatchSize = benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_ROWS", 64);
const metricsPartitionCount = benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BENCH_PARTITIONS", 64);
const multiRegionCount = 4;
if (laneBatchSize === 0 || metricsPartitionCount === 0 || laneBatchSize % multiRegionCount !== 0) {
  throw new Error(
    "Kafka Source Adapter benchmark rows and partitions must be positive, and rows must divide evenly across four Regions.",
  );
}
const multiRegionBatchSize = laneBatchSize / multiRegionCount;
const encoder = new TextEncoder();
const benchmarkCaseNames = [
  `single Region Lane (${laneBatchSize} accepted records across ${metricsPartitionCount} partitions)`,
  `poison Rejection plus valid continuation (${laneBatchSize} records across ${metricsPartitionCount} partitions)`,
  `four concurrent Region Lanes (${laneBatchSize} accepted records across ${metricsPartitionCount} partitions)`,
  `${metricsPartitionCount}-partition assignment, commit, lag, and snapshot metrics`,
] as const;
const outputJsonPath =
  process.env["VIEW_SERVER_KAFKA_SOURCE_BENCH_OUTPUT_JSON"]?.trim() ||
  join(".artifacts", "source-lanes-64rows-64partitions.json");

type MemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

const memorySnapshot = (): MemorySnapshot => {
  const current = process.memoryUsage();
  return {
    arrayBuffersBytes: current.arrayBuffers,
    externalBytes: current.external,
    heapTotalBytes: current.heapTotal,
    heapUsedBytes: current.heapUsed,
    rssBytes: current.rss,
  };
};

const memoryDelta = (before: MemorySnapshot, after: MemorySnapshot): MemorySnapshot => ({
  arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
  externalBytes: after.externalBytes - before.externalBytes,
  heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
  heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
  rssBytes: after.rssBytes - before.rssBytes,
});

const benchmarkSummaryPath = (path: string): string =>
  path.endsWith(".json")
    ? `${path.slice(0, -".json".length)}.summary.json`
    : `${path}.summary.json`;

const writeJsonFile = (path: string, value: unknown): void => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
};

const memoryBefore = memorySnapshot();
let memoryAfterSetup = memoryBefore;

const Row = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
  region: Schema.String,
});
const WireRow = Schema.Struct({
  value: Schema.Number,
});

type BenchmarkRecord = {
  readonly key: string;
  readonly value: string;
};

type BenchmarkRegion = {
  readonly runtime: KafkaServerRegion;
  readonly awaitBindings: (viewServerTopics: ReadonlyArray<string>) => Effect.Effect<void>;
  readonly offerBatch: (
    viewServerTopic: string,
    sourceTopic: string,
    batch: ReadonlyArray<BenchmarkRecord>,
  ) => Effect.Effect<void>;
};

type BenchmarkState = {
  readonly regions: ReadonlyMap<string, BenchmarkRegion>;
};

let state: BenchmarkState | undefined;
let benchmarkScope: Scope.Closeable | undefined;
let nextBatch = 0;

const requireState = (): BenchmarkState => {
  if (state === undefined) {
    throw new Error("Kafka Source Lane benchmark setup did not complete.");
  }
  return state;
};

const requireRegion = (current: BenchmarkState, region: string): BenchmarkRegion => {
  const found = current.regions.get(region);
  if (found === undefined) {
    throw new Error(`Kafka benchmark Region ${region} is not configured.`);
  }
  return found;
};

const metadata = (
  sourceTopic: string,
  sourceRegion: string,
  partition: number,
  offset: bigint,
): KafkaMessageMetadata => ({
  sourceTopic,
  sourceRegion,
  partition,
  offset,
  timestampNanos: offset * 1_000_000n,
  headers: {},
});

const emptyRegionMetrics = (region: string) => ({
  region,
  assignments: [],
  commits: 0n,
  commitFailures: 0n,
  decoded: 0n,
  decodeFailures: 0n,
  mapped: 0n,
  mappingFailures: 0n,
  rejections: 0n,
  reconnects: 0n,
  rebalances: 0n,
  closes: 0n,
  closeFailures: 0n,
});

const makeBenchmarkRegion = (region: string): BenchmarkRegion => {
  const bindings = new Map<
    string,
    Queue.Queue<KafkaServerRecord, import("./contract").KafkaAdapterFailure>
  >();
  const runtime: KafkaServerRegion = {
    acquire: (input) =>
      Effect.gen(function* () {
        const queue = yield* Queue.unbounded<
          KafkaServerRecord,
          import("./contract").KafkaAdapterFailure
        >();
        bindings.set(input.viewServerTopic, queue);
        const scope = yield* Effect.scope;
        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            if (bindings.get(input.viewServerTopic) === queue) {
              bindings.delete(input.viewServerTopic);
            }
          }).pipe(Effect.andThen(Queue.shutdown(queue))),
        );
        return {
          records: Stream.fromQueue(queue),
          recordDecoded: Effect.void,
          recordDecodeFailure: Effect.void,
          recordMapped: Effect.void,
          recordMappingFailure: Effect.void,
          recordRejection: Effect.void,
        };
      }),
    metrics: () => Effect.succeed(emptyRegionMetrics(region)),
  };
  return {
    runtime,
    awaitBindings: (viewServerTopics) =>
      Effect.gen(function* () {
        for (let turn = 0; turn < 10_000; turn += 1) {
          if (viewServerTopics.every((topic) => bindings.has(topic))) {
            return;
          }
          yield* Effect.yieldNow;
        }
        return yield* Effect.die(
          new Error(`Kafka benchmark Region ${region} did not acquire every binding.`),
        );
      }),
    offerBatch: (viewServerTopic, sourceTopic, batch) =>
      Effect.gen(function* () {
        const queue = bindings.get(viewServerTopic);
        if (queue === undefined) {
          return yield* Effect.die(
            new Error(`Kafka benchmark binding ${region}:${viewServerTopic} is not active.`),
          );
        }
        const settled = yield* Deferred.make<void>();
        yield* Effect.forEach(
          batch,
          (record, index) =>
            Queue.offer(queue, {
              key: encoder.encode(record.key),
              value: encoder.encode(record.value),
              metadata: metadata(sourceTopic, region, index % metricsPartitionCount, BigInt(index)),
              settlement: (applicationExit) =>
                Exit.isSuccess(applicationExit)
                  ? index === batch.length - 1
                    ? Deferred.succeed(settled, undefined).pipe(Effect.asVoid)
                    : Effect.void
                  : Effect.void,
            }),
          { discard: true },
        );
        yield* Deferred.await(settled);
      }),
  };
};

const makeSource = (sourceTopic: string, regions: readonly [string, ...ReadonlyArray<string>]) =>
  kafka.source({
    topic: sourceTopic,
    regions,
    key: kafka.string(),
    value: kafka.json(() => Schema.toCodecJson(WireRow)),
    localRowKey: ({ key }) => key,
    map: ({ value, region }) => ({
      value: value.value,
      region,
    }),
    startFrom: "earliest",
  });

beforeAll(async () => {
  const scope = Effect.runSync(Scope.make("sequential"));
  benchmarkScope = scope;
  await Effect.runPromise(
    Effect.gen(function* () {
      const single = makeBenchmarkRegion("single");
      const region1 = makeBenchmarkRegion("region-1");
      const region2 = makeBenchmarkRegion("region-2");
      const region3 = makeBenchmarkRegion("region-3");
      const region4 = makeBenchmarkRegion("region-4");
      const regions = new Map([
        ["single", single],
        ["region-1", region1],
        ["region-2", region2],
        ["region-3", region3],
        ["region-4", region4],
      ]);
      const config = defineViewServerConfig({
        topics: {
          accepted: {
            schema: Row,
            source: makeSource("accepted-source", ["single"]),
          },
          poisoned: {
            schema: Row,
            source: makeSource("poisoned-source", ["single"]),
          },
          multiRegion: {
            schema: Row,
            source: makeSource("multi-region-source", [
              "region-1",
              "region-2",
              "region-3",
              "region-4",
            ]),
          },
        },
      });
      const layer = makeKafkaServerLayer({
        consumerGroupPrefix: "kafka-source-benchmark",
        regions: new Map(Array.from(regions, ([name, value]) => [name, value.runtime])),
      });
      yield* Effect.acquireRelease(
        makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer)),
        (runtime) => runtime.close,
      );
      yield* Effect.all(
        [
          single.awaitBindings(["accepted", "poisoned"]),
          region1.awaitBindings(["multiRegion"]),
          region2.awaitBindings(["multiRegion"]),
          region3.awaitBindings(["multiRegion"]),
          region4.awaitBindings(["multiRegion"]),
        ],
        { concurrency: "unbounded", discard: true },
      );
      state = {
        regions,
      };
      memoryAfterSetup = memorySnapshot();
    }).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    ),
  );
});

afterAll(async () => {
  if (benchmarkScope !== undefined) {
    await Effect.runPromise(Scope.close(benchmarkScope, Exit.void));
  }
  const memoryAfterBenchmark = memorySnapshot();
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount: 0,
    benchmarkCases: benchmarkCaseNames,
    benchmarkName: "Kafka Source Adapter lane and metrics benchmark",
    benchmarkScope: "kafka-source-adapter",
    cleanupLeakCount: 0,
    health: null,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memoryAfterBenchmark,
      afterSetup: memoryAfterSetup,
      before: memoryBefore,
      setupDelta: memoryDelta(memoryBefore, memoryAfterSetup),
      totalDelta: memoryDelta(memoryBefore, memoryAfterBenchmark),
    },
    mutationCount: laneBatchSize,
    notes: [
      "Source Lane cases exercise the production materialized Kafka Source Adapter processor.",
      "Every Lane batch spans 64 synthetic partitions so partition-sensitive hot paths stay visible.",
      "The metrics case exercises assignment, commit, lag, and sampling bookkeeping across 64 partitions.",
    ],
    queuedEventCount: 0,
    rowCount: laneBatchSize,
    subscriberCount: 0,
    topics: ["accepted", "poisoned", "multiRegion"],
  });
});

const validBatch = (label: string, batch: number, count: number): ReadonlyArray<BenchmarkRecord> =>
  Array.from({ length: count }, (_, index) => ({
    key: `${label}-${batch}-${index}`,
    value: JSON.stringify({ value: index }),
  }));

describe("Kafka Source Adapter lanes", () => {
  bench(
    benchmarkCaseNames[0],
    async () => {
      const current = requireState();
      const batch = nextBatch;
      nextBatch += 1;
      await Effect.runPromise(
        requireRegion(current, "single").offerBatch(
          "accepted",
          "accepted-source",
          validBatch("accepted", batch, laneBatchSize),
        ),
      );
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[1],
    async () => {
      const current = requireState();
      const batch = nextBatch;
      nextBatch += 1;
      const valid = validBatch("poisoned", batch, laneBatchSize - 1);
      await Effect.runPromise(
        requireRegion(current, "single").offerBatch("poisoned", "poisoned-source", [
          {
            key: `poisoned-${batch}-invalid`,
            value: "{not-json",
          },
          ...valid,
        ]),
      );
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[2],
    async () => {
      const current = requireState();
      const batch = nextBatch;
      nextBatch += 1;
      await Effect.runPromise(
        Effect.forEach(
          ["region-1", "region-2", "region-3", "region-4"],
          (region) =>
            requireRegion(current, region).offerBatch(
              "multiRegion",
              "multi-region-source",
              validBatch(region, batch, multiRegionBatchSize),
            ),
          { concurrency: "unbounded", discard: true },
        ),
      );
    },
    benchmarkOptions,
  );

  const metrics = kafkaNodeInternals.emptyMutableMetrics();
  const offsets = Array.from({ length: metricsPartitionCount }, (_, partition) => ({
    topic: "source-orders",
    partition,
    offset: BigInt(partition),
  }));
  const latestOffsets = Array.from({ length: metricsPartitionCount }, (_, partition) =>
    BigInt(partition + laneBatchSize),
  );
  const initial = {
    offsets,
    latestOffsets,
  };
  const partitions = Array.from({ length: metricsPartitionCount }, (_, partition) => partition);
  const lag = new Map([
    [
      "source-orders",
      Array.from({ length: metricsPartitionCount }, (_, partition) =>
        BigInt(metricsPartitionCount - partition),
      ),
    ],
  ]);

  bench(
    benchmarkCaseNames[3],
    () => {
      kafkaNodeInternals.resetAttemptAssignments(metrics, offsets, latestOffsets);
      for (const partition of partitions) {
        kafkaNodeInternals.updateCommit(metrics, initial, partition, BigInt(partition + 1));
      }
      kafkaNodeInternals.updateLag(metrics, "source-orders", lag);
      kafkaNodeInternals.updateAssignmentOffsets(metrics, offsets, latestOffsets, partitions);
      kafkaNodeInternals.snapshotMetrics("benchmark", metrics);
    },
    benchmarkOptions,
  );
});
