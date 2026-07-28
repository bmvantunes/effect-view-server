// Import Vitest directly so the Effect test-runtime graph does not distort
// Kafka Source Lane hot-path measurements.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Deferred, Effect, Exit, Queue, Schema, Scope, Stream } from "effect";
import { kafka, type KafkaMessageMetadata } from "./contract";
import { kafkaNodeInternals } from "./node-internal";
import { makeKafkaServerLayer, type KafkaServerRecord, type KafkaServerRegion } from "./server";
import { OrderValueSchema } from "./test-fixtures/orders_pb";

const benchmarkInteger = (name: string, fallback: number): number => {
  const configured = process.env[name];
  if (configured === undefined) {
    return fallback;
  }
  const parsed = Number(configured);
  if (configured.trim().length === 0 || !Number.isSafeInteger(parsed) || parsed < 0) {
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
  `JSON single Region Lane (${laneBatchSize} accepted records across ${metricsPartitionCount} partitions)`,
  `protobuf single Region Lane (${laneBatchSize} accepted records across ${metricsPartitionCount} partitions)`,
  `${laneBatchSize}-record mixed JSON/protobuf burst`,
  `sustained JSON/protobuf ingestion (${laneBatchSize} records per wave)`,
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
  id: ViewServerId,
  value: Schema.Number,
  region: Schema.String,
});
const WireRow = Schema.Struct({
  value: Schema.Number,
});

type BenchmarkRecord = {
  readonly key: string;
  readonly value: string | Uint8Array;
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
  readonly retainedRowIds: Effect.Effect<
    {
      readonly accepted: ReadonlyArray<string>;
      readonly protobuf: ReadonlyArray<string>;
      readonly poisoned: ReadonlyArray<string>;
      readonly multiRegion: ReadonlyArray<string>;
    },
    unknown
  >;
};

let state: BenchmarkState | undefined;
let benchmarkScope: Scope.Closeable | undefined;
let acceptedRevision = 0;
let protobufRevision = 0;
let mixedRevision = 0;
let sustainedRevision = 0;
let poisonedRevision = 0;
let multiRegionRevision = 0;
let successfulMutationCount = 0;

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
              value: typeof record.value === "string" ? encoder.encode(record.value) : record.value,
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

const makeProtobufSource = (
  sourceTopic: string,
  regions: readonly [string, ...ReadonlyArray<string>],
) =>
  kafka.source({
    topic: sourceTopic,
    regions,
    key: kafka.string(),
    value: kafka.protobuf(OrderValueSchema),
    localRowKey: ({ key }) => key,
    map: ({ value, region }) => ({
      value: value.price,
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
          protobuf: {
            schema: Row,
            source: makeProtobufSource("protobuf-source", ["single"]),
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
      const runtime = yield* Effect.acquireRelease(
        makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer)),
        (runtime) => runtime.close,
      );
      yield* Effect.all(
        [
          single.awaitBindings(["accepted", "protobuf", "poisoned"]),
          region1.awaitBindings(["multiRegion"]),
          region2.awaitBindings(["multiRegion"]),
          region3.awaitBindings(["multiRegion"]),
          region4.awaitBindings(["multiRegion"]),
        ],
        { concurrency: "unbounded", discard: true },
      );
      state = {
        regions,
        retainedRowIds: Effect.gen(function* () {
          const accepted = yield* runtime.client.snapshot("accepted", {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc" }],
          });
          const protobuf = yield* runtime.client.snapshot("protobuf", {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc" }],
          });
          const poisoned = yield* runtime.client.snapshot("poisoned", {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc" }],
          });
          const multiRegion = yield* runtime.client.snapshot("multiRegion", {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc" }],
          });
          return {
            accepted: accepted.rows.map((row) => row.id),
            protobuf: protobuf.rows.map((row) => row.id),
            poisoned: poisoned.rows.map((row) => row.id),
            multiRegion: multiRegion.rows.map((row) => row.id),
          };
        }),
      };
      memoryAfterSetup = memorySnapshot();
    }).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    ),
  );
});

afterAll(async () => {
  const scope = benchmarkScope;
  const retainedRowIds = await Effect.runPromise(
    Effect.sync(requireState).pipe(
      Effect.flatMap((current) => current.retainedRowIds),
      Effect.ensuring(scope === undefined ? Effect.void : Scope.close(scope, Exit.void)),
    ),
  );
  benchmarkScope = undefined;
  const expectedRetainedRowIds = {
    accepted: Array.from({ length: laneBatchSize }, (_, index) => `single:accepted-${index}`).sort(
      (left, right) => left.localeCompare(right),
    ),
    protobuf: Array.from({ length: laneBatchSize }, (_, index) => `single:protobuf-${index}`).sort(
      (left, right) => left.localeCompare(right),
    ),
    poisoned: Array.from(
      { length: laneBatchSize - 1 },
      (_, index) => `single:poisoned-${index}`,
    ).sort((left, right) => left.localeCompare(right)),
    multiRegion: Array.from({ length: multiRegionCount }, (_, regionIndex) =>
      Array.from(
        { length: multiRegionBatchSize },
        (_, rowIndex) => `region-${regionIndex + 1}:region-${regionIndex + 1}-${rowIndex}`,
      ),
    )
      .flat()
      .sort((left, right) => left.localeCompare(right)),
  };
  if (JSON.stringify(retainedRowIds) !== JSON.stringify(expectedRetainedRowIds)) {
    throw new Error("Kafka Source Lane benchmark retained rows diverged from its fixed state.");
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
    mutationCount: successfulMutationCount,
    notes: [
      "Source Lane cases exercise the production materialized Kafka Source Adapter processor.",
      "The canonical profile configures 2,000-row JSON/protobuf batches and a mixed 2,000-record burst.",
      "Sustained ingestion applies eight consecutive mixed-codec waves per measured sample.",
      "Every Lane batch spans the configured synthetic partitions so partition-sensitive hot paths stay visible.",
      "The metrics case exercises assignment, commit, lag, and sampling bookkeeping across the configured partitions.",
    ],
    queuedEventCount: 0,
    retainedRowsByTopic: {
      accepted: retainedRowIds.accepted.length,
      protobuf: retainedRowIds.protobuf.length,
      poisoned: retainedRowIds.poisoned.length,
      multiRegion: retainedRowIds.multiRegion.length,
    },
    rowCount: retainedRowIds.accepted.length,
    subscriberCount: 0,
    topics: ["accepted", "protobuf", "poisoned", "multiRegion"],
  });
});

const validBatch = (
  label: string,
  revision: number,
  count: number,
): ReadonlyArray<BenchmarkRecord> =>
  Array.from({ length: count }, (_, index) => ({
    key: `${label}-${index}`,
    value: JSON.stringify({ value: revision * laneBatchSize + index }),
  }));

const protobufBatch = (
  label: string,
  revision: number,
  count: number,
): ReadonlyArray<BenchmarkRecord> =>
  Array.from({ length: count }, (_, index) => ({
    key: `${label}-${index}`,
    value: toBinary(
      OrderValueSchema,
      create(OrderValueSchema, {
        customerId: label,
        price: revision * laneBatchSize + index,
      }),
    ),
  }));

describe("Kafka Source Adapter lanes", () => {
  bench(
    benchmarkCaseNames[0],
    async () => {
      const current = requireState();
      acceptedRevision += 1;
      await Effect.runPromise(
        requireRegion(current, "single").offerBatch(
          "accepted",
          "accepted-source",
          validBatch("accepted", acceptedRevision, laneBatchSize),
        ),
      );
      successfulMutationCount += laneBatchSize;
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[1],
    async () => {
      const current = requireState();
      protobufRevision += 1;
      await Effect.runPromise(
        requireRegion(current, "single").offerBatch(
          "protobuf",
          "protobuf-source",
          protobufBatch("protobuf", protobufRevision, laneBatchSize),
        ),
      );
      successfulMutationCount += laneBatchSize;
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[2],
    async () => {
      const current = requireState();
      mixedRevision += 1;
      const half = laneBatchSize / 2;
      await Effect.runPromise(
        Effect.all(
          [
            requireRegion(current, "single").offerBatch(
              "accepted",
              "accepted-source",
              validBatch("accepted", mixedRevision, half),
            ),
            requireRegion(current, "single").offerBatch(
              "protobuf",
              "protobuf-source",
              protobufBatch("protobuf", mixedRevision, laneBatchSize - half),
            ),
          ],
          { concurrency: "unbounded", discard: true },
        ),
      );
      successfulMutationCount += laneBatchSize;
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[3],
    async () => {
      const current = requireState();
      for (let wave = 0; wave < 8; wave += 1) {
        sustainedRevision += 1;
        const half = laneBatchSize / 2;
        await Effect.runPromise(
          Effect.all(
            [
              requireRegion(current, "single").offerBatch(
                "accepted",
                "accepted-source",
                validBatch("accepted", sustainedRevision, half),
              ),
              requireRegion(current, "single").offerBatch(
                "protobuf",
                "protobuf-source",
                protobufBatch("protobuf", sustainedRevision, laneBatchSize - half),
              ),
            ],
            { concurrency: "unbounded", discard: true },
          ),
        );
      }
      successfulMutationCount += laneBatchSize * 8;
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[4],
    async () => {
      const current = requireState();
      poisonedRevision += 1;
      const valid = validBatch("poisoned", poisonedRevision, laneBatchSize - 1);
      await Effect.runPromise(
        requireRegion(current, "single").offerBatch("poisoned", "poisoned-source", [
          {
            key: "poisoned-invalid",
            value: "{not-json",
          },
          ...valid,
        ]),
      );
      successfulMutationCount += laneBatchSize - 1;
    },
    benchmarkOptions,
  );

  bench(
    benchmarkCaseNames[5],
    async () => {
      const current = requireState();
      multiRegionRevision += 1;
      await Effect.runPromise(
        Effect.forEach(
          ["region-1", "region-2", "region-3", "region-4"],
          (region) =>
            requireRegion(current, region).offerBatch(
              "multiRegion",
              "multi-region-source",
              validBatch(region, multiRegionRevision, multiRegionBatchSize),
            ),
          { concurrency: "unbounded", discard: true },
        ),
      );
      successfulMutationCount += laneBatchSize;
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
    benchmarkCaseNames[6],
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
