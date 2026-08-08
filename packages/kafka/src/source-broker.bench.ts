// Import Vitest directly so the Effect test-runtime graph does not distort
// the broker-backed Kafka Source Adapter measurement.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { Admin, Producer } from "@platformatic/kafka";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Buffer } from "node:buffer";
import { mkdirSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { Effect, Exit, Schema, Scope } from "effect";
import { kafka } from "./contract";
import { kafkaNode } from "./node";

const benchmarkInteger = (name: string, fallback: number): number => {
  const configured = process.env[name];
  if (configured === undefined) {
    return fallback;
  }
  const parsed = Number(configured);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive safe integer.`);
  }
  return parsed;
};

const benchmarkOptions = {
  iterations: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_ITERATIONS", 5),
  time: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_TIME_MS", 1),
  warmupIterations: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_WARMUP_ITERATIONS", 1),
  warmupTime: benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_WARMUP_TIME_MS", 1),
};
const rowCount = benchmarkInteger("VIEW_SERVER_KAFKA_SOURCE_BROKER_BENCH_ROWS", 64);
const bootstrapServers = process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";
const outputJsonPath =
  process.env["VIEW_SERVER_KAFKA_SOURCE_BROKER_OUTPUT_JSON"]?.trim() ||
  join(".artifacts", `source-broker-${rowCount}rows.json`);
const benchmarkCaseNames = [
  `production Node Adapter broker ingest and commit (${rowCount} rows)`,
] as const;
const encoder = new TextEncoder();

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

type BenchmarkState = {
  readonly ingest: (batch: number) => Effect.Effect<void, unknown>;
  readonly verifyFinalState: Effect.Effect<number, unknown>;
};

let state: BenchmarkState | undefined;
let benchmarkScope: Scope.Closeable | undefined;
let nextBatch = 0;
let retainedRowsAfterBenchmark = 0;

const requireState = (): BenchmarkState => {
  if (state === undefined) {
    throw new Error("Kafka Source broker benchmark setup did not complete.");
  }
  return state;
};

const Row = Schema.Struct({
  id: ViewServerId,
  value: Schema.Number,
});
const WireRow = Schema.Struct({
  value: Schema.Number,
});

class KafkaSourceBrokerBenchmarkError extends Schema.TaggedError<KafkaSourceBrokerBenchmarkError>()(
  "KafkaSourceBrokerBenchmarkError",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Unknown),
  },
) {}

beforeAll(async () => {
  const scope = Effect.runSync(Scope.make("sequential"));
  benchmarkScope = scope;
  await Effect.runPromise(
    Effect.gen(function* () {
      const suffix = randomUUID().replaceAll("-", "");
      const sourceTopic = `view-server-source-bench-${suffix}`;
      const groupPrefix = `view-server-source-bench-${suffix}`;
      const groupId = `${groupPrefix}:rows`;
      const admin = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Admin({
              bootstrapBrokers: [bootstrapServers],
              clientId: `source-bench-admin-${suffix}`,
            }),
        ),
        (current) =>
          Effect.tryPromise({
            try: () => current.close(),
            catch: (cause) =>
              new KafkaSourceBrokerBenchmarkError({
                message: "Kafka Source benchmark Admin close failed.",
                cause,
              }),
          }).pipe(Effect.orDie),
      );
      const producer = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Producer<Buffer | null, Buffer | null, Buffer, Buffer>({
              bootstrapBrokers: [bootstrapServers],
              clientId: `source-bench-producer-${suffix}`,
            }),
        ),
        (current) =>
          Effect.tryPromise({
            try: () => current.close(),
            catch: (cause) =>
              new KafkaSourceBrokerBenchmarkError({
                message: "Kafka Source benchmark Producer close failed.",
                cause,
              }),
          }).pipe(Effect.orDie),
      );
      yield* Effect.tryPromise({
        try: () =>
          admin.createTopics({
            partitions: 1,
            replicas: 1,
            topics: [sourceTopic],
          }),
        catch: (cause) =>
          new KafkaSourceBrokerBenchmarkError({
            message: "Kafka Source benchmark Topic creation failed.",
            cause,
          }),
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: kafka.source({
              cleanupPolicy: "delete",
              retentionPolicy: "Infinity",
              topic: sourceTopic,
              regions: ["local"],
              key: kafka.string(),
              value: kafka.json(() => Schema.toCodecJson(WireRow)),
              localRowKey: ({ key }) => key,
              map: ({ value }) => ({
                value: value.value,
              }),
              startFrom: "earliest",
            }),
          },
        },
      });
      const runtime = yield* Effect.acquireRelease(
        makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(
            kafkaNode.layer(config, {
              consumerGroupPrefix: groupPrefix,
              regions: {
                local: {
                  bootstrapServers,
                },
              },
            }),
          ),
        ),
        (current) => current.close,
      );
      for (let poll = 0; poll < 2_000; poll += 1) {
        const snapshot = yield* runtime.client.snapshot("rows", {
          select: ["id"],
        });
        if (snapshot.status === "ready") {
          break;
        }
        if (poll === 1_999) {
          return yield* new KafkaSourceBrokerBenchmarkError({
            message: "Kafka Source broker benchmark did not become Ready.",
          });
        }
        yield* Effect.sleep("5 millis");
      }
      let expectedCommittedOffset = 0n;
      let lastCompletedBatch: number | undefined;
      state = {
        ingest: (batch) =>
          Effect.gen(function* () {
            yield* Effect.tryPromise({
              try: () =>
                producer.send({
                  messages: Array.from({ length: rowCount }, (_, index) => ({
                    topic: sourceTopic,
                    key: Buffer.from(encoder.encode(`row-${index}`)),
                    value: Buffer.from(encoder.encode(JSON.stringify({ value: batch }))),
                  })),
                }),
              catch: (cause) =>
                new KafkaSourceBrokerBenchmarkError({
                  message: "Kafka Source benchmark Producer send failed.",
                  cause,
                }),
            });
            expectedCommittedOffset += BigInt(rowCount);
            for (let poll = 0; poll < 4_000; poll += 1) {
              const groups = yield* Effect.tryPromise({
                try: () =>
                  admin.listConsumerGroupOffsets({
                    groups: [
                      {
                        groupId,
                        topics: [
                          {
                            name: sourceTopic,
                            partitionIndexes: [0],
                          },
                        ],
                      },
                    ],
                    requireStable: false,
                  }),
                catch: (cause) =>
                  new KafkaSourceBrokerBenchmarkError({
                    message: "Kafka Source benchmark committed-offset read failed.",
                    cause,
                  }),
              });
              const committedOffset = groups[0]?.topics[0]?.partitions[0]?.committedOffset ?? -1n;
              if (committedOffset >= expectedCommittedOffset) {
                lastCompletedBatch = batch;
                return;
              }
              yield* Effect.sleep("5 millis");
            }
            return yield* new KafkaSourceBrokerBenchmarkError({
              message: "Kafka Source benchmark did not converge and commit its broker batch.",
            });
          }),
        verifyFinalState: Effect.gen(function* () {
          const completedBatch = lastCompletedBatch;
          if (completedBatch === undefined) {
            return yield* new KafkaSourceBrokerBenchmarkError({
              message: "Kafka Source benchmark did not complete a broker batch.",
            });
          }
          const snapshot = yield* runtime.client.snapshot("rows", {
            select: ["id", "value"],
          });
          if (
            snapshot.status !== "ready" ||
            snapshot.totalRows !== rowCount ||
            snapshot.rows.length !== rowCount
          ) {
            return yield* new KafkaSourceBrokerBenchmarkError({
              message: "Kafka Source benchmark retained state did not remain fixed-size.",
            });
          }
          const rowsById = new Map(snapshot.rows.map((row) => [row.id, row.value]));
          for (let index = 0; index < rowCount; index += 1) {
            if (rowsById.get(`local:0:row-${index}`) !== completedBatch) {
              return yield* new KafkaSourceBrokerBenchmarkError({
                message: "Kafka Source benchmark retained state did not converge.",
              });
            }
          }
          return snapshot.totalRows;
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
  if (benchmarkScope !== undefined) {
    retainedRowsAfterBenchmark = await Effect.runPromise(
      Effect.suspend(() => requireState().verifyFinalState).pipe(
        Effect.ensuring(Scope.close(benchmarkScope, Exit.void)),
      ),
    );
  }
  const memoryAfterBenchmark = memorySnapshot();
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount: 0,
    benchmarkCases: benchmarkCaseNames,
    benchmarkName: "Kafka Source Adapter production broker benchmark",
    benchmarkScope: "kafka-source-adapter-broker",
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
    mutationCount: rowCount,
    notes: [
      "Exercises the production Platformatic Kafka Node Adapter against a real Apache Kafka broker.",
      "Each sample overwrites a fixed key set and waits for the post-application consumer-group commit.",
      "A final untimed snapshot verifies exact fixed-size Runtime Core convergence.",
    ],
    queuedEventCount: 0,
    rowCount: retainedRowsAfterBenchmark,
    subscriberCount: 0,
    topics: ["rows"],
  });
});

describe("Kafka Source Adapter production broker", () => {
  bench(
    benchmarkCaseNames[0],
    async () => {
      const current = requireState();
      const batch = nextBatch;
      nextBatch += 1;
      await Effect.runPromise(current.ingest(batch));
    },
    benchmarkOptions,
  );
});
