// Import Vitest directly so @effect/vitest's eager test-runtime module graph does not
// distort the heap, JIT, and GC behavior this benchmark is measuring.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Effect } from "effect";
import {
  runGrpcMaterializedBenchmarkSample,
  summarizeGrpcMaterializedBenchmarkSamples,
  type GrpcMaterializedBenchmarkSample,
  type GrpcMaterializedBenchmarkWorkload,
} from "../test-harness/grpc-materialized-benchmark";
import {
  grpcBenchmarkExplicitGcFromEnv,
  makeGrpcBenchmarkMemoryLifecycle,
} from "../test-harness/grpc-benchmark-memory";

declare const gc: (() => void) | undefined;

declare const process: {
  readonly env: Record<string, string | undefined>;
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
};

type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

type BenchmarkCase = {
  readonly name: string;
  readonly workload: GrpcMaterializedBenchmarkWorkload;
};

const defaultBatchSize = 256;
const defaultBenchmarkTimeMs = 0;
const defaultIterations = 5;
const defaultSeedRows = 1_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const convergenceTimeout = "10 seconds";

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed > 0) {
    return parsed;
  }
  throw new Error(`${name} must be a positive integer.`);
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(trimmed)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed >= 0) {
    return parsed;
  }
  throw new Error(`${name} must be a non-negative integer.`);
};

const batchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_BATCH_SIZE",
  defaultBatchSize,
);
const seedRows = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_SEED_ROWS",
  defaultSeedRows,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `grpc-materialized-${seedRows}seed-${batchSize}batch.json`,
);
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};
if (benchOptions.time > 0 || benchOptions.warmupIterations > 0 || benchOptions.warmupTime > 0) {
  throw new Error(
    "gRPC materialized benchmark requires fixed independent samples; time and warmup must stay disabled.",
  );
}

const samples: Array<GrpcMaterializedBenchmarkSample> = [];
const memoryLifecycle = makeGrpcBenchmarkMemoryLifecycle({
  capture: memorySnapshot,
  collectGarbage: typeof gc === "function" ? gc : undefined,
  explicitGc: grpcBenchmarkExplicitGcFromEnv(process.env["VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC"]),
  settle: () => new Promise<void>((resolve) => setImmediate(resolve)),
});

function memorySnapshot(): BenchmarkMemorySnapshot {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
}

function memoryDelta(
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemorySnapshot {
  return {
    arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
    externalBytes: after.externalBytes - before.externalBytes,
    heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
    heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
    rssBytes: after.rssBytes - before.rssBytes,
  };
}

function benchmarkOutputJsonPath(fallbackName: string): string {
  const configured = process.env["VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON"];
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return join(".artifacts", fallbackName);
}

function benchmarkSummaryPath(path: string): string {
  if (path.endsWith(".json")) {
    return `${path.slice(0, -".json".length)}.summary.json`;
  }
  return `${path}.summary.json`;
}

function writeJsonFile(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
}

const runBenchmarkSample = async (workload: GrpcMaterializedBenchmarkWorkload): Promise<void> => {
  samples.push(
    await Effect.runPromise(
      runGrpcMaterializedBenchmarkSample(workload, {
        batchSize,
        convergenceTimeout,
        seedRows,
      }),
    ),
  );
};

const benchmarkCases: ReadonlyArray<BenchmarkCase> = [
  {
    name: "gRPC materialized stream batch",
    workload: "stream-batch",
  },
  {
    name: "gRPC materialized burst",
    workload: "burst",
  },
  {
    name: "gRPC materialized health overlay",
    workload: "health-overlay",
  },
];

beforeAll(() => memoryLifecycle.captureBefore());

afterAll(async () => {
  const cases = benchmarkCases.map((benchmarkCase) =>
    summarizeGrpcMaterializedBenchmarkSamples(samples, benchmarkCase.name, benchOptions.iterations),
  );
  const reportedSamples = cases.flatMap((benchmarkCase) => benchmarkCase.samples);
  const health = samples[samples.length - 1]?.health;
  if (health === undefined) {
    throw new Error("gRPC materialized benchmark did not record final health evidence.");
  }
  const cleanupLeakCount = reportedSamples.reduce(
    (total, sample) => total + sample.cleanupLeakCount,
    0,
  );
  const backpressureCount = reportedSamples.reduce(
    (total, sample) => total + sample.backpressureCount,
    0,
  );
  const queuedEventCount = reportedSamples.reduce(
    (total, sample) => total + sample.queuedEventCount,
    0,
  );
  const memory = await memoryLifecycle.captureAfterCleanup();
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    batchSize,
    benchmarkCases: benchmarkCases.map((benchmarkCase) => benchmarkCase.name),
    benchmarkName: "gRPC materialized runtime benchmark",
    benchmarkScope: "runtime-grpc-materialized",
    cases,
    cleanupLeakCount,
    health,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memory.afterCleanup,
      before: memory.before,
      totalDelta: memoryDelta(memory.before, memory.afterCleanup),
    },
    grpcParameters: {
      batchSize,
      seedRows,
    },
    mutationCount: cases.reduce((total, benchmarkCase) => total + benchmarkCase.mutationCount, 0),
    notes: [
      "Vitest latency includes fresh Runtime Core, source, deterministic seed, measured operation, health audit, and cleanup for each sample.",
      "Operation timers isolate the production materialized gRPC ingress, convergence, health-overlay, and snapshot work.",
      "Every sample owns an independently seeded runtime and emits raw state, timing, and cleanup evidence.",
      "Endpoint RSS is captured after a settled explicit-GC checkpoint so closed sample runtimes do not contaminate retained-memory evidence.",
    ],
    queuedEventCount,
    rowCount: seedRows,
    seedMutationCount: cases.reduce(
      (total, benchmarkCase) => total + benchmarkCase.seedMutationCount,
      0,
    ),
    seedRows,
    subscriberCount: 0,
    topics: ["orders"],
  });
  if (cleanupLeakCount > 0) {
    throw new Error(
      `gRPC materialized benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`,
    );
  }
});

describe("runtime gRPC materialized benchmark", () => {
  for (const benchmarkCase of benchmarkCases) {
    bench(
      benchmarkCase.name,
      async () => {
        await runBenchmarkSample(benchmarkCase.workload);
      },
      benchOptions,
    );
  }
});
