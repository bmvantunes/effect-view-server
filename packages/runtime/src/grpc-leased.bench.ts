// Import Vitest directly so @effect/vitest's eager test-runtime module graph does not
// distort the heap, JIT, and GC behavior this benchmark is measuring.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { Effect } from "effect";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import {
  grpcLeasedBenchmarkCases,
  runGrpcLeasedBenchmarkSample,
  summarizeGrpcLeasedBenchmarkSamples,
  type GrpcLeasedBenchmarkSample,
  type GrpcLeasedBenchmarkWorkload,
} from "../test-harness/grpc-leased-benchmark";
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

const defaultBenchmarkTimeMs = 0;
const defaultIterations = 5;
const defaultRetainedRows = 50_000;
const defaultRowsPerFeed = 50;
const defaultRouteCount = 25;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const convergenceTimeout = "10 seconds";

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/u.test(normalized)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a safe positive integer.`);
  }
  return parsed;
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(normalized)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a safe non-negative integer.`);
  }
  return parsed;
};

const rowsPerFeed = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROWS_PER_FEED",
  defaultRowsPerFeed,
);
const routeCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROUTE_COUNT",
  defaultRouteCount,
);
const retainedRows = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_RETAINED_ROWS",
  defaultRetainedRows,
);
const outputJsonPath = benchmarkOutputJsonPath(
  `grpc-leased-${rowsPerFeed}rows-${routeCount}routes-${retainedRows}retained.json`,
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
    "gRPC leased benchmark requires fixed independent samples; time and warmup must stay disabled.",
  );
}

const samples: Array<GrpcLeasedBenchmarkSample> = [];
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
  return configured === undefined || configured.trim() === ""
    ? join(".artifacts", fallbackName)
    : configured.trim();
}

function benchmarkSummaryPath(path: string): string {
  return path.endsWith(".json")
    ? `${path.slice(0, -".json".length)}.summary.json`
    : `${path}.summary.json`;
}

function writeJsonFile(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
}

const benchmarkBody = (workload: GrpcLeasedBenchmarkWorkload) => async (): Promise<void> => {
  samples.push(
    await Effect.runPromise(
      runGrpcLeasedBenchmarkSample(workload, {
        convergenceTimeout,
        retainedRows,
        routeCount,
        rowsPerFeed,
      }),
    ),
  );
};

beforeAll(() => memoryLifecycle.captureBefore());

afterAll(async () => {
  const cases = grpcLeasedBenchmarkCases.map((benchmarkCase) =>
    summarizeGrpcLeasedBenchmarkSamples(samples, benchmarkCase.name, benchOptions.iterations),
  );
  const reportedSamples = cases.flatMap((benchmarkCase) => benchmarkCase.samples);
  const health = samples.at(-1)?.health;
  if (health === undefined) {
    throw new Error("gRPC leased benchmark did not record final health evidence.");
  }
  const cleanupLeakCount = reportedSamples.reduce(
    (total, sample) => total + sample.measuredCleanup.leakCount + sample.cleanupLeakCount,
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
    benchmarkCases: grpcLeasedBenchmarkCases.map((benchmarkCase) => benchmarkCase.name),
    benchmarkName: "gRPC leased runtime benchmark",
    benchmarkScope: "runtime-grpc-leased",
    cases,
    cleanupLeakCount,
    grpcParameters: {
      retainedRows,
      routeCount,
      rowsPerFeed,
    },
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
    mutationCount: cases.reduce((total, benchmarkCase) => total + benchmarkCase.mutationCount, 0),
    notes: [
      "Vitest latency includes a fresh Runtime Core, leased manager, deterministic sources, measured operation, health audit, and cleanup for each sample.",
      "Operation timers isolate subscription, convergence, health-overlay, delta-fanout, and last-subscriber cleanup work on the production leased gRPC path.",
      "Retained local-filter snapshot timing starts before subscription so its pooled throughput includes retained query evaluation and initial event delivery.",
      "Measured subscription-release cleanup is audited immediately after close and before the emergency whole-manager teardown.",
      "Every sample owns independent routes and emits separate raw measured-cleanup and emergency-teardown evidence, including the gRPC client active-feed ledger.",
      "Endpoint RSS is captured after a settled explicit-GC checkpoint so closed sample runtimes do not contaminate retained-memory evidence.",
    ],
    queuedEventCount,
    rowCount: rowsPerFeed,
    rowsPerFeed,
    retainedRows,
    routeCount,
    seedMutationCount: cases.reduce(
      (total, benchmarkCase) => total + benchmarkCase.seedMutationCount,
      0,
    ),
    subscriberCount: Math.max(50, routeCount),
    topics: ["orders"],
  });
  if (cleanupLeakCount > 0) {
    throw new Error(`gRPC leased benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`);
  }
});

describe("runtime gRPC leased benchmark", () => {
  for (const benchmarkCase of grpcLeasedBenchmarkCases) {
    bench(benchmarkCase.name, benchmarkBody(benchmarkCase.workload), benchOptions);
  }
});
