// Import Vitest directly so @effect/vitest's eager test-runtime module graph does not
// distort the heap, JIT, and GC behavior this benchmark is measuring.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { Effect, Schema } from "effect";
import {
  activeRawQueryExecutionCount,
  preparedRawQueryPlanCompilationCount,
} from "./active-raw-query";
import {
  benchmarkOutputJsonPath,
  failOnBenchmarkCleanupLeaks,
  writeBenchmarkArtifact,
} from "./benchmark-artifact";
import { makeBenchmarkMemoryRecorder } from "./benchmark-memory-recorder";
import type { CompiledRawPredicate } from "./raw-predicate-compiler";
import { prepareRuntimeRawQuery, rawQueryCompilerMetadata } from "./raw-query-compiler";
import {
  acquireTopicStoreRuntimeRawQueryExecution,
  releaseTopicStoreRawQueryExecution,
  TopicStore,
} from "./topic-store";
import { topicStoreQueryResources } from "./topic-store-state";

declare const process: {
  readonly env: Record<string, string | undefined>;
};

const Row = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  region: Schema.String,
});

type RowValue = typeof Row.Type;

const candidateCount = 50_000;
const partitionCount = 25;
const subscriberCount = 32;
const rowsPerPartition = 4_000;
const rowCount = partitionCount * rowsPerPartition;
const evaluationBenchmarkCase = "evaluate 50k candidates across 100k partitioned rows";
const compilationBenchmarkCase = "compile raw query with 50k membership candidates";
const sharedPlanBenchmarkCase = "acquire 32 equivalent subscribers with one 50k membership plan";
const outputJsonPath = benchmarkOutputJsonPath(
  "raw-large-membership-50000candidates-100000rows.json",
);
const benchmarkMemory = makeBenchmarkMemoryRecorder();

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

const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", 5),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", 0),
  warmupIterations: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS", 0),
  warmupTime: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS", 0),
};

const candidates = Array.from({ length: candidateCount }, (_value, index) => `customer-${index}`);
const metadata = rawQueryCompilerMetadata(Row);
const subscriptionStore = new TopicStore("rows", Row, "id", () => {});
const queryInput = {
  select: ["id"],
  where: [
    {
      type: "OR",
      conditions: [
        { field: "customerId", type: "in", filter: candidates },
        { field: "region", type: "equals", filter: "unmatched" },
      ],
    },
  ],
};
const partitions = Array.from({ length: partitionCount }, (_value, partition) =>
  Array.from({ length: rowsPerPartition }, (_entry, offset): RowValue => {
    const index = partition * rowsPerPartition + offset;
    return {
      id: `order-${index}`,
      customerId: `customer-${index}`,
      region: "emea",
    };
  }),
);

let compiled: CompiledRawPredicate<RowValue> | undefined;
let lastCompilationCallbackRequired: boolean | undefined;
let lastEvaluationMatches: number | undefined;
let lastPreparedPlanCompilationCount: number | undefined;
let lastSharedActivePlanCount: number | undefined;
let lastSharedCleanupLeakCount: number | undefined;

const prepareLargeMembershipQuery = () =>
  Effect.runSync(prepareRuntimeRawQuery("rows", metadata, queryInput));

const compiledPredicate = (): CompiledRawPredicate<RowValue> => {
  if (compiled === undefined) {
    throw new Error("Large membership benchmark is not initialized.");
  }
  return compiled;
};

const requiredBenchmarkOutcome = (value: number | undefined, label: string): number => {
  if (value === undefined) {
    throw new Error(`Large membership benchmark did not record ${label}.`);
  }
  return value;
};

beforeAll(() => {
  compiled = prepareLargeMembershipQuery().plan.predicate;
  expect(compiled.plan.callbackRequired).toBe(true);
  benchmarkMemory.captureAfterSetup();
});

afterAll(() => {
  const preparedPlanCompilationCount = requiredBenchmarkOutcome(
    lastPreparedPlanCompilationCount,
    "the prepared plan compilation count",
  );
  const sharedActivePlanCount = requiredBenchmarkOutcome(
    lastSharedActivePlanCount,
    "the shared active plan count",
  );
  const cleanupLeakCount = requiredBenchmarkOutcome(
    lastSharedCleanupLeakCount,
    "the shared Active Query cleanup leak count",
  );
  expect(lastEvaluationMatches).toBe(candidateCount);
  expect(lastCompilationCallbackRequired).toBe(true);
  expect(preparedPlanCompilationCount).toBe(1);
  expect(sharedActivePlanCount).toBe(1);
  compiled = undefined;
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: 0,
    benchmarkCases: [evaluationBenchmarkCase, compilationBenchmarkCase, sharedPlanBenchmarkCase],
    benchmarkName: "raw large membership compilation and evaluation benchmark",
    benchmarkScope: "engine-raw-large-membership",
    cleanupLeakCount,
    health: {
      candidateCount,
      cleanupActivePlanCount: cleanupLeakCount,
      partitionCount,
      preparedPlanCompilationCount,
      rowCount,
      status: "active-query-level",
      subscriberCount,
    },
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    ...benchmarkMemory.captureAfterBenchmark(undefined),
    mutationCount: 0,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "The evaluation case guards Set-backed nested in-filter callback scans across partitioned rows.",
      "The compilation case guards production query snapshotting, validation, normalization, and Raw Query Plan compilation.",
      "The sharing case guards one canonical predicate-plan compilation across equivalent Active Query leases.",
      "Every timed sharing iteration releases all Active Query leases before returning.",
    ],
    outputJsonPath,
    queuedEventCount: 0,
    rawLargeMembershipParameters: {
      candidateCount,
      partitionCount,
      preparedPlanCompilationCount,
      subscriberCount,
    },
    rowCount,
    subscriberCount,
    topics: ["rows"],
  });
  failOnBenchmarkCleanupLeaks(cleanupLeakCount);
  expect(cleanupLeakCount).toBe(0);
});

describe("raw nested membership callback benchmark (localhost CPU/GC partition stress)", () => {
  bench(
    evaluationBenchmarkCase,
    () => {
      const predicate = compiledPredicate();
      let matches = 0;
      for (const partition of partitions) {
        for (const row of partition) {
          if (predicate.matches(row)) {
            matches += 1;
          }
        }
      }
      lastEvaluationMatches = matches;
    },
    benchOptions,
  );

  bench(
    compilationBenchmarkCase,
    () => {
      lastCompilationCallbackRequired =
        prepareLargeMembershipQuery().plan.predicate.plan.callbackRequired;
    },
    benchOptions,
  );

  bench(
    sharedPlanBenchmarkCase,
    () => {
      const result = Effect.runSync(
        Effect.gen(function* () {
          const resources = topicStoreQueryResources(subscriptionStore);
          const compilationsBefore = yield* preparedRawQueryPlanCompilationCount(
            resources.activeQueries,
          );
          const acquiredSubscriptions = [];
          for (let subscriber = 0; subscriber < subscriberCount; subscriber += 1) {
            acquiredSubscriptions.push(
              yield* acquireTopicStoreRuntimeRawQueryExecution(subscriptionStore, queryInput),
            );
          }
          const compilationsAfter = yield* preparedRawQueryPlanCompilationCount(
            resources.activeQueries,
          );
          const activePlanCount = yield* activeRawQueryExecutionCount(resources.activeQueries);
          for (const acquired of acquiredSubscriptions) {
            yield* releaseTopicStoreRawQueryExecution(subscriptionStore, acquired.releaseToken);
          }
          const cleanupLeakCount = yield* activeRawQueryExecutionCount(resources.activeQueries);
          return {
            activePlanCount,
            cleanupLeakCount,
            preparedPlanCompilationCount: compilationsAfter - compilationsBefore,
          };
        }),
      );
      lastPreparedPlanCompilationCount = result.preparedPlanCompilationCount;
      lastSharedActivePlanCount = result.activePlanCount;
      lastSharedCleanupLeakCount = result.cleanupLeakCount;
    },
    benchOptions,
  );
});
