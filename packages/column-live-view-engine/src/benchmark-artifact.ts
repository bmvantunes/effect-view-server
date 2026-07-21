import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  memoryDelta,
  type BenchmarkArtifactMemoryInput,
  type BenchmarkMemorySnapshot,
  type BenchmarkProcessPeakRss,
} from "./benchmark-memory-recorder";

declare const process: {
  readonly cwd: () => string;
  readonly env: Record<string, string | undefined>;
};

export type BenchmarkEngineHealth = {
  readonly activeSubscriptions: number;
  readonly backpressureEvents: number;
  readonly maxQueueDepth: number;
  readonly queuedEvents: number;
  readonly topics?: Readonly<Record<string, BenchmarkTopicHealth>>;
};

export type BenchmarkTopicHealth = {
  readonly activeFallbackGroupedViews?: number;
  readonly activeIncrementalGroupedViews?: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount?: number;
  readonly groupedPatchedEvaluationCount?: number;
  readonly pendingMutationBatches: number;
};

export type BenchmarkGroupedWriteAdmission = {
  readonly activeFallbackGroupedViewsAfterSetup: number;
  readonly activeFallbackGroupedViewsBeforeCleanup: number;
  readonly activeIncrementalGroupedViewsAfterSetup: number;
  readonly activeIncrementalGroupedViewsBeforeCleanup: number;
  readonly activeViewsAfterSetup: number;
  readonly activeViewsBeforeCleanup: number;
  readonly configuredMode: "fallback" | "incremental";
  readonly expectedAdmission: "fallback" | "incremental";
  readonly groupedFullEvaluationCountAfterSetup?: number;
  readonly groupedFullEvaluationCountBeforeCleanup?: number;
  readonly groupedPatchedEvaluationCountAfterSetup?: number;
  readonly groupedPatchedEvaluationCountBeforeCleanup?: number;
  readonly incrementalAdmissionLimits: {
    readonly maxGroups: number;
    readonly maxMembers: number;
    readonly maxMembersPerGroup: number;
    readonly maxRetainedValueEntries: number;
  };
  readonly priceThreshold: number | null;
  readonly readerProfile?: "aggregate-ordered" | "dual" | "order-neutral";
  readonly seedMutationCount: number;
  readonly timedMutationCount: number;
  readonly writeBatchSize: number;
};

export type BenchmarkGroupedKeyWidthParameters = {
  readonly constantGroupCount: number;
  readonly keyWidths: ReadonlyArray<number>;
  readonly orderedKeyCount: number;
  readonly semanticProbe: {
    readonly groupByEightTotalRows: number;
    readonly groupByEightOrderedTotalRows: number;
    readonly groupByFourTotalRows: number;
    readonly groupByOneTotalRows: number;
    readonly groupByTwoTotalRows: number;
    readonly orderedFirstGroupKey8: string;
    readonly orderedFirstRowCount: string;
    readonly orderedSecondGroupKey8: string;
    readonly orderedSecondRowCount: string;
    readonly orderedWindowRows: number;
  };
  readonly windowLimit: number;
};

type BenchmarkExplicitGcMeasurementProtocol = {
  readonly memoryCheckpoint: "settled-explicit-gc-after-cleanup";
  readonly postGcEventLoopTurns?: never;
  readonly priming?: "append-delete-restore-before-sampling";
};

type BenchmarkPostGcEventLoopMeasurementProtocol = {
  readonly memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup";
  readonly postGcEventLoopTurns: 8;
  readonly priming?: "append-delete-restore-before-sampling";
};

type BenchmarkPrimingMeasurementProtocol = {
  readonly memoryCheckpoint?: never;
  readonly postGcEventLoopTurns?: never;
  readonly priming: "append-delete-restore-before-sampling";
};

export type BenchmarkMeasurementProtocol =
  | BenchmarkExplicitGcMeasurementProtocol
  | BenchmarkPostGcEventLoopMeasurementProtocol
  | BenchmarkPrimingMeasurementProtocol;

type BenchmarkPostGcEventLoopSample = {
  readonly cleanupLedger: {
    readonly activeSubscriptions: number;
    readonly activeViews: number;
    readonly pendingMutationBatches: number;
    readonly queuedEvents: number;
  };
  readonly eventLoopTurn: number;
  readonly memory: BenchmarkMemorySnapshot;
};

export type BenchmarkArtifactMeasurementInput =
  | {
      readonly measurementProtocol: BenchmarkPostGcEventLoopMeasurementProtocol;
      readonly postGcEventLoopSamples: ReadonlyArray<BenchmarkPostGcEventLoopSample>;
    }
  | {
      readonly measurementProtocol?:
        | BenchmarkExplicitGcMeasurementProtocol
        | BenchmarkPrimingMeasurementProtocol;
      readonly postGcEventLoopSamples?: never;
    };

export type BenchmarkRawLargeMembershipParameters = {
  readonly candidateCount: number;
  readonly partitionCount: number;
  readonly preparedPlanCompilationCount: number;
  readonly subscriberCount: number;
};

type BenchmarkScope =
  | "engine-raw-snapshot"
  | "engine-raw-predicate-index"
  | "engine-raw-large-membership"
  | "engine-raw-live-fanout"
  | "engine-query-delta-operations"
  | "engine-raw-active-retained-delta"
  | "engine-raw-write"
  | "engine-grouped-aggregate"
  | "engine-grouped-key-width"
  | "engine-grouped-write";

type BenchmarkScopeWithoutRawLargeMembership = Exclude<
  BenchmarkScope,
  "engine-raw-large-membership"
>;

type BenchmarkScopeFields =
  | {
      readonly benchmarkScope: "engine-raw-large-membership";
      readonly rawLargeMembershipParameters: BenchmarkRawLargeMembershipParameters;
    }
  | {
      readonly [Scope in BenchmarkScopeWithoutRawLargeMembership]: {
        readonly benchmarkScope: Scope;
        readonly rawLargeMembershipParameters?: never;
      };
    }[BenchmarkScopeWithoutRawLargeMembership];

type BenchmarkArtifactFields = {
  readonly activeViewCountBeforeCleanup?: number;
  readonly artifactKind: "engine-benchmark-summary";
  readonly benchmarkName: string;
  readonly rowCount: number;
  readonly mutationCount: number;
  readonly subscriberCount: number;
  readonly topics: ReadonlyArray<string>;
  readonly benchmarkCases: ReadonlyArray<string>;
  readonly outputJsonPath: string;
  readonly latency: {
    readonly source: "vitest-output-json";
    readonly outputJsonPath: string;
  };
  readonly backpressureCount: number;
  readonly cleanupLeakCount: number;
  readonly groupedKeyWidthParameters?: BenchmarkGroupedKeyWidthParameters;
  readonly groupedWriteAdmission?: BenchmarkGroupedWriteAdmission;
  readonly queuedEventCount: number;
  readonly health: unknown;
  readonly notes: ReadonlyArray<string>;
  readonly preCleanupHealth?: unknown;
} & BenchmarkScopeFields;

export type BenchmarkArtifactInput = BenchmarkArtifactFields &
  BenchmarkArtifactMemoryInput &
  BenchmarkArtifactMeasurementInput;

const processPeakRssSummary = (initialCurrentRssBytes: number, peak: BenchmarkProcessPeakRss) => {
  if (
    peak.beforeBytes < initialCurrentRssBytes ||
    peak.afterSetupBytes < peak.beforeBytes ||
    peak.afterBenchmarkBytes < peak.afterSetupBytes
  ) {
    throw new Error("Process peak RSS checkpoints must be monotonic.");
  }
  return {
    ...peak,
    benchmarkDeltaBytes: peak.afterBenchmarkBytes - peak.afterSetupBytes,
    setupDeltaBytes: peak.afterSetupBytes - initialCurrentRssBytes,
    totalDeltaBytes: peak.afterBenchmarkBytes - initialCurrentRssBytes,
  };
};

export const benchmarkOutputJsonPath = (fallbackName: string): string => {
  const configured = process.env["VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON"];
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return join(".artifacts", fallbackName);
};

export const benchmarkSummaryPath = (outputJsonPath: string): string => {
  if (outputJsonPath.endsWith(".json")) {
    return `${outputJsonPath.slice(0, -".json".length)}.summary.json`;
  }
  return `${outputJsonPath}.summary.json`;
};

export const cleanupLeakCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.activeSubscriptions + health.queuedEvents + activeViewCountFromEngineHealth(health);
};

export const backpressureCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.backpressureEvents;
};

export const queuedEventCountFromEngineHealth = (health: unknown): number => {
  if (!isBenchmarkEngineHealth(health)) {
    return 0;
  }
  return health.queuedEvents;
};

export const failOnBenchmarkCleanupLeaks = (cleanupLeakCount: number): void => {
  if (cleanupLeakCount > 0) {
    throw new Error(`Benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`);
  }
};

export const isBenchmarkEngineHealth = (value: unknown): value is BenchmarkEngineHealth => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (
    !("activeSubscriptions" in value) ||
    !("backpressureEvents" in value) ||
    !("maxQueueDepth" in value) ||
    !("queuedEvents" in value)
  ) {
    return false;
  }
  const hasEngineCounters =
    isFiniteNumber(value.activeSubscriptions) &&
    isFiniteNumber(value.backpressureEvents) &&
    isFiniteNumber(value.maxQueueDepth) &&
    isFiniteNumber(value.queuedEvents);

  if (!hasEngineCounters) {
    return false;
  }

  if (!("topics" in value)) {
    return true;
  }

  const topics = value.topics;
  if (topics === undefined) {
    return true;
  }
  if (typeof topics !== "object" || topics === null) {
    return false;
  }

  for (const topic of Object.values(topics)) {
    if (!isBenchmarkTopicHealth(topic)) {
      return false;
    }
  }

  return true;
};

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isNonNegativeInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isInteger(value) && value >= 0;

const isOptionalFiniteNumber = (value: unknown): value is number | undefined =>
  value === undefined || isFiniteNumber(value);

const isBenchmarkTopicHealth = (value: unknown): value is BenchmarkTopicHealth => {
  if (
    typeof value !== "object" ||
    value === null ||
    !("activeViews" in value) ||
    !("pendingMutationBatches" in value)
  ) {
    return false;
  }
  const activeFallbackGroupedViews =
    "activeFallbackGroupedViews" in value ? value.activeFallbackGroupedViews : undefined;
  const activeIncrementalGroupedViews =
    "activeIncrementalGroupedViews" in value ? value.activeIncrementalGroupedViews : undefined;
  const groupedFullEvaluationCount =
    "groupedFullEvaluationCount" in value ? value.groupedFullEvaluationCount : undefined;
  const groupedPatchedEvaluationCount =
    "groupedPatchedEvaluationCount" in value ? value.groupedPatchedEvaluationCount : undefined;
  return (
    isFiniteNumber(value.activeViews) &&
    isOptionalFiniteNumber(activeFallbackGroupedViews) &&
    isOptionalFiniteNumber(activeIncrementalGroupedViews) &&
    isOptionalFiniteNumber(groupedFullEvaluationCount) &&
    isOptionalFiniteNumber(groupedPatchedEvaluationCount) &&
    isNonNegativeInteger(value.pendingMutationBatches)
  );
};

export const activeViewCountFromEngineHealth = (health: BenchmarkEngineHealth): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeViews;
  }
  return activeViewCount;
};

export const activeFallbackGroupedViewCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeFallbackGroupedViews ?? 0;
  }
  return activeViewCount;
};

export const activeIncrementalGroupedViewCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let activeViewCount = 0;
  for (const topic of Object.values(health.topics)) {
    activeViewCount += topic.activeIncrementalGroupedViews ?? 0;
  }
  return activeViewCount;
};

export const groupedFullEvaluationCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let groupedFullEvaluationCount = 0;
  for (const topic of Object.values(health.topics)) {
    groupedFullEvaluationCount += topic.groupedFullEvaluationCount ?? 0;
  }
  return groupedFullEvaluationCount;
};

export const groupedPatchedEvaluationCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
): number => {
  if (health.topics === undefined) {
    return 0;
  }
  let groupedPatchedEvaluationCount = 0;
  for (const topic of Object.values(health.topics)) {
    groupedPatchedEvaluationCount += topic.groupedPatchedEvaluationCount ?? 0;
  }
  return groupedPatchedEvaluationCount;
};

export const pendingMutationBatchCountFromEngineHealth = (
  health: BenchmarkEngineHealth,
  expectedTopics: ReadonlyArray<string>,
): number => {
  if (health.topics === undefined) {
    throw new Error(
      "Benchmark engine health must include topic health to prove pending mutation batches are zero.",
    );
  }
  if (expectedTopics.length === 0) {
    throw new Error("Benchmark pending-mutation proof must name at least one expected topic.");
  }
  for (const expectedTopic of expectedTopics) {
    if (!Object.hasOwn(health.topics, expectedTopic)) {
      throw new Error(
        `Benchmark engine health must include expected topic ${expectedTopic} to prove pending mutation batches are zero.`,
      );
    }
  }
  let pendingMutationBatchCount = 0;
  for (const topic of Object.values(health.topics)) {
    pendingMutationBatchCount += topic.pendingMutationBatches;
  }
  return pendingMutationBatchCount;
};

export const writeBenchmarkArtifact = (input: BenchmarkArtifactInput): void => {
  const summaryPath = benchmarkSummaryPath(input.outputJsonPath);
  const processPeakRss =
    input.processPeakRss === undefined
      ? undefined
      : processPeakRssSummary(input.memoryBefore.rssBytes, input.processPeakRss);
  mkdirSync(dirname(summaryPath), { recursive: true });
  writeFileSync(
    summaryPath,
    `${JSON.stringify(
      {
        artifactKind: input.artifactKind,
        activeViewCountBeforeCleanup: input.activeViewCountBeforeCleanup,
        backpressureCount: input.backpressureCount,
        benchmarkCases: input.benchmarkCases,
        benchmarkName: input.benchmarkName,
        benchmarkScope: input.benchmarkScope,
        cleanupLeakCount: input.cleanupLeakCount,
        groupedKeyWidthParameters: input.groupedKeyWidthParameters,
        groupedWriteAdmission: input.groupedWriteAdmission,
        health: input.health,
        latency: input.latency,
        measurementProtocol: input.measurementProtocol,
        memory: {
          afterBenchmark: input.memoryAfterBenchmark,
          afterSetup: input.memoryAfterSetup,
          before: input.memoryBefore,
          benchmarkDelta: memoryDelta(input.memoryAfterSetup, input.memoryAfterBenchmark),
          postGcEventLoopSamples: input.postGcEventLoopSamples,
          ...(processPeakRss === undefined ? {} : { processPeakRss }),
          setupDelta: memoryDelta(input.memoryBefore, input.memoryAfterSetup),
          totalDelta: memoryDelta(input.memoryBefore, input.memoryAfterBenchmark),
        },
        mutationCount: input.mutationCount,
        notes: input.notes,
        outputJsonPath: input.outputJsonPath,
        preCleanupHealth: input.preCleanupHealth,
        queuedEventCount: input.queuedEventCount,
        rawLargeMembershipParameters: input.rawLargeMembershipParameters,
        rowCount: input.rowCount,
        samplingPolicy: input.samplingPolicy,
        subscriberCount: input.subscriberCount,
        topics: input.topics,
      },
      undefined,
      2,
    )}\n`,
  );
};
