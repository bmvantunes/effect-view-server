import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import {
  arrayValue,
  exactObjectValue,
  finiteNumber,
  mapByUniqueKey,
  nonEmptyArrayValue,
  nonNegativeFiniteNumber,
  nonNegativeInteger,
  objectValue,
  positiveFiniteNumber,
  positiveInteger,
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";
import {
  benchmarkComparisonPolicyForProfile,
  benchmarkThresholdsForProfile,
  compareBenchmarkArtifacts,
} from "./benchmark-comparison-policy.mjs";
import {
  decodeGrpcMaterializedBenchmarkParameters,
  decodeGrpcMaterializedOperationCases,
  decodeGrpcMaterializedSeedMutationCount,
  validateGrpcMaterializedOperationAccounting,
} from "./grpc-materialized-benchmark-policy.mjs";
import {
  decodeGrpcLeasedBenchmarkParameters,
  decodeGrpcLeasedOperationCases,
  decodeGrpcLeasedSeedMutationCount,
  validateGrpcLeasedOperationAccounting,
} from "./grpc-leased-benchmark-policy.mjs";
import {
  decodeBenchmarkMemoryRssTotalDeltaBytes,
  decodeBenchmarkSamplingPolicy,
  validateBenchmarkSamplingPolicy,
  validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes,
} from "./benchmark-sampling-policy.mjs";

const readJsonFile = (path) => JSON.parse(readFileSync(path, "utf8"));

const writeJsonFile = (path, value) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
};

const optionalObjectValue = (value, path) =>
  value === undefined ? undefined : objectValue(value, path);

const optionalFiniteNumber = (value, path) =>
  value === undefined ? undefined : finiteNumber(value, path);

const stringArrayValue = (value, path) =>
  arrayValue(value, path).map((item, index) => stringValue(item, `${path}[${index}]`));

const benchmarkKey = (benchmark) => `${benchmark.groupName} / ${benchmark.name}`;

const throughputCaseByName = (throughputCases, path) =>
  mapByUniqueKey(throughputCases, (throughputCase) => throughputCase.name, path, "throughput case");

const baselineArtifactKind = (value, path) => {
  const artifactKind = stringValue(value, path);
  if (artifactKind !== "view-server-benchmark-baseline") {
    throw new Error(`Benchmark artifact field ${path} must be view-server-benchmark-baseline.`);
  }
  return artifactKind;
};

const summaryArtifactKind = (value, path) => {
  const artifactKind = stringValue(value, path);
  if (
    artifactKind !== "engine-benchmark-summary" &&
    artifactKind !== "react-browser-benchmark-summary" &&
    artifactKind !== "runtime-benchmark-summary"
  ) {
    throw new Error(
      `Benchmark artifact field ${path} must be engine-benchmark-summary, react-browser-benchmark-summary, or runtime-benchmark-summary.`,
    );
  }
  return artifactKind;
};

const nonNegativeSafeInteger = (value, path) => {
  const number = nonNegativeInteger(value, path);
  if (!Number.isSafeInteger(number)) {
    throw new Error(`Benchmark artifact field ${path} must be a safe non-negative integer.`);
  }
  return number;
};

const grpcBenchmarkParametersValue = (value, path, benchmarkScope) => {
  if (benchmarkScope === "runtime-grpc-leased") {
    return decodeGrpcLeasedBenchmarkParameters(value, path);
  }
  if (benchmarkScope === "runtime-grpc-materialized") {
    return decodeGrpcMaterializedBenchmarkParameters(value, path);
  }
  if (value !== undefined) {
    throw new Error(
      `Benchmark artifact field ${path} is only supported for gRPC runtime benchmark scopes.`,
    );
  }
  return undefined;
};

const grpcSeedMutationCountValue = (value, path, benchmarkScope) => {
  if (benchmarkScope === "runtime-grpc-leased") {
    return decodeGrpcLeasedSeedMutationCount(value, path);
  }
  if (benchmarkScope === "runtime-grpc-materialized") {
    return decodeGrpcMaterializedSeedMutationCount(value, path);
  }
  return undefined;
};

const measurementProtocolValue = (value, path) => {
  if (value === undefined) {
    return undefined;
  }
  const protocol = objectValue(value, path);
  const keys = Object.keys(protocol).sort();
  const supportedKeys = ["memoryCheckpoint", "priming"];
  if (keys.length === 0 || keys.some((key) => !supportedKeys.includes(key))) {
    throw new Error(
      `Benchmark artifact field ${path} must contain one or more of these keys only: ${supportedKeys.join(", ")}.`,
    );
  }
  const memoryCheckpoint =
    Object.hasOwn(protocol, "memoryCheckpoint")
      ? stringValue(protocol.memoryCheckpoint, `${path}.memoryCheckpoint`)
      : undefined;
  if (
    memoryCheckpoint !== undefined &&
    memoryCheckpoint !== "settled-explicit-gc-after-cleanup"
  ) {
    throw new Error(
      `Benchmark artifact field ${path}.memoryCheckpoint must be settled-explicit-gc-after-cleanup.`,
    );
  }
  const priming =
    Object.hasOwn(protocol, "priming")
      ? stringValue(protocol.priming, `${path}.priming`)
      : undefined;
  if (priming !== undefined && priming !== "append-delete-restore-before-sampling") {
    throw new Error(
      `Benchmark artifact field ${path}.priming must be append-delete-restore-before-sampling.`,
    );
  }
  return {
    ...(memoryCheckpoint === undefined ? {} : { memoryCheckpoint }),
    ...(priming === undefined ? {} : { priming }),
  };
};

const comparableBenchmark = (groupName, benchmark) => ({
  groupName,
  maxMs: finiteNumber(benchmark.max, `${benchmark.name}.max`),
  meanMs: finiteNumber(benchmark.mean, `${benchmark.name}.mean`),
  minMs: finiteNumber(benchmark.min, `${benchmark.name}.min`),
  name: stringValue(benchmark.name, "benchmark.name"),
  p99Ms: finiteNumber(benchmark.p99, `${benchmark.name}.p99`),
  sampleCount: positiveInteger(benchmark.sampleCount, `${benchmark.name}.sampleCount`),
});

const nonNegativeIntegerString = (value, path) => {
  const text = stringValue(value, path);
  if (!/^(0|[1-9]\d*)$/u.test(text)) {
    throw new Error(`Benchmark artifact field ${path} must be a non-negative integer string.`);
  }
  return text;
};

const nonNegativeSafeIntegerString = (value, path) => {
  const text = nonNegativeIntegerString(value, path);
  const number = Number.parseInt(text, 10);
  if (!Number.isSafeInteger(number)) {
    throw new Error(`Benchmark artifact field ${path} must be a safe integer string.`);
  }
  return number;
};

const kafkaIngestLaneValue = (value, path) => {
  const lane = objectValue(value, path);
  return {
    internalTopic: stringValue(lane.internalTopic, `${path}.internalTopic`),
    lane: stringValue(lane.lane, `${path}.lane`),
    producedRows: nonNegativeInteger(lane.producedRows, `${path}.producedRows`),
    region: stringValue(lane.region, `${path}.region`),
    sourceTopic: stringValue(lane.sourceTopic, `${path}.sourceTopic`),
    sourceTopicAlias: stringValue(lane.sourceTopicAlias, `${path}.sourceTopicAlias`),
  };
};

const comparableKafkaIngestLane = (lane) => ({
  internalTopic: lane.internalTopic,
  lane: lane.lane,
  producedRows: lane.producedRows,
  region: lane.region,
  sourceTopicAlias: lane.sourceTopicAlias,
});

const comparableKafkaIngestLaneValue = (value, path) => {
  const lane = objectValue(value, path);
  return {
    internalTopic: stringValue(lane.internalTopic, `${path}.internalTopic`),
    lane: stringValue(lane.lane, `${path}.lane`),
    producedRows: nonNegativeInteger(lane.producedRows, `${path}.producedRows`),
    region: stringValue(lane.region, `${path}.region`),
    sourceTopicAlias: stringValue(lane.sourceTopicAlias, `${path}.sourceTopicAlias`),
  };
};

const nonNegativeIntegerStringOrNumber = (value, path) => {
  if (typeof value === "number") {
    return String(nonNegativeSafeInteger(value, path));
  }
  return nonNegativeIntegerString(value, path);
};

const optionalNonNegativeIntegerStringOrNumber = (value, path) =>
  value === null ? null : nonNegativeIntegerStringOrNumber(value, path);

const runtimeMetricsValue = (value, path) => {
  const metrics = objectValue(value, path);
  const eventLoopDelay = objectValue(metrics.eventLoopDelay, `${path}.eventLoopDelay`);
  const healthPolling = objectValue(metrics.healthPolling, `${path}.healthPolling`);
  const kafkaLag = objectValue(metrics.kafkaLag, `${path}.kafkaLag`);
  const normalized = {
    eventLoopDelay: {
      maxMs: nonNegativeFiniteNumber(eventLoopDelay.maxMs, `${path}.eventLoopDelay.maxMs`),
      meanMs: nonNegativeFiniteNumber(eventLoopDelay.meanMs, `${path}.eventLoopDelay.meanMs`),
      p99Ms: nonNegativeFiniteNumber(eventLoopDelay.p99Ms, `${path}.eventLoopDelay.p99Ms`),
    },
    healthPolling: {
      count: nonNegativeInteger(healthPolling.count, `${path}.healthPolling.count`),
      maxMs: nonNegativeFiniteNumber(healthPolling.maxMs, `${path}.healthPolling.maxMs`),
      totalMs: nonNegativeFiniteNumber(healthPolling.totalMs, `${path}.healthPolling.totalMs`),
    },
    kafkaLag: {
      maxConsumerLagMessages: optionalNonNegativeIntegerStringOrNumber(
        kafkaLag.maxConsumerLagMessages,
        `${path}.kafkaLag.maxConsumerLagMessages`,
      ),
      sampledRegionCount: nonNegativeInteger(
        kafkaLag.sampledRegionCount,
        `${path}.kafkaLag.sampledRegionCount`,
      ),
      totalConsumerLagMessages: nonNegativeIntegerStringOrNumber(
        kafkaLag.totalConsumerLagMessages,
        `${path}.kafkaLag.totalConsumerLagMessages`,
      ),
    },
  };
  if (normalized.eventLoopDelay.p99Ms > normalized.eventLoopDelay.maxMs) {
    throw new Error(
      `Benchmark artifact field ${path}.eventLoopDelay.p99Ms must be less than or equal to ${path}.eventLoopDelay.maxMs.`,
    );
  }
  if (normalized.eventLoopDelay.meanMs > normalized.eventLoopDelay.maxMs) {
    throw new Error(
      `Benchmark artifact field ${path}.eventLoopDelay.meanMs must be less than or equal to ${path}.eventLoopDelay.maxMs.`,
    );
  }
  if (normalized.healthPolling.totalMs < normalized.healthPolling.maxMs) {
    throw new Error(
      `Benchmark artifact field ${path}.healthPolling.totalMs must be greater than or equal to ${path}.healthPolling.maxMs.`,
    );
  }
  return normalized;
};

const throughputCaseValue = (value, path, options) => {
  const throughputCase = objectValue(value, path);
  const readSnapshotMetrics =
    options.requireReadSnapshot === true
      ? {
          maxCommitObservedMs: positiveFiniteNumber(
            throughputCase.maxCommitObservedMs,
            `${path}.maxCommitObservedMs`,
          ),
          maxReadSnapshotMs: positiveFiniteNumber(
            throughputCase.maxReadSnapshotMs,
            `${path}.maxReadSnapshotMs`,
          ),
          meanCommitObservedMs: positiveFiniteNumber(
            throughputCase.meanCommitObservedMs,
            `${path}.meanCommitObservedMs`,
          ),
          meanReadSnapshotMs: positiveFiniteNumber(
            throughputCase.meanReadSnapshotMs,
            `${path}.meanReadSnapshotMs`,
          ),
          readSnapshotRowsPerSample: positiveInteger(
            throughputCase.readSnapshotRowsPerSample,
            `${path}.readSnapshotRowsPerSample`,
          ),
        }
      : {};
  const result = {
    aggregateRowsPerSecond: positiveFiniteNumber(
      throughputCase.aggregateRowsPerSecond,
      `${path}.aggregateRowsPerSecond`,
    ),
    maxTotalMs: positiveFiniteNumber(throughputCase.maxTotalMs, `${path}.maxTotalMs`),
    meanConvergenceMs: positiveFiniteNumber(
      throughputCase.meanConvergenceMs,
      `${path}.meanConvergenceMs`,
    ),
    meanProducerSendMs: positiveFiniteNumber(
      throughputCase.meanProducerSendMs,
      `${path}.meanProducerSendMs`,
    ),
    meanRowsPerSecond: positiveFiniteNumber(
      throughputCase.meanRowsPerSecond,
      `${path}.meanRowsPerSecond`,
    ),
    meanTotalMs: positiveFiniteNumber(throughputCase.meanTotalMs, `${path}.meanTotalMs`),
    minRowsPerSecond: positiveFiniteNumber(throughputCase.minRowsPerSecond, `${path}.minRowsPerSecond`),
    name: stringValue(throughputCase.name, `${path}.name`),
    producedRowsPerSample: positiveInteger(
      throughputCase.producedRowsPerSample,
      `${path}.producedRowsPerSample`,
    ),
    ...readSnapshotMetrics,
    sampleCount: positiveInteger(throughputCase.sampleCount, `${path}.sampleCount`),
    totalProducedRows: positiveInteger(
      throughputCase.totalProducedRows,
      `${path}.totalProducedRows`,
    ),
  };
  const expectedTotalProducedRows = result.producedRowsPerSample * result.sampleCount;
  if (result.totalProducedRows !== expectedTotalProducedRows) {
    throw new Error(
      `Benchmark artifact field ${path}.totalProducedRows must equal producedRowsPerSample * sampleCount (${expectedTotalProducedRows}).`,
    );
  }
  const expectedAggregateRowsPerSecond = (result.producedRowsPerSample * 1000) / result.meanTotalMs;
  const aggregateTolerance = Math.max(1e-9, expectedAggregateRowsPerSecond * 1e-9);
  if (Math.abs(result.aggregateRowsPerSecond - expectedAggregateRowsPerSecond) > aggregateTolerance) {
    throw new Error(
      `Benchmark artifact field ${path}.aggregateRowsPerSecond must match producedRowsPerSample * 1000 / meanTotalMs.`,
    );
  }
  if (result.minRowsPerSecond > result.meanRowsPerSecond) {
    throw new Error(
      `Benchmark artifact field ${path}.minRowsPerSecond must be less than or equal to meanRowsPerSecond.`,
    );
  }
  if (result.meanTotalMs > result.maxTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanTotalMs must be less than or equal to maxTotalMs.`,
    );
  }
  if (result.meanProducerSendMs > result.meanTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanProducerSendMs must be less than or equal to meanTotalMs.`,
    );
  }
  if (result.meanConvergenceMs > result.meanTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanConvergenceMs must be less than or equal to meanTotalMs.`,
    );
  }
  if (options.requireReadSnapshot === true) {
    if (result.meanCommitObservedMs > result.maxCommitObservedMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanCommitObservedMs must be less than or equal to maxCommitObservedMs.`,
      );
    }
    if (result.maxCommitObservedMs > result.maxTotalMs) {
      throw new Error(
        `Benchmark artifact field ${path}.maxCommitObservedMs must be less than or equal to maxTotalMs.`,
      );
    }
    if (result.meanCommitObservedMs > result.meanTotalMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanCommitObservedMs must be less than or equal to meanTotalMs.`,
      );
    }
    if (result.meanReadSnapshotMs > result.maxReadSnapshotMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanReadSnapshotMs must be less than or equal to maxReadSnapshotMs.`,
      );
    }
    if (result.meanReadSnapshotMs > result.meanTotalMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanReadSnapshotMs must be less than or equal to meanTotalMs.`,
      );
    }
  }
  return result;
};

const throughputCasesValue = (value, path, options) => {
  const throughput = objectValue(value, path);
  const source = stringValue(throughput.source, `${path}.source`);
  if (source !== "benchmark-operation-timers") {
    throw new Error(`Benchmark artifact field ${path}.source must be benchmark-operation-timers.`);
  }
  return nonEmptyArrayValue(throughput.cases, `${path}.cases`).map((throughputCase, index) =>
    throughputCaseValue(throughputCase, `${path}.cases[${index}]`, options),
  );
};

const runtimeOperationCasesValue = (value, path, benchmarkScope) => {
  if (benchmarkScope === "runtime-grpc-leased") {
    return decodeGrpcLeasedOperationCases(value, path);
  }
  if (benchmarkScope === "runtime-grpc-materialized") {
    return decodeGrpcMaterializedOperationCases(value, path);
  }
  throw new Error(`Benchmark artifact field ${path} is only supported for gRPC runtime scopes.`);
};

const validateGrpcOperationAccounting = (
  operationCases,
  grpcParameters,
  mutationCount,
  seedMutationCount,
  path,
  benchmarkScope,
) => {
  if (benchmarkScope === "runtime-grpc-leased") {
    validateGrpcLeasedOperationAccounting(
      operationCases,
      grpcParameters,
      mutationCount,
      seedMutationCount,
      path,
    );
    return;
  }
  validateGrpcMaterializedOperationAccounting(
    operationCases,
    grpcParameters,
    mutationCount,
    seedMutationCount,
    path,
  );
};

const validateBenchmarkCasesMatchBenchmarks = (benchmarkCases, benchmarks, path) => {
  const benchmarkNames = new Set(benchmarks.map((benchmark) => benchmark.name));
  for (const benchmarkCase of benchmarkCases) {
    if (!benchmarkNames.has(benchmarkCase)) {
      throw new Error(
        `Benchmark artifact field ${path} contains benchmarkCase without matching Vitest benchmark: ${benchmarkCase}.`,
      );
    }
  }
  for (const benchmarkName of benchmarkNames) {
    if (!benchmarkCases.includes(benchmarkName)) {
      throw new Error(
        `Benchmark artifact field ${path} is missing benchmarkCase for Vitest benchmark: ${benchmarkName}.`,
      );
    }
  }
};

const validateRuntimeOperationCasesMatchBenchmarkCases = (
  operationCases,
  benchmarkCases,
  benchmarks,
  minimumSampleCount,
  path,
  benchmarkPath,
) => {
  const benchmarkCaseNames = new Set(benchmarkCases);
  const benchmarkSampleCountByName = new Map();
  for (const benchmark of benchmarks) {
    const previousSampleCount = benchmarkSampleCountByName.get(benchmark.name);
    if (previousSampleCount !== undefined) {
      throw new Error(
        `Benchmark artifact field ${benchmarkPath} contains duplicate benchmark name for runtime operation case: ${benchmark.name}.`,
      );
    }
    benchmarkSampleCountByName.set(benchmark.name, benchmark.sampleCount);
  }
  const operationCaseNames = new Set();
  for (const operationCase of operationCases) {
    if (operationCase.sampleCount < minimumSampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.${operationCase.name}.sampleCount must be at least ${minimumSampleCount} but was ${operationCase.sampleCount}.`,
      );
    }
    if (!benchmarkCaseNames.has(operationCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains runtime operation case without matching benchmarkCase: ${operationCase.name}.`,
      );
    }
    const benchmarkSampleCount = benchmarkSampleCountByName.get(operationCase.name);
    if (operationCase.sampleCount !== benchmarkSampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.${operationCase.name}.sampleCount must equal Vitest benchmark sampleCount ${benchmarkSampleCount} but was ${operationCase.sampleCount}.`,
      );
    }
    if (operationCaseNames.has(operationCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains duplicate runtime operation case: ${operationCase.name}.`,
      );
    }
    operationCaseNames.add(operationCase.name);
  }
  for (const benchmarkCase of benchmarkCases) {
    if (!operationCaseNames.has(benchmarkCase)) {
      throw new Error(
        `Benchmark artifact field ${path} is missing runtime operation case for benchmarkCase: ${benchmarkCase}.`,
      );
    }
  }
};

const validateThroughputCasesMatchBenchmarks = (throughputCases, benchmarks, path) => {
  const benchmarkSampleCountByName = new Map();
  for (const benchmark of benchmarks) {
    const previousSampleCount = benchmarkSampleCountByName.get(benchmark.name);
    if (previousSampleCount !== undefined && previousSampleCount !== benchmark.sampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.benchmarks contains ambiguous benchmark sampleCount values for ${benchmark.name}.`,
      );
    }
    benchmarkSampleCountByName.set(benchmark.name, benchmark.sampleCount);
  }
  const throughputByName = throughputCaseByName(throughputCases, path);
  for (const [benchmarkName, benchmarkSampleCount] of benchmarkSampleCountByName) {
    const throughputCase = throughputByName.get(benchmarkName);
    if (throughputCase === undefined) {
      throw new Error(`Benchmark artifact field ${path} is missing throughput case ${benchmarkName}.`);
    }
    if (throughputCase.sampleCount !== benchmarkSampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.${benchmarkName}.sampleCount must equal benchmark sampleCount ${benchmarkSampleCount} but was ${throughputCase.sampleCount}.`,
      );
    }
  }
  for (const throughputCase of throughputCases) {
    if (!benchmarkSampleCountByName.has(throughputCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains throughput case without matching benchmark: ${throughputCase.name}.`,
      );
    }
  }
};

const validateThroughputCasesMatchMutationCount = (throughputCases, mutationCount, path) => {
  const totalProducedRows = throughputCases.reduce(
    (total, throughputCase) => total + throughputCase.totalProducedRows,
    0,
  );
  if (totalProducedRows !== mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path} totalProducedRows must equal mutationCount ${mutationCount} but was ${totalProducedRows}.`,
    );
  }
};

const validateRuntimeSummaryIngestCompleteness = (summary, path, mutationCount) => {
  const health = objectValue(summary.health, `${path}.health`);
  const engine = objectValue(health.engine, `${path}.health.engine`);
  const engineTopics = objectValue(engine.topics, `${path}.health.engine.topics`);
  const kafka = objectValue(health.kafka, `${path}.health.kafka`);
  const kafkaTopics = objectValue(kafka.topics, `${path}.health.kafka.topics`);
  const lanes = nonEmptyArrayValue(
    objectValue(summary.kafka, `${path}.kafka`).ingestLanes,
    `${path}.kafka.ingestLanes`,
  ).map((lane, index) => kafkaIngestLaneValue(lane, `${path}.kafka.ingestLanes[${index}]`));
  const uniqueKeys = new Map();
  const requireUniqueLaneKey = (key, label, lane) => {
    const previousLane = uniqueKeys.get(`${label}:${key}`);
    if (previousLane !== undefined) {
      throw new Error(
        `Benchmark artifact field ${path}.kafka.ingestLanes contains duplicate ${label} ${key} in lanes ${previousLane} and ${lane}.`,
      );
    }
    uniqueKeys.set(`${label}:${key}`, lane);
  };

  let totalProducedRows = 0;
  for (const lane of lanes) {
    requireUniqueLaneKey(lane.lane, "lane", lane.lane);
    requireUniqueLaneKey(lane.internalTopic, "internalTopic", lane.lane);
    requireUniqueLaneKey(lane.sourceTopicAlias, "sourceTopicAlias", lane.lane);
    requireUniqueLaneKey(`${lane.sourceTopic}:${lane.region}`, "sourceTopic+region", lane.lane);
    totalProducedRows += lane.producedRows;
    const topicHealth = objectValue(
      engineTopics[lane.internalTopic],
      `${path}.health.engine.topics.${lane.internalTopic}`,
    );
    const rowCount = nonNegativeInteger(
      topicHealth.rowCount,
      `${path}.health.engine.topics.${lane.internalTopic}.rowCount`,
    );
    if (rowCount !== lane.producedRows) {
      throw new Error(
        `Benchmark artifact field ${path}.health.engine.topics.${lane.internalTopic}.rowCount must equal producedRows ${lane.producedRows} for Kafka ingest lane ${lane.lane} but was ${rowCount}.`,
      );
    }

    const kafkaTopicHealth = objectValue(
      kafkaTopics[lane.sourceTopic],
      `${path}.health.kafka.topics.${lane.sourceTopic}`,
    );
    const viewServerTopic = stringValue(
      kafkaTopicHealth.viewServerTopic,
      `${path}.health.kafka.topics.${lane.sourceTopic}.viewServerTopic`,
    );
    if (viewServerTopic !== lane.internalTopic) {
      throw new Error(
        `Benchmark artifact field ${path}.health.kafka.topics.${lane.sourceTopic}.viewServerTopic must equal internalTopic ${lane.internalTopic} for Kafka ingest lane ${lane.lane} but was ${viewServerTopic}.`,
      );
    }
    const committedOffset = nonNegativeSafeIntegerString(
      objectValue(
        objectValue(
          kafkaTopicHealth.regions,
          `${path}.health.kafka.topics.${lane.sourceTopic}.regions`,
        )[lane.region],
        `${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}`,
      ).committedOffset,
      `${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}.committedOffset`,
    );
    if (committedOffset !== lane.producedRows) {
      throw new Error(
        `Benchmark artifact field ${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}.committedOffset must equal producedRows ${lane.producedRows} for Kafka ingest lane ${lane.lane} but was ${committedOffset}.`,
      );
    }
  }
  if (totalProducedRows !== mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path}.kafka.ingestLanes producedRows total must equal mutationCount ${mutationCount} but was ${totalProducedRows}.`,
    );
  }

  return lanes.map(comparableKafkaIngestLane);
};

export const comparableBenchmarksFromVitestOutput = (vitestOutput) =>
  arrayValue(objectValue(vitestOutput, "vitestOutput").files, "vitestOutput.files").flatMap(
    (file, fileIndex) =>
      arrayValue(objectValue(file, `files[${fileIndex}]`).groups, `files[${fileIndex}].groups`)
        .flatMap((group, groupIndex) => {
          const groupPath = `files[${fileIndex}].groups[${groupIndex}]`;
          const groupName = stringValue(
            objectValue(group, groupPath).fullName,
            `${groupPath}.fullName`,
          );
          return arrayValue(
            objectValue(group, `files[${fileIndex}].groups[${groupIndex}]`).benchmarks,
            `files[${fileIndex}].groups[${groupIndex}].benchmarks`,
          ).map((benchmark) => comparableBenchmark(groupName, benchmark));
        }),
  );

export const decodeBenchmarkObservation = (task, summaryArtifact, vitestOutput) => {
  const summary = objectValue(summaryArtifact, task.summaryPath);
  const artifactKind = summaryArtifactKind(summary.artifactKind, `${task.summaryPath}.artifactKind`);
  const latency = objectValue(summary.latency, `${task.summaryPath}.latency`);
  const latencyOutputJsonPath = stringValue(
    latency.outputJsonPath,
    `${task.summaryPath}.latency.outputJsonPath`,
  );
  if (latencyOutputJsonPath !== task.packageOutputJsonPath) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.latency.outputJsonPath changed from ${task.packageOutputJsonPath} to ${latencyOutputJsonPath}.`,
    );
  }
  const latencySource = stringValue(latency.source, `${task.summaryPath}.latency.source`);
  const expectedSamplingPolicy = decodeBenchmarkSamplingPolicy(
    task.samplingPolicy,
    `${task.label}.samplingPolicy`,
  );
  const artifactSamplingPolicy = decodeBenchmarkSamplingPolicy(
    summary.samplingPolicy,
    `${task.summaryPath}.samplingPolicy`,
  );
  if (JSON.stringify(artifactSamplingPolicy) !== JSON.stringify(expectedSamplingPolicy)) {
    throw new Error(`${task.label}: benchmark samplingPolicy did not match the runner policy.`);
  }
  const samplingPolicy = artifactSamplingPolicy;
  const memory = objectValue(summary.memory, `${task.summaryPath}.memory`);
  const rssBytes = decodeBenchmarkMemoryRssTotalDeltaBytes(
    memory,
    `${task.summaryPath}.memory`,
    samplingPolicy,
  );
  if (
    (artifactKind === "engine-benchmark-summary" ||
      artifactKind === "runtime-benchmark-summary") &&
    rssBytes === undefined
  ) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.memory.totalDelta.rssBytes is required for ${artifactKind}.`,
    );
  }
  if (task.expectedArtifactKind !== undefined && artifactKind !== task.expectedArtifactKind) {
    throw new Error(
      `${task.label}: artifactKind changed from ${task.expectedArtifactKind} to ${artifactKind}.`,
    );
  }
  const benchmarkScope = stringValue(summary.benchmarkScope, `${task.summaryPath}.benchmarkScope`);
  if (task.expectedBenchmarkScope !== undefined && benchmarkScope !== task.expectedBenchmarkScope) {
    throw new Error(
      `${task.label}: benchmarkScope changed from ${task.expectedBenchmarkScope} to ${benchmarkScope}.`,
    );
  }
  const measurementProtocol = measurementProtocolValue(
    summary.measurementProtocol,
    `${task.summaryPath}.measurementProtocol`,
  );
  const expectedMeasurementProtocol = measurementProtocolValue(
    task.expectedMeasurementProtocol,
    `${task.label}.expectedMeasurementProtocol`,
  );
  if (JSON.stringify(measurementProtocol) !== JSON.stringify(expectedMeasurementProtocol)) {
    throw new Error(`${task.label}: measurementProtocol did not match the runner policy.`);
  }
  const rowCount = finiteNumber(summary.rowCount, `${task.summaryPath}.rowCount`);
  if (task.expectedRowCount !== undefined && rowCount !== task.expectedRowCount) {
    throw new Error(`${task.label}: rowCount changed from ${task.expectedRowCount} to ${rowCount}.`);
  }
  const benchmarks = comparableBenchmarksFromVitestOutput(vitestOutput);
  const minimumSampleCount = positiveInteger(
    task.minimumSampleCount,
    `${task.label}.minimumSampleCount`,
  );
  for (const benchmark of benchmarks) {
    if (benchmark.sampleCount < minimumSampleCount) {
      throw new Error(
        `${task.label} / ${benchmarkKey(benchmark)}: sampleCount must be at least ${minimumSampleCount} but was ${benchmark.sampleCount}.`,
      );
    }
  }
  validateBenchmarkSamplingPolicy(samplingPolicy, benchmarks, minimumSampleCount, task.label);

  const benchmarkCases = stringArrayValue(summary.benchmarkCases, `${task.summaryPath}.benchmarkCases`);
  const mutationCount = nonNegativeInteger(summary.mutationCount, `${task.summaryPath}.mutationCount`);
  const expectedMutationCount =
    task.expectedMutationCount === undefined
      ? undefined
      : nonNegativeInteger(task.expectedMutationCount, `${task.label}.expectedMutationCount`);
  if (expectedMutationCount !== undefined && mutationCount !== expectedMutationCount) {
    throw new Error(
      `${task.label}: mutationCount must be exactly ${expectedMutationCount} but was ${mutationCount}.`,
    );
  }
  const seedMutationCount = grpcSeedMutationCountValue(
    summary.seedMutationCount,
    `${task.summaryPath}.seedMutationCount`,
    benchmarkScope,
  );
  const topics = stringArrayValue(summary.topics, `${task.summaryPath}.topics`);
  const requiresKafkaThroughput =
    artifactKind === "runtime-benchmark-summary" && benchmarkScope.startsWith("runtime-kafka-");
  const kafkaIngestLanes =
    requiresKafkaThroughput
      ? validateRuntimeSummaryIngestCompleteness(summary, task.summaryPath, mutationCount)
      : undefined;
  const throughputCases =
    summary.throughput === undefined
      ? undefined
      : throughputCasesValue(summary.throughput, `${task.summaryPath}.throughput`, {
          requireReadSnapshot: requiresKafkaThroughput,
        });
  if (requiresKafkaThroughput && throughputCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.throughput is required for ${benchmarkScope}.`,
    );
  }
  if (throughputCases !== undefined) {
    validateThroughputCasesMatchBenchmarks(
      throughputCases,
      benchmarks,
      `${task.summaryPath}.throughput.cases`,
    );
    if (requiresKafkaThroughput) {
      validateThroughputCasesMatchMutationCount(
        throughputCases,
        mutationCount,
        `${task.summaryPath}.throughput.cases`,
      );
    }
  }
  const grpcParameters = grpcBenchmarkParametersValue(
    summary.grpcParameters,
    `${task.summaryPath}.grpcParameters`,
    benchmarkScope,
  );
  const requiresGrpcOperationCases =
    benchmarkScope === "runtime-grpc-materialized" || benchmarkScope === "runtime-grpc-leased";
  if (requiresGrpcOperationCases) {
    validateBenchmarkCasesMatchBenchmarks(
      benchmarkCases,
      benchmarks,
      `${task.summaryPath}.benchmarkCases`,
    );
  }
  const runtimeOperationCases =
    summary.cases === undefined
      ? undefined
      : runtimeOperationCasesValue(summary.cases, `${task.summaryPath}.cases`, benchmarkScope);
  if (requiresGrpcOperationCases && runtimeOperationCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.cases is required for ${benchmarkScope}.`,
    );
  }
  if (runtimeOperationCases !== undefined) {
    validateRuntimeOperationCasesMatchBenchmarkCases(
      runtimeOperationCases,
      benchmarkCases,
      benchmarks,
      minimumSampleCount,
      `${task.summaryPath}.cases`,
      `${task.summaryPath}.benchmarks`,
    );
    validateGrpcOperationAccounting(
      runtimeOperationCases,
      grpcParameters,
      mutationCount,
      seedMutationCount,
      `${task.summaryPath}.cases`,
      benchmarkScope,
    );
  }

  return {
    ...(summary.activeViewCountBeforeCleanup === undefined
      ? {}
      : {
          activeViewCountBeforeCleanup: nonNegativeInteger(
            summary.activeViewCountBeforeCleanup,
            `${task.summaryPath}.activeViewCountBeforeCleanup`,
          ),
        }),
    artifactKind,
    backpressureCount: finiteNumber(
      summary.backpressureCount,
      `${task.summaryPath}.backpressureCount`,
    ),
    benchmarks,
    benchmarkCases,
    benchmarkName: stringValue(summary.benchmarkName, `${task.summaryPath}.benchmarkName`),
    benchmarkScope,
    browser: optionalObjectValue(summary.browser, `${task.summaryPath}.browser`),
    cleanupLeakCount: finiteNumber(summary.cleanupLeakCount, `${task.summaryPath}.cleanupLeakCount`),
    groupedKeyWidthParameters: optionalObjectValue(
      summary.groupedKeyWidthParameters,
      `${task.summaryPath}.groupedKeyWidthParameters`,
    ),
    groupedWriteAdmission: optionalObjectValue(
      summary.groupedWriteAdmission,
      `${task.summaryPath}.groupedWriteAdmission`,
    ),
    ...(grpcParameters === undefined ? {} : { grpcParameters }),
    kafkaIngestLanes,
    latencySource,
    ...(measurementProtocol === undefined ? {} : { measurementProtocol }),
    memoryRssTotalDeltaBytes: rssBytes,
    minimumSampleCount,
    mutationCount,
    outputJsonPath: task.outputJsonPath,
    queuedEventCount: finiteNumber(summary.queuedEventCount, `${task.summaryPath}.queuedEventCount`),
    rowCount,
    ...(runtimeOperationCases === undefined ? {} : { runtimeOperationCases }),
    ...(summary.runtimeMetrics === undefined
      ? {}
      : {
          runtimeMetrics: runtimeMetricsValue(
            summary.runtimeMetrics,
            `${task.summaryPath}.runtimeMetrics`,
          ),
        }),
    seedBatchSize: optionalFiniteNumber(summary.seedBatchSize, `${task.summaryPath}.seedBatchSize`),
    ...(seedMutationCount === undefined ? {} : { seedMutationCount }),
    subscriberCount: finiteNumber(summary.subscriberCount, `${task.summaryPath}.subscriberCount`),
    summaryPath: task.summaryPath,
    ...(samplingPolicy === undefined ? {} : { samplingPolicy }),
    taskLabel: task.label,
    throughputCases,
    topics,
  };
};

export const readBenchmarkObservation = (task) =>
  decodeBenchmarkObservation(
    task,
    readJsonFile(task.summaryPath),
    readJsonFile(task.outputJsonPath),
  );

export const buildBenchmarkBaseline = (profile, observations) => ({
  artifactKind: "view-server-benchmark-baseline",
  profile,
  tasks: observations,
  thresholds: benchmarkThresholdsForProfile(profile),
});

const writableBenchmarkBaseline = (path, baseline) => {
  const validated = validateBenchmarkBaseline(baseline, path);
  const comparison = compareBenchmarkArtifacts({
    actual: validated,
    baseline: validated,
    policy: benchmarkComparisonPolicyForProfile(validated.profile),
  });
  if (!comparison.ok) {
    throw new Error(
      [`Benchmark baseline ${path} is not writable:`, ...comparison.regressions].join("\n"),
    );
  }
  return validated;
};

export const readBenchmarkBaseline = (path) => writableBenchmarkBaseline(path, readJsonFile(path));

export const writeBenchmarkBaseline = (path, baseline) => {
  writeJsonFile(path, writableBenchmarkBaseline(path, baseline));
};

const thresholdsValue = (value, path, expectedThresholds) => {
  const thresholds = exactObjectValue(value, path, Object.keys(expectedThresholds));
  const validatedThresholds = {};
  const deltaThresholdValue = (threshold, thresholdPath) => {
    const thresholdObject = exactObjectValue(threshold, thresholdPath, [
      "maxAbsoluteDeltaMs",
      "maxRatio",
    ]);
    return {
      maxAbsoluteDeltaMs: finiteNumber(
        thresholdObject.maxAbsoluteDeltaMs,
        `${thresholdPath}.maxAbsoluteDeltaMs`,
      ),
      maxRatio: finiteNumber(thresholdObject.maxRatio, `${thresholdPath}.maxRatio`),
    };
  };
  const throughputThresholdValue = (threshold, thresholdPath) => {
    const thresholdObject = exactObjectValue(threshold, thresholdPath, ["minRatio"]);
    return {
      minRatio: finiteNumber(thresholdObject.minRatio, `${thresholdPath}.minRatio`),
    };
  };
  const memoryThresholdValue = (threshold, thresholdPath) => {
    const thresholdObject = exactObjectValue(threshold, thresholdPath, [
      "maxAbsoluteDeltaBytes",
      "maxRatio",
    ]);
    return {
      maxAbsoluteDeltaBytes: finiteNumber(
        thresholdObject.maxAbsoluteDeltaBytes,
        `${thresholdPath}.maxAbsoluteDeltaBytes`,
      ),
      maxRatio: finiteNumber(thresholdObject.maxRatio, `${thresholdPath}.maxRatio`),
    };
  };
  if (expectedThresholds.commitObservedMean !== undefined) {
    validatedThresholds.commitObservedMean = deltaThresholdValue(
      thresholds.commitObservedMean,
      `${path}.commitObservedMean`,
    );
  }
  if (expectedThresholds.commitObservedMax !== undefined) {
    validatedThresholds.commitObservedMax = deltaThresholdValue(
      thresholds.commitObservedMax,
      `${path}.commitObservedMax`,
    );
  }
  validatedThresholds.latencyMean = deltaThresholdValue(
    thresholds.latencyMean,
    `${path}.latencyMean`,
  );
  validatedThresholds.latencyP99 = deltaThresholdValue(
    thresholds.latencyP99,
    `${path}.latencyP99`,
  );
  validatedThresholds.memoryRssTotalDelta = memoryThresholdValue(
    thresholds.memoryRssTotalDelta,
    `${path}.memoryRssTotalDelta`,
  );
  if (expectedThresholds.operationMax !== undefined) {
    validatedThresholds.operationMax = deltaThresholdValue(
      thresholds.operationMax,
      `${path}.operationMax`,
    );
  }
  if (expectedThresholds.operationMean !== undefined) {
    validatedThresholds.operationMean = deltaThresholdValue(
      thresholds.operationMean,
      `${path}.operationMean`,
    );
  }
  validatedThresholds.throughputAggregateRowsPerSecond = throughputThresholdValue(
    thresholds.throughputAggregateRowsPerSecond,
    `${path}.throughputAggregateRowsPerSecond`,
  );
  if (expectedThresholds.throughputReadSnapshotMax !== undefined) {
    validatedThresholds.throughputReadSnapshotMax = deltaThresholdValue(
      thresholds.throughputReadSnapshotMax,
      `${path}.throughputReadSnapshotMax`,
    );
  }
  if (expectedThresholds.throughputReadSnapshotMean !== undefined) {
    validatedThresholds.throughputReadSnapshotMean = deltaThresholdValue(
      thresholds.throughputReadSnapshotMean,
      `${path}.throughputReadSnapshotMean`,
    );
  }
  if (JSON.stringify(validatedThresholds) !== JSON.stringify(expectedThresholds)) {
    throw new Error(`Benchmark artifact field ${path} must match code-owned profile thresholds.`);
  }
  return validatedThresholds;
};

const validateBenchmark = (benchmark, path) => ({
  groupName: stringValue(benchmark.groupName, `${path}.groupName`),
  maxMs: finiteNumber(benchmark.maxMs, `${path}.maxMs`),
  meanMs: finiteNumber(benchmark.meanMs, `${path}.meanMs`),
  minMs: finiteNumber(benchmark.minMs, `${path}.minMs`),
  name: stringValue(benchmark.name, `${path}.name`),
  p99Ms: finiteNumber(benchmark.p99Ms, `${path}.p99Ms`),
  sampleCount: positiveInteger(benchmark.sampleCount, `${path}.sampleCount`),
});

export const validateBenchmarkObservation = (task, path) => {
  const artifactKind = summaryArtifactKind(task.artifactKind, `${path}.artifactKind`);
  const benchmarkScope = stringValue(task.benchmarkScope, `${path}.benchmarkScope`);
  const taskLabel = stringValue(task.taskLabel, `${path}.taskLabel`);
  const measurementProtocol = measurementProtocolValue(
    task.measurementProtocol,
    `${path}.measurementProtocol`,
  );
  const mutationCount = nonNegativeInteger(task.mutationCount, `${path}.mutationCount`);
  const seedMutationCount = grpcSeedMutationCountValue(
    task.seedMutationCount,
    `${path}.seedMutationCount`,
    benchmarkScope,
  );
  const isKafkaScope = benchmarkScope.startsWith("runtime-kafka-");
  const requiresKafkaThroughput = artifactKind === "runtime-benchmark-summary" && isKafkaScope;
  const memoryRssTotalDeltaBytes = optionalFiniteNumber(
    task.memoryRssTotalDeltaBytes,
    `${path}.memoryRssTotalDeltaBytes`,
  );
  if (
    (artifactKind === "engine-benchmark-summary" ||
      artifactKind === "runtime-benchmark-summary") &&
    memoryRssTotalDeltaBytes === undefined
  ) {
    throw new Error(
      `Benchmark artifact field ${path}.memoryRssTotalDeltaBytes is required for ${artifactKind}.`,
    );
  }
  const benchmarks = nonEmptyArrayValue(task.benchmarks, `${path}.benchmarks`).map(
    (benchmark, index) => validateBenchmark(benchmark, `${path}.benchmarks[${index}]`),
  );
  const throughputCases =
    task.throughputCases === undefined
      ? undefined
      : nonEmptyArrayValue(task.throughputCases, `${path}.throughputCases`).map(
          (throughputCase, index) =>
            throughputCaseValue(throughputCase, `${path}.throughputCases[${index}]`, {
              requireReadSnapshot: requiresKafkaThroughput,
            }),
        );
  if (requiresKafkaThroughput && throughputCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${path}.throughputCases is required for ${benchmarkScope}.`,
    );
  }
  if (throughputCases !== undefined) {
    validateThroughputCasesMatchBenchmarks(throughputCases, benchmarks, `${path}.throughputCases`);
    if (requiresKafkaThroughput) {
      validateThroughputCasesMatchMutationCount(
        throughputCases,
        mutationCount,
        `${path}.throughputCases`,
      );
    }
  }
  const kafkaIngestLanes =
    task.kafkaIngestLanes === undefined
      ? undefined
      : nonEmptyArrayValue(task.kafkaIngestLanes, `${path}.kafkaIngestLanes`).map(
          (lane, index) => comparableKafkaIngestLaneValue(lane, `${path}.kafkaIngestLanes[${index}]`),
        );
  if (isKafkaScope && kafkaIngestLanes === undefined) {
    throw new Error(
      `Benchmark artifact field ${path}.kafkaIngestLanes is required for ${benchmarkScope}.`,
    );
  }
  const grpcParameters = grpcBenchmarkParametersValue(
    task.grpcParameters,
    `${path}.grpcParameters`,
    benchmarkScope,
  );
  const requiresGrpcOperationCases =
    benchmarkScope === "runtime-grpc-materialized" || benchmarkScope === "runtime-grpc-leased";
  const runtimeOperationCases =
    task.runtimeOperationCases === undefined
      ? undefined
      : runtimeOperationCasesValue(
          task.runtimeOperationCases,
          `${path}.runtimeOperationCases`,
          benchmarkScope,
        );
  if (requiresGrpcOperationCases && runtimeOperationCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${path}.runtimeOperationCases is required for ${benchmarkScope}.`,
    );
  }
  const benchmarkCases = stringArrayValue(task.benchmarkCases, `${path}.benchmarkCases`);
  const minimumSampleCount = positiveInteger(task.minimumSampleCount, `${path}.minimumSampleCount`);
  const samplingPolicy = decodeBenchmarkSamplingPolicy(
    task.samplingPolicy,
    `${path}.samplingPolicy`,
  );
  validateBenchmarkSamplingPolicy(samplingPolicy, benchmarks, minimumSampleCount, taskLabel);
  validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes(
    samplingPolicy,
    memoryRssTotalDeltaBytes,
    `${path}.memoryRssTotalDeltaBytes`,
  );
  if (requiresGrpcOperationCases) {
    validateBenchmarkCasesMatchBenchmarks(benchmarkCases, benchmarks, `${path}.benchmarkCases`);
  }
  if (runtimeOperationCases !== undefined) {
    validateRuntimeOperationCasesMatchBenchmarkCases(
      runtimeOperationCases,
      benchmarkCases,
      benchmarks,
      minimumSampleCount,
      `${path}.runtimeOperationCases`,
      `${path}.benchmarks`,
    );
    validateGrpcOperationAccounting(
      runtimeOperationCases,
      grpcParameters,
      mutationCount,
      seedMutationCount,
      `${path}.runtimeOperationCases`,
      benchmarkScope,
    );
  }
  return {
    ...(task.activeViewCountBeforeCleanup === undefined
      ? {}
      : {
          activeViewCountBeforeCleanup: nonNegativeInteger(
            task.activeViewCountBeforeCleanup,
            `${path}.activeViewCountBeforeCleanup`,
          ),
        }),
    artifactKind,
    backpressureCount: finiteNumber(task.backpressureCount, `${path}.backpressureCount`),
    benchmarks,
    benchmarkCases,
    benchmarkName: stringValue(task.benchmarkName, `${path}.benchmarkName`),
    benchmarkScope,
    browser: optionalObjectValue(task.browser, `${path}.browser`),
    cleanupLeakCount: finiteNumber(task.cleanupLeakCount, `${path}.cleanupLeakCount`),
    groupedKeyWidthParameters: optionalObjectValue(
      task.groupedKeyWidthParameters,
      `${path}.groupedKeyWidthParameters`,
    ),
    groupedWriteAdmission: optionalObjectValue(
      task.groupedWriteAdmission,
      `${path}.groupedWriteAdmission`,
    ),
    ...(grpcParameters === undefined ? {} : { grpcParameters }),
    kafkaIngestLanes,
    latencySource: stringValue(task.latencySource, `${path}.latencySource`),
    ...(measurementProtocol === undefined ? {} : { measurementProtocol }),
    memoryRssTotalDeltaBytes,
    minimumSampleCount,
    mutationCount,
    outputJsonPath: stringValue(task.outputJsonPath, `${path}.outputJsonPath`),
    queuedEventCount: finiteNumber(task.queuedEventCount, `${path}.queuedEventCount`),
    rowCount: finiteNumber(task.rowCount, `${path}.rowCount`),
    ...(runtimeOperationCases === undefined ? {} : { runtimeOperationCases }),
    ...(task.runtimeMetrics === undefined
      ? {}
      : {
          runtimeMetrics: runtimeMetricsValue(task.runtimeMetrics, `${path}.runtimeMetrics`),
        }),
    seedBatchSize: optionalFiniteNumber(task.seedBatchSize, `${path}.seedBatchSize`),
    ...(seedMutationCount === undefined ? {} : { seedMutationCount }),
    subscriberCount: finiteNumber(task.subscriberCount, `${path}.subscriberCount`),
    summaryPath: stringValue(task.summaryPath, `${path}.summaryPath`),
    ...(samplingPolicy === undefined ? {} : { samplingPolicy }),
    taskLabel,
    throughputCases,
    topics: stringArrayValue(task.topics, `${path}.topics`),
  };
};

export const validateBenchmarkBaseline = (baseline, path = "baseline") => {
  const baselineObject = objectValue(baseline, path);
  const profile = stringValue(baselineObject.profile, `${path}.profile`);
  return {
    artifactKind: baselineArtifactKind(baselineObject.artifactKind, `${path}.artifactKind`),
    profile,
    tasks: nonEmptyArrayValue(baselineObject.tasks, `${path}.tasks`).map((task, index) =>
      validateBenchmarkObservation(task, `${path}.tasks[${index}]`),
    ),
    thresholds: thresholdsValue(
      baselineObject.thresholds,
      `${path}.thresholds`,
      benchmarkThresholdsForProfile(profile),
    ),
  };
};
