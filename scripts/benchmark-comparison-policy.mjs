import {
  compareExact,
  compareExactJson,
  compareLatency,
  compareThroughput,
  mapByUniqueKey,
  pushRegression,
} from "./benchmark-artifact-mechanics.mjs";
import { compareGrpcMaterializedBenchmarkTask } from "./grpc-materialized-benchmark-policy.mjs";
import { compareGrpcLeasedBenchmarkTask } from "./grpc-leased-benchmark-policy.mjs";
import { samplingPolicyRequiresExactMutationCount } from "./benchmark-sampling-policy.mjs";

export const defaultBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 5,
    maxRatio: 8,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 10,
    maxRatio: 8,
  },
  memoryRssTotalDelta: {
    maxAbsoluteDeltaBytes: 128 * 1024 * 1024,
    maxRatio: 3,
  },
  throughputAggregateRowsPerSecond: {
    minRatio: 0.5,
  },
};

const kafkaReadSnapshotThresholds = {
  throughputReadSnapshotMax: {
    maxAbsoluteDeltaMs: 50,
    maxRatio: 10,
  },
  throughputReadSnapshotMean: {
    maxAbsoluteDeltaMs: 25,
    maxRatio: 8,
  },
};

const kafkaRssReportThresholds = {
  maxAbsoluteDeltaBytes: 256 * 1024 * 1024,
  maxRatio: 64,
};

export const groupedOrderNeutralBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 0.5,
    maxRatio: 6,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 1,
    maxRatio: 6,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond:
    defaultBenchmarkThresholds.throughputAggregateRowsPerSecond,
};

export const rawReadWriteBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 0.5,
    maxRatio: 4,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 20,
    maxRatio: 8,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond:
    defaultBenchmarkThresholds.throughputAggregateRowsPerSecond,
};

export const kafkaIngestBenchmarkThresholds = {
  commitObservedMean: {
    maxAbsoluteDeltaMs: 2_000,
    maxRatio: 1.5,
  },
  commitObservedMax: {
    maxAbsoluteDeltaMs: 2_500,
    maxRatio: 1.5,
  },
  latencyMean: {
    maxAbsoluteDeltaMs: 2_000,
    maxRatio: 1.5,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 2_500,
    maxRatio: 1.5,
  },
  memoryRssTotalDelta: kafkaRssReportThresholds,
  throughputAggregateRowsPerSecond: {
    minRatio: 0.75,
  },
  ...kafkaReadSnapshotThresholds,
};

export const kafkaSustainedFirehoseBenchmarkThresholds = {
  commitObservedMean: {
    maxAbsoluteDeltaMs: 5_000,
    maxRatio: 1.75,
  },
  commitObservedMax: {
    maxAbsoluteDeltaMs: 6_000,
    maxRatio: 1.75,
  },
  latencyMean: {
    maxAbsoluteDeltaMs: 5_000,
    maxRatio: 1.75,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 6_000,
    maxRatio: 1.75,
  },
  memoryRssTotalDelta: kafkaRssReportThresholds,
  throughputAggregateRowsPerSecond: {
    minRatio: 0.75,
  },
  ...kafkaReadSnapshotThresholds,
};

export const websocketFirehoseBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 3,
    maxRatio: 3,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 4,
    maxRatio: 3,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond:
    defaultBenchmarkThresholds.throughputAggregateRowsPerSecond,
};

export const grpcRuntimeBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 25,
    maxRatio: 12,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 100,
    maxRatio: 16,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  operationMax: {
    maxAbsoluteDeltaMs: 100,
    maxRatio: 16,
  },
  operationMean: {
    maxAbsoluteDeltaMs: 25,
    maxRatio: 12,
  },
  throughputAggregateRowsPerSecond:
    defaultBenchmarkThresholds.throughputAggregateRowsPerSecond,
};

export const grpcRetainedRuntimeBenchmarkThresholds = {
  ...grpcRuntimeBenchmarkThresholds,
  memoryRssTotalDelta: kafkaRssReportThresholds,
  operationMax: {
    maxAbsoluteDeltaMs: 500,
    maxRatio: 16,
  },
  operationMean: {
    maxAbsoluteDeltaMs: 250,
    maxRatio: 12,
  },
};

export const benchmarkThresholdsForProfile = (profile) =>
  profile === "grouped-order-neutral"
    ? groupedOrderNeutralBenchmarkThresholds
    : profile === "raw-read-write"
      ? rawReadWriteBenchmarkThresholds
    : profile === "kafka-ingest"
      ? kafkaIngestBenchmarkThresholds
    : profile === "kafka-sustained-firehose"
      ? kafkaSustainedFirehoseBenchmarkThresholds
    : profile === "websocket-firehose"
      ? websocketFirehoseBenchmarkThresholds
    : profile === "grpc-leased-retained"
      ? grpcRetainedRuntimeBenchmarkThresholds
    : profile === "grpc-materialized" || profile === "grpc-leased"
      ? grpcRuntimeBenchmarkThresholds
    : defaultBenchmarkThresholds;

const metricDefinitions = {
  commitObservedMax: {
    applicability: "kafka-throughput-case",
    direction: "lower-is-better",
  },
  commitObservedMean: {
    applicability: "kafka-throughput-case",
    direction: "lower-is-better",
  },
  latencyMean: {
    applicability: "benchmark-case",
    direction: "lower-is-better",
  },
  latencyP99: {
    applicability: "benchmark-case",
    direction: "lower-is-better",
  },
  memoryRssTotalDelta: {
    applicability: "task-memory",
    direction: "lower-is-better",
  },
  operationMax: {
    applicability: "grpc-operation-case",
    direction: "lower-is-better",
  },
  operationMean: {
    applicability: "grpc-operation-case",
    direction: "lower-is-better",
  },
  throughputAggregateRowsPerSecond: {
    applicability: "throughput-case",
    direction: "higher-is-better",
  },
  throughputReadSnapshotMax: {
    applicability: "kafka-throughput-case",
    direction: "lower-is-better",
  },
  throughputReadSnapshotMean: {
    applicability: "kafka-throughput-case",
    direction: "lower-is-better",
  },
};

const latencyToleranceDefinition = {
  maxAbsoluteDeltaMs: "non-negative",
  maxRatio: "positive",
};

const toleranceDefinitions = {
  commitObservedMax: latencyToleranceDefinition,
  commitObservedMean: latencyToleranceDefinition,
  latencyMean: latencyToleranceDefinition,
  latencyP99: latencyToleranceDefinition,
  memoryRssTotalDelta: {
    maxAbsoluteDeltaBytes: "non-negative",
    maxRatio: "positive",
  },
  operationMax: latencyToleranceDefinition,
  operationMean: latencyToleranceDefinition,
  throughputAggregateRowsPerSecond: {
    minRatio: "positive",
  },
  throughputReadSnapshotMax: latencyToleranceDefinition,
  throughputReadSnapshotMean: latencyToleranceDefinition,
};

const comparisonPolicy = (profile, thresholds) => {
  const requiredMetrics = Object.keys(thresholds).sort();
  return {
    applicableProfiles: [profile],
    metrics: Object.fromEntries(
      requiredMetrics.map((metricName) => [
        metricName,
        {
          ...metricDefinitions[metricName],
          tolerance: thresholds[metricName],
        },
      ]),
    ),
    requiredMetrics,
  };
};

export const benchmarkComparisonPolicyForProfile = (profile) =>
  comparisonPolicy(profile, benchmarkThresholdsForProfile(profile));

export const thresholdsFromComparisonPolicy = (policy) =>
  Object.fromEntries(
    policy.requiredMetrics.map((metricName) => [
      metricName,
      policy.metrics[metricName].tolerance,
    ]),
  );

const requiredMetricNamesForProfiles = (profiles) =>
  [...new Set(profiles.flatMap((profile) => Object.keys(benchmarkThresholdsForProfile(profile))))].sort();

const joinedFieldNames = (fieldNames) =>
  fieldNames.length === 1
    ? fieldNames[0]
    : `${fieldNames.slice(0, -1).join(", ")} and ${fieldNames.at(-1)}`;

const toleranceRegressions = (policyLabel, metricName, tolerance) => {
  if (
    tolerance === undefined ||
    tolerance === null ||
    typeof tolerance !== "object" ||
    Array.isArray(tolerance)
  ) {
    return [
      `Comparison policy ${policyLabel} metric ${metricName} must define a tolerance object.`,
    ];
  }
  const definition = toleranceDefinitions[metricName];
  const expectedFields = Object.keys(definition).sort();
  const actualFields = Object.keys(tolerance).sort();
  if (JSON.stringify(actualFields) !== JSON.stringify(expectedFields)) {
    return [
      `Comparison policy ${policyLabel} metric ${metricName} tolerance must contain exactly ${joinedFieldNames(expectedFields)}.`,
    ];
  }
  const regressions = [];
  for (const fieldName of expectedFields) {
    const value = tolerance[fieldName];
    const requirement = definition[fieldName];
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      (requirement === "positive" ? value <= 0 : value < 0)
    ) {
      regressions.push(
        `Comparison policy ${policyLabel} metric ${metricName} tolerance.${fieldName} must be a ${requirement} finite number.`,
      );
    }
  }
  return regressions;
};

const comparisonPolicyRegressions = (policy, baseline, actual) => {
  const regressions = [];
  const policyLabel = policy.applicableProfiles.join(", ");
  const expectedMetricNames = requiredMetricNamesForProfiles(policy.applicableProfiles);
  const declaredMetricNames = new Set(policy.requiredMetrics);
  for (const metricName of expectedMetricNames) {
    const metric = policy.metrics[metricName];
    if (!declaredMetricNames.has(metricName) || metric === undefined) {
      regressions.push(
        `Comparison policy ${policyLabel} is missing required metric ${metricName}.`,
      );
      continue;
    }
    const definition = metricDefinitions[metricName];
    if (metric.direction !== definition.direction) {
      regressions.push(
        `Comparison policy ${policyLabel} metric ${metricName} direction must be ${definition.direction} but was ${metric.direction}.`,
      );
    }
    if (metric.applicability !== definition.applicability) {
      regressions.push(
        `Comparison policy ${policyLabel} metric ${metricName} applicability must be ${definition.applicability} but was ${metric.applicability}.`,
      );
    }
    regressions.push(...toleranceRegressions(policyLabel, metricName, metric.tolerance));
  }
  const expectedMetricNameSet = new Set(expectedMetricNames);
  for (const metricName of declaredMetricNames) {
    if (expectedMetricNameSet.has(metricName)) {
      continue;
    }
    if (metricDefinitions[metricName] === undefined) {
      regressions.push(
        `Comparison policy ${policyLabel} requires unknown metric ${metricName}.`,
      );
      continue;
    }
    regressions.push(
      `Comparison policy ${policyLabel} requires metric ${metricName}, which does not apply to its profiles.`,
    );
  }
  if (!policy.applicableProfiles.includes(baseline.profile)) {
    regressions.push(
      `Comparison policy applies to ${policyLabel} but baseline artifact uses ${baseline.profile}.`,
    );
  }
  if (!policy.applicableProfiles.includes(actual.profile)) {
    regressions.push(
      `Comparison policy applies to ${policyLabel} but actual artifact uses ${actual.profile}.`,
    );
  }
  return regressions;
};

const byteMetricLimit = (baseline, threshold) =>
  Math.min(baseline * threshold.maxRatio, baseline + threshold.maxAbsoluteDeltaBytes);

const benchmarkKey = (benchmark) => `${benchmark.groupName} / ${benchmark.name}`;

const taskByLabel = (tasks, path) =>
  mapByUniqueKey(tasks, (task) => task.taskLabel, path, "taskLabel");

const benchmarkByName = (benchmarks, path) =>
  mapByUniqueKey(benchmarks, benchmarkKey, path, "benchmark case");

const throughputCaseByName = (throughputCases, path) =>
  mapByUniqueKey(throughputCases, (throughputCase) => throughputCase.name, path, "throughput case");

const compareZeroCounter = (regressions, taskLabel, name, actual) => {
  if (actual !== 0) {
    pushRegression(regressions, `${taskLabel}: ${name} must stay 0 but was ${actual}.`);
  }
};

const compareMinimumCount = (regressions, taskLabel, name, baseline, actual) => {
  const allowedDrop = Math.max(10, baseline * 0.05);
  const minimum = baseline - allowedDrop;
  if (actual < minimum) {
    pushRegression(
      regressions,
      `${taskLabel}: ${name} dropped from ${baseline} to ${actual}; allowed >= ${Math.ceil(minimum)}.`,
    );
  }
};

const compareRss = (regressions, taskLabel, threshold, baseline, actual) => {
  const limit = byteMetricLimit(baseline, threshold);
  if (actual > limit) {
    pushRegression(
      regressions,
      `${taskLabel}: total RSS delta regressed from ${baseline} bytes to ${actual} bytes; allowed <= ${Math.round(
        limit,
      )} bytes.`,
    );
  }
};

const compareThroughputCases = (regressions, taskLabel, threshold, baselineCases, actualCases) => {
  if (baselineCases === undefined && actualCases === undefined) {
    return;
  }
  if (baselineCases === undefined || actualCases === undefined) {
    pushRegression(regressions, `${taskLabel}: throughputCases presence changed.`);
    return;
  }
  const baselineByName = throughputCaseByName(baselineCases, `baseline.tasks[${taskLabel}]`);
  const actualByName = throughputCaseByName(actualCases, `actual.tasks[${taskLabel}]`);
  for (const caseName of actualByName.keys()) {
    if (!baselineByName.has(caseName)) {
      pushRegression(regressions, `${taskLabel}: unexpected throughput case ${caseName}.`);
    }
  }
  for (const baselineCase of baselineCases) {
    const actualCase = actualByName.get(baselineCase.name);
    if (actualCase === undefined) {
      pushRegression(regressions, `${taskLabel}: missing throughput case ${baselineCase.name}.`);
      continue;
    }
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput producedRowsPerSample`,
      baselineCase.producedRowsPerSample,
      actualCase.producedRowsPerSample,
    );
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput sampleCount`,
      baselineCase.sampleCount,
      actualCase.sampleCount,
    );
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput totalProducedRows`,
      baselineCase.totalProducedRows,
      actualCase.totalProducedRows,
    );
    compareThroughput(
      regressions,
      taskLabel,
      baselineCase.name,
      "aggregateRowsPerSecond",
      threshold.throughputAggregateRowsPerSecond,
      baselineCase.aggregateRowsPerSecond,
      actualCase.aggregateRowsPerSecond,
    );
    if (threshold.throughputReadSnapshotMean !== undefined) {
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "meanCommitObservedMs",
        threshold.commitObservedMean,
        baselineCase.meanCommitObservedMs,
        actualCase.meanCommitObservedMs,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "maxCommitObservedMs",
        threshold.commitObservedMax,
        baselineCase.maxCommitObservedMs,
        actualCase.maxCommitObservedMs,
      );
      compareExact(
        regressions,
        taskLabel,
        `${baselineCase.name} throughput readSnapshotRowsPerSample`,
        baselineCase.readSnapshotRowsPerSample,
        actualCase.readSnapshotRowsPerSample,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "meanReadSnapshotMs",
        threshold.throughputReadSnapshotMean,
        baselineCase.meanReadSnapshotMs,
        actualCase.meanReadSnapshotMs,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "maxReadSnapshotMs",
        threshold.throughputReadSnapshotMax,
        baselineCase.maxReadSnapshotMs,
        actualCase.maxReadSnapshotMs,
      );
    }
  }
};

const compareReportOnlyRuntimeMetricsPresence = (regressions, taskLabel, baselineTask, actualTask) => {
  if (baselineTask.runtimeMetrics !== undefined && actualTask.runtimeMetrics === undefined) {
    pushRegression(regressions, `${taskLabel}: runtimeMetrics presence changed.`);
  }
};

const compareKafkaSustainedFirehoseFinalLag = (regressions, taskLabel, actualTask) => {
  if (actualTask.benchmarkScope !== "runtime-kafka-sustained-firehose") {
    return;
  }
  const kafkaLag = actualTask.runtimeMetrics?.kafkaLag;
  if (kafkaLag === undefined) {
    pushRegression(regressions, `${taskLabel}: runtimeMetrics.kafkaLag is required.`);
    return;
  }
  if (kafkaLag.sampledRegionCount !== actualTask.kafkaIngestLanes.length) {
    pushRegression(
      regressions,
      `${taskLabel}: runtimeMetrics.kafkaLag sampled ${kafkaLag.sampledRegionCount} regions but expected ${actualTask.kafkaIngestLanes.length}.`,
    );
  }
  if (kafkaLag.totalConsumerLagMessages !== "0") {
    pushRegression(
      regressions,
      `${taskLabel}: final Kafka lag must be 0 but was ${kafkaLag.totalConsumerLagMessages}.`,
    );
  }
  if (kafkaLag.maxConsumerLagMessages !== "0") {
    pushRegression(
      regressions,
      `${taskLabel}: max final Kafka lag must be 0 but was ${kafkaLag.maxConsumerLagMessages}.`,
    );
  }
};

const benchmarkScopeRequiresExactMutationCount = (benchmarkScope) =>
  benchmarkScope === "engine-raw-write" ||
  benchmarkScope === "runtime-grpc-leased" ||
  benchmarkScope === "runtime-grpc-materialized" ||
  benchmarkScope === "runtime-kafka-ingest" ||
  benchmarkScope === "runtime-websocket-firehose";

const benchmarkTaskRequiresExactMutationCount = (task) =>
  benchmarkScopeRequiresExactMutationCount(task.benchmarkScope) ||
  samplingPolicyRequiresExactMutationCount(task.samplingPolicy);

const benchmarkScopeRequiresExactSampleCount = (benchmarkScope) =>
  benchmarkScope === "engine-raw-write";

export const compareBenchmarkArtifacts = ({
  actual: validatedActual,
  baseline: validatedBaseline,
  policy,
}) => {
  const policyRegressions = comparisonPolicyRegressions(
    policy,
    validatedBaseline,
    validatedActual,
  );
  if (policyRegressions.length > 0) {
    return {
      ok: false,
      regressions: policyRegressions,
    };
  }
  const thresholds = thresholdsFromComparisonPolicy(policy);
  if (
    validatedBaseline.tasks.some(
      (task) =>
        task.benchmarkScope === "runtime-grpc-materialized" ||
        task.benchmarkScope === "runtime-grpc-leased",
    ) &&
    (thresholds.operationMean === undefined || thresholds.operationMax === undefined)
  ) {
    return {
      ok: false,
      regressions: [
        `Comparison policy ${validatedBaseline.profile} does not define operationMean and operationMax metrics required by its gRPC runtime operation cases.`,
      ],
    };
  }
  const baselineTasks = taskByLabel(validatedBaseline.tasks, "baseline.tasks");
  const actualTasks = taskByLabel(validatedActual.tasks, "actual.tasks");
  const regressions = [];

  compareExact(
    regressions,
    validatedBaseline.profile,
    "baseline artifactKind",
    validatedBaseline.artifactKind,
    validatedActual.artifactKind,
  );
  compareExact(
    regressions,
    validatedBaseline.profile,
    "profile",
    validatedBaseline.profile,
    validatedActual.profile,
  );

  for (const taskLabel of actualTasks.keys()) {
    if (!baselineTasks.has(taskLabel)) {
      pushRegression(regressions, `${taskLabel}: unexpected benchmark task in actual run.`);
    }
  }

  for (const [taskLabel, baselineTask] of baselineTasks) {
    const actualTask = actualTasks.get(taskLabel);
    if (actualTask === undefined) {
      pushRegression(regressions, `${taskLabel}: missing benchmark task in actual run.`);
      continue;
    }

    compareExact(
      regressions,
      taskLabel,
      "artifactKind",
      baselineTask.artifactKind,
      actualTask.artifactKind,
    );
    compareExact(
      regressions,
      taskLabel,
      "benchmarkScope",
      baselineTask.benchmarkScope,
      actualTask.benchmarkScope,
    );
    compareExact(
      regressions,
      taskLabel,
      "benchmarkName",
      baselineTask.benchmarkName,
      actualTask.benchmarkName,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "benchmarkCases",
      baselineTask.benchmarkCases,
      actualTask.benchmarkCases,
    );
    compareExact(regressions, taskLabel, "rowCount", baselineTask.rowCount, actualTask.rowCount);
    if (benchmarkTaskRequiresExactMutationCount(baselineTask)) {
      compareExact(
        regressions,
        taskLabel,
        "mutationCount",
        baselineTask.mutationCount,
        actualTask.mutationCount,
      );
    } else {
      compareMinimumCount(
        regressions,
        taskLabel,
        "mutationCount",
        baselineTask.mutationCount,
        actualTask.mutationCount,
      );
    }
    compareExact(
      regressions,
      taskLabel,
      "subscriberCount",
      baselineTask.subscriberCount,
      actualTask.subscriberCount,
    );
    compareExactJson(regressions, taskLabel, "topics", baselineTask.topics, actualTask.topics);
    compareExact(
      regressions,
      taskLabel,
      "latencySource",
      baselineTask.latencySource,
      actualTask.latencySource,
    );
    compareExactJson(regressions, taskLabel, "browser", baselineTask.browser, actualTask.browser);
    if (baselineTask.activeViewCountBeforeCleanup !== undefined) {
      compareExact(
        regressions,
        taskLabel,
        "activeViewCountBeforeCleanup",
        baselineTask.activeViewCountBeforeCleanup,
        actualTask.activeViewCountBeforeCleanup,
      );
    }
    compareExactJson(
      regressions,
      taskLabel,
      "kafkaIngestLanes",
      baselineTask.kafkaIngestLanes,
      actualTask.kafkaIngestLanes,
    );
    compareThroughputCases(
      regressions,
      taskLabel,
      thresholds,
      baselineTask.throughputCases,
      actualTask.throughputCases,
    );
    if (baselineTask.benchmarkScope === "runtime-grpc-leased") {
      regressions.push(
        ...compareGrpcLeasedBenchmarkTask(taskLabel, thresholds, baselineTask, actualTask),
      );
    } else if (baselineTask.benchmarkScope === "runtime-grpc-materialized") {
      regressions.push(
        ...compareGrpcMaterializedBenchmarkTask(
          taskLabel,
          thresholds,
          baselineTask,
          actualTask,
        ),
      );
    }
    compareReportOnlyRuntimeMetricsPresence(regressions, taskLabel, baselineTask, actualTask);
    compareKafkaSustainedFirehoseFinalLag(regressions, taskLabel, actualTask);
    compareExact(
      regressions,
      taskLabel,
      "seedBatchSize",
      baselineTask.seedBatchSize,
      actualTask.seedBatchSize,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "groupedKeyWidthParameters",
      baselineTask.groupedKeyWidthParameters,
      actualTask.groupedKeyWidthParameters,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "groupedWriteAdmission",
      baselineTask.groupedWriteAdmission,
      actualTask.groupedWriteAdmission,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "grpcParameters",
      baselineTask.grpcParameters,
      actualTask.grpcParameters,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "measurementProtocol",
      baselineTask.measurementProtocol,
      actualTask.measurementProtocol,
    );
    compareExact(
      regressions,
      taskLabel,
      "minimumSampleCount",
      baselineTask.minimumSampleCount,
      actualTask.minimumSampleCount,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "samplingPolicy",
      baselineTask.samplingPolicy,
      actualTask.samplingPolicy,
    );
    compareExact(
      regressions,
      taskLabel,
      "outputJsonPath",
      baselineTask.outputJsonPath,
      actualTask.outputJsonPath,
    );
    compareExact(
      regressions,
      taskLabel,
      "summaryPath",
      baselineTask.summaryPath,
      actualTask.summaryPath,
    );
    compareZeroCounter(regressions, taskLabel, "cleanupLeakCount", actualTask.cleanupLeakCount);
    compareZeroCounter(regressions, taskLabel, "backpressureCount", actualTask.backpressureCount);
    compareZeroCounter(regressions, taskLabel, "queuedEventCount", actualTask.queuedEventCount);

    if (
      baselineTask.memoryRssTotalDeltaBytes !== undefined &&
      actualTask.memoryRssTotalDeltaBytes !== undefined
    ) {
      compareRss(
        regressions,
        taskLabel,
        thresholds.memoryRssTotalDelta,
        baselineTask.memoryRssTotalDeltaBytes,
        actualTask.memoryRssTotalDeltaBytes,
      );
    } else if (baselineTask.memoryRssTotalDeltaBytes !== actualTask.memoryRssTotalDeltaBytes) {
      pushRegression(
        regressions,
        `${taskLabel}: memoryRssTotalDeltaBytes presence changed between baseline and actual run.`,
      );
    }

    const baselineBenchmarks = benchmarkByName(
      baselineTask.benchmarks,
      `baseline.tasks[${taskLabel}].benchmarks`,
    );
    const actualBenchmarks = benchmarkByName(
      actualTask.benchmarks,
      `actual.tasks[${taskLabel}].benchmarks`,
    );
    for (const benchmarkName of actualBenchmarks.keys()) {
      if (!baselineBenchmarks.has(benchmarkName)) {
        pushRegression(regressions, `${taskLabel}: unexpected benchmark case ${benchmarkName}.`);
      }
    }
    for (const baselineBenchmark of baselineTask.benchmarks) {
      const baselineBenchmarkKey = benchmarkKey(baselineBenchmark);
      const actualBenchmark = actualBenchmarks.get(baselineBenchmarkKey);
      if (actualBenchmark === undefined) {
        pushRegression(
          regressions,
          `${taskLabel}: missing benchmark case ${baselineBenchmarkKey}.`,
        );
        continue;
      }
      if (actualBenchmark.sampleCount < actualTask.minimumSampleCount) {
        pushRegression(
          regressions,
          `${taskLabel} / ${baselineBenchmarkKey}: sampleCount must be at least ${actualTask.minimumSampleCount} but was ${actualBenchmark.sampleCount}.`,
        );
      }
      if (benchmarkScopeRequiresExactSampleCount(baselineTask.benchmarkScope)) {
        compareExact(
          regressions,
          `${taskLabel} / ${baselineBenchmarkKey}`,
          "sampleCount",
          baselineBenchmark.sampleCount,
          actualBenchmark.sampleCount,
        );
      }
      compareLatency(
        regressions,
        taskLabel,
        baselineBenchmarkKey,
        "mean",
        thresholds.latencyMean,
        baselineBenchmark.meanMs,
        actualBenchmark.meanMs,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineBenchmarkKey,
        "p99",
        thresholds.latencyP99,
        baselineBenchmark.p99Ms,
        actualBenchmark.p99Ms,
      );
    }
  }

  return {
    ok: regressions.length === 0,
    regressions,
  };
};
