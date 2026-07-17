const enginePackageDirectory = "packages/column-live-view-engine";
const reactPackageDirectory = "packages/react";
const runtimePackageDirectory = "packages/runtime";

export const summaryPath = (outputJsonPath) =>
  outputJsonPath.endsWith(".json")
    ? `${outputJsonPath.slice(0, -".json".length)}.summary.json`
    : `${outputJsonPath}.summary.json`;

export const repeatArtifactPath = (artifactPath, repeatIndex, repeatCount) => {
  if (repeatCount === 1) {
    return artifactPath;
  }
  const run = String(repeatIndex + 1).padStart(2, "0");
  const total = String(repeatCount).padStart(2, "0");
  const suffix = `.run-${run}-of-${total}`;
  return artifactPath.endsWith(".json")
    ? `${artifactPath.slice(0, -".json".length)}${suffix}.json`
    : `${artifactPath}${suffix}`;
};

const packageArtifactPath = (packageDirectory, outputJsonPath) =>
  `${packageDirectory}/${outputJsonPath}`;

const engineArtifactName = (name) => `.artifacts/${name}`;

const reactArtifactName = (name) => `.artifacts/${name}`;

const explicitGcMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-after-cleanup",
};

const runtimeMeasurementProtocolFromEnv = (env) =>
  env.VIEW_SERVER_RUNTIME_BENCH_EXPLICIT_GC === "1"
    ? explicitGcMeasurementProtocol
    : undefined;

const groupedWriteMeasurementProtocolFromEnv = (env) => {
  const explicitGc = env.VIEW_SERVER_ENGINE_BENCH_EXPLICIT_GC === "1";
  const priming = env.VIEW_SERVER_ENGINE_BENCH_PRIMING_APPEND_BATCHES === "1";
  return explicitGc || priming
    ? {
        ...(explicitGc ? explicitGcMeasurementProtocol : {}),
        ...(priming ? { priming: "append-delete-restore-before-sampling" } : {}),
      }
    : undefined;
};

const task = ({
  artifactKind,
  benchmarkScope,
  env,
  expectedMutationCount,
  expectedMeasurementProtocol,
  label,
  minimumSampleCount,
  outputJsonPath,
  packageDirectory,
  rowCount,
  samplingPolicy,
  vpTask,
}) => ({
  args: ["run", "--no-cache", vpTask],
  command: "vp",
  env,
  expectedArtifactKind: artifactKind,
  expectedBenchmarkScope: benchmarkScope,
  expectedMeasurementProtocol,
  expectedMutationCount,
  expectedRowCount: rowCount,
  label,
  minimumSampleCount,
  outputJsonPath: packageArtifactPath(packageDirectory, outputJsonPath),
  packageOutputJsonPath: outputJsonPath,
  samplingPolicy,
  summaryPath: packageArtifactPath(packageDirectory, summaryPath(outputJsonPath)),
});

const minimumSampleCountFrom = (env, key) => Number.parseInt(env[key] ?? "5", 10);

const timedReadSamplingPolicyFrom = (env, iterationBoundBenchmarkName) => {
  const minimumTimedReadSampleCount = env["VIEW_SERVER_ENGINE_BENCH_TIMED_READ_MINIMUM_SAMPLES"];
  const memoryRssMetric = env["VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC"];
  if (minimumTimedReadSampleCount === undefined) {
    if (memoryRssMetric !== undefined) {
      throw new Error("Peak RSS measurement requires timed read sampling.");
    }
    return undefined;
  }
  if (memoryRssMetric !== "process-peak-over-initial-current") {
    throw new Error(
      "Timed read sampling requires process-peak-over-initial-current RSS measurement.",
    );
  }
  const iterationBoundSampleCount = env["VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS"];
  return {
    iterationBoundCases:
      iterationBoundBenchmarkName === undefined
        ? []
        : [
            {
              name: iterationBoundBenchmarkName,
              sampleCount: Number.parseInt(iterationBoundSampleCount, 10),
              timeMs: 0,
              warmupIterations: 0,
              warmupTimeMs: 0,
            },
          ],
    memoryRssMetric,
    measured: {
      minimumSampleCount: Number.parseInt(minimumTimedReadSampleCount, 10),
      timeMs: Number.parseInt(env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"], 10),
      warmupIterations: Number.parseInt(
        env["VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS"],
        10,
      ),
      warmupTimeMs: Number.parseInt(
        env["VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS"],
        10,
      ),
    },
  };
};

export const taskForRepeat = (currentTask, repeatIndex, repeatCount) => {
  if (repeatCount === 1) {
    return currentTask;
  }
  const packageOutputJsonPath = repeatArtifactPath(
    currentTask.packageOutputJsonPath,
    repeatIndex,
    repeatCount,
  );
  const env = Object.fromEntries(
    Object.entries(currentTask.env).map(([key, value]) => [
      key,
      value === currentTask.packageOutputJsonPath ? packageOutputJsonPath : value,
    ]),
  );
  const outputJsonPath = repeatArtifactPath(currentTask.outputJsonPath, repeatIndex, repeatCount);
  return {
    ...currentTask,
    env: {
      ...env,
      VIEW_SERVER_BENCH_REPEAT_INDEX: String(repeatIndex + 1),
      VIEW_SERVER_BENCH_REPEAT_TOTAL: String(repeatCount),
    },
    label: `${currentTask.label} run ${repeatIndex + 1}/${repeatCount}`,
    outputJsonPath,
    packageOutputJsonPath,
    summaryPath: summaryPath(outputJsonPath),
  };
};

export const rawSnapshotTask = (rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-snapshot-${rowCount}rows.json`);
  const samplingPolicy = timedReadSamplingPolicyFrom(
    env,
    "live subscription delta after publish",
  );
  const expectedMutationCount =
    samplingPolicy === undefined
      ? undefined
      : rowCount + samplingPolicy.iterationBoundCases[0].sampleCount;
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-snapshot",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    expectedMutationCount,
    label: `raw snapshot ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(
      env,
      samplingPolicy === undefined
        ? "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"
        : "VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS",
    ),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    samplingPolicy,
    vpTask: "column-live-view-engine#bench:raw-snapshot",
  });
};

export const rawPredicateIndexTask = (rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-predicate-index-${rowCount}rows.json`);
  const samplingPolicy = timedReadSamplingPolicyFrom(env);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-predicate-index",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `raw predicate index ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    samplingPolicy,
    vpTask: "column-live-view-engine#bench:raw-predicate-index",
  });
};

export const rawWriteTask = (writeMode, rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-write-${writeMode}-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-write",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      VIEW_SERVER_ENGINE_BENCH_WRITE_MODE: writeMode,
      ...env,
    },
    label: `raw write ${writeMode} ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-write",
  });
};

export const rawLiveFanoutTask = (fanoutCase, rowCount, subscriberCount, env = {}) => {
  const outputJsonPath = engineArtifactName(
    `raw-live-fanout-${fanoutCase}-${rowCount}rows-${subscriberCount}subs.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-live-fanout",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE: fanoutCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS: String(subscriberCount),
      ...env,
    },
    label: `raw live fanout ${fanoutCase} ${rowCount} rows ${subscriberCount} subscribers`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-live-fanout",
  });
};

export const groupedAggregateTask = (rowCount, env) => {
  const outputJsonPath = engineArtifactName(`grouped-aggregate-${rowCount}rows.json`);
  const samplingPolicy = timedReadSamplingPolicyFrom(
    env,
    "live grouped aggregate delta after publish",
  );
  const expectedMutationCount =
    samplingPolicy === undefined
      ? undefined
      : rowCount + samplingPolicy.iterationBoundCases[0].sampleCount;
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-aggregate",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    expectedMutationCount,
    label: `grouped aggregate ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(
      env,
      samplingPolicy === undefined
        ? "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"
        : "VIEW_SERVER_ENGINE_BENCH_MUTATION_ITERATIONS",
    ),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    samplingPolicy,
    vpTask: "column-live-view-engine#bench:grouped-aggregate",
  });
};

export const groupedKeyWidthTask = (rowCount, env) => {
  const outputJsonPath = engineArtifactName(`grouped-key-width-${rowCount}rows.json`);
  const samplingPolicy = timedReadSamplingPolicyFrom(env);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-key-width",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `grouped key width ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    samplingPolicy,
    vpTask: "column-live-view-engine#bench:grouped-key-width",
  });
};

const deltaOperationArtifactOperationCount = (_deltaCase, _rowCount, operationCount) =>
  operationCount * 2;

export const queryDeltaOperationsTask = (deltaCase, rowCount, operationCount, env = {}) => {
  const artifactOperationCount = deltaOperationArtifactOperationCount(
    deltaCase,
    rowCount,
    operationCount,
  );
  const outputJsonPath = engineArtifactName(
    `query-delta-operations-${deltaCase}-${rowCount}rows-${artifactOperationCount}ops.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-query-delta-operations",
    env: {
      VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE: deltaCase,
      VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT: String(operationCount),
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `query delta operations ${deltaCase} ${rowCount} rows ${artifactOperationCount} ops`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:query-delta-operations",
  });
};

export const groupedWriteTask = (mode, rowCount, env) => {
  const writeBatchSize = env.VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE;
  const readerProfile = env.VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE ?? "dual";
  const readerProfileLabel = readerProfile === "dual" ? "" : ` ${readerProfile}`;
  const readerProfileSegment = readerProfile === "dual" ? "" : `-${readerProfile}`;
  const labelSuffix =
    env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX === undefined
      ? ""
      : ` ${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX}`;
  const suffix =
    env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX === undefined
      ? ""
      : `-${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX}`;
  const outputJsonPath = engineArtifactName(
    `grouped-write-${mode}${readerProfileSegment}-${rowCount}rows-${writeBatchSize}mutations${suffix}.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-write",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_EXPECTED_GROUPED_ADMISSION: mode,
      VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: mode,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    expectedMeasurementProtocol: groupedWriteMeasurementProtocolFromEnv(env),
    label: `grouped write ${mode}${readerProfileLabel} ${rowCount} rows ${writeBatchSize} mutations${labelSuffix}`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:grouped-write",
  });
};

export const rawActiveRetainedDeltaTask = (retainedCase, rowCount, env) => {
  const retainedWindowLimit = env["VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT"];
  const replacementBatchSize = env["VIEW_SERVER_ENGINE_BENCH_REPLACEMENT_BATCH_SIZE"];
  const retainedWindowSuffix =
    retainedWindowLimit !== undefined &&
    (retainedCase === "noop" ||
      retainedCase === "match-replacement-batch" ||
      retainedCase === "visible-delete-batch");
  const artifactSegment = retainedWindowSuffix
    ? `${retainedCase}-${rowCount}rows-${retainedWindowLimit}limit-${replacementBatchSize ?? "2"}batch`
    : `${retainedCase}-${rowCount}rows`;
  const outputJsonPath = engineArtifactName(
    `raw-active-retained-delta-${artifactSegment}.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-active-retained-delta",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE: retainedCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label:
      retainedWindowLimit === undefined
        ? `raw active retained delta ${retainedCase} ${rowCount} rows`
        : `raw active retained delta ${retainedCase} ${rowCount} rows ${retainedWindowLimit} limit ${replacementBatchSize ?? "2"} batch`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-active-retained-delta",
  });
};

export const reactInMemoryTask = (browser, rowCount, env = {}) => {
  const outputJsonPath = reactArtifactName(`in-memory-live-query-${rowCount}rows-${browser}.json`);
  return task({
    artifactKind: "react-browser-benchmark-summary",
    benchmarkScope: "react-in-memory-live-query",
    env: {
      VIEW_SERVER_REACT_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_REACT_BENCH_BROWSER: browser,
      VIEW_SERVER_REACT_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `React in-memory ${browser} ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_REACT_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: reactPackageDirectory,
    rowCount,
    vpTask: "react#bench:in-memory-live-query",
  });
};

export const runtimeKafkaIngestTask = (rowCount, env) => {
  const outputJsonPath = `.artifacts/kafka-ingest-${rowCount}rows.json`;
  return task({
    artifactKind: "runtime-benchmark-summary",
    benchmarkScope: "runtime-kafka-ingest",
    env: {
      VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: String(rowCount),
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
      ...env,
    },
    label: `Kafka ingest ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: runtimePackageDirectory,
    rowCount,
    vpTask: "runtime#bench:kafka-ingest",
  });
};

export const runtimeKafkaSustainedFirehoseTask = (rowCount, sustainedBatchCount, env) => {
  const outputJsonPath = `.artifacts/kafka-sustained-firehose-${rowCount}rows-${sustainedBatchCount}batches.json`;
  return task({
    artifactKind: "runtime-benchmark-summary",
    benchmarkScope: "runtime-kafka-sustained-firehose",
    env: {
      VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: String(rowCount),
      VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE: "sustained-firehose",
      VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES: String(sustainedBatchCount),
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
      ...env,
    },
    label: `Kafka sustained firehose ${rowCount} rows x ${sustainedBatchCount} batches`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: runtimePackageDirectory,
    rowCount,
    vpTask: "runtime#bench:kafka-ingest",
  });
};

export const runtimeGrpcMaterializedTask = (seedRows, batchSize, env) => {
  const outputJsonPath = `.artifacts/grpc-materialized-${seedRows}seed-${batchSize}batch.json`;
  return task({
    artifactKind: "runtime-benchmark-summary",
    benchmarkScope: "runtime-grpc-materialized",
    env: {
      VIEW_SERVER_RUNTIME_BENCH_GRPC_BATCH_SIZE: String(batchSize),
      VIEW_SERVER_RUNTIME_BENCH_GRPC_SEED_ROWS: String(seedRows),
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
      ...env,
    },
    expectedMeasurementProtocol: runtimeMeasurementProtocolFromEnv(env),
    label: `gRPC materialized ${seedRows} seed rows ${batchSize} batch`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: runtimePackageDirectory,
    rowCount: seedRows,
    vpTask: "runtime#bench:grpc-materialized",
  });
};

export const runtimeGrpcLeasedTask = (rowsPerFeed, routeCount, retainedRows, env) => {
  const outputJsonPath = `.artifacts/grpc-leased-${rowsPerFeed}rows-${routeCount}routes-${retainedRows}retained.json`;
  return task({
    artifactKind: "runtime-benchmark-summary",
    benchmarkScope: "runtime-grpc-leased",
    env: {
      VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROUTE_COUNT: String(routeCount),
      VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_ROWS_PER_FEED: String(rowsPerFeed),
      VIEW_SERVER_RUNTIME_BENCH_GRPC_LEASED_RETAINED_ROWS: String(retainedRows),
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
      ...env,
    },
    expectedMeasurementProtocol: runtimeMeasurementProtocolFromEnv(env),
    label: `gRPC leased ${rowsPerFeed} rows per feed ${routeCount} routes ${retainedRows} retained rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: runtimePackageDirectory,
    rowCount: rowsPerFeed,
    vpTask: "runtime#bench:grpc-leased",
  });
};

export const runtimeWebSocketFirehoseTask = (firehoseCase, rowCount, subscriberCount, env) => {
  const outputJsonPath = `.artifacts/websocket-firehose-${firehoseCase}-${rowCount}rows-${subscriberCount}subs.json`;
  return task({
    artifactKind: "runtime-benchmark-summary",
    benchmarkScope: "runtime-websocket-firehose",
    env: {
      VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_CASE: firehoseCase,
      VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_ROWS: String(rowCount),
      VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_SUBSCRIBERS: String(subscriberCount),
      ...env,
    },
    label: `WebSocket firehose ${firehoseCase} ${rowCount} rows ${subscriberCount} subscribers`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: runtimePackageDirectory,
    rowCount,
    vpTask: "runtime#bench:websocket-firehose",
  });
};
