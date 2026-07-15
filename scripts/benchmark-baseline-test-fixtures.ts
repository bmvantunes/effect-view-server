export const vitestOutput = {
  files: [
    {
      groups: [
        {
          fullName: "src/example.bench.ts > example benchmark group",
          benchmarks: [
            {
              max: 3,
              mean: 2,
              min: 1,
              name: "case a",
              p99: 3,
              sampleCount: 7,
            },
          ],
        },
      ],
    },
  ],
};

export const summary = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "example benchmark",
  benchmarkScope: "engine-raw-snapshot",
  browser: undefined,
  cleanupLeakCount: 0,
  groupedKeyWidthParameters: undefined,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  latency: {
    outputJsonPath: "actual.json",
    source: "vitest-output-json",
  },
  memory: {
    totalDelta: {
      rssBytes: 1024,
    },
  },
  mutationCount: 100,
  queuedEventCount: 0,
  rowCount: 100,
  subscriberCount: 1,
  topics: ["orders"],
};

export const runtimeHealth = {
  engine: {
    topics: {
      orders: {
        rowCount: 100,
      },
    },
  },
  kafka: {
    topics: {
      sourceOrders: {
        regions: {
          local: {
            committedOffset: "100",
          },
        },
        viewServerTopic: "orders",
      },
    },
  },
};

export const runtimeKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: 100,
    region: "local",
    sourceTopic: "sourceOrders",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

export const comparableRuntimeKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: 100,
    region: "local",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

export const runtimeThroughputMutationCount = 700;

export const runtimeThroughputHealth = {
  engine: {
    topics: {
      orders: {
        rowCount: runtimeThroughputMutationCount,
      },
    },
  },
  kafka: {
    topics: {
      sourceOrders: {
        regions: {
          local: {
            committedOffset: String(runtimeThroughputMutationCount),
          },
        },
        viewServerTopic: "orders",
      },
    },
  },
};

export const runtimeThroughputKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: runtimeThroughputMutationCount,
    region: "local",
    sourceTopic: "sourceOrders",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

export const comparableRuntimeThroughputKafkaIngestLanes = [
  {
    internalTopic: "orders",
    lane: "orders",
    producedRows: runtimeThroughputMutationCount,
    region: "local",
    sourceTopicAlias: "unique-topic-per-run:orders",
  },
];

export const runtimeThroughput = {
  source: "benchmark-operation-timers",
  cases: [
    {
      aggregateRowsPerSecond: 1000,
      maxCommitObservedMs: 80,
      maxReadSnapshotMs: 6,
      maxTotalMs: 100,
      meanCommitObservedMs: 75,
      meanConvergenceMs: 75,
      meanProducerSendMs: 25,
      meanReadSnapshotMs: 5,
      meanRowsPerSecond: 1000,
      meanTotalMs: 100,
      minRowsPerSecond: 900,
      name: "case a",
      producedRowsPerSample: 100,
      readSnapshotRowsPerSample: 25,
      sampleCount: 7,
      totalProducedRows: 700,
    },
  ],
};

export const comparableRuntimeThroughputCases = runtimeThroughput.cases;

export const rawRuntimeMetrics = {
  eventLoopDelay: {
    maxMs: 4,
    meanMs: 2,
    p99Ms: 3,
  },
  healthPolling: {
    count: 11,
    maxMs: 2,
    totalMs: 7,
  },
  kafkaLag: {
    maxConsumerLagMessages: "9007199254740993",
    sampledRegionCount: 1,
    totalConsumerLagMessages: "9007199254740993",
  },
};

export const runtimeMetrics = rawRuntimeMetrics;

export const drainedRuntimeMetrics = {
  ...runtimeMetrics,
  kafkaLag: {
    maxConsumerLagMessages: "0",
    sampledRegionCount: 1,
    totalConsumerLagMessages: "0",
  },
};

export const comparableNonKafkaRuntimeThroughputCases = [
  {
    aggregateRowsPerSecond: 1000,
    maxTotalMs: 100,
    meanConvergenceMs: 75,
    meanProducerSendMs: 25,
    meanRowsPerSecond: 1000,
    meanTotalMs: 100,
    minRowsPerSecond: 900,
    name: "case a",
    producedRowsPerSample: 100,
    sampleCount: 7,
    totalProducedRows: 700,
  },
];

export const runtimeGrpcLeasedSample = {
  acquiredFeedCount: 1,
  activeLeasedFeeds: 1,
  backpressureCount: 0,
  cleanupActiveLeasedFeeds: 0,
  cleanupClientActiveFeeds: 0,
  cleanupLeakCount: 0,
  cleanupMs: 2,
  cleanupRowCount: 0,
  deltaFanoutMs: 4,
  healthOverlayMs: 1,
  measurementRowCount: 50,
  measuredCleanup: {
    activeLeasedFeeds: 0,
    activeSubscriptions: 0,
    activeViews: 0,
    clientActiveFeeds: 0,
    leakCount: 0,
    queuedEvents: 0,
    rowCount: 0,
  },
  mutationCount: 50,
  name: "gRPC leased first subscriber",
  queuedEventCount: 0,
  releasedFeedCount: 1,
  rows: 50,
  rowsPerSecond: 1000,
  seedMutationCount: 0,
  snapshotMs: 50,
  subscriberCount: 1,
  subscriptionMs: 3,
};

export const runtimeGrpcLeasedOperationCaseFor = (
  sample: typeof runtimeGrpcLeasedSample,
  sampleCount = 7,
) => ({
  maxActiveLeasedFeeds: sample.activeLeasedFeeds,
  maxCleanupActiveLeasedFeeds: sample.cleanupActiveLeasedFeeds,
  maxCleanupClientActiveFeeds: sample.cleanupClientActiveFeeds,
  maxCleanupMs: sample.cleanupMs,
  maxDeltaFanoutMs: sample.deltaFanoutMs,
  maxHealthOverlayMs: sample.healthOverlayMs,
  maxMeasuredCleanupActiveLeasedFeeds: sample.measuredCleanup.activeLeasedFeeds,
  maxMeasuredCleanupActiveSubscriptions: sample.measuredCleanup.activeSubscriptions,
  maxMeasuredCleanupActiveViews: sample.measuredCleanup.activeViews,
  maxMeasuredCleanupClientActiveFeeds: sample.measuredCleanup.clientActiveFeeds,
  maxMeasuredCleanupLeakCount: sample.measuredCleanup.leakCount,
  maxMeasuredCleanupQueuedEvents: sample.measuredCleanup.queuedEvents,
  maxMeasuredCleanupRowCount: sample.measuredCleanup.rowCount,
  maxSnapshotMs: sample.snapshotMs,
  maxSubscriptionMs: sample.subscriptionMs,
  meanCleanupMs: sample.cleanupMs,
  meanDeltaFanoutMs: sample.deltaFanoutMs,
  meanHealthOverlayMs: sample.healthOverlayMs,
  meanRowsPerSecond: sample.rowsPerSecond,
  meanSnapshotMs: sample.snapshotMs,
  meanSubscriptionMs: sample.subscriptionMs,
  medianRowsPerSecond: sample.rowsPerSecond,
  mutationCount: sample.mutationCount * sampleCount,
  name: sample.name,
  pooledRowsPerSecond: sample.rowsPerSecond,
  rowsPerSecondCoefficientOfVariation: 0,
  sampleCount,
  samples: Array.from({ length: sampleCount }, () => ({ ...sample })),
  seedMutationCount: sample.seedMutationCount * sampleCount,
});

export const runtimeGrpcLeasedOperationCase = runtimeGrpcLeasedOperationCaseFor(
  runtimeGrpcLeasedSample,
);

export const runtimeGrpcLeasedReuseOperationCase = runtimeGrpcLeasedOperationCaseFor({
  ...runtimeGrpcLeasedSample,
  name: "gRPC leased same-route reuse",
  subscriberCount: 10,
});

export const runtimeGrpcLeasedOperationCases = [
  runtimeGrpcLeasedOperationCase,
  runtimeGrpcLeasedReuseOperationCase,
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    name: "gRPC leased one route many subscribers",
    subscriberCount: 50,
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    name: "gRPC leased local-filter live snapshot",
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    measurementRowCount: 500,
    mutationCount: 0,
    name: "gRPC leased retained local-filter snapshot",
    rows: 500,
    seedMutationCount: 500,
    snapshotMs: 500,
    subscriberCount: 2,
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    measurementRowCount: 51,
    name: "gRPC leased delta fanout",
    seedMutationCount: 1,
    subscriberCount: 25,
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    acquiredFeedCount: 25,
    activeLeasedFeeds: 25,
    measurementRowCount: 1_250,
    mutationCount: 1_250,
    name: "gRPC leased partitioned write convergence",
    releasedFeedCount: 25,
    rows: 1_250,
    snapshotMs: 1_250,
    subscriberCount: 25,
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    acquiredFeedCount: 25,
    activeLeasedFeeds: 25,
    measurementRowCount: 25,
    mutationCount: 0,
    name: "gRPC leased health refresh overhead",
    releasedFeedCount: 25,
    rows: 0,
    rowsPerSecond: 0,
    seedMutationCount: 25,
    snapshotMs: 0,
    subscriberCount: 25,
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    name: "gRPC leased last-subscriber cleanup",
  }),
  runtimeGrpcLeasedOperationCaseFor({
    ...runtimeGrpcLeasedSample,
    acquiredFeedCount: 25,
    activeLeasedFeeds: 25,
    measurementRowCount: 25,
    mutationCount: 25,
    name: "gRPC leased many routes",
    releasedFeedCount: 25,
    rows: 25,
    snapshotMs: 25,
    subscriberCount: 25,
  }),
];

export const replaceGrpcLeasedOperationCase = (
  replacement: typeof runtimeGrpcLeasedOperationCase,
) =>
  runtimeGrpcLeasedOperationCases.map((operationCase) =>
    operationCase.name === replacement.name ? replacement : operationCase,
  );

export const runtimeGrpcMaterializedParameters = {
  batchSize: 100,
  seedRows: 1000,
};

export const runtimeGrpcMaterializedSampleCount = 7;

export const runtimeGrpcMaterializedSampleFor = (
  parameters: typeof runtimeGrpcMaterializedParameters,
  name: string,
  rows: number,
  resultRowId: string | null,
) => ({
  backpressureCount: 0,
  cleanupLeakCount: 0,
  cleanupMs: 2,
  healthOverlayMs: 1,
  name,
  queuedEventCount: 0,
  resultRowId,
  rows,
  rowsPerSecond: rows * 10,
  seedRows: parameters.seedRows,
  snapshotMs: rows === 0 ? 0 : 7,
  startTotalRows: parameters.seedRows,
  streamConvergenceMs: rows === 0 ? 0 : 100,
  totalRows: parameters.seedRows + rows,
});

export const runtimeGrpcMaterializedOperationCaseFor = (
  sample: ReturnType<typeof runtimeGrpcMaterializedSampleFor>,
  sampleCount = runtimeGrpcMaterializedSampleCount,
) => ({
  maxCleanupMs: sample.cleanupMs,
  maxHealthOverlayMs: sample.healthOverlayMs,
  maxSnapshotMs: sample.snapshotMs,
  maxStreamConvergenceMs: sample.streamConvergenceMs,
  meanCleanupMs: sample.cleanupMs,
  meanHealthOverlayMs: sample.healthOverlayMs,
  meanRowsPerSecond: sample.rowsPerSecond,
  meanSnapshotMs: sample.snapshotMs,
  meanStreamConvergenceMs: sample.streamConvergenceMs,
  medianRowsPerSecond: sample.rowsPerSecond,
  mutationCount: sample.rows * sampleCount,
  name: sample.name,
  pooledRowsPerSecond: sample.rowsPerSecond,
  rowsPerSecondCoefficientOfVariation: 0,
  sampleCount,
  samples: Array.from({ length: sampleCount }, () => ({ ...sample })),
  seedMutationCount: sample.seedRows * sampleCount,
  startTotalRows: sample.startTotalRows,
  totalRows: sample.totalRows,
});

export const runtimeGrpcMaterializedOperationCasesFor = (
  parameters: typeof runtimeGrpcMaterializedParameters,
) => [
  runtimeGrpcMaterializedOperationCaseFor(
    runtimeGrpcMaterializedSampleFor(
      parameters,
      "gRPC materialized stream batch",
      parameters.batchSize,
      `order-${parameters.seedRows + parameters.batchSize - 1}`,
    ),
  ),
  runtimeGrpcMaterializedOperationCaseFor(
    runtimeGrpcMaterializedSampleFor(
      parameters,
      "gRPC materialized burst",
      parameters.batchSize * 4,
      `order-${parameters.seedRows + parameters.batchSize * 4 - 1}`,
    ),
  ),
  runtimeGrpcMaterializedOperationCaseFor(
    runtimeGrpcMaterializedSampleFor(
      parameters,
      "gRPC materialized health overlay",
      0,
      null,
    ),
  ),
];

export const runtimeGrpcMaterializedStreamSample = runtimeGrpcMaterializedSampleFor(
  runtimeGrpcMaterializedParameters,
  "gRPC materialized stream batch",
  runtimeGrpcMaterializedParameters.batchSize,
  "order-1099",
);

export const runtimeGrpcMaterializedHealthSample = runtimeGrpcMaterializedSampleFor(
  runtimeGrpcMaterializedParameters,
  "gRPC materialized health overlay",
  0,
  null,
);

export const runtimeGrpcMaterializedOperationCase = runtimeGrpcMaterializedOperationCaseFor(
  runtimeGrpcMaterializedStreamSample,
);

export const runtimeGrpcMaterializedBurstOperationCase = runtimeGrpcMaterializedOperationCaseFor(
  runtimeGrpcMaterializedSampleFor(
    runtimeGrpcMaterializedParameters,
    "gRPC materialized burst",
    runtimeGrpcMaterializedParameters.batchSize * 4,
    "order-1399",
  ),
);

export const runtimeGrpcMaterializedHealthOperationCase = runtimeGrpcMaterializedOperationCaseFor(
  runtimeGrpcMaterializedHealthSample,
);

export const runtimeGrpcMaterializedOperationCases = [
  runtimeGrpcMaterializedOperationCase,
  runtimeGrpcMaterializedBurstOperationCase,
  runtimeGrpcMaterializedHealthOperationCase,
];

export const runtimeGrpcMaterializedComparisonState = (
  operationCases: ReadonlyArray<typeof runtimeGrpcMaterializedOperationCase>,
  caseName: string,
) =>
  operationCases
    .filter((operationCase) => operationCase.name === caseName)
    .flatMap((operationCase) =>
      operationCase.samples.map((sample) => ({
        backpressureCount: sample.backpressureCount,
        cleanupLeakCount: sample.cleanupLeakCount,
        name: sample.name,
        queuedEventCount: sample.queuedEventCount,
        resultRowId: sample.resultRowId,
        rows: sample.rows,
        seedRows: sample.seedRows,
        startTotalRows: sample.startTotalRows,
        totalRows: sample.totalRows,
      })),
    );

export const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarks: [
    {
      groupName: "src/example.bench.ts > example benchmark group",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "case a",
      p99Ms: 3,
      sampleCount: 7,
    },
  ],
  benchmarkCases: ["case a"],
  benchmarkName: "example benchmark",
  benchmarkScope: "engine-raw-snapshot",
  browser: undefined,
  cleanupLeakCount: 0,
  groupedKeyWidthParameters: undefined,
  groupedWriteAdmission: {
    configuredMode: "incremental",
    expectedAdmission: "incremental",
  },
  kafkaIngestLanes: undefined,
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  throughputCases: undefined,
  topics: ["orders"],
};

export const grpcLeasedObservationFor = (
  operationCases: ReadonlyArray<typeof runtimeGrpcLeasedOperationCase>,
) => ({
  ...observation,
  benchmarks: operationCases.map((operationCase) => ({
    ...observation.benchmarks[0],
    name: operationCase.name,
    sampleCount: operationCase.sampleCount,
  })),
  benchmarkCases: operationCases.map((operationCase) => operationCase.name),
  benchmarkScope: "runtime-grpc-leased",
  mutationCount: operationCases.reduce(
    (total, operationCase) => total + operationCase.mutationCount,
    0,
  ),
  seedMutationCount: operationCases.reduce(
    (total, operationCase) => total + operationCase.seedMutationCount,
    0,
  ),
});

export const grpcLeasedObservation = grpcLeasedObservationFor([runtimeGrpcLeasedOperationCase]);
export const completeGrpcLeasedObservation = grpcLeasedObservationFor(
  runtimeGrpcLeasedOperationCases,
);

export const runtimeGrpcMaterializedObservationFor = (
  operationCases: ReadonlyArray<typeof runtimeGrpcMaterializedOperationCase>,
) => ({
  ...observation,
  benchmarks: operationCases.map((operationCase) => ({
    ...observation.benchmarks[0],
    name: operationCase.name,
    sampleCount: operationCase.sampleCount,
  })),
  benchmarkCases: operationCases.map((operationCase) => operationCase.name),
  benchmarkScope: "runtime-grpc-materialized",
  mutationCount: operationCases.reduce(
    (total, operationCase) => total + operationCase.mutationCount,
    0,
  ),
  seedMutationCount: operationCases.reduce(
    (total, operationCase) => total + operationCase.seedMutationCount,
    0,
  ),
});

export const replaceGrpcMaterializedOperationCase = (
  replacement: typeof runtimeGrpcMaterializedOperationCase,
) =>
  runtimeGrpcMaterializedOperationCases.map((operationCase) =>
    operationCase.name === replacement.name ? replacement : operationCase,
  );

export const runtimeGrpcMaterializedObservation = runtimeGrpcMaterializedObservationFor(
  runtimeGrpcMaterializedOperationCases,
);

export const runtimeGrpcMaterializedVitestOutput = {
  files: [
    {
      groups: [
        {
          fullName: "src/example.bench.ts > example benchmark group",
          benchmarks: runtimeGrpcMaterializedOperationCases.map((operationCase) => ({
            max: 3,
            mean: 2,
            min: 1,
            name: operationCase.name,
            p99: 3,
            sampleCount: operationCase.sampleCount,
          })),
        },
      ],
    },
  ],
};

export const taskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "engine-benchmark-summary",
  expectedBenchmarkScope: "engine-raw-snapshot",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath,
  packageOutputJsonPath: "actual.json",
  summaryPath,
});

export const browserTaskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "react-browser-benchmark-summary",
  expectedBenchmarkScope: "react-in-memory-live-query",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath,
  packageOutputJsonPath: "actual.json",
  summaryPath,
});

export const runtimeTaskPaths = (summaryPath: string, outputJsonPath: string) => ({
  expectedArtifactKind: "runtime-benchmark-summary",
  expectedBenchmarkScope: "runtime-kafka-ingest",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath,
  packageOutputJsonPath: "actual.json",
  summaryPath,
});
