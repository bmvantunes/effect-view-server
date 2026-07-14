import {
  arrayValue,
  exactObjectValue,
  finiteNumber,
  mapByUniqueKey,
  nonNegativeInteger,
  objectValue,
  positiveInteger,
  stringValue,
} from "./benchmark-artifact-mechanics.mjs";

const processPeakRssMetric = "process-peak-over-initial-current";

const benchmarkKey = (benchmark) => `${benchmark.groupName} / ${benchmark.name}`;

const optionalFiniteNumber = (value, path) =>
  value === undefined ? undefined : finiteNumber(value, path);

export const decodeBenchmarkSamplingPolicy = (value, path) => {
  if (value === undefined) {
    return undefined;
  }
  const policy = exactObjectValue(value, path, [
    "iterationBoundCases",
    "memoryRssMetric",
    "measured",
  ]);
  const memoryRssMetric = stringValue(policy.memoryRssMetric, `${path}.memoryRssMetric`);
  if (memoryRssMetric !== processPeakRssMetric) {
    throw new Error(
      `Benchmark artifact field ${path}.memoryRssMetric must be ${processPeakRssMetric}.`,
    );
  }
  const measured = exactObjectValue(policy.measured, `${path}.measured`, [
    "minimumSampleCount",
    "timeMs",
    "warmupIterations",
    "warmupTimeMs",
  ]);
  const iterationBoundCases = arrayValue(
    policy.iterationBoundCases,
    `${path}.iterationBoundCases`,
  ).map((item, index) => {
    const itemPath = `${path}.iterationBoundCases[${index}]`;
    const iterationBoundCase = exactObjectValue(item, itemPath, [
      "name",
      "sampleCount",
      "timeMs",
      "warmupIterations",
      "warmupTimeMs",
    ]);
    const timeMs = nonNegativeInteger(iterationBoundCase.timeMs, `${itemPath}.timeMs`);
    const warmupIterations = nonNegativeInteger(
      iterationBoundCase.warmupIterations,
      `${itemPath}.warmupIterations`,
    );
    const warmupTimeMs = nonNegativeInteger(
      iterationBoundCase.warmupTimeMs,
      `${itemPath}.warmupTimeMs`,
    );
    if (timeMs !== 0 || warmupIterations !== 0 || warmupTimeMs !== 0) {
      throw new Error(
        `Benchmark artifact field ${itemPath} must disable time and warmup for an iteration-bound case.`,
      );
    }
    return {
      name: stringValue(iterationBoundCase.name, `${itemPath}.name`),
      sampleCount: positiveInteger(iterationBoundCase.sampleCount, `${itemPath}.sampleCount`),
      timeMs,
      warmupIterations,
      warmupTimeMs,
    };
  });
  mapByUniqueKey(iterationBoundCases, (item) => item.name, `${path}.iterationBoundCases`, "name");
  return {
    iterationBoundCases,
    memoryRssMetric,
    measured: {
      minimumSampleCount: positiveInteger(
        measured.minimumSampleCount,
        `${path}.measured.minimumSampleCount`,
      ),
      timeMs: nonNegativeInteger(measured.timeMs, `${path}.measured.timeMs`),
      warmupIterations: nonNegativeInteger(
        measured.warmupIterations,
        `${path}.measured.warmupIterations`,
      ),
      warmupTimeMs: nonNegativeInteger(
        measured.warmupTimeMs,
        `${path}.measured.warmupTimeMs`,
      ),
    },
  };
};

export const decodeProcessPeakRssTotalDeltaBytes = (memory, path) => {
  if (!("processPeakRss" in memory)) {
    throw new Error(
      `Benchmark artifact field ${path}.processPeakRss is required for ${processPeakRssMetric} RSS measurement.`,
    );
  }
  const peakPath = `${path}.processPeakRss`;
  const peak = exactObjectValue(memory.processPeakRss, peakPath, [
    "afterBenchmarkBytes",
    "afterSetupBytes",
    "beforeBytes",
    "benchmarkDeltaBytes",
    "setupDeltaBytes",
    "totalDeltaBytes",
  ]);
  const before = objectValue(memory.before, `${path}.before`);
  const initialCurrentRssBytes = nonNegativeInteger(
    before.rssBytes,
    `${path}.before.rssBytes`,
  );
  const beforeBytes = nonNegativeInteger(peak.beforeBytes, `${peakPath}.beforeBytes`);
  const afterSetupBytes = nonNegativeInteger(
    peak.afterSetupBytes,
    `${peakPath}.afterSetupBytes`,
  );
  const afterBenchmarkBytes = nonNegativeInteger(
    peak.afterBenchmarkBytes,
    `${peakPath}.afterBenchmarkBytes`,
  );
  if (
    beforeBytes < initialCurrentRssBytes ||
    afterSetupBytes < beforeBytes ||
    afterBenchmarkBytes < afterSetupBytes
  ) {
    throw new Error(`Benchmark artifact field ${peakPath} checkpoints must be monotonic.`);
  }
  const expectedBenchmarkDeltaBytes = afterBenchmarkBytes - afterSetupBytes;
  const expectedSetupDeltaBytes = afterSetupBytes - initialCurrentRssBytes;
  const expectedTotalDeltaBytes = afterBenchmarkBytes - initialCurrentRssBytes;
  const benchmarkDeltaBytes = nonNegativeInteger(
    peak.benchmarkDeltaBytes,
    `${peakPath}.benchmarkDeltaBytes`,
  );
  const setupDeltaBytes = nonNegativeInteger(peak.setupDeltaBytes, `${peakPath}.setupDeltaBytes`);
  const totalDeltaBytes = nonNegativeInteger(peak.totalDeltaBytes, `${peakPath}.totalDeltaBytes`);
  if (
    benchmarkDeltaBytes !== expectedBenchmarkDeltaBytes ||
    setupDeltaBytes !== expectedSetupDeltaBytes ||
    totalDeltaBytes !== expectedTotalDeltaBytes
  ) {
    throw new Error(`Benchmark artifact field ${peakPath} deltas must match its checkpoints.`);
  }
  if (totalDeltaBytes === 0) {
    throw new Error(`Benchmark artifact field ${peakPath}.totalDeltaBytes must be positive.`);
  }
  return totalDeltaBytes;
};

export const decodeBenchmarkMemoryRssTotalDeltaBytes = (memory, path, samplingPolicy) => {
  if (samplingPolicy !== undefined) {
    return decodeProcessPeakRssTotalDeltaBytes(memory, path);
  }
  if (!("totalDelta" in memory)) {
    return undefined;
  }
  const totalDelta = objectValue(memory.totalDelta, `${path}.totalDelta`);
  return optionalFiniteNumber(totalDelta.rssBytes, `${path}.totalDelta.rssBytes`);
};

export const validateBenchmarkSamplingPolicy = (
  samplingPolicy,
  benchmarks,
  minimumSampleCount,
  taskLabel,
) => {
  if (samplingPolicy === undefined) {
    return;
  }
  const iterationBoundByName = mapByUniqueKey(
    samplingPolicy.iterationBoundCases,
    (item) => item.name,
    `${taskLabel}.samplingPolicy.iterationBoundCases`,
    "name",
  );
  const minimumPolicySampleCount = Math.min(
    samplingPolicy.measured.minimumSampleCount,
    ...samplingPolicy.iterationBoundCases.map((item) => item.sampleCount),
  );
  if (minimumSampleCount !== minimumPolicySampleCount) {
    throw new Error(
      `${taskLabel}: minimumSampleCount must equal sampling policy minimum ${minimumPolicySampleCount} but was ${minimumSampleCount}.`,
    );
  }
  const benchmarkByName = mapByUniqueKey(
    benchmarks,
    (benchmark) => benchmark.name,
    `${taskLabel}.benchmarks`,
    "benchmark name",
  );
  for (const benchmark of benchmarks) {
    const iterationBoundCase = iterationBoundByName.get(benchmark.name);
    if (iterationBoundCase === undefined) {
      if (benchmark.sampleCount < samplingPolicy.measured.minimumSampleCount) {
        throw new Error(
          `${taskLabel} / ${benchmarkKey(benchmark)}: timed read sampleCount must be at least ${samplingPolicy.measured.minimumSampleCount} but was ${benchmark.sampleCount}.`,
        );
      }
    } else if (benchmark.sampleCount !== iterationBoundCase.sampleCount) {
      throw new Error(
        `${taskLabel} / ${benchmarkKey(benchmark)}: iteration-bound sampleCount must be exactly ${iterationBoundCase.sampleCount} but was ${benchmark.sampleCount}.`,
      );
    }
  }
  for (const iterationBoundCase of samplingPolicy.iterationBoundCases) {
    if (!benchmarkByName.has(iterationBoundCase.name)) {
      throw new Error(
        `${taskLabel}: missing iteration-bound benchmark case ${iterationBoundCase.name}.`,
      );
    }
  }
};

export const validateBenchmarkSamplingPolicyMemoryRssTotalDeltaBytes = (
  samplingPolicy,
  memoryRssTotalDeltaBytes,
  path,
) => {
  if (samplingPolicy !== undefined) {
    positiveInteger(memoryRssTotalDeltaBytes, path);
  }
};

export const samplingPolicyRequiresExactMutationCount = (samplingPolicy) =>
  (samplingPolicy?.iterationBoundCases.length ?? 0) > 0;
