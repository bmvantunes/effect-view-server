export const finiteNumber = (value, path) => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Benchmark artifact field ${path} must be a finite number.`);
  }
  return value;
};

export const stringValue = (value, path) => {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-empty string.`);
  }
  return value;
};

export const objectValue = (value, path) => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Benchmark artifact field ${path} must be an object.`);
  }
  return value;
};

export const exactObjectValue = (value, path, expectedKeys) => {
  const object = objectValue(value, path);
  const actualKeys = Object.keys(object).sort();
  const sortedExpectedKeys = [...expectedKeys].sort();
  if (JSON.stringify(actualKeys) !== JSON.stringify(sortedExpectedKeys)) {
    throw new Error(
      `Benchmark artifact field ${path} must contain exactly these keys: ${sortedExpectedKeys.join(", ")}.`,
    );
  }
  return object;
};

export const arrayValue = (value, path) => {
  if (!Array.isArray(value)) {
    throw new Error(`Benchmark artifact field ${path} must be an array.`);
  }
  return value;
};

export const nonEmptyArrayValue = (value, path) => {
  const array = arrayValue(value, path);
  if (array.length === 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-empty array.`);
  }
  return array;
};

export const positiveFiniteNumber = (value, path) => {
  const number = finiteNumber(value, path);
  if (number <= 0) {
    throw new Error(`Benchmark artifact field ${path} must be a positive finite number.`);
  }
  return number;
};

export const nonNegativeFiniteNumber = (value, path) => {
  const number = finiteNumber(value, path);
  if (number < 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-negative finite number.`);
  }
  return number;
};

export const positiveInteger = (value, path) => {
  const number = finiteNumber(value, path);
  if (!Number.isInteger(number) || number <= 0) {
    throw new Error(`Benchmark artifact field ${path} must be a positive integer.`);
  }
  return number;
};

export const nonNegativeInteger = (value, path) => {
  const number = finiteNumber(value, path);
  if (!Number.isInteger(number) || number < 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-negative integer.`);
  }
  return number;
};

export const mapByUniqueKey = (values, key, path, label) => {
  const entries = [];
  const seen = new Set();
  for (const value of values) {
    const valueKey = key(value);
    if (seen.has(valueKey)) {
      throw new Error(`Benchmark artifact field ${path} contains duplicate ${label}: ${valueKey}.`);
    }
    seen.add(valueKey);
    entries.push([valueKey, value]);
  }
  return new Map(entries);
};

export const pushRegression = (regressions, message) => {
  regressions.push(message);
};

export const compareExact = (regressions, taskLabel, name, baseline, actual) => {
  if (actual !== baseline) {
    pushRegression(regressions, `${taskLabel}: ${name} changed from ${baseline} to ${actual}.`);
  }
};

export const compareExactJson = (regressions, taskLabel, name, baseline, actual) => {
  const baselineJson = JSON.stringify(baseline);
  const actualJson = JSON.stringify(actual);
  if (actualJson !== baselineJson) {
    pushRegression(
      regressions,
      `${taskLabel}: ${name} changed from ${baselineJson} to ${actualJson}.`,
    );
  }
};

export const compareLatency = (
  regressions,
  taskLabel,
  benchmarkName,
  metricName,
  threshold,
  baseline,
  actual,
) => {
  const limit = Math.max(
    baseline * threshold.maxRatio,
    baseline + threshold.maxAbsoluteDeltaMs,
  );
  if (actual > limit) {
    pushRegression(
      regressions,
      `${taskLabel} / ${benchmarkName}: ${metricName} regressed from ${baseline.toFixed(
        3,
      )}ms to ${actual.toFixed(3)}ms; allowed <= ${limit.toFixed(3)}ms.`,
    );
  }
};

export const compareThroughput = (
  regressions,
  taskLabel,
  caseName,
  metricName,
  threshold,
  baseline,
  actual,
) => {
  if (baseline === 0) {
    compareExact(regressions, taskLabel, `${caseName} ${metricName}`, baseline, actual);
    return;
  }
  const minimum = baseline * threshold.minRatio;
  if (actual < minimum) {
    pushRegression(
      regressions,
      `${taskLabel} / ${caseName}: ${metricName} throughput regressed from ${baseline.toFixed(
        3,
      )} rows/sec to ${actual.toFixed(3)} rows/sec; allowed >= ${minimum.toFixed(3)} rows/sec.`,
    );
  }
};
