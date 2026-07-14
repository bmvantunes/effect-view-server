import { describe, expect, it } from "@effect/vitest";
import {
  iterationBoundBenchmarkOptions,
  parseBenchmarkMemoryRssMetric,
  timedReadSamplingPolicy,
} from "./benchmark-sampling";

const measuredOptions = {
  iterations: 200,
  time: 250,
  warmupIterations: 5,
  warmupTime: 100,
};

describe("benchmark sampling policy", () => {
  it("preserves legacy benchmark options when policy sampling is disabled", () => {
    const liveOptions = iterationBoundBenchmarkOptions({
      fallbackOptions: measuredOptions,
      iterationCount: undefined,
    });

    expect({
      liveOptions,
      samplingPolicy: timedReadSamplingPolicy({
        iterationBoundCases: [{ name: "live case", options: liveOptions }],
        memoryRssMetric: undefined,
        measuredMinimumSampleCount: undefined,
        measuredOptions,
      }),
    }).toStrictEqual({
      liveOptions: measuredOptions,
      samplingPolicy: undefined,
    });
  });

  it("makes policy-owned mutation cases exactly iteration-bound", () => {
    const liveOptions = iterationBoundBenchmarkOptions({
      fallbackOptions: measuredOptions,
      iterationCount: 5,
    });

    expect({
      liveOptions,
      samplingPolicy: timedReadSamplingPolicy({
        iterationBoundCases: [{ name: "live case", options: liveOptions }],
        memoryRssMetric: "process-peak-over-initial-current",
        measuredMinimumSampleCount: 200,
        measuredOptions,
      }),
    }).toStrictEqual({
      liveOptions: {
        iterations: 5,
        time: 0,
        warmupIterations: 0,
        warmupTime: 0,
      },
      samplingPolicy: {
        iterationBoundCases: [
          {
            name: "live case",
            sampleCount: 5,
            timeMs: 0,
            warmupIterations: 0,
            warmupTimeMs: 0,
          },
        ],
        memoryRssMetric: "process-peak-over-initial-current",
        measured: {
          minimumSampleCount: 200,
          timeMs: 250,
          warmupIterations: 5,
          warmupTimeMs: 100,
        },
      },
    });
  });

  it("requires the peak RSS metric and timed read floor to be enabled together", () => {
    expect(() =>
      timedReadSamplingPolicy({
        iterationBoundCases: [],
        memoryRssMetric: undefined,
        measuredMinimumSampleCount: 200,
        measuredOptions,
      }),
    ).toThrow("Timed read sampling requires process-peak-over-initial-current RSS measurement.");
    expect(() =>
      timedReadSamplingPolicy({
        iterationBoundCases: [],
        memoryRssMetric: "process-peak-over-initial-current",
        measuredMinimumSampleCount: undefined,
        measuredOptions,
      }),
    ).toThrow("Peak RSS measurement requires timed read sampling.");
  });

  it("parses only the supported peak RSS metric", () => {
    expect(parseBenchmarkMemoryRssMetric(undefined)).toBeUndefined();
    expect(parseBenchmarkMemoryRssMetric("process-peak-over-initial-current")).toBe(
      "process-peak-over-initial-current",
    );
    expect(() => parseBenchmarkMemoryRssMetric("endpoint-rss")).toThrow(
      "VIEW_SERVER_ENGINE_BENCH_MEMORY_RSS_METRIC must be process-peak-over-initial-current.",
    );
  });
});
