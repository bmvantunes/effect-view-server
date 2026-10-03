import { expectTypeOf, it } from "@effect/vitest";
import type {
  BenchmarkArtifactMemoryInput,
  BenchmarkMemorySnapshot,
  BenchmarkProcessPeakRss,
} from "./benchmark-memory-recorder";
import type { BenchmarkSamplingPolicy } from "./benchmark-sampling";

declare const memoryBefore: BenchmarkMemorySnapshot;
declare const memoryAfterSetup: BenchmarkMemorySnapshot;
declare const memoryAfterBenchmark: BenchmarkMemorySnapshot;
declare const processPeakRss: BenchmarkProcessPeakRss;
declare const samplingPolicy: BenchmarkSamplingPolicy;

const endpointMemory: BenchmarkArtifactMemoryInput = {
  memoryAfterBenchmark,
  memoryAfterSetup,
  memoryBefore,
};
const peakMemory: BenchmarkArtifactMemoryInput = {
  memoryAfterBenchmark,
  memoryAfterSetup,
  memoryBefore,
  processPeakRss,
  samplingPolicy,
};

// @ts-expect-error A sampling policy must carry its matching process-peak checkpoints.
const policyWithoutPeak: BenchmarkArtifactMemoryInput = {
  memoryAfterBenchmark,
  memoryAfterSetup,
  memoryBefore,
  samplingPolicy,
};

// @ts-expect-error Process-peak checkpoints cannot be emitted without their sampling policy.
const peakWithoutPolicy: BenchmarkArtifactMemoryInput = {
  memoryAfterBenchmark,
  memoryAfterSetup,
  memoryBefore,
  processPeakRss,
};

it("types benchmark memory artifact inputs", () => {
  expectTypeOf(endpointMemory).toMatchTypeOf<BenchmarkArtifactMemoryInput>();
  expectTypeOf(peakMemory).toMatchTypeOf<BenchmarkArtifactMemoryInput>();
  expectTypeOf(policyWithoutPeak).toMatchTypeOf<BenchmarkArtifactMemoryInput>();
  expectTypeOf(peakWithoutPolicy).toMatchTypeOf<BenchmarkArtifactMemoryInput>();
});
