import { expectTypeOf } from "@effect/vitest";
import type {
  BenchmarkArtifactInput,
  BenchmarkArtifactMeasurementInput,
  BenchmarkMeasurementProtocol,
} from "./benchmark-artifact";

type RawLargeMembershipArtifactInput = Extract<
  BenchmarkArtifactInput,
  { readonly benchmarkScope: "engine-raw-large-membership" }
>;
type RawSnapshotArtifactInput = Extract<
  BenchmarkArtifactInput,
  { readonly benchmarkScope: "engine-raw-snapshot" }
>;

declare const rawLargeMembershipWithoutParameters: Omit<
  RawLargeMembershipArtifactInput,
  "rawLargeMembershipParameters"
>;
declare const rawSnapshotArtifact: RawSnapshotArtifactInput;

// @ts-expect-error The large-membership benchmark must declare its exact workload parameters.
const missingRawLargeMembershipParameters: RawLargeMembershipArtifactInput = {
  ...rawLargeMembershipWithoutParameters,
};
const rawSnapshotWithLargeMembershipParameters: RawSnapshotArtifactInput = {
  ...rawSnapshotArtifact,
  // @ts-expect-error Other benchmark scopes cannot claim large-membership workload metadata.
  rawLargeMembershipParameters: {
    candidateCount: 50_000,
    partitionCount: 25,
    preparedPlanCompilationCount: 1,
    subscriberCount: 32,
  },
};

const memoryCheckpointProtocol: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-after-cleanup",
};
const primingProtocol: BenchmarkMeasurementProtocol = {
  priming: "append-delete-restore-before-sampling",
};
const combinedProtocol: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
  postGcEventLoopTurns: 8,
  priming: "append-delete-restore-before-sampling",
};
const postGcProtocol: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
  postGcEventLoopTurns: 8,
};
const postGcArtifactMeasurement: BenchmarkArtifactMeasurementInput = {
  measurementProtocol: postGcProtocol,
  postGcEventLoopSamples: [],
};
const primingArtifactMeasurement: BenchmarkArtifactMeasurementInput = {
  measurementProtocol: {
    priming: "append-delete-restore-before-sampling",
  },
};

expectTypeOf(memoryCheckpointProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(primingProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(combinedProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcArtifactMeasurement).toMatchTypeOf<BenchmarkArtifactMeasurementInput>();
expectTypeOf(primingArtifactMeasurement).toMatchTypeOf<BenchmarkArtifactMeasurementInput>();

// @ts-expect-error A measurement protocol must contain at least one supported protocol field.
const emptyProtocol: BenchmarkMeasurementProtocol = {};

// @ts-expect-error Explicit undefined does not satisfy the required memory checkpoint value.
const undefinedMemoryCheckpoint: BenchmarkMeasurementProtocol = { memoryCheckpoint: undefined };

// @ts-expect-error Explicit undefined does not satisfy the required priming value.
const undefinedPriming: BenchmarkMeasurementProtocol = { priming: undefined };

// @ts-expect-error Post-GC turns require the explicit-GC memory checkpoint protocol.
const postGcTurnsWithoutMemoryCheckpoint: BenchmarkMeasurementProtocol = {
  postGcEventLoopTurns: 8,
  priming: "append-delete-restore-before-sampling",
};

const postGcTurnsWithOriginalMemoryCheckpoint: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-after-cleanup",
  // @ts-expect-error The original explicit-GC protocol does not include post-GC event-loop turns.
  postGcEventLoopTurns: 8,
};

// @ts-expect-error The settled post-GC protocol requires its fixed event-loop turn count.
const postGcCheckpointWithoutTurns: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
};

const postGcCheckpointWithWrongTurns: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-plus-post-gc-turns-after-cleanup",
  // @ts-expect-error The settled post-GC protocol requires exactly eight event-loop turns.
  postGcEventLoopTurns: 7,
};

// @ts-expect-error A post-GC checkpoint protocol must include its diagnostic samples.
const postGcArtifactWithoutSamples: BenchmarkArtifactMeasurementInput = {
  measurementProtocol: postGcProtocol,
};

// @ts-expect-error Post-GC diagnostic samples require the post-GC checkpoint protocol.
const postGcSamplesWithoutProtocol: BenchmarkArtifactMeasurementInput = {
  postGcEventLoopSamples: [],
};

// @ts-expect-error The original explicit-GC checkpoint does not emit post-GC samples.
const postGcSamplesWithOriginalCheckpoint: BenchmarkArtifactMeasurementInput = {
  measurementProtocol: {
    memoryCheckpoint: "settled-explicit-gc-after-cleanup",
  },
  postGcEventLoopSamples: [],
};

expectTypeOf(emptyProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(undefinedMemoryCheckpoint).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(undefinedPriming).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcTurnsWithoutMemoryCheckpoint).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcTurnsWithOriginalMemoryCheckpoint).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcCheckpointWithoutTurns).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcCheckpointWithWrongTurns).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(postGcArtifactWithoutSamples).toMatchTypeOf<BenchmarkArtifactMeasurementInput>();
expectTypeOf(postGcSamplesWithoutProtocol).toMatchTypeOf<BenchmarkArtifactMeasurementInput>();
expectTypeOf(
  postGcSamplesWithOriginalCheckpoint,
).toMatchTypeOf<BenchmarkArtifactMeasurementInput>();
expectTypeOf(missingRawLargeMembershipParameters).toMatchTypeOf<RawLargeMembershipArtifactInput>();
expectTypeOf(rawSnapshotWithLargeMembershipParameters).toMatchTypeOf<RawSnapshotArtifactInput>();
