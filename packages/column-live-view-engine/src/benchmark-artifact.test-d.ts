import { expectTypeOf } from "@effect/vitest";
import type { BenchmarkMeasurementProtocol } from "./benchmark-artifact";

const memoryCheckpointProtocol: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-after-cleanup",
};
const primingProtocol: BenchmarkMeasurementProtocol = {
  priming: "append-delete-restore-before-sampling",
};
const combinedProtocol: BenchmarkMeasurementProtocol = {
  memoryCheckpoint: "settled-explicit-gc-after-cleanup",
  priming: "append-delete-restore-before-sampling",
};

expectTypeOf(memoryCheckpointProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(primingProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(combinedProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();

// @ts-expect-error A measurement protocol must contain at least one supported protocol field.
const emptyProtocol: BenchmarkMeasurementProtocol = {};

// @ts-expect-error Explicit undefined does not satisfy the required memory checkpoint value.
const undefinedMemoryCheckpoint: BenchmarkMeasurementProtocol = { memoryCheckpoint: undefined };

// @ts-expect-error Explicit undefined does not satisfy the required priming value.
const undefinedPriming: BenchmarkMeasurementProtocol = { priming: undefined };

expectTypeOf(emptyProtocol).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(undefinedMemoryCheckpoint).toMatchTypeOf<BenchmarkMeasurementProtocol>();
expectTypeOf(undefinedPriming).toMatchTypeOf<BenchmarkMeasurementProtocol>();
