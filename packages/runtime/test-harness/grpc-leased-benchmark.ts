import { Cause, Effect, Exit, Ref } from "effect";

import {
  GrpcLeasedBenchmarkSampleError,
  grpcLeasedBenchmarkCases,
  summarizeGrpcLeasedBenchmarkSamples,
  type GrpcLeasedBenchmarkOptions,
  type GrpcLeasedBenchmarkSampleEvidence,
  type GrpcLeasedBenchmarkWorkload,
  type GrpcLeasedMeasuredCleanupEvidence,
} from "./grpc-leased-benchmark-model";
import {
  acquireGrpcLeasedBenchmarkContext,
  cleanupGrpcLeasedBenchmarkContext,
  type GrpcLeasedBenchmarkCleanup,
} from "./grpc-leased-benchmark-runtime";
import { runGrpcLeasedBenchmarkWorkload } from "./grpc-leased-benchmark-workloads";

export type GrpcLeasedBenchmarkSample = GrpcLeasedBenchmarkSampleEvidence & {
  readonly health: GrpcLeasedBenchmarkCleanup["health"];
};

export {
  GrpcLeasedBenchmarkSampleError,
  grpcLeasedBenchmarkCases,
  summarizeGrpcLeasedBenchmarkSamples,
};
export type {
  GrpcLeasedBenchmarkOptions,
  GrpcLeasedBenchmarkSampleEvidence,
  GrpcLeasedBenchmarkWorkload,
  GrpcLeasedMeasuredCleanupEvidence,
};

export const runGrpcLeasedBenchmarkSample = Effect.fn(
  "ViewServerRuntime.grpc.leased.bench.sample.run",
)(function* (workload: GrpcLeasedBenchmarkWorkload, options: GrpcLeasedBenchmarkOptions) {
  const cleanup = yield* Ref.make<GrpcLeasedBenchmarkCleanup | undefined>(undefined);
  const cleanupFailure = yield* Ref.make<Cause.Cause<unknown> | undefined>(undefined);
  const measurementExit = yield* Effect.exit(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Effect.acquireRelease(
          acquireGrpcLeasedBenchmarkContext(options),
          (currentContext) =>
            cleanupGrpcLeasedBenchmarkContext(currentContext).pipe(
              Effect.matchCauseEffect({
                onFailure: (cause) => Ref.set(cleanupFailure, cause),
                onSuccess: (evidence) => Ref.set(cleanup, evidence),
              }),
            ),
        );
        return yield* runGrpcLeasedBenchmarkWorkload(workload, context);
      }),
    ),
  );
  const cleanupEvidence = yield* Ref.get(cleanup);
  const cleanupCause = yield* Ref.get(cleanupFailure);
  if (Exit.isFailure(measurementExit) && Cause.hasInterruptsOnly(measurementExit.cause)) {
    return yield* Effect.interrupt;
  }
  if (cleanupCause !== undefined && Cause.hasInterruptsOnly(cleanupCause)) {
    return yield* Effect.interrupt;
  }
  if (
    Exit.isFailure(measurementExit) ||
    cleanupCause !== undefined ||
    cleanupEvidence === undefined
  ) {
    const cleanupFailureMessage =
      cleanupCause === undefined
        ? "gRPC leased benchmark cleanup evidence was not recorded."
        : Cause.pretty(cleanupCause);
    return yield* new GrpcLeasedBenchmarkSampleError({
      acquiredFeedCount: cleanupEvidence?.acquiredFeedCount ?? 0,
      cleanupActiveLeasedFeeds: cleanupEvidence?.cleanupActiveLeasedFeeds ?? 0,
      cleanupClientActiveFeeds: cleanupEvidence?.cleanupClientActiveFeeds ?? 0,
      cleanupLeakCount: cleanupEvidence?.cleanupLeakCount ?? 0,
      cleanupObserved: cleanupEvidence !== undefined,
      cleanupRowCount: cleanupEvidence?.cleanupRowCount ?? 0,
      message:
        Exit.isFailure(measurementExit) && cleanupCause !== undefined
          ? `${Cause.pretty(measurementExit.cause)}\nCleanup: ${Cause.pretty(cleanupCause)}`
          : Exit.isFailure(measurementExit)
            ? Cause.pretty(measurementExit.cause)
            : cleanupFailureMessage,
      releasedFeedCount: cleanupEvidence?.releasedFeedCount ?? 0,
      workload,
    });
  }
  return {
    ...measurementExit.value,
    ...cleanupEvidence,
  } satisfies GrpcLeasedBenchmarkSample;
});
