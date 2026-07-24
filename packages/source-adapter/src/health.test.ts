import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import {
  SourceBufferMetricsSchema,
  SourceLaneRuntimeMetricsSchema,
  SourceRuntimeMetricsSchema,
  sourceHealthSchema,
  sourceRejectionDiagnosticSchema,
  sourceStatusSchema,
  sourceTargetSchema,
  sourceTerminationSchema,
} from "./health";

const Failure = Schema.TaggedStruct("HealthFixtureFailure", {
  code: Schema.String,
});
const Route = Schema.Struct({
  region: Schema.String,
});
const Metrics = Schema.Struct({
  connected: Schema.Boolean,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

const adapterFailure = {
  _tag: "AdapterFailure",
  failure: {
    _tag: "HealthFixtureFailure",
    code: "offline",
  },
} as const;
const termination = {
  _tag: "Failed",
  failure: adapterFailure,
} as const;
const rejection = {
  failure: adapterFailure,
  location: { offset: 10n },
  rejectedAtNanos: 20n,
} as const;

describe("Source Health schemas", () => {
  it.effect("decodes every exact Source Status branch", () =>
    Effect.gen(function* () {
      const status = sourceStatusSchema(Failure, Location);
      const statuses = [
        { _tag: "Starting", attempt: 1n, startedAtNanos: 1n },
        { _tag: "Ready", attempt: 1n, readyAtNanos: 2n },
        {
          _tag: "Degraded",
          attempt: 1n,
          degradedAtNanos: 2n,
          latestRejection: rejection,
        },
        {
          _tag: "WaitingToRetry",
          nextAttempt: 2n,
          termination,
          retryAtNanos: 3n,
        },
        {
          _tag: "Reacquiring",
          previousTermination: termination,
          attempt: 2n,
          startedAtNanos: 3n,
        },
        {
          _tag: "Exhausted",
          exhaustion: {
            _tag: "RetryExhausted",
            lastTermination: termination,
          },
          exhaustedAtNanos: 4n,
        },
        {
          _tag: "Stopping",
          reason: "runtime-shutdown",
          stoppingAtNanos: 5n,
        },
      ];
      const decoded = yield* Effect.forEach(statuses, (candidate) =>
        Schema.decodeUnknownEffect(status)(candidate),
      );
      expect(decoded.map((candidate) => candidate._tag)).toStrictEqual([
        "Starting",
        "Ready",
        "Degraded",
        "WaitingToRetry",
        "Reacquiring",
        "Exhausted",
        "Stopping",
      ]);
      expect(
        Schema.is(status)({
          _tag: "Starting",
          attempt: 2n,
          startedAtNanos: 1n,
        }),
      ).toBe(false);
    }),
  );

  it.effect("round-trips exact targets, failures, metrics, and health", () =>
    Effect.gen(function* () {
      const runtimeMetrics = {
        startedAtNanos: 1n,
        lastAttemptStartedAtNanos: 2n,
        lastDeliveryAtNanos: 3n,
        lastRejectionAtNanos: 4n,
        lastAppliedMutationAtNanos: 3n,
        lastTerminationAtNanos: null,
        currentAttempt: 2n,
        retryCount: 1n,
        receivedDeliveryCount: 1n,
        rejectedItemCount: 1n,
        attemptedMutationCount: 1n,
        appliedUpsertCount: 1n,
        appliedDeleteCount: 0n,
        failedMutationCount: 0n,
        completedSettlementCount: 2n,
        failedSettlementCount: 0n,
        retainedRowCount: 1,
        lanes: [
          {
            id: "events",
            buffer: {
              _tag: "Bounded",
              capacity: 8,
              depth: 1,
              highWaterMark: 2,
              overflowCount: 0n,
            },
          },
        ],
      };
      const health = {
        adapter: {
          name: "health-fixture",
          version: "1",
        },
        target: {
          _tag: "Leased",
          route: {
            region: "eu",
          },
        },
        status: {
          _tag: "Degraded",
          attempt: 2n,
          degradedAtNanos: 4n,
          latestRejection: rejection,
        },
        metrics: {
          runtime: runtimeMetrics,
          adapter: {
            connected: true,
          },
        },
        sampledAtNanos: 5n,
      };
      const codec = sourceHealthSchema({
        adapterFailure: Failure,
        route: Route,
        adapterMetrics: Metrics,
        rejectionLocation: Location,
      });

      expect(
        yield* Schema.decodeUnknownEffect(SourceBufferMetricsSchema)({
          _tag: "Unbuffered",
        }),
      ).toStrictEqual({ _tag: "Unbuffered" });
      expect(
        yield* Schema.decodeUnknownEffect(SourceLaneRuntimeMetricsSchema)(runtimeMetrics.lanes[0]),
      ).toStrictEqual(runtimeMetrics.lanes[0]);
      expect(
        yield* Schema.decodeUnknownEffect(SourceRuntimeMetricsSchema)(runtimeMetrics),
      ).toStrictEqual(runtimeMetrics);
      expect(
        yield* Schema.decodeUnknownEffect(sourceTargetSchema(Route))(health.target),
      ).toStrictEqual(health.target);
      expect(
        yield* Schema.decodeUnknownEffect(sourceTerminationSchema(Failure))(termination),
      ).toStrictEqual(termination);
      expect(
        yield* Schema.decodeUnknownEffect(sourceRejectionDiagnosticSchema(Failure, Location))(
          rejection,
        ),
      ).toStrictEqual(rejection);
      expect(yield* Schema.decodeUnknownEffect(codec)(health)).toStrictEqual(health);

      const invalid = yield* Effect.exit(
        Schema.decodeUnknownEffect(codec)({
          ...health,
          sampledAtNanos: 5,
        }),
      );
      expect(Exit.isFailure(invalid)).toBe(true);
    }),
  );
});
