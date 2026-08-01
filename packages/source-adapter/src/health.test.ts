import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect, Exit, Schema } from "effect";
import {
  SourceBufferMetricsSchema,
  SourceLaneRuntimeMetricsSchema,
  SourceRuntimeMetricsSchema,
  sourceHealthContractSchemas,
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
          reasons: [
            {
              _tag: "SourceItemRejection",
              latestRejection: rejection,
            },
          ],
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
          reasons: [
            {
              _tag: "SourceItemRejection",
              latestRejection: rejection,
            },
          ],
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
        lifecycle: "leased",
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
        yield* Schema.decodeUnknownEffect(sourceTargetSchema("leased", Route))(health.target),
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

  it.effect("derives exact materialized and leased Source Health result schemas", () =>
    Effect.gen(function* () {
      const input = {
        adapterFailure: Failure,
        route: Route,
        adapterMetrics: Metrics,
        rejectionLocation: Location,
      };
      const materialized = sourceHealthContractSchemas({
        ...input,
        lifecycle: "materialized",
      });
      const leased = sourceHealthContractSchemas({
        ...input,
        lifecycle: "leased",
      });
      expectTypeOf<typeof materialized.result.Type>().toEqualTypeOf<
        typeof materialized.health.Type
      >();
      expectTypeOf<typeof leased.result.Type>().toEqualTypeOf<
        | {
            readonly _tag: "Inactive";
            readonly route: typeof Route.Type;
          }
        | {
            readonly _tag: "Active";
            readonly route: typeof Route.Type;
            readonly health: typeof leased.health.Type;
          }
      >();
      const health = {
        adapter: { name: "health-fixture", version: "1" },
        target: { _tag: "Materialized" },
        status: { _tag: "Ready", attempt: 1n, readyAtNanos: 2n },
        metrics: {
          runtime: {
            startedAtNanos: 1n,
            lastAttemptStartedAtNanos: 1n,
            lastDeliveryAtNanos: null,
            lastRejectionAtNanos: null,
            lastAppliedMutationAtNanos: null,
            lastTerminationAtNanos: null,
            currentAttempt: 1n,
            retryCount: 0n,
            receivedDeliveryCount: 0n,
            rejectedItemCount: 0n,
            attemptedMutationCount: 0n,
            appliedUpsertCount: 0n,
            appliedDeleteCount: 0n,
            failedMutationCount: 0n,
            completedSettlementCount: 0n,
            failedSettlementCount: 0n,
            retainedRowCount: 0,
            lanes: [{ id: "events", buffer: { _tag: "Unbuffered" } }],
          },
          adapter: { connected: true },
        },
        sampledAtNanos: 2n,
      };

      expect(yield* Schema.decodeUnknownEffect(materialized.health)(health)).toStrictEqual(health);
      expect(yield* Schema.decodeUnknownEffect(materialized.result)(health)).toStrictEqual(health);
      expect(
        yield* Schema.decodeUnknownEffect(leased.result)({
          _tag: "Inactive",
          route: { region: "eu" },
        }),
      ).toStrictEqual({
        _tag: "Inactive",
        route: { region: "eu" },
      });
      const leasedHealth = {
        ...health,
        target: {
          _tag: "Leased",
          route: { region: "eu" },
        },
      };
      expect(
        yield* Schema.decodeUnknownEffect(leased.result)({
          _tag: "Active",
          route: { region: "eu" },
          health: leasedHealth,
        }),
      ).toStrictEqual({
        _tag: "Active",
        route: { region: "eu" },
        health: leasedHealth,
      });
    }),
  );

  it("rejects surplus fields throughout every public Source Health wire shape", () => {
    const rejects = <Type>(
      schema: Schema.Codec<Type, unknown, never, never>,
      candidate: unknown,
    ): boolean => Exit.isFailure(Schema.decodeUnknownExit(schema)(candidate));
    const runtime = {
      startedAtNanos: 1n,
      lastAttemptStartedAtNanos: 1n,
      lastDeliveryAtNanos: null,
      lastRejectionAtNanos: null,
      lastAppliedMutationAtNanos: null,
      lastTerminationAtNanos: null,
      currentAttempt: 1n,
      retryCount: 0n,
      receivedDeliveryCount: 0n,
      rejectedItemCount: 0n,
      attemptedMutationCount: 0n,
      appliedUpsertCount: 0n,
      appliedDeleteCount: 0n,
      failedMutationCount: 0n,
      completedSettlementCount: 0n,
      failedSettlementCount: 0n,
      retainedRowCount: 0,
      lanes: [{ id: "events", buffer: { _tag: "Unbuffered" } }],
    } as const;
    const health = {
      adapter: { name: "health-fixture", version: "1" },
      target: { _tag: "Materialized" },
      status: {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 2n,
        reasons: [{ _tag: "SourceItemRejection", latestRejection: rejection }],
      },
      metrics: {
        runtime,
        adapter: { connected: true },
      },
      sampledAtNanos: 3n,
    } as const;
    const materialized = sourceHealthSchema({
      adapterFailure: Failure,
      route: Route,
      adapterMetrics: Metrics,
      rejectionLocation: Location,
      lifecycle: "materialized",
    });
    const leased = sourceHealthContractSchemas({
      adapterFailure: Failure,
      route: Route,
      adapterMetrics: Metrics,
      rejectionLocation: Location,
      lifecycle: "leased",
    });
    const runtimeSurplus = { ...runtime, surplus: true };
    const laneSurplus = {
      ...runtime,
      lanes: [{ id: "events", buffer: { _tag: "Unbuffered", surplus: true } }],
    };
    const healthSurplusCandidates = [
      { ...health, surplus: true },
      { ...health, adapter: { ...health.adapter, surplus: true } },
      { ...health, target: { ...health.target, surplus: true } },
      { ...health, status: { ...health.status, surplus: true } },
      {
        ...health,
        status: {
          ...health.status,
          reasons: [
            {
              ...health.status.reasons[0],
              surplus: true,
            },
          ],
        },
      },
      {
        ...health,
        status: {
          ...health.status,
          reasons: [
            {
              ...health.status.reasons[0],
              latestRejection: { ...rejection, surplus: true },
            },
          ],
        },
      },
      { ...health, metrics: { ...health.metrics, surplus: true } },
      { ...health, metrics: { ...health.metrics, runtime: runtimeSurplus } },
      { ...health, metrics: { ...health.metrics, runtime: laneSurplus } },
      {
        ...health,
        metrics: {
          ...health.metrics,
          adapter: { ...health.metrics.adapter, surplus: true },
        },
      },
    ];

    expect(
      healthSurplusCandidates.map((candidate) => rejects(materialized, candidate)),
    ).toStrictEqual(healthSurplusCandidates.map(() => true));
    expect(
      rejects(SourceBufferMetricsSchema, {
        _tag: "Bounded",
        capacity: 1,
        depth: 0,
        highWaterMark: 0,
        overflowCount: 0n,
        surplus: true,
      }),
    ).toBe(true);
    expect(rejects(SourceLaneRuntimeMetricsSchema, { ...runtime.lanes[0], surplus: true })).toBe(
      true,
    );
    expect(rejects(SourceRuntimeMetricsSchema, runtimeSurplus)).toBe(true);
    expect(
      Exit.isFailure(
        Schema.decodeUnknownExit(sourceTargetSchema("leased", Route))({
          _tag: "Leased",
          route: { region: "eu", surplus: true },
        }),
      ),
    ).toBe(true);
    expect(
      rejects(sourceTerminationSchema(Failure), {
        ...termination,
        failure: {
          ...termination.failure,
          failure: {
            ...termination.failure.failure,
            surplus: true,
          },
        },
      }),
    ).toBe(true);
    expect(
      rejects(sourceRejectionDiagnosticSchema(Failure, Location), {
        ...rejection,
        location: { ...rejection.location, surplus: true },
      }),
    ).toBe(true);
    expect(
      rejects(leased.result, {
        _tag: "Inactive",
        route: { region: "eu" },
        surplus: true,
      }),
    ).toBe(true);
    expect(
      rejects(leased.result, {
        _tag: "Active",
        route: { region: "eu" },
        health: {
          ...health,
          target: { _tag: "Leased", route: { region: "eu" } },
        },
        surplus: true,
      }),
    ).toBe(true);
  });

  it.effect("rejects semantically impossible bounded Source Buffer metrics", () =>
    Effect.gen(function* () {
      const invalidMetrics = [
        {
          _tag: "Bounded",
          capacity: 0,
          depth: 0,
          highWaterMark: 0,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 1.5,
          depth: 0,
          highWaterMark: 0,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: -1,
          highWaterMark: 0,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: 1,
          highWaterMark: -1,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: 3,
          highWaterMark: 3,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: 1,
          highWaterMark: 3,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: 2,
          highWaterMark: 1,
          overflowCount: 0n,
        },
        {
          _tag: "Bounded",
          capacity: 2,
          depth: 1,
          highWaterMark: 1,
          overflowCount: -1n,
        },
      ];
      const exits = yield* Effect.forEach(invalidMetrics, (metrics) =>
        Schema.decodeUnknownEffect(SourceBufferMetricsSchema)(metrics).pipe(Effect.exit),
      );

      expect(exits.map(Exit.isFailure)).toStrictEqual(invalidMetrics.map(() => true));
    }),
  );

  it("enforces lifecycle-exact targets", () => {
    expect(Schema.is(sourceTargetSchema("materialized", Route))({ _tag: "Materialized" })).toBe(
      true,
    );
    expect(
      Schema.is(sourceTargetSchema("materialized", Route))({
        _tag: "Leased",
        route: { region: "eu" },
      }),
    ).toBe(false);
    expect(
      Schema.is(sourceTargetSchema("leased", Route))({
        _tag: "Leased",
        route: { region: "eu" },
      }),
    ).toBe(true);
    expect(Schema.is(sourceTargetSchema("leased", Route))({ _tag: "Materialized" })).toBe(false);
  });

  it("rejects semantically impossible Source Runtime metrics and statuses", () => {
    const validMetrics = {
      startedAtNanos: 0n,
      lastAttemptStartedAtNanos: 0n,
      lastDeliveryAtNanos: null,
      lastRejectionAtNanos: null,
      lastAppliedMutationAtNanos: null,
      lastTerminationAtNanos: null,
      currentAttempt: 1n,
      retryCount: 0n,
      receivedDeliveryCount: 0n,
      rejectedItemCount: 0n,
      attemptedMutationCount: 0n,
      appliedUpsertCount: 0n,
      appliedDeleteCount: 0n,
      failedMutationCount: 0n,
      completedSettlementCount: 0n,
      failedSettlementCount: 0n,
      retainedRowCount: 0,
      lanes: [{ id: "events", buffer: { _tag: "Unbuffered" } }],
    } as const;
    const invalidMetrics = [
      { ...validMetrics, startedAtNanos: -1n },
      { ...validMetrics, currentAttempt: 0n },
      { ...validMetrics, retryCount: -1n },
      { ...validMetrics, retainedRowCount: -1 },
      { ...validMetrics, retainedRowCount: 0.5 },
      {
        ...validMetrics,
        lanes: [
          { id: "events", buffer: { _tag: "Unbuffered" } },
          { id: "events", buffer: { _tag: "Unbuffered" } },
        ],
      },
    ];
    expect(
      invalidMetrics.map((candidate) => Schema.is(SourceRuntimeMetricsSchema)(candidate)),
    ).toStrictEqual(invalidMetrics.map(() => false));

    const status = sourceStatusSchema(Failure, Location);
    const invalidStatuses = [
      { _tag: "Ready", attempt: 0n, readyAtNanos: 0n },
      { _tag: "Ready", attempt: 1n, readyAtNanos: -1n },
      {
        _tag: "WaitingToRetry",
        nextAttempt: 0n,
        termination,
        retryAtNanos: 0n,
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        latestRejection: { ...rejection, rejectedAtNanos: -1n },
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [],
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [{ _tag: "AdapterMaintenanceFailure" }, { _tag: "AdapterMaintenanceFailure" }],
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [
          { _tag: "AdapterMaintenanceFailure" },
          {
            _tag: "SourceItemRejection",
            latestRejection: rejection,
          },
        ],
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [{ _tag: "SourceItemRejection" }],
      },
      {
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [{ _tag: "UnknownDegradationReason" }],
      },
    ];
    expect(invalidStatuses.map((candidate) => Schema.is(status)(candidate))).toStrictEqual(
      invalidStatuses.map(() => false),
    );
  });
});
