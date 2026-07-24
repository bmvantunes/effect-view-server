import { Schema } from "effect";
import { sourceExecutionFailureSchema } from "./model";
import type {
  SourceAdapterIdentity,
  SourceAdapterFailure,
  SourceBufferMetrics,
  SourceDefinitionAdapter,
  SourceDefinitionLifecycle,
  SourceDefinitionRouteFields,
  SourceItemRejectionDiagnostic,
  SourceLifecycle,
  SourceLifecycleLocation,
  SourceLifecycleMetrics,
  SourceTargetForLifecycle,
  SourceTermination,
} from "./model";

export type SourceRouteForDefinition<Definition, Row extends object> = {
  readonly [Field in Extract<
    SourceDefinitionRouteFields<Definition>[number],
    keyof Row
  >]: Row[Field];
};

type SourceDeclarationForDefinition<Definition> =
  SourceDefinitionAdapter<Definition> extends infer Adapter
    ? SourceDefinitionLifecycle<Definition> extends "materialized"
      ? Adapter extends { readonly materialized: infer Declaration }
        ? Declaration
        : never
      : Adapter extends { readonly leased: infer Declaration }
        ? Declaration
        : never
    : never;

type SourceLifecycleForDefinition<Definition> =
  SourceDefinitionLifecycle<Definition> extends SourceLifecycle
    ? SourceDefinitionLifecycle<Definition>
    : never;

export type SourceHealthForDefinition<Definition, Row extends object> = SourceHealth<
  SourceAdapterFailure<SourceDefinitionAdapter<Definition>>,
  SourceRouteForDefinition<Definition, Row>,
  SourceLifecycleMetrics<SourceDeclarationForDefinition<Definition>>,
  SourceLifecycleLocation<SourceDeclarationForDefinition<Definition>>,
  SourceLifecycleForDefinition<Definition>
>;

export type SourceHealthResultForDefinition<Definition, Row extends object> =
  SourceDefinitionLifecycle<Definition> extends "leased"
    ? LeasedSourceHealthResult<
        SourceRouteForDefinition<Definition, Row>,
        SourceHealthForDefinition<Definition, Row>
      >
    : MaterializedSourceHealthResult<SourceHealthForDefinition<Definition, Row>>;

export type SourceLaneRuntimeMetrics = {
  readonly id: string;
  readonly buffer: SourceBufferMetrics;
};

export type SourceRuntimeMetrics = {
  readonly startedAtNanos: bigint;
  readonly lastAttemptStartedAtNanos: bigint;
  readonly lastDeliveryAtNanos: bigint | null;
  readonly lastRejectionAtNanos: bigint | null;
  readonly lastAppliedMutationAtNanos: bigint | null;
  readonly lastTerminationAtNanos: bigint | null;
  readonly currentAttempt: bigint;
  readonly retryCount: bigint;
  readonly receivedDeliveryCount: bigint;
  readonly rejectedItemCount: bigint;
  readonly attemptedMutationCount: bigint;
  readonly appliedUpsertCount: bigint;
  readonly appliedDeleteCount: bigint;
  readonly failedMutationCount: bigint;
  readonly completedSettlementCount: bigint;
  readonly failedSettlementCount: bigint;
  readonly retainedRowCount: number;
  readonly lanes: readonly [SourceLaneRuntimeMetrics, ...ReadonlyArray<SourceLaneRuntimeMetrics>];
};

export type SourceStoppingReason = "runtime-shutdown" | "lease-release";

export type SourceRetryExhaustion<AdapterFailure> = {
  readonly _tag: "RetryExhausted";
  readonly lastTermination: SourceTermination<AdapterFailure>;
};

export type SourceStatus<AdapterFailure, RejectionLocation> =
  | {
      readonly _tag: "Starting";
      readonly attempt: 1n;
      readonly startedAtNanos: bigint;
    }
  | {
      readonly _tag: "Ready";
      readonly attempt: bigint;
      readonly readyAtNanos: bigint;
    }
  | {
      readonly _tag: "Degraded";
      readonly attempt: bigint;
      readonly degradedAtNanos: bigint;
      readonly latestRejection: SourceItemRejectionDiagnostic<AdapterFailure, RejectionLocation>;
    }
  | {
      readonly _tag: "WaitingToRetry";
      readonly nextAttempt: bigint;
      readonly termination: SourceTermination<AdapterFailure>;
      readonly retryAtNanos: bigint;
    }
  | {
      readonly _tag: "Reacquiring";
      readonly previousTermination: SourceTermination<AdapterFailure>;
      readonly attempt: bigint;
      readonly startedAtNanos: bigint;
    }
  | {
      readonly _tag: "Exhausted";
      readonly exhaustion: SourceRetryExhaustion<AdapterFailure>;
      readonly exhaustedAtNanos: bigint;
    }
  | {
      readonly _tag: "Stopping";
      readonly reason: SourceStoppingReason;
      readonly stoppingAtNanos: bigint;
    };

export type SourceHealth<
  AdapterFailure,
  Route extends Readonly<Record<string, unknown>>,
  AdapterMetrics,
  RejectionLocation,
  Lifecycle extends SourceLifecycle = SourceLifecycle,
> = {
  readonly adapter: SourceAdapterIdentity;
  readonly target: SourceTargetForLifecycle<Lifecycle, Route>;
  readonly status: SourceStatus<AdapterFailure, RejectionLocation>;
  readonly metrics: {
    readonly runtime: SourceRuntimeMetrics;
    readonly adapter: AdapterMetrics;
  };
  readonly sampledAtNanos: bigint;
};

export type MaterializedSourceHealthResult<Health> = Health;

export type LeasedSourceHealthResult<Route extends Readonly<Record<string, unknown>>, Health> =
  | {
      readonly _tag: "Inactive";
      readonly route: Route;
    }
  | {
      readonly _tag: "Active";
      readonly route: Route;
      readonly health: Health;
    };

const PositiveFiniteInteger = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeFiniteInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const NonNegativeBigInt = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));
const PositiveBigInt = Schema.BigInt.check(Schema.isGreaterThanBigInt(0n));

const BoundedSourceBufferMetricsSchema = Schema.TaggedStruct("Bounded", {
  capacity: PositiveFiniteInteger,
  depth: NonNegativeFiniteInteger,
  highWaterMark: NonNegativeFiniteInteger,
  overflowCount: NonNegativeBigInt,
}).check(
  Schema.makeFilter(
    ({ capacity, depth, highWaterMark }) => depth <= highWaterMark && highWaterMark <= capacity,
    {
      expected:
        "Source Buffer depth must not exceed its high-water mark and neither may exceed capacity",
    },
  ),
);

export const SourceBufferMetricsSchema = Schema.Union([
  Schema.TaggedStruct("Unbuffered", {}),
  BoundedSourceBufferMetricsSchema,
]);

export const SourceLaneRuntimeMetricsSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  buffer: SourceBufferMetricsSchema,
});

const SourceLaneRuntimeMetricsArraySchema = Schema.NonEmptyArray(
  SourceLaneRuntimeMetricsSchema,
).check(
  Schema.makeFilter((lanes) => new Set(lanes.map((lane) => lane.id)).size === lanes.length, {
    expected: "Source Lane Runtime Metrics must contain unique Lane IDs",
  }),
);

export const SourceRuntimeMetricsSchema = Schema.Struct({
  startedAtNanos: NonNegativeBigInt,
  lastAttemptStartedAtNanos: NonNegativeBigInt,
  lastDeliveryAtNanos: Schema.NullOr(NonNegativeBigInt),
  lastRejectionAtNanos: Schema.NullOr(NonNegativeBigInt),
  lastAppliedMutationAtNanos: Schema.NullOr(NonNegativeBigInt),
  lastTerminationAtNanos: Schema.NullOr(NonNegativeBigInt),
  currentAttempt: PositiveBigInt,
  retryCount: NonNegativeBigInt,
  receivedDeliveryCount: NonNegativeBigInt,
  rejectedItemCount: NonNegativeBigInt,
  attemptedMutationCount: NonNegativeBigInt,
  appliedUpsertCount: NonNegativeBigInt,
  appliedDeleteCount: NonNegativeBigInt,
  failedMutationCount: NonNegativeBigInt,
  completedSettlementCount: NonNegativeBigInt,
  failedSettlementCount: NonNegativeBigInt,
  retainedRowCount: NonNegativeFiniteInteger,
  lanes: SourceLaneRuntimeMetricsArraySchema,
});

export const sourceTargetSchema = <Route>(
  lifecycle: SourceLifecycle,
  route: Schema.Codec<Route, unknown, never, never>,
) =>
  lifecycle === "materialized"
    ? Schema.TaggedStruct("Materialized", {})
    : Schema.TaggedStruct("Leased", { route });

export const sourceTerminationSchema = <AdapterFailure>(
  adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>,
) => {
  const failure = sourceExecutionFailureSchema(adapterFailure);
  return Schema.Union([
    Schema.TaggedStruct("Failed", { failure }),
    Schema.TaggedStruct("UnexpectedCompletion", {}),
  ]);
};

export const sourceRejectionDiagnosticSchema = <AdapterFailure, RejectionLocation>(
  adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>,
  rejectionLocation: Schema.Codec<RejectionLocation, unknown, never, never>,
) =>
  Schema.Struct({
    failure: sourceExecutionFailureSchema(adapterFailure),
    location: rejectionLocation,
    rejectedAtNanos: NonNegativeBigInt,
  });

export const sourceStatusSchema = <AdapterFailure, RejectionLocation>(
  adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>,
  rejectionLocation: Schema.Codec<RejectionLocation, unknown, never, never>,
) => {
  const termination = sourceTerminationSchema(adapterFailure);
  const exhaustion = Schema.TaggedStruct("RetryExhausted", {
    lastTermination: termination,
  });
  return Schema.Union([
    Schema.TaggedStruct("Starting", {
      attempt: Schema.Literal(1n),
      startedAtNanos: NonNegativeBigInt,
    }),
    Schema.TaggedStruct("Ready", {
      attempt: PositiveBigInt,
      readyAtNanos: NonNegativeBigInt,
    }),
    Schema.TaggedStruct("Degraded", {
      attempt: PositiveBigInt,
      degradedAtNanos: NonNegativeBigInt,
      latestRejection: sourceRejectionDiagnosticSchema(adapterFailure, rejectionLocation),
    }),
    Schema.TaggedStruct("WaitingToRetry", {
      nextAttempt: PositiveBigInt,
      termination,
      retryAtNanos: NonNegativeBigInt,
    }),
    Schema.TaggedStruct("Reacquiring", {
      previousTermination: termination,
      attempt: PositiveBigInt,
      startedAtNanos: NonNegativeBigInt,
    }),
    Schema.TaggedStruct("Exhausted", {
      exhaustion,
      exhaustedAtNanos: NonNegativeBigInt,
    }),
    Schema.TaggedStruct("Stopping", {
      reason: Schema.Literals(["runtime-shutdown", "lease-release"]),
      stoppingAtNanos: NonNegativeBigInt,
    }),
  ]);
};

export const sourceHealthSchema = <
  AdapterFailure,
  Route,
  AdapterMetrics,
  RejectionLocation,
>(input: {
  readonly adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>;
  readonly route: Schema.Codec<Route, unknown, never, never>;
  readonly adapterMetrics: Schema.Codec<AdapterMetrics, unknown, never, never>;
  readonly rejectionLocation: Schema.Codec<RejectionLocation, unknown, never, never>;
  readonly lifecycle: SourceLifecycle;
}) =>
  Schema.Struct({
    adapter: Schema.Struct({
      name: Schema.NonEmptyString,
      version: Schema.optionalKey(Schema.NonEmptyString),
    }),
    target: sourceTargetSchema(input.lifecycle, input.route),
    status: sourceStatusSchema(input.adapterFailure, input.rejectionLocation),
    metrics: Schema.Struct({
      runtime: SourceRuntimeMetricsSchema,
      adapter: input.adapterMetrics,
    }),
    sampledAtNanos: NonNegativeBigInt,
  });
