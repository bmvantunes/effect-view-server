import { Schema } from "effect";
import type {
  SourceAdapterIdentity,
  SourceAdapterFailure,
  SourceBufferMetrics,
  SourceDefinitionAdapter,
  SourceDefinitionLifecycle,
  SourceDefinitionRouteFields,
  SourceItemRejectionDiagnostic,
  SourceLifecycleLocation,
  SourceLifecycleMetrics,
  SourceTarget,
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

export type SourceHealthForDefinition<Definition, Row extends object> = SourceHealth<
  SourceAdapterFailure<SourceDefinitionAdapter<Definition>>,
  SourceRouteForDefinition<Definition, Row>,
  SourceLifecycleMetrics<SourceDeclarationForDefinition<Definition>>,
  SourceLifecycleLocation<SourceDeclarationForDefinition<Definition>>
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
> = {
  readonly adapter: SourceAdapterIdentity;
  readonly target: SourceTarget<Route>;
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

export const SourceBufferMetricsSchema = Schema.Union([
  Schema.TaggedStruct("Unbuffered", {}),
  Schema.TaggedStruct("Bounded", {
    capacity: Schema.Number,
    depth: Schema.Number,
    highWaterMark: Schema.Number,
    overflowCount: Schema.BigInt,
  }),
]);

export const SourceLaneRuntimeMetricsSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  buffer: SourceBufferMetricsSchema,
});

export const SourceRuntimeMetricsSchema = Schema.Struct({
  startedAtNanos: Schema.BigInt,
  lastAttemptStartedAtNanos: Schema.BigInt,
  lastDeliveryAtNanos: Schema.NullOr(Schema.BigInt),
  lastRejectionAtNanos: Schema.NullOr(Schema.BigInt),
  lastAppliedMutationAtNanos: Schema.NullOr(Schema.BigInt),
  lastTerminationAtNanos: Schema.NullOr(Schema.BigInt),
  currentAttempt: Schema.BigInt,
  retryCount: Schema.BigInt,
  receivedDeliveryCount: Schema.BigInt,
  rejectedItemCount: Schema.BigInt,
  attemptedMutationCount: Schema.BigInt,
  appliedUpsertCount: Schema.BigInt,
  appliedDeleteCount: Schema.BigInt,
  failedMutationCount: Schema.BigInt,
  completedSettlementCount: Schema.BigInt,
  failedSettlementCount: Schema.BigInt,
  retainedRowCount: Schema.Number,
  lanes: Schema.NonEmptyArray(SourceLaneRuntimeMetricsSchema),
});

export const sourceTargetSchema = <Route>(route: Schema.Codec<Route, unknown, never, never>) =>
  Schema.Union([Schema.TaggedStruct("Materialized", {}), Schema.TaggedStruct("Leased", { route })]);

export const sourceTerminationSchema = <AdapterFailure>(
  adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>,
) => {
  const failure = Schema.Union([
    Schema.TaggedStruct("AdapterFailure", { failure: adapterFailure }),
    Schema.TaggedStruct("RuntimeFailure", {
      failure: Schema.Union([
        Schema.TaggedStruct("InvalidSourceDefinition", { message: Schema.String }),
        Schema.TaggedStruct("InvalidSourceDelivery", { message: Schema.String }),
        Schema.TaggedStruct("InvalidTopicRow", {
          message: Schema.String,
          topic: Schema.String,
        }),
        Schema.TaggedStruct("InvalidCanonicalId", {
          message: Schema.String,
          topic: Schema.String,
        }),
        Schema.TaggedStruct("InvalidFeedRoute", {
          message: Schema.String,
          topic: Schema.String,
        }),
        Schema.TaggedStruct("InvalidSourceMetrics", { message: Schema.String }),
        Schema.TaggedStruct("SourceBufferOverflow", {
          message: Schema.String,
          capacity: Schema.Number,
        }),
      ]),
    }),
  ]);
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
    failure: Schema.Union([
      Schema.TaggedStruct("AdapterFailure", { failure: adapterFailure }),
      Schema.TaggedStruct("RuntimeFailure", {
        failure: Schema.Union([
          Schema.TaggedStruct("InvalidSourceDefinition", { message: Schema.String }),
          Schema.TaggedStruct("InvalidSourceDelivery", { message: Schema.String }),
          Schema.TaggedStruct("InvalidTopicRow", {
            message: Schema.String,
            topic: Schema.String,
          }),
          Schema.TaggedStruct("InvalidCanonicalId", {
            message: Schema.String,
            topic: Schema.String,
          }),
          Schema.TaggedStruct("InvalidFeedRoute", {
            message: Schema.String,
            topic: Schema.String,
          }),
          Schema.TaggedStruct("InvalidSourceMetrics", { message: Schema.String }),
          Schema.TaggedStruct("SourceBufferOverflow", {
            message: Schema.String,
            capacity: Schema.Number,
          }),
        ]),
      }),
    ]),
    location: rejectionLocation,
    rejectedAtNanos: Schema.BigInt,
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
      startedAtNanos: Schema.BigInt,
    }),
    Schema.TaggedStruct("Ready", {
      attempt: Schema.BigInt,
      readyAtNanos: Schema.BigInt,
    }),
    Schema.TaggedStruct("Degraded", {
      attempt: Schema.BigInt,
      degradedAtNanos: Schema.BigInt,
      latestRejection: sourceRejectionDiagnosticSchema(adapterFailure, rejectionLocation),
    }),
    Schema.TaggedStruct("WaitingToRetry", {
      nextAttempt: Schema.BigInt,
      termination,
      retryAtNanos: Schema.BigInt,
    }),
    Schema.TaggedStruct("Reacquiring", {
      previousTermination: termination,
      attempt: Schema.BigInt,
      startedAtNanos: Schema.BigInt,
    }),
    Schema.TaggedStruct("Exhausted", {
      exhaustion,
      exhaustedAtNanos: Schema.BigInt,
    }),
    Schema.TaggedStruct("Stopping", {
      reason: Schema.Literals(["runtime-shutdown", "lease-release"]),
      stoppingAtNanos: Schema.BigInt,
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
}) =>
  Schema.Struct({
    adapter: Schema.Struct({
      name: Schema.NonEmptyString,
      version: Schema.optionalKey(Schema.NonEmptyString),
    }),
    target: sourceTargetSchema(input.route),
    status: sourceStatusSchema(input.adapterFailure, input.rejectionLocation),
    metrics: Schema.Struct({
      runtime: SourceRuntimeMetricsSchema,
      adapter: input.adapterMetrics,
    }),
    sampledAtNanos: Schema.BigInt,
  });
