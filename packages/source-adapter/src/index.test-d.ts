import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  SourceAdapter,
  type SourceAdapterDescriptor,
  type SourceAdapterDependencyInput,
  type SourceApplicationExit,
  type SourceApplicationTransition,
  type SourceAdapterFailure,
  type SourceAdapterHandle,
  type SourceAttempt,
  type SourceDefinition,
  type SourceDefinitionAdapter,
  type SourceDefinitionOptionsFamily,
  type SourceDefinitionRow,
  type SourceDelivery,
  type SourceDeliveryLane,
  type SourceDefinitionLifecycle,
  type SourceDefinitionOptions,
  type SourceDefinitionRouteFields,
  type SourceDefinitionRetryServices,
  type SourceExecutionFailure,
  type SourceDependencyFailureTarget,
  type SourceDependencyIssue,
  type SourceFailureClassification,
  type SourceHealthForDefinition,
  type SourceLifecycleDeclaration,
  type SourceLifecycleMetricsInput,
  type SourceMaintenanceOperation,
  type SourceMutation,
  type SourceStatus,
  type SourceToolkit,
} from "./index";
import {
  SourceAdapterServer,
  SourceBuffer,
  type BackpressurableSourceBuffer,
  type NonPausableSourceBuffer,
  type SourceAdapterServerDefinitionEntry,
  type SourceAdapterServerImplementations,
  type SourceAdapterServerLifecycle,
  type SourceApplicationStateModule,
  type SourceApplicationStateRegistration,
  type SourceApplicationStateSweepInput,
} from "./server";
import { Chunk, Context, Effect, Layer, Schedule, Schema, Scope, Stream } from "effect";

const Failure = Schema.TaggedStruct("TypeFixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  connected: Schema.Boolean,
});
const Location = Schema.Struct({
  partition: Schema.Number,
  offset: Schema.BigInt,
});

declare const unsafeAny: any;

type MappedDefinitionOptions<Row extends object> = {
  readonly initial: Row;
};

interface MappedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: MappedDefinitionOptions<this["Row"]>;
}

interface ErasedAnyDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: any;
}

interface ErasedUnknownDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: unknown;
}

const mappedAdapter = SourceAdapter.make({
  identity: { name: "mapped-type-fixture" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptionsFamily<MappedDefinitionOptionsFamily>(),
  },
  leased: undefined,
});
const mappedDefinition = mappedAdapter.materializedSource<{
  readonly id: string;
  readonly value: number;
}>({
  initial: {
    id: "initial",
    value: 1,
  },
});
const extraMappedDefinition = mappedAdapter.materializedSource<{
  readonly id: string;
  readonly value: number;
  readonly extra: boolean;
}>({
  initial: {
    id: "initial",
    value: 1,
    extra: true,
  },
});
const conditionalMappedDefinition = Math.random() > 0.5 ? mappedDefinition : extraMappedDefinition;
// @ts-expect-error concrete Source Adapter factory results preserve every conditional Row branch.
const invalidConditionalMappedDefinition: typeof mappedDefinition = conditionalMappedDefinition;

const adapter = SourceAdapter.make({
  identity: {
    name: "type-fixture",
    version: "1",
  },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
      readonly batchSize: number;
    }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
      readonly batchSize: number;
    }>(),
  },
});

const materialized = adapter.materializedSource({
  label: "orders",
  batchSize: 100,
});
const leased = adapter.leasedSource(["region", "desk"], {
  label: "orders",
  batchSize: 100,
});
const adapterDescriptor = SourceAdapter.descriptor(adapter);
const _adapterDescriptorContract: SourceAdapterDescriptor<
  "type-fixture",
  "1",
  typeof Failure.Type,
  typeof adapter.materialized,
  typeof adapter.leased
> = adapterDescriptor;

type TypeFixtureRow = {
  readonly id: string;
  readonly region: string;
  readonly desk: string;
};
type MaterializedHealthTarget = SourceHealthForDefinition<
  typeof materialized,
  TypeFixtureRow
>["target"];
type LeasedHealthTarget = SourceHealthForDefinition<typeof leased, TypeFixtureRow>["target"];
type RegionalDefinition = SourceDefinition<
  typeof adapter,
  "materialized",
  typeof materialized.options,
  readonly [],
  never,
  TypeFixtureRow,
  {
    readonly _tag: "RegionalFailure";
    readonly region: "eu" | "us";
  },
  {
    readonly regions: ReadonlyArray<{
      readonly region: "eu" | "us";
    }>;
  },
  {
    readonly region: "eu" | "us";
  },
  "eu" | "us"
>;
type RegionalHealth = SourceHealthForDefinition<RegionalDefinition, TypeFixtureRow>;

const _materializedHealthTarget: MaterializedHealthTarget = { _tag: "Materialized" };
const _leasedHealthTarget: LeasedHealthTarget = {
  _tag: "Leased",
  route: { region: "eu", desk: "cash" },
};
type RegionalRetryFailure = Extract<
  Extract<RegionalHealth["status"], { readonly _tag: "WaitingToRetry" }>["termination"],
  { readonly _tag: "Failed" }
>["failure"];
expectTypeOf<
  Extract<RegionalRetryFailure, { readonly _tag: "AdapterFailure" }>["failure"]
>().toEqualTypeOf<{
  readonly _tag: "RegionalFailure";
  readonly region: "eu" | "us";
}>();
expectTypeOf<
  Extract<
    Extract<RegionalHealth["status"], { readonly _tag: "Degraded" }>["reasons"][number],
    { readonly _tag: "SourceItemRejection" }
  >["latestRejection"]["location"]["region"]
>().toEqualTypeOf<"eu" | "us">();
expectTypeOf<RegionalHealth["metrics"]["adapter"]["regions"][number]["region"]>().toEqualTypeOf<
  "eu" | "us"
>();
expectTypeOf<RegionalHealth["metrics"]["runtime"]["lanes"][number]["id"]>().toEqualTypeOf<
  "eu" | "us"
>();
// @ts-expect-error Materialized Source Health cannot expose a Leased target.
const _invalidMaterializedHealthTarget: MaterializedHealthTarget = _leasedHealthTarget;
// @ts-expect-error Leased Source Health cannot expose a Materialized target.
const _invalidLeasedHealthTarget: LeasedHealthTarget = _materializedHealthTarget;

class AdapterDependency extends Context.Service<
  AdapterDependency,
  { readonly connected: boolean }
>()("@effect-view-server/source-adapter/type-test/AdapterDependency") {}
class RetryOnlyDependency extends Context.Service<
  RetryOnlyDependency,
  { readonly enabled: boolean }
>()("@effect-view-server/source-adapter/type-test/RetryOnlyDependency") {}

const reportingAdapter = SourceAdapter.make({
  identity: { name: "reporting-type-fixture" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly materializedLabel: string;
    }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly leasedBatchSize: number;
    }>(),
  },
});

type ReportingImplementation = NonNullable<
  SourceAdapterServerImplementations<typeof reportingAdapter, AdapterDependency>["reporting"]
>;

const reportingImplementation: ReportingImplementation = {
  dependencies: (input) => {
    expectTypeOf(input.topic).toEqualTypeOf<string>();
    if (input.lifecycle === "materialized") {
      expectTypeOf(input.definition).toEqualTypeOf<{
        readonly materializedLabel: string;
      }>();
    } else {
      expectTypeOf(input.definition).toEqualTypeOf<{
        readonly leasedBatchSize: number;
      }>();
    }
    return AdapterDependency.pipe(
      Effect.map(() => [{ target: "orders", endpoints: ["fixture://orders"] }]),
    );
  },
  classifyFailure: (failure) => {
    expectTypeOf(failure).toEqualTypeOf<typeof Failure.Type>();
    return { problem: "dependency", targets: ["orders"] };
  },
};

const invalidReportingProblem: ReportingImplementation = {
  dependencies: reportingImplementation.dependencies,
  // @ts-expect-error reporting classifications reject unknown problem provenance.
  classifyFailure: () => ({ problem: "network" }),
};

const invalidSelfTargets: ReportingImplementation = {
  dependencies: reportingImplementation.dependencies,
  // @ts-expect-error self provenance cannot carry dependency targets.
  classifyFailure: () => ({ problem: "self", targets: ["orders"] }),
};

const structuredDependencyTarget: SourceDependencyFailureTarget = {
  dependency: "schema-registry",
  target: "orders",
};
const structuredDependencyIssue: SourceDependencyIssue = {
  code: "SchemaMismatch",
  message: "Schema Registry contract drifted.",
  attributes: [{ name: "subject", value: "orders-value" }],
};
const structuredDependencyClassification = {
  problem: "dependency",
  targets: [structuredDependencyTarget],
  issue: structuredDependencyIssue,
} as const;
expectTypeOf(structuredDependencyClassification).toExtend<SourceFailureClassification>();

// @ts-expect-error self provenance cannot carry a dependency issue.
const _invalidSelfIssue: SourceFailureClassification = {
  problem: "self",
  issue: structuredDependencyIssue,
};

// @ts-expect-error self provenance cannot carry dependency targets.
const _invalidSelfTargetsClassification: SourceFailureClassification = {
  problem: "self",
  targets: [structuredDependencyTarget],
};

const invalidReportingDescriptor: ReportingImplementation = {
  // @ts-expect-error dependency reporting endpoints must be strings.
  dependencies: () => Effect.succeed([{ target: "orders", endpoints: [1] }]),
  classifyFailure: reportingImplementation.classifyFailure,
};

type ClosedReportingImplementation = NonNullable<
  SourceAdapterServerImplementations<typeof reportingAdapter, never>["reporting"]
>;

// This deliberately invalid Effect environment is the contract under test.
// @effect-diagnostics missingEffectContext:off
const invalidReportingEnvironment: ClosedReportingImplementation = {
  dependencies: () =>
    // @ts-expect-error reporting dependency Effects must use only declared implementation Services.
    AdapterDependency.pipe(
      Effect.map(() => [{ target: "orders", endpoints: ["fixture://orders"] }]),
    ),
  classifyFailure: reportingImplementation.classifyFailure,
};
// @effect-diagnostics missingEffectContext:on

expectTypeOf<SourceAdapterDependencyInput>().toEqualTypeOf<
  | {
      readonly topic: string;
      readonly lifecycle: "materialized";
      readonly definition: unknown;
    }
  | {
      readonly topic: string;
      readonly lifecycle: "leased";
      readonly definition: unknown;
    }
>();

expectTypeOf(invalidReportingProblem).not.toBeAny();
expectTypeOf(invalidSelfTargets).not.toBeAny();
expectTypeOf(invalidReportingDescriptor).not.toBeAny();
expectTypeOf(invalidReportingEnvironment).not.toBeAny();

const delegatedDefinition = SourceAdapter.materializedSource(
  adapter,
  {
    label: "delegated",
    batchSize: 64,
  },
  Schedule.recurs(0).pipe(Schedule.tap(() => RetryOnlyDependency.pipe(Effect.asVoid))),
);

const materializedLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  initialLaneIds: (input) => {
    void input.topic;
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.definition).toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(input.target).toEqualTypeOf<{
      readonly _tag: "Materialized";
    }>();
    return ["materialized", "sibling"];
  },
  acquire: (input) => {
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.definition).toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(input.target).toEqualTypeOf<{
      readonly _tag: "Materialized";
    }>();
    return Effect.succeed(
      SourceAdapterServer.attempt([
        SourceAdapterServer.lane({
          id: "materialized",
          events: Stream.never,
        }),
      ]),
    );
  },
  metrics: (input) => {
    void input.topic;
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.definition).toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(input.target).toEqualTypeOf<{
      readonly _tag: "Materialized";
    }>();
    return AdapterDependency.pipe(
      Effect.map((dependency) => ({
        connected: dependency.connected,
      })),
    );
  },
  retry: Schedule.recurs(0),
};

const leasedLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.leased>,
  "leased",
  AdapterDependency
> = {
  acquire: (input) => {
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.target._tag).toEqualTypeOf<"Leased">();
    void input.target.route;
    return Effect.succeed(
      SourceAdapterServer.attempt([
        SourceAdapterServer.lane({
          id: "leased",
          events: Stream.never,
        }),
      ]),
    );
  },
  metrics: (input) => {
    void input.topic;
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.target._tag).toEqualTypeOf<"Leased">();
    void input.target.route;
    return AdapterDependency.pipe(
      Effect.map((dependency) => ({
        connected: dependency.connected,
      })),
    );
  },
  retry: Schedule.recurs(0),
};

declare const lifecycleScope: Scope.Scope;
type MaterializedMetricsInput = SourceLifecycleMetricsInput<
  {
    readonly label: string;
    readonly batchSize: number;
  },
  "materialized",
  Readonly<Record<string, unknown>>,
  "orders"
>;
const _validMaterializedMetricsInput: MaterializedMetricsInput = {
  topic: "orders",
  definition: {
    label: "orders",
    batchSize: 1,
  },
  lifetimeScope: lifecycleScope,
  target: { _tag: "Materialized" },
};
// @ts-expect-error server lifecycle metrics require the logical lifetime Scope.
const _missingLifetimeMaterializedMetricsInput: MaterializedMetricsInput = {
  topic: "orders",
  definition: {
    label: "orders",
    batchSize: 1,
  },
  target: { _tag: "Materialized" },
};

const serverLayer = SourceAdapterServer.make(adapter, {
  materialized: materializedLifecycle,
  leased: leasedLifecycle,
});
const serverDefinitions = SourceAdapterServer.definitions(
  {
    topics: {
      materialized: { source: materialized },
      leased: { source: leased },
    },
  },
  adapter,
);
expectTypeOf(serverDefinitions).toEqualTypeOf<
  ReadonlyArray<SourceAdapterServerDefinitionEntry<typeof adapter>>
>();

const _invalidMetricsLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: materializedLifecycle.acquire,
  // @ts-expect-error lifecycle metrics must return the declaration's exact metrics output.
  metrics: () => Effect.succeed({ connected: "yes" }),
  retry: Schedule.recurs(0),
};
const _invalidInitialLaneIdsLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  // @ts-expect-error initial Lane declarations must return a non-empty string tuple.
  initialLaneIds: () => [],
  acquire: materializedLifecycle.acquire,
  metrics: materializedLifecycle.metrics,
  retry: Schedule.recurs(0),
};
const _invalidMetricsFailureLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: materializedLifecycle.acquire,
  // This deliberately invalid Effect error channel is the contract under test.
  // @effect-diagnostics missingEffectError:off
  // @ts-expect-error lifecycle metrics must remain infallible.
  metrics: () => Effect.fail("metrics failure"),
  retry: Schedule.recurs(0),
};
// @effect-diagnostics missingEffectError:on
expectTypeOf(_invalidMetricsFailureLifecycle.metrics).not.toBeAny();
// This deliberately invalid Effect error channel is the contract under test.
// @effect-diagnostics missingEffectError:off
const _invalidFailureLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  // @ts-expect-error lifecycle acquisition preserves the exact Source Execution Failure channel.
  acquire: () => Effect.fail("wrong failure"),
  metrics: materializedLifecycle.metrics,
  retry: Schedule.recurs(0),
};
// @effect-diagnostics missingEffectError:on
const _invalidRetryInputLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: materializedLifecycle.acquire,
  metrics: materializedLifecycle.metrics,
  // @ts-expect-error retry input is the exact Source Termination union.
  retry: Schedule.identity<string>(),
};
const _invalidRetryEnvironmentLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: materializedLifecycle.acquire,
  metrics: materializedLifecycle.metrics,
  // @ts-expect-error retry requirements must be included in the lifecycle Services generic.
  retry: Schedule.recurs(0).pipe(Schedule.tap(() => RetryOnlyDependency.pipe(Effect.asVoid))),
};
const _rejectionLocationLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: (input) => {
    const invalidRejection = input.toolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "invalid location",
        },
      },
      location: {
        partition: 1,
        // @ts-expect-error lifecycle rejection locations preserve the declared Schema type.
        offset: "1",
      },
      rejectedAtNanos: 1n,
    });
    expectTypeOf(invalidRejection).not.toBeAny();
    return materializedLifecycle.acquire(input);
  },
  metrics: materializedLifecycle.metrics,
  retry: Schedule.recurs(0),
};

const mappedServerLayer = SourceAdapterServer.make(mappedAdapter, {
  materialized: {
    acquire: (input) =>
      Effect.gen(function* () {
        const mutation = yield* input.toolkit.upsert(input.definition.initial);
        const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
        return SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "mapped",
            events: Stream.make(delivery).pipe(Stream.concat(Stream.never)),
          }),
        ]);
      }),
    metrics: () => Effect.succeed({ connected: true }),
    retry: Schedule.recurs(0),
  },
});

declare const topicToolkit: SourceToolkit<
  {
    readonly id: string;
    readonly value: number;
  },
  typeof Failure.Type,
  typeof Location.Type,
  never,
  "orders"
>;
declare const settlementToolkit: SourceToolkit<
  {
    readonly id: string;
    readonly value: number;
  },
  typeof Failure.Type,
  typeof Location.Type,
  AdapterDependency,
  "orders"
>;
declare const wrongSettlement: (
  exit: SourceApplicationExit,
) => Effect.Effect<void, string, AdapterDependency>;
declare const foreignTransition: SourceApplicationTransition<"payments">;
declare const ordersTransition: SourceApplicationTransition<"orders">;
declare const typedMutation: SourceMutation<{
  readonly id: string;
  readonly value: number;
}>;
declare const extraFieldMutation: SourceMutation<{
  readonly id: string;
  readonly value: number;
  readonly extra: boolean;
}>;
type ErasedAnyLifecycle = SourceLifecycleDeclaration<
  typeof Metrics.Type,
  typeof Location.Type,
  { readonly label: string },
  ErasedAnyDefinitionOptionsFamily
>;
type ErasedUnknownLifecycle = SourceLifecycleDeclaration<
  typeof Metrics.Type,
  typeof Location.Type,
  { readonly label: string },
  ErasedUnknownDefinitionOptionsFamily
>;
declare const erasedAnyAdapter: SourceAdapterHandle<
  "erased-any",
  undefined,
  typeof Failure.Type,
  ErasedAnyLifecycle,
  undefined
>;
declare const erasedUnknownAdapter: SourceAdapterHandle<
  "erased-unknown",
  undefined,
  typeof Failure.Type,
  ErasedUnknownLifecycle,
  undefined
>;
const voidAdapter = SourceAdapter.make({
  identity: { name: "void-options" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});
const voidDefinition = voidAdapter.materializedSource(undefined);

type ApplicationState = {
  readonly count: number;
};
type ApplicationCommand = {
  readonly delta: number;
};
type ApplicationMetrics = {
  readonly count: number;
};
type ApplicationSweepOutcome = {
  readonly attempted: number;
};

const applicationState = SourceAdapterServer.applicationState({
  sweepIntervalNanos: 15n,
  initialState: (input): ApplicationState => {
    expectTypeOf(input.topic).toEqualTypeOf<string>();
    expectTypeOf(input.lifetimeScope).toEqualTypeOf<Scope.Scope>();
    expectTypeOf(input.definition).toEqualTypeOf<unknown>();
    expectTypeOf(input.target._tag).toEqualTypeOf<"Materialized" | "Leased">();
    return { count: 0 };
  },
  reduce: (state: ApplicationState, command: ApplicationCommand): ApplicationState => ({
    count: state.count + command.delta,
  }),
  cancelledMaintenanceWorkIds: (
    state: ApplicationState,
    command: ApplicationCommand,
  ): ReadonlyArray<string> => {
    void state;
    return command.delta === 0 ? ["unchanged"] : [];
  },
  acquireTransition: (state: ApplicationState, command: ApplicationCommand) => {
    void state;
    void command;
    return Effect.succeed(() => undefined);
  },
  metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
  runDueSweep: <Topic extends string>(
    input: SourceApplicationStateSweepInput<Topic, ApplicationState, ApplicationCommand>,
  ): Effect.Effect<ApplicationSweepOutcome> => {
    expectTypeOf(input.epochNowNanos).toEqualTypeOf<bigint>();
    expectTypeOf(input.state).toEqualTypeOf<ApplicationState>();
    input.update({ delta: 1 });
    return Effect.succeed({ attempted: 0 });
  },
});
expectTypeOf(applicationState).toEqualTypeOf<
  SourceApplicationStateRegistration<
    ApplicationState,
    ApplicationCommand,
    ApplicationMetrics,
    ApplicationSweepOutcome
  >
>();

declare const applicationStateScope: Scope.Scope;
const applicationStateModule = applicationState.forLifetime(applicationStateScope, "orders");
declare const paymentsApplicationStateModule: SourceApplicationStateModule<
  "payments",
  ApplicationState,
  ApplicationCommand,
  ApplicationMetrics,
  ApplicationSweepOutcome
>;
expectTypeOf(applicationStateModule).toEqualTypeOf<
  SourceApplicationStateModule<
    "orders",
    ApplicationState,
    ApplicationCommand,
    ApplicationMetrics,
    ApplicationSweepOutcome
  >
>();
const preparedApplicationTransition = applicationStateModule.prepare({ delta: 1 });
expectTypeOf<
  Effect.Success<typeof preparedApplicationTransition>["transition"]["topic"]
>().toEqualTypeOf<"orders">();
expectTypeOf<Effect.Services<typeof preparedApplicationTransition>>().toEqualTypeOf<Scope.Scope>();

const StatefulMetrics = Schema.Struct({
  count: Schema.Number,
});
const statefulAdapter = SourceAdapter.make({
  identity: { name: "stateful-type-fixture" },
  failure: Failure,
  materialized: {
    applicationState: "required",
    metrics: StatefulMetrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});
const statefulLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof statefulAdapter.materialized>,
  "materialized",
  never
> = {
  applicationState,
  acquire: () =>
    Effect.succeed(
      SourceAdapterServer.attempt([
        SourceAdapterServer.lane({
          id: "stateful",
          events: Stream.never,
        }),
      ]),
    ),
  metrics: () => Effect.succeed({ count: 0 }),
  retry: Schedule.recurs(0),
};
const statefulLayer = SourceAdapterServer.make(statefulAdapter, {
  materialized: statefulLifecycle,
});

describe("Source Adapter public type contracts", () => {
  it("preserves exact declaration and definition inference without as const", () => {
    expectTypeOf(adapter.identity.name).toEqualTypeOf<"type-fixture">();
    expectTypeOf(adapter.identity.version).toEqualTypeOf<"1" | undefined>();
    // @ts-expect-error Application State belongs to the server lifecycle, not diagnostic identity.
    void adapter.identity.applicationState;
    expectTypeOf<SourceAdapterFailure<typeof adapter>>().toEqualTypeOf<typeof Failure.Type>();
    expectTypeOf(adapterDescriptor).toEqualTypeOf<
      Omit<typeof adapter, "materializedSource" | "leasedSource">
    >();
    expectTypeOf<SourceDefinitionAdapter<typeof delegatedDefinition>>().toEqualTypeOf<
      typeof adapterDescriptor
    >();
    expectTypeOf<SourceDefinitionOptions<typeof delegatedDefinition>>().toEqualTypeOf<{
      readonly label: "delegated";
      readonly batchSize: 64;
    }>();
    expectTypeOf<SourceDefinitionRow<typeof delegatedDefinition>>().toEqualTypeOf<object>();
    expectTypeOf<
      SourceDefinitionRetryServices<typeof delegatedDefinition>
    >().toEqualTypeOf<RetryOnlyDependency>();
    expectTypeOf<SourceDefinitionRetryServices<typeof materialized>>().toEqualTypeOf<never>();
    expectTypeOf<SourceDefinitionLifecycle<typeof materialized>>().toEqualTypeOf<"materialized">();
    expectTypeOf<SourceDefinitionLifecycle<typeof leased>>().toEqualTypeOf<"leased">();
    expectTypeOf<SourceDefinitionRouteFields<typeof leased>>().toEqualTypeOf<
      readonly ["region", "desk"]
    >();
    expectTypeOf<SourceDefinitionOptions<typeof leased>>().toEqualTypeOf<{
      readonly label: "orders";
      readonly batchSize: 100;
    }>();
    expectTypeOf(serverLayer).toEqualTypeOf<
      Layer.Layer<
        Context.Service.Identifier<typeof adapter.runtimeService>,
        never,
        AdapterDependency
      >
    >();
    expectTypeOf(serverLayer).not.toBeAny();
    expectTypeOf<SourceDefinitionRow<typeof mappedDefinition>>().toEqualTypeOf<{
      readonly id: string;
      readonly value: number;
    }>();
    expectTypeOf(invalidConditionalMappedDefinition).not.toBeAny();
    expectTypeOf(mappedServerLayer).not.toBeAny();
    expectTypeOf<SourceDefinitionOptions<typeof voidDefinition>>().toEqualTypeOf<undefined>();
  });

  it("rejects invalid portable declarations and definitions", () => {
    // @ts-expect-error a Source Adapter must declare at least one lifecycle.
    SourceAdapter.make({
      identity: { name: "empty" },
      failure: Failure,
      materialized: undefined,
      leased: undefined,
    });

    // @ts-expect-error lifecycle declarations are exact.
    SourceAdapter.make({
      identity: { name: "extra-lifecycle-field" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
        unexpected: true,
      },
      leased: undefined,
    });

    // @ts-expect-error definition options must include every adapter field.
    adapter.materializedSource({
      label: "orders",
    });
    adapter.materializedSource({
      label: "orders",
      batchSize: 100,
      // @ts-expect-error definition options reject extra fields.
      unexpected: true,
    });
    // @ts-expect-error delegated Source Definition options must include every adapter field.
    SourceAdapter.materializedSource(adapter, {
      label: "orders",
    });
    SourceAdapter.materializedSource(adapter, {
      label: "orders",
      batchSize: 100,
      // @ts-expect-error delegated Source Definition options reject extra fields.
      unexpected: true,
    });
    // @ts-expect-error public descriptors cannot recover the delegated SDK builder.
    SourceAdapter.materializedSource(adapterDescriptor, {
      label: "orders",
      batchSize: 100,
    });
    // @ts-expect-error public descriptors cannot be projected as SDK handles.
    SourceAdapter.descriptor(adapterDescriptor);
    // @ts-expect-error Leased Source routes must be non-empty tuples.
    adapter.leasedSource([], {
      label: "orders",
      batchSize: 100,
    });

    // @ts-expect-error adapter failures may not erase to any.
    SourceAdapter.make({
      identity: { name: "unsafe-failure" },
      failure: Schema.Any,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error adapter metrics may not erase to any.
    SourceAdapter.make({
      identity: { name: "unsafe-metrics" },
      failure: Failure,
      materialized: {
        metrics: Schema.Any,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error rejection locations may not erase to unknown.
    SourceAdapter.make({
      identity: { name: "unsafe-location" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Schema.Unknown,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error Source Definition options may not erase to any.
    SourceAdapter.definitionOptions<any>();
    // @ts-expect-error Source Definition options may not erase to unknown.
    SourceAdapter.definitionOptions<unknown>();
    // @ts-expect-error Source Definition option families may not erase to any.
    SourceAdapter.definitionOptionsFamily<any, { readonly label: string }>();
    // @ts-expect-error evaluated Source Definition option families may not erase to any.
    SourceAdapter.definitionOptionsFamily<ErasedAnyDefinitionOptionsFamily>();
    // @ts-expect-error explicit token options cannot mask an any-valued family.
    SourceAdapter.definitionOptionsFamily<
      ErasedAnyDefinitionOptionsFamily,
      { readonly label: string }
    >();
    // @ts-expect-error evaluated Source Definition option families may not erase to unknown.
    SourceAdapter.definitionOptionsFamily<ErasedUnknownDefinitionOptionsFamily>();
    // @ts-expect-error explicit token options cannot mask an unknown-valued family.
    SourceAdapter.definitionOptionsFamily<
      ErasedUnknownDefinitionOptionsFamily,
      { readonly label: string }
    >();
    // @ts-expect-error Materialized Source options reject an any-valued evaluated family.
    erasedAnyAdapter.materializedSource<{ readonly id: string }>({ totallyWrong: true });
    // @ts-expect-error Materialized Source options reject an unknown-valued evaluated family.
    erasedUnknownAdapter.materializedSource<{ readonly id: string }>({ totallyWrong: true });
    const unsafeMaterializedRow = adapter.materializedSource<any>({
      label: "unsafe-row",
      batchSize: 100,
    });
    expectTypeOf<SourceDefinitionRow<typeof unsafeMaterializedRow>>().toBeNever();
    const unsafeLeasedRow = adapter.leasedSource<readonly ["region"], any>(["region"], {
      label: "unsafe-row",
      batchSize: 100,
    });
    expectTypeOf<SourceDefinitionRow<typeof unsafeLeasedRow>>().toBeNever();
  });

  it("keeps the Topic-Bound Toolkit exact", () => {
    expectTypeOf(topicToolkit.topic).toEqualTypeOf<"orders">();
    const upsert = topicToolkit.upsert({
      id: "a",
      value: 1,
    });
    expectTypeOf<Effect.Success<typeof upsert>["row"]>().toEqualTypeOf<{
      readonly id: string;
      readonly value: number;
    }>();
    const extraRow = {
      id: "variable-extra",
      value: 1,
      extra: true,
    };
    const operations = [
      // @ts-expect-error Topic-Bound upserts require every row field.
      topicToolkit.upsert({ id: "missing-value" }),
      // @ts-expect-error Topic-Bound upserts reject unknown fields.
      topicToolkit.upsert({ id: "extra", value: 1, extra: true }),
      // @ts-expect-error Topic-Bound upserts reject extra fields carried by variables.
      topicToolkit.upsert(extraRow),
      // @ts-expect-error Topic-Bound upserts preserve field types.
      topicToolkit.upsert({ id: "wrong", value: "1" }),
      // @ts-expect-error canonical deletes require string ids.
      topicToolkit.delete(1),
      topicToolkit.reject({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "TypeFixtureFailure",
            message: "rejected",
          },
        },
        location: {
          partition: 1,
          offset: 2n,
        },
        rejectedAtNanos: 3n,
      }),
      topicToolkit.reject({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "TypeFixtureFailure",
            message: "rejected",
          },
        },
        // @ts-expect-error rejection locations are exact.
        location: { partition: 1 },
        rejectedAtNanos: 3n,
      }),
    ];
    expectTypeOf(operations).not.toBeAny();
  });

  it("keeps Source Application State synchronous, exact, and topic-bound", () => {
    expectTypeOf(statefulLayer).not.toBeAny();
    expectTypeOf(applicationStateModule.metrics()).toEqualTypeOf<ApplicationMetrics>();
    expectTypeOf(
      applicationStateModule.runDueSweep(1n, () =>
        Effect.succeed({
          _tag: "Inactive",
        }),
      ),
    ).toEqualTypeOf<Effect.Effect<ApplicationSweepOutcome>>();
    const ordersPrepared = applicationStateModule.prepare({ delta: 1 });
    const paymentsPrepared = paymentsApplicationStateModule.prepare({ delta: 1 });
    expectTypeOf<
      Effect.Success<typeof ordersPrepared>["transition"]["topic"]
    >().toEqualTypeOf<"orders">();
    expectTypeOf<
      Effect.Success<typeof paymentsPrepared>["transition"]["topic"]
    >().toEqualTypeOf<"payments">();
    const invalidForeignMaintenance = applicationStateModule.runDueSweep(
      1n,
      // @ts-expect-error maintenance execution cannot accept an operation from another Topic.
      (_operation: SourceMaintenanceOperation<"payments">) => Effect.succeed({ _tag: "Inactive" }),
    );
    expectTypeOf(invalidForeignMaintenance).not.toBeAny();

    const transitionDelivery = preparedApplicationTransition.pipe(
      Effect.flatMap((prepared) =>
        topicToolkit.delivery(typedMutation, undefined, prepared.transition),
      ),
    );
    expectTypeOf<Effect.Success<typeof transitionDelivery>>().toEqualTypeOf<
      SourceDelivery<
        {
          readonly id: string;
          readonly value: number;
        },
        typeof Failure.Type,
        never,
        "orders"
      >
    >();

    const invalidForeignTransition = topicToolkit.delivery(
      typedMutation,
      undefined,
      // @ts-expect-error a Topic-Bound Toolkit rejects an Application Transition from another Topic.
      foreignTransition,
    );
    expectTypeOf(invalidForeignTransition).not.toBeAny();
    const invalidBatchedTransition = topicToolkit.delivery(
      // @ts-expect-error Application Transitions require exactly one Source Mutation.
      Chunk.of(typedMutation),
      undefined,
      ordersTransition,
    );
    expectTypeOf(invalidBatchedTransition).not.toBeAny();

    // @ts-expect-error Application State Modules do not expose raw state reads.
    applicationStateModule.select();
    // @ts-expect-error Application State Modules do not expose raw state writes.
    applicationStateModule.update({ delta: 1 });
    // @ts-expect-error Application State Modules do not expose deadline indexes.
    void applicationStateModule.deadlines;

    const _effectReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers must be synchronous.
      reduce: (state: ApplicationState) => Effect.succeed(state),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _promiseReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers cannot return Promises.
      reduce: (state: ApplicationState) => Promise.resolve(state),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _asyncReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers cannot be async.
      reduce: async (state: ApplicationState) => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _fallibleReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers cannot return fallible Effects.
      reduce: () => Effect.fail("reducer failed"),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _asyncInitial: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      // @ts-expect-error Source Application State initialization cannot be async.
      initialState: async () => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: () => ({ count: 0 }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _promiseInitial: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      // @ts-expect-error Source Application State initialization cannot return a Promise.
      initialState: () => Promise.resolve({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: () => ({ count: 0 }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _effectInitial: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      // @ts-expect-error Source Application State initialization cannot return an Effect.
      initialState: () => Effect.succeed({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: () => ({ count: 0 }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State metrics cannot be async.
    const _asyncMetrics = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: async (state: ApplicationState) => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State metrics cannot return a Promise.
    const _promiseMetrics = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState) => Promise.resolve({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State metrics cannot return an Effect.
    const _effectMetrics = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState) => Effect.succeed({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _wrongInitial: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      // @ts-expect-error Source Application State initialization must return the declared state.
      initialState: () => ({ other: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _wrongReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers must return the declared state.
      reduce: () => ({ other: 0 }),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    const _undefinedReducer: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      // @ts-expect-error Source Application State reducers cannot return undefined.
      reduce: () => undefined,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State metrics must return the declared snapshot.
    const _wrongMetrics: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: () => ({ other: 0 }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State descriptors require sweepIntervalNanos.
    SourceAdapterServer.applicationState({
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State descriptors require initialState.
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State descriptors require reduce.
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State descriptors require metrics.
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    // @ts-expect-error Source Application State descriptors require runDueSweep.
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
    });
    // @ts-expect-error Source Application State descriptors reject unknown capabilities.
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
      unknownCapability: () => undefined,
    });
    const validApplicationStateOptions = {
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: (): Effect.Effect<ApplicationSweepOutcome> => Effect.succeed({ attempted: 0 }),
    };
    SourceAdapterServer.applicationState(validApplicationStateOptions);
    const applicationStateOptionsWithExtra = {
      ...validApplicationStateOptions,
      sweepIntervalNanos: 2n as const,
      unknownCapability: () => undefined,
    };
    // @ts-expect-error Source Application State descriptor variables reject unknown capabilities.
    SourceAdapterServer.applicationState(applicationStateOptionsWithExtra);
    const applicationStateOptionsWithAnySweepInterval = {
      ...validApplicationStateOptions,
      sweepIntervalNanos: unsafeAny,
    };
    // @ts-expect-error Source Application State descriptor variables reject any-typed sweep intervals.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnySweepInterval);
    const applicationStateOptionsUnionWithAnySweepInterval =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnySweepInterval;
    // @ts-expect-error every descriptor union member requires a typed sweep interval.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnySweepInterval);
    const applicationStateOptionsWithAnyInitialState = {
      ...validApplicationStateOptions,
      initialState: unsafeAny,
    };
    // @ts-expect-error application-state callbacks cannot be any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyInitialState);
    const applicationStateOptionsWithAnyOptionalCallback = {
      ...validApplicationStateOptions,
      acquireTransition: unsafeAny,
    };
    // @ts-expect-error optional application-state callbacks cannot be any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyOptionalCallback);
    const applicationStateOptionsUnionWithAnyCallback =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnyInitialState;
    // @ts-expect-error every descriptor union member requires typed callbacks.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnyCallback);
    const applicationStateOptionsWithAnyInitialStateResult = {
      ...validApplicationStateOptions,
      initialState: () => unsafeAny,
    };
    const applicationStateOptionsWithAnyReducerResult = {
      ...validApplicationStateOptions,
      reduce: () => unsafeAny,
    };
    const applicationStateOptionsWithAnyCancellationResult = {
      ...validApplicationStateOptions,
      cancelledMaintenanceWorkIds: () => unsafeAny,
    };
    const applicationStateOptionsWithAnyAcquisitionResult = {
      ...validApplicationStateOptions,
      acquireTransition: () => unsafeAny,
    };
    const applicationStateOptionsWithAnyMetricsResult = {
      ...validApplicationStateOptions,
      metrics: () => unsafeAny,
    };
    const applicationStateOptionsWithAnySweepResult = {
      ...validApplicationStateOptions,
      runDueSweep: () => unsafeAny,
    };
    // @ts-expect-error initial-state callbacks cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyInitialStateResult);
    // @ts-expect-error reducers cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyReducerResult);
    // @ts-expect-error cancellation callbacks cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyCancellationResult);
    // @ts-expect-error transition acquisition callbacks cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyAcquisitionResult);
    // @ts-expect-error metrics callbacks cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnyMetricsResult);
    // @ts-expect-error sweep callbacks cannot return any.
    SourceAdapterServer.applicationState(applicationStateOptionsWithAnySweepResult);
    const applicationStateOptionsUnionWithAnyInitialStateResult =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnyInitialStateResult;
    const applicationStateOptionsUnionWithAnyReducerResult =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnyReducerResult;
    const applicationStateOptionsUnionWithAnyMetricsResult =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnyMetricsResult;
    const applicationStateOptionsUnionWithAnySweepResult =
      Math.random() > 0.5
        ? validApplicationStateOptions
        : applicationStateOptionsWithAnySweepResult;
    // @ts-expect-error every descriptor union member requires a typed initial-state result.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnyInitialStateResult);
    // @ts-expect-error every descriptor union member requires a typed reducer result.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnyReducerResult);
    // @ts-expect-error every descriptor union member requires a typed metrics result.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnyMetricsResult);
    // @ts-expect-error every descriptor union member requires a typed sweep result.
    SourceAdapterServer.applicationState(applicationStateOptionsUnionWithAnySweepResult);
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      // @ts-expect-error cancelled work IDs must be strings.
      cancelledMaintenanceWorkIds: () => [1],
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      // @ts-expect-error transition acquisition must return an infallible scoped release Effect.
      acquireTransition: () => Effect.fail("cannot acquire"),
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      runDueSweep: () => Effect.succeed({ attempted: 0 }),
    });
    SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      // @ts-expect-error Source Application State sweeps return Effects, never Promises.
      runDueSweep: async () => ({ attempted: 0 }),
    });
    expectTypeOf([
      _asyncInitial,
      _promiseInitial,
      _effectInitial,
      _asyncMetrics,
      _promiseMetrics,
      _effectMetrics,
      _wrongInitial,
      _wrongReducer,
      _undefinedReducer,
      _wrongMetrics,
    ]).not.toBeAny();
    // This deliberately invalid Effect error channel is the contract under test.
    // @effect-diagnostics missingEffectError:off
    const _fallibleSweep: SourceApplicationStateRegistration<
      ApplicationState,
      ApplicationCommand,
      ApplicationMetrics,
      ApplicationSweepOutcome
    > = SourceAdapterServer.applicationState({
      sweepIntervalNanos: 1n,
      initialState: (): ApplicationState => ({ count: 0 }),
      reduce: (state: ApplicationState): ApplicationState => state,
      metrics: (state: ApplicationState): ApplicationMetrics => ({ count: state.count }),
      // @ts-expect-error Source Application State sweeps are infallible at their public seam.
      runDueSweep: () => Effect.fail("sweep failed"),
    });
    expectTypeOf(_fallibleSweep).not.toBeAny();
    // @effect-diagnostics missingEffectError:on

    SourceAdapterServer.make(statefulAdapter, {
      // @ts-expect-error stateful lifecycle implementations must provide Application State.
      materialized: {
        acquire: statefulLifecycle.acquire,
        metrics: statefulLifecycle.metrics,
        retry: Schedule.recurs(0),
      },
    });
    SourceAdapterServer.make(adapter, {
      materialized: {
        ...materializedLifecycle,
        // @ts-expect-error stateless lifecycle implementations reject Application State.
        applicationState,
      },
      leased: leasedLifecycle,
    });
  });

  it("requires exact non-empty Source degradation reasons", () => {
    type Status = SourceStatus<typeof Failure.Type, typeof Location.Type>;
    const validMaintenanceStatus: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [{ _tag: "AdapterMaintenanceFailure" }],
    };
    const validCombinedStatus: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "TypeFixtureFailure",
                message: "safe diagnostic",
              },
            },
            location: { partition: 1, offset: 2n },
            rejectedAtNanos: 3n,
          },
        },
        { _tag: "AdapterMaintenanceFailure" },
      ],
    };
    expectTypeOf(validMaintenanceStatus).toExtend<Status>();
    expectTypeOf(validCombinedStatus).toExtend<Status>();

    const invalidEmptyReasons: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      // @ts-expect-error degraded Source Health requires at least one safe reason.
      reasons: [],
    };
    const invalidMaintenanceDiagnostic: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "AdapterMaintenanceFailure",
          // @ts-expect-error maintenance degradation intentionally exposes no unsafe diagnostics.
          message: "unsafe detail",
        },
      ],
    };
    const invalidReasonOrder: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        { _tag: "AdapterMaintenanceFailure" },
        {
          // @ts-expect-error combined reasons have one canonical source-then-maintenance order.
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "TypeFixtureFailure",
                message: "safe diagnostic",
              },
            },
            location: { partition: 1, offset: 2n },
            rejectedAtNanos: 3n,
          },
        },
      ],
    };
    const invalidDuplicateReasons: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "TypeFixtureFailure",
                message: "first",
              },
            },
            location: { partition: 1, offset: 2n },
            rejectedAtNanos: 3n,
          },
        },
        {
          // @ts-expect-error one degradation episode cannot contain duplicate rejection reasons.
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "TypeFixtureFailure",
                message: "second",
              },
            },
            location: { partition: 1, offset: 3n },
            rejectedAtNanos: 4n,
          },
        },
      ],
    };
    const invalidUnknownReason: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          // @ts-expect-error degradation reasons are a closed transport-neutral union.
          _tag: "UnknownDegradationReason",
        },
      ],
    };
    const invalidOptionalRejection: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          // @ts-expect-error rejection diagnostics are mandatory for rejection degradation.
          latestRejection: undefined,
        },
      ],
    };
    const invalidLegacyOnlyStatus: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      // @ts-expect-error the former top-level rejection payload is not a degradation reason tuple.
      latestRejection: {
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "TypeFixtureFailure",
            message: "legacy",
          },
        },
        location: { partition: 1, offset: 2n },
        rejectedAtNanos: 3n,
      },
    };
    const invalidSurplusStatus: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [{ _tag: "AdapterMaintenanceFailure" }],
      // @ts-expect-error Source Status values reject surplus fields.
      message: "unsafe detail",
    };
    const invalidFailureBinding: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              // @ts-expect-error rejection failures remain bound to the Adapter failure Schema.
              failure: { _tag: "ForeignFailure", message: "foreign" },
            },
            location: { partition: 1, offset: 2n },
            rejectedAtNanos: 3n,
          },
        },
      ],
    };
    const invalidLocationBinding: Status = {
      _tag: "Degraded",
      attempt: 1n,
      degradedAtNanos: 2n,
      reasons: [
        {
          _tag: "SourceItemRejection",
          latestRejection: {
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "TypeFixtureFailure",
                message: "location",
              },
            },
            // @ts-expect-error rejection locations remain bound to their exact Schema.
            location: { region: "eu" },
            rejectedAtNanos: 3n,
          },
        },
      ],
    };
    expectTypeOf([
      invalidEmptyReasons,
      invalidMaintenanceDiagnostic,
      invalidReasonOrder,
      invalidDuplicateReasons,
      invalidUnknownReason,
      invalidOptionalRejection,
      invalidLegacyOnlyStatus,
      invalidSurplusStatus,
      invalidFailureBinding,
      invalidLocationBinding,
    ]).not.toBeAny();
  });

  it("preserves Delivery, lane, attempt, and settlement generics", () => {
    type Row = {
      readonly id: string;
      readonly value: number;
    };

    const settlement = (_exit: SourceApplicationExit) => AdapterDependency.pipe(Effect.asVoid);
    const delivery = settlementToolkit
      .upsert({ id: "typed", value: 1 })
      .pipe(
        Effect.flatMap((mutation) => settlementToolkit.delivery(Chunk.of(mutation), settlement)),
      );
    expectTypeOf<Effect.Success<typeof delivery>>().toEqualTypeOf<
      SourceDelivery<Row, typeof Failure.Type, AdapterDependency, "orders">
    >();
    expectTypeOf<Effect.Error<typeof delivery>>().toEqualTypeOf<
      SourceExecutionFailure<typeof Failure.Type>
    >();

    const rejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      settlement: (exit) => {
        expectTypeOf(exit).toEqualTypeOf<SourceApplicationExit>();
        return AdapterDependency.pipe(Effect.asVoid);
      },
    });
    const events = AdapterDependency.pipe(
      Effect.as(
        Stream.fromEffect(delivery).pipe(
          Stream.concat(Stream.fromEffect(rejection)),
          Stream.concat(Stream.never),
        ),
      ),
      Stream.unwrap,
    );
    const lane = SourceAdapterServer.lane({
      id: "typed",
      events,
    });
    expectTypeOf(lane).toEqualTypeOf<
      SourceDeliveryLane<Row, typeof Failure.Type, typeof Location.Type, AdapterDependency>
    >();
    const attempt = SourceAdapterServer.attempt([lane]);
    expectTypeOf(attempt).toEqualTypeOf<
      SourceAttempt<Row, typeof Failure.Type, typeof Location.Type, AdapterDependency>
    >();

    // @ts-expect-error Source Attempts require at least one lane.
    SourceAdapterServer.attempt([]);
    // @ts-expect-error Deliveries require a non-empty mutation Chunk.
    const invalidEmptyDelivery = settlementToolkit.delivery(Chunk.empty(), settlement);
    expectTypeOf(invalidEmptyDelivery).not.toBeAny();
    // These deliberately invalid Effect error channels are the contract under test.
    // @effect-diagnostics missingEffectError:off
    const invalidSettlementDelivery = settlementToolkit.delivery(
      Chunk.of(typedMutation),
      // @ts-expect-error Delivery settlements preserve the Adapter Failure channel.
      wrongSettlement,
    );
    expectTypeOf(invalidSettlementDelivery).not.toBeAny();
    const invalidSettlementRejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      // @ts-expect-error Rejection settlements preserve the Adapter Failure channel.
      settlement: wrongSettlement,
    });
    expectTypeOf(invalidSettlementRejection).not.toBeAny();
    const invalidVoidSettlementRejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      // @ts-expect-error Rejection settlements must return an Effect, not void.
      settlement: () => undefined,
    });
    expectTypeOf(invalidVoidSettlementRejection).not.toBeAny();
    const invalidPromiseSettlementRejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      // @ts-expect-error Rejection settlements must return an Effect, not a Promise.
      settlement: () => Promise.resolve(),
    });
    expectTypeOf(invalidPromiseSettlementRejection).not.toBeAny();
    const invalidAsyncSettlementRejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      // @ts-expect-error Rejection settlements must not be async.
      settlement: async () => undefined,
    });
    expectTypeOf(invalidAsyncSettlementRejection).not.toBeAny();
    // @effect-diagnostics missingEffectError:on
    const invalidMutationDelivery = settlementToolkit.delivery(
      // @ts-expect-error Delivery mutations must use the Toolkit's exact Topic Row.
      Chunk.of(extraFieldMutation),
      settlement,
    );
    expectTypeOf(invalidMutationDelivery).not.toBeAny();
  });

  it("preserves both finite Source Buffer constructor contracts", () => {
    type Value = {
      readonly id: string;
    };
    const backpressurable = SourceBuffer.backpressurable<
      Value,
      typeof Failure.Type,
      AdapterDependency
    >({
      capacity: 2,
      register: (emit) =>
        AdapterDependency.pipe(
          Effect.map(() => {
            const accepted = emit({ id: "one" });
            expectTypeOf(accepted).toEqualTypeOf<Effect.Effect<void>>();
            return Effect.void;
          }),
        ),
    });
    expectTypeOf<Effect.Success<typeof backpressurable>>().toEqualTypeOf<
      BackpressurableSourceBuffer<Value>
    >();
    expectTypeOf<Effect.Error<typeof backpressurable>>().toEqualTypeOf<typeof Failure.Type>();
    expectTypeOf<Effect.Services<typeof backpressurable>>().toEqualTypeOf<
      AdapterDependency | Scope.Scope
    >();

    const nonPausable = SourceBuffer.nonPausable<Value>({
      capacity: 2,
      register: (emit) =>
        Effect.sync(() => {
          expectTypeOf(emit({ id: "one" })).toEqualTypeOf<void>();
          return Effect.void;
        }),
    });
    expectTypeOf<Effect.Success<typeof nonPausable>>().toEqualTypeOf<
      NonPausableSourceBuffer<Value>
    >();

    const invalidBackpressurable = SourceBuffer.backpressurable<Value>({
      capacity: 1,
      register: (emit) =>
        // @ts-expect-error backpressurable emitters preserve the Value type.
        emit({ id: 1 }).pipe(Effect.as(Effect.void)),
    });
    const invalidNonPausable = SourceBuffer.nonPausable<Value>({
      capacity: 1,
      register: (emit) =>
        Effect.sync(() => {
          // @ts-expect-error non-pausable emitters preserve the Value type.
          emit({ id: 1 });
          return Effect.void;
        }),
    });
    expectTypeOf(invalidBackpressurable).not.toBeAny();
    expectTypeOf(invalidNonPausable).not.toBeAny();
  });

  it("preserves executable value inference and rejects scalar values", () => {
    const executable = SourceAdapter.executable({
      decode: (input: string) => Effect.succeed(input.length),
    });
    expectTypeOf(executable).toEqualTypeOf<{
      decode: (input: string) => Effect.Effect<number>;
    }>();
    expectTypeOf(executable.decode("value")).toEqualTypeOf<Effect.Effect<number>>();

    // @ts-expect-error executable values must be objects or functions.
    SourceAdapter.executable("value");
    // @ts-expect-error executable values cannot be null.
    SourceAdapter.executable(null);
    // @ts-expect-error executable values cannot be numbers.
    SourceAdapter.executable(1);
  });

  it("requires exactly the declared server implementations", () => {
    // @ts-expect-error every declared lifecycle requires one implementation.
    SourceAdapterServer.make(adapter, {
      materialized: materializedLifecycle,
    });
    SourceAdapterServer.make(adapter, {
      materialized: materializedLifecycle,
      leased: leasedLifecycle,
      // @ts-expect-error server implementation records are exact.
      unexpected: leasedLifecycle,
    });
    SourceAdapterServer.make(
      // @ts-expect-error structurally similar values are not nominal handles.
      {
        identity: adapter.identity,
      },
      {
        materialized: materializedLifecycle,
        leased: leasedLifecycle,
      },
    );
  });
});
