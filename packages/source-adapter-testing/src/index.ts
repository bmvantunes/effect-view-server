import {
  SourceAdapter,
  type SourceAdapterHandle,
  type SourceDefinition,
  type SourceDefinitionOptionsFamily,
  type SourceExecutionFailure,
  type SourceLaneEvent,
  type SourceLifecycleDeclaration,
  type SourceMutation,
  type SourceRetryPolicy,
  type SourceToolkit,
} from "@effect-view-server/source-adapter";
import {
  SourceAdapterServer,
  type SourceAdapterServerLifecycle,
} from "@effect-view-server/source-adapter/server";
import {
  Chunk,
  Clock,
  Context,
  Effect,
  Layer,
  Queue,
  Schedule,
  Schema,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
export {
  SourceAdapterConformance,
  SourceAdapterConformanceSubject,
  conformanceCallbackBuffer,
  registerSourceAdapterConformance,
} from "./conformance";
export type {
  SourceAdapterConformanceAttemptValidation,
  SourceAdapterConformanceCallbackBufferSnapshot,
  SourceAdapterConformanceDiagnostics,
  SourceAdapterConformanceFinalizationProbe,
  SourceAdapterConformanceLease,
  SourceAdapterConformanceLeasedSession,
  SourceAdapterConformanceLeasedSnapshot,
  SourceAdapterConformanceMaterializedSession,
  SourceAdapterConformanceMaterializedSnapshot,
  SourceAdapterConformanceRetryProbe,
  SourceAdapterConformanceSubjectValue,
  SourceAdapterConformanceSuiteOptions,
  SourceAdapterConformanceTermination,
} from "./conformance";
export {
  SourceAdapterPackageConformance,
  SourceAdapterPackageConformanceSubject,
  registerSourceAdapterPackageConformance,
  validateSourceAdapterPackageConformance,
} from "./package-conformance";
export type {
  SourceAdapterPackageConformanceIssue,
  SourceAdapterPackageConformanceOptions,
  SourceAdapterPackageConformanceSnapshot,
  SourceAdapterPackageConformanceSubjectValue,
  SourceAdapterPackageContractEvidence,
  SourceAdapterPackagePlatformEvidence,
  SourceAdapterPackageSchemaProbe,
} from "./package-conformance";

export const SourceFixtureFailure = Schema.TaggedStruct("SourceFixtureFailure", {
  message: Schema.String,
  phase: Schema.Literals(["acquire", "stream", "settlement"]),
});
export type SourceFixtureFailure = typeof SourceFixtureFailure.Type;

export const SourceFixtureMetrics = Schema.Struct({
  observed: Schema.BigInt,
  details: Schema.optional(
    Schema.Struct({
      samples: Schema.Array(Schema.Json),
      payload: Schema.Json,
    }),
  ),
});
export type SourceFixtureMetrics = typeof SourceFixtureMetrics.Type;

export const SourceFixtureRejectionLocation = Schema.Struct({
  lane: Schema.String,
  offset: Schema.BigInt,
});
export type SourceFixtureRejectionLocation = typeof SourceFixtureRejectionLocation.Type;

export type SourceFixtureDefinitionOptions<Row extends object = object> = {
  readonly label: string;
  readonly lanes?: readonly [string, ...ReadonlyArray<string>];
  readonly row: Schema.Codec<Row, unknown, never, never>;
};

export type SourceFixtureMutationInput =
  | {
      readonly _tag: "Upsert";
      readonly row: object;
    }
  | {
      readonly _tag: "Delete";
      readonly id: string;
    };

interface SourceFixtureDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: SourceFixtureDefinitionOptions<this["Row"]>;
}

type SourceFixtureLifecycle = SourceLifecycleDeclaration<
  SourceFixtureMetrics,
  SourceFixtureRejectionLocation,
  SourceFixtureDefinitionOptions,
  SourceFixtureDefinitionOptionsFamily
>;

type SourceFixtureAdapter = SourceAdapterHandle<
  "controllable-fixture",
  "1",
  SourceFixtureFailure,
  SourceFixtureLifecycle,
  SourceFixtureLifecycle
>;

const FixtureAdapter: SourceFixtureAdapter = SourceAdapter.make({
  identity: {
    name: "controllable-fixture",
    version: "1",
  },
  failure: SourceFixtureFailure,
  materialized: {
    metrics: SourceFixtureMetrics,
    rejectionLocation: SourceFixtureRejectionLocation,
    definitionOptions:
      SourceAdapter.definitionOptionsFamily<SourceFixtureDefinitionOptionsFamily>(),
  },
  leased: {
    metrics: SourceFixtureMetrics,
    rejectionLocation: SourceFixtureRejectionLocation,
    definitionOptions:
      SourceAdapter.definitionOptionsFamily<SourceFixtureDefinitionOptionsFamily>(),
  },
});

export type SourceFixtureTarget =
  | {
      readonly _tag: "Materialized";
      readonly lane?: string;
    }
  | {
      readonly _tag: "Leased";
      readonly route: Readonly<Record<string, unknown>>;
      readonly lane?: string;
    };

type FixtureCommand =
  | {
      readonly _tag: "Delivery";
      readonly mutations: readonly [
        SourceFixtureMutationInput,
        ...ReadonlyArray<SourceFixtureMutationInput>,
      ];
      readonly settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>;
    }
  | {
      readonly _tag: "Upsert";
      readonly row: object;
      readonly settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>;
    }
  | {
      readonly _tag: "Delete";
      readonly id: string;
      readonly settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>;
    }
  | {
      readonly _tag: "CorruptAfterDecode";
      readonly row: object;
      readonly field: string;
      readonly value: unknown;
      readonly settle: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>;
    }
  | {
      readonly _tag: "Reject";
      readonly failure: SourceFixtureFailure;
      readonly location: SourceFixtureRejectionLocation;
      readonly settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>;
    }
  | {
      readonly _tag: "Fail";
      readonly failure: SourceFixtureFailure;
    }
  | {
      readonly _tag: "Complete";
    };

type FixtureDataCommand = Exclude<FixtureCommand, { readonly _tag: "Complete" }>;

const decodeFixtureRow = <Row extends object>(
  row: Schema.Codec<Row, unknown, never, never>,
  candidate: unknown,
): Effect.Effect<Row, SourceExecutionFailure<SourceFixtureFailure>> =>
  Schema.decodeUnknownEffect(row)(candidate).pipe(
    Effect.mapError(
      (): SourceExecutionFailure<SourceFixtureFailure> => ({
        _tag: "AdapterFailure",
        failure: fixtureFailure("Fixture row did not satisfy its transport Schema.", "stream"),
      }),
    ),
  );

const makeFixtureMutation = <Row extends object>(
  row: Schema.Codec<Row, unknown, never, never>,
  toolkit: SourceToolkit<Row, SourceFixtureFailure, SourceFixtureRejectionLocation>,
  mutation: SourceFixtureMutationInput,
): Effect.Effect<SourceMutation<Row>, SourceExecutionFailure<SourceFixtureFailure>> => {
  if (mutation._tag === "Delete") {
    return toolkit.delete(mutation.id);
  }
  return decodeFixtureRow(row, mutation.row).pipe(Effect.flatMap(toolkit.upsert));
};

const fixtureCommandEvent = Effect.fn("SourceAdapterTesting.fixture.command")(function* <
  Row extends object,
>(
  definition: SourceFixtureDefinitionOptions<Row>,
  toolkit: SourceToolkit<Row, SourceFixtureFailure, SourceFixtureRejectionLocation>,
  command: FixtureDataCommand,
): Effect.fn.Return<
  SourceLaneEvent<Row, SourceFixtureFailure, SourceFixtureRejectionLocation>,
  SourceExecutionFailure<SourceFixtureFailure>
> {
  if (command._tag === "Fail") {
    return yield* Effect.fail<SourceExecutionFailure<SourceFixtureFailure>>({
      _tag: "AdapterFailure",
      failure: command.failure,
    });
  }
  if (command._tag === "Reject") {
    const rejectedAtNanos = yield* Clock.currentTimeNanos;
    return yield* toolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: command.failure,
      },
      location: command.location,
      rejectedAtNanos,
      ...(command.settle === undefined ? {} : { settlement: command.settle }),
    });
  }
  if (command._tag === "CorruptAfterDecode") {
    const decoded = yield* decodeFixtureRow(definition.row, command.row);
    const mutation = yield* toolkit.upsert(decoded);
    Reflect.set(mutation.row, command.field, command.value);
    return yield* toolkit.delivery(Chunk.of(mutation), command.settle);
  }
  if (command._tag === "Delivery") {
    const [firstInput, ...restInputs] = command.mutations;
    const first = yield* makeFixtureMutation(definition.row, toolkit, firstInput);
    const rest = yield* Effect.forEach(restInputs, (mutation) =>
      makeFixtureMutation(definition.row, toolkit, mutation),
    );
    return yield* toolkit.delivery(Chunk.make(first, ...rest), command.settle);
  }
  const mutation = yield* makeFixtureMutation(definition.row, toolkit, command);
  return yield* toolkit.delivery(Chunk.of(mutation), command.settle);
});

type ActiveFixtureTarget = {
  readonly queues: ReadonlyMap<
    string,
    Queue.Queue<FixtureCommand, SourceExecutionFailure<SourceFixtureFailure>>
  >;
  acquisitions: bigint;
  finalizations: bigint;
};

export type ControllableSourceFixture<Row extends object = object> = {
  readonly adapter: typeof FixtureAdapter;
  readonly layer: Layer.Layer<Context.Service.Identifier<typeof FixtureAdapter.runtimeService>>;
  readonly materializedSource: (
    options?: Omit<SourceFixtureDefinitionOptions<Row>, "row">,
    retryPolicy?: SourceRetryPolicy<SourceFixtureFailure>,
  ) => SourceDefinition<
    typeof FixtureAdapter,
    "materialized",
    SourceFixtureDefinitionOptions<Row>,
    readonly [],
    never,
    Row
  >;
  readonly leasedSource: <const RouteFields extends readonly [string, ...ReadonlyArray<string>]>(
    routeBy: RouteFields,
    options?: Omit<SourceFixtureDefinitionOptions<Row>, "row">,
    retryPolicy?: SourceRetryPolicy<SourceFixtureFailure>,
  ) => SourceDefinition<
    typeof FixtureAdapter,
    "leased",
    SourceFixtureDefinitionOptions<Row>,
    RouteFields,
    never,
    Row
  >;
  readonly controls: {
    readonly delivery: (
      target: SourceFixtureTarget,
      mutations: readonly [
        SourceFixtureMutationInput,
        ...ReadonlyArray<SourceFixtureMutationInput>,
      ],
      settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly upsert: (
      target: SourceFixtureTarget,
      row: object,
      settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly delete: (
      target: SourceFixtureTarget,
      id: string,
      settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly corruptAfterDecode: (
      target: SourceFixtureTarget,
      row: object,
      field: string,
      value: unknown,
      settle: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly reject: (
      target: SourceFixtureTarget,
      failure: SourceFixtureFailure,
      location: SourceFixtureRejectionLocation,
      settle?: (
        exit: import("@effect-view-server/source-adapter").SourceApplicationExit,
      ) => Effect.Effect<void, SourceFixtureFailure>,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly fail: (
      target: SourceFixtureTarget,
      failure: SourceFixtureFailure,
    ) => Effect.Effect<void, SourceFixtureFailure>;
    readonly complete: (target: SourceFixtureTarget) => Effect.Effect<void, SourceFixtureFailure>;
    readonly awaitActive: (target: SourceFixtureTarget) => Effect.Effect<void>;
    readonly awaitCounts: (
      target: SourceFixtureTarget,
      expected: {
        readonly acquisitions: bigint;
        readonly finalizations: bigint;
      },
    ) => Effect.Effect<void>;
    readonly failNextAcquisition: (
      target: SourceFixtureTarget,
      failure: SourceFixtureFailure,
    ) => Effect.Effect<void>;
    readonly setMetrics: (metrics: SourceFixtureMetrics) => Effect.Effect<void>;
    readonly setRawMetricObserved: (value: unknown) => Effect.Effect<void>;
    readonly metricReads: () => bigint;
    readonly counts: (target: SourceFixtureTarget) => {
      readonly acquisitions: bigint;
      readonly finalizations: bigint;
    };
  };
};

const routeKey = (route: Readonly<Record<string, unknown>>): string =>
  Object.keys(route)
    .sort()
    .map((field) => `${field}:${String(route[field])}`)
    .join("|");

const targetKey = (target: SourceFixtureTarget): string =>
  target._tag === "Materialized" ? "materialized" : `leased:${routeKey(target.route)}`;

const targetLane = (target: SourceFixtureTarget): string => target.lane ?? "fixture";

const fixtureFailure = (
  message: string,
  phase: SourceFixtureFailure["phase"],
): SourceFixtureFailure => ({
  _tag: "SourceFixtureFailure",
  message,
  phase,
});

const makeControllableSourceFixtureEffect = Effect.fn("SourceAdapterTesting.fixture.make")(
  function* <Row extends object>(
    row: Schema.Codec<Row, unknown, never, never>,
  ): Effect.fn.Return<ControllableSourceFixture<Row>> {
    let metricObserved: unknown = 0n;
    let metricDetails: SourceFixtureMetrics["details"];
    const metricSampleWithoutDetails: SourceFixtureMetrics = {
      observed: 0n,
    };
    Object.defineProperty(metricSampleWithoutDetails, "observed", {
      configurable: false,
      enumerable: true,
      get: () => metricObserved,
    });
    const metricSampleWithDetails: SourceFixtureMetrics = {
      observed: 0n,
      details: {
        samples: [],
        payload: null,
      },
    };
    Object.defineProperties(metricSampleWithDetails, {
      observed: {
        configurable: false,
        enumerable: true,
        get: () => metricObserved,
      },
      details: {
        configurable: false,
        enumerable: true,
        get: () => metricDetails,
      },
    });
    let metricReads = 0n;
    const activity = yield* SubscriptionRef.make(0n);
    const active = new Map<string, ActiveFixtureTarget>();
    const failedAcquisitions = new Map<string, SourceFixtureFailure>();
    const allCounts = new Map<string, { acquisitions: bigint; finalizations: bigint }>();

    const activeFor = (
      target: SourceFixtureTarget,
    ): Effect.Effect<ActiveFixtureTarget, SourceFixtureFailure> => {
      const current = active.get(targetKey(target));
      return current === undefined
        ? Effect.fail(fixtureFailure("Fixture target is not active.", "stream"))
        : Effect.succeed(current);
    };

    const offer = (target: SourceFixtureTarget, command: FixtureCommand) =>
      activeFor(target).pipe(
        Effect.flatMap((current) => {
          const queue = current.queues.get(targetLane(target));
          return queue === undefined
            ? Effect.fail(
                fixtureFailure(
                  `Fixture lane ${targetLane(target)} is not active for this target.`,
                  "stream",
                ),
              )
            : Queue.offer(queue, command);
        }),
      );

    const makeLifecycle = <
      Lifecycle extends import("@effect-view-server/source-adapter").SourceLifecycle,
    >(): SourceAdapterServerLifecycle<
      SourceFixtureFailure,
      NonNullable<typeof FixtureAdapter.materialized>,
      Lifecycle,
      never
    > => ({
      acquire: (input) =>
        Effect.gen(function* () {
          const key = targetKey(input.target);
          const acquisitionFailure = failedAcquisitions.get(key);
          if (acquisitionFailure !== undefined) {
            failedAcquisitions.delete(key);
            return yield* Effect.fail<SourceExecutionFailure<SourceFixtureFailure>>({
              _tag: "AdapterFailure",
              failure: acquisitionFailure,
            });
          }
          const laneIds = input.definition.lanes ?? ["fixture"];
          const queues = new Map<
            string,
            Queue.Queue<FixtureCommand, SourceExecutionFailure<SourceFixtureFailure>>
          >();
          const firstLaneId = laneIds[0];
          const firstQueue = yield* Queue.bounded<
            FixtureCommand,
            SourceExecutionFailure<SourceFixtureFailure>
          >(128);
          queues.set(firstLaneId, firstQueue);
          const remainingLanes = yield* Effect.forEach(laneIds.slice(1), (laneId) =>
            Queue.bounded<FixtureCommand, SourceExecutionFailure<SourceFixtureFailure>>(128).pipe(
              Effect.map((queue) => {
                queues.set(laneId, queue);
                return { laneId, queue };
              }),
            ),
          );
          const counts = allCounts.get(key) ?? {
            acquisitions: 0n,
            finalizations: 0n,
          };
          counts.acquisitions += 1n;
          allCounts.set(key, counts);
          const current: ActiveFixtureTarget = {
            queues,
            acquisitions: counts.acquisitions,
            finalizations: counts.finalizations,
          };
          active.set(key, current);
          yield* Scope.addFinalizer(
            yield* Effect.scope,
            Effect.gen(function* () {
              counts.finalizations += 1n;
              current.finalizations = counts.finalizations;
              active.delete(key);
              yield* Effect.forEach(queues.values(), Queue.shutdown, {
                discard: true,
              });
              yield* SubscriptionRef.update(activity, (version) => version + 1n);
            }),
          );
          yield* SubscriptionRef.update(activity, (version) => version + 1n);
          const makeLane = (
            laneId: string,
            queue: Queue.Queue<FixtureCommand, SourceExecutionFailure<SourceFixtureFailure>>,
          ) =>
            SourceAdapterServer.lane({
              id: laneId,
              events: Stream.fromQueue(queue).pipe(
                Stream.takeWhile(
                  (command): command is FixtureDataCommand => command._tag !== "Complete",
                ),
                Stream.mapEffect((command) =>
                  fixtureCommandEvent(input.definition, input.toolkit, command),
                ),
              ),
            });
          return SourceAdapterServer.attempt([
            makeLane(firstLaneId, firstQueue),
            ...remainingLanes.map(({ laneId, queue }) => makeLane(laneId, queue)),
          ]);
        }),
      metrics: () =>
        Effect.sync(() => {
          metricReads += 1n;
          return metricDetails === undefined ? metricSampleWithoutDetails : metricSampleWithDetails;
        }),
      retry: Schedule.recurs(3),
    });

    const layer = SourceAdapterServer.make(FixtureAdapter, {
      materialized: makeLifecycle<"materialized">(),
      leased: makeLifecycle<"leased">(),
    });

    const complete = (target: SourceFixtureTarget) => offer(target, { _tag: "Complete" });
    const awaitActive = (target: SourceFixtureTarget): Effect.Effect<void> =>
      SubscriptionRef.changes(activity).pipe(
        Stream.filter(() => active.has(targetKey(target))),
        Stream.take(1),
        Stream.runDrain,
      );
    const countsFor = (target: SourceFixtureTarget) => {
      const counts = allCounts.get(targetKey(target));
      return {
        acquisitions: counts?.acquisitions ?? 0n,
        finalizations: counts?.finalizations ?? 0n,
      };
    };
    const awaitCounts = (
      target: SourceFixtureTarget,
      expected: {
        readonly acquisitions: bigint;
        readonly finalizations: bigint;
      },
    ): Effect.Effect<void> =>
      SubscriptionRef.changes(activity).pipe(
        Stream.filter(() => {
          const counts = countsFor(target);
          return (
            counts.acquisitions === expected.acquisitions &&
            counts.finalizations === expected.finalizations
          );
        }),
        Stream.take(1),
        Stream.runDrain,
      );

    return {
      adapter: FixtureAdapter,
      layer,
      materializedSource: (options = { label: "materialized" }, retryPolicy) =>
        FixtureAdapter.materializedSource({ ...options, row }, retryPolicy),
      leasedSource: (routeBy, options = { label: "leased" }, retryPolicy) =>
        FixtureAdapter.leasedSource(routeBy, { ...options, row }, retryPolicy),
      controls: {
        delivery: (target, mutations, settle) =>
          offer(target, {
            _tag: "Delivery",
            mutations,
            ...(settle === undefined ? {} : { settle }),
          }),
        upsert: (target, row, settle) =>
          offer(target, {
            _tag: "Upsert",
            row,
            ...(settle === undefined ? {} : { settle }),
          }),
        delete: (target, id, settle) =>
          offer(target, {
            _tag: "Delete",
            id,
            ...(settle === undefined ? {} : { settle }),
          }),
        corruptAfterDecode: (target, row, field, value, settle) =>
          offer(target, {
            _tag: "CorruptAfterDecode",
            row,
            field,
            value,
            settle,
          }),
        reject: (target, failure, location, settle) =>
          offer(target, {
            _tag: "Reject",
            failure,
            location,
            ...(settle === undefined ? {} : { settle }),
          }),
        fail: (target, failure) => offer(target, { _tag: "Fail", failure }),
        complete,
        awaitActive,
        awaitCounts,
        failNextAcquisition: (target, failure) =>
          Effect.sync(() => {
            failedAcquisitions.set(targetKey(target), failure);
          }),
        setMetrics: (value) =>
          Effect.sync(() => {
            metricObserved = value.observed;
            metricDetails = value.details;
          }),
        setRawMetricObserved: (value) =>
          Effect.sync(() => {
            metricObserved = value;
          }),
        metricReads: () => metricReads,
        counts: countsFor,
      },
    };
  },
);

export const makeControllableSourceFixture = <Row extends object>(
  row: Schema.Codec<Row, unknown, never, never>,
): Effect.Effect<ControllableSourceFixture<Row>> => makeControllableSourceFixtureEffect(row);

export type SourceFixtureMaterializedDefinition<Row extends object = object> = SourceDefinition<
  typeof FixtureAdapter,
  "materialized",
  SourceFixtureDefinitionOptions<Row>,
  readonly [],
  never,
  Row
>;

export type SourceFixtureLeasedDefinition<
  RouteFields extends ReadonlyArray<string>,
  Row extends object = object,
> = SourceDefinition<
  typeof FixtureAdapter,
  "leased",
  SourceFixtureDefinitionOptions<Row>,
  RouteFields,
  never,
  Row
>;

export const SourceFixture = {
  make: makeControllableSourceFixture,
  failure: fixtureFailure,
} as const;
