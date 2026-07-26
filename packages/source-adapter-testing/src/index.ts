import {
  SourceAdapter,
  type SourceApplicationExit,
  type SourceDefinition,
  type SourceDefinitionOptionsFamily,
  type SourceExecutionFailure,
  type SourceLaneEvent,
  type SourceMutation,
  type SourceRetryPolicy,
  type SourceToolkit,
} from "@effect-view-server/source-adapter";
import {
  SourceAdapterServer,
  SourceBuffer,
  type SourceAdapterServerLifecycle,
} from "@effect-view-server/source-adapter/server";
import {
  makeSourceAdapterConformanceDriver,
  type SourceAdapterConformanceAttemptFault,
  type SourceAdapterConformanceCommand,
  type SourceAdapterConformanceRow,
  type SourceAdapterConformanceTarget,
  type SourceAdapterConformanceTransportObservation,
} from "./conformance";
import {
  Chunk,
  Clock,
  Context,
  Deferred,
  Effect,
  Layer,
  Queue,
  Schedule,
  Schema,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";

declare const SourceFixtureDefinitionRowTypeId: unique symbol;

type SourceFixtureDefinitionResult<Definition, Row> = Definition & {
  readonly [SourceFixtureDefinitionRowTypeId]?: (_row: Row) => Row;
};

export {
  SourceAdapterConformance,
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceRow,
  isSourceAdapterConformanceDriverValue,
  makeSourceAdapterConformanceDriver,
  sourceAdapterConformanceDefinitionIsLinked,
} from "./conformance";
export type {
  SourceAdapterConformanceAttemptFault,
  SourceAdapterConformanceCallbackBridge,
  SourceAdapterConformanceCommand,
  SourceAdapterConformanceDriverInput,
  SourceAdapterConformanceDriverValue,
  SourceAdapterConformanceExpectations,
  SourceAdapterLifecycleConformanceExpectations,
  SourceAdapterConformanceMutation,
  SourceAdapterConformanceEventModel,
  SourceAdapterConformanceSuiteOptions,
  SourceAdapterConformanceTarget,
  SourceAdapterConformanceTransport,
  SourceAdapterConformanceTransportObservation,
} from "./conformance";
export {
  SourceAdapterPackageConformance,
  SourceAdapterPackageInspectionError,
  inspectSourceAdapterPackageConformance,
  validateSourceAdapterPackageConformance,
} from "./package-conformance";
export type {
  SourceAdapterPackageBrowserBundleProbe,
  SourceAdapterPackageConformanceIssue,
  SourceAdapterPackageConformanceSnapshot,
  SourceAdapterPackageContractEvidence,
  SourceAdapterPackageContractProbe,
  SourceAdapterPackageLifecycleEvidence,
  SourceAdapterPackageLifecycleProbe,
  SourceAdapterPackagePlatformEvidence,
  SourceAdapterPackagePlatformProbe,
  SourceAdapterPackageInspectionOptions,
  SourceAdapterPackageSchemaProbe,
  SourceAdapterPackageTypeTestEvidence,
  SourceAdapterPackageValueProbe,
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

const FixtureAdapter = SourceAdapter.make({
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
      readonly _tag: "CorruptLaterMutationAfterDecode";
      readonly firstRow: object;
      readonly laterRow: object;
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
  if (command._tag === "CorruptLaterMutationAfterDecode") {
    const firstDecoded = yield* decodeFixtureRow(definition.row, command.firstRow);
    const laterDecoded = yield* decodeFixtureRow(definition.row, command.laterRow);
    const first = yield* toolkit.upsert(firstDecoded);
    const later = yield* toolkit.upsert(laterDecoded);
    Reflect.set(later.row, command.field, command.value);
    return yield* toolkit.delivery(Chunk.make(first, later), command.settle);
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

type CallbackFixtureEmission = {
  readonly row: unknown;
  readonly settled: Deferred.Deferred<void>;
};

export type ControllableSourceFixture<Row extends object = object> = {
  readonly adapter: typeof FixtureAdapter;
  readonly layer: Layer.Layer<Context.Service.Identifier<typeof FixtureAdapter.runtimeService>>;
  readonly materializedSource: (
    options?: Omit<SourceFixtureDefinitionOptions<Row>, "row">,
    retryPolicy?: SourceRetryPolicy<SourceFixtureFailure>,
  ) => SourceFixtureDefinitionResult<
    SourceDefinition<
      typeof FixtureAdapter,
      "materialized",
      SourceFixtureDefinitionOptions<Row>,
      readonly [],
      never,
      Row
    >,
    Row
  >;
  readonly leasedSource: <const RouteFields extends readonly [string, ...ReadonlyArray<string>]>(
    routeBy: RouteFields,
    options?: Omit<SourceFixtureDefinitionOptions<Row>, "row">,
    retryPolicy?: SourceRetryPolicy<SourceFixtureFailure>,
  ) => SourceFixtureDefinitionResult<
    SourceDefinition<
      typeof FixtureAdapter,
      "leased",
      SourceFixtureDefinitionOptions<Row>,
      RouteFields,
      never,
      Row
    >,
    Row
  >;
  readonly callbackBridge: {
    readonly layer: Layer.Layer<Context.Service.Identifier<typeof FixtureAdapter.runtimeService>>;
    readonly source: SourceFixtureDefinitionResult<
      SourceDefinition<
        typeof FixtureAdapter,
        "materialized",
        SourceFixtureDefinitionOptions<Row>,
        readonly [],
        never,
        Row
      >,
      Row
    >;
    readonly capacity: number;
    readonly pauseNextConsumer: Effect.Effect<void>;
    readonly releaseConsumer: Effect.Effect<void>;
    readonly offerBackpressurable: (row: Row) => Effect.Effect<void, SourceFixtureFailure>;
    readonly offerNonPausable: (row: Row) => Effect.Effect<void, SourceFixtureFailure>;
    readonly emitBackpressurable: (row: Row) => Effect.Effect<void, SourceFixtureFailure>;
    readonly emitNonPausable: (row: Row) => Effect.Effect<void, SourceFixtureFailure>;
    readonly registrations: () => bigint;
    readonly finalizations: () => bigint;
  };
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
    readonly corruptLaterMutationAfterDecode: (
      target: SourceFixtureTarget,
      firstRow: object,
      laterRow: object,
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
    readonly failNextPartialAcquisition: (
      target: SourceFixtureTarget,
      failure: SourceFixtureFailure,
    ) => Effect.Effect<void>;
    readonly configureNextAttempt: (
      target: SourceFixtureTarget,
      fault: SourceAdapterConformanceAttemptFault,
    ) => Effect.Effect<void>;
    readonly blockNextFinalizer: (target: SourceFixtureTarget) => Effect.Effect<void>;
    readonly releaseFinalizer: (target: SourceFixtureTarget) => Effect.Effect<void>;
    readonly setMetrics: (metrics: SourceFixtureMetrics) => Effect.Effect<void>;
    readonly setRawMetricObserved: (value: unknown) => Effect.Effect<void>;
    readonly metricReads: () => bigint;
    readonly counts: (target: SourceFixtureTarget) => {
      readonly acquisitions: bigint;
      readonly finalizations: bigint;
    };
    readonly partialAcquisitionFinalizations: (target: SourceFixtureTarget) => bigint;
    readonly finalizerStarted: (target: SourceFixtureTarget) => boolean;
    readonly changes: (
      target: SourceFixtureTarget,
    ) => Stream.Stream<SourceAdapterConformanceTransportObservation>;
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
    const partialFailedAcquisitions = new Map<string, SourceFixtureFailure>();
    const nextAttemptFaults = new Map<string, Array<SourceAdapterConformanceAttemptFault>>();
    const allCounts = new Map<string, { acquisitions: bigint; finalizations: bigint }>();
    const partialAcquisitionFinalizations = new Map<string, bigint>();
    const finalizerBlocks = new Map<string, Deferred.Deferred<void>>();
    const startedFinalizers = new Set<string>();
    let callbackRegistrations = 0n;
    let callbackFinalizations = 0n;
    let pauseNextCallbackConsumer = false;
    let callbackConsumerGate: Deferred.Deferred<void> | undefined;
    let offerBackpressurable:
      | ((row: unknown) => Effect.Effect<void, SourceFixtureFailure>)
      | undefined;
    let offerNonPausable: ((row: unknown) => Effect.Effect<void, SourceFixtureFailure>) | undefined;
    let emitBackpressurable:
      | ((row: unknown) => Effect.Effect<void, SourceFixtureFailure>)
      | undefined;
    let emitNonPausable: ((row: unknown) => Effect.Effect<void, SourceFixtureFailure>) | undefined;

    const callbackAcquire: SourceAdapterServerLifecycle<
      SourceFixtureFailure,
      NonNullable<typeof FixtureAdapter.materialized>,
      "materialized",
      never
    >["acquire"] = (input) =>
      Effect.gen(function* () {
        const consumerGate = pauseNextCallbackConsumer ? yield* Deferred.make<void>() : undefined;
        pauseNextCallbackConsumer = false;
        callbackConsumerGate = consumerGate;
        const backpressurable = yield* SourceBuffer.backpressurable<CallbackFixtureEmission>({
          capacity: 1,
          register: (emit) =>
            Effect.gen(function* () {
              callbackRegistrations += 1n;
              const offer = (value: unknown) =>
                Effect.gen(function* () {
                  const settled = yield* Deferred.make<void>();
                  yield* emit({ row: value, settled });
                  return settled;
                });
              offerBackpressurable = (value) => offer(value).pipe(Effect.asVoid);
              emitBackpressurable = (value) => offer(value).pipe(Effect.flatMap(Deferred.await));
              yield* SubscriptionRef.update(activity, (version) => version + 1n);
              // Registration intentionally returns the cleanup Effect as its acquired value.
              // @effect-diagnostics-next-line returnEffectInGen:off
              return Effect.gen(function* () {
                offerBackpressurable = undefined;
                emitBackpressurable = undefined;
                callbackConsumerGate = undefined;
                callbackFinalizations += 1n;
                yield* SubscriptionRef.update(activity, (version) => version + 1n);
              });
            }),
        });
        const nonPausable = yield* SourceBuffer.nonPausable<CallbackFixtureEmission>({
          capacity: 1,
          register: (emit) =>
            Effect.gen(function* () {
              callbackRegistrations += 1n;
              const offer = (value: unknown) =>
                Effect.gen(function* () {
                  const settled = yield* Deferred.make<void>();
                  emit({ row: value, settled });
                  return settled;
                });
              offerNonPausable = (value) => offer(value).pipe(Effect.asVoid);
              emitNonPausable = (value) => offer(value).pipe(Effect.flatMap(Deferred.await));
              yield* SubscriptionRef.update(activity, (version) => version + 1n);
              // Registration intentionally returns the cleanup Effect as its acquired value.
              // @effect-diagnostics-next-line returnEffectInGen:off
              return Effect.gen(function* () {
                offerNonPausable = undefined;
                emitNonPausable = undefined;
                callbackFinalizations += 1n;
                yield* SubscriptionRef.update(activity, (version) => version + 1n);
              });
            }),
        });
        const toDelivery = Effect.fn("SourceAdapterTesting.callbackFixture.delivery")(function* (
          emission: CallbackFixtureEmission,
        ) {
          const decoded = yield* decodeFixtureRow(input.definition.row, emission.row);
          const mutation = yield* input.toolkit.upsert(decoded);
          return yield* input.toolkit.delivery(Chunk.of(mutation), () =>
            Deferred.succeed(emission.settled, undefined).pipe(Effect.asVoid),
          );
        });
        const gateEvents = <Value, Error>(
          events: Stream.Stream<Value, Error>,
        ): Stream.Stream<Value, Error> =>
          consumerGate === undefined
            ? events
            : Stream.fromEffect(Deferred.await(consumerGate)).pipe(
                Stream.drain,
                Stream.concat(events),
              );
        return SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "backpressurable",
            events: gateEvents(backpressurable.stream).pipe(Stream.mapEffect(toDelivery)),
            bufferMetrics: backpressurable.metrics,
          }),
          SourceAdapterServer.lane({
            id: "non-pausable",
            events: gateEvents(nonPausable.stream).pipe(Stream.mapEffect(toDelivery)),
            bufferMetrics: nonPausable.metrics,
          }),
        ]);
      });
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
          const partialAcquisitionFailure = partialFailedAcquisitions.get(key);
          if (partialAcquisitionFailure !== undefined) {
            partialFailedAcquisitions.delete(key);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                partialAcquisitionFinalizations.set(
                  key,
                  (partialAcquisitionFinalizations.get(key) ?? 0n) + 1n,
                );
              }).pipe(Effect.andThen(SubscriptionRef.update(activity, (version) => version + 1n))),
            );
            return yield* Effect.fail<SourceExecutionFailure<SourceFixtureFailure>>({
              _tag: "AdapterFailure",
              failure: partialAcquisitionFailure,
            });
          }
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
              const finalizerBlock = finalizerBlocks.get(key);
              if (finalizerBlock !== undefined) {
                startedFinalizers.add(key);
                yield* SubscriptionRef.update(activity, (version) => version + 1n);
                yield* Deferred.await(finalizerBlock);
                finalizerBlocks.delete(key);
                startedFinalizers.delete(key);
              }
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
          const lanes = [
            makeLane(firstLaneId, firstQueue),
            ...remainingLanes.map(({ laneId, queue }) => makeLane(laneId, queue)),
          ] as const;
          const attempt = SourceAdapterServer.attempt(lanes);
          const faults = nextAttemptFaults.get(key);
          const fault = faults?.shift();
          if (faults?.length === 0) {
            nextAttemptFaults.delete(key);
          }
          if (fault === undefined) {
            return attempt;
          }
          const nominalOverride = <Value extends object>(
            source: Value,
            property: string,
            value: unknown,
          ): Value => {
            const clone: Value = Object.create(Object.getPrototypeOf(source));
            Object.assign(clone, source);
            Object.defineProperty(clone, property, {
              enumerable: true,
              value,
            });
            Object.getOwnPropertySymbols(source).forEach((brand) => {
              Object.defineProperty(clone, brand, {
                value: () => clone,
              });
            });
            return Object.freeze(clone);
          };
          const faultedLane = (
            lane: (typeof lanes)[number],
            property: "id" | "bufferMetrics",
            value: unknown,
          ) => nominalOverride(lane, property, value);
          const faultedLanes =
            fault === "EmptyLanes"
              ? []
              : fault === "EmptyLaneId"
                ? [faultedLane(lanes[0], "id", ""), ...lanes.slice(1)]
                : fault === "DuplicateLaneId"
                  ? [lanes[0], faultedLane(lanes[1] ?? lanes[0], "id", lanes[0].id)]
                  : fault === "ChangedLaneIds"
                    ? lanes.map((lane) => faultedLane(lane, "id", `changed-${lane.id}`))
                    : [faultedLane(lanes[0], "bufferMetrics", {}), ...lanes.slice(1)];
          return nominalOverride(attempt, "lanes", Object.freeze(faultedLanes));
        }),
      metrics: () =>
        Effect.sync(() => {
          metricReads += 1n;
          return metricDetails === undefined ? metricSampleWithoutDetails : metricSampleWithDetails;
        }),
      retry: Schedule.recurs(3),
    });

    const materializedLifecycle = makeLifecycle<"materialized">();
    const layer = SourceAdapterServer.make(FixtureAdapter, {
      materialized: {
        ...materializedLifecycle,
        acquire: (input) =>
          input.definition.label === "callback"
            ? callbackAcquire(input)
            : materializedLifecycle.acquire(input),
      },
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
      const key = targetKey(target);
      const counts = allCounts.get(key);
      return {
        acquisitions: counts?.acquisitions ?? 0n,
        finalizations: counts?.finalizations ?? 0n,
      };
    };
    const observeTransport = (
      target: SourceFixtureTarget,
    ): SourceAdapterConformanceTransportObservation => ({
      ...countsFor(target),
      partialAcquisitionFinalizations: partialAcquisitionFinalizations.get(targetKey(target)) ?? 0n,
      registrations: callbackRegistrations,
      callbackFinalizations,
      finalizerStarted: startedFinalizers.has(targetKey(target)),
    });
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
      callbackBridge: {
        layer,
        source: FixtureAdapter.materializedSource(
          {
            label: "callback",
            row,
          },
          Schedule.recurs(1),
        ),
        capacity: 1,
        pauseNextConsumer: Effect.sync(() => {
          pauseNextCallbackConsumer = true;
        }),
        releaseConsumer: Effect.suspend(() =>
          callbackConsumerGate === undefined
            ? Effect.void
            : Deferred.succeed(callbackConsumerGate, undefined).pipe(Effect.asVoid),
        ),
        offerBackpressurable: (value) =>
          offerBackpressurable === undefined
            ? Effect.fail(fixtureFailure("callback bridge is not active", "stream"))
            : offerBackpressurable(value),
        offerNonPausable: (value) =>
          offerNonPausable === undefined
            ? Effect.fail(fixtureFailure("callback bridge is not active", "stream"))
            : offerNonPausable(value),
        emitBackpressurable: (value) =>
          emitBackpressurable === undefined
            ? Effect.fail(fixtureFailure("callback bridge is not active", "stream"))
            : emitBackpressurable(value),
        emitNonPausable: (value) =>
          emitNonPausable === undefined
            ? Effect.fail(fixtureFailure("callback bridge is not active", "stream"))
            : emitNonPausable(value),
        registrations: () => callbackRegistrations,
        finalizations: () => callbackFinalizations,
      },
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
        corruptLaterMutationAfterDecode: (target, firstRow, laterRow, field, value, settle) =>
          offer(target, {
            _tag: "CorruptLaterMutationAfterDecode",
            firstRow,
            laterRow,
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
        failNextPartialAcquisition: (target, failure) =>
          Effect.sync(() => {
            partialFailedAcquisitions.set(targetKey(target), failure);
          }),
        configureNextAttempt: (target, fault) =>
          Effect.sync(() => {
            const key = targetKey(target);
            const faults = nextAttemptFaults.get(key) ?? [];
            faults.push(fault);
            nextAttemptFaults.set(key, faults);
          }),
        blockNextFinalizer: (target) =>
          Effect.gen(function* () {
            finalizerBlocks.set(targetKey(target), yield* Deferred.make<void>());
          }),
        releaseFinalizer: (target) =>
          Effect.gen(function* () {
            const finalizerBlock = finalizerBlocks.get(targetKey(target));
            if (finalizerBlock !== undefined) {
              yield* Deferred.succeed(finalizerBlock, undefined);
            }
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
        partialAcquisitionFinalizations: (target) =>
          partialAcquisitionFinalizations.get(targetKey(target)) ?? 0n,
        finalizerStarted: (target) => startedFinalizers.has(targetKey(target)),
        changes: (target) =>
          SubscriptionRef.changes(activity).pipe(Stream.map(() => observeTransport(target))),
      },
    };
  },
);

export const makeControllableSourceFixture = <Row extends object>(
  row: Schema.Codec<Row, unknown, never, never>,
): Effect.Effect<ControllableSourceFixture<Row>> => makeControllableSourceFixtureEffect(row);

export type SourceFixtureMaterializedDefinition<Row extends object = object> =
  SourceFixtureDefinitionResult<
    SourceDefinition<
      typeof FixtureAdapter,
      "materialized",
      SourceFixtureDefinitionOptions<Row>,
      readonly [],
      never,
      Row
    >,
    Row
  >;

export type SourceFixtureLeasedDefinition<
  RouteFields extends ReadonlyArray<string>,
  Row extends object = object,
> = SourceFixtureDefinitionResult<
  SourceDefinition<
    typeof FixtureAdapter,
    "leased",
    SourceFixtureDefinitionOptions<Row>,
    RouteFields,
    never,
    Row
  >,
  Row
>;

export const SourceFixture = {
  make: makeControllableSourceFixture,
  failure: fixtureFailure,
  conformanceDriver: (fixture: ControllableSourceFixture<SourceAdapterConformanceRow>) => {
    const materializedTarget = (
      target: Extract<SourceAdapterConformanceTarget, { readonly _tag: "Materialized" }>,
    ): SourceFixtureTarget => ({
      _tag: "Materialized",
      lane: target.lane,
    });
    const fixtureTarget = (target: SourceAdapterConformanceTarget): SourceFixtureTarget =>
      target._tag === "Materialized"
        ? materializedTarget(target)
        : {
            _tag: "Leased",
            route: target.route,
            lane: target.lane,
          };
    const command = (input: SourceAdapterConformanceCommand): Effect.Effect<void, unknown> => {
      const settlement = (
        settle: ((exit: SourceApplicationExit) => Effect.Effect<void, unknown>) | undefined,
      ) =>
        settle === undefined
          ? undefined
          : (exit: SourceApplicationExit) =>
              settle(exit).pipe(
                Effect.mapError(() =>
                  fixtureFailure("conformance settlement failure", "settlement"),
                ),
              );
      if (input._tag === "Delivery") {
        return fixture.controls.delivery(
          fixtureTarget(input.target),
          input.mutations,
          settlement(input.settle),
        );
      }
      if (input._tag === "CorruptLaterMutation") {
        return fixture.controls.corruptLaterMutationAfterDecode(
          fixtureTarget(input.target),
          input.firstRow,
          input.laterRow,
          input.field,
          input.value,
          (exit) =>
            input
              .settle(exit)
              .pipe(
                Effect.mapError(() =>
                  fixtureFailure("conformance settlement failure", "settlement"),
                ),
              ),
        );
      }
      if (input._tag === "Reject") {
        return fixture.controls.reject(
          fixtureTarget(input.target),
          fixtureFailure("conformance rejection", input.phase),
          {
            lane: input.target.lane,
            offset: input.offset,
          },
          settlement(input.settle),
        );
      }
      if (input._tag === "FailLane") {
        return fixture.controls.fail(
          fixtureTarget(input.target),
          fixtureFailure("conformance lane failure", input.phase),
        );
      }
      if (input._tag === "CompleteLane") {
        return fixture.controls.complete(fixtureTarget(input.target));
      }
      if (input._tag === "FailNextAcquisition") {
        const fail = input.afterFirstResource
          ? fixture.controls.failNextPartialAcquisition
          : fixture.controls.failNextAcquisition;
        return fail(
          fixtureTarget(input.target),
          fixtureFailure("conformance acquisition failure", input.phase),
        );
      }
      if (input._tag === "ConfigureNextAttempt") {
        return fixture.controls.configureNextAttempt(fixtureTarget(input.target), input.fault);
      }
      if (input._tag === "SetMetrics") {
        if (input.sample === "updated") {
          return fixture.controls.setMetrics({ observed: 42n });
        }
        return input.sample === "invalid"
          ? fixture.controls.setRawMetricObserved("invalid")
          : fixture.controls.setMetrics({ observed: 0n });
      }
      if (input._tag === "BlockNextFinalizer") {
        return fixture.controls.blockNextFinalizer(fixtureTarget(input.target));
      }
      if (input._tag === "ReleaseFinalizer") {
        return fixture.controls.releaseFinalizer(fixtureTarget(input.target));
      }
      return Effect.void;
    };
    return makeSourceAdapterConformanceDriver({
      adapter: fixture.adapter,
      expectations: {
        materialized: {
          acquisitionFailure: fixtureFailure("conformance acquisition failure", "acquire"),
          partialAcquisitionFinalizationCount: 1n,
          streamFailure: fixtureFailure("conformance lane failure", "stream"),
          settlementFailure: fixtureFailure("conformance settlement failure", "settlement"),
          rejectionFailure: (phase) => fixtureFailure("conformance rejection", phase),
          rejectionLocation: (target, offset) => ({
            lane: target.lane,
            offset,
          }),
          rowId: (_target, localId) => localId,
          updatedMetrics: {
            observed: 42n,
          },
        },
        leased: {
          acquisitionFailure: fixtureFailure("conformance acquisition failure", "acquire"),
          partialAcquisitionFinalizationCount: 1n,
          streamFailure: fixtureFailure("conformance lane failure", "stream"),
          settlementFailure: fixtureFailure("conformance settlement failure", "settlement"),
          rejectionFailure: (phase) => fixtureFailure("conformance rejection", phase),
          rejectionLocation: (target, offset) => ({
            lane: target.lane,
            offset,
          }),
          rowId: (_target, localId) => localId,
          updatedMetrics: {
            observed: 42n,
          },
        },
      },
      runtimeLayer: fixture.layer,
      materialized: {
        source: fixture.materializedSource({
          label: "conformance-materialized",
          lanes: ["primary", "sibling"],
        }),
        delayedRetrySource: fixture.materializedSource(
          {
            label: "conformance-delayed",
            lanes: ["primary", "sibling"],
          },
          Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
        ),
        singleRetrySource: fixture.materializedSource(
          {
            label: "conformance-single-retry",
            lanes: ["primary", "sibling"],
          },
          Schedule.recurs(1),
        ),
      },
      leased: {
        source: fixture.leasedSource(["region"], {
          label: "conformance-leased",
          lanes: ["primary", "sibling"],
        }),
        delayedRetrySource: fixture.leasedSource(
          ["region"],
          {
            label: "conformance-leased-delayed",
            lanes: ["primary", "sibling"],
          },
          Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
        ),
        singleRetrySource: fixture.leasedSource(
          ["region"],
          {
            label: "conformance-leased-single-retry",
            lanes: ["primary", "sibling"],
          },
          Schedule.recurs(1),
        ),
        sameRoute: { region: "eu" },
        distinctRoute: { region: "us" },
      },
      callbackBridge: {
        source: fixture.callbackBridge.source,
        capacity: fixture.callbackBridge.capacity,
        pauseNextConsumer: fixture.callbackBridge.pauseNextConsumer,
        releaseConsumer: fixture.callbackBridge.releaseConsumer,
        offerBackpressurable: fixture.callbackBridge.offerBackpressurable,
        offerNonPausable: fixture.callbackBridge.offerNonPausable,
        emitBackpressurable: fixture.callbackBridge.emitBackpressurable,
        emitNonPausable: fixture.callbackBridge.emitNonPausable,
      },
      transport: {
        command,
        observe: (target) =>
          Effect.sync(() => {
            const fixtureOwnedTarget = fixtureTarget(target);
            return {
              ...fixture.controls.counts(fixtureOwnedTarget),
              partialAcquisitionFinalizations:
                fixture.controls.partialAcquisitionFinalizations(fixtureOwnedTarget),
              registrations: fixture.callbackBridge.registrations(),
              callbackFinalizations: fixture.callbackBridge.finalizations(),
              finalizerStarted: fixture.controls.finalizerStarted(fixtureOwnedTarget),
            };
          }),
        changes: (target) => fixture.controls.changes(fixtureTarget(target)),
      },
    });
  },
} as const;
