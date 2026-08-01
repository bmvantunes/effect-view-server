import type {
  ColumnLiveViewEngineQueryPartition,
  ColumnLiveViewTerminalObserver,
} from "@effect-view-server/column-live-view-engine/internal";
import type {
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerSourceHealthResultForTopic,
  ViewServerSourceHealthSubscriber,
  ViewServerSourceHealthSubscription,
  ViewServerSourceOwnedTopic,
} from "@effect-view-server/client";
import type {
  RowSchema,
  StatusEvent,
  TopicDefinitions,
  ViewServerHealth,
  ViewServerRuntimeError,
  ViewServerSourceHealth,
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import {
  captureSourceHealthInput,
  makeSchemaJsonIdentity,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import type {
  SourceDefinition,
  SourceDefinitionAdapter,
  SourceDefinitionAny,
  SourceDefinitionOptionsFamily,
  SourceDefinitionRetryServices,
  SourceHealth,
  LeasedSourceHealthResult,
  SourceLaneRuntimeMetrics,
  SourceRuntimeMetrics,
  SourceStatus,
  SourceTarget,
  SourceTermination,
} from "@effect-view-server/source-adapter";
import {
  SourceBufferMetricsSchema,
  sourceExecutionFailureSchema,
  sourceHealthContractSchemas,
} from "@effect-view-server/source-adapter";
import {
  isSourceApplicationStateRegistration,
  isSourceApplicationTransition,
  isSourceAttempt,
  isSourceDelivery,
  isSourceItemRejection,
  isSourceMutation,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceTransitionDelivery,
  makeSourceUpsert,
  markSourceToolkit,
  resolveSourceApplicationTransition,
  resolveSourceApplicationStateRegistration,
  resolveSourceMaintenanceOperation,
  currentEpochNanos,
  epochNanosFromWallMillis as epochNanos,
  validateSourceDefinition,
  type SourceAdapterRuntimeService,
  type SourceRuntimeLifecycle,
} from "@effect-view-server/source-adapter/internal";
import {
  BigDecimal,
  Cause,
  Chunk,
  Context,
  Deferred,
  Duration,
  Effect,
  Equal,
  Exit,
  Fiber,
  Option,
  Ref,
  Result,
  Schedule,
  Schema,
  Semaphore,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import type { RuntimeCoreBaseHealth } from "./health";
import { makeTopicSourceBindings } from "./source-binding-resolution";
import {
  acquireRuntimeCoreResourceHandoff,
  type RuntimeCoreResourceHandoffOptions,
} from "./subscription-handoff";

type SourceLifecycleDeclarationAny =
  import("@effect-view-server/source-adapter").SourceLifecycleDeclaration<
    unknown,
    unknown,
    unknown,
    SourceDefinitionOptionsFamily
  >;

type RuntimeSourceDefinition = SourceDefinition<
  import("@effect-view-server/source-adapter").SourceAdapterDescriptor<
    string,
    string | undefined,
    unknown,
    | import("@effect-view-server/source-adapter").SourceLifecycleDeclaration<
        unknown,
        unknown,
        unknown,
        SourceDefinitionOptionsFamily
      >
    | undefined,
    | import("@effect-view-server/source-adapter").SourceLifecycleDeclaration<
        unknown,
        unknown,
        unknown,
        SourceDefinitionOptionsFamily
      >
    | undefined
  >,
  "materialized" | "leased",
  unknown,
  ReadonlyArray<string>,
  never
>;

// `SourceDefinitionAny` intentionally erases adapter generics at the config boundary.
// Topic Source bindings only expose values accepted by the nominal definition validator.
function restoreRuntimeSourceDefinition(definition: SourceDefinitionAny): RuntimeSourceDefinition;
function restoreRuntimeSourceDefinition(definition: SourceDefinitionAny): SourceDefinitionAny {
  return definition;
}

type RuntimeLifecycle = SourceRuntimeLifecycle<
  unknown,
  "materialized" | "leased",
  SourceLifecycleDeclarationAny,
  unknown,
  unknown
>;
type RuntimeService = SourceAdapterRuntimeService<
  unknown,
  SourceLifecycleDeclarationAny | undefined,
  SourceLifecycleDeclarationAny | undefined
>;

type SourceRuntimeError = import("@effect-view-server/source-adapter").SourceRuntimeFailure;
type SourceExecutionError =
  import("@effect-view-server/source-adapter").SourceExecutionFailure<unknown>;
type SourceMutation = import("@effect-view-server/source-adapter").SourceMutation<object>;
type SourceLane = import("@effect-view-server/source-adapter").SourceDeliveryLane<
  object,
  unknown,
  unknown
>;
type RuntimeSourceHealth = SourceHealth<
  unknown,
  Readonly<Record<string, unknown>>,
  unknown,
  unknown
>;
type RuntimeLeasedSourceHealthResult = LeasedSourceHealthResult<
  Readonly<Record<string, unknown>>,
  RuntimeSourceHealth
>;

type SourceDefinitionRequirements<Definition> =
  SourceDefinitionAdapter<Definition> extends infer Adapter
    ? Adapter extends {
        readonly runtimeService: infer AdapterRuntimeService;
      }
      ? AdapterRuntimeService extends Context.Service.Any
        ?
            | Context.Service.Identifier<AdapterRuntimeService>
            | SourceDefinitionRetryServices<Definition>
        : SourceDefinitionRetryServices<Definition>
      : SourceDefinitionRetryServices<Definition>
    : never;

type TopicSourceRequirements<Topic> = Topic extends {
  readonly source: infer Definition extends SourceDefinitionAny;
}
  ? SourceDefinitionRequirements<Definition>
  : never;

export type ViewServerSourceRequirements<Topics extends object> = {
  readonly [Topic in keyof Topics]: TopicSourceRequirements<Topics[Topic]>;
}[keyof Topics];

type SourceRuntimeEntry = {
  readonly topic: string;
  readonly schema: RowSchema;
  readonly definition: RuntimeSourceDefinition;
  readonly service: RuntimeService;
  readonly lifecycle: RuntimeLifecycle;
  readonly declaration: SourceLifecycleDeclarationAny;
};

export type SourceRuntimeRouteEntry = {
  readonly topic: string;
  readonly schema: RowSchema;
  readonly definition: {
    readonly routeBy: ReadonlyArray<string>;
  };
};

type SourceLaneCounters = {
  buffer: SourceLane["bufferMetrics"];
};

type SourceMutationOperations = {
  readonly publishRows: (
    topic: string,
    rows: ReadonlyArray<object>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishRowsWithStorageKeys: (
    topic: string,
    rows: ReadonlyArray<{ readonly storageKey: string; readonly row: object }>,
    partitionKey: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: (topic: string, id: string) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly deleteStorageKey: (
    topic: string,
    storageKey: string,
    partitionKey: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
};

type AppliedSourceMutation =
  | {
      readonly _tag: "Upsert";
      readonly id: string;
    }
  | {
      readonly _tag: "Delete";
      readonly id: string;
    };

type PreparedSourceMutation =
  | {
      readonly _tag: "Upsert";
      readonly id: string;
      readonly row: object;
    }
  | {
      readonly _tag: "Delete";
      readonly id: string;
    };

type SourceLogicalRuntime = {
  readonly entry: SourceRuntimeEntry;
  readonly target: SourceTarget<Readonly<Record<string, unknown>>>;
  readonly health: SubscriptionRef.SubscriptionRef<Option.Option<RuntimeSourceHealth>>;
  readonly status: SubscriptionRef.SubscriptionRef<SourceStatus<unknown, unknown>>;
  readonly activate: Effect.Effect<void>;
  readonly run: Effect.Effect<void>;
  readonly stop: (
    reason: import("@effect-view-server/source-adapter").SourceStoppingReason,
  ) => Effect.Effect<void>;
};

type SourceLease = {
  readonly feedKey: string;
  readonly partition: ColumnLiveViewEngineQueryPartition;
  readonly route: Readonly<Record<string, unknown>>;
  readonly runtime: SourceLogicalRuntime;
  readonly translate: <Row extends object>(
    subscription: ViewServerLiveSubscription<Row>,
    query: Readonly<Record<string, unknown>>,
    queryId: string,
  ) => ViewServerLiveSubscription<Row>;
  readonly release: Effect.Effect<void>;
};

export type RuntimeCoreSourceLease = Pick<SourceLease, "partition" | "release" | "translate">;

export type RuntimeCoreSourceManager<Topics extends TopicDefinitions> = {
  readonly hasSources: boolean;
  readonly acquireLeased: (
    topic: Extract<keyof Topics, string>,
    query: Readonly<Record<string, unknown>>,
    registerAcquired?: (finalizer: Effect.Effect<void>) => Effect.Effect<void>,
  ) => Effect.Effect<Option.Option<RuntimeCoreSourceLease>, ViewServerRuntimeError>;
  readonly subscribeSourceHealth: ViewServerSourceHealthSubscriber<Topics, ViewServerRuntimeError>;
  readonly subscribeProtocolSourceHealth: (
    topic: string,
    route: ReadonlyArray<Readonly<Record<string, unknown>>>,
  ) => Effect.Effect<ViewServerSourceHealthSubscription<unknown>, ViewServerRuntimeError>;
  readonly decorateMaterialized: <Row extends object>(
    topic: string,
    subscription: ViewServerLiveSubscription<Row>,
    queryId: string,
  ) => ViewServerLiveSubscription<Row>;
  readonly overlayHealth: (health: RuntimeCoreBaseHealth<Topics>) => ViewServerHealth<Topics>;
  readonly close: Effect.Effect<void>;
  readonly fatal: Effect.Effect<never, ViewServerRuntimeError>;
};

export type RuntimeCoreSourceManagerConstructionOptions = {
  readonly handoff?: RuntimeCoreResourceHandoffOptions;
  readonly leaseHandoff?: RuntimeCoreResourceHandoffOptions;
  readonly leaseSubscriberHandoff?: RuntimeCoreResourceHandoffOptions;
  readonly sourceHealthHandoff?: RuntimeCoreResourceHandoffOptions;
  readonly afterApplicationStateBind?: Effect.Effect<void>;
  readonly afterMutationApplication?: Effect.Effect<void>;
  readonly afterSourceHealthObservationClose?: Effect.Effect<void>;
  readonly settlementHandoff?: {
    readonly afterApplicationExit?: Effect.Effect<void>;
    readonly afterCallbackApplication?: Effect.Effect<void>;
    readonly afterHandoffCompleted?: Effect.Effect<void>;
    readonly afterHandoffObserved?: Effect.Effect<void>;
    readonly beforeReturnedEffectRestore?: Effect.Effect<void>;
    readonly afterFatalCompleted?: Effect.Effect<void>;
    readonly afterSettlementChildFork?: Effect.Effect<void>;
    readonly afterSettlementChildRegistration?: Effect.Effect<void>;
    readonly afterFailureCounted?: (failedSettlementCount: bigint) => Effect.Effect<void>;
    readonly beforeOrdinaryTerminationClaim?: Effect.Effect<void>;
    readonly duringReturnedEffectPromotion?: Effect.Effect<void>;
  };
  readonly afterAttemptCancellationRequested?: Effect.Effect<void>;
  readonly beforeAttemptTerminationClaim?: Effect.Effect<void>;
};

const runtimeError = (
  topic: string,
  message: string,
  code: "InvalidQuery" | "RuntimeUnavailable" = "RuntimeUnavailable",
): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code,
  topic,
  message,
});

const fatalRuntimeCause = <Error>(
  topic: string,
  cause: Cause.Cause<Error>,
  message: string,
): Cause.Cause<ViewServerRuntimeError> => {
  const rootFailure = runtimeError(topic, message);
  const preserved = Cause.map(cause, () => rootFailure);
  return Option.isSome(Cause.findErrorOption(preserved))
    ? preserved
    : Cause.combine(Cause.fail(rootFailure), preserved);
};

const maintenanceSupervisorCause = (
  topic: string,
  exit: Exit.Exit<unknown, ViewServerRuntimeError>,
  closing: boolean,
): Cause.Cause<ViewServerRuntimeError> | undefined => {
  if (Exit.isFailure(exit)) {
    return Cause.hasInterruptsOnly(exit.cause) && closing
      ? undefined
      : fatalRuntimeCause(
          topic,
          exit.cause,
          "Source maintenance supervisor failed fatally and closed the complete runtime.",
        );
  }
  return Cause.fail(
    runtimeError(
      topic,
      "Source maintenance supervisor stopped unexpectedly and closed the complete runtime.",
    ),
  );
};

const fatalSourceApplicationCause = (
  topic: string,
  cause: Cause.Cause<SourceRuntimeError>,
  message: string,
): Cause.Cause<ViewServerRuntimeError> => {
  return fatalRuntimeCause(topic, cause, message);
};

const sourceRuntimeFailure = (failure: SourceRuntimeError): SourceExecutionError => ({
  _tag: "RuntimeFailure",
  failure,
});

const sourceApplicationFailure = (message: string): SourceRuntimeError => ({
  _tag: "InvalidSourceDelivery",
  message,
});

const equalRouteValue = (left: unknown, right: unknown): boolean => {
  if (BigDecimal.isBigDecimal(left)) {
    return (
      BigDecimal.isBigDecimal(right) &&
      left.value === right.value &&
      Object.is(left.scale, right.scale)
    );
  }
  return Object.is(left, right);
};

const routeMatchesRow = (
  fields: ReadonlyArray<string>,
  route: Readonly<Record<string, unknown>>,
  row: object,
): boolean => fields.every((field) => equalRouteValue(route[field], Reflect.get(row, field)));

const copyRoute = (
  fields: ReadonlyArray<string>,
  value: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> => {
  const route: Record<string, unknown> = {};
  for (const field of fields) {
    Object.defineProperty(route, field, {
      configurable: false,
      enumerable: true,
      value: value[field],
      writable: false,
    });
  }
  return Object.freeze(route);
};

const sourceDefinitionFailure = (topic: string, message: string): SourceRuntimeError => ({
  _tag: "InvalidSourceDefinition",
  message: `${topic}: ${message}`,
});

const sourceDefinitionError = (topic: string, message: string): SourceExecutionError =>
  sourceRuntimeFailure(sourceDefinitionFailure(topic, message));

const sourceRowFailure = (topic: string, message: string): SourceRuntimeError => ({
  _tag: "InvalidTopicRow",
  topic,
  message,
});

const sourceRowError = (topic: string, message: string): SourceExecutionError =>
  sourceRuntimeFailure(sourceRowFailure(topic, message));

const sourceIdFailure = (topic: string): SourceRuntimeError => ({
  _tag: "InvalidCanonicalId",
  topic,
  message: `Source Topic ${topic} requires a non-empty canonical string id.`,
});

const sourceIdError = (topic: string): SourceExecutionError =>
  sourceRuntimeFailure(sourceIdFailure(topic));

const sourceRouteFailure = (topic: string): SourceRuntimeError => ({
  _tag: "InvalidFeedRoute",
  topic,
  message: `Source Topic ${topic} row does not match the acquired Feed Route.`,
});

const sourceRouteError = (topic: string): SourceExecutionError =>
  sourceRuntimeFailure(sourceRouteFailure(topic));

const declarationFor = (
  definition: RuntimeSourceDefinition,
): SourceLifecycleDeclarationAny | undefined =>
  definition.lifecycle === "materialized"
    ? definition.adapter.materialized
    : definition.adapter.leased;

const lifecycleFor = (
  definition: RuntimeSourceDefinition,
  service: RuntimeService,
): RuntimeLifecycle | undefined =>
  definition.lifecycle === "materialized" ? service.materialized : service.leased;

const resolveEntries = Effect.fn("ViewServerRuntimeCore.source.entries.resolve")(function* <
  Topics extends TopicDefinitions,
>(
  config: ViewServerTopicConfig<Topics>,
  context: Context.Context<ViewServerSourceRequirements<Topics>>,
) {
  const entries = new Map<string, SourceRuntimeEntry>();
  for (const [topic, topicDefinition] of Object.entries(config.topics)) {
    if (
      typeof topicDefinition === "object" &&
      topicDefinition !== null &&
      Object.hasOwn(topicDefinition, "source") &&
      !validateSourceDefinition(Reflect.get(topicDefinition, "source"))
    ) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source-owned Topic ${topic} has an invalid Source Definition envelope.`,
        ),
      );
    }
  }
  for (const [topic, binding] of makeTopicSourceBindings(config)) {
    const definition = binding.source;
    if (definition === undefined) {
      continue;
    }
    const runtimeDefinition = restoreRuntimeSourceDefinition(definition);
    if (binding.schema === undefined) {
      return yield* Effect.fail(
        runtimeError(topic, `Source-owned Topic ${topic} has no valid row Schema.`),
      );
    }
    const serviceOption = Context.getOption(context, runtimeDefinition.adapter.runtimeService);
    if (Option.isNone(serviceOption)) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service ${runtimeDefinition.identity.name} is missing.`,
        ),
      );
    }
    const service = serviceOption.value;
    if (service.adapter !== runtimeDefinition.adapter) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service for ${runtimeDefinition.identity.name} does not match its nominal definition descriptor.`,
        ),
      );
    }
    const lifecycle = lifecycleFor(runtimeDefinition, service);
    const declaration = declarationFor(runtimeDefinition);
    if (lifecycle === undefined || declaration === undefined) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service does not implement declared ${runtimeDefinition.lifecycle} lifecycle.`,
        ),
      );
    }
    const requiresApplicationState = declaration.applicationState === "required";
    const hasApplicationState = isSourceApplicationStateRegistration(lifecycle.applicationState);
    if (requiresApplicationState !== hasApplicationState) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          "Source Adapter runtime service Application State registration does not match its lifecycle declaration.",
        ),
      );
    }
    entries.set(topic, {
      topic,
      schema: binding.schema,
      definition: runtimeDefinition,
      service,
      lifecycle,
      declaration,
    });
  }
  return entries;
});

type MetricFailureRegistration =
  | {
      readonly _tag: "Failed";
      readonly failure: SourceExecutionError;
    }
  | {
      readonly _tag: "Registered";
    };

const makeMetricFailureObservation = () => {
  const lock = Semaphore.makeUnsafe(1);
  let latest: Result.Result<void, SourceExecutionError> = Result.succeed(undefined);
  let active: Deferred.Deferred<SourceExecutionError> | undefined;

  const record = Effect.fn("ViewServerRuntimeCore.source.metrics.observation.record")(function* (
    result: Result.Result<void, SourceExecutionError>,
  ) {
    const failure = Result.isFailure(result) ? result.failure : undefined;
    const signal = yield* lock.withPermit(
      Effect.sync(() => {
        latest = result;
        return failure === undefined ? undefined : active;
      }),
    );
    if (failure !== undefined && signal !== undefined) {
      yield* Deferred.succeed(signal, failure).pipe(Effect.asVoid);
    }
  });

  const register = Effect.fn("ViewServerRuntimeCore.source.metrics.observation.register")(
    function* (signal: Deferred.Deferred<SourceExecutionError>) {
      const failure = yield* lock.withPermit(
        Effect.sync(() => {
          if (Result.isFailure(latest)) {
            return latest.failure;
          }
          active = signal;
          return undefined;
        }),
      );
      return failure === undefined
        ? ({
            _tag: "Registered",
          } satisfies MetricFailureRegistration)
        : ({
            _tag: "Failed",
            failure,
          } satisfies MetricFailureRegistration);
    },
  );

  const unregister = Effect.fn("ViewServerRuntimeCore.source.metrics.observation.unregister")(
    function* (signal: Deferred.Deferred<SourceExecutionError>) {
      yield* lock.withPermit(
        Effect.sync(() => {
          if (active === signal) {
            active = undefined;
          }
        }),
      );
    },
  );

  return {
    record,
    register,
    unregister,
  } as const;
};

const initialLaneMetrics = (): readonly [SourceLaneRuntimeMetrics] => [
  {
    id: "source",
    buffer: { _tag: "Unbuffered" },
  },
];

const internalStorageKey = (topic: string, feedKey: string, id: string): string =>
  `source/${encodeURIComponent(topic)}/${encodeURIComponent(feedKey)}/${encodeURIComponent(id)}`;

const internalPublicId = (storageKey: string): string | undefined => {
  if (!storageKey.startsWith("source/")) {
    return undefined;
  }
  const separator = storageKey.lastIndexOf("/");
  if (separator < "source/".length) {
    return undefined;
  }
  const decoded = Result.try(() => decodeURIComponent(storageKey.slice(separator + 1)));
  return Result.isSuccess(decoded) ? decoded.success : undefined;
};

const publicId = (row: object): string | undefined => {
  const id = Result.try(() => Reflect.get(row, "id"));
  return Result.isSuccess(id) && typeof id.success === "string" ? id.success : undefined;
};

type LogicalRuntimeInput<Topics extends TopicDefinitions> = {
  readonly entry: SourceRuntimeEntry;
  readonly target: SourceTarget<Readonly<Record<string, unknown>>>;
  readonly mutations: SourceMutationOperations;
  readonly context: Context.Context<ViewServerSourceRequirements<Topics>>;
  readonly partitionKey?: string;
  readonly feedKey?: string;
  readonly feedRouteReference?: string;
  readonly ownedStorageKeys?: Set<string>;
  readonly ownerScope: Scope.Scope;
  readonly constructionOptions: RuntimeCoreSourceManagerConstructionOptions;
  readonly onHealth: (health: RuntimeSourceHealth) => Effect.Effect<void>;
  readonly onStatus: (status: SourceStatus<unknown, unknown>) => Effect.Effect<void>;
  readonly onFatal: (cause: Cause.Cause<ViewServerRuntimeError>) => Effect.Effect<void>;
};

type SourceAttemptArbitrationState = "Open" | "CancellationWon" | "OrdinaryTerminationWon";

type SourceAttemptArbitration = {
  readonly cancel: Effect.Effect<boolean>;
  readonly cancellationWon: Effect.Effect<boolean>;
  readonly claimOrdinaryTermination: Effect.Effect<boolean>;
  readonly promoteReturnedEffect: (fiber: Fiber.Fiber<unknown, unknown>) => Effect.Effect<boolean>;
  readonly settlementFibers: Map<
    Fiber.Fiber<unknown, unknown>,
    "CallbackHandoff" | "ReturnedEffect"
  >;
};

const makeSourceAttemptArbitration = Effect.fn(
  "ViewServerRuntimeCore.source.attempt.arbitration.make",
)(function* (duringReturnedEffectPromotion: Effect.Effect<void> = Effect.void) {
  const state = yield* Ref.make<SourceAttemptArbitrationState>("Open");
  const lock = Semaphore.makeUnsafe(1);
  const settlementFibers = new Map<
    Fiber.Fiber<unknown, unknown>,
    "CallbackHandoff" | "ReturnedEffect"
  >();
  const claimCancellation = Ref.modify(state, (current) => {
    const result: readonly [boolean, SourceAttemptArbitrationState] =
      current === "Open" ? [true, "CancellationWon"] : [false, current];
    return result;
  });
  const cancel = lock
    .withPermit(
      Effect.gen(function* () {
        const cancellationWon = yield* claimCancellation;
        yield* Effect.sync(() => {
          for (const [fiber, phase] of settlementFibers) {
            if (phase === "ReturnedEffect") {
              fiber.interruptUnsafe();
            }
          }
        });
        return cancellationWon;
      }),
    )
    .pipe(Effect.uninterruptible);
  const promoteReturnedEffect = (fiber: Fiber.Fiber<unknown, unknown>): Effect.Effect<boolean> =>
    lock
      .withPermit(
        Effect.gen(function* () {
          const cancellationWon = yield* Ref.get(state).pipe(
            Effect.map((current) => current === "CancellationWon"),
          );
          if (cancellationWon) {
            return true;
          }
          yield* duringReturnedEffectPromotion;
          yield* Effect.sync(() => {
            settlementFibers.set(fiber, "ReturnedEffect");
          });
          return false;
        }),
      )
      .pipe(Effect.uninterruptible);
  const claimOrdinaryTermination = Ref.modify(state, (current) => {
    const result: readonly [boolean, SourceAttemptArbitrationState] =
      current === "CancellationWon"
        ? [false, current]
        : [true, current === "Open" ? "OrdinaryTerminationWon" : current];
    return result;
  });
  return {
    cancel,
    cancellationWon: Ref.get(state).pipe(Effect.map((current) => current === "CancellationWon")),
    claimOrdinaryTermination,
    promoteReturnedEffect,
    settlementFibers,
  } satisfies SourceAttemptArbitration;
});

function snapshotDecodedMetrics<Value>(value: Value): Value;
function snapshotDecodedMetrics(value: unknown, active?: WeakSet<object>): unknown;
function snapshotDecodedMetrics(value: unknown, active = new WeakSet<object>()): unknown {
  if (Array.isArray(value)) {
    if (active.has(value)) {
      throw new TypeError("Source Adapter metrics must not contain cycles.");
    }
    active.add(value);
    const snapshot = value.map((entry) => snapshotDecodedMetrics(entry, active));
    active.delete(value);
    return Object.freeze(snapshot);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  if (BigDecimal.isBigDecimal(value)) {
    return Object.freeze(BigDecimal.make(value.value, value.scale));
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError("Source Adapter metrics must contain only immutable data values.");
  }
  if (active.has(value)) {
    throw new TypeError("Source Adapter metrics must not contain cycles.");
  }
  active.add(value);
  const snapshot: Record<string, unknown> = {};
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string") {
      throw new TypeError("Source Adapter metrics must use string data fields.");
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
      throw new TypeError("Source Adapter metrics must contain enumerable data properties.");
    }
    Object.defineProperty(snapshot, key, {
      enumerable: true,
      value: snapshotDecodedMetrics(descriptor.value, active),
    });
  }
  active.delete(value);
  return Object.freeze(snapshot);
}

function freezeDecodedMetrics<Value>(value: Value): Value;
function freezeDecodedMetrics(value: unknown, active?: WeakSet<object>): unknown;
function freezeDecodedMetrics(value: unknown, active = new WeakSet<object>()): unknown {
  if (typeof value !== "object" || value === null || Object.isFrozen(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      throw new TypeError("Source Adapter metrics must not contain cycles.");
    }
    active.add(value);
    for (const entry of value) {
      freezeDecodedMetrics(entry, active);
    }
    active.delete(value);
    return Object.freeze(value);
  }
  if (active.has(value)) {
    throw new TypeError("Source Adapter metrics must not contain cycles.");
  }
  active.add(value);
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string") {
      throw new TypeError("Source Adapter metrics must use string data fields.");
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
      throw new TypeError("Source Adapter metrics must contain enumerable data properties.");
    }
    freezeDecodedMetrics(descriptor.value, active);
  }
  active.delete(value);
  return Object.freeze(value);
}

const makeLogicalRuntime = Effect.fn("ViewServerRuntimeCore.source.makeLogical")(function* <
  Topics extends TopicDefinitions,
>(input: LogicalRuntimeInput<Topics>) {
  const startedAtNanos = yield* currentEpochNanos;
  let currentAttempt = 1n;
  let retryCount = 0n;
  let receivedDeliveryCount = 0n;
  let rejectedItemCount = 0n;
  let attemptedMutationCount = 0n;
  let appliedUpsertCount = 0n;
  let appliedDeleteCount = 0n;
  let failedMutationCount = 0n;
  let completedSettlementCount = 0n;
  let failedSettlementCount = 0n;
  let lastAttemptStartedAtNanos = startedAtNanos;
  let lastDeliveryAtNanos: bigint | null = null;
  let lastRejectionAtNanos: bigint | null = null;
  let lastAppliedMutationAtNanos: bigint | null = null;
  let lastTerminationAtNanos: bigint | null = null;
  let degradedAtNanos: bigint | undefined;
  let latestRejection:
    | import("@effect-view-server/source-adapter").SourceItemRejectionDiagnostic<unknown, unknown>
    | undefined;
  let cachedAdapterMetrics: unknown;
  let hasCachedAdapterMetrics = false;
  const laneCounters = new Map<string, SourceLaneCounters>();
  let lastValidLaneMetrics: readonly [
    SourceLaneRuntimeMetrics,
    ...ReadonlyArray<SourceLaneRuntimeMetrics>,
  ] = initialLaneMetrics();
  let stableLaneIds: ReadonlyArray<string> | undefined;
  const materializedRetainedIds = new Set<string>();
  const scope = yield* Scope.fork(input.ownerScope, "sequential");
  const applicationStateRegistration =
    input.entry.lifecycle.applicationState === undefined
      ? undefined
      : resolveSourceApplicationStateRegistration(input.entry.lifecycle.applicationState);
  let applicationLifetimeIdentity: object | undefined;
  if (input.entry.lifecycle.applicationState !== undefined) {
    if (applicationStateRegistration === undefined) {
      return yield* Effect.fail(
        runtimeError(
          input.entry.topic,
          "Source Application State registration lost its nominal linkage.",
        ),
      );
    }
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        yield* Effect.try({
          try: () =>
            applicationStateRegistration.bind({
              topic: input.entry.topic,
              definition: input.entry.definition.options,
              lifetimeScope: scope,
              target: input.target,
            }),
          catch: () =>
            runtimeError(
              input.entry.topic,
              "Source Application State registration could not initialize its logical lifetime.",
            ),
        });
        yield* input.constructionOptions.afterApplicationStateBind ?? Effect.void;
        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            applicationStateRegistration.unbind(scope);
          }),
        );
      }),
    );
    applicationLifetimeIdentity = yield* Effect.try({
      try: () => applicationStateRegistration.lifetimeIdentity(scope),
      catch: () =>
        runtimeError(
          input.entry.topic,
          "Source Application State registration could not identify its logical lifetime.",
        ),
    });
  }
  const metricFailureObservation = makeMetricFailureObservation();
  let declaredLaneFailure: SourceExecutionError | undefined;
  let cancelActiveAttempt: Effect.Effect<boolean> = Effect.succeed(false);
  const supervisorFiber = yield* Deferred.make<Fiber.Fiber<void>>();
  let maintenanceFiber: Fiber.Fiber<void> | undefined;
  const healthLock = Semaphore.makeUnsafe(1);
  const lifecycleGate = Semaphore.makeUnsafe(1);
  const failedMaintenanceWork = new Set<string>();
  let maintenanceActive = false;
  let closing = false;

  const validateLaneBufferMetrics = Effect.fn(
    "ViewServerRuntimeCore.source.metrics.buffer.validate",
  )(function* (lane: string, metrics: unknown) {
    const decoded = yield* Schema.decodeUnknownEffect(SourceBufferMetricsSchema)(metrics).pipe(
      Effect.mapError(() =>
        sourceRuntimeFailure({
          _tag: "InvalidSourceMetrics",
          message: `Source Adapter ${input.entry.definition.identity.name} lane ${lane} returned buffer metrics outside the Source Buffer Metrics Schema.`,
        }),
      ),
    );
    return Object.freeze(decoded);
  });

  const runtimeMetricsFromLanes = (
    lanes: readonly [SourceLaneRuntimeMetrics, ...ReadonlyArray<SourceLaneRuntimeMetrics>],
  ): SourceRuntimeMetrics => ({
    startedAtNanos,
    lastAttemptStartedAtNanos,
    lastDeliveryAtNanos,
    lastRejectionAtNanos,
    lastAppliedMutationAtNanos,
    lastTerminationAtNanos,
    currentAttempt,
    retryCount,
    receivedDeliveryCount,
    rejectedItemCount,
    attemptedMutationCount,
    appliedUpsertCount,
    appliedDeleteCount,
    failedMutationCount,
    completedSettlementCount,
    failedSettlementCount,
    retainedRowCount: input.ownedStorageKeys?.size ?? materializedRetainedIds.size,
    lanes,
  });
  const runtimeMetrics = Effect.fn("ViewServerRuntimeCore.source.metrics.runtime")(function* () {
    const lanes: Array<SourceLaneRuntimeMetrics> = [];
    for (const [id, counters] of laneCounters) {
      lanes.push({
        id,
        buffer: yield* counters.buffer.pipe(
          Effect.flatMap((metrics) => validateLaneBufferMetrics(id, metrics)),
        ),
      });
    }
    lanes.sort((left, right) => left.id.localeCompare(right.id));
    const nonEmptyLanes: readonly [
      SourceLaneRuntimeMetrics,
      ...ReadonlyArray<SourceLaneRuntimeMetrics>,
    ] = lanes.length === 0 ? initialLaneMetrics() : [lanes[0]!, ...lanes.slice(1)];
    lastValidLaneMetrics = nonEmptyLanes;
    return runtimeMetricsFromLanes(nonEmptyLanes);
  });
  const initializeDeclaredLanes = Effect.fn(
    "ViewServerRuntimeCore.source.lanes.initializeDeclared",
  )(() =>
    Effect.sync(() => {
      const initialLaneIds = input.entry.lifecycle.initialLaneIds;
      if (initialLaneIds === undefined) {
        return;
      }
      const candidate = Result.try(() =>
        initialLaneIds({
          topic: input.entry.topic,
          definition: input.entry.definition.options,
          lifetimeScope: scope,
          target: input.target,
        }),
      );
      if (Result.isFailure(candidate)) {
        return sourceDefinitionError(
          input.entry.topic,
          "Source Adapter initial Lane IDs must be returned without throwing.",
        );
      }
      const snapshot = Result.try(() => Array.from(candidate.success));
      if (
        Result.isFailure(snapshot) ||
        snapshot.success.length === 0 ||
        snapshot.success.some((laneId) => typeof laneId !== "string" || laneId.length === 0) ||
        new Set(snapshot.success).size !== snapshot.success.length
      ) {
        return sourceDefinitionError(
          input.entry.topic,
          "Source Adapter initial Lane IDs must be non-empty, unique strings.",
        );
      }
      const laneIds = snapshot.success.sort((left, right) => left.localeCompare(right));
      stableLaneIds = laneIds;
      for (const laneId of laneIds) {
        laneCounters.set(laneId, {
          buffer: Effect.succeed({ _tag: "Unbuffered" }),
        });
      }
      return undefined;
    }),
  );

  const initialStatus: SourceStatus<unknown, unknown> = {
    _tag: "Starting",
    attempt: 1n,
    startedAtNanos,
  };
  const status = yield* SubscriptionRef.make<SourceStatus<unknown, unknown>>(initialStatus);
  const health = yield* SubscriptionRef.make<Option.Option<RuntimeSourceHealth>>(Option.none());

  const publishHealth = Effect.fn("ViewServerRuntimeCore.source.health.publishSnapshot")(function* (
    status: SourceStatus<unknown, unknown>,
    adapterMetrics: unknown,
  ) {
    const sampledAtNanos = yield* currentEpochNanos;
    const runtimeMetricsResult = yield* runtimeMetrics().pipe(Effect.result);
    if (Result.isFailure(runtimeMetricsResult)) {
      yield* metricFailureObservation.record(Result.fail(runtimeMetricsResult.failure));
    }
    const nextHealth: RuntimeSourceHealth = {
      adapter: input.entry.definition.identity,
      target: input.target,
      status,
      metrics: {
        runtime: Result.isSuccess(runtimeMetricsResult)
          ? runtimeMetricsResult.success
          : runtimeMetricsFromLanes(lastValidLaneMetrics),
        adapter: adapterMetrics,
      },
      sampledAtNanos,
    };
    yield* SubscriptionRef.set(health, Option.some(nextHealth));
    yield* input.onHealth(nextHealth);
  });

  const publish = Effect.fn("ViewServerRuntimeCore.source.health.publishStatus")(function* (
    nextStatus: SourceStatus<unknown, unknown>,
  ) {
    yield* healthLock.withPermit(
      Effect.gen(function* () {
        if (closing && nextStatus._tag !== "Stopping") {
          return;
        }
        yield* SubscriptionRef.set(status, nextStatus);
        if (hasCachedAdapterMetrics) {
          yield* publishHealth(nextStatus, cachedAdapterMetrics);
        }
        yield* input.onStatus(nextStatus);
      }),
    );
    yield* Effect.yieldNow;
  });

  const degradationReasons = ():
    | import("@effect-view-server/source-adapter").SourceDegradationReasons<unknown, unknown>
    | undefined => {
    const maintenance = failedMaintenanceWork.size > 0;
    if (latestRejection === undefined) {
      return maintenance
        ? [
            {
              _tag: "AdapterMaintenanceFailure",
            },
          ]
        : undefined;
    }
    const rejection: import("@effect-view-server/source-adapter").SourceItemRejectionReason<
      unknown,
      unknown
    > = {
      _tag: "SourceItemRejection",
      latestRejection,
    };
    if (!maintenance) {
      return [rejection];
    }
    return [
      rejection,
      {
        _tag: "AdapterMaintenanceFailure",
      },
    ];
  };

  const sameDegradationReasons = (
    left: import("@effect-view-server/source-adapter").SourceDegradationReasons<unknown, unknown>,
    right: import("@effect-view-server/source-adapter").SourceDegradationReasons<unknown, unknown>,
  ): boolean => {
    const leftRejection =
      left[0]._tag === "SourceItemRejection" ? left[0].latestRejection : undefined;
    const rightRejection =
      right[0]._tag === "SourceItemRejection" ? right[0].latestRejection : undefined;
    const leftMaintenance = left.some((reason) => reason._tag === "AdapterMaintenanceFailure");
    const rightMaintenance = right.some((reason) => reason._tag === "AdapterMaintenanceFailure");
    return leftRejection === rightRejection && leftMaintenance === rightMaintenance;
  };

  const publishDegradationLedger = Effect.fn(
    "ViewServerRuntimeCore.source.health.publishDegradationLedger",
  )(function* () {
    const reasons = degradationReasons();
    const currentStatus = SubscriptionRef.getUnsafe(status);
    if (reasons === undefined) {
      degradedAtNanos = undefined;
      if (currentStatus._tag === "Ready") {
        return;
      }
      const readyAtNanos = yield* currentEpochNanos;
      yield* publish({
        _tag: "Ready",
        attempt: currentAttempt,
        readyAtNanos,
      });
      return;
    }
    degradedAtNanos ??= yield* currentEpochNanos;
    if (
      currentStatus._tag === "Degraded" &&
      currentStatus.attempt === currentAttempt &&
      currentStatus.degradedAtNanos === degradedAtNanos &&
      sameDegradationReasons(currentStatus.reasons, reasons)
    ) {
      return;
    }
    yield* publish({
      _tag: "Degraded",
      attempt: currentAttempt,
      degradedAtNanos,
      reasons,
    });
  });

  const validateAdapterMetrics = Effect.fn("ViewServerRuntimeCore.source.metrics.adapter.validate")(
    function* (metrics: unknown) {
      const codec = input.entry.declaration.metrics;
      const decoded = yield* Schema.decodeUnknownEffect(codec)(metrics).pipe(
        Effect.mapError(() =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics outside its declared Schema.`,
          }),
        ),
      );
      const encoded = yield* Schema.encodeUnknownEffect(codec)(decoded).pipe(
        Effect.mapError(() =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics that cannot be frozen.`,
          }),
        ),
      );
      const encodedSnapshot = yield* Effect.try({
        try: () => snapshotDecodedMetrics(encoded),
        catch: () =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics that cannot be frozen.`,
          }),
      });
      const snapshot = yield* Schema.decodeUnknownEffect(codec)(encodedSnapshot).pipe(
        Effect.mapError(() =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics that cannot be frozen.`,
          }),
        ),
      );
      const frozen = yield* Effect.try({
        try: () => freezeDecodedMetrics(snapshot),
        catch: () =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics that cannot be frozen.`,
          }),
      });
      yield* healthLock.withPermit(
        Effect.gen(function* () {
          // Adapter metrics are an opaque, Schema-validated snapshot. Keep
          // reactivity equality at that Adapter seam so Effect does not
          // structurally hash custom decoded values such as frozen BigDecimal.
          const reactiveSnapshot =
            typeof frozen === "object" && frozen !== null
              ? Equal.byReferenceUnsafe(frozen)
              : frozen;
          cachedAdapterMetrics = reactiveSnapshot;
          hasCachedAdapterMetrics = true;
          const currentStatus = SubscriptionRef.getUnsafe(status);
          yield* publishHealth(currentStatus, reactiveSnapshot);
          yield* input.onStatus(currentStatus);
        }),
      );
    },
  );
  const sampleAdapterMetrics = Effect.fn("ViewServerRuntimeCore.source.metrics.adapter.sample")(
    function* () {
      const exit = yield* Effect.exit(
        input.entry.lifecycle
          .metrics({
            topic: input.entry.topic,
            definition: input.entry.definition.options,
            lifetimeScope: scope,
            target: input.target,
          })
          .pipe(Effect.flatMap(validateAdapterMetrics)),
      );
      if (Exit.isSuccess(exit)) {
        yield* metricFailureObservation.record(Result.succeed(undefined));
        return;
      }
      if (Cause.hasInterruptsOnly(exit.cause)) {
        return yield* Effect.interrupt;
      }
      if (Cause.hasDies(exit.cause)) {
        yield* metricFailureObservation.record(
          Result.fail(
            sourceRuntimeFailure({
              _tag: "InvalidSourceMetrics",
              message: `Source Adapter ${input.entry.definition.identity.name} failed while sampling metrics.`,
            }),
          ),
        );
        return;
      }
      const typedFailure = Option.getOrThrow(Cause.findErrorOption(exit.cause));
      yield* metricFailureObservation.record(Result.fail(typedFailure));
    },
  );

  const validateFailure = Effect.fn("ViewServerRuntimeCore.source.failure.validate")(function* (
    failure: SourceExecutionError,
  ) {
    return yield* Schema.decodeUnknownEffect(
      sourceExecutionFailureSchema(input.entry.definition.adapter.failureSchema),
    )(failure).pipe(
      Effect.mapError(() =>
        sourceDefinitionError(
          input.entry.topic,
          "Source Execution Failure did not satisfy the SDK Schema.",
        ),
      ),
    );
  });

  const validateRejectionFailure = Effect.fn(
    "ViewServerRuntimeCore.source.rejection.failure.validate",
  )(function* (failure: SourceExecutionError) {
    return yield* Schema.decodeUnknownEffect(
      sourceExecutionFailureSchema(input.entry.definition.adapter.failureSchema),
    )(failure).pipe(
      Effect.mapError(() =>
        sourceDefinitionFailure(
          input.entry.topic,
          "Source Execution Failure did not satisfy the SDK Schema.",
        ),
      ),
    );
  });

  const makeToolkit = () => {
    const decodeUpsert = (candidate: unknown) =>
      validateDecodedRow(input.entry.schema, candidate).pipe(
        Effect.mapError(() =>
          sourceRowError(
            input.entry.topic,
            `Source Upsert does not satisfy Topic ${input.entry.topic} Schema.`,
          ),
        ),
        Effect.flatMap((row) => {
          const id = publicId(row);
          if (id === undefined || id.length === 0) {
            return Effect.fail(sourceIdError(input.entry.topic));
          }
          if (
            input.target._tag === "Leased" &&
            !routeMatchesRow(input.entry.definition.routeBy, input.target.route, row)
          ) {
            return Effect.fail(sourceRouteError(input.entry.topic));
          }
          return Effect.succeed(makeSourceUpsert(row));
        }),
      );
    function delivery(
      mutations: Chunk.NonEmptyChunk<SourceMutation>,
      settlement?: import("@effect-view-server/source-adapter").SourceSettlement<unknown>,
    ): Effect.Effect<
      import("@effect-view-server/source-adapter").SourceDelivery<object, unknown>,
      SourceExecutionError
    >;
    function delivery(
      mutation: SourceMutation,
      settlement:
        | import("@effect-view-server/source-adapter").SourceSettlement<unknown>
        | undefined,
      transition: import("@effect-view-server/source-adapter").SourceApplicationTransition,
    ): Effect.Effect<
      import("@effect-view-server/source-adapter").SourceDelivery<object, unknown>,
      SourceExecutionError
    >;
    function delivery(
      mutationsOrMutation: Chunk.Chunk<SourceMutation> | SourceMutation,
      settlement?: import("@effect-view-server/source-adapter").SourceSettlement<unknown>,
      transition?: import("@effect-view-server/source-adapter").SourceApplicationTransition,
    ): Effect.Effect<
      import("@effect-view-server/source-adapter").SourceDelivery<object, unknown>,
      SourceExecutionError
    > {
      if (settlement !== undefined && typeof settlement !== "function") {
        return Effect.fail(
          sourceDefinitionError(
            input.entry.topic,
            "Source Delivery settlement must be an Effect function.",
          ),
        );
      }
      if (transition === undefined) {
        if (
          !Chunk.isChunk(mutationsOrMutation) ||
          !Chunk.isNonEmpty(mutationsOrMutation) ||
          !Chunk.every(mutationsOrMutation, isSourceMutation)
        ) {
          return Effect.fail(
            sourceDefinitionError(
              input.entry.topic,
              "Source Delivery requires one or more nominal Source Mutations.",
            ),
          );
        }
        return Effect.succeed(makeSourceDelivery(mutationsOrMutation, settlement));
      }
      if (!isSourceMutation(mutationsOrMutation) || !isSourceApplicationTransition(transition)) {
        return Effect.fail(
          sourceDefinitionError(
            input.entry.topic,
            "Source Application Transition requires exactly one nominal Source Mutation.",
          ),
        );
      }
      return Effect.succeed(
        makeSourceTransitionDelivery(mutationsOrMutation, settlement, transition),
      );
    }
    return markSourceToolkit<object, unknown, unknown, never, string>({
      topic: input.entry.topic,
      upsert: decodeUpsert,
      decodeUpsert,
      delete: (id) =>
        typeof id === "string" && id.length > 0
          ? Effect.succeed(makeSourceDelete(id))
          : Effect.fail(sourceIdError(input.entry.topic)),
      delivery,
      reject: (rejection) =>
        Effect.gen(function* () {
          if (rejection.settlement !== undefined && typeof rejection.settlement !== "function") {
            return yield* Effect.fail(
              sourceDefinitionError(
                input.entry.topic,
                "Source Rejection settlement must be an Effect function.",
              ),
            );
          }
          const failure = yield* validateFailure(rejection.failure);
          const location = yield* Schema.decodeUnknownEffect(
            input.entry.declaration.rejectionLocation,
          )(rejection.location).pipe(
            Effect.mapError(() =>
              sourceDefinitionError(
                input.entry.topic,
                "Source Rejection Location does not satisfy its declared Schema.",
              ),
            ),
          );
          if (typeof rejection.rejectedAtNanos !== "bigint" || rejection.rejectedAtNanos < 0n) {
            return yield* Effect.fail(
              sourceDefinitionError(
                input.entry.topic,
                "Source Rejection timestamp must be non-negative epoch nanoseconds.",
              ),
            );
          }
          return makeSourceItemRejection({
            failure,
            location,
            rejectedAtNanos: rejection.rejectedAtNanos,
            ...(rejection.settlement === undefined ? {} : { settlement: rejection.settlement }),
          });
        }),
    });
  };

  const applicationFailure = (error: ViewServerRuntimeError): SourceRuntimeError =>
    sourceApplicationFailure(error.message);

  const prepareMutationOperation = Effect.fn("ViewServerRuntimeCore.source.mutation.prepare")(
    function* (mutation: SourceMutation) {
      if (mutation._tag === "Upsert") {
        const row = yield* validateDecodedRow(input.entry.schema, mutation.row).pipe(
          Effect.mapError(() =>
            sourceRowFailure(
              input.entry.topic,
              `Source Upsert does not satisfy Topic ${input.entry.topic} Schema.`,
            ),
          ),
        );
        const id = publicId(row);
        if (id === undefined || id.length === 0) {
          return yield* Effect.fail({
            _tag: "InvalidCanonicalId",
            topic: input.entry.topic,
            message: `Source Topic ${input.entry.topic} requires a canonical string id.`,
          } satisfies SourceRuntimeError);
        }
        if (
          input.target._tag === "Leased" &&
          !routeMatchesRow(input.entry.definition.routeBy, input.target.route, row)
        ) {
          return yield* Effect.fail(sourceRouteFailure(input.entry.topic));
        }
        return {
          _tag: "Upsert",
          id,
          row,
        } satisfies PreparedSourceMutation;
      }
      if (mutation.id.length === 0) {
        return yield* Effect.fail(sourceIdFailure(input.entry.topic));
      }
      return {
        _tag: "Delete",
        id: mutation.id,
      } satisfies PreparedSourceMutation;
    },
  );

  const applyPreparedMutationOperation = Effect.fn(
    "ViewServerRuntimeCore.source.mutation.operation",
  )(function* (mutation: PreparedSourceMutation) {
    if (mutation._tag === "Upsert") {
      if (
        input.feedKey !== undefined &&
        input.partitionKey !== undefined &&
        input.ownedStorageKeys !== undefined
      ) {
        const ownedStorageKeys = input.ownedStorageKeys;
        const storageKey = internalStorageKey(input.entry.topic, input.feedKey, mutation.id);
        const previouslyOwned = ownedStorageKeys.has(storageKey);
        ownedStorageKeys.add(storageKey);
        yield* input.mutations
          .publishRowsWithStorageKeys(
            input.entry.topic,
            [{ storageKey, row: mutation.row }],
            input.partitionKey,
          )
          .pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                if (!previouslyOwned) {
                  ownedStorageKeys.delete(storageKey);
                }
              }),
            ),
            Effect.mapError(applicationFailure),
          );
        return {
          _tag: "Upsert",
          id: mutation.id,
        } satisfies AppliedSourceMutation;
      }
      yield* input.mutations
        .publishRows(input.entry.topic, [mutation.row])
        .pipe(Effect.mapError(applicationFailure));
      return {
        _tag: "Upsert",
        id: mutation.id,
      } satisfies AppliedSourceMutation;
    }
    if (
      input.feedKey !== undefined &&
      input.partitionKey !== undefined &&
      input.ownedStorageKeys !== undefined
    ) {
      const storageKey = internalStorageKey(input.entry.topic, input.feedKey, mutation.id);
      yield* input.mutations
        .deleteStorageKey(input.entry.topic, storageKey, input.partitionKey)
        .pipe(Effect.mapError(applicationFailure));
      input.ownedStorageKeys.delete(storageKey);
      return {
        _tag: "Delete",
        id: mutation.id,
      } satisfies AppliedSourceMutation;
    }
    yield* input.mutations
      .delete(input.entry.topic, mutation.id)
      .pipe(Effect.mapError(applicationFailure));
    return {
      _tag: "Delete",
      id: mutation.id,
    } satisfies AppliedSourceMutation;
  });

  const prepareMutation = Effect.fn("ViewServerRuntimeCore.source.mutation.validate")(function* (
    mutation: SourceMutation,
  ) {
    attemptedMutationCount += 1n;
    const preparation = yield* Effect.exit(prepareMutationOperation(mutation));
    if (Exit.isFailure(preparation)) {
      failedMutationCount += 1n;
      return yield* Effect.failCause(preparation.cause);
    }
    return preparation.value;
  });

  const applyPreparedMutation = Effect.fn("ViewServerRuntimeCore.source.mutation.apply")(function* (
    mutation: PreparedSourceMutation,
  ) {
    const application = yield* Effect.exit(applyPreparedMutationOperation(mutation));
    if (Exit.isFailure(application)) {
      failedMutationCount += 1n;
      return yield* Effect.failCause(application.cause);
    }
    yield* input.constructionOptions.afterMutationApplication ?? Effect.void;
    lastAppliedMutationAtNanos = yield* currentEpochNanos;
    if (application.value._tag === "Upsert") {
      appliedUpsertCount += 1n;
      if (input.ownedStorageKeys === undefined) {
        materializedRetainedIds.add(application.value.id);
      }
      return;
    }
    appliedDeleteCount += 1n;
    if (input.ownedStorageKeys === undefined) {
      materializedRetainedIds.delete(application.value.id);
    }
  });

  const applyMutation = Effect.fn("ViewServerRuntimeCore.source.mutation.prepareAndApply")(
    function* (mutation: SourceMutation) {
      const prepared = yield* prepareMutation(mutation);
      return yield* applyPreparedMutation(prepared);
    },
  );

  const signalMaintenanceFatal = Effect.fn("ViewServerRuntimeCore.source.maintenance.signalFatal")(
    function* (cause: Cause.Cause<ViewServerRuntimeError>) {
      maintenanceActive = false;
      return yield* input.onFatal(cause);
    },
  );

  const runMaintenance = Effect.fn("ViewServerRuntimeCore.source.maintenance.run")(function* (
    operation: import("@effect-view-server/source-adapter").SourceMaintenanceOperation,
  ): Effect.fn.Return<import("@effect-view-server/source-adapter").SourceMaintenanceResult> {
    return yield* lifecycleGate.withPermit(
      Effect.uninterruptibleMask(() =>
        Effect.gen(function* () {
          const internal = resolveSourceMaintenanceOperation(operation);
          if (internal === undefined) {
            yield* signalMaintenanceFatal(
              Cause.fail(
                runtimeError(
                  input.entry.topic,
                  "Source Maintenance Operation was not issued by the Source Adapter SDK.",
                ),
              ),
            );
            return {
              _tag: "Inactive",
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          if (
            internal.topic !== input.entry.topic ||
            internal.lifetimeIdentity !== applicationLifetimeIdentity
          ) {
            yield* signalMaintenanceFatal(
              Cause.fail(
                runtimeError(
                  input.entry.topic,
                  "Source Maintenance Operation is bound to a different Topic or logical lifetime.",
                ),
              ),
            );
            return {
              _tag: "Inactive",
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          const currentStatus = SubscriptionRef.getUnsafe(status);
          if (
            !maintenanceActive ||
            (currentStatus._tag !== "Ready" && currentStatus._tag !== "Degraded")
          ) {
            return {
              _tag: "Inactive",
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          const current = yield* Effect.exit(Effect.sync(internal.isCurrent));
          if (Exit.isFailure(current)) {
            yield* signalMaintenanceFatal(
              fatalRuntimeCause(
                input.entry.topic,
                current.cause,
                "Source maintenance state validation failed fatally and stopped the complete runtime.",
              ),
            );
            return {
              _tag: "Applied",
              exit: Exit.failCause(current.cause),
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          if (!current.value) {
            const stale = yield* Effect.exit(Effect.sync(internal.onStale));
            if (Exit.isFailure(stale)) {
              yield* signalMaintenanceFatal(
                fatalRuntimeCause(
                  input.entry.topic,
                  stale.cause,
                  "Source maintenance stale transition failed fatally and stopped the complete runtime.",
                ),
              );
              return {
                _tag: "Applied",
                exit: Exit.failCause(stale.cause),
              } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
            }
            failedMaintenanceWork.delete(internal.workId);
            yield* publishDegradationLedger();
            return {
              _tag: "Stale",
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          const applicationExit = yield* Effect.exit(applyMutation(makeSourceDelete(internal.id)));
          if (Exit.isSuccess(applicationExit)) {
            const transitioned = yield* Effect.exit(Effect.sync(internal.onSuccess));
            if (Exit.isFailure(transitioned)) {
              yield* signalMaintenanceFatal(
                fatalRuntimeCause(
                  input.entry.topic,
                  transitioned.cause,
                  "Source maintenance success transition failed fatally and stopped the complete runtime.",
                ),
              );
              return {
                _tag: "Applied",
                exit: Exit.failCause(transitioned.cause),
              } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
            }
            failedMaintenanceWork.delete(internal.workId);
            yield* publishDegradationLedger();
            return {
              _tag: "Applied",
              exit: applicationExit,
            } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
          }
          let maintenanceExit: import("@effect-view-server/source-adapter").SourceApplicationExit =
            applicationExit;
          if (!Cause.hasInterruptsOnly(applicationExit.cause)) {
            const transitioned = yield* Effect.exit(
              Effect.sync(() => internal.onFailure(applicationExit)),
            );
            if (Exit.isFailure(transitioned)) {
              const transitionCause = Cause.hasDies(applicationExit.cause)
                ? Cause.combine(transitioned.cause, applicationExit.cause)
                : transitioned.cause;
              yield* signalMaintenanceFatal(
                fatalRuntimeCause(
                  input.entry.topic,
                  transitionCause,
                  "Source maintenance failure transition failed fatally and stopped the complete runtime.",
                ),
              );
              maintenanceExit = Exit.failCause(transitionCause);
            } else if (Cause.hasDies(applicationExit.cause)) {
              yield* signalMaintenanceFatal(
                fatalSourceApplicationCause(
                  input.entry.topic,
                  applicationExit.cause,
                  "Source maintenance execution failed fatally and stopped the complete runtime.",
                ),
              );
            }
            failedMaintenanceWork.add(internal.workId);
            yield* publishDegradationLedger();
          }
          return {
            _tag: "Applied",
            exit: maintenanceExit,
          } satisfies import("@effect-view-server/source-adapter").SourceMaintenanceResult;
        }),
      ),
    );
  });

  const maintenanceRegistration = applicationStateRegistration;
  const runDueSweepIfActive =
    maintenanceRegistration === undefined
      ? undefined
      : lifecycleGate
          .withPermit(
            Effect.sync(() => {
              const currentStatus = SubscriptionRef.getUnsafe(status);
              return (
                maintenanceActive &&
                (currentStatus._tag === "Ready" || currentStatus._tag === "Degraded")
              );
            }),
          )
          .pipe(
            Effect.flatMap((active) =>
              active
                ? currentEpochNanos.pipe(
                    Effect.flatMap((epochNowNanos) =>
                      maintenanceRegistration.runDueSweep(scope, epochNowNanos, runMaintenance),
                    ),
                  )
                : Effect.void,
            ),
          );

  const supervisedMaintenance =
    maintenanceRegistration === undefined || runDueSweepIfActive === undefined
      ? undefined
      : Effect.uninterruptibleMask((restore) =>
          Effect.exit(
            restore(
              Effect.forever(
                Effect.sleep(maintenanceRegistration.sweepIntervalNanos).pipe(
                  Effect.andThen(runDueSweepIfActive),
                ),
              ),
            ),
          ).pipe(
            Effect.flatMap((exit) => {
              const cause = maintenanceSupervisorCause(input.entry.topic, exit, closing);
              return cause === undefined ? Effect.void : input.onFatal(cause);
            }),
          ),
        );

  const settlementEffect = Effect.fn("ViewServerRuntimeCore.source.settlement.capture")(function* (
    settlement: import("@effect-view-server/source-adapter").SourceSettlement<unknown>,
    applicationExit: import("@effect-view-server/source-adapter").SourceApplicationExit,
  ) {
    const settlementResult = yield* Effect.try({
      try: () => {
        const candidate = settlement(applicationExit);
        return Effect.isEffect(candidate) ? Option.some(candidate) : Option.none();
      },
      catch: () =>
        sourceRuntimeFailure({
          _tag: "InvalidSourceSettlement",
          message: "Source Settlement callback threw before returning an Effect",
        }),
    });
    if (Option.isNone(settlementResult)) {
      return yield* Effect.fail(
        sourceDefinitionError(
          input.entry.topic,
          "Source settlement must return an Effect without throwing.",
        ),
      );
    }
    // Narrow inference workaround: keep the returned settlement Effect as data.
    // A plain return is inferred as a nested Effect by Effect.gen rather than as
    // this helper's success value.
    return yield* Effect.succeed(settlementResult.value);
  });

  const recordFailedSettlement = Effect.sync(() => {
    failedSettlementCount += 1n;
    return failedSettlementCount;
  }).pipe(
    Effect.flatMap(
      (count) =>
        input.constructionOptions.settlementHandoff?.afterFailureCounted?.(count) ?? Effect.void,
    ),
  );

  const observeSettlement = (
    effect: Effect.Effect<void, unknown>,
  ): Effect.Effect<void, SourceExecutionError> =>
    effect.pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          completedSettlementCount += 1n;
        }),
      ),
      Effect.tapError((settlementFailure) =>
        recordFailedSettlement.pipe(
          Effect.andThen(
            Effect.logError("Source settlement returned a typed failure.", {
              topic: input.entry.topic,
              settlementFailure,
            }),
          ),
        ),
      ),
      Effect.mapError(
        (failure) =>
          ({
            _tag: "AdapterFailure",
            failure,
          }) as const,
      ),
    );

  const applyWithSettlement = Effect.fn(
    "ViewServerRuntimeCore.source.application.applyWithSettlement",
  )(function* (
    arbitration: SourceAttemptArbitration,
    requiresLifecycleGate: boolean,
    operation: (
      restore: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>,
      executeTransition: (effect: Effect.Effect<void>) => Effect.Effect<void>,
    ) => Effect.Effect<void, SourceRuntimeError>,
    settlement: import("@effect-view-server/source-adapter").SourceSettlement<unknown>,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let transitionDefect = false;
        const executeTransition = (effect: Effect.Effect<void>): Effect.Effect<void> => {
          transitionDefect = true;
          return effect.pipe(
            Effect.onExit((exit) =>
              Effect.sync(() => {
                transitionDefect = Exit.isFailure(exit);
              }),
            ),
          );
        };
        const application = Effect.suspend(() => operation(restore, executeTransition));
        const applicationExit = requiresLifecycleGate
          ? yield* Effect.acquireUseRelease(
              restore(lifecycleGate.take(1)),
              () => Effect.exit(application),
              (permits) => lifecycleGate.release(permits),
            )
          : yield* Effect.exit(application);
        yield* input.constructionOptions.settlementHandoff?.afterApplicationExit ?? Effect.void;
        const attemptScope = yield* Effect.scope;
        const handoff =
          yield* Deferred.make<Result.Result<Effect.Effect<void, unknown>, SourceExecutionError>>();
        const settlementFiberRegistration = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
        const settlementChild = Effect.uninterruptible(
          Deferred.await(settlementFiberRegistration),
        ).pipe(
          Effect.flatMap((registeredSettlementFiber) =>
            Effect.gen(function* () {
              const captured = yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  const capturedSettlement = yield* settlementEffect(
                    settlement,
                    applicationExit,
                  ).pipe(Effect.result);
                  if (Result.isFailure(capturedSettlement)) {
                    yield* recordFailedSettlement;
                  }
                  yield* (
                    input.constructionOptions.settlementHandoff?.afterCallbackApplication ??
                      Effect.void
                  );
                  yield* Deferred.succeed(handoff, capturedSettlement);
                  yield* (
                    input.constructionOptions.settlementHandoff?.afterHandoffCompleted ??
                      Effect.void
                  );
                  return capturedSettlement;
                }),
              );
              if (Result.isFailure(captured)) {
                if (yield* arbitration.cancellationWon) {
                  return {
                    _tag: "CancellationWon" as const,
                  };
                }
                return {
                  _tag: "Completed" as const,
                  exit: Exit.fail(captured.failure),
                };
              }
              if (yield* arbitration.promoteReturnedEffect(registeredSettlementFiber)) {
                return {
                  _tag: "CancellationWon" as const,
                };
              }
              yield* (
                input.constructionOptions.settlementHandoff?.beforeReturnedEffectRestore ??
                  Effect.void
              );
              const settlementExit = yield* Effect.exit(
                restore(observeSettlement(captured.success)),
              );
              if (transitionDefect && Exit.isFailure(settlementExit)) {
                yield* Effect.logError(
                  "Source fatal-path settlement ended unsuccessfully after handoff.",
                  {
                    topic: input.entry.topic,
                    cause: settlementExit.cause,
                  },
                );
              }
              return {
                _tag: "Completed" as const,
                exit: settlementExit,
              };
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  arbitration.settlementFibers.delete(registeredSettlementFiber);
                }),
              ),
            ),
          ),
        );
        const settlementFiber = yield* Effect.forkIn(settlementChild, attemptScope, {
          startImmediately: true,
        });
        yield* input.constructionOptions.settlementHandoff?.afterSettlementChildFork ?? Effect.void;
        arbitration.settlementFibers.set(settlementFiber, "CallbackHandoff");
        yield* (
          input.constructionOptions.settlementHandoff?.afterSettlementChildRegistration ??
            Effect.void
        );
        yield* Deferred.succeed(settlementFiberRegistration, settlementFiber);
        const capturedSettlement = yield* Deferred.await(handoff);
        yield* input.constructionOptions.settlementHandoff?.afterHandoffObserved ?? Effect.void;
        if (transitionDefect && Exit.isFailure(applicationExit)) {
          if (Result.isFailure(capturedSettlement)) {
            yield* Effect.logError("Source fatal-path settlement callback failed before handoff.", {
              topic: input.entry.topic,
              failure: capturedSettlement.failure,
            });
          }
          yield* input.onFatal(
            fatalSourceApplicationCause(
              input.entry.topic,
              applicationExit.cause,
              "Source application transition failed and stopped the complete runtime.",
            ),
          );
          yield* input.constructionOptions.settlementHandoff?.afterFatalCompleted ?? Effect.void;
          return yield* Effect.failCause(applicationExit.cause);
        }
        const settlementOutcome = (yield* arbitration.cancellationWon)
          ? yield* Fiber.join(settlementFiber)
          : yield* restore(Fiber.join(settlementFiber));
        if (settlementOutcome._tag === "CancellationWon") {
          return yield* restore(Effect.interrupt);
        }
        const settlementFailed = Exit.isFailure(settlementOutcome.exit);
        const applicationFailed = Exit.isFailure(applicationExit);
        if (!settlementFailed && !applicationFailed) {
          return;
        }
        yield* (
          input.constructionOptions.settlementHandoff?.beforeOrdinaryTerminationClaim ?? Effect.void
        );
        if (!(yield* arbitration.claimOrdinaryTermination)) {
          yield* Effect.logError(
            "Source application failure was secondary to attempt cancellation.",
            {
              topic: input.entry.topic,
              applicationFailed,
              settlementFailed,
            },
          );
          return yield* restore(Effect.interrupt);
        }
        if (Exit.isFailure(settlementOutcome.exit)) {
          return yield* Effect.failCause(settlementOutcome.exit.cause);
        }
        const applicationCause = Option.getOrThrow(Exit.getCause(applicationExit));
        if (Cause.hasDies(applicationCause)) {
          return yield* Effect.fail(
            sourceApplicationFailure(
              "Source mutation application defected and terminated the Source Attempt.",
            ),
          );
        }
        return yield* Effect.failCause(applicationCause);
      }),
    );
  });

  const handoffAttemptTermination = (
    arbitration: SourceAttemptArbitration,
    failure: SourceTermination<unknown>,
  ): Effect.Effect<never, SourceTermination<unknown>> =>
    Effect.gen(function* () {
      yield* input.constructionOptions.beforeAttemptTerminationClaim ?? Effect.void;
      return yield* arbitration.claimOrdinaryTermination.pipe(
        Effect.flatMap((ordinaryTerminationWon) =>
          ordinaryTerminationWon ? Effect.fail(failure) : Effect.interrupt,
        ),
      );
    });

  const laneEvent = Effect.fn("ViewServerRuntimeCore.source.lane.event")(function* (
    arbitration: SourceAttemptArbitration,
    laneId: string,
    event: import("@effect-view-server/source-adapter").SourceLaneEvent<object, unknown, unknown>,
  ) {
    const counters = laneCounters.get(laneId);
    if (counters === undefined) {
      return yield* Effect.fail(
        sourceDefinitionError(
          input.entry.topic,
          `Source Lane ${laneId} was not registered by the active attempt.`,
        ),
      );
    }
    if (isSourceItemRejection(event)) {
      return yield* applyWithSettlement(
        arbitration,
        false,
        (restore) =>
          restore(
            Effect.gen(function* () {
              if (event.diagnostic.rejectedAtNanos < 0n) {
                return yield* Effect.fail(
                  sourceDefinitionFailure(
                    input.entry.topic,
                    "Source Rejection timestamp must be non-negative epoch nanoseconds.",
                  ),
                );
              }
              const failure = yield* validateRejectionFailure(event.diagnostic.failure);
              const location = yield* Schema.decodeUnknownEffect(
                input.entry.declaration.rejectionLocation,
              )(event.diagnostic.location).pipe(
                Effect.mapError(() =>
                  sourceDefinitionFailure(
                    input.entry.topic,
                    "Source Rejection Location does not satisfy its declared Schema.",
                  ),
                ),
              );
              latestRejection = {
                failure,
                location,
                rejectedAtNanos: event.diagnostic.rejectedAtNanos,
              };
              rejectedItemCount += 1n;
              lastRejectionAtNanos = event.diagnostic.rejectedAtNanos;
              yield* publishDegradationLedger();
            }),
          ),
        event.settle,
      ).pipe(
        Effect.mapError((failure) =>
          failure._tag === "AdapterFailure" || failure._tag === "RuntimeFailure"
            ? failure
            : sourceRuntimeFailure(failure),
        ),
      );
    }
    if (!isSourceDelivery(event)) {
      return yield* Effect.fail(
        sourceDefinitionError(
          input.entry.topic,
          "Source Lane emitted a structurally forged event.",
        ),
      );
    }
    receivedDeliveryCount += 1n;
    lastDeliveryAtNanos = yield* currentEpochNanos;
    yield* applyWithSettlement(
      arbitration,
      event.transition !== undefined,
      (restore, executeTransition) =>
        Effect.gen(function* () {
          if (event.transition !== undefined) {
            if (Chunk.size(event.mutations) !== 1) {
              return yield* Effect.fail(
                sourceDefinitionFailure(
                  input.entry.topic,
                  "Source Application Transition requires exactly one nominal Source Mutation.",
                ),
              );
            }
            const transition = Option.getOrThrow(
              Option.fromUndefinedOr(resolveSourceApplicationTransition(event.transition)),
            );
            if (
              applicationLifetimeIdentity === undefined ||
              transition.topic !== input.entry.topic ||
              transition.lifetimeIdentity !== applicationLifetimeIdentity
            ) {
              return yield* executeTransition(
                Effect.die(
                  runtimeError(
                    input.entry.topic,
                    "Source Application Transition is bound to a different Topic or logical lifetime.",
                  ),
                ),
              );
            }
            const mutation = Chunk.headNonEmpty(event.mutations);
            const preparedMutation = yield* Effect.interruptible(prepareMutation(mutation));
            yield* applyPreparedMutation(preparedMutation);
            yield* executeTransition(Effect.sync(transition.apply));
            for (const workId of transition.cancelledMaintenanceWorkIds) {
              failedMaintenanceWork.delete(workId);
            }
            yield* publishDegradationLedger();
            return;
          }
          yield* restore(Effect.forEach(event.mutations, applyMutation, { discard: true }));
        }),
      event.settle,
    ).pipe(
      Effect.mapError((failure) =>
        failure._tag === "AdapterFailure" || failure._tag === "RuntimeFailure"
          ? failure
          : sourceRuntimeFailure(failure),
      ),
    );
  });

  const runLane = (arbitration: SourceAttemptArbitration, lane: SourceLane) =>
    lane.events.pipe(
      Stream.runForEach((event) => laneEvent(arbitration, lane.id, event)),
      Effect.catch((failure) =>
        validateFailure(failure).pipe(
          Effect.catch((validationFailure) => Effect.succeed(validationFailure)),
          Effect.flatMap((validatedFailure) =>
            handoffAttemptTermination(arbitration, {
              _tag: "Failed" as const,
              failure: validatedFailure,
            }),
          ),
        ),
      ),
      Effect.andThen(
        handoffAttemptTermination(arbitration, {
          _tag: "UnexpectedCompletion",
        }),
      ),
    );

  const runAttempt = Effect.fn("ViewServerRuntimeCore.source.attempt.run")(function* (
    previous: SourceTermination<unknown> | undefined,
    arbitration: SourceAttemptArbitration,
  ) {
    lastAttemptStartedAtNanos = yield* currentEpochNanos;
    if (previous !== undefined) {
      currentAttempt += 1n;
      retryCount += 1n;
      yield* publish({
        _tag: "Reacquiring",
        previousTermination: previous,
        attempt: currentAttempt,
        startedAtNanos: lastAttemptStartedAtNanos,
      });
    }
    const attempt = yield* input.entry.lifecycle
      .acquire({
        definition: input.entry.definition.options,
        lifetimeScope: scope,
        target: input.target,
        toolkit: makeToolkit(),
      })
      .pipe(
        Effect.catch((failure) =>
          validateFailure(failure).pipe(
            Effect.catch((validationFailure) => Effect.succeed(validationFailure)),
            Effect.flatMap((validatedFailure) =>
              handoffAttemptTermination(arbitration, {
                _tag: "Failed" as const,
                failure: validatedFailure,
              }),
            ),
          ),
        ),
      );
    if (!isSourceAttempt(attempt)) {
      return yield* handoffAttemptTermination(arbitration, {
        _tag: "Failed" as const,
        failure: sourceDefinitionError(
          input.entry.topic,
          "Lifecycle acquisition returned a structurally forged Source Attempt.",
        ),
      });
    }
    const laneMetadata = Result.try(() =>
      attempt.lanes.map((lane) => ({
        id: lane.id,
        events: lane.events,
        bufferMetrics: lane.bufferMetrics,
      })),
    );
    if (
      Result.isFailure(laneMetadata) ||
      laneMetadata.success.length === 0 ||
      laneMetadata.success.some(
        (lane) =>
          typeof lane.id !== "string" ||
          lane.id.length === 0 ||
          !Stream.isStream(lane.events) ||
          !Effect.isEffect(lane.bufferMetrics),
      ) ||
      new Set(laneMetadata.success.map((lane) => lane.id)).size !== laneMetadata.success.length
    ) {
      return yield* handoffAttemptTermination(arbitration, {
        _tag: "Failed" as const,
        failure: sourceDefinitionError(
          input.entry.topic,
          "Source Attempt requires non-empty unique lane IDs, Streams, and buffer metrics.",
        ),
      });
    }
    const nextLaneIds = laneMetadata.success
      .map((lane) => lane.id)
      .sort((left, right) => left.localeCompare(right));
    if (
      stableLaneIds !== undefined &&
      (stableLaneIds.length !== nextLaneIds.length ||
        stableLaneIds.some((laneId, index) => laneId !== nextLaneIds[index]))
    ) {
      return yield* handoffAttemptTermination(arbitration, {
        _tag: "Failed" as const,
        failure: sourceDefinitionError(
          input.entry.topic,
          "Source Delivery Lane IDs must remain stable across retries.",
        ),
      });
    }
    stableLaneIds ??= nextLaneIds;
    laneCounters.clear();
    for (const lane of attempt.lanes) {
      laneCounters.set(lane.id, {
        buffer: lane.bufferMetrics,
      });
    }
    const readyAtNanos = yield* currentEpochNanos;
    const stickyDegradedAtNanos = degradedAtNanos ?? readyAtNanos;
    const reasons = degradationReasons();
    yield* lifecycleGate.withPermit(
      Effect.sync(() => {
        maintenanceActive = true;
      }).pipe(
        Effect.andThen(
          publish(
            reasons === undefined
              ? {
                  _tag: "Ready",
                  attempt: currentAttempt,
                  readyAtNanos,
                }
              : {
                  _tag: "Degraded",
                  attempt: currentAttempt,
                  degradedAtNanos: stickyDegradedAtNanos,
                  reasons,
                },
          ),
        ),
      ),
    );
    const laneWorkers = attempt.lanes.map((lane) => runLane(arbitration, lane));
    return yield* Effect.all(laneWorkers, {
      concurrency: "unbounded",
      discard: true,
    });
  });

  let previousTermination: SourceTermination<unknown> | undefined;
  const attemptWithObservation = Effect.suspend(() => {
    const attempt = previousTermination === undefined ? currentAttempt : currentAttempt + 1n;
    return Effect.scoped(
      Effect.gen(function* () {
        const arbitration = yield* makeSourceAttemptArbitration(
          input.constructionOptions.settlementHandoff?.duringReturnedEffectPromotion,
        );
        cancelActiveAttempt = arbitration.cancel;
        yield* Effect.addFinalizer(() => arbitration.cancel.pipe(Effect.asVoid));
        if (declaredLaneFailure !== undefined) {
          return yield* handoffAttemptTermination(arbitration, {
            _tag: "Failed" as const,
            failure: declaredLaneFailure,
          });
        }
        const metricFailure = yield* Deferred.make<SourceExecutionError>();
        const registration = yield* metricFailureObservation.register(metricFailure);
        if (registration._tag === "Failed") {
          return yield* handoffAttemptTermination(arbitration, {
            _tag: "Failed" as const,
            failure: registration.failure,
          });
        }
        return yield* Effect.raceFirst(
          runAttempt(previousTermination, arbitration),
          Deferred.await(metricFailure).pipe(
            Effect.flatMap((failure) =>
              handoffAttemptTermination(arbitration, {
                _tag: "Failed" as const,
                failure,
              }),
            ),
          ),
        ).pipe(Effect.ensuring(metricFailureObservation.unregister(metricFailure)));
      }).pipe(
        Effect.tapError((termination) =>
          lifecycleGate.withPermit(
            Effect.gen(function* () {
              maintenanceActive = false;
              previousTermination = termination;
              lastTerminationAtNanos = yield* currentEpochNanos;
            }),
          ),
        ),
        Effect.ensuring(
          lifecycleGate.withPermit(
            Effect.sync(() => {
              maintenanceActive = false;
            }),
          ),
        ),
      ),
    ).pipe(
      Effect.annotateLogs({
        attempt,
        ...(input.feedRouteReference === undefined ? {} : { feedRoute: input.feedRouteReference }),
      }),
    );
  });
  const onRetry = Effect.fn("ViewServerRuntimeCore.source.retry.waiting")(function* (
    metadata: Schedule.Metadata<unknown, SourceTermination<unknown>>,
  ) {
    const decidedAtNanos = yield* currentEpochNanos;
    const delayNanos = Duration.toNanos(metadata.duration);
    if (Option.isNone(delayNanos)) {
      return yield* Effect.fail<SourceTermination<unknown>>({
        _tag: "Failed",
        failure: sourceDefinitionError(
          input.entry.topic,
          "Source Retry Schedule must produce a finite delay.",
        ),
      });
    }
    yield* publish({
      _tag: "WaitingToRetry",
      nextAttempt: currentAttempt + 1n,
      termination: metadata.input,
      retryAtNanos: decidedAtNanos + delayNanos.value,
    });
  });
  const retried =
    input.entry.definition.retry._tag === "UseAdapterDefault"
      ? input.entry.lifecycle.retryDefault(attemptWithObservation, onRetry)
      : Effect.retry(
          attemptWithObservation,
          input.entry.definition.retry.policy.pipe(Schedule.tap(onRetry)),
        );
  const run = retried.pipe(
    Effect.catch((termination) =>
      currentEpochNanos.pipe(
        Effect.flatMap((exhaustedAtNanos) =>
          publish({
            _tag: "Exhausted",
            exhaustion: {
              _tag: "RetryExhausted",
              lastTermination: termination,
            },
            exhaustedAtNanos,
          }),
        ),
      ),
    ),
    Effect.provideService(Scope.Scope, scope),
    Effect.provide(input.context),
  );
  const supervisedRun = Effect.exit(run).pipe(
    Effect.flatMap((exit) =>
      Exit.isSuccess(exit) || closing
        ? Effect.void
        : input.onFatal(
            fatalRuntimeCause(
              input.entry.topic,
              exit.cause,
              "Source supervisor failed fatally and stopped the complete runtime.",
            ),
          ),
    ),
  );

  const stop = Effect.fn("ViewServerRuntimeCore.source.stop")(function* (
    reason: import("@effect-view-server/source-adapter").SourceStoppingReason,
  ) {
    closing = true;
    yield* cancelActiveAttempt;
    yield* input.constructionOptions.afterAttemptCancellationRequested ?? Effect.void;
    const activeSupervisorFiber = yield* Deferred.await(supervisorFiber);
    yield* Effect.sync(() => {
      activeSupervisorFiber.interruptUnsafe();
    });
    if (maintenanceFiber !== undefined) {
      yield* Fiber.interrupt(maintenanceFiber).pipe(Effect.asVoid);
    }
    yield* lifecycleGate.withPermit(
      Effect.gen(function* () {
        const stoppingAtNanos = yield* currentEpochNanos;
        maintenanceActive = false;
        yield* publish({
          _tag: "Stopping",
          reason,
          stoppingAtNanos,
        });
      }),
    );
    yield* Fiber.interrupt(activeSupervisorFiber).pipe(Effect.asVoid);
    yield* Scope.close(scope, Exit.void);
  });

  declaredLaneFailure = yield* initializeDeclaredLanes();
  yield* sampleAdapterMetrics();
  const activate = (yield* Effect.cached(
    Effect.gen(function* () {
      if (supervisedMaintenance !== undefined) {
        maintenanceFiber = yield* Effect.forkIn(supervisedMaintenance, scope, {
          startImmediately: true,
        });
      }
      const activatedSupervisorFiber = yield* Effect.forkIn(supervisedRun, scope, {
        startImmediately: true,
      });
      yield* Deferred.succeed(supervisorFiber, activatedSupervisorFiber);
      yield* Effect.forkIn(
        Effect.forever(Effect.sleep("1 second").pipe(Effect.andThen(sampleAdapterMetrics()))),
        scope,
        {
          startImmediately: true,
        },
      );
    }),
  )).pipe(Effect.uninterruptible);

  const logical: SourceLogicalRuntime = {
    entry: input.entry,
    target: input.target,
    health,
    status,
    activate,
    run,
    stop,
  };
  return logical;
});

const exactRoute = (
  entry: SourceRuntimeRouteEntry,
  candidate: unknown,
): Result.Result<Readonly<Record<string, unknown>>, ViewServerRuntimeError> => {
  if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
    return Result.fail(
      runtimeError(entry.topic, "Leased Source requires exact routeBy.", "InvalidQuery"),
    );
  }
  const keys = Result.try(() => Reflect.ownKeys(candidate));
  if (
    Result.isFailure(keys) ||
    keys.success.length !== entry.definition.routeBy.length ||
    keys.success.some((key) => typeof key !== "string" || !entry.definition.routeBy.includes(key))
  ) {
    return Result.fail(
      runtimeError(
        entry.topic,
        `Leased Source routeBy must contain all and only: ${entry.definition.routeBy.join(", ")}.`,
        "InvalidQuery",
      ),
    );
  }
  const route: Record<string, unknown> = {};
  for (const field of entry.definition.routeBy) {
    const fieldSchema = entry.schema.fields[field];
    const descriptor = Result.try(() => Object.getOwnPropertyDescriptor(candidate, field));
    if (
      fieldSchema === undefined ||
      Result.isFailure(descriptor) ||
      descriptor.success === undefined ||
      descriptor.success.enumerable !== true ||
      !("value" in descriptor.success) ||
      !Schema.is(fieldSchema)(descriptor.success.value)
    ) {
      return Result.fail(
        runtimeError(
          entry.topic,
          `Leased Source route field ${field} does not satisfy the Topic Schema.`,
          "InvalidQuery",
        ),
      );
    }
    Object.defineProperty(route, field, {
      enumerable: true,
      value: descriptor.success.value,
    });
  }
  return Result.succeed(copyRoute(entry.definition.routeBy, route));
};

const feedKeyFor = (
  entry: SourceRuntimeRouteEntry,
  route: Readonly<Record<string, unknown>>,
): Result.Result<string, ViewServerRuntimeError> => {
  const parts: Array<string> = [];
  for (const field of entry.definition.routeBy) {
    const fieldSchema = entry.schema.fields[field];
    if (fieldSchema === undefined) {
      return Result.fail(
        runtimeError(
          entry.topic,
          `Leased Source route field ${field} is not present in the Topic Schema.`,
        ),
      );
    }
    const identity = Result.try(() => makeSchemaJsonIdentity(fieldSchema));
    if (Result.isFailure(identity)) {
      return Result.fail(
        runtimeError(
          entry.topic,
          `Leased Source route field ${field} has no stable identity encoding.`,
        ),
      );
    }
    const key = Result.try(() => identity.success.canonicalKey(route[field]));
    if (Result.isFailure(key)) {
      return Result.fail(
        runtimeError(
          entry.topic,
          `Leased Source route field ${field} cannot be encoded.`,
          "InvalidQuery",
        ),
      );
    }
    parts.push(`${encodeURIComponent(field)}=${encodeURIComponent(key.success)}`);
  }
  return Result.succeed(`${encodeURIComponent(entry.topic)}/${parts.join("&")}`);
};

const translateSubscription = <Row extends object>(
  subscription: ViewServerLiveSubscription<Row>,
  query: Readonly<Record<string, unknown>>,
): ViewServerLiveSubscription<Row> => {
  if (Object.hasOwn(query, "groupBy")) {
    return subscription;
  }
  const publicKey = (storageKey: string, row?: object): string =>
    internalPublicId(storageKey) ??
    (row === undefined ? storageKey : (publicId(row) ?? storageKey));
  const translate = (event: ViewServerLiveEvent<Row>): ViewServerLiveEvent<Row> => {
    if (event.type === "snapshot") {
      return {
        ...event,
        keys: event.keys.map((key, index) => publicKey(key, event.rows[index])),
      };
    }
    if (event.type === "delta") {
      return {
        ...event,
        operations: event.operations.map((operation) =>
          operation.type === "insert" || operation.type === "update"
            ? {
                ...operation,
                key: publicKey(operation.key, operation.row),
              }
            : {
                ...operation,
                key: publicKey(operation.key),
              },
        ),
      };
    }
    return event;
  };
  return {
    events: subscription.events.pipe(Stream.map(translate)),
    close: subscription.close,
  };
};

const sourceAvailabilityEvent = (
  topic: string,
  queryId: string,
  status: SourceStatus<unknown, unknown>,
): StatusEvent => {
  if (status._tag === "Ready" || status._tag === "Degraded") {
    return {
      type: "status",
      topic,
      queryId,
      status: "ready",
      code: "Ready",
      message:
        status._tag === "Degraded"
          ? "Source delivery continues with active degradation diagnostics."
          : "Source is ready.",
    };
  }
  if (
    status._tag === "Starting" ||
    status._tag === "WaitingToRetry" ||
    status._tag === "Reacquiring"
  ) {
    return {
      type: "status",
      topic,
      queryId,
      status: "stale",
      code: "SnapshotStale",
      message:
        status._tag === "Starting"
          ? "Source is starting; retained rows may be incomplete."
          : "Source is retrying; retained rows may be stale.",
    };
  }
  return {
    type: "status",
    topic,
    queryId,
    status: "error",
    code: "RuntimeUnavailable",
    message:
      status._tag === "Exhausted"
        ? "Source retries are exhausted; retained rows are preserved."
        : "Source is stopping.",
  };
};

type AggregateSourceStatus = "ready" | "degraded" | "starting";

type AggregateSourceDefinition = {
  readonly lifecycle: "materialized" | "leased";
  readonly topic: string;
};

type AggregateSourceHealthSnapshot = AggregateSourceDefinition & {
  readonly key: string;
  readonly health: RuntimeSourceHealth;
};

function restoreAggregateSourceHealth<Topics extends TopicDefinitions>(
  sources: Readonly<Record<string, RuntimeSourceHealth | ReadonlyArray<RuntimeSourceHealth>>>,
): ViewServerSourceHealth<Topics>;
function restoreAggregateSourceHealth(
  sources: Readonly<Record<string, RuntimeSourceHealth | ReadonlyArray<RuntimeSourceHealth>>>,
): Readonly<Record<string, RuntimeSourceHealth | ReadonlyArray<RuntimeSourceHealth>>> {
  return sources;
}

const aggregateSourceHealth = <Topics extends TopicDefinitions>(
  definitions: Iterable<AggregateSourceDefinition>,
  snapshots: Iterable<AggregateSourceHealthSnapshot>,
): ViewServerSourceHealth<Topics> => {
  const materialized = new Map<string, RuntimeSourceHealth>();
  const leased = new Map<
    string,
    Array<{
      readonly key: string;
      readonly health: RuntimeSourceHealth;
    }>
  >();
  for (const snapshot of snapshots) {
    if (snapshot.lifecycle === "materialized") {
      materialized.set(snapshot.topic, snapshot.health);
      continue;
    }
    const topicSnapshots = leased.get(snapshot.topic);
    const value = { key: snapshot.key, health: snapshot.health };
    if (topicSnapshots === undefined) {
      leased.set(snapshot.topic, [value]);
    } else {
      topicSnapshots.push(value);
    }
  }
  const sources: Record<string, RuntimeSourceHealth | ReadonlyArray<RuntimeSourceHealth>> = {};
  for (const definition of definitions) {
    if (definition.lifecycle === "materialized") {
      const snapshot = materialized.get(definition.topic);
      if (snapshot !== undefined) {
        Reflect.set(sources, definition.topic, snapshot);
      }
      continue;
    }
    const active = leased.get(definition.topic) ?? [];
    active.sort((left, right) => left.key.localeCompare(right.key));
    Reflect.set(sources, definition.topic, Object.freeze(active.map(({ health }) => health)));
  }
  // Runtime admission validates every Source Definition and every cached health
  // value before this single nominal restoration seam. A Materialized Source
  // remains absent until its adapter publishes its first schema-valid metrics
  // snapshot; invalid metrics stay inside that Source's supervision.
  return restoreAggregateSourceHealth<Topics>(Object.freeze(sources));
};

const combineAggregateSourceStatus = (
  left: AggregateSourceStatus,
  right: AggregateSourceStatus,
): AggregateSourceStatus =>
  left === "starting" || right === "starting"
    ? "starting"
    : left === "degraded" || right === "degraded"
      ? "degraded"
      : "ready";

const overlaySourceHealth = <Topics extends TopicDefinitions>(
  health: RuntimeCoreBaseHealth<Topics>,
  statuses: Iterable<{
    readonly topic: string;
    readonly status: SourceStatus<unknown, unknown>;
  }>,
  definitions: Iterable<AggregateSourceDefinition>,
  snapshots: Iterable<AggregateSourceHealthSnapshot>,
): ViewServerHealth<Topics> => {
  const statusByTopic = new Map<string, AggregateSourceStatus>();
  for (const { topic, status } of statuses) {
    const next =
      status._tag === "Ready"
        ? "ready"
        : status._tag === "Starting" || status._tag === "Reacquiring" || status._tag === "Exhausted"
          ? "starting"
          : "degraded";
    const current = statusByTopic.get(topic);
    statusByTopic.set(
      topic,
      current === undefined ? next : combineAggregateSourceStatus(current, next),
    );
  }
  const topics = { ...health.engine.topics };
  let aggregateSourceStatus: AggregateSourceStatus = "ready";
  for (const [topic, sourceStatus] of statusByTopic) {
    const current: unknown = Reflect.get(topics, topic);
    if (typeof current === "object" && current !== null) {
      const engineStatus = Reflect.get(current, "status");
      const status = combineAggregateSourceStatus(
        engineStatus === "starting"
          ? "starting"
          : engineStatus === "degraded"
            ? "degraded"
            : "ready",
        sourceStatus,
      );
      Reflect.set(topics, topic, { ...current, status });
    }
    aggregateSourceStatus = combineAggregateSourceStatus(aggregateSourceStatus, sourceStatus);
  }
  const status =
    health.status === "stopping"
      ? "stopping"
      : health.status === "starting" || aggregateSourceStatus === "starting"
        ? "starting"
        : health.status === "degraded" || aggregateSourceStatus === "degraded"
          ? "degraded"
          : "ready";
  return {
    ...health,
    status,
    sources: aggregateSourceHealth<Topics>(definitions, snapshots),
    engine: {
      topics,
    },
  };
};

const attachSourceAvailability = <Row extends object>(
  subscription: ViewServerLiveSubscription<Row>,
  runtime: SourceLogicalRuntime,
  queryId: string,
): ViewServerLiveSubscription<Row> => {
  const availability = SubscriptionRef.changes(runtime.status).pipe(
    Stream.map((status) => sourceAvailabilityEvent(runtime.entry.topic, queryId, status)),
    Stream.changesWith(
      (left, right) =>
        left.status === right.status && left.code === right.code && left.message === right.message,
    ),
  );
  return {
    events: subscription.events.pipe(Stream.merge(availability, { haltStrategy: "left" })),
    close: subscription.close,
  };
};

export const makeRuntimeCoreSourceManager = Effect.fn("ViewServerRuntimeCore.source.manager.make")(
  function* <const Topics extends TopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    mutations: ViewServerRuntimeCoreInternalMutations<Topics>,
    onHealthChange: Effect.Effect<void> = Effect.void,
    constructionOptions: RuntimeCoreSourceManagerConstructionOptions = {},
  ) {
    return yield* acquireRuntimeCoreResourceHandoff(
      (markAcquired) =>
        Effect.gen(function* () {
          const context = yield* Effect.context<ViewServerSourceRequirements<Topics>>();
          const entries = yield* resolveEntries(config, context);
          const runtimeTopic = (topic: string): topic is Extract<keyof Topics, string> =>
            Object.hasOwn(config.topics, topic);
          const checkedTopic = (
            topic: string,
          ): Effect.Effect<Extract<keyof Topics, string>, ViewServerRuntimeError> =>
            runtimeTopic(topic)
              ? Effect.succeed(topic)
              : Effect.fail(runtimeError(topic, `Unknown Source-owned Topic ${topic}.`));
          const sourceMutations: SourceMutationOperations = {
            publishRows: (topic, rows) =>
              checkedTopic(topic).pipe(
                Effect.flatMap((ownedTopic) => mutations.publishManyDecodedRows(ownedTopic, rows)),
              ),
            publishRowsWithStorageKeys: (topic, rows, partitionKey) =>
              checkedTopic(topic).pipe(
                Effect.flatMap((ownedTopic) =>
                  mutations.publishManyDecodedRowsWithStorageKeys(ownedTopic, rows, partitionKey),
                ),
              ),
            delete: (topic, id) =>
              checkedTopic(topic).pipe(
                Effect.flatMap((ownedTopic) => mutations.delete(ownedTopic, id)),
              ),
            deleteStorageKey: (topic, storageKey, partitionKey) =>
              checkedTopic(topic).pipe(
                Effect.flatMap((ownedTopic) =>
                  mutations.deleteStorageKey(ownedTopic, storageKey, partitionKey),
                ),
              ),
          };
          const managerScope = yield* Scope.make("sequential");
          yield* markAcquired(Scope.close(managerScope, Exit.void));
          const fatalSignal = yield* Deferred.make<Cause.Cause<ViewServerRuntimeError>>();
          const onFatal = (cause: Cause.Cause<ViewServerRuntimeError>): Effect.Effect<void> =>
            Deferred.succeed(fatalSignal, cause).pipe(Effect.asVoid);
          const fatal = Deferred.await(fatalSignal).pipe(
            Effect.flatMap((cause) => Effect.failCause(cause)),
          );
          const materialized = new Map<string, SourceLogicalRuntime>();
          type ManagedLeasedSource = {
            readonly entry: SourceRuntimeEntry;
            readonly feedKey: string;
            readonly route: Readonly<Record<string, unknown>>;
            runtime: SourceLogicalRuntime | undefined;
            readonly ownedStorageKeys: Set<string>;
            readonly partition: ColumnLiveViewEngineQueryPartition;
            readonly scope: Scope.Closeable;
            subscribers: number;
          };
          const leases = new Map<string, ManagedLeasedSource>();
          const leasedDiagnostics = new Map<
            string,
            {
              readonly route: Readonly<Record<string, unknown>>;
              readonly state: SubscriptionRef.SubscriptionRef<SourceLogicalRuntime | undefined>;
              observers: number;
            }
          >();
          const sourceHealthObservations = new Set<{
            readonly close: Effect.Effect<void>;
          }>();
          const leaseLock = Semaphore.makeUnsafe(1);
          const sourceStatuses = new Map<
            string,
            {
              readonly topic: string;
              readonly status: SourceStatus<unknown, unknown>;
            }
          >();
          const aggregateSourceDefinitions: ReadonlyArray<AggregateSourceDefinition> =
            Object.freeze(
              Array.from(entries.values(), (entry) => ({
                lifecycle: entry.definition.lifecycle,
                topic: entry.topic,
              })),
            );
          const sourceHealthSnapshots = new Map<string, AggregateSourceHealthSnapshot>();
          let closed = false;
          let leaseSequence = 0n;

          for (const entry of entries.values()) {
            if (entry.definition.lifecycle !== "materialized") {
              continue;
            }
            const sourceKey = `materialized:${entry.topic}`;
            const runtime = yield* makeLogicalRuntime({
              entry,
              target: { _tag: "Materialized" },
              mutations: sourceMutations,
              context,
              ownerScope: managerScope,
              constructionOptions,
              onHealth: (health) =>
                Effect.sync(() => {
                  sourceHealthSnapshots.set(sourceKey, {
                    key: sourceKey,
                    lifecycle: "materialized",
                    topic: entry.topic,
                    health,
                  });
                }),
              onStatus: (status) =>
                Effect.sync(() => {
                  sourceStatuses.set(sourceKey, {
                    topic: entry.topic,
                    status,
                  });
                }).pipe(Effect.andThen(onHealthChange)),
              onFatal,
            });
            materialized.set(entry.topic, runtime);
          }
          for (const runtime of materialized.values()) {
            yield* runtime.activate;
          }
          const cleanupLease = Effect.fn("ViewServerRuntimeCore.source.lease.cleanup")(
            (lease: ManagedLeasedSource) =>
              Effect.gen(function* () {
                yield* runAllFinalizers([
                  lease.runtime === undefined ? Effect.void : lease.runtime.stop("lease-release"),
                  Scope.close(lease.scope, Exit.void),
                ]);
                const deletion = yield* Effect.exit(
                  runAllFinalizers(
                    Array.from(lease.ownedStorageKeys, (storageKey) =>
                      sourceMutations.deleteStorageKey(
                        lease.entry.topic,
                        storageKey,
                        lease.partition.key,
                      ),
                    ),
                  ),
                );
                const diagnostics = leasedDiagnostics.get(lease.feedKey);
                yield* runAllFinalizers([
                  diagnostics === undefined
                    ? Effect.void
                    : SubscriptionRef.set(diagnostics.state, undefined),
                  Effect.sync(() => {
                    sourceStatuses.delete(lease.feedKey);
                    sourceHealthSnapshots.delete(lease.feedKey);
                  }),
                  onHealthChange,
                ]);
                if (Exit.isFailure(deletion)) {
                  yield* Effect.logWarning(
                    "Source lease row cleanup failed; ownership is retained for retry.",
                    deletion.cause,
                  );
                  return false;
                }
                yield* Effect.sync(() => {
                  lease.ownedStorageKeys.clear();
                  leases.delete(lease.feedKey);
                });
                return true;
              }),
          );

          const acquireLeased: RuntimeCoreSourceManager<Topics>["acquireLeased"] = (
            topic,
            query,
            registerAcquired,
          ) =>
            acquireRuntimeCoreResourceHandoff(
              (markAcquired) =>
                leaseLock.withPermit(
                  Effect.uninterruptibleMask((restore) =>
                    Effect.gen(function* () {
                      if (closed) {
                        return yield* Effect.fail(
                          runtimeError(topic, "Runtime Core Source Manager is closed."),
                        );
                      }
                      const entry = entries.get(topic);
                      if (entry === undefined || entry.definition.lifecycle !== "leased") {
                        return Option.none<RuntimeCoreSourceLease>();
                      }
                      const routeResult = exactRoute(entry, Reflect.get(query, "routeBy"));
                      const route = yield* Effect.fromResult(routeResult);
                      const feedKey = yield* Effect.fromResult(feedKeyFor(entry, route));
                      let lease = leases.get(feedKey);
                      if (lease !== undefined && lease.subscribers === 0) {
                        const cleaned = yield* cleanupLease(lease);
                        if (!cleaned) {
                          return yield* Effect.fail(
                            runtimeError(
                              topic,
                              "Source lease row cleanup is pending retry.",
                              "RuntimeUnavailable",
                            ),
                          );
                        }
                        lease = undefined;
                      }
                      if (lease === undefined) {
                        lease = yield* restore(
                          acquireRuntimeCoreResourceHandoff((markLeaseAcquired) =>
                            Effect.gen(function* () {
                              const scope = yield* Scope.fork(managerScope, "sequential");
                              const ownedStorageKeys = new Set<string>();
                              leaseSequence += 1n;
                              const partition: ColumnLiveViewEngineQueryPartition = Object.freeze({
                                key: `${feedKey}/lease:${leaseSequence}`,
                                ownedStorageKeys: () => ownedStorageKeys,
                                matches: (row, storageKey) =>
                                  storageKey === undefined
                                    ? routeMatchesRow(entry.definition.routeBy, route, row)
                                    : ownedStorageKeys.has(storageKey),
                              });
                              const acquiredLease: ManagedLeasedSource = {
                                entry,
                                feedKey,
                                route,
                                runtime: undefined,
                                ownedStorageKeys,
                                partition,
                                scope,
                                subscribers: 0,
                              };
                              leases.set(feedKey, acquiredLease);
                              yield* markLeaseAcquired(
                                cleanupLease(acquiredLease).pipe(Effect.asVoid),
                              );
                              const runtime = yield* makeLogicalRuntime({
                                entry,
                                target: { _tag: "Leased", route },
                                mutations: sourceMutations,
                                context,
                                ownerScope: scope,
                                constructionOptions,
                                partitionKey: partition.key,
                                feedKey,
                                feedRouteReference: `leased-feed-${leaseSequence}`,
                                ownedStorageKeys,
                                onHealth: (health) =>
                                  Effect.sync(() => {
                                    sourceHealthSnapshots.set(feedKey, {
                                      key: feedKey,
                                      lifecycle: "leased",
                                      topic: entry.topic,
                                      health,
                                    });
                                  }),
                                onStatus: (status) =>
                                  Effect.sync(() => {
                                    sourceStatuses.set(feedKey, {
                                      topic: entry.topic,
                                      status,
                                    });
                                  }).pipe(Effect.andThen(onHealthChange)),
                                onFatal,
                              });
                              yield* runtime.activate;
                              acquiredLease.runtime = runtime;
                              yield* constructionOptions.leaseHandoff?.beforeReturn ?? Effect.void;
                              const diagnostics = leasedDiagnostics.get(feedKey);
                              if (diagnostics !== undefined) {
                                yield* SubscriptionRef.set(diagnostics.state, runtime);
                              }
                              return acquiredLease;
                            }),
                          ),
                        );
                      }
                      const currentRuntime = Option.getOrThrow(
                        Option.fromUndefinedOr(lease.runtime),
                      );
                      lease.subscribers += 1;
                      let released = false;
                      const release = Effect.suspend(() => {
                        if (released) {
                          return Effect.void;
                        }
                        released = true;
                        return leaseLock.withPermit(
                          Effect.suspend(() => {
                            if (leases.get(lease!.feedKey) !== lease) {
                              return Effect.void;
                            }
                            lease!.subscribers -= 1;
                            return lease!.subscribers === 0
                              ? cleanupLease(lease!).pipe(Effect.asVoid)
                              : Effect.void;
                          }),
                        );
                      }).pipe(Effect.uninterruptible);
                      const currentLease = lease;
                      const acquiredSubscriptionLease: RuntimeCoreSourceLease = {
                        partition: currentLease.partition,
                        translate: (subscription, ownedQuery, queryId) =>
                          attachSourceAvailability(
                            translateSubscription(subscription, ownedQuery),
                            currentRuntime,
                            queryId,
                          ),
                        release,
                      };
                      yield* markAcquired(release);
                      yield* registerAcquired?.(release) ?? Effect.void;
                      return Option.some(acquiredSubscriptionLease);
                    }),
                  ),
                ),
              constructionOptions.leaseSubscriberHandoff,
            );

          const decorateMaterialized: RuntimeCoreSourceManager<Topics>["decorateMaterialized"] = (
            topic,
            subscription,
            queryId,
          ) => {
            const runtime = materialized.get(topic);
            return runtime === undefined
              ? subscription
              : attachSourceAvailability(subscription, runtime, queryId);
          };

          const subscribeProtocolSourceHealth: RuntimeCoreSourceManager<Topics>["subscribeProtocolSourceHealth"] =
            Effect.fn("ViewServerRuntimeCore.source.health.subscribeProtocol")(
              function* (topic, routeArgs) {
                const entry = entries.get(topic);
                if (entry === undefined) {
                  return yield* Effect.fail(
                    runtimeError(
                      topic,
                      `Topic ${topic} has no canonical Source Definition.`,
                      "InvalidQuery",
                    ),
                  );
                }
                if (entry.definition.lifecycle === "materialized") {
                  if (routeArgs.length !== 0) {
                    return yield* Effect.fail(
                      runtimeError(
                        topic,
                        `Materialized Source Topic ${topic} does not accept routeBy.`,
                        "InvalidQuery",
                      ),
                    );
                  }
                  const runtime = Option.getOrThrow(
                    Option.fromUndefinedOr(materialized.get(topic)),
                  );
                  return yield* acquireRuntimeCoreResourceHandoff((markAcquired) =>
                    leaseLock.withPermit(
                      Effect.uninterruptibleMask((restore) =>
                        Effect.gen(function* () {
                          if (closed) {
                            return yield* Effect.fail(
                              runtimeError(topic, "Runtime Core Source Manager is closed."),
                            );
                          }
                          const stopped = yield* Deferred.make<void>();
                          let observationClosed = false;
                          const observation = {
                            close: Effect.suspend(() => {
                              if (observationClosed) {
                                return Effect.void;
                              }
                              observationClosed = true;
                              sourceHealthObservations.delete(observation);
                              return Deferred.succeed(stopped, undefined).pipe(Effect.asVoid);
                            }),
                          };
                          sourceHealthObservations.add(observation);
                          yield* markAcquired(observation.close);
                          const subscription = {
                            events: SubscriptionRef.changes(runtime.health).pipe(
                              Stream.filter(Option.isSome),
                              Stream.map((value) => value.value),
                              Stream.interruptWhen(Deferred.await(stopped)),
                              Stream.ensuring(observation.close),
                            ),
                            close: () => observation.close,
                          };
                          yield* restore(
                            constructionOptions.sourceHealthHandoff?.beforeReturn ?? Effect.void,
                          );
                          return subscription;
                        }),
                      ),
                    ),
                  );
                }
                const routeCandidate = routeArgs[0];
                if (routeArgs.length !== 1 || routeCandidate === undefined) {
                  return yield* Effect.fail(
                    runtimeError(
                      topic,
                      `Leased Source Topic ${topic} requires exact routeBy.`,
                      "InvalidQuery",
                    ),
                  );
                }
                return yield* acquireRuntimeCoreResourceHandoff((markAcquired) =>
                  leaseLock.withPermit(
                    Effect.uninterruptibleMask((restore) =>
                      Effect.gen(function* () {
                        if (closed) {
                          return yield* Effect.fail(
                            runtimeError(topic, "Runtime Core Source Manager is closed."),
                          );
                        }
                        const route = yield* Effect.fromResult(exactRoute(entry, routeCandidate));
                        const feedKey = yield* Effect.fromResult(feedKeyFor(entry, route));
                        let diagnostics = leasedDiagnostics.get(feedKey);
                        if (diagnostics === undefined) {
                          const state = yield* SubscriptionRef.make(leases.get(feedKey)?.runtime);
                          diagnostics = {
                            route,
                            state,
                            observers: 0,
                          };
                          leasedDiagnostics.set(feedKey, diagnostics);
                        }
                        diagnostics.observers += 1;
                        const stopped = yield* Deferred.make<void>();
                        let observationClosed = false;
                        const observationState = diagnostics;
                        const observation = {
                          close: leaseLock.withPermit(
                            Effect.suspend(() => {
                              if (observationClosed) {
                                return Effect.void;
                              }
                              observationClosed = true;
                              observationState.observers -= 1;
                              if (
                                observationState.observers === 0 &&
                                leasedDiagnostics.get(feedKey) === observationState
                              ) {
                                leasedDiagnostics.delete(feedKey);
                              }
                              sourceHealthObservations.delete(observation);
                              return Deferred.succeed(stopped, undefined).pipe(
                                Effect.andThen(
                                  constructionOptions.afterSourceHealthObservationClose ??
                                    Effect.void,
                                ),
                                Effect.asVoid,
                              );
                            }),
                          ),
                        };
                        sourceHealthObservations.add(observation);
                        yield* markAcquired(observation.close);
                        const observeRuntime = (
                          runtime: SourceLogicalRuntime | undefined,
                        ): Stream.Stream<RuntimeLeasedSourceHealthResult> =>
                          runtime === undefined
                            ? Stream.succeed({ _tag: "Inactive", route })
                            : SubscriptionRef.changes(runtime.health).pipe(
                                Stream.filter(Option.isSome),
                                Stream.map((value) => value.value),
                                Stream.map((health) => ({
                                  _tag: "Active" as const,
                                  route,
                                  health,
                                })),
                              );
                        const subscription = {
                          events: SubscriptionRef.changes(observationState.state).pipe(
                            Stream.switchMap(observeRuntime),
                            Stream.interruptWhen(Deferred.await(stopped)),
                            Stream.ensuring(observation.close),
                          ),
                          close: () => observation.close,
                        };
                        yield* restore(
                          constructionOptions.sourceHealthHandoff?.beforeReturn ?? Effect.void,
                        );
                        return subscription;
                      }),
                    ),
                  ),
                );
              },
            );

          const sourceHealthResultCodec = (topic: string) => {
            const entry = Option.getOrThrow(Option.fromUndefinedOr(entries.get(topic)));
            const routeFields: Record<string, Schema.Codec<unknown, unknown, never, never>> = {};
            for (const field of entry.definition.routeBy) {
              routeFields[field] = Option.getOrThrow(
                Option.fromUndefinedOr(entry.schema.fields[field]),
              );
            }
            const route = Schema.Struct(routeFields);
            return sourceHealthContractSchemas({
              adapterFailure: entry.definition.adapter.failureSchema,
              route,
              adapterMetrics: entry.declaration.metrics,
              rejectionLocation: entry.declaration.rejectionLocation,
              lifecycle: entry.definition.lifecycle,
            }).result;
          };
          const isExactSourceHealthResult = <Topic extends ViewServerSourceOwnedTopic<Topics>>(
            topic: Topic,
            value: unknown,
          ): value is ViewServerSourceHealthResultForTopic<Topics, Topic> => {
            const codec = sourceHealthResultCodec(topic);
            return Schema.is(codec)(value);
          };
          const validateExactSourceHealth = Effect.fn(
            "ViewServerRuntimeCore.source.health.validateExact",
          )(function* <Topic extends ViewServerSourceOwnedTopic<Topics>>(
            topic: Topic,
            value: unknown,
          ) {
            if (!isExactSourceHealthResult(topic, value)) {
              return yield* Effect.fail(
                runtimeError(
                  topic,
                  `Cached Source Health for Topic ${topic} violated its configured contract.`,
                ),
              );
            }
            return value;
          });
          const subscribeSourceHealth: RuntimeCoreSourceManager<Topics>["subscribeSourceHealth"] = (
            input,
          ) => {
            const captured = captureSourceHealthInput<ViewServerSourceOwnedTopic<Topics>>(input);
            if (Result.isFailure(captured)) {
              return Effect.fail(
                runtimeError(
                  "<invalid>",
                  "Source Health input must be one exact { topic, routeBy? } object.",
                  "InvalidQuery",
                ),
              );
            }
            const { topic, route } = captured.success;
            return subscribeProtocolSourceHealth(topic, route).pipe(
              Effect.map((subscription) => ({
                events: subscription.events.pipe(
                  Stream.mapEffect((value) => validateExactSourceHealth(topic, value)),
                ),
                close: subscription.close,
              })),
            );
          };

          const overlayHealth: RuntimeCoreSourceManager<Topics>["overlayHealth"] = (health) =>
            overlaySourceHealth(
              health,
              sourceStatuses.values(),
              aggregateSourceDefinitions,
              sourceHealthSnapshots.values(),
            );

          const close = (yield* Effect.cached(
            leaseLock
              .withPermit(
                Effect.sync(() => {
                  closed = true;
                  return {
                    leases: Array.from(leases.values()),
                    observations: Array.from(sourceHealthObservations),
                  };
                }),
              )
              .pipe(
                Effect.flatMap(({ leases: activeLeases, observations }) =>
                  runAllFinalizers([
                    ...observations.map((observation) => observation.close),
                    ...activeLeases.map((lease) => cleanupLease(lease).pipe(Effect.asVoid)),
                    ...Array.from(materialized.values(), (runtime) =>
                      runtime.stop("runtime-shutdown"),
                    ),
                    Scope.close(managerScope, Exit.void),
                  ]),
                ),
              ),
          )).pipe(Effect.uninterruptible);

          yield* markAcquired(close);
          return {
            hasSources: entries.size > 0,
            acquireLeased,
            subscribeSourceHealth,
            subscribeProtocolSourceHealth,
            decorateMaterialized,
            overlayHealth,
            close,
            fatal,
          };
        }),
      constructionOptions.handoff,
    );
  },
);

export const sourceLeaseTerminalObserver: ColumnLiveViewTerminalObserver = {
  onQueryRegistered: () => Effect.void,
  onTerminalOccurrence: () => Effect.void,
  onTerminalReady: () => Effect.void,
};

export const sourceRuntimeInternals = {
  epochNanos,
  equalRouteValue,
  exactRoute,
  feedKeyFor,
  fatalSourceApplicationCause,
  freezeDecodedMetrics,
  internalPublicId,
  internalStorageKey,
  maintenanceSupervisorCause,
  makeMetricFailureObservation,
  overlaySourceHealth,
  resolveEntries,
  sourceAvailabilityEvent,
  translateSubscription,
} as const;
