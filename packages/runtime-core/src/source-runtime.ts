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
  ViewServerTopicConfig,
} from "@effect-view-server/config";
import { validateDecodedRow } from "@effect-view-server/config/internal";
import { makeSchemaJsonIdentity, runAllFinalizers } from "@effect-view-server/effect-utils";
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
  SourceRuntimeFailureSchema,
  sourceHealthSchema,
} from "@effect-view-server/source-adapter";
import {
  isSourceAttempt,
  isSourceDelivery,
  isSourceItemRejection,
  isSourceMutation,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceUpsert,
  markSourceToolkit,
  validateSourceDefinition,
  type SourceAdapterRuntimeService,
  type SourceRuntimeLifecycle,
} from "@effect-view-server/source-adapter/internal";
import {
  Chunk,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Result,
  Schedule,
  Schema,
  Semaphore,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
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
  import("@effect-view-server/source-adapter").SourceAdapterHandle<
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

type SourceLogicalRuntime = {
  readonly entry: SourceRuntimeEntry;
  readonly target: SourceTarget<Readonly<Record<string, unknown>>>;
  readonly health: SubscriptionRef.SubscriptionRef<Option.Option<RuntimeSourceHealth>>;
  readonly status: SubscriptionRef.SubscriptionRef<SourceStatus<unknown, unknown>>;
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
    markAcquired: (release: Effect.Effect<void>) => Effect.Effect<void>,
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
  readonly overlayHealth: (health: ViewServerHealth<Topics>) => ViewServerHealth<Topics>;
  readonly close: Effect.Effect<void>;
};

export type RuntimeCoreSourceManagerConstructionOptions = {
  readonly handoff?: RuntimeCoreResourceHandoffOptions;
  readonly leaseHandoff?: RuntimeCoreResourceHandoffOptions;
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

const sourceRuntimeFailure = (failure: SourceRuntimeError): SourceExecutionError => ({
  _tag: "RuntimeFailure",
  failure,
});

const sourceApplicationFailure = (message: string): SourceRuntimeError => ({
  _tag: "InvalidSourceDelivery",
  message,
});

const equalRouteValue = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left)) {
    return isBigDecimal(right) && left.value === right.value && Object.is(left.scale, right.scale);
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

const sourceDefinitionError = (topic: string, message: string): SourceExecutionError =>
  sourceRuntimeFailure({
    _tag: "InvalidSourceDefinition",
    message: `${topic}: ${message}`,
  });

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
  Topics extends import("@effect-view-server/column-live-view-engine").DecodableTopicDefinitions,
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
    if (binding.schema === undefined) {
      return yield* Effect.fail(
        runtimeError(topic, `Source-owned Topic ${topic} has no valid row Schema.`),
      );
    }
    const serviceOption = Context.getOption(context, definition.adapter.runtimeService);
    if (Option.isNone(serviceOption)) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service ${definition.identity.name} is missing.`,
        ),
      );
    }
    const service = serviceOption.value;
    if (service.adapter !== definition.adapter) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service for ${definition.identity.name} does not match its nominal definition handle.`,
        ),
      );
    }
    const lifecycle = lifecycleFor(definition, service);
    const declaration = declarationFor(definition);
    if (lifecycle === undefined || declaration === undefined) {
      return yield* Effect.fail(
        runtimeError(
          topic,
          `Source Adapter runtime service does not implement declared ${definition.lifecycle} lifecycle.`,
        ),
      );
    }
    entries.set(topic, {
      topic,
      schema: binding.schema,
      definition,
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

type LogicalRuntimeInput<
  Topics extends import("@effect-view-server/column-live-view-engine").DecodableTopicDefinitions,
> = {
  readonly entry: SourceRuntimeEntry;
  readonly target: SourceTarget<Readonly<Record<string, unknown>>>;
  readonly mutations: SourceMutationOperations;
  readonly context: Context.Context<ViewServerSourceRequirements<Topics>>;
  readonly partitionKey?: string;
  readonly feedKey?: string;
  readonly ownedStorageKeys?: Set<string>;
  readonly ownerScope: Scope.Scope;
  readonly onStatus: (status: SourceStatus<unknown, unknown>) => Effect.Effect<void>;
};

function freezeDecodedMetrics<Value>(value: Value): Value;
function freezeDecodedMetrics(value: unknown, active?: WeakSet<object>): unknown;
function freezeDecodedMetrics(value: unknown, active = new WeakSet<object>()): unknown {
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
  if (typeof value !== "object" || value === null) {
    return value;
  }
  if (active.has(value)) {
    throw new TypeError("Source Adapter metrics must not contain cycles.");
  }
  active.add(value);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor !== undefined && "value" in descriptor) {
      freezeDecodedMetrics(descriptor.value, active);
    }
  }
  active.delete(value);
  return Object.freeze(value);
}

const makeLogicalRuntime = Effect.fn("ViewServerRuntimeCore.source.makeLogical")(function* <
  Topics extends import("@effect-view-server/column-live-view-engine").DecodableTopicDefinitions,
>(input: LogicalRuntimeInput<Topics>) {
  const startedAtNanos = yield* Clock.currentTimeNanos;
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
  const metricFailureObservation = makeMetricFailureObservation();
  let supervisorFiber: Fiber.Fiber<void> | undefined;
  const healthLock = Semaphore.makeUnsafe(1);

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
    const sampledAtNanos = yield* Clock.currentTimeNanos;
    const runtimeMetricsResult = yield* runtimeMetrics().pipe(Effect.result);
    if (Result.isFailure(runtimeMetricsResult)) {
      yield* metricFailureObservation.record(Result.fail(runtimeMetricsResult.failure));
    }
    yield* SubscriptionRef.set(
      health,
      Option.some({
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
      }),
    );
  });

  const publish = Effect.fn("ViewServerRuntimeCore.source.health.publishStatus")(function* (
    nextStatus: SourceStatus<unknown, unknown>,
  ) {
    yield* healthLock.withPermit(
      Effect.gen(function* () {
        yield* SubscriptionRef.set(status, nextStatus);
        if (hasCachedAdapterMetrics) {
          yield* publishHealth(nextStatus, cachedAdapterMetrics);
        }
        yield* input.onStatus(nextStatus);
      }),
    );
    yield* Effect.yieldNow;
  });

  const validateAdapterMetrics = Effect.fn("ViewServerRuntimeCore.source.metrics.adapter.validate")(
    function* (metrics: unknown) {
      const decoded = yield* Schema.decodeUnknownEffect(input.entry.declaration.metrics)(
        metrics,
      ).pipe(
        Effect.mapError(() =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics outside its declared Schema.`,
          }),
        ),
      );
      const frozen = yield* Effect.try({
        try: () => freezeDecodedMetrics(decoded),
        catch: () =>
          sourceRuntimeFailure({
            _tag: "InvalidSourceMetrics",
            message: `Source Adapter ${input.entry.definition.identity.name} returned metrics that cannot be frozen.`,
          }),
      });
      yield* healthLock.withPermit(
        Effect.gen(function* () {
          cachedAdapterMetrics = frozen;
          hasCachedAdapterMetrics = true;
          const currentStatus = SubscriptionRef.getUnsafe(status);
          yield* publishHealth(currentStatus, frozen);
          yield* input.onStatus(currentStatus);
        }),
      );
    },
  );
  const sampleAdapterMetrics = Effect.fn("ViewServerRuntimeCore.source.metrics.adapter.sample")(
    function* () {
      const result = yield* input.entry.lifecycle
        .metrics({
          topic: input.entry.topic,
          definition: input.entry.definition.options,
          target: input.target,
        })
        .pipe(Effect.flatMap(validateAdapterMetrics), Effect.result);
      yield* metricFailureObservation.record(result);
    },
  );

  const validateFailure = Effect.fn("ViewServerRuntimeCore.source.failure.validate")(function* (
    failure: SourceExecutionError,
  ) {
    if (failure._tag === "AdapterFailure") {
      return yield* input.entry.definition.adapter
        .failure(failure.failure)
        .pipe(Effect.mapError(sourceRuntimeFailure));
    }
    const runtime = yield* Schema.decodeUnknownEffect(SourceRuntimeFailureSchema)(
      failure.failure,
    ).pipe(
      Effect.mapError(() =>
        sourceDefinitionError(
          input.entry.topic,
          "Source Runtime Failure did not satisfy the SDK Schema.",
        ),
      ),
    );
    return sourceRuntimeFailure(runtime);
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
    return markSourceToolkit<object, unknown, unknown, never, string>({
      topic: input.entry.topic,
      upsert: decodeUpsert,
      decodeUpsert,
      delete: (id) =>
        typeof id === "string" && id.length > 0
          ? Effect.succeed(makeSourceDelete(id))
          : Effect.fail(sourceIdError(input.entry.topic)),
      delivery: (mutations, settlement) => {
        if (
          Chunk.size(mutations) === 0 ||
          !Chunk.every(mutations, isSourceMutation) ||
          (settlement !== undefined && typeof settlement !== "function")
        ) {
          return Effect.fail(
            sourceDefinitionError(
              input.entry.topic,
              "Source Delivery requires one or more nominal Source Mutations.",
            ),
          );
        }
        return Effect.succeed(makeSourceDelivery(mutations, settlement));
      },
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
          if (typeof rejection.rejectedAtNanos !== "bigint") {
            return yield* Effect.fail(
              sourceDefinitionError(
                input.entry.topic,
                "Source Rejection timestamp must be epoch nanoseconds.",
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

  const applyMutationOperation = Effect.fn("ViewServerRuntimeCore.source.mutation.operation")(
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
        if (
          input.feedKey !== undefined &&
          input.partitionKey !== undefined &&
          input.ownedStorageKeys !== undefined
        ) {
          const ownedStorageKeys = input.ownedStorageKeys;
          const storageKey = internalStorageKey(input.entry.topic, input.feedKey, id);
          const previouslyOwned = ownedStorageKeys.has(storageKey);
          ownedStorageKeys.add(storageKey);
          yield* input.mutations
            .publishRowsWithStorageKeys(
              input.entry.topic,
              [{ storageKey, row }],
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
            id,
          } satisfies AppliedSourceMutation;
        }
        yield* input.mutations
          .publishRows(input.entry.topic, [row])
          .pipe(Effect.mapError(applicationFailure));
        return {
          _tag: "Upsert",
          id,
        } satisfies AppliedSourceMutation;
      }
      if (mutation.id.length === 0) {
        return yield* Effect.fail(sourceIdFailure(input.entry.topic));
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
    },
  );

  const applyMutation = Effect.fn("ViewServerRuntimeCore.source.mutation.apply")(function* (
    mutation: SourceMutation,
  ) {
    attemptedMutationCount += 1n;
    const application = yield* Effect.exit(applyMutationOperation(mutation));
    if (Exit.isFailure(application)) {
      failedMutationCount += 1n;
      return yield* Effect.failCause(application.cause);
    }
    lastAppliedMutationAtNanos = yield* Clock.currentTimeNanos;
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

  const settle = Effect.fn("ViewServerRuntimeCore.source.settle")(function* (
    settlement: import("@effect-view-server/source-adapter").SourceSettlement<unknown>,
    applicationExit: import("@effect-view-server/source-adapter").SourceApplicationExit,
  ) {
    yield* Effect.uninterruptible(settlement(applicationExit)).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          completedSettlementCount += 1n;
        }),
      ),
      Effect.tapError((settlementFailure) =>
        Effect.sync(() => {
          failedSettlementCount += 1n;
        }).pipe(
          Effect.andThen(
            Exit.isFailure(applicationExit)
              ? Effect.logError("Source settlement failed after mutation application failure.", {
                  topic: input.entry.topic,
                  applicationCause: applicationExit.cause,
                  settlementFailure,
                })
              : Effect.void,
          ),
        ),
      ),
      Effect.mapError(
        (failure): SourceExecutionError => ({
          _tag: "AdapterFailure",
          failure,
        }),
      ),
    );
  });

  const laneEvent = Effect.fn("ViewServerRuntimeCore.source.lane.event")(function* (
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
      return yield* Effect.acquireUseRelease(
        Effect.void,
        () =>
          Effect.gen(function* () {
            const failure = yield* validateFailure(event.diagnostic.failure);
            const location = yield* Schema.decodeUnknownEffect(
              input.entry.declaration.rejectionLocation,
            )(event.diagnostic.location).pipe(
              Effect.mapError(() =>
                sourceDefinitionError(
                  input.entry.topic,
                  "Source Rejection Location does not satisfy its declared Schema.",
                ),
              ),
            );
            if (typeof event.diagnostic.rejectedAtNanos !== "bigint") {
              return yield* Effect.fail(
                sourceDefinitionError(
                  input.entry.topic,
                  "Source Rejection timestamp must be epoch nanoseconds.",
                ),
              );
            }
            latestRejection = {
              failure,
              location,
              rejectedAtNanos: event.diagnostic.rejectedAtNanos,
            };
            rejectedItemCount += 1n;
            lastRejectionAtNanos = event.diagnostic.rejectedAtNanos;
            degradedAtNanos ??= yield* Clock.currentTimeNanos;
            yield* publish({
              _tag: "Degraded",
              attempt: currentAttempt,
              degradedAtNanos,
              latestRejection,
            });
          }),
        () => settle(event.settle, Exit.void),
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
    lastDeliveryAtNanos = yield* Clock.currentTimeNanos;
    yield* Effect.acquireUseRelease(
      Effect.void,
      () => Effect.forEach(event.mutations, applyMutation, { discard: true }),
      (_resource, applicationExit) => settle(event.settle, applicationExit),
    ).pipe(
      Effect.mapError((failure) =>
        failure._tag === "AdapterFailure" || failure._tag === "RuntimeFailure"
          ? failure
          : sourceRuntimeFailure(failure),
      ),
    );
  });

  const runLane = (lane: SourceLane) =>
    lane.events.pipe(
      Stream.runForEach((event) => laneEvent(lane.id, event)),
      Effect.catch((failure) =>
        validateFailure(failure).pipe(
          Effect.catch((validationFailure) => Effect.succeed(validationFailure)),
          Effect.flatMap((validatedFailure) =>
            Effect.fail<SourceTermination<unknown>>({
              _tag: "Failed",
              failure: validatedFailure,
            }),
          ),
        ),
      ),
      Effect.andThen(
        Effect.fail<SourceTermination<unknown>>({
          _tag: "UnexpectedCompletion",
        }),
      ),
    );

  const runAttempt = Effect.fn("ViewServerRuntimeCore.source.attempt.run")(function* (
    previous: SourceTermination<unknown> | undefined,
  ) {
    lastAttemptStartedAtNanos = yield* Clock.currentTimeNanos;
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
        target: input.target,
        toolkit: makeToolkit(),
      })
      .pipe(
        Effect.catch((failure) =>
          validateFailure(failure).pipe(
            Effect.catch((validationFailure) => Effect.succeed(validationFailure)),
            Effect.flatMap((validatedFailure) =>
              Effect.fail<SourceTermination<unknown>>({
                _tag: "Failed",
                failure: validatedFailure,
              }),
            ),
          ),
        ),
      );
    if (!isSourceAttempt(attempt)) {
      return yield* Effect.fail({
        _tag: "Failed",
        failure: sourceDefinitionError(
          input.entry.topic,
          "Lifecycle acquisition returned a structurally forged Source Attempt.",
        ),
      } satisfies SourceTermination<unknown>);
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
      return yield* Effect.fail({
        _tag: "Failed",
        failure: sourceDefinitionError(
          input.entry.topic,
          "Source Attempt requires non-empty unique lane IDs, Streams, and buffer metrics.",
        ),
      } satisfies SourceTermination<unknown>);
    }
    const nextLaneIds = laneMetadata.success
      .map((lane) => lane.id)
      .sort((left, right) => left.localeCompare(right));
    if (
      stableLaneIds !== undefined &&
      (stableLaneIds.length !== nextLaneIds.length ||
        stableLaneIds.some((laneId, index) => laneId !== nextLaneIds[index]))
    ) {
      return yield* Effect.fail({
        _tag: "Failed",
        failure: sourceDefinitionError(
          input.entry.topic,
          "Source Delivery Lane IDs must remain stable across retries.",
        ),
      } satisfies SourceTermination<unknown>);
    }
    stableLaneIds ??= nextLaneIds;
    laneCounters.clear();
    for (const lane of attempt.lanes) {
      laneCounters.set(lane.id, {
        buffer: lane.bufferMetrics,
      });
    }
    const readyAtNanos = yield* Clock.currentTimeNanos;
    const stickyDegradedAtNanos = degradedAtNanos ?? readyAtNanos;
    yield* publish(
      latestRejection === undefined
        ? {
            _tag: "Ready",
            attempt: currentAttempt,
            readyAtNanos,
          }
        : {
            _tag: "Degraded",
            attempt: currentAttempt,
            degradedAtNanos: stickyDegradedAtNanos,
            latestRejection,
          },
    );
    const laneWorkers = attempt.lanes.map(runLane);
    return yield* Effect.all(laneWorkers, {
      concurrency: "unbounded",
      discard: true,
    });
  });

  let previousTermination: SourceTermination<unknown> | undefined;
  const attemptWithObservation = Effect.scoped(
    Effect.gen(function* () {
      const metricFailure = yield* Deferred.make<SourceExecutionError>();
      const registration = yield* metricFailureObservation.register(metricFailure);
      if (registration._tag === "Failed") {
        return yield* Effect.fail<SourceTermination<unknown>>({
          _tag: "Failed",
          failure: registration.failure,
        });
      }
      return yield* Effect.raceFirst(
        runAttempt(previousTermination),
        Deferred.await(metricFailure).pipe(
          Effect.flatMap((failure) =>
            Effect.fail<SourceTermination<unknown>>({
              _tag: "Failed",
              failure,
            }),
          ),
        ),
      ).pipe(Effect.ensuring(metricFailureObservation.unregister(metricFailure)));
    }).pipe(
      Effect.tapError((termination) =>
        Effect.gen(function* () {
          previousTermination = termination;
          lastTerminationAtNanos = yield* Clock.currentTimeNanos;
        }),
      ),
    ),
  );
  const onRetry = Effect.fn("ViewServerRuntimeCore.source.retry.waiting")(function* (
    metadata: Schedule.Metadata<unknown, SourceTermination<unknown>>,
  ) {
    const decidedAtNanos = yield* Clock.currentTimeNanos;
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
      Clock.currentTimeNanos.pipe(
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

  const stop = Effect.fn("ViewServerRuntimeCore.source.stop")(function* (
    reason: import("@effect-view-server/source-adapter").SourceStoppingReason,
  ) {
    const stoppingAtNanos = yield* Clock.currentTimeNanos;
    yield* publish({ _tag: "Stopping", reason, stoppingAtNanos });
    yield* Fiber.interrupt(Option.getOrThrow(Option.fromUndefinedOr(supervisorFiber))).pipe(
      Effect.asVoid,
    );
    yield* Scope.close(scope, Exit.void);
  });

  const logical: SourceLogicalRuntime = {
    entry: input.entry,
    target: input.target,
    health,
    status,
    run,
    stop,
  };
  yield* sampleAdapterMetrics();
  yield* Effect.forkIn(
    Effect.forever(Effect.sleep("1 second").pipe(Effect.andThen(sampleAdapterMetrics()))),
    scope,
    {
      startImmediately: true,
    },
  );
  supervisorFiber = yield* Effect.forkIn(run, scope, {
    startImmediately: true,
  });
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
          ? "Source delivery continues with one or more settled item rejections."
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

const overlaySourceHealth = <
  Topics extends import("@effect-view-server/column-live-view-engine").DecodableTopicDefinitions,
>(
  health: ViewServerHealth<Topics>,
  statuses: Iterable<{
    readonly topic: string;
    readonly status: SourceStatus<unknown, unknown>;
  }>,
): ViewServerHealth<Topics> => {
  const statusByTopic = new Map<string, "ready" | "degraded" | "starting">();
  for (const { topic, status } of statuses) {
    const next =
      status._tag === "Ready"
        ? "ready"
        : status._tag === "Starting" || status._tag === "Reacquiring"
          ? "starting"
          : "degraded";
    const current = statusByTopic.get(topic);
    if (
      current === undefined ||
      next === "degraded" ||
      (next === "starting" && current === "ready")
    ) {
      statusByTopic.set(topic, next);
    }
  }
  const topics = { ...health.engine.topics };
  let aggregateSourceStatus: "ready" | "degraded" | "starting" = "ready";
  for (const [topic, sourceStatus] of statusByTopic) {
    const current: unknown = Reflect.get(topics, topic);
    if (typeof current === "object" && current !== null) {
      const engineStatus = Reflect.get(current, "status");
      const status =
        engineStatus === "degraded" || sourceStatus === "degraded"
          ? "degraded"
          : engineStatus === "starting" || sourceStatus === "starting"
            ? "starting"
            : "ready";
      Reflect.set(topics, topic, { ...current, status });
    }
    aggregateSourceStatus =
      aggregateSourceStatus === "degraded" || sourceStatus === "degraded"
        ? "degraded"
        : aggregateSourceStatus === "starting" || sourceStatus === "starting"
          ? "starting"
          : "ready";
  }
  const status =
    health.status === "stopping"
      ? "stopping"
      : health.status === "degraded" || aggregateSourceStatus === "degraded"
        ? "degraded"
        : health.status === "starting" || aggregateSourceStatus === "starting"
          ? "starting"
          : "ready";
  return {
    ...health,
    status,
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
  function* <
    const Topics extends
      import("@effect-view-server/column-live-view-engine").DecodableTopicDefinitions,
  >(
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
          const materialized = new Map<string, SourceLogicalRuntime>();
          const leases = new Map<
            string,
            {
              readonly feedKey: string;
              readonly route: Readonly<Record<string, unknown>>;
              readonly runtime: SourceLogicalRuntime;
              readonly ownedStorageKeys: Set<string>;
              readonly partition: ColumnLiveViewEngineQueryPartition;
              readonly scope: Scope.Closeable;
              subscribers: number;
            }
          >();
          const leasedDiagnostics = new Map<
            string,
            {
              readonly route: Readonly<Record<string, unknown>>;
              readonly state: SubscriptionRef.SubscriptionRef<SourceLogicalRuntime | undefined>;
              observers: number;
            }
          >();
          const leaseLock = Semaphore.makeUnsafe(1);
          const sourceStatuses = new Map<
            string,
            {
              readonly topic: string;
              readonly status: SourceStatus<unknown, unknown>;
            }
          >();
          let closed = false;
          let leaseSequence = 0n;

          for (const entry of entries.values()) {
            if (entry.definition.lifecycle !== "materialized") {
              continue;
            }
            const runtime = yield* makeLogicalRuntime({
              entry,
              target: { _tag: "Materialized" },
              mutations: sourceMutations,
              context,
              ownerScope: managerScope,
              onStatus: (status) =>
                Effect.sync(() => {
                  sourceStatuses.set(`materialized:${entry.topic}`, {
                    topic: entry.topic,
                    status,
                  });
                }).pipe(Effect.andThen(onHealthChange)),
            });
            materialized.set(entry.topic, runtime);
          }

          const cleanupLease = Effect.fn("ViewServerRuntimeCore.source.lease.cleanup")(
            (lease: {
              readonly feedKey: string;
              readonly runtime: SourceLogicalRuntime;
              readonly ownedStorageKeys: Set<string>;
              readonly partition: ColumnLiveViewEngineQueryPartition;
              readonly scope: Scope.Closeable;
            }) =>
              runAllFinalizers([
                lease.runtime.stop("lease-release"),
                runAllFinalizers(
                  Array.from(lease.ownedStorageKeys, (storageKey) =>
                    sourceMutations.deleteStorageKey(
                      lease.runtime.entry.topic,
                      storageKey,
                      lease.partition.key,
                    ),
                  ),
                ),
                Effect.sync(() => {
                  lease.ownedStorageKeys.clear();
                  leases.delete(lease.feedKey);
                  sourceStatuses.delete(lease.feedKey);
                }),
                Effect.suspend(() => {
                  const diagnostics = leasedDiagnostics.get(lease.feedKey);
                  return diagnostics === undefined
                    ? Effect.void
                    : SubscriptionRef.set(diagnostics.state, undefined);
                }),
                onHealthChange,
                Scope.close(lease.scope, Exit.void),
              ]).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Source lease cleanup failed.", cause),
                ),
              ),
          );

          const acquireLeased: RuntimeCoreSourceManager<Topics>["acquireLeased"] = (
            topic,
            query,
            markAcquired,
          ) =>
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
                  if (lease === undefined) {
                    lease = yield* restore(
                      acquireRuntimeCoreResourceHandoff(
                        (markLeaseAcquired) =>
                          Effect.gen(function* () {
                            const scope = yield* Scope.fork(managerScope, "sequential");
                            yield* markLeaseAcquired(Scope.close(scope, Exit.void));
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
                            const runtime = yield* makeLogicalRuntime({
                              entry,
                              target: { _tag: "Leased", route },
                              mutations: sourceMutations,
                              context,
                              ownerScope: scope,
                              partitionKey: partition.key,
                              feedKey,
                              ownedStorageKeys,
                              onStatus: (status) =>
                                Effect.sync(() => {
                                  sourceStatuses.set(feedKey, {
                                    topic: entry.topic,
                                    status,
                                  });
                                }).pipe(Effect.andThen(onHealthChange)),
                            });
                            const acquiredLease = {
                              feedKey,
                              route,
                              runtime,
                              ownedStorageKeys,
                              partition,
                              scope,
                              subscribers: 0,
                            };
                            yield* markLeaseAcquired(cleanupLease(acquiredLease));
                            leases.set(feedKey, acquiredLease);
                            const diagnostics = leasedDiagnostics.get(feedKey);
                            if (diagnostics !== undefined) {
                              yield* SubscriptionRef.set(diagnostics.state, runtime);
                            }
                            return acquiredLease;
                          }),
                        constructionOptions.leaseHandoff,
                      ),
                    );
                  }
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
                        return lease!.subscribers === 0 ? cleanupLease(lease!) : Effect.void;
                      }),
                    );
                  }).pipe(Effect.uninterruptible);
                  const currentLease = lease;
                  const acquiredLease: RuntimeCoreSourceLease = {
                    partition: currentLease.partition,
                    translate: (subscription, ownedQuery, queryId) =>
                      attachSourceAvailability(
                        translateSubscription(subscription, ownedQuery),
                        currentLease.runtime,
                        queryId,
                      ),
                    release,
                  };
                  yield* markAcquired(release);
                  return Option.some(acquiredLease);
                }),
              ),
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
                  return {
                    events: SubscriptionRef.changes(runtime.health).pipe(
                      Stream.filter(Option.isSome),
                      Stream.map((value) => value.value),
                    ),
                    close: () => Effect.void,
                  };
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
                return yield* leaseLock.withPermit(
                  Effect.gen(function* () {
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
                    let observationClosed = false;
                    const observation = diagnostics;
                    const closeObservation = leaseLock.withPermit(
                      Effect.sync(() => {
                        if (observationClosed) {
                          return;
                        }
                        observationClosed = true;
                        observation.observers -= 1;
                        if (
                          observation.observers === 0 &&
                          leasedDiagnostics.get(feedKey) === observation
                        ) {
                          leasedDiagnostics.delete(feedKey);
                        }
                      }),
                    );
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
                    return {
                      events: SubscriptionRef.changes(observation.state).pipe(
                        Stream.switchMap(observeRuntime),
                        Stream.ensuring(closeObservation),
                      ),
                      close: () => closeObservation,
                    };
                  }),
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
            const health = sourceHealthSchema({
              adapterFailure: entry.definition.adapter.failureSchema,
              route,
              adapterMetrics: entry.declaration.metrics,
              rejectionLocation: entry.declaration.rejectionLocation,
            });
            return entry.definition.lifecycle === "materialized"
              ? health
              : Schema.Union([
                  Schema.TaggedStruct("Inactive", { route }),
                  Schema.TaggedStruct("Active", { route, health }),
                ]);
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
            ...arguments_
          ) => {
            const [topic, route] = arguments_;
            return subscribeProtocolSourceHealth(topic, route === undefined ? [] : [route]).pipe(
              Effect.map((subscription) => ({
                events: subscription.events.pipe(
                  Stream.mapEffect((value) => validateExactSourceHealth(topic, value)),
                ),
                close: subscription.close,
              })),
            );
          };

          const overlayHealth: RuntimeCoreSourceManager<Topics>["overlayHealth"] = (health) =>
            overlaySourceHealth(health, sourceStatuses.values());

          const close = (yield* Effect.cached(
            leaseLock.withPermit(
              Effect.suspend(() => {
                closed = true;
                return runAllFinalizers([
                  ...Array.from(leases.values(), cleanupLease),
                  ...Array.from(materialized.values(), (runtime) =>
                    runtime.stop("runtime-shutdown"),
                  ),
                  Scope.close(managerScope, Exit.void),
                ]);
              }),
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
  equalRouteValue,
  exactRoute,
  feedKeyFor,
  internalPublicId,
  internalStorageKey,
  makeMetricFailureObservation,
  overlaySourceHealth,
  resolveEntries,
  sourceAvailabilityEvent,
  translateSubscription,
} as const;
