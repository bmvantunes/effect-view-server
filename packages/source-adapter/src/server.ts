import {
  Cause,
  Chunk,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Result,
  Schedule,
  Scope,
  Stream,
} from "effect";
import {
  decodeSourceToolkitUpsert,
  isSourceApplicationStateRegistration,
  isSourceAdapterHandle,
  isSourceAttempt,
  isSourceDefinition,
  isSourceDelivery,
  isSourceItemRejection,
  makeSourceAttempt,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeRuntimeSourceFailure,
  makeSourceApplicationTransition,
  makeSourceApplicationStateRegistration as makeNominalSourceApplicationStateRegistration,
  markSourceToolkit,
  makeSourceMaintenanceOperation,
  makeSourceTransitionDelivery,
  resolveSourceAdapterHandle,
} from "./model";
import type {
  SourceAdapterFailure,
  SourceAdapterDescriptor,
  SourceAdapterRuntimeService,
  SourceApplicationExit,
  SourceApplicationTransition,
  SourceApplicationStateRegistration as SourceApplicationStateRegistrationDescriptor,
  SourceApplicationStateRegistrationBindingInput,
  SourceAttempt,
  SourceBufferMetrics,
  SourceDeliveryLane,
  SourceExecutionFailure,
  SourceDefinitionOptionsFamily,
  SourceDefinitionAny,
  SourceDelivery,
  SourceLifecycleDeclaration,
  SourceLifecycleFactoryInput,
  SourceLifecycle,
  SourceLifecycleLocation,
  SourceLifecycleMetrics,
  SourceLifecycleMetricsInput,
  SourceLifecycleOptions,
  SourceMaintenanceOperation,
  SourceMaintenanceResult,
  SourceMutation,
  SourceRuntimeLifecycle,
  SourceSettlement,
  SourceToolkit,
  SourceTermination,
} from "./model";
export { currentEpochNanos, epochNanosFromWallMillis } from "./epoch-clock";
export {
  SourceBuffer,
  makeBackpressurableSourceBuffer,
  makeNonPausableSourceBuffer,
} from "./source-buffer";
export type { BackpressurableSourceBuffer, NonPausableSourceBuffer } from "./source-buffer";

type SourceLifecycleDeclarationAny = SourceLifecycleDeclaration<
  unknown,
  unknown,
  unknown,
  SourceDefinitionOptionsFamily
>;

type AdapterMaterialized<Adapter> = Adapter extends {
  readonly materialized: infer Materialized;
}
  ? Materialized
  : never;

type AdapterLeased<Adapter> = Adapter extends {
  readonly leased: infer Leased;
}
  ? Leased
  : never;

const invalidLifecycleOutput = (message: string) =>
  makeRuntimeSourceFailure({
    _tag: "InvalidSourceDefinition",
    message,
  });

const closeToolkitEnvironment = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, never, Topic>,
  context: Context.Context<Services>,
): SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic> => {
  function delivery(
    mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>,
    settlement?: SourceSettlement<AdapterFailure, Services>,
  ): Effect.Effect<
    SourceDelivery<Row, AdapterFailure, Services, Topic>,
    SourceExecutionFailure<AdapterFailure>
  >;
  function delivery(
    mutation: SourceMutation<Row>,
    settlement: SourceSettlement<AdapterFailure, Services> | undefined,
    transition: SourceApplicationTransition<Topic>,
  ): Effect.Effect<
    SourceDelivery<Row, AdapterFailure, Services, Topic>,
    SourceExecutionFailure<AdapterFailure>
  >;
  function delivery(
    mutationsOrMutation: Chunk.NonEmptyChunk<SourceMutation<Row>> | SourceMutation<Row>,
    settlement?: SourceSettlement<AdapterFailure, Services>,
    transition?: SourceApplicationTransition<Topic>,
  ): Effect.Effect<
    SourceDelivery<Row, AdapterFailure, Services, Topic>,
    SourceExecutionFailure<AdapterFailure>
  > {
    const closedSettlement =
      settlement === undefined
        ? undefined
        : (exit: SourceApplicationExit) => settlement(exit).pipe(Effect.provide(context));
    if (transition === undefined && Chunk.isChunk(mutationsOrMutation)) {
      return toolkit.delivery(mutationsOrMutation, closedSettlement);
    }
    if (transition !== undefined && !Chunk.isChunk(mutationsOrMutation)) {
      return toolkit.delivery(mutationsOrMutation, closedSettlement, transition);
    }
    return Effect.die(
      new TypeError("Closed Source Toolkit received an invalid transition delivery shape."),
    );
  }
  return markSourceToolkit({
    topic: toolkit.topic,
    upsert: toolkit.upsert,
    decodeUpsert: (row) => decodeSourceToolkitUpsert(toolkit, row),
    delete: toolkit.delete,
    delivery,
    reject: (input) => {
      const settlement = input.settlement;
      return toolkit.reject({
        failure: input.failure,
        location: input.location,
        rejectedAtNanos: input.rejectedAtNanos,
        ...(settlement === undefined
          ? {}
          : {
              settlement: (exit) => settlement(exit).pipe(Effect.provide(context)),
            }),
      });
    },
  });
};

export type SourceAdapterServerLifecycle<
  AdapterFailure,
  Declaration extends SourceLifecycleDeclarationAny,
  Lifecycle extends SourceLifecycle,
  Services,
> = {
  readonly initialLaneIds?: <
    const Topic extends string,
    Row extends object,
    Route extends Readonly<Record<string, unknown>>,
  >(
    input: SourceLifecycleMetricsInput<
      SourceLifecycleOptions<Declaration, Row>,
      Lifecycle,
      Route,
      Topic
    >,
  ) => readonly [string, ...ReadonlyArray<string>];
  readonly acquire: <
    const Topic extends string,
    Row extends object,
    Route extends Readonly<Record<string, unknown>>,
  >(
    input: SourceLifecycleFactoryInput<
      SourceLifecycleOptions<Declaration, Row>,
      Lifecycle,
      Route,
      Row,
      AdapterFailure,
      SourceLifecycleLocation<Declaration>,
      Services,
      Topic
    >,
  ) => Effect.Effect<
    SourceAttempt<
      Row,
      AdapterFailure,
      SourceLifecycleLocation<Declaration>,
      Services | Scope.Scope
    >,
    SourceExecutionFailure<AdapterFailure>,
    Services | Scope.Scope
  >;
  readonly metrics: <
    const Topic extends string,
    Row extends object,
    Route extends Readonly<Record<string, unknown>>,
  >(
    input: SourceLifecycleMetricsInput<
      SourceLifecycleOptions<Declaration, Row>,
      Lifecycle,
      Route,
      Topic
    >,
  ) => Effect.Effect<SourceLifecycleMetrics<Declaration>, never, Services>;
  readonly retry: Schedule.Schedule<unknown, SourceTermination<AdapterFailure>, never, Services>;
} & (Declaration extends { readonly applicationState: "required" }
  ? {
      readonly applicationState: SourceApplicationStateRegistrationDescriptor;
    }
  : {
      readonly applicationState?: never;
    });

export type SourceAdapterServerImplementations<Adapter, Services> =
  (AdapterMaterialized<Adapter> extends SourceLifecycleDeclarationAny
    ? {
        readonly materialized: SourceAdapterServerLifecycle<
          SourceAdapterFailure<Adapter>,
          AdapterMaterialized<Adapter>,
          "materialized",
          Services
        >;
      }
    : {
        readonly materialized?: never;
      }) &
    (AdapterLeased<Adapter> extends SourceLifecycleDeclarationAny
      ? {
          readonly leased: SourceAdapterServerLifecycle<
            SourceAdapterFailure<Adapter>,
            AdapterLeased<Adapter>,
            "leased",
            Services
          >;
        }
      : {
          readonly leased?: never;
        });

const closeLaneEnvironment = <Row extends object, AdapterFailure, RejectionLocation, Services>(
  lane: SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services | Scope.Scope>,
  context: Context.Context<Services>,
  scope: Scope.Scope,
): SourceDeliveryLane<Row, AdapterFailure, RejectionLocation> => ({
  id: lane.id,
  events: lane.events.pipe(
    Stream.mapEffect((event) => {
      const closed = Result.try(() => {
        if (event._tag === "SourceDelivery") {
          if (!isSourceDelivery(event)) {
            return undefined;
          }
          const settlement = (exit: SourceApplicationExit) =>
            event
              .settle(exit)
              .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context));
          if (event.transition === undefined) {
            return makeSourceDelivery<Row, AdapterFailure>(event.mutations, settlement);
          }
          return Chunk.size(event.mutations) === 1
            ? makeSourceTransitionDelivery<Row, AdapterFailure>(
                Chunk.headNonEmpty(event.mutations),
                settlement,
                event.transition,
              )
            : undefined;
        }
        return isSourceItemRejection(event)
          ? makeSourceItemRejection<AdapterFailure, RejectionLocation>({
              failure: event.diagnostic.failure,
              location: event.diagnostic.location,
              rejectedAtNanos: event.diagnostic.rejectedAtNanos,
              settlement: (exit) =>
                event
                  .settle(exit)
                  .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context)),
            })
          : undefined;
      });
      return Result.isSuccess(closed) && closed.success !== undefined
        ? Effect.succeed(closed.success)
        : Effect.fail(
            invalidLifecycleOutput(
              "Source Adapter lifecycle emitted a structurally forged Source Lane Event.",
            ),
          );
    }),
    Stream.provideService(Scope.Scope, scope),
    Stream.provideContext(context),
  ),
  bufferMetrics: lane.bufferMetrics,
});

const closeAttemptEnvironment = <Row extends object, AdapterFailure, RejectionLocation, Services>(
  attempt: SourceAttempt<Row, AdapterFailure, RejectionLocation, Services | Scope.Scope>,
  context: Context.Context<Services>,
  scope: Scope.Scope,
): Effect.Effect<
  SourceAttempt<Row, AdapterFailure, RejectionLocation>,
  SourceExecutionFailure<AdapterFailure>
> => {
  if (!isSourceAttempt(attempt)) {
    return Effect.fail(
      invalidLifecycleOutput(
        "Source Adapter lifecycle returned a structurally forged Source Attempt.",
      ),
    );
  }
  const closed = Result.try(() => {
    const [first, ...rest] = attempt.lanes;
    return first === undefined
      ? undefined
      : makeSourceAttempt([
          closeLaneEnvironment(first, context, scope),
          ...rest.map((lane) => closeLaneEnvironment(lane, context, scope)),
        ]);
  });
  return Result.isSuccess(closed) && closed.success !== undefined
    ? Effect.succeed(closed.success)
    : Effect.fail(
        invalidLifecycleOutput(
          "Source Adapter lifecycle returned an invalid nominal Source Attempt.",
        ),
      );
};

const closeLifecycleEnvironment = <
  AdapterFailure,
  Declaration extends SourceLifecycleDeclarationAny,
  Lifecycle extends SourceLifecycle,
  Services,
>(
  implementation: SourceAdapterServerLifecycle<AdapterFailure, Declaration, Lifecycle, Services>,
  context: Context.Context<Services>,
): SourceRuntimeLifecycle<
  AdapterFailure,
  Lifecycle,
  Declaration,
  SourceLifecycleMetrics<Declaration>,
  SourceLifecycleLocation<Declaration>
> => {
  type ClosedLifecycle = SourceRuntimeLifecycle<
    AdapterFailure,
    Lifecycle,
    Declaration,
    SourceLifecycleMetrics<Declaration>,
    SourceLifecycleLocation<Declaration>
  >;
  const acquire: ClosedLifecycle["acquire"] = Effect.fn("SourceAdapterServer.lifecycle.acquire")(
    function* (input) {
      const scope = yield* Effect.scope;
      const attempt = yield* implementation
        .acquire({
          definition: input.definition,
          lifetimeScope: input.lifetimeScope,
          target: input.target,
          toolkit: closeToolkitEnvironment(input.toolkit, context),
        })
        .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context));
      return yield* closeAttemptEnvironment(attempt, context, scope);
    },
  );
  const metrics: ClosedLifecycle["metrics"] = Effect.fn("SourceAdapterServer.lifecycle.metrics")(
    function* (input) {
      return yield* implementation.metrics(input).pipe(Effect.provide(context));
    },
  );
  return {
    ...(implementation.applicationState === undefined
      ? {}
      : { applicationState: implementation.applicationState }),
    acquire,
    metrics,
    ...(implementation.initialLaneIds === undefined
      ? {}
      : { initialLaneIds: implementation.initialLaneIds }),
    retryDefault: (effect, onRetry) =>
      Effect.retry(effect, implementation.retry.pipe(Schedule.tap(onRetry))).pipe(
        Effect.provide(context),
      ),
  };
};

const validateImplementations = (
  adapter: {
    readonly materialized: unknown;
    readonly leased: unknown;
  },
  implementations: object,
): void => {
  const hasMaterialized = Object.hasOwn(implementations, "materialized");
  const hasLeased = Object.hasOwn(implementations, "leased");
  if (hasMaterialized !== (adapter.materialized !== undefined)) {
    throw new TypeError(
      "Source Adapter Server must implement exactly the declared Materialized lifecycle.",
    );
  }
  if (hasLeased !== (adapter.leased !== undefined)) {
    throw new TypeError(
      "Source Adapter Server must implement exactly the declared Leased lifecycle.",
    );
  }
  for (const lifecycle of ["materialized", "leased"] as const) {
    const declaration = Reflect.get(adapter, lifecycle);
    const implementation = Reflect.get(implementations, lifecycle);
    if (typeof implementation !== "object" || implementation === null) {
      continue;
    }
    const registration = Reflect.get(implementation, "applicationState");
    const required =
      typeof declaration === "object" &&
      declaration !== null &&
      Reflect.get(declaration, "applicationState") === "required";
    if (
      (required && !isSourceApplicationStateRegistration(registration)) ||
      (!required && registration !== undefined)
    ) {
      throw new TypeError(
        `Source Adapter Server ${lifecycle} application state registration must exactly match its lifecycle declaration.`,
      );
    }
  }
};

const requireLifecycleImplementation = <Implementation>(
  implementation: Implementation | undefined,
  lifecycle: "Materialized" | "Leased",
): Implementation => {
  if (implementation === undefined) {
    throw new TypeError(`Source Adapter Server ${lifecycle} implementation must be defined.`);
  }
  return implementation;
};

export const makeSourceAdapterServer = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny | undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
  Services = never,
>(
  adapter: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
  implementations: SourceAdapterServerImplementations<
    SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
    Services
  >,
): Layer.Layer<
  Context.Service.Identifier<
    SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>["runtimeService"]
  >,
  never,
  Services
> => {
  if (!isSourceAdapterHandle(adapter)) {
    throw new TypeError("Source Adapter Server requires a nominal Source Adapter descriptor.");
  }
  const handle = resolveSourceAdapterHandle(adapter);
  validateImplementations(adapter, implementations);
  return Layer.effect(handle.runtimeService)(
    Effect.context<Services>().pipe(
      Effect.map((context) => {
        const adapterContext = Context.makeUnsafe<Services>(
          Context.omit(Scope.Scope)(context).mapUnsafe,
        );
        const materializedImplementation =
          "materialized" in implementations ? implementations.materialized : undefined;
        const leasedImplementation =
          "leased" in implementations ? implementations.leased : undefined;
        const service: SourceAdapterRuntimeService<
          AdapterFailure,
          Materialized,
          Leased,
          Name,
          Version
        > = {
          adapter,
          materialized:
            handle.materialized === undefined
              ? undefined
              : closeLifecycleEnvironment(
                  requireLifecycleImplementation(materializedImplementation, "Materialized"),
                  adapterContext,
                ),
          leased:
            handle.leased === undefined
              ? undefined
              : closeLifecycleEnvironment(
                  requireLifecycleImplementation(leasedImplementation, "Leased"),
                  adapterContext,
                ),
        };
        return service;
      }),
    ),
  );
};

export const makeSourceDeliveryLane = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services = never,
>(input: {
  readonly id: string;
  readonly events: Stream.Stream<
    import("./model").SourceLaneEvent<Row, AdapterFailure, RejectionLocation, Services>,
    SourceExecutionFailure<AdapterFailure>,
    Services
  >;
  readonly bufferMetrics?: Effect.Effect<SourceBufferMetrics>;
}): SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services> => {
  if (input.id.length === 0) {
    throw new TypeError("Source Delivery Lane ID must be non-empty.");
  }
  return Object.freeze({
    id: input.id,
    events: input.events,
    bufferMetrics:
      input.bufferMetrics ??
      Effect.succeed({
        _tag: "Unbuffered",
      }),
  });
};

export type SourceAdapterServerView = {
  readonly topics: Readonly<Record<string, unknown>>;
};

export type SourceAdapterServerDefinition<
  Adapter extends SourceDefinitionAny["adapter"] = SourceDefinitionAny["adapter"],
> = SourceDefinitionAny & {
  readonly adapter: Adapter;
};

export type SourceAdapterServerDefinitionEntry<
  Adapter extends SourceDefinitionAny["adapter"] = SourceDefinitionAny["adapter"],
> = {
  readonly topic: string;
  readonly definition: SourceAdapterServerDefinition<Adapter>;
};

export type SourceApplicationStateMaintenanceInput<State, Command> = {
  readonly id: string;
  readonly workId: string;
  readonly isCurrent: (state: State) => boolean;
  readonly onSuccess: Command;
  readonly onFailure: (exit: SourceApplicationExit) => Command;
  readonly onStale: Command;
};

export type SourceApplicationStateSweepInput<Topic extends string, State, Command> = {
  readonly epochNowNanos: bigint;
  readonly state: State;
  readonly update: (command: Command) => void;
  readonly operation: (
    input: SourceApplicationStateMaintenanceInput<State, Command>,
  ) => SourceMaintenanceOperation<Topic>;
  readonly execute: (
    operation: SourceMaintenanceOperation<Topic>,
  ) => Effect.Effect<SourceMaintenanceResult>;
};

export type SourceApplicationStatePreparedTransition<Topic extends string = string> = {
  readonly transition: SourceApplicationTransition<Topic>;
  readonly release: Effect.Effect<void>;
};

const SourceApplicationStateModuleStateTypeId: unique symbol = Symbol.for(
  "@effect-view-server/source-adapter/ApplicationStateModuleState",
);

const preserveSourceApplicationState = <State>(state: State): State => state;

export type SourceApplicationStateModule<
  Topic extends string,
  State,
  Command,
  Metrics,
  SweepOutcome,
> = {
  readonly [SourceApplicationStateModuleStateTypeId]: (state: State) => State;
  readonly prepare: (
    command: Command,
  ) => Effect.Effect<SourceApplicationStatePreparedTransition<Topic>, never, Scope.Scope>;
  readonly metrics: () => Metrics;
  readonly runDueSweep: (
    epochNowNanos: bigint,
    execute: (
      operation: SourceMaintenanceOperation<Topic>,
    ) => Effect.Effect<SourceMaintenanceResult>,
  ) => Effect.Effect<SweepOutcome>;
};

type InternalSourceApplicationStateModule<State, Command, Metrics, SweepOutcome> = {
  readonly topic: string;
  readonly lifetimeIdentity: object;
  readonly [SourceApplicationStateModuleStateTypeId]: (state: State) => State;
  readonly prepare: <const Topic extends string>(
    topic: Topic,
    command: Command,
  ) => Effect.Effect<SourceApplicationStatePreparedTransition<Topic>, never, Scope.Scope>;
  readonly metrics: () => Metrics;
  readonly runDueSweep: <const Topic extends string>(
    topic: Topic,
    epochNowNanos: bigint,
    execute: (
      operation: SourceMaintenanceOperation<Topic>,
    ) => Effect.Effect<SourceMaintenanceResult>,
  ) => Effect.Effect<SweepOutcome>;
};

const makeSourceApplicationStateModule = <State, Command, Metrics, SweepOutcome>(input: {
  readonly topic: string;
  readonly initialState: State;
  readonly reduce: (state: State, command: Command) => State;
  readonly cancelledMaintenanceWorkIds: (state: State, command: Command) => ReadonlyArray<string>;
  readonly acquireTransition: (
    state: State,
    command: Command,
  ) => Effect.Effect<() => void, never, Scope.Scope>;
  readonly metrics: (state: State) => Metrics;
  readonly runDueSweep: <Topic extends string>(
    input: SourceApplicationStateSweepInput<Topic, State, Command>,
  ) => Effect.Effect<SweepOutcome>;
}): InternalSourceApplicationStateModule<State, Command, Metrics, SweepOutcome> => {
  let state = input.initialState;
  const lifetimeIdentity = Object.freeze({});
  type AttemptTransitionReleases = {
    closed: boolean;
    readonly active: Set<() => void>;
  };
  const attemptReleaseSlots = new WeakMap<
    Scope.Scope,
    Deferred.Deferred<AttemptTransitionReleases>
  >();
  const releaseActiveTransitions = (registry: AttemptTransitionReleases) =>
    Effect.suspend(() => {
      registry.closed = true;
      return releaseAttemptTransitions(registry);
    });
  const releaseAttemptTransitions = Effect.fn(
    "SourceAdapterServer.applicationState.releaseAttemptTransitions",
  )(function* (registry: AttemptTransitionReleases) {
    let failure: Cause.Cause<never> | undefined;
    for (const release of registry.active) {
      const exit = yield* Effect.exit(Effect.sync(release));
      if (Exit.isFailure(exit)) {
        failure = failure === undefined ? exit.cause : Cause.combine(failure, exit.cause);
      }
    }
    registry.active.clear();
    if (failure !== undefined) {
      return yield* Effect.failCause(failure);
    }
  });
  const attemptTransitionReleases = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const existing = attemptReleaseSlots.get(scope);
      if (existing !== undefined) {
        return yield* restore(Deferred.await(existing));
      }
      const slot = Deferred.makeUnsafe<AttemptTransitionReleases>();
      attemptReleaseSlots.set(scope, slot);
      const registry: AttemptTransitionReleases = {
        closed: false,
        active: new Set(),
      };
      yield* Scope.addFinalizer(scope, releaseActiveTransitions(registry));
      Deferred.doneUnsafe(slot, Effect.succeed(registry));
      return registry;
    }),
  );
  const dispatch = (command: Command): void => {
    const next = input.reduce(state, command);
    if (
      isSourceAsynchronousValue(next) ||
      (isSourceStateReference(next) && (next === state || !Object.isFrozen(next)))
    ) {
      throw new TypeError(
        "Source Application State reducer must return a new immutable state synchronously.",
      );
    }
    state = next;
  };
  const operation = <const Topic extends string>(
    topic: Topic,
    maintenance: SourceApplicationStateMaintenanceInput<State, Command>,
  ): SourceMaintenanceOperation<Topic> =>
    makeSourceMaintenanceOperation({
      topic,
      id: maintenance.id,
      workId: maintenance.workId,
      lifetimeIdentity,
      isCurrent: () => maintenance.isCurrent(state),
      onSuccess: () => dispatch(maintenance.onSuccess),
      onFailure: (exit) => dispatch(maintenance.onFailure(exit)),
      onStale: () => dispatch(maintenance.onStale),
    });
  return Object.freeze({
    topic: input.topic,
    lifetimeIdentity,
    [SourceApplicationStateModuleStateTypeId]: preserveSourceApplicationState<State>,
    prepare: <const Topic extends string>(topic: Topic, command: Command) =>
      Effect.suspend(() => {
        const cancelledMaintenanceWorkIds = Object.freeze(
          Array.from(input.cancelledMaintenanceWorkIds(state, command)),
        );
        return Effect.uninterruptibleMask(() =>
          Effect.gen(function* () {
            const registry = yield* attemptTransitionReleases;
            if (registry.closed) {
              return yield* Effect.interrupt;
            }
            const release = yield* input.acquireTransition(state, command);
            let released = false;
            const releaseOnce = (): void => {
              if (released) {
                return;
              }
              released = true;
              registry.active.delete(releaseOnce);
              release();
            };
            if (registry.closed) {
              releaseOnce();
              return yield* Effect.interrupt;
            }
            registry.active.add(releaseOnce);
            const transition = makeSourceApplicationTransition(
              topic,
              () => {
                dispatch(command);
                releaseOnce();
              },
              cancelledMaintenanceWorkIds,
              lifetimeIdentity,
            );
            return {
              transition,
              release: Effect.sync(releaseOnce),
            };
          }),
        );
      }),
    metrics: () => {
      const snapshot = input.metrics(state);
      if (isSourceAsynchronousValue(snapshot)) {
        throw new TypeError("Source Application State metrics must return a synchronous snapshot.");
      }
      return snapshot;
    },
    runDueSweep: <const Topic extends string>(
      topic: Topic,
      epochNowNanos: bigint,
      execute: (
        operation: SourceMaintenanceOperation<Topic>,
      ) => Effect.Effect<SourceMaintenanceResult>,
    ) =>
      input.runDueSweep({
        epochNowNanos,
        state,
        update: dispatch,
        operation: (maintenance) => operation(topic, maintenance),
        execute,
      }),
  });
};

const bindSourceApplicationStateModule = <
  const Topic extends string,
  State,
  Command,
  Metrics,
  SweepOutcome,
>(
  module: InternalSourceApplicationStateModule<State, Command, Metrics, SweepOutcome>,
  topic: Topic,
): SourceApplicationStateModule<Topic, State, Command, Metrics, SweepOutcome> => {
  if (module.topic !== topic) {
    throw new TypeError(
      `Source Application State is bound to topic "${module.topic}", not "${topic}".`,
    );
  }
  return Object.freeze({
    [SourceApplicationStateModuleStateTypeId]: module[SourceApplicationStateModuleStateTypeId],
    prepare: (command) => module.prepare(topic, command),
    metrics: module.metrics,
    runDueSweep: (epochNowNanos, execute) => module.runDueSweep(topic, epochNowNanos, execute),
  });
};

const hasExactRegistrationKeys = (
  value: object,
  requiredKeys: ReadonlyArray<string>,
  optionalKeys: ReadonlyArray<string>,
): boolean => {
  const keys = Result.try(() => Reflect.ownKeys(value));
  const allowedKeys = [...requiredKeys, ...optionalKeys];
  if (
    Result.isFailure(keys) ||
    keys.success.length < requiredKeys.length ||
    keys.success.length > allowedKeys.length ||
    keys.success.some((key) => typeof key !== "string" || !allowedKeys.includes(key)) ||
    requiredKeys.some((key) => !keys.success.includes(key))
  ) {
    return false;
  }
  return keys.success.every((key) => {
    const descriptor = Result.try(() => Object.getOwnPropertyDescriptor(value, key));
    return (
      Result.isSuccess(descriptor) &&
      descriptor.success !== undefined &&
      descriptor.success.enumerable === true &&
      "value" in descriptor.success
    );
  });
};

const hasInspectableSynchronousFunction = (value: unknown): boolean => {
  if (typeof value !== "function") {
    return false;
  }
  const constructor = Result.try(() => Reflect.get(value, "constructor"));
  if (Result.isFailure(constructor)) {
    return false;
  }
  const constructorName = Result.try(() => Reflect.get(constructor.success, "name"));
  return (
    Result.isSuccess(constructorName) &&
    constructorName.success !== "AsyncFunction" &&
    constructorName.success !== "AsyncGeneratorFunction"
  );
};

const isSourceAsynchronousValue = (value: unknown): boolean => {
  if (Effect.isEffect(value)) {
    return true;
  }
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    return false;
  }
  const then = Result.try(() => Reflect.get(value, "then"));
  return Result.isFailure(then) || typeof then.success === "function";
};

const isSourceStateReference = (value: unknown): boolean =>
  (typeof value === "object" && value !== null) || typeof value === "function";

export type SourceApplicationStateRegistration<State, Command, Metrics, SweepOutcome> =
  SourceApplicationStateRegistrationDescriptor<
    <const Topic extends string>(
      lifetimeScope: Scope.Scope,
      topic: Topic,
    ) => SourceApplicationStateModule<Topic, State, Command, Metrics, SweepOutcome>
  >;

type SourceSynchronousValue<Value> = Value &
  (Value extends Effect.Effect<unknown, unknown, unknown>
    ? never
    : Value extends PromiseLike<unknown>
      ? never
      : unknown);

type SourceAsynchronousValue = Effect.Effect<unknown, unknown, unknown> | PromiseLike<unknown>;

type RejectSourceAsynchronousValue<Value> = [Extract<Value, SourceAsynchronousValue>] extends [
  never,
]
  ? unknown
  : never;

type SourceApplicationStateRegistrationInput<State, Command, Metrics, SweepOutcome> = {
  readonly sweepIntervalNanos: bigint;
  readonly initialState: (
    input: SourceApplicationStateRegistrationBindingInput,
  ) => SourceSynchronousValue<State>;
  readonly reduce: (state: State, command: Command) => SourceSynchronousValue<State>;
  readonly cancelledMaintenanceWorkIds?: (state: State, command: Command) => ReadonlyArray<string>;
  /**
   * Runs inside the SDK's masked ownership-transfer region. Implementations with a blocking permit
   * wait must mask their own bookkeeping and make only that wait explicitly interruptible before
   * returning an idempotent release callback.
   */
  readonly acquireTransition?: (
    state: State,
    command: Command,
  ) => Effect.Effect<() => void, never, Scope.Scope>;
  readonly metrics: (state: State) => SourceSynchronousValue<Metrics>;
  readonly runDueSweep: <Topic extends string>(
    input: SourceApplicationStateSweepInput<Topic, State, Command>,
  ) => Effect.Effect<SweepOutcome>;
};

type ApplicationStateCandidateKeys<Candidate> = Candidate extends unknown ? keyof Candidate : never;

type RejectExtraApplicationStateKeys<Candidate, Shape> = {
  readonly [Key in Exclude<ApplicationStateCandidateKeys<Candidate>, keyof Shape>]: never;
};

type SourceApplicationStateAdditionalArguments<Candidate, Shape> =
  Exclude<ApplicationStateCandidateKeys<Candidate>, keyof Shape> extends never
    ? readonly []
    : readonly [never];

type SourceApplicationStateCandidateField<Input, Key extends PropertyKey> = Input extends unknown
  ? Key extends keyof Input
    ? Input[Key]
    : never
  : never;

type RejectAnyApplicationStateField<Input, Key extends PropertyKey> = 0 extends 1 &
  SourceApplicationStateCandidateField<Input, Key>
  ? never
  : unknown;

export const makeSourceApplicationStateRegistration = <
  State,
  Command,
  Metrics,
  SweepOutcome,
  const Input,
>(
  input: SourceApplicationStateRegistrationInput<State, Command, Metrics, SweepOutcome> &
    Input &
    RejectExtraApplicationStateKeys<
      NoInfer<Input>,
      SourceApplicationStateRegistrationInput<State, Command, Metrics, SweepOutcome>
    > &
    RejectAnyApplicationStateField<
      NoInfer<Input>,
      keyof SourceApplicationStateRegistrationInput<State, Command, Metrics, SweepOutcome>
    > &
    RejectSourceAsynchronousValue<State> &
    RejectSourceAsynchronousValue<Metrics>,
  ..._unsupported: SourceApplicationStateAdditionalArguments<
    NoInfer<Input>,
    SourceApplicationStateRegistrationInput<State, Command, Metrics, SweepOutcome>
  >
): SourceApplicationStateRegistration<State, Command, Metrics, SweepOutcome> => {
  if (
    !hasExactRegistrationKeys(
      input,
      ["sweepIntervalNanos", "initialState", "reduce", "metrics", "runDueSweep"],
      ["cancelledMaintenanceWorkIds", "acquireTransition"],
    ) ||
    typeof input.sweepIntervalNanos !== "bigint" ||
    input.sweepIntervalNanos <= 0n ||
    !hasInspectableSynchronousFunction(input.initialState) ||
    !hasInspectableSynchronousFunction(input.reduce) ||
    (input.cancelledMaintenanceWorkIds !== undefined &&
      !hasInspectableSynchronousFunction(input.cancelledMaintenanceWorkIds)) ||
    (input.acquireTransition !== undefined &&
      !hasInspectableSynchronousFunction(input.acquireTransition)) ||
    !hasInspectableSynchronousFunction(input.metrics) ||
    !hasInspectableSynchronousFunction(input.runDueSweep)
  ) {
    throw new TypeError(
      "Source Application State registration requires exact synchronous state capabilities and a positive finite sweep interval.",
    );
  }
  const snapshot = Object.freeze({
    sweepIntervalNanos: input.sweepIntervalNanos,
    initialState: input.initialState,
    reduce: input.reduce,
    cancelledMaintenanceWorkIds: input.cancelledMaintenanceWorkIds ?? (() => []),
    acquireTransition: input.acquireTransition ?? (() => Effect.succeed(() => undefined)),
    metrics: input.metrics,
    runDueSweep: input.runDueSweep,
  });
  const modules = new WeakMap<
    Scope.Scope,
    InternalSourceApplicationStateModule<State, Command, Metrics, SweepOutcome>
  >();
  return makeNominalSourceApplicationStateRegistration({
    bind: (binding) => {
      if (modules.has(binding.lifetimeScope)) {
        throw new TypeError(
          "Source Application State registration cannot bind one logical lifetime twice.",
        );
      }
      const initialState = snapshot.initialState(binding);
      if (isSourceAsynchronousValue(initialState)) {
        throw new TypeError(
          "Source Application State initial state must return a synchronous snapshot.",
        );
      }
      if (isSourceStateReference(initialState) && !Object.isFrozen(initialState)) {
        throw new TypeError("Source Application State initial state must be immutable.");
      }
      const module = makeSourceApplicationStateModule<State, Command, Metrics, SweepOutcome>({
        topic: binding.topic,
        initialState,
        reduce: snapshot.reduce,
        cancelledMaintenanceWorkIds: snapshot.cancelledMaintenanceWorkIds,
        acquireTransition: snapshot.acquireTransition,
        metrics: snapshot.metrics,
        runDueSweep: snapshot.runDueSweep,
      });
      module.metrics();
      modules.set(binding.lifetimeScope, module);
    },
    forLifetime: <const Topic extends string>(lifetimeScope: Scope.Scope, topic: Topic) => {
      const module = modules.get(lifetimeScope);
      if (module === undefined) {
        throw new TypeError(
          "Source Application State registration is not bound to this logical lifetime.",
        );
      }
      return bindSourceApplicationStateModule(module, topic);
    },
    lifetimeIdentity: (lifetimeScope) => {
      const module = modules.get(lifetimeScope);
      if (module === undefined) {
        throw new TypeError(
          "Source Application State registration is not bound to this logical lifetime.",
        );
      }
      return module.lifetimeIdentity;
    },
    unbind: (lifetimeScope) => {
      modules.delete(lifetimeScope);
    },
    sweepIntervalNanos: snapshot.sweepIntervalNanos,
    runDueSweep: (lifetimeScope, epochNowNanos, execute) => {
      const module = modules.get(lifetimeScope);
      if (module === undefined) {
        return Effect.die(
          new TypeError(
            "Source Application State registration is not bound to this logical lifetime.",
          ),
        );
      }
      return module.runDueSweep(module.topic, epochNowNanos, execute);
    },
  });
};

const sourceDefinitionUsesAdapter = <const Adapter extends SourceDefinitionAny["adapter"]>(
  value: unknown,
  adapter: Adapter,
): value is SourceAdapterServerDefinition<Adapter> =>
  isSourceDefinition(value) && value.adapter === adapter;

export const collectSourceAdapterDefinitions = <
  const Adapter extends SourceDefinitionAny["adapter"],
>(
  viewServer: SourceAdapterServerView,
  adapter: Adapter,
): ReadonlyArray<SourceAdapterServerDefinitionEntry<Adapter>> => {
  const definitions: Array<SourceAdapterServerDefinitionEntry<Adapter>> = [];
  for (const [topic, descriptor] of Object.entries(
    Object.getOwnPropertyDescriptors(viewServer.topics),
  )) {
    if (!descriptor.enumerable || !("value" in descriptor)) {
      continue;
    }
    const configured = descriptor.value;
    const source =
      typeof configured === "object" && configured !== null
        ? Object.getOwnPropertyDescriptor(configured, "source")?.value
        : undefined;
    if (sourceDefinitionUsesAdapter(source, adapter)) {
      definitions.push({
        topic,
        definition: source,
      });
    }
  }
  return Object.freeze(definitions);
};

export const SourceAdapterServer = {
  applicationState: makeSourceApplicationStateRegistration,
  attempt: makeSourceAttempt,
  definitions: collectSourceAdapterDefinitions,
  lane: makeSourceDeliveryLane,
  make: makeSourceAdapterServer,
} as const;

export type { SourceAdapterRuntimeService };
