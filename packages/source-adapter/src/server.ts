import { Context, Effect, Layer, Schedule, Scope, Stream } from "effect";
import {
  decodeSourceToolkitUpsert,
  isSourceAdapterHandle,
  makeSourceAttempt,
  makeSourceDelivery,
  makeSourceItemRejection,
  markSourceToolkit,
} from "./model";
import type {
  SourceAdapterFailure,
  SourceAdapterHandle,
  SourceAdapterRuntimeService,
  SourceAttempt,
  SourceBufferMetrics,
  SourceDeliveryLane,
  SourceExecutionFailure,
  SourceDefinitionOptionsFamily,
  SourceLifecycleDeclaration,
  SourceLifecycleFactoryInput,
  SourceLifecycle,
  SourceLifecycleLocation,
  SourceLifecycleMetrics,
  SourceLifecycleMetricsInput,
  SourceLifecycleOptions,
  SourceRuntimeLifecycle,
  SourceToolkit,
  SourceTermination,
} from "./model";
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

const closeToolkitEnvironment = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, never, Topic>,
  context: Context.Context<Services>,
): SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic> =>
  markSourceToolkit({
    topic: toolkit.topic,
    upsert: toolkit.upsert,
    decodeUpsert: (row) => decodeSourceToolkitUpsert(toolkit, row),
    delete: toolkit.delete,
    delivery: (mutations, settlement) =>
      toolkit.delivery(
        mutations,
        settlement === undefined
          ? undefined
          : (exit) => settlement(exit).pipe(Effect.provide(context)),
      ),
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

export type SourceAdapterServerLifecycle<
  AdapterFailure,
  Declaration extends SourceLifecycleDeclarationAny,
  Lifecycle extends SourceLifecycle,
  Services,
> = {
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
    SourceAttempt<Row, AdapterFailure, SourceLifecycleLocation<Declaration>, Services>,
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
};

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
  lane: SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services>,
  context: Context.Context<Services>,
  scope: Scope.Scope,
): SourceDeliveryLane<Row, AdapterFailure, RejectionLocation> => ({
  id: lane.id,
  events: lane.events.pipe(
    Stream.map((event) =>
      event._tag === "SourceDelivery"
        ? makeSourceDelivery(event.mutations, (exit) =>
            event
              .settle(exit)
              .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context)),
          )
        : makeSourceItemRejection({
            failure: event.diagnostic.failure,
            location: event.diagnostic.location,
            rejectedAtNanos: event.diagnostic.rejectedAtNanos,
            settlement: (exit) =>
              event
                .settle(exit)
                .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context)),
          }),
    ),
    Stream.provideService(Scope.Scope, scope),
    Stream.provideContext(context),
  ),
  bufferMetrics: lane.bufferMetrics,
});

const closeAttemptEnvironment = <Row extends object, AdapterFailure, RejectionLocation, Services>(
  attempt: SourceAttempt<Row, AdapterFailure, RejectionLocation, Services>,
  context: Context.Context<Services>,
  scope: Scope.Scope,
): SourceAttempt<Row, AdapterFailure, RejectionLocation> => {
  const [first, ...rest] = attempt.lanes;
  return makeSourceAttempt([
    closeLaneEnvironment(first, context, scope),
    ...rest.map((lane) => closeLaneEnvironment(lane, context, scope)),
  ]);
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
          target: input.target,
          toolkit: closeToolkitEnvironment(input.toolkit, context),
        })
        .pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(context));
      return closeAttemptEnvironment(attempt, context, scope);
    },
  );
  const metrics: ClosedLifecycle["metrics"] = Effect.fn("SourceAdapterServer.lifecycle.metrics")(
    function* (input) {
      return yield* implementation.metrics(input).pipe(Effect.provide(context));
    },
  );
  return {
    acquire,
    metrics,
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
  adapter: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
  implementations: SourceAdapterServerImplementations<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
    Services
  >,
): Layer.Layer<
  Context.Service.Identifier<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>["runtimeService"]
  >,
  never,
  Services
> => {
  if (!isSourceAdapterHandle(adapter)) {
    throw new TypeError("Source Adapter Server requires a nominal Source Adapter handle.");
  }
  validateImplementations(adapter, implementations);
  return Layer.effect(adapter.runtimeService)(
    Effect.context<Services>().pipe(
      Effect.map((context) => {
        const adapterContext = Context.makeUnsafe<Services>(
          Context.omit(Scope.Scope)(context).mapUnsafe,
        );
        const materializedImplementation =
          "materialized" in implementations ? implementations.materialized : undefined;
        const leasedImplementation =
          "leased" in implementations ? implementations.leased : undefined;
        const service = {
          adapter,
          materialized:
            adapter.materialized === undefined
              ? undefined
              : closeLifecycleEnvironment(
                  requireLifecycleImplementation(materializedImplementation, "Materialized"),
                  adapterContext,
                ),
          leased:
            adapter.leased === undefined
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

export const SourceAdapterServer = {
  attempt: makeSourceAttempt,
  lane: makeSourceDeliveryLane,
  make: makeSourceAdapterServer,
} as const;

export type { SourceAdapterRuntimeService };
