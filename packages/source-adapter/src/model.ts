import { Chunk, Context, Effect, Exit, Result, Schedule, Schema, Scope, Stream } from "effect";
import {
  hasSourceModelSelfBrand as hasSelfBrand,
  isSourceAdapterHandle,
  SourceAdapterTypeId,
  sourceAdapterDescriptors,
  sourceAdapterHandles,
} from "./adapter-brand";
import {
  isSourceDefinition,
  registerSourceDefinition,
  SourceDefinitionTypeId,
  SourceDefinitionTypesTypeId,
  validateSourceDefinition,
} from "./definition-brand";

declare const SourceDefinitionFactoryRowTypeId: unique symbol;
const SourceMutationTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceMutation",
);
const SourceDeliveryTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDelivery",
);
const SourceItemRejectionTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceItemRejection",
);
const SourceToolkitTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceToolkit",
);
const SourceToolkitDecodeTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceToolkitDecode",
);
const SourceAttemptTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceAttempt",
);
const SourceExecutableTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceExecutable",
);
const SourceApplicationTransitionTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceApplicationTransition",
);
const SourceMaintenanceOperationTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceMaintenanceOperation",
);
const SourceApplicationStateRegistrationTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceApplicationStateRegistration",
);
const sourceApplicationTransitions = new WeakMap<object, SourceApplicationTransitionInternal>();
const sourceMaintenanceOperations = new WeakMap<object, SourceMaintenanceOperationInternal>();
const sourceApplicationStateRegistrations = new WeakMap<
  object,
  SourceApplicationStateRegistrationInternal
>();

export type SourceLifecycle = "materialized" | "leased";

export type SourceAdapterIdentity<
  Name extends string = string,
  Version extends string | undefined = string | undefined,
> = {
  readonly name: Name;
  readonly version?: Version;
};

export const SourceAdapterIdentitySchema = Schema.Struct({
  name: Schema.NonEmptyString,
  version: Schema.optionalKey(Schema.NonEmptyString),
});

export interface SourceDefinitionOptionsFamily {
  readonly Row: object;
  readonly type: unknown;
}

export interface ConstantSourceDefinitionOptionsFamily<
  Options,
> extends SourceDefinitionOptionsFamily {
  readonly type: Options;
}

export type SourceDefinitionOptionsFor<
  Family extends SourceDefinitionOptionsFamily,
  Row extends object,
> = (Family & { readonly Row: Row })["type"];

export type SourceLifecycleDeclaration<
  Metrics,
  RejectionLocation,
  DefinitionOptions = unknown,
  DefinitionFamily extends SourceDefinitionOptionsFamily =
    ConstantSourceDefinitionOptionsFamily<DefinitionOptions>,
> = {
  readonly metrics: Schema.Codec<Metrics, unknown, never, never>;
  readonly rejectionLocation: Schema.Codec<RejectionLocation, unknown, never, never>;
  readonly definitionOptions: SourceDefinitionOptionsToken<DefinitionOptions, DefinitionFamily>;
  readonly applicationState?: "required";
};

const SourceDefinitionOptionsTokenTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDefinitionOptionsToken",
);

export type SourceDefinitionOptionsToken<
  Options,
  Family extends SourceDefinitionOptionsFamily = ConstantSourceDefinitionOptionsFamily<Options>,
> = {
  readonly [SourceDefinitionOptionsTokenTypeId]: () => SourceDefinitionOptionsToken<
    Options,
    Family
  >;
  readonly _Options?: Options;
  readonly _Family?: Family;
};

type SourceLifecycleDeclarationAny = SourceLifecycleDeclaration<
  unknown,
  unknown,
  unknown,
  SourceDefinitionOptionsFamily
>;

export type SourceRuntimeFailure =
  | {
      readonly _tag: "InvalidSourceDefinition";
      readonly message: string;
    }
  | {
      readonly _tag: "InvalidSourceDelivery";
      readonly message: string;
    }
  | {
      readonly _tag: "InvalidTopicRow";
      readonly message: string;
      readonly topic: string;
    }
  | {
      readonly _tag: "InvalidCanonicalId";
      readonly message: string;
      readonly topic: string;
    }
  | {
      readonly _tag: "InvalidFeedRoute";
      readonly message: string;
      readonly topic: string;
    }
  | {
      readonly _tag: "InvalidSourceMetrics";
      readonly message: string;
    }
  | {
      readonly _tag: "InvalidSourceSettlement";
      readonly message: "Source Settlement callback threw before returning an Effect";
    }
  | {
      readonly _tag: "SourceBufferOverflow";
      readonly message: string;
      readonly capacity: number;
    };

export const SourceRuntimeFailureSchema: Schema.Codec<SourceRuntimeFailure> = Schema.Union([
  Schema.TaggedStruct("InvalidSourceDefinition", {
    message: Schema.String,
  }),
  Schema.TaggedStruct("InvalidSourceDelivery", {
    message: Schema.String,
  }),
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
  Schema.TaggedStruct("InvalidSourceMetrics", {
    message: Schema.String,
  }),
  Schema.TaggedStruct("InvalidSourceSettlement", {
    message: Schema.Literal("Source Settlement callback threw before returning an Effect"),
  }),
  Schema.TaggedStruct("SourceBufferOverflow", {
    message: Schema.String,
    capacity: Schema.Number,
  }),
]);

export type SourceExecutionFailure<AdapterFailure> =
  | {
      readonly _tag: "AdapterFailure";
      readonly failure: AdapterFailure;
    }
  | {
      readonly _tag: "RuntimeFailure";
      readonly failure: SourceRuntimeFailure;
    };

export const sourceExecutionFailureSchema = <AdapterFailure>(
  adapterFailure: Schema.Codec<AdapterFailure, unknown, never, never>,
): Schema.Codec<SourceExecutionFailure<AdapterFailure>, unknown, never, never> =>
  Schema.Union([
    Schema.TaggedStruct("AdapterFailure", {
      failure: adapterFailure,
    }),
    Schema.TaggedStruct("RuntimeFailure", {
      failure: SourceRuntimeFailureSchema,
    }),
  ]);

export type SourceTermination<AdapterFailure> =
  | {
      readonly _tag: "Failed";
      readonly failure: SourceExecutionFailure<AdapterFailure>;
    }
  | {
      readonly _tag: "UnexpectedCompletion";
    };

export type SourceRetryPolicy<AdapterFailure, Services = never> = Schedule.Schedule<
  unknown,
  SourceTermination<AdapterFailure>,
  never,
  Services
>;

export type SourceRetrySelection<AdapterFailure, Services = never> =
  | {
      readonly _tag: "UseAdapterDefault";
    }
  | {
      readonly _tag: "Override";
      readonly policy: SourceRetryPolicy<AdapterFailure, Services>;
    };

export type SourceTarget<Route extends Readonly<Record<string, unknown>> = never> =
  | {
      readonly _tag: "Materialized";
    }
  | {
      readonly _tag: "Leased";
      readonly route: Route;
    };

export type SourceTargetForLifecycle<
  Lifecycle extends SourceLifecycle,
  Route extends Readonly<Record<string, unknown>>,
> = Lifecycle extends "materialized"
  ? {
      readonly _tag: "Materialized";
    }
  : {
      readonly _tag: "Leased";
      readonly route: Route;
    };

export type SourceUpsert<Row extends object> = {
  readonly _tag: "Upsert";
  readonly row: Row;
  readonly [SourceMutationTypeId]: (row: Row) => SourceUpsert<Row>;
};

export type SourceDelete = {
  readonly _tag: "Delete";
  readonly id: string;
  readonly [SourceMutationTypeId]: () => SourceDelete;
};

export type SourceMutation<Row extends object> = SourceUpsert<Row> | SourceDelete;

export type SourceApplicationExit = Exit.Exit<void, SourceRuntimeFailure>;

export type SourceApplicationTransition<Topic extends string = string> = {
  readonly _tag: "SourceApplicationTransition";
  readonly topic: Topic;
  readonly [SourceApplicationTransitionTypeId]: () => SourceApplicationTransition<Topic>;
};

type SourceApplicationTransitionInternal = {
  readonly topic: string;
  readonly apply: () => void;
  readonly cancelledMaintenanceWorkIds: ReadonlyArray<string>;
  readonly lifetimeIdentity: object;
};

export type SourceMaintenanceOperation<Topic extends string = string> = {
  readonly _tag: "SourceMaintenanceOperation";
  readonly topic: Topic;
  readonly id: string;
  readonly workId: string;
  readonly [SourceMaintenanceOperationTypeId]: () => SourceMaintenanceOperation<Topic>;
};

type SourceMaintenanceOperationInternal = {
  readonly topic: string;
  readonly id: string;
  readonly workId: string;
  readonly lifetimeIdentity: object;
  readonly isCurrent: () => boolean;
  readonly onSuccess: () => void;
  readonly onFailure: (exit: SourceApplicationExit) => void;
  readonly onStale: () => void;
};

export type SourceMaintenanceResult =
  | {
      readonly _tag: "Applied";
      readonly exit: SourceApplicationExit;
    }
  | {
      readonly _tag: "Stale";
    }
  | {
      readonly _tag: "Inactive";
    };

export type SourceApplicationStateForLifetime = (
  lifetimeScope: Scope.Scope,
  topic: string,
) => unknown;

export type SourceApplicationStateRegistration<
  ForLifetime extends SourceApplicationStateForLifetime = SourceApplicationStateForLifetime,
> = {
  readonly _tag: "SourceApplicationStateRegistration";
  readonly forLifetime: ForLifetime;
  readonly [SourceApplicationStateRegistrationTypeId]: () => SourceApplicationStateRegistration<ForLifetime>;
};

export type SourceApplicationStateRegistrationBindingInput = {
  readonly topic: string;
  readonly definition: unknown;
  readonly lifetimeScope: Scope.Scope;
  readonly target: SourceTarget<Readonly<Record<string, unknown>>>;
};

type SourceApplicationStateRegistrationInternal = {
  readonly bind: (input: SourceApplicationStateRegistrationBindingInput) => void;
  readonly lifetimeIdentity: (lifetimeScope: Scope.Scope) => object;
  readonly unbind: (lifetimeScope: Scope.Scope) => void;
  readonly sweepIntervalNanos: bigint;
  readonly runDueSweep: (
    lifetimeScope: Scope.Scope,
    epochNowNanos: bigint,
    execute: (operation: SourceMaintenanceOperation) => Effect.Effect<SourceMaintenanceResult>,
  ) => Effect.Effect<unknown>;
};

export type SourceSettlement<AdapterFailure, Services = never> = (
  exit: SourceApplicationExit,
) => Effect.Effect<void, AdapterFailure, Services>;

export type SourceDelivery<
  Row extends object,
  AdapterFailure,
  SettlementServices = never,
  Topic extends string = string,
> = {
  readonly _tag: "SourceDelivery";
  readonly mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>;
  readonly settle: SourceSettlement<AdapterFailure, SettlementServices>;
  readonly transition?: SourceApplicationTransition<Topic>;
  readonly [SourceDeliveryTypeId]: () => SourceDelivery<
    Row,
    AdapterFailure,
    SettlementServices,
    Topic
  >;
};

export type SourceItemRejectionDiagnostic<AdapterFailure, RejectionLocation> = {
  readonly failure: SourceExecutionFailure<AdapterFailure>;
  readonly location: RejectionLocation;
  readonly rejectedAtNanos: bigint;
};

export type SourceItemRejection<AdapterFailure, RejectionLocation, SettlementServices = never> = {
  readonly _tag: "SourceItemRejection";
  readonly diagnostic: SourceItemRejectionDiagnostic<AdapterFailure, RejectionLocation>;
  readonly settle: SourceSettlement<AdapterFailure, SettlementServices>;
  readonly [SourceItemRejectionTypeId]: () => SourceItemRejection<
    AdapterFailure,
    RejectionLocation,
    SettlementServices
  >;
};

export type SourceLaneEvent<
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  SettlementServices = never,
  Topic extends string = string,
> =
  | SourceDelivery<Row, AdapterFailure, SettlementServices, Topic>
  | SourceItemRejection<AdapterFailure, RejectionLocation, SettlementServices>;

export type SourceBufferMetrics =
  | {
      readonly _tag: "Unbuffered";
    }
  | {
      readonly _tag: "Bounded";
      readonly capacity: number;
      readonly depth: number;
      readonly highWaterMark: number;
      readonly overflowCount: bigint;
    };

export type SourceDeliveryLane<
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services = never,
  Topic extends string = string,
> = {
  readonly id: string;
  readonly events: Stream.Stream<
    SourceLaneEvent<Row, AdapterFailure, RejectionLocation, Services, Topic>,
    SourceExecutionFailure<AdapterFailure>,
    Services
  >;
  readonly bufferMetrics: Effect.Effect<SourceBufferMetrics>;
};

export type SourceAttempt<
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services = never,
  Topic extends string = string,
> = {
  readonly lanes: readonly [
    SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services, Topic>,
    ...ReadonlyArray<SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services, Topic>>,
  ];
  readonly [SourceAttemptTypeId]: () => SourceAttempt<
    Row,
    AdapterFailure,
    RejectionLocation,
    Services,
    Topic
  >;
};

export type SourceToolkit<
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  SettlementServices = never,
  Topic extends string = string,
> = {
  readonly topic: Topic;
  readonly upsert: <const Candidate extends Row>(
    row: Candidate & Record<Exclude<keyof Candidate, keyof Row>, never>,
  ) => Effect.Effect<SourceUpsert<Row>, SourceExecutionFailure<AdapterFailure>>;
  readonly decodeUpsert: (
    row: unknown,
  ) => Effect.Effect<SourceUpsert<Row>, SourceExecutionFailure<AdapterFailure>>;
  readonly delete: (
    id: string,
  ) => Effect.Effect<SourceDelete, SourceExecutionFailure<AdapterFailure>>;
  readonly delivery: {
    (
      mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>,
      settlement?: SourceSettlement<AdapterFailure, SettlementServices>,
    ): Effect.Effect<
      SourceDelivery<Row, AdapterFailure, SettlementServices, Topic>,
      SourceExecutionFailure<AdapterFailure>
    >;
    (
      mutation: SourceMutation<Row>,
      settlement: SourceSettlement<AdapterFailure, SettlementServices> | undefined,
      transition: SourceApplicationTransition<Topic>,
    ): Effect.Effect<
      SourceDelivery<Row, AdapterFailure, SettlementServices, Topic>,
      SourceExecutionFailure<AdapterFailure>
    >;
  };
  readonly reject: (input: {
    readonly failure: SourceExecutionFailure<AdapterFailure>;
    readonly location: RejectionLocation;
    readonly rejectedAtNanos: bigint;
    readonly settlement?: SourceSettlement<AdapterFailure, SettlementServices>;
  }) => Effect.Effect<
    SourceItemRejection<AdapterFailure, RejectionLocation, SettlementServices>,
    SourceExecutionFailure<AdapterFailure>
  >;
  readonly [SourceToolkitTypeId]: () => SourceToolkit<
    Row,
    AdapterFailure,
    RejectionLocation,
    SettlementServices,
    Topic
  >;
};

type SourceToolkitInternal<
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  SettlementServices,
  Topic extends string,
> = SourceToolkit<Row, AdapterFailure, RejectionLocation, SettlementServices, Topic> & {
  readonly [SourceToolkitDecodeTypeId]: (
    row: unknown,
  ) => Effect.Effect<SourceUpsert<Row>, SourceExecutionFailure<AdapterFailure>>;
};

export interface SourceAdapterServiceIdentifier<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
> {
  readonly _SourceAdapterService: {
    readonly name: Name;
    readonly version: Version;
    readonly failure: AdapterFailure;
  };
}

export type SourceLifecycleFactoryInput<
  Options,
  Lifecycle extends SourceLifecycle,
  Route extends Readonly<Record<string, unknown>>,
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services = never,
  Topic extends string = string,
> = {
  readonly definition: Options;
  readonly lifetimeScope: Scope.Scope;
  readonly target: SourceTargetForLifecycle<Lifecycle, Route>;
  readonly toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic>;
};

export type SourceLifecycleMetricsInput<
  Options,
  Lifecycle extends SourceLifecycle,
  Route extends Readonly<Record<string, unknown>>,
  Topic extends string = string,
> = {
  readonly topic: Topic;
  readonly definition: Options;
  readonly lifetimeScope: Scope.Scope;
  readonly target: SourceTargetForLifecycle<Lifecycle, Route>;
};

export type SourceRuntimeLifecycle<
  AdapterFailure,
  Lifecycle extends SourceLifecycle,
  Declaration extends SourceLifecycleDeclarationAny,
  Metrics,
  RejectionLocation,
> = {
  readonly applicationState?: SourceApplicationStateRegistration;
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
      RejectionLocation,
      never,
      Topic
    >,
  ) => Effect.Effect<
    SourceAttempt<Row, AdapterFailure, RejectionLocation>,
    SourceExecutionFailure<AdapterFailure>,
    Scope.Scope
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
  ) => Effect.Effect<Metrics>;
  readonly retryDefault: <A>(
    effect: Effect.Effect<A, SourceTermination<AdapterFailure>>,
    onRetry: (
      metadata: Schedule.Metadata<unknown, SourceTermination<AdapterFailure>>,
    ) => Effect.Effect<void, SourceTermination<AdapterFailure>>,
  ) => Effect.Effect<A, SourceTermination<AdapterFailure>>;
};

export interface SourceAdapterDescriptor<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
> {
  readonly identity: SourceAdapterIdentity<Name, Version>;
  readonly failureSchema: Schema.Codec<AdapterFailure, unknown, never, never>;
  readonly materialized: Materialized;
  readonly leased: Leased;
  readonly runtimeService: Context.Service<
    SourceAdapterServiceIdentifier<Name, Version, AdapterFailure>,
    SourceAdapterRuntimeService<AdapterFailure, Materialized, Leased, Name, Version>
  >;
  readonly failure: (
    failure: AdapterFailure,
  ) => Effect.Effect<SourceExecutionFailure<AdapterFailure>, SourceRuntimeFailure>;
  readonly [SourceAdapterTypeId]: typeof SourceAdapterTypeId;
}

export type SourceAdapterRuntimeService<
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
  Name extends string = string,
  Version extends string | undefined = string | undefined,
> = {
  readonly adapter: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>;
  readonly materialized:
    | SourceRuntimeLifecycle<
        AdapterFailure,
        "materialized",
        Exclude<Materialized, undefined>,
        SourceLifecycleMetrics<Exclude<Materialized, undefined>>,
        SourceLifecycleLocation<Exclude<Materialized, undefined>>
      >
    | undefined;
  readonly leased:
    | SourceRuntimeLifecycle<
        AdapterFailure,
        "leased",
        Exclude<Leased, undefined>,
        SourceLifecycleMetrics<Exclude<Leased, undefined>>,
        SourceLifecycleLocation<Exclude<Leased, undefined>>
      >
    | undefined;
};

type SourceDefinitionFactoryResult<Definition, Row> = Definition & {
  readonly [SourceDefinitionFactoryRowTypeId]?: (_row: Row) => Row;
};

export interface SourceAdapterHandle<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
> extends SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased> {
  readonly materializedSource: <
    Row extends object = object,
    const Options extends SourceLifecycleOptions<Materialized, Row> = SourceLifecycleOptions<
      Materialized,
      Row
    >,
    RetryServices = never,
  >(
    options: ExactDefinitionOptions<Options, SourceLifecycleOptions<Materialized, Row>> &
      RejectAnyOrUnknown<SourceLifecycleOptions<Materialized, Row>>,
    retryPolicy?: SourceRetryPolicy<AdapterFailure, RetryServices>,
  ) => SourceDefinitionFactoryResult<
    SourceDefinition<
      SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
      "materialized",
      Options,
      readonly [],
      RetryServices,
      Row
    >,
    Row
  >;
  readonly leasedSource: <
    const RouteFields extends readonly [string, ...ReadonlyArray<string>],
    Row extends object = object,
    const Options extends SourceLifecycleOptions<Leased, Row> = SourceLifecycleOptions<Leased, Row>,
    RetryServices = never,
  >(
    routeBy: RouteFields,
    options: ExactDefinitionOptions<Options, SourceLifecycleOptions<Leased, Row>> &
      RejectAnyOrUnknown<SourceLifecycleOptions<Leased, Row>>,
    retryPolicy?: SourceRetryPolicy<AdapterFailure, RetryServices>,
  ) => SourceDefinitionFactoryResult<
    SourceDefinition<
      SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
      "leased",
      Options,
      RouteFields,
      RetryServices,
      Row
    >,
    Row
  >;
}

export type SourceLifecycleMetrics<Declaration> =
  Declaration extends SourceLifecycleDeclaration<
    infer Metrics,
    unknown,
    unknown,
    SourceDefinitionOptionsFamily
  >
    ? Metrics
    : never;

export type SourceLifecycleLocation<Declaration> =
  Declaration extends SourceLifecycleDeclaration<
    unknown,
    infer Location,
    unknown,
    SourceDefinitionOptionsFamily
  >
    ? Location
    : never;

export type SourceLifecycleOptions<Declaration, Row extends object = object> =
  Declaration extends SourceLifecycleDeclaration<unknown, unknown, unknown, infer DefinitionFamily>
    ? SourceDefinitionOptionsFor<DefinitionFamily, Row>
    : never;

export interface SourceDefinition<
  Adapter,
  Lifecycle extends SourceLifecycle,
  Options,
  RouteFields extends ReadonlyArray<string>,
  RetryServices = never,
  Row extends object = object,
  AdapterFailure = SourceAdapterFailure<Adapter>,
  AdapterMetrics = SourceLifecycleMetrics<SourceAdapterLifecycleDeclaration<Adapter, Lifecycle>>,
  RejectionLocation = SourceLifecycleLocation<
    SourceAdapterLifecycleDeclaration<Adapter, Lifecycle>
  >,
  LaneId extends string = string,
> {
  readonly adapter: Adapter;
  readonly identity: Adapter extends { readonly identity: infer Identity } ? Identity : never;
  readonly lifecycle: Lifecycle;
  readonly options: Options;
  readonly routeBy: RouteFields;
  readonly retry: SourceRetrySelection<AdapterFailure, RetryServices>;
  readonly [SourceDefinitionTypeId]: () => SourceDefinition<
    Adapter,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row,
    AdapterFailure,
    AdapterMetrics,
    RejectionLocation,
    LaneId
  >;
  readonly [SourceDefinitionTypesTypeId]: {
    readonly adapter: Adapter;
    readonly lifecycle: Lifecycle;
    readonly options: Options;
    readonly retryServices?: RetryServices;
    readonly routeFields: RouteFields;
    readonly row?: Row;
    readonly diagnostics?: {
      readonly adapterFailure: AdapterFailure;
      readonly adapterMetrics: AdapterMetrics;
      readonly rejectionLocation: RejectionLocation;
      readonly laneId: LaneId;
    };
  };
}

export type SourceDefinitionAny = {
  readonly adapter: object;
  readonly identity: {
    readonly name: string;
    readonly version?: string | undefined;
  };
  readonly lifecycle: SourceLifecycle;
  readonly options: unknown;
  readonly routeBy: ReadonlyArray<string>;
  readonly retry: object;
  readonly [SourceDefinitionTypeId]: () => unknown;
  readonly [SourceDefinitionTypesTypeId]: object;
};

export type SourceAdapterFailure<Adapter> = Adapter extends {
  readonly failureSchema: Schema.Codec<infer AdapterFailure, unknown, never, never>;
}
  ? AdapterFailure
  : never;

type SourceAdapterLifecycleDeclaration<
  Adapter,
  Lifecycle extends SourceLifecycle,
> = Lifecycle extends "materialized"
  ? Adapter extends { readonly materialized: infer Declaration }
    ? Exclude<Declaration, undefined>
    : never
  : Adapter extends { readonly leased: infer Declaration }
    ? Exclude<Declaration, undefined>
    : never;

type SourceDefinitionDiagnostics<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly diagnostics?: infer Diagnostics;
  };
}
  ? Exclude<Diagnostics, undefined>
  : never;

export type SourceDefinitionAdapterFailure<Definition> =
  SourceDefinitionDiagnostics<Definition> extends {
    readonly adapterFailure: infer AdapterFailure;
  }
    ? AdapterFailure
    : SourceAdapterFailure<SourceDefinitionAdapter<Definition>>;

export type SourceDefinitionAdapterMetrics<Definition> =
  SourceDefinitionDiagnostics<Definition> extends {
    readonly adapterMetrics: infer AdapterMetrics;
  }
    ? AdapterMetrics
    : SourceLifecycleMetrics<
        SourceAdapterLifecycleDeclaration<
          SourceDefinitionAdapter<Definition>,
          Extract<SourceDefinitionLifecycle<Definition>, SourceLifecycle>
        >
      >;

export type SourceDefinitionRejectionLocation<Definition> =
  SourceDefinitionDiagnostics<Definition> extends {
    readonly rejectionLocation: infer RejectionLocation;
  }
    ? RejectionLocation
    : SourceLifecycleLocation<
        SourceAdapterLifecycleDeclaration<
          SourceDefinitionAdapter<Definition>,
          Extract<SourceDefinitionLifecycle<Definition>, SourceLifecycle>
        >
      >;

export type SourceDefinitionLaneId<Definition> =
  SourceDefinitionDiagnostics<Definition> extends {
    readonly laneId: infer LaneId extends string;
  }
    ? LaneId
    : string;

export type SourceDefinitionAdapter<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly adapter: infer Adapter;
  };
}
  ? Adapter
  : never;

export type SourceDefinitionLifecycle<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly lifecycle: infer Lifecycle;
  };
}
  ? Lifecycle
  : never;

export type SourceDefinitionOptions<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly options: infer Options;
  };
}
  ? Options
  : never;

export type SourceDefinitionRow<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly row?: infer Row extends object;
  };
}
  ? IsAny<Row> extends true
    ? never
    : IsUnknown<Row> extends true
      ? never
      : Row
  : never;

export type SourceDefinitionRouteFields<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly routeFields: infer RouteFields;
  };
}
  ? RouteFields extends ReadonlyArray<string>
    ? RouteFields
    : never
  : never;

export type SourceDefinitionRetryServices<Definition> = Definition extends {
  readonly [SourceDefinitionTypesTypeId]: {
    readonly retryServices?: infer RetryServices;
  };
}
  ? Exclude<RetryServices, undefined>
  : never;

let nextAdapterServiceId = 0;

type MakeSourceAdapterInput<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
> = {
  readonly identity: {
    readonly name: Name;
    readonly version?: Version;
  };
  readonly failure: Schema.Codec<AdapterFailure, unknown, never, never>;
  readonly materialized: Materialized;
  readonly leased: Leased;
};

type AtLeastOneLifecycle<
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
> = MakeSourceAdapterInput<Name, Version, AdapterFailure, Materialized, Leased> &
  (
    | {
        readonly materialized: Exclude<Materialized, undefined>;
      }
    | {
        readonly leased: Exclude<Leased, undefined>;
      }
  );

type ExactLifecycleDeclaration<Declaration> = [Declaration] extends [undefined]
  ? unknown
  : Exclude<Declaration, undefined> extends infer Defined
    ? Defined extends SourceLifecycleDeclarationAny
      ? Exclude<keyof Defined, keyof SourceLifecycleDeclarationAny> extends never
        ? unknown
        : never
      : unknown
    : unknown;

type ExactDefinitionOptions<Candidate, Expected> = Candidate &
  Expected &
  (Candidate extends object
    ? {
        readonly [Key in Exclude<keyof Candidate, keyof Expected>]: never;
      }
    : unknown);

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type IsUnknown<Value> = IsAny<Value> extends true ? false : unknown extends Value ? true : false;

type RejectAnyOrUnknown<Value> =
  IsAny<Value> extends true ? never : IsUnknown<Value> extends true ? never : unknown;

type RejectUnsafeLifecycleDeclaration<Declaration> = [Exclude<Declaration, undefined>] extends [
  never,
]
  ? unknown
  : Exclude<Declaration, undefined> extends SourceLifecycleDeclaration<
        infer Metrics,
        infer RejectionLocation,
        unknown,
        SourceDefinitionOptionsFamily
      >
    ? RejectAnyOrUnknown<Metrics> & RejectAnyOrUnknown<RejectionLocation>
    : unknown;

const sourceExecutable = <Value extends object>(value: Value): Value => {
  if (hasSelfBrand(value, SourceExecutableTypeId)) {
    return value;
  }
  if (
    (typeof value !== "object" && typeof value !== "function") ||
    value === null ||
    !Object.isExtensible(value)
  ) {
    throw new TypeError("Source Adapter executable values must be extensible objects.");
  }
  Object.defineProperty(value, SourceExecutableTypeId, {
    enumerable: false,
    value: () => value,
  });
  return Object.freeze(value);
};

const hasExactEnumerableDataKeys = (
  value: object,
  expectedKeys: ReadonlyArray<string>,
): boolean => {
  const keys = Result.try(() => Reflect.ownKeys(value));
  if (
    Result.isFailure(keys) ||
    keys.success.length !== expectedKeys.length ||
    keys.success.some((key) => typeof key !== "string" || !expectedKeys.includes(key))
  ) {
    return false;
  }
  return expectedKeys.every((key) => {
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

const validateLifecycleDeclaration = <Declaration extends SourceLifecycleDeclarationAny>(
  declaration: Declaration,
): Declaration => {
  const applicationState = Reflect.get(declaration, "applicationState");
  const expectedKeys =
    applicationState === "required"
      ? ["metrics", "rejectionLocation", "definitionOptions", "applicationState"]
      : ["metrics", "rejectionLocation", "definitionOptions"];
  if (
    !hasExactEnumerableDataKeys(declaration, expectedKeys) ||
    !Schema.isSchema(declaration.metrics) ||
    !Schema.isSchema(declaration.rejectionLocation) ||
    !hasSelfBrand(declaration.definitionOptions, SourceDefinitionOptionsTokenTypeId) ||
    (applicationState !== undefined && applicationState !== "required")
  ) {
    throw new TypeError(
      "Every Source Adapter lifecycle requires exact metrics, rejection-location, definition-options, and optional required Application State declarations.",
    );
  }
  return Object.freeze(declaration);
};

function snapshotValue<Value>(value: Value): Value;
function snapshotValue(value: unknown, active?: WeakSet<object>): unknown;
function snapshotValue(value: unknown, active = new WeakSet<object>()): unknown {
  if (Array.isArray(value)) {
    if (active.has(value)) {
      throw new TypeError("Source Definition options must not contain cycles.");
    }
    active.add(value);
    const snapshot = value.map((entry) => snapshotValue(entry, active));
    active.delete(value);
    return Object.freeze(snapshot);
  }
  if (typeof value === "object" && value !== null) {
    if (
      hasSelfBrand(value, SourceExecutableTypeId) ||
      Schema.isSchema(value) ||
      Effect.isEffect(value) ||
      Schedule.isSchedule(value)
    ) {
      return value;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(
        "Source Definition options must use plain data objects or supported Effect executable values.",
      );
    }
    if (active.has(value)) {
      throw new TypeError("Source Definition options must not contain cycles.");
    }
    active.add(value);
    const snapshot: Record<string, unknown> = {};
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string") {
        throw new TypeError("Source Definition options must use string data fields.");
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
        throw new TypeError("Source Definition options must contain enumerable data properties.");
      }
      Object.defineProperty(snapshot, key, {
        enumerable: true,
        value: snapshotValue(descriptor.value, active),
      });
    }
    active.delete(value);
    return Object.freeze(snapshot);
  }
  return value;
}

const retrySelection = <AdapterFailure, Services>(
  retryPolicy: SourceRetryPolicy<AdapterFailure, Services> | undefined,
): SourceRetrySelection<AdapterFailure, Services> =>
  Object.freeze(
    retryPolicy === undefined
      ? {
          _tag: "UseAdapterDefault",
        }
      : {
          _tag: "Override",
          policy: retryPolicy,
        },
  );

const makeDefinition = <
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
  Lifecycle extends SourceLifecycle,
  Options,
  RouteFields extends ReadonlyArray<string>,
  RetryServices,
  Row extends object,
>(
  adapter: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
  lifecycle: Lifecycle,
  options: Options,
  routeBy: RouteFields,
  retryPolicy: SourceRetryPolicy<AdapterFailure, RetryServices> | undefined,
): SourceDefinitionFactoryResult<
  SourceDefinition<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row
  >,
  Row
> => {
  const capturedOptions = snapshotValue(options);
  const capturedRouteBy = snapshotValue(routeBy);
  const definition: SourceDefinition<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row
  > = Object.freeze({
    adapter,
    identity: adapter.identity,
    lifecycle,
    options: capturedOptions,
    routeBy: capturedRouteBy,
    retry: retrySelection(retryPolicy),
    [SourceDefinitionTypeId]: () => definition,
    [SourceDefinitionTypesTypeId]: Object.freeze({
      adapter,
      lifecycle,
      options: capturedOptions,
      routeFields: capturedRouteBy,
    }),
  });
  return registerSourceDefinition(definition);
};

const makeDescriptorDefinition = <
  Name extends string,
  Version extends string | undefined,
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
  Lifecycle extends SourceLifecycle,
  Options,
  RouteFields extends ReadonlyArray<string>,
  RetryServices,
  Row extends object,
>(
  adapter: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
  lifecycle: Lifecycle,
  options: Options,
  routeBy: RouteFields,
  retryPolicy: SourceRetryPolicy<AdapterFailure, RetryServices> | undefined,
): SourceDefinitionFactoryResult<
  SourceDefinition<
    SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row
  >,
  Row
> => {
  const capturedOptions = snapshotValue(options);
  const capturedRouteBy = snapshotValue(routeBy);
  const definition: SourceDefinition<
    SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row
  > = Object.freeze({
    adapter,
    identity: adapter.identity,
    lifecycle,
    options: capturedOptions,
    routeBy: capturedRouteBy,
    retry: retrySelection(retryPolicy),
    [SourceDefinitionTypeId]: () => definition,
    [SourceDefinitionTypesTypeId]: Object.freeze({
      adapter,
      lifecycle,
      options: capturedOptions,
      routeFields: capturedRouteBy,
    }),
  });
  return registerSourceDefinition(definition);
};

export const makeMaterializedSourceDefinition = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
  Row extends object = object,
  const Options extends SourceLifecycleOptions<Materialized, Row> = SourceLifecycleOptions<
    Materialized,
    Row
  >,
  RetryServices = never,
>(
  adapter: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
  options: ExactDefinitionOptions<Options, SourceLifecycleOptions<Materialized, Row>> &
    RejectAnyOrUnknown<SourceLifecycleOptions<Materialized, Row>>,
  retryPolicy?: SourceRetryPolicy<AdapterFailure, RetryServices>,
): SourceDefinitionFactoryResult<
  SourceDefinition<
    SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
    "materialized",
    Options,
    readonly [],
    RetryServices,
    Row
  >,
  Row
> => {
  if (sourceAdapterHandles.get(adapter) !== adapter) {
    throw new TypeError(
      "Source Adapter delegated construction requires a nominal Source Adapter handle.",
    );
  }
  if (adapter.materialized === undefined) {
    throw new TypeError("This Source Adapter does not declare a Materialized lifecycle.");
  }
  return makeDescriptorDefinition(
    makeSourceAdapterDescriptor(adapter),
    "materialized",
    options,
    [],
    retryPolicy,
  );
};

const registeredHandleMatchesDescriptor = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny | undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
>(
  candidate: unknown,
  descriptor: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
): candidate is SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased> =>
  typeof candidate === "object" &&
  candidate !== null &&
  sourceAdapterHandles.get(descriptor) === candidate &&
  sourceAdapterHandles.get(candidate) === candidate;

const registeredDescriptorMatchesHandle = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny | undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
>(
  candidate: unknown,
  handle: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
): candidate is SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased> =>
  typeof candidate === "object" &&
  candidate !== null &&
  sourceAdapterHandles.get(candidate) === handle;

export const resolveSourceAdapterHandle = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny | undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
>(
  adapter: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased>,
): SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased> => {
  const handle = sourceAdapterHandles.get(adapter);
  if (!registeredHandleMatchesDescriptor(handle, adapter)) {
    throw new TypeError("Source Adapter descriptor is not linked to a nominal handle.");
  }
  return handle;
};

export const makeSourceAdapterDescriptor = <
  const Name extends string,
  const Version extends string | undefined,
  AdapterFailure,
  const Materialized extends SourceLifecycleDeclarationAny | undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined,
>(
  handle: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
): SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased> => {
  const existing = sourceAdapterDescriptors.get(handle);
  if (registeredDescriptorMatchesHandle(existing, handle)) {
    return existing;
  }
  if (sourceAdapterHandles.get(handle) !== handle) {
    throw new TypeError("Source Adapter descriptor requires a nominal Source Adapter handle.");
  }
  const descriptor: SourceAdapterDescriptor<Name, Version, AdapterFailure, Materialized, Leased> = {
    identity: handle.identity,
    failureSchema: handle.failureSchema,
    materialized: handle.materialized,
    leased: handle.leased,
    runtimeService: handle.runtimeService,
    failure: handle.failure,
    [SourceAdapterTypeId]: SourceAdapterTypeId,
  };
  Object.freeze(descriptor);
  sourceAdapterHandles.set(descriptor, handle);
  sourceAdapterDescriptors.set(handle, descriptor);
  return descriptor;
};

export const makeSourceAdapter = <
  const Name extends string,
  const Version extends string | undefined = undefined,
  AdapterFailure = never,
  const Materialized extends SourceLifecycleDeclarationAny | undefined = undefined,
  const Leased extends SourceLifecycleDeclarationAny | undefined = undefined,
>(
  input: AtLeastOneLifecycle<Name, Version, AdapterFailure, Materialized, Leased> &
    ExactLifecycleDeclaration<Materialized> &
    ExactLifecycleDeclaration<Leased> &
    RejectAnyOrUnknown<AdapterFailure> &
    RejectUnsafeLifecycleDeclaration<Materialized> &
    RejectUnsafeLifecycleDeclaration<Leased>,
): SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased> => {
  if (
    typeof input.identity !== "object" ||
    input.identity === null ||
    !hasExactEnumerableDataKeys(
      input.identity,
      Object.hasOwn(input.identity, "version") ? ["name", "version"] : ["name"],
    )
  ) {
    throw new TypeError(
      "Source Adapter identity must contain exactly name and optional version data fields.",
    );
  }
  if (
    typeof input.identity.name !== "string" ||
    input.identity.name.length === 0 ||
    (Object.hasOwn(input.identity, "version") && input.identity.version === undefined) ||
    (input.identity.version !== undefined &&
      (typeof input.identity.version !== "string" || input.identity.version.length === 0))
  ) {
    throw new TypeError("Source Adapter identity name and version must be non-empty.");
  }
  if (!Schema.isSchema(input.failure)) {
    throw new TypeError("Source Adapter failure must be an Effect Schema.");
  }
  if (input.materialized === undefined && input.leased === undefined) {
    throw new TypeError("Source Adapter must declare Materialized and/or Leased.");
  }
  if (input.materialized !== undefined) {
    validateLifecycleDeclaration(input.materialized);
  }
  if (input.leased !== undefined) {
    validateLifecycleDeclaration(input.leased);
  }
  const materialized = input.materialized;
  const leased = input.leased;
  nextAdapterServiceId += 1;
  const identity: SourceAdapterIdentity<Name, Version> = Object.freeze(
    snapshotValue(input.identity),
  );
  const runtimeService = Context.Service<
    SourceAdapterServiceIdentifier<Name, Version, AdapterFailure>,
    SourceAdapterRuntimeService<AdapterFailure, Materialized, Leased, Name, Version>
  >(`@effect-view-server/source-adapter/${input.identity.name}/${nextAdapterServiceId}`);
  const handle: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased> = {
    identity,
    failureSchema: input.failure,
    materialized,
    leased,
    runtimeService,
    failure: (failure: AdapterFailure) =>
      Schema.decodeUnknownEffect(input.failure)(failure).pipe(
        Effect.map(
          (decoded): SourceExecutionFailure<AdapterFailure> => ({
            _tag: "AdapterFailure",
            failure: decoded,
          }),
        ),
        Effect.mapError(
          (): SourceRuntimeFailure => ({
            _tag: "InvalidSourceDefinition",
            message: "Adapter Failure does not satisfy the declared Source Adapter Schema.",
          }),
        ),
      ),
    materializedSource: (options, retryPolicy) => {
      if (materialized === undefined) {
        throw new TypeError("This Source Adapter does not declare a Materialized lifecycle.");
      }
      return makeDefinition(handle, "materialized", options, [], retryPolicy);
    },
    leasedSource: (routeBy, options, retryPolicy) => {
      if (leased === undefined) {
        throw new TypeError("This Source Adapter does not declare a Leased lifecycle.");
      }
      if (
        routeBy.length === 0 ||
        routeBy.some((field) => field.length === 0) ||
        new Set(routeBy).size !== routeBy.length
      ) {
        throw new TypeError("Leased Source route fields must be non-empty, unique strings.");
      }
      return makeDefinition(handle, "leased", options, routeBy, retryPolicy);
    },
    [SourceAdapterTypeId]: SourceAdapterTypeId,
  };
  Object.freeze(handle);
  sourceAdapterHandles.set(handle, handle);
  makeSourceAdapterDescriptor(handle);
  return handle;
};

export const sourceDefinitionOptions = <Options>(
  ..._unsafe: IsAny<Options> extends true
    ? readonly [never]
    : IsUnknown<Options> extends true
      ? readonly [never]
      : readonly []
): SourceDefinitionOptionsToken<Options> => {
  const token: SourceDefinitionOptionsToken<Options> = {
    [SourceDefinitionOptionsTokenTypeId]: () => token,
  };
  return Object.freeze(token);
};

export const sourceDefinitionOptionsFamily = <
  Family extends SourceDefinitionOptionsFamily,
  Options = SourceDefinitionOptionsFor<Family, object>,
>(
  ..._unsafe: IsAny<Family> extends true
    ? readonly [never]
    : IsUnknown<Family> extends true
      ? readonly [never]
      : IsAny<Options> extends true
        ? readonly [never]
        : IsUnknown<Options> extends true
          ? readonly [never]
          : IsAny<SourceDefinitionOptionsFor<Family, object>> extends true
            ? readonly [never]
            : IsUnknown<SourceDefinitionOptionsFor<Family, object>> extends true
              ? readonly [never]
              : readonly []
): SourceDefinitionOptionsToken<Options, Family> => {
  const token: SourceDefinitionOptionsToken<Options, Family> = {
    [SourceDefinitionOptionsTokenTypeId]: () => token,
  };
  return Object.freeze(token);
};

export const makeSourceUpsert = <Row extends object>(row: Row): SourceUpsert<Row> => {
  const mutation: SourceUpsert<Row> = {
    _tag: "Upsert",
    row,
    [SourceMutationTypeId]: () => mutation,
  };
  Object.freeze(mutation);
  return mutation;
};

export const makeSourceDelete = (id: string): SourceDelete => {
  const mutation: SourceDelete = {
    _tag: "Delete",
    id,
    [SourceMutationTypeId]: () => mutation,
  };
  Object.freeze(mutation);
  return mutation;
};

export const isSourceMutation = (value: unknown): value is SourceMutation<object> => {
  if (!hasSelfBrand(value, SourceMutationTypeId) || typeof value !== "object" || value === null) {
    return false;
  }
  const inspected = Result.try(() => {
    const tag = Reflect.get(value, "_tag");
    if (tag === "Delete") {
      return typeof Reflect.get(value, "id") === "string";
    }
    const row = Reflect.get(value, "row");
    return tag === "Upsert" && typeof row === "object" && row !== null;
  });
  return Result.isSuccess(inspected) && inspected.success;
};

const noSettlement = () => Effect.void;

const snapshotMaintenanceWorkIds = (
  cancelledMaintenanceWorkIds: ReadonlyArray<string>,
): ReadonlyArray<string> => {
  const snapshot = Array.from(cancelledMaintenanceWorkIds);
  if (
    snapshot.some((workId) => typeof workId !== "string" || workId.length === 0) ||
    new Set(snapshot).size !== snapshot.length
  ) {
    throw new TypeError(
      "Source Application Transition cancellation IDs must be unique non-empty strings.",
    );
  }
  return Object.freeze(snapshot);
};

export const makeSourceApplicationTransition = <const Topic extends string>(
  topic: Topic,
  apply: () => void,
  cancelledMaintenanceWorkIds: ReadonlyArray<string>,
  lifetimeIdentity: object,
): SourceApplicationTransition<Topic> => {
  const cancellationSnapshot = snapshotMaintenanceWorkIds(cancelledMaintenanceWorkIds);
  const transition: SourceApplicationTransition<Topic> = {
    _tag: "SourceApplicationTransition",
    topic,
    [SourceApplicationTransitionTypeId]: () => transition,
  };
  sourceApplicationTransitions.set(transition, {
    topic,
    apply,
    cancelledMaintenanceWorkIds: cancellationSnapshot,
    lifetimeIdentity,
  });
  Object.freeze(transition);
  return transition;
};

export const makeSourceApplicationStateRegistration = <
  ForLifetime extends SourceApplicationStateForLifetime,
>(input: {
  readonly bind: (input: SourceApplicationStateRegistrationBindingInput) => void;
  readonly forLifetime: ForLifetime;
  readonly lifetimeIdentity: (lifetimeScope: Scope.Scope) => object;
  readonly unbind: (lifetimeScope: Scope.Scope) => void;
  readonly sweepIntervalNanos: bigint;
  readonly runDueSweep: (
    lifetimeScope: Scope.Scope,
    epochNowNanos: bigint,
    execute: (operation: SourceMaintenanceOperation) => Effect.Effect<SourceMaintenanceResult>,
  ) => Effect.Effect<unknown>;
}): SourceApplicationStateRegistration<ForLifetime> => {
  if (
    !hasExactEnumerableDataKeys(input, [
      "bind",
      "forLifetime",
      "lifetimeIdentity",
      "unbind",
      "sweepIntervalNanos",
      "runDueSweep",
    ]) ||
    !hasInspectableSynchronousFunction(input.bind) ||
    !hasInspectableSynchronousFunction(input.forLifetime) ||
    !hasInspectableSynchronousFunction(input.lifetimeIdentity) ||
    !hasInspectableSynchronousFunction(input.unbind) ||
    typeof input.sweepIntervalNanos !== "bigint" ||
    input.sweepIntervalNanos <= 0n ||
    !hasInspectableSynchronousFunction(input.runDueSweep)
  ) {
    throw new TypeError(
      "Source Application State registration requires exact synchronous lifecycle capabilities.",
    );
  }
  const snapshot = Object.freeze({
    bind: input.bind,
    forLifetime: input.forLifetime,
    lifetimeIdentity: input.lifetimeIdentity,
    unbind: input.unbind,
    sweepIntervalNanos: input.sweepIntervalNanos,
    runDueSweep: input.runDueSweep,
  });
  const registration: SourceApplicationStateRegistration<ForLifetime> = {
    _tag: "SourceApplicationStateRegistration",
    forLifetime: snapshot.forLifetime,
    [SourceApplicationStateRegistrationTypeId]: () => registration,
  };
  sourceApplicationStateRegistrations.set(registration, snapshot);
  Object.freeze(registration);
  return registration;
};

export const resolveSourceApplicationStateRegistration = (
  registration: SourceApplicationStateRegistration,
): SourceApplicationStateRegistrationInternal | undefined =>
  sourceApplicationStateRegistrations.get(registration);

export const isSourceApplicationStateRegistration = (
  value: unknown,
): value is SourceApplicationStateRegistration =>
  typeof value === "object" &&
  value !== null &&
  hasSelfBrand(value, SourceApplicationStateRegistrationTypeId) &&
  sourceApplicationStateRegistrations.has(value);

export const resolveSourceApplicationTransition = (
  transition: SourceApplicationTransition,
): SourceApplicationTransitionInternal | undefined => sourceApplicationTransitions.get(transition);

export const isSourceApplicationTransition = (
  value: unknown,
): value is SourceApplicationTransition =>
  typeof value === "object" &&
  value !== null &&
  hasSelfBrand(value, SourceApplicationTransitionTypeId) &&
  sourceApplicationTransitions.has(value);

export const makeSourceMaintenanceOperation = <const Topic extends string>(input: {
  readonly topic: Topic;
  readonly id: string;
  readonly workId: string;
  readonly lifetimeIdentity: object;
  readonly isCurrent: () => boolean;
  readonly onSuccess: () => void;
  readonly onFailure: (exit: SourceApplicationExit) => void;
  readonly onStale: () => void;
}): SourceMaintenanceOperation<Topic> => {
  const operation: SourceMaintenanceOperation<Topic> = {
    _tag: "SourceMaintenanceOperation",
    topic: input.topic,
    id: input.id,
    workId: input.workId,
    [SourceMaintenanceOperationTypeId]: () => operation,
  };
  sourceMaintenanceOperations.set(operation, {
    topic: input.topic,
    id: input.id,
    workId: input.workId,
    lifetimeIdentity: input.lifetimeIdentity,
    isCurrent: input.isCurrent,
    onSuccess: input.onSuccess,
    onFailure: input.onFailure,
    onStale: input.onStale,
  });
  Object.freeze(operation);
  return operation;
};

export const resolveSourceMaintenanceOperation = (
  operation: SourceMaintenanceOperation,
): SourceMaintenanceOperationInternal | undefined => sourceMaintenanceOperations.get(operation);

export const isSourceMaintenanceOperation = (value: unknown): value is SourceMaintenanceOperation =>
  typeof value === "object" &&
  value !== null &&
  hasSelfBrand(value, SourceMaintenanceOperationTypeId) &&
  sourceMaintenanceOperations.has(value);

export const makeSourceDelivery = <
  Row extends object,
  AdapterFailure,
  Services = never,
  Topic extends string = string,
>(
  mutationsChunk: Chunk.NonEmptyChunk<SourceMutation<Row>>,
  settlement?: SourceSettlement<AdapterFailure, Services>,
): SourceDelivery<Row, AdapterFailure, Services, Topic> => {
  const delivery: SourceDelivery<Row, AdapterFailure, Services, Topic> = {
    _tag: "SourceDelivery",
    mutations: mutationsChunk,
    settle: settlement ?? noSettlement,
    [SourceDeliveryTypeId]: () => delivery,
  };
  Object.freeze(delivery);
  return delivery;
};

export const makeSourceTransitionDelivery = <
  Row extends object,
  AdapterFailure,
  Services = never,
  const Topic extends string = string,
>(
  mutation: SourceMutation<Row>,
  settlement: SourceSettlement<AdapterFailure, Services> | undefined,
  transition: SourceApplicationTransition<Topic>,
): SourceDelivery<Row, AdapterFailure, Services, Topic> => {
  const delivery: SourceDelivery<Row, AdapterFailure, Services, Topic> = {
    _tag: "SourceDelivery",
    mutations: Chunk.of(mutation),
    settle: settlement ?? noSettlement,
    transition,
    [SourceDeliveryTypeId]: () => delivery,
  };
  Object.freeze(delivery);
  return delivery;
};

export function isSourceDelivery<
  Row extends object,
  AdapterFailure,
  SettlementServices,
  Topic extends string,
>(
  value: SourceDelivery<Row, AdapterFailure, SettlementServices, Topic>,
): value is SourceDelivery<Row, AdapterFailure, SettlementServices, Topic>;
export function isSourceDelivery(value: unknown): value is SourceDelivery<object, unknown, unknown>;
export function isSourceDelivery(
  value: unknown,
): value is SourceDelivery<object, unknown, unknown> {
  if (!hasSelfBrand(value, SourceDeliveryTypeId) || typeof value !== "object" || value === null) {
    return false;
  }
  const inspected = Result.try(() => {
    const mutations = Reflect.get(value, "mutations");
    return (
      Reflect.get(value, "_tag") === "SourceDelivery" &&
      Chunk.isChunk(mutations) &&
      Chunk.isNonEmpty(mutations) &&
      Chunk.every(mutations, isSourceMutation) &&
      typeof Reflect.get(value, "settle") === "function" &&
      (!Reflect.has(value, "transition") ||
        isSourceApplicationTransition(Reflect.get(value, "transition")))
    );
  });
  return Result.isSuccess(inspected) && inspected.success;
}

export const makeSourceItemRejection = <
  AdapterFailure,
  RejectionLocation,
  Services = never,
>(input: {
  readonly failure: SourceExecutionFailure<AdapterFailure>;
  readonly location: RejectionLocation;
  readonly rejectedAtNanos: bigint;
  readonly settlement?: SourceSettlement<AdapterFailure, Services>;
}): SourceItemRejection<AdapterFailure, RejectionLocation, Services> => {
  const rejection: SourceItemRejection<AdapterFailure, RejectionLocation, Services> = {
    _tag: "SourceItemRejection",
    diagnostic: Object.freeze({
      failure: input.failure,
      location: input.location,
      rejectedAtNanos: input.rejectedAtNanos,
    }),
    settle: input.settlement ?? noSettlement,
    [SourceItemRejectionTypeId]: () => rejection,
  };
  Object.freeze(rejection);
  return rejection;
};

export const isSourceItemRejection = (
  value: unknown,
): value is SourceItemRejection<unknown, unknown, unknown> => {
  if (
    !hasSelfBrand(value, SourceItemRejectionTypeId) ||
    typeof value !== "object" ||
    value === null
  ) {
    return false;
  }
  const inspected = Result.try(() => {
    const diagnostic = Reflect.get(value, "diagnostic");
    return (
      Reflect.get(value, "_tag") === "SourceItemRejection" &&
      typeof diagnostic === "object" &&
      diagnostic !== null &&
      Reflect.has(diagnostic, "failure") &&
      Reflect.has(diagnostic, "location") &&
      typeof Reflect.get(diagnostic, "rejectedAtNanos") === "bigint" &&
      typeof Reflect.get(value, "settle") === "function"
    );
  });
  return Result.isSuccess(inspected) && inspected.success;
};

export const markSourceToolkit = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services,
  const Topic extends string,
>(
  toolkit: Omit<
    SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic>,
    typeof SourceToolkitTypeId
  >,
): SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic> => {
  const nominal: SourceToolkitInternal<Row, AdapterFailure, RejectionLocation, Services, Topic> = {
    ...toolkit,
    decodeUpsert: toolkit.decodeUpsert,
    [SourceToolkitDecodeTypeId]: toolkit.decodeUpsert,
    [SourceToolkitTypeId]: () => nominal,
  };
  Object.freeze(nominal);
  return nominal;
};

export const decodeSourceToolkitUpsert = <
  Row extends object,
  AdapterFailure,
  RejectionLocation,
  Services,
  Topic extends string,
>(
  toolkit: SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic>,
  row: unknown,
): Effect.Effect<SourceUpsert<Row>, SourceExecutionFailure<AdapterFailure>> => {
  const decoder = Reflect.get(toolkit, SourceToolkitDecodeTypeId);
  return Reflect.apply(decoder, toolkit, [row]);
};

export const isSourceToolkit = (value: unknown): boolean =>
  hasSelfBrand(value, SourceToolkitTypeId);

export const makeSourceAttempt = <Row extends object, AdapterFailure, RejectionLocation, Services>(
  lanes: readonly [
    SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services>,
    ...ReadonlyArray<SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services>>,
  ],
): SourceAttempt<Row, AdapterFailure, RejectionLocation, Services> => {
  const laneIds = lanes.map((lane) => lane.id);
  if (
    lanes.length === 0 ||
    laneIds.some((laneId) => laneId.length === 0) ||
    new Set(laneIds).size !== laneIds.length
  ) {
    throw new TypeError("Source Attempt lane IDs must be non-empty and unique.");
  }
  const [first, ...rest] = lanes;
  const attempt: SourceAttempt<Row, AdapterFailure, RejectionLocation, Services> = {
    lanes: Object.freeze([first, ...rest]),
    [SourceAttemptTypeId]: () => attempt,
  };
  Object.freeze(attempt);
  return attempt;
};

export const isSourceAttempt = (value: unknown): boolean =>
  hasSelfBrand(value, SourceAttemptTypeId);

export const makeRuntimeSourceFailure = (
  failure: SourceRuntimeFailure,
): SourceExecutionFailure<never> => ({
  _tag: "RuntimeFailure",
  failure,
});

export const SourceAdapter = {
  descriptor: makeSourceAdapterDescriptor,
  definitionOptions: sourceDefinitionOptions,
  definitionOptionsFamily: sourceDefinitionOptionsFamily,
  executable: sourceExecutable,
  materializedSource: makeMaterializedSourceDefinition,
  make: makeSourceAdapter,
} as const;

export const sourceModelInternals = {
  isSourceAdapterHandle,
  isSourceAttempt,
  isSourceDefinition,
  isSourceDelivery,
  isSourceItemRejection,
  isSourceMutation,
  isSourceToolkit,
  decodeSourceToolkitUpsert,
  makeSourceAttempt,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceUpsert,
  markSourceToolkit,
} as const;

export { isSourceAdapterHandle, isSourceDefinition, validateSourceDefinition };
