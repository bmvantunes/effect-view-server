import { Chunk, Context, Effect, Exit, Result, Schedule, Schema, Scope, Stream } from "effect";

const SourceAdapterTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceAdapter",
);
const SourceDefinitionTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDefinition",
);
const SourceDefinitionTypesTypeId: unique symbol = Symbol(
  "@effect-view-server/source-adapter/SourceDefinitionTypes",
);
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

export type SourceSettlement<AdapterFailure, Services = never> = (
  exit: SourceApplicationExit,
) => Effect.Effect<void, AdapterFailure, Services>;

export type SourceDelivery<Row extends object, AdapterFailure, SettlementServices = never> = {
  readonly _tag: "SourceDelivery";
  readonly mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>;
  readonly settle: SourceSettlement<AdapterFailure, SettlementServices>;
  readonly [SourceDeliveryTypeId]: () => SourceDelivery<Row, AdapterFailure, SettlementServices>;
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
> =
  | SourceDelivery<Row, AdapterFailure, SettlementServices>
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
> = {
  readonly id: string;
  readonly events: Stream.Stream<
    SourceLaneEvent<Row, AdapterFailure, RejectionLocation, Services>,
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
> = {
  readonly lanes: readonly [
    SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services>,
    ...ReadonlyArray<SourceDeliveryLane<Row, AdapterFailure, RejectionLocation, Services>>,
  ];
  readonly [SourceAttemptTypeId]: () => SourceAttempt<
    Row,
    AdapterFailure,
    RejectionLocation,
    Services
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
  readonly delete: (
    id: string,
  ) => Effect.Effect<SourceDelete, SourceExecutionFailure<AdapterFailure>>;
  readonly delivery: (
    mutations: Chunk.NonEmptyChunk<SourceMutation<Row>>,
    settlement?: SourceSettlement<AdapterFailure, SettlementServices>,
  ) => Effect.Effect<
    SourceDelivery<Row, AdapterFailure, SettlementServices>,
    SourceExecutionFailure<AdapterFailure>
  >;
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
  readonly target: SourceTargetForLifecycle<Lifecycle, Route>;
};

export type SourceRuntimeLifecycle<
  AdapterFailure,
  Lifecycle extends SourceLifecycle,
  Declaration extends SourceLifecycleDeclarationAny,
  Metrics,
  RejectionLocation,
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

export type SourceAdapterRuntimeService<
  AdapterFailure,
  Materialized extends SourceLifecycleDeclarationAny | undefined,
  Leased extends SourceLifecycleDeclarationAny | undefined,
  Name extends string = string,
  Version extends string | undefined = string | undefined,
> = {
  readonly adapter: SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>;
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

export interface SourceAdapterHandle<
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
  ) => SourceDefinition<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
    "materialized",
    SourceLifecycleOptions<Materialized, Row>,
    readonly [],
    RetryServices,
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
  ) => SourceDefinition<
    SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
    "leased",
    SourceLifecycleOptions<Leased, Row>,
    RouteFields,
    RetryServices,
    Row
  >;
  readonly [SourceAdapterTypeId]: () => SourceAdapterHandle<
    Name,
    Version,
    AdapterFailure,
    Materialized,
    Leased
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
> {
  readonly adapter: Adapter;
  readonly identity: Adapter extends { readonly identity: infer Identity } ? Identity : never;
  readonly lifecycle: Lifecycle;
  readonly options: Options;
  readonly routeBy: RouteFields;
  readonly retry: SourceRetrySelection<SourceAdapterFailure<Adapter>, RetryServices>;
  readonly [SourceDefinitionTypeId]: () => SourceDefinition<
    Adapter,
    Lifecycle,
    Options,
    RouteFields,
    RetryServices,
    Row
  >;
  readonly [SourceDefinitionTypesTypeId]: {
    readonly adapter: Adapter;
    readonly lifecycle: Lifecycle;
    readonly options: Options;
    readonly retryServices?: RetryServices;
    readonly routeFields: RouteFields;
    readonly row?: Row;
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
  ? RetryServices
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

type ExactDefinitionOptions<Candidate, Expected> = Candidate extends Expected
  ? Exclude<keyof Candidate, keyof Expected> extends never
    ? Candidate
    : never
  : never;

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

const hasSelfBrand = (value: unknown, key: symbol): boolean => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const inspected = Result.try(() => Reflect.get(value, key));
  if (Result.isFailure(inspected) || typeof inspected.success !== "function") {
    return false;
  }
  const branded = Result.try(() => Reflect.apply(inspected.success, undefined, []));
  return Result.isSuccess(branded) && branded.success === value;
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

const validateLifecycleDeclaration = <Declaration extends SourceLifecycleDeclarationAny>(
  declaration: Declaration,
): Declaration => {
  if (
    !hasExactEnumerableDataKeys(declaration, [
      "metrics",
      "rejectionLocation",
      "definitionOptions",
    ]) ||
    !Schema.isSchema(declaration.metrics) ||
    !Schema.isSchema(declaration.rejectionLocation) ||
    !hasSelfBrand(declaration.definitionOptions, SourceDefinitionOptionsTokenTypeId)
  ) {
    throw new TypeError(
      "Every Source Adapter lifecycle requires exact metrics, rejection-location, and definition-options declarations.",
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
    if (Schema.isSchema(value) || Effect.isEffect(value) || Schedule.isSchedule(value)) {
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
): SourceDefinition<
  SourceAdapterHandle<Name, Version, AdapterFailure, Materialized, Leased>,
  Lifecycle,
  Options,
  RouteFields,
  RetryServices,
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
  return definition;
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
    [SourceAdapterTypeId]: () => handle,
  };
  Object.freeze(handle);
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

export const isSourceAdapterHandle = (value: unknown): boolean =>
  hasSelfBrand(value, SourceAdapterTypeId);

const hasExactDefinitionDataKeys = (
  value: object,
  expectedKeys: ReadonlyArray<PropertyKey>,
): boolean => {
  const keys = Result.try(() => Reflect.ownKeys(value));
  if (
    Result.isFailure(keys) ||
    keys.success.length !== expectedKeys.length ||
    keys.success.some((key) => !expectedKeys.includes(key))
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

const validateSourceDefinitionEnvelope = (value: unknown): boolean => {
  if (
    typeof value !== "object" ||
    value === null ||
    !hasSelfBrand(value, SourceDefinitionTypeId) ||
    !hasExactDefinitionDataKeys(value, [
      "adapter",
      "identity",
      "lifecycle",
      "options",
      "routeBy",
      "retry",
      SourceDefinitionTypeId,
      SourceDefinitionTypesTypeId,
    ]) ||
    !Object.isFrozen(value)
  ) {
    return false;
  }
  const adapter = Reflect.get(value, "adapter");
  const identity = Reflect.get(value, "identity");
  const lifecycle = Reflect.get(value, "lifecycle");
  const options = Reflect.get(value, "options");
  const routeBy = Reflect.get(value, "routeBy");
  const retry = Reflect.get(value, "retry");
  const types = Reflect.get(value, SourceDefinitionTypesTypeId);
  if (
    typeof adapter !== "object" ||
    adapter === null ||
    !hasSelfBrand(adapter, SourceAdapterTypeId) ||
    !Object.isFrozen(adapter) ||
    Reflect.get(adapter, "identity") !== identity ||
    (lifecycle !== "materialized" && lifecycle !== "leased") ||
    !Array.isArray(routeBy) ||
    !Object.isFrozen(routeBy) ||
    routeBy.some((field) => typeof field !== "string" || field.length === 0) ||
    new Set(routeBy).size !== routeBy.length ||
    (lifecycle === "materialized" ? routeBy.length !== 0 : routeBy.length === 0) ||
    Reflect.get(adapter, lifecycle) === undefined ||
    typeof retry !== "object" ||
    retry === null ||
    !Object.isFrozen(retry) ||
    typeof types !== "object" ||
    types === null ||
    !hasExactDefinitionDataKeys(types, ["adapter", "lifecycle", "options", "routeFields"]) ||
    !Object.isFrozen(types) ||
    Reflect.get(types, "adapter") !== adapter ||
    Reflect.get(types, "lifecycle") !== lifecycle ||
    Reflect.get(types, "options") !== options ||
    Reflect.get(types, "routeFields") !== routeBy
  ) {
    return false;
  }
  const retryTag = Reflect.get(retry, "_tag");
  return retryTag === "UseAdapterDefault"
    ? hasExactDefinitionDataKeys(retry, ["_tag"])
    : retryTag === "Override" &&
        hasExactDefinitionDataKeys(retry, ["_tag", "policy"]) &&
        Schedule.isSchedule(Reflect.get(retry, "policy"));
};

export const validateSourceDefinition = (value: unknown): boolean => {
  const validation = Result.try(() => validateSourceDefinitionEnvelope(value));
  return Result.isSuccess(validation) && validation.success;
};

export const isSourceDefinition = (
  value: unknown,
): value is SourceDefinition<
  SourceAdapterHandle<
    string,
    string | undefined,
    unknown,
    SourceLifecycleDeclarationAny | undefined,
    SourceLifecycleDeclarationAny | undefined
  >,
  SourceLifecycle,
  unknown,
  ReadonlyArray<string>,
  never
> => validateSourceDefinition(value);

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

export const isSourceMutation = (value: unknown): value is SourceMutation<object> =>
  hasSelfBrand(value, SourceMutationTypeId);

const noSettlement = () => Effect.void;

export const makeSourceDelivery = <Row extends object, AdapterFailure, Services = never>(
  mutationsChunk: Chunk.NonEmptyChunk<SourceMutation<Row>>,
  settlement?: SourceSettlement<AdapterFailure, Services>,
): SourceDelivery<Row, AdapterFailure, Services> => {
  const delivery: SourceDelivery<Row, AdapterFailure, Services> = {
    _tag: "SourceDelivery",
    mutations: mutationsChunk,
    settle: settlement ?? noSettlement,
    [SourceDeliveryTypeId]: () => delivery,
  };
  Object.freeze(delivery);
  return delivery;
};

export const isSourceDelivery = (
  value: unknown,
): value is SourceDelivery<object, unknown, unknown> => hasSelfBrand(value, SourceDeliveryTypeId);

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
): value is SourceItemRejection<unknown, unknown, unknown> =>
  hasSelfBrand(value, SourceItemRejectionTypeId);

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
  > & {
    readonly decodeUpsert: (
      row: unknown,
    ) => Effect.Effect<SourceUpsert<Row>, SourceExecutionFailure<AdapterFailure>>;
  },
): SourceToolkit<Row, AdapterFailure, RejectionLocation, Services, Topic> => {
  const nominal: SourceToolkitInternal<Row, AdapterFailure, RejectionLocation, Services, Topic> = {
    ...toolkit,
    [SourceToolkitDecodeTypeId]: toolkit.decodeUpsert,
    [SourceToolkitTypeId]: () => nominal,
  };
  Reflect.deleteProperty(nominal, "decodeUpsert");
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
  definitionOptions: sourceDefinitionOptions,
  definitionOptionsFamily: sourceDefinitionOptionsFamily,
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
