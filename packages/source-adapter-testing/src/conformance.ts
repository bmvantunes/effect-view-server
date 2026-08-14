import type {
  SourceApplicationExit,
  SourceDefinitionAny,
  SourceDefinitionRow,
} from "@effect-view-server/source-adapter";
import {
  isSourceAdapterHandle,
  isSourceDefinition,
} from "@effect-view-server/source-adapter/internal";
import { Context, Effect, Layer, Result, Schema, Scope, Stream } from "effect";

export const SourceAdapterConformanceRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  value: Schema.String,
});
export type SourceAdapterConformanceRow = typeof SourceAdapterConformanceRow.Type;
type SchemaValueInput = Schema.Schema.Type<typeof Schema.Unknown>;

export type SourceAdapterConformanceTarget =
  | {
      readonly _tag: "Materialized";
      readonly lane: "primary" | "sibling";
    }
  | {
      readonly _tag: "Leased";
      readonly route: Readonly<Record<string, unknown>>;
      readonly lane: "primary" | "sibling";
    };

export type SourceAdapterConformanceMutation =
  | {
      readonly _tag: "Upsert";
      readonly row: SourceAdapterConformanceRow;
    }
  | {
      readonly _tag: "Delete";
      readonly id: string;
    };

export type SourceAdapterConformanceAttemptFault =
  | "EmptyLanes"
  | "EmptyLaneId"
  | "DuplicateLaneId"
  | "ChangedLaneIds"
  | "MissingBufferMetrics";

export type SourceAdapterConformanceCommand =
  | {
      readonly _tag: "Delivery";
      readonly target: SourceAdapterConformanceTarget;
      readonly mutations: readonly [
        SourceAdapterConformanceMutation,
        ...ReadonlyArray<SourceAdapterConformanceMutation>,
      ];
      readonly settle?: (exit: SourceApplicationExit) => Effect.Effect<void, unknown>;
    }
  | {
      readonly _tag: "CorruptLaterMutation";
      readonly target: SourceAdapterConformanceTarget;
      readonly firstRow: SourceAdapterConformanceRow;
      readonly laterRow: SourceAdapterConformanceRow;
      readonly field: string;
      readonly value: unknown;
      readonly settle: (exit: SourceApplicationExit) => Effect.Effect<void, unknown>;
    }
  | {
      readonly _tag: "TransitionDefect";
      readonly target: SourceAdapterConformanceTarget;
      readonly row: SourceAdapterConformanceRow;
      readonly settle: (exit: SourceApplicationExit) => Effect.Effect<void, unknown>;
    }
  | {
      readonly _tag: "Reject";
      readonly target: SourceAdapterConformanceTarget;
      readonly phase: "acquire" | "stream" | "settlement";
      readonly offset: bigint;
      readonly settle?: (exit: SourceApplicationExit) => Effect.Effect<void, unknown>;
    }
  | {
      readonly _tag: "FailLane";
      readonly target: SourceAdapterConformanceTarget;
      readonly phase: "acquire" | "stream" | "settlement";
    }
  | {
      readonly _tag: "CompleteLane";
      readonly target: SourceAdapterConformanceTarget;
    }
  | {
      readonly _tag: "FailNextAcquisition";
      readonly target: SourceAdapterConformanceTarget;
      readonly phase: "acquire" | "stream" | "settlement";
      readonly afterFirstResource: boolean;
    }
  | {
      readonly _tag: "ConfigureNextAttempt";
      readonly target: SourceAdapterConformanceTarget;
      readonly fault: SourceAdapterConformanceAttemptFault;
    }
  | {
      readonly _tag: "SetMetrics";
      readonly sample: "updated" | "invalid" | "reset";
    }
  | {
      readonly _tag: "BlockNextFinalizer";
      readonly target: SourceAdapterConformanceTarget;
    }
  | {
      readonly _tag: "ReleaseFinalizer";
      readonly target: SourceAdapterConformanceTarget;
    };

export type SourceAdapterConformanceTransportObservation = {
  readonly acquisitions: bigint;
  readonly finalizations: bigint;
  readonly partialAcquisitionFinalizations: bigint;
  readonly registrations: bigint;
  readonly callbackFinalizations: bigint;
  readonly finalizerStarted: boolean;
};

export type SourceAdapterConformanceTransport = {
  readonly command: (command: SourceAdapterConformanceCommand) => Effect.Effect<void, unknown>;
  readonly observe: (
    target: SourceAdapterConformanceTarget,
  ) => Effect.Effect<SourceAdapterConformanceTransportObservation, unknown>;
  readonly changes: (
    target: SourceAdapterConformanceTarget,
  ) => Stream.Stream<SourceAdapterConformanceTransportObservation, unknown>;
};

export type SourceAdapterLifecycleConformanceExpectations<
  Target extends SourceAdapterConformanceTarget,
> = {
  readonly acquisitionFailure: unknown;
  readonly partialAcquisitionFinalizationCount: bigint;
  readonly streamFailure: unknown;
  readonly settlementFailure: unknown;
  readonly rejectionFailure: (phase: "acquire" | "stream" | "settlement") => unknown;
  readonly rejectionLocation: (target: Target, offset: bigint) => unknown;
  readonly rowId: (target: Target, localId: string) => string;
  readonly updatedMetrics: unknown;
};

export type SourceAdapterConformanceExpectations = {
  readonly materialized:
    | SourceAdapterLifecycleConformanceExpectations<
        Extract<SourceAdapterConformanceTarget, { readonly _tag: "Materialized" }>
      >
    | undefined;
  readonly leased:
    | SourceAdapterLifecycleConformanceExpectations<
        Extract<SourceAdapterConformanceTarget, { readonly _tag: "Leased" }>
      >
    | undefined;
};

type ConformanceAdapter = SourceDefinitionAny["adapter"] & {
  readonly identity: SourceDefinitionAny["identity"];
};

type SourceAdapterConformanceDefinition<
  Adapter extends ConformanceAdapter,
  Lifecycle extends "materialized" | "leased",
  Route extends ReadonlyArray<string>,
> = SourceDefinitionAny & {
  readonly adapter: Adapter;
  readonly lifecycle: Lifecycle;
  readonly routeBy: Route;
};

export type SourceAdapterConformanceCallbackBridge<
  Adapter extends ConformanceAdapter = ConformanceAdapter,
> = {
  readonly source: SourceAdapterConformanceDefinition<Adapter, "materialized", readonly []>;
  readonly capacity: number;
  readonly pauseNextConsumer: Effect.Effect<void>;
  readonly releaseConsumer: Effect.Effect<void>;
  readonly offerBackpressurable: (
    value: SourceAdapterConformanceRow,
  ) => Effect.Effect<void, unknown>;
  readonly offerNonPausable: (value: SourceAdapterConformanceRow) => Effect.Effect<void, unknown>;
  readonly emitBackpressurable: (
    value: SourceAdapterConformanceRow,
  ) => Effect.Effect<void, unknown>;
  readonly emitNonPausable: (value: SourceAdapterConformanceRow) => Effect.Effect<void, unknown>;
};

type SourceAdapterConformanceLifecycleDefinitions<
  Adapter extends ConformanceAdapter,
  Lifecycle extends "materialized" | "leased",
  Route extends ReadonlyArray<string>,
> = {
  readonly source: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route>;
  readonly delayedRetrySource: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route>;
  readonly singleRetrySource: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route>;
};

type SourceAdapterConformanceMaterializedDefinitions<Adapter extends ConformanceAdapter> =
  SourceAdapterConformanceLifecycleDefinitions<Adapter, "materialized", readonly []>;

type SourceAdapterConformanceLeasedDefinitions<Adapter extends ConformanceAdapter> =
  SourceAdapterConformanceLifecycleDefinitions<Adapter, "leased", readonly ["region"]> & {
    readonly sameRoute: Readonly<Record<string, unknown>>;
    readonly distinctRoute: Readonly<Record<string, unknown>>;
  };

const SourceAdapterConformanceDriverValueTypeId: unique symbol = Symbol.for(
  "@effect-view-server/source-adapter-testing/ConformanceDriverValue",
);

// These registries provide runtime provenance for the factory-produced
// callable seams without executing caller-owned functions during validation.
// A structural shape alone cannot prove that a callback returns an Effect or
// that a transport method returns a Stream, while probing either seam can
// mutate state, consume a one-shot stream, or throw for a valid configuration.
const trustedDriverBrandFunctions = new WeakSet<object>();
const trustedDriverValues = new WeakSet<object>();
const trustedCallbackFunctions = new WeakSet<object>();
const trustedTransportFunctions = new WeakSet<object>();

export type SourceAdapterConformanceDriverValue<
  Adapter extends ConformanceAdapter = ConformanceAdapter,
> = {
  readonly [SourceAdapterConformanceDriverValueTypeId]: () => Adapter;
  readonly adapter: Adapter;
  readonly expectations: SourceAdapterConformanceExpectations;
  readonly runtimeContext: Effect.Effect<Context.Context<unknown>, unknown, Scope.Scope>;
  readonly materialized: SourceAdapterConformanceMaterializedDefinitions<Adapter> | undefined;
  readonly leased: SourceAdapterConformanceLeasedDefinitions<Adapter> | undefined;
  readonly callbackBridge: SourceAdapterConformanceCallbackBridge<Adapter> | undefined;
  readonly transport: SourceAdapterConformanceTransport;
};

/*
 * Keep the Context service broad while makeSourceAdapterConformanceDriver retains
 * each concrete Adapter type for adapter-author inference.
 */
export class SourceAdapterConformanceDriver extends Context.Service<
  SourceAdapterConformanceDriver,
  SourceAdapterConformanceDriverValue
>()("@effect-view-server/source-adapter-testing/ConformanceDriver") {}

type SourceAdapterConformanceCapabilitySelection =
  | {
      readonly materialized: true;
      readonly leased?: boolean;
      readonly callbackBridge?: boolean;
    }
  | {
      readonly materialized?: boolean;
      readonly leased: true;
      readonly callbackBridge?: false;
    };

export type SourceAdapterConformanceEventModel = "complete-deliveries" | "continuous-upserts";

export type SourceAdapterConformanceSuiteOptions = {
  readonly name: string;
  readonly layer: Layer.Layer<SourceAdapterConformanceDriver, unknown>;
  /**
   * Describes only the events the real transport can produce. Lifecycle,
   * acquisition, retry, rejection, metrics, sharing, and finalization
   * invariants are always registered.
   */
  readonly eventModel?: SourceAdapterConformanceEventModel;
} & SourceAdapterConformanceCapabilitySelection;

export type SourceAdapterConformanceDriverInput<
  RuntimeServices,
  RuntimeError,
  Adapter extends ConformanceAdapter = ConformanceAdapter,
> = {
  readonly adapter: Adapter;
  readonly expectations: SourceAdapterConformanceExpectations;
  readonly transport: SourceAdapterConformanceTransport;
  readonly runtimeLayer: Layer.Layer<RuntimeServices, RuntimeError, never>;
  readonly materialized?: SourceAdapterConformanceMaterializedDefinitions<NoInfer<Adapter>>;
  readonly leased?: SourceAdapterConformanceLeasedDefinitions<NoInfer<Adapter>>;
  readonly callbackBridge?: SourceAdapterConformanceCallbackBridge<NoInfer<Adapter>>;
};

type IsNever<Value> = [Value] extends [never] ? true : false;

type IsAny<Value> = 0 extends 1 & Value ? true : false;

type ConformanceRowMemberIsExact<Row> =
  IsNever<Row> extends true
    ? false
    : IsAny<Row> extends true
      ? false
      : [Row] extends [SourceAdapterConformanceRow]
        ? [SourceAdapterConformanceRow] extends [Row]
          ? Exclude<keyof Row, keyof SourceAdapterConformanceRow> extends never
            ? Exclude<keyof SourceAdapterConformanceRow, keyof Row> extends never
              ? true
              : false
            : false
          : false
        : false;

type ConformanceRowsAreExact<Row> =
  IsNever<Row> extends true
    ? false
    : IsAny<Row> extends true
      ? false
      : Row extends unknown
        ? ConformanceRowMemberIsExact<Row>
        : false;

type ConformanceDefinitionRowIsExact<Definition> =
  IsNever<Definition> extends true
    ? false
    : Definition extends unknown
      ? false extends ConformanceRowsAreExact<SourceDefinitionRow<Definition>>
        ? false
        : true
      : false;

type ConformanceLifecycleDefinitionRowsAreExact<Definitions> = Definitions extends {
  readonly source: infer Source;
  readonly delayedRetrySource: infer DelayedRetrySource;
  readonly singleRetrySource: infer SingleRetrySource;
}
  ? false extends
      | ConformanceDefinitionRowIsExact<Source>
      | ConformanceDefinitionRowIsExact<DelayedRetrySource>
      | ConformanceDefinitionRowIsExact<SingleRetrySource>
    ? false
    : true
  : false;

type ValidateConformanceLifecycleDefinitionRows<Definitions> = [
  Exclude<Definitions, undefined>,
] extends [never]
  ? unknown
  : false extends ConformanceLifecycleDefinitionRowsAreExact<Exclude<Definitions, undefined>>
    ? never
    : unknown;

type ConformanceCallbackBridgeRowIsExact<Bridge> = Bridge extends {
  readonly source: infer Source;
}
  ? false extends ConformanceDefinitionRowIsExact<Source>
    ? false
    : true
  : false;

type ValidateConformanceCallbackBridgeRow<Bridge> = [Exclude<Bridge, undefined>] extends [never]
  ? unknown
  : false extends ConformanceCallbackBridgeRowIsExact<Exclude<Bridge, undefined>>
    ? never
    : unknown;

type InputProperty<Input, Key extends PropertyKey> = Key extends keyof Input
  ? Input[Key]
  : undefined;

type ConformanceDriverInputRowsAreExact<Input> = Input extends unknown
  ? [
      ValidateConformanceLifecycleDefinitionRows<InputProperty<Input, "materialized">> &
        ValidateConformanceLifecycleDefinitionRows<InputProperty<Input, "leased">> &
        ValidateConformanceCallbackBridgeRow<InputProperty<Input, "callbackBridge">>,
    ] extends [never]
    ? false
    : true
  : false;

type SourceAdapterConformanceDriverInputRowValidation<Input> =
  false extends ConformanceDriverInputRowsAreExact<Input> ? never : unknown;

const validateDefinition = (
  definition: SourceDefinitionAny | undefined,
  lifecycle: "materialized" | "leased",
  adapter: ConformanceAdapter,
): void => {
  if (
    definition !== undefined &&
    !sourceAdapterConformanceDefinitionIsLinked(definition, adapter, lifecycle)
  ) {
    throw new TypeError(
      `Source Adapter conformance requires a nominal ${lifecycle} Definition linked to its adapter.`,
    );
  }
};

export const sourceAdapterConformanceDefinitionIsLinked = <Definition, Adapter>(
  definition: Definition,
  adapter: Adapter,
  lifecycle: "materialized" | "leased",
): boolean =>
  isSourceDefinition(definition) &&
  definition.lifecycle === lifecycle &&
  definition.adapter === adapter;

const conformanceExpectationKeys = [
  "acquisitionFailure",
  "partialAcquisitionFinalizationCount",
  "streamFailure",
  "settlementFailure",
  "rejectionFailure",
  "rejectionLocation",
  "rowId",
  "updatedMetrics",
] as const;

const isObjectValue = (value: SchemaValueInput): value is object =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasOwnValue = (value: object, key: string): boolean => Object.hasOwn(value, key);

const isLeasedRoute = (value: SchemaValueInput): value is Readonly<Record<string, unknown>> => {
  if (!isObjectValue(value) || Object.keys(value).length !== 1 || !hasOwnValue(value, "region")) {
    return false;
  }
  const region = Reflect.get(value, "region");
  return typeof region === "string" && region.length > 0;
};

const validateLifecycleExpectations = <Definitions, Expectations>(
  lifecycle: "materialized" | "leased",
  definitions: Definitions,
  expectations: Expectations,
): void => {
  if ((definitions === undefined) !== (expectations === undefined)) {
    throw new TypeError(
      `Source Adapter conformance requires exact ${lifecycle} expectations for every supplied lifecycle.`,
    );
  }
  if (expectations !== undefined) {
    const countDescriptor =
      typeof expectations === "object" && expectations !== null
        ? Object.getOwnPropertyDescriptor(expectations, "partialAcquisitionFinalizationCount")
        : undefined;
    if (
      countDescriptor === undefined ||
      !("value" in countDescriptor) ||
      typeof countDescriptor.value !== "bigint" ||
      countDescriptor.value <= 0n
    ) {
      throw new TypeError(
        `Source Adapter conformance requires a positive ${lifecycle} partial-acquisition finalization count.`,
      );
    }
    if (
      !isObjectValue(expectations) ||
      !conformanceExpectationKeys.every((key) => hasOwnValue(expectations, key))
    ) {
      throw new TypeError(
        `Source Adapter conformance requires complete ${lifecycle} expectations.`,
      );
    }
  }
};

const isConformanceDefinition = (
  value: SchemaValueInput,
  adapter: SchemaValueInput,
  lifecycle: "materialized" | "leased",
): boolean => {
  if (!isObjectValue(value) || !isSourceDefinition(value)) return false;
  const routeBy = Reflect.get(value, "routeBy");
  const hasExpectedRouteBy =
    Array.isArray(routeBy) &&
    (lifecycle === "materialized"
      ? routeBy.length === 0
      : routeBy.length === 1 && routeBy[0] === "region");
  return (
    hasOwnValue(value, "adapter") &&
    Reflect.get(value, "adapter") === adapter &&
    hasOwnValue(value, "lifecycle") &&
    Reflect.get(value, "lifecycle") === lifecycle &&
    hasExpectedRouteBy
  );
};

const isConformanceLifecycleDefinitions = (
  value: SchemaValueInput,
  adapter: SchemaValueInput,
  lifecycle: "materialized" | "leased",
): boolean => {
  if (value === undefined) return true;
  if (!isObjectValue(value)) return false;
  const definitions = ["source", "delayedRetrySource", "singleRetrySource"] as const;
  if (!definitions.every((key) => hasOwnValue(value, key))) return false;
  if (
    !definitions.every((key) =>
      isConformanceDefinition(Reflect.get(value, key), adapter, lifecycle),
    )
  ) {
    return false;
  }
  if (lifecycle === "leased") {
    const sameRoute = Reflect.get(value, "sameRoute");
    const distinctRoute = Reflect.get(value, "distinctRoute");
    return (
      isLeasedRoute(sameRoute) &&
      isLeasedRoute(distinctRoute) &&
      Reflect.get(sameRoute, "region") !== Reflect.get(distinctRoute, "region")
    );
  }
  return true;
};

const isConformanceExpectations = (value: SchemaValueInput): boolean => {
  if (
    !isObjectValue(value) ||
    !hasOwnValue(value, "materialized") ||
    !hasOwnValue(value, "leased")
  ) {
    return false;
  }
  return ["materialized", "leased"].every((lifecycle) => {
    const expectations = Reflect.get(value, lifecycle);
    if (expectations === undefined) return true;
    if (!isObjectValue(expectations)) return false;
    const count = Reflect.get(expectations, "partialAcquisitionFinalizationCount");
    return (
      conformanceExpectationKeys.every((key) => hasOwnValue(expectations, key)) &&
      typeof count === "bigint" &&
      count > 0n &&
      typeof Reflect.get(expectations, "rejectionFailure") === "function" &&
      typeof Reflect.get(expectations, "rejectionLocation") === "function" &&
      typeof Reflect.get(expectations, "rowId") === "function"
    );
  });
};

const isConformanceCallbackBridge = (
  value: SchemaValueInput,
  adapter: SchemaValueInput,
): boolean => {
  if (value === undefined) return true;
  if (!isObjectValue(value)) return false;
  const callbacks = [
    Reflect.get(value, "offerBackpressurable"),
    Reflect.get(value, "offerNonPausable"),
    Reflect.get(value, "emitBackpressurable"),
    Reflect.get(value, "emitNonPausable"),
  ];
  const isEffectCallback = (callback: SchemaValueInput): boolean =>
    typeof callback === "function" && trustedCallbackFunctions.has(callback);
  return (
    isConformanceDefinition(Reflect.get(value, "source"), adapter, "materialized") &&
    typeof Reflect.get(value, "capacity") === "number" &&
    Reflect.get(value, "capacity") >= 0 &&
    Effect.isEffect(Reflect.get(value, "pauseNextConsumer")) &&
    Effect.isEffect(Reflect.get(value, "releaseConsumer")) &&
    callbacks.every(isEffectCallback)
  );
};

const isConformanceTransport = (value: SchemaValueInput): boolean => {
  if (!isObjectValue(value)) return false;
  const command = Reflect.get(value, "command");
  const observe = Reflect.get(value, "observe");
  const changes = Reflect.get(value, "changes");
  if (
    typeof command !== "function" ||
    typeof observe !== "function" ||
    typeof changes !== "function"
  ) {
    return false;
  }
  return (
    trustedTransportFunctions.has(command) &&
    trustedTransportFunctions.has(observe) &&
    trustedTransportFunctions.has(changes)
  );
};

export const isSourceAdapterConformanceDriverValue = (
  value: Schema.Schema.Type<typeof Schema.Unknown>,
): value is SourceAdapterConformanceDriverValue => {
  const inspected = Result.try(() => {
    if (!isObjectValue(value)) return false;
    if (!trustedDriverValues.has(value)) return false;
    const brand = Reflect.get(value, SourceAdapterConformanceDriverValueTypeId);
    const adapter = Reflect.get(value, "adapter");
    return (
      typeof brand === "function" &&
      trustedDriverBrandFunctions.has(brand) &&
      isSourceAdapterHandle(adapter) &&
      hasOwnValue(value, "expectations") &&
      isConformanceExpectations(Reflect.get(value, "expectations")) &&
      hasOwnValue(value, "runtimeContext") &&
      Effect.isEffect(Reflect.get(value, "runtimeContext")) &&
      isConformanceLifecycleDefinitions(
        Reflect.get(value, "materialized"),
        adapter,
        "materialized",
      ) &&
      isConformanceLifecycleDefinitions(Reflect.get(value, "leased"), adapter, "leased") &&
      isConformanceCallbackBridge(Reflect.get(value, "callbackBridge"), adapter) &&
      hasOwnValue(value, "transport") &&
      isConformanceTransport(Reflect.get(value, "transport"))
    );
  });
  return Result.isSuccess(inspected) && inspected.success;
};

export const makeSourceAdapterConformanceDriver = <
  const Adapter extends ConformanceAdapter,
  RuntimeServices,
  RuntimeError,
  const Input extends {
    readonly adapter: Adapter;
    readonly runtimeLayer: Layer.Layer<RuntimeServices, RuntimeError, never>;
  },
>(
  input: Input & SourceAdapterConformanceDriverInput<RuntimeServices, RuntimeError, Adapter>,
  ..._invalidDefinitionRows: [
    SourceAdapterConformanceDriverInputRowValidation<NoInfer<Input>>,
  ] extends [never]
    ? readonly [never]
    : readonly []
): SourceAdapterConformanceDriverValue<Adapter> => {
  if (!Layer.isLayer(input.runtimeLayer)) {
    throw new TypeError("Source Adapter conformance requires a nominal aggregate runtime Layer.");
  }
  if (!isSourceAdapterHandle(input.adapter)) {
    throw new TypeError("Source Adapter conformance requires one nominal adapter handle.");
  }
  validateDefinition(input.materialized?.source, "materialized", input.adapter);
  validateDefinition(input.materialized?.delayedRetrySource, "materialized", input.adapter);
  validateDefinition(input.materialized?.singleRetrySource, "materialized", input.adapter);
  validateDefinition(input.leased?.source, "leased", input.adapter);
  validateDefinition(input.leased?.delayedRetrySource, "leased", input.adapter);
  validateDefinition(input.leased?.singleRetrySource, "leased", input.adapter);
  validateDefinition(input.callbackBridge?.source, "materialized", input.adapter);
  if (input.materialized === undefined && input.leased === undefined) {
    throw new TypeError("Source Adapter conformance requires at least one lifecycle.");
  }
  validateLifecycleExpectations(
    "materialized",
    input.materialized,
    input.expectations.materialized,
  );
  validateLifecycleExpectations("leased", input.leased, input.expectations.leased);
  if (
    input.leased !== undefined &&
    (!isLeasedRoute(input.leased.sameRoute) ||
      !isLeasedRoute(input.leased.distinctRoute) ||
      Reflect.get(input.leased.sameRoute, "region") ===
        Reflect.get(input.leased.distinctRoute, "region"))
  ) {
    throw new TypeError(
      "Leased Source Adapter conformance requires two non-empty, distinct region routes.",
    );
  }
  const { runtimeLayer, ...driver } = input;
  const brand = () => value.adapter;
  const value: SourceAdapterConformanceDriverValue<Adapter> = {
    [SourceAdapterConformanceDriverValueTypeId]: brand,
    adapter: driver.adapter,
    expectations: driver.expectations,
    materialized: driver.materialized,
    leased: driver.leased,
    callbackBridge: driver.callbackBridge,
    transport: driver.transport,
    runtimeContext: Layer.build<never, RuntimeError, RuntimeServices>(runtimeLayer).pipe(
      Effect.map((context) => Context.makeUnsafe<unknown>(context.mapUnsafe)),
    ),
  };
  trustedDriverBrandFunctions.add(brand);
  if (driver.callbackBridge !== undefined) {
    trustedCallbackFunctions.add(driver.callbackBridge.offerBackpressurable);
    trustedCallbackFunctions.add(driver.callbackBridge.offerNonPausable);
    trustedCallbackFunctions.add(driver.callbackBridge.emitBackpressurable);
    trustedCallbackFunctions.add(driver.callbackBridge.emitNonPausable);
  }
  trustedTransportFunctions.add(driver.transport.command);
  trustedTransportFunctions.add(driver.transport.observe);
  trustedTransportFunctions.add(driver.transport.changes);
  trustedDriverValues.add(value);
  return Object.freeze(value);
};

export const SourceAdapterConformance = {
  Driver: SourceAdapterConformanceDriver,
  driver: makeSourceAdapterConformanceDriver,
  isDriverValue: isSourceAdapterConformanceDriverValue,
} as const;
