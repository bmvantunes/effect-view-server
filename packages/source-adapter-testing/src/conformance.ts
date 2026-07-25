import type {
  SourceApplicationExit,
  SourceDefinition,
  SourceDefinitionAny,
} from "@effect-view-server/source-adapter";
import {
  isSourceAdapterHandle,
  isSourceDefinition,
} from "@effect-view-server/source-adapter/internal";
import { Context, Effect, Layer, Schema, Scope, Stream } from "effect";

export const SourceAdapterConformanceRow = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  value: Schema.String,
});
export type SourceAdapterConformanceRow = typeof SourceAdapterConformanceRow.Type;

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
  Row extends object = object,
> = SourceDefinition<Adapter, Lifecycle, unknown, Route, unknown, Row>;

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
  Row extends object = object,
> = {
  readonly source: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route, Row>;
  readonly delayedRetrySource: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route, Row>;
  readonly singleRetrySource: SourceAdapterConformanceDefinition<Adapter, Lifecycle, Route, Row>;
};

type SourceAdapterConformanceMaterializedDefinitions<
  Adapter extends ConformanceAdapter,
  Row extends object = object,
> = SourceAdapterConformanceLifecycleDefinitions<Adapter, "materialized", readonly [], Row>;

type SourceAdapterConformanceLeasedDefinitions<
  Adapter extends ConformanceAdapter,
  Row extends object = object,
> = SourceAdapterConformanceLifecycleDefinitions<Adapter, "leased", readonly ["region"], Row> & {
  readonly sameRoute: Readonly<Record<string, unknown>>;
  readonly distinctRoute: Readonly<Record<string, unknown>>;
};

const SourceAdapterConformanceDriverValueTypeId: unique symbol = Symbol.for(
  "@effect-view-server/source-adapter-testing/ConformanceDriverValue",
);

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
  readonly materialized?: SourceAdapterConformanceMaterializedDefinitions<
    NoInfer<Adapter>,
    SourceAdapterConformanceRow
  >;
  readonly leased?: SourceAdapterConformanceLeasedDefinitions<
    NoInfer<Adapter>,
    SourceAdapterConformanceRow
  >;
  readonly callbackBridge?: SourceAdapterConformanceCallbackBridge<NoInfer<Adapter>>;
};

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

export const sourceAdapterConformanceDefinitionIsLinked = (
  definition: unknown,
  adapter: unknown,
  lifecycle: "materialized" | "leased",
): boolean =>
  isSourceDefinition(definition) &&
  definition.lifecycle === lifecycle &&
  definition.adapter === adapter;

const validateLifecycleExpectations = (
  lifecycle: "materialized" | "leased",
  definitions: SourceAdapterConformanceDriverValue["materialized" | "leased"],
  expectations: unknown,
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
  }
};

export const isSourceAdapterConformanceDriverValue = (
  value: unknown,
): value is SourceAdapterConformanceDriverValue => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const brand = Reflect.get(value, SourceAdapterConformanceDriverValueTypeId);
  return (
    typeof brand === "function" && Reflect.apply(brand, value, []) === Reflect.get(value, "adapter")
  );
};

export const makeSourceAdapterConformanceDriver = <
  const Adapter extends ConformanceAdapter,
  RuntimeServices,
  RuntimeError,
>(
  input: SourceAdapterConformanceDriverInput<RuntimeServices, RuntimeError, Adapter>,
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
    (Object.keys(input.leased.sameRoute).length === 0 ||
      Object.keys(input.leased.distinctRoute).length === 0)
  ) {
    throw new TypeError("Leased Source Adapter conformance requires two non-empty routes.");
  }
  const { runtimeLayer, ...driver } = input;
  const value: SourceAdapterConformanceDriverValue<Adapter> = {
    [SourceAdapterConformanceDriverValueTypeId]: () => value.adapter,
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
  return Object.freeze(value);
};

export const SourceAdapterConformance = {
  Driver: SourceAdapterConformanceDriver,
  driver: makeSourceAdapterConformanceDriver,
  isDriverValue: isSourceAdapterConformanceDriverValue,
} as const;
