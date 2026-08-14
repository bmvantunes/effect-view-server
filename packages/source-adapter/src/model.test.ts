import { conditionalFields } from "@effect-view-server/effect-utils";
import { describe, expect, it } from "@effect/vitest";
import { Chunk, Effect, Exit, Option, Schedule, Schema, Stream } from "effect";
import type {
  SourceApplicationTransition,
  SourceBufferMetrics,
  SourceDefinitionOptionsFamily,
  SourceDelivery,
  SourceMutation,
  SourceSettlement,
} from "./index";
import { definedFields } from "./optional-fields";
import {
  decodeSourceToolkitUpsert,
  isSourceAdapterHandle,
  isSourceApplicationTransition,
  isSourceAttempt,
  isSourceDefinition,
  isSourceDelivery,
  isSourceItemRejection,
  isSourceMutation,
  isSourceMaintenanceOperation,
  isSourceToolkit,
  makeRuntimeSourceFailure,
  makeSourceAttempt,
  makeSourceApplicationStateRegistration,
  makeSourceApplicationTransition,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceMaintenanceOperation,
  makeSourceTransitionDelivery,
  makeSourceUpsert,
  markSourceToolkit,
  resolveSourceApplicationStateRegistration,
  resolveSourceApplicationTransition,
  resolveSourceMaintenanceOperation,
} from "./internal";
import { registerSourceDefinition } from "./definition-brand";
import { resolveSourceAdapterHandle } from "./model";
import { SourceAdapter, sourceExecutionFailureSchema } from "./index";

const Failure = Schema.TaggedStruct("FixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  observed: Schema.BigInt,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

type ToolkitRow = { readonly row: unknown };

function toolkitDelivery(
  mutations: Chunk.NonEmptyChunk<SourceMutation<ToolkitRow>>,
  settlement?: SourceSettlement<unknown>,
): Effect.Effect<SourceDelivery<ToolkitRow, unknown>>;
function toolkitDelivery(
  mutation: SourceMutation<ToolkitRow>,
  settlement: SourceSettlement<unknown> | undefined,
  transition: SourceApplicationTransition,
): Effect.Effect<SourceDelivery<ToolkitRow, unknown>>;
function toolkitDelivery(
  mutationsOrMutation: Chunk.NonEmptyChunk<SourceMutation<ToolkitRow>> | SourceMutation<ToolkitRow>,
  settlement?: SourceSettlement<unknown>,
  transition?: SourceApplicationTransition,
): Effect.Effect<SourceDelivery<ToolkitRow, unknown>> {
  return transition === undefined && Chunk.isChunk(mutationsOrMutation)
    ? Effect.succeed(makeSourceDelivery(mutationsOrMutation, settlement))
    : transition !== undefined && !Chunk.isChunk(mutationsOrMutation)
      ? Effect.succeed(makeSourceTransitionDelivery(mutationsOrMutation, settlement, transition))
      : Effect.die(new TypeError("Invalid fixture delivery shape."));
}

type MappedDefinitionOptions<Row extends object> = {
  readonly row: Schema.Codec<Row, unknown, never, never>;
};

interface MappedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: MappedDefinitionOptions<this["Row"]>;
}

const makeMaterializedAdapter = () =>
  SourceAdapter.make({
    identity: {
      name: "materialized-fixture",
      version: "1",
    },
    failure: Failure,
    materialized: {
      metrics: Metrics,
      rejectionLocation: Location,
      definitionOptions: SourceAdapter.definitionOptions<{
        readonly label: string;
        readonly nested?: {
          readonly enabled: boolean;
        };
      }>(),
    },
    leased: undefined,
  });

const nominalClone = <Value extends object>(
  value: Value,
  input: {
    readonly overrides?: ReadonlyMap<PropertyKey, unknown>;
    readonly omit?: ReadonlySet<PropertyKey>;
    readonly extras?: ReadonlyMap<PropertyKey, unknown>;
    readonly accessors?: ReadonlyMap<PropertyKey, () => unknown>;
    readonly nonEnumerable?: ReadonlySet<PropertyKey>;
    readonly freeze?: boolean;
  } = {},
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    if (input.omit?.has(property) === true) {
      continue;
    }
    const descriptor = Option.getOrThrow(
      Option.fromUndefinedOr(Reflect.getOwnPropertyDescriptor(value, property)),
    );
    const accessor = input.accessors?.get(property);
    if (accessor !== undefined) {
      Object.defineProperty(clone, property, {
        configurable: true,
        enumerable: input.nonEnumerable?.has(property) !== true,
        get: accessor,
      });
      continue;
    }
    const next: PropertyDescriptor = {
      ...descriptor,
      ...conditionalFields(input.nonEnumerable?.has(property) === true, () => ({
        enumerable: false,
      })),
      ...conditionalFields("value" in descriptor, () => ({
        value:
          typeof property === "symbol" && typeof descriptor.value === "function"
            ? () => clone
            : input.overrides?.has(property) === true
              ? input.overrides.get(property)
              : descriptor.value,
      })),
    };
    Object.defineProperty(clone, property, next);
  }
  for (const [property, extra] of input.extras ?? []) {
    Object.defineProperty(clone, property, {
      enumerable: true,
      value: extra,
    });
  }
  return input.freeze === false ? clone : Object.freeze(clone);
};

describe("Source Adapter portable model", () => {
  it("omits undefined optional fields", () => {
    expect(definedFields(undefined, () => ({ omitted: true }))).toStrictEqual({});
    expect(
      definedFields("value", () => ({
        omitted: undefined,
        present: "value",
      })),
    ).toStrictEqual({ present: "value" });
  });

  it("rejects malformed nominal Application State lifecycle capabilities", () => {
    const validInput = () => ({
      bind: () => undefined,
      forLifetime: () => undefined,
      lifetimeIdentity: () => Object.freeze({}),
      unbind: () => undefined,
      sweepIntervalNanos: 1n,
      runDueSweep: () => Effect.void,
    });
    const hostileBind = new Proxy(() => undefined, {
      get: (target, property, receiver) => {
        if (property === "constructor") {
          throw new Error("hostile constructor");
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const missingCapability = validInput();
    Reflect.deleteProperty(missingCapability, "bind");
    const surplusCapability = {
      ...validInput(),
      select: () => undefined,
    };
    const asyncGeneratorBind = async function* () {
      yield undefined;
    };
    const invalidInputs = [
      missingCapability,
      surplusCapability,
      {
        ...validInput(),
        bind: undefined,
      },
      {
        ...validInput(),
        bind: hostileBind,
      },
      {
        ...validInput(),
        bind: asyncGeneratorBind,
      },
      {
        ...validInput(),
        forLifetime: async () => Promise.resolve(undefined),
      },
      {
        ...validInput(),
        lifetimeIdentity: undefined,
      },
      {
        ...validInput(),
        unbind: undefined,
      },
      {
        ...validInput(),
        sweepIntervalNanos: 0n,
      },
      {
        ...validInput(),
        sweepIntervalNanos: 1,
      },
      {
        ...validInput(),
        runDueSweep: undefined,
      },
    ];

    for (const input of invalidInputs) {
      expect(() =>
        Reflect.apply(makeSourceApplicationStateRegistration, undefined, [input]),
      ).toThrow(
        "Source Application State registration requires exact synchronous lifecycle capabilities.",
      );
    }
  });

  it("snapshots nominal Application State lifecycle capabilities", () => {
    let originalBindings = 0;
    let replacementBindings = 0;
    const input = {
      bind: () => {
        originalBindings += 1;
      },
      forLifetime: () => "original",
      lifetimeIdentity: () => Object.freeze({}),
      unbind: () => undefined,
      sweepIntervalNanos: 1n,
      runDueSweep: () => Effect.void,
    };
    const registration = makeSourceApplicationStateRegistration(input);
    Reflect.set(input, "bind", () => {
      replacementBindings += 1;
    });
    Reflect.set(input, "forLifetime", () => "replacement");
    const internal = Option.getOrThrow(
      Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
    );
    Reflect.apply(internal.bind, undefined, [{}]);

    expect({
      forLifetime: Reflect.apply(registration.forLifetime, undefined, [undefined, "orders"]),
      originalBindings,
      replacementBindings,
    }).toStrictEqual({
      forLifetime: "original",
      originalBindings: 1,
      replacementBindings: 0,
    });
  });

  it("creates frozen row-bound definition-option family tokens", () => {
    const token = SourceAdapter.definitionOptionsFamily<MappedDefinitionOptionsFamily>();
    expect(Object.isFrozen(token)).toBe(true);
    const adapter = SourceAdapter.make({
      identity: { name: "mapped-definition-options" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: token,
      },
      leased: undefined,
    });
    const Row = Schema.Struct({ id: Schema.String });
    const definition = adapter.materializedSource<typeof Row.Type>({ row: Row });
    const delegatedDefinition = SourceAdapter.materializedSource(adapter, {
      row: Row,
    });
    expect(definition.options.row).toBe(Row);
    expect(delegatedDefinition.options.row).toBe(Row);
  });

  it("creates stable runtime descriptors without delegated lifecycle builders", () => {
    const handle = makeMaterializedAdapter();
    const descriptor = SourceAdapter.descriptor(handle);
    const leasedOnly = SourceAdapter.make({
      identity: { name: "leased-only-fixture" },
      failure: Failure,
      materialized: undefined,
      leased: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
      },
    });
    const directDefinition = handle.materializedSource({ label: "direct" });
    const definition = SourceAdapter.materializedSource(handle, { label: "orders" });
    const symbolValues = Reflect.ownKeys(descriptor)
      .filter((key): key is symbol => typeof key === "symbol")
      .map((key) => Reflect.get(descriptor, key));
    const copiedHandle = Object.defineProperties({}, Object.getOwnPropertyDescriptors(handle));
    const copiedDescriptor = Object.defineProperties(
      {},
      Object.getOwnPropertyDescriptors(descriptor),
    );

    expect(SourceAdapter.descriptor(handle)).toBe(descriptor);
    expect(directDefinition.adapter).toBe(handle);
    expect(definition.adapter).toBe(descriptor);
    expect(isSourceDefinition(definition)).toBe(true);
    expect(isSourceAdapterHandle(descriptor)).toBe(true);
    expect(Object.keys(descriptor)).toStrictEqual([
      "identity",
      "failureSchema",
      "materialized",
      "leased",
      "runtimeService",
      "failure",
    ]);
    expect(Reflect.get(descriptor, "materializedSource")).toBeUndefined();
    expect(Reflect.get(descriptor, "leasedSource")).toBeUndefined();
    expect(symbolValues).toHaveLength(1);
    expect(typeof symbolValues[0]).toBe("symbol");
    expect(Object.isFrozen(descriptor)).toBe(true);
    expect(() => Reflect.apply(SourceAdapter.descriptor, undefined, [copiedHandle])).toThrow(
      "requires a nominal Source Adapter handle",
    );
    expect(() => Reflect.apply(resolveSourceAdapterHandle, undefined, [copiedDescriptor])).toThrow(
      "is not linked to a nominal handle",
    );
    expect(() =>
      Reflect.apply(SourceAdapter.materializedSource, undefined, [descriptor, { label: "orders" }]),
    ).toThrow("delegated construction requires a nominal Source Adapter handle");
    expect(() =>
      Reflect.apply(SourceAdapter.materializedSource, undefined, [leasedOnly, { label: "orders" }]),
    ).toThrow("This Source Adapter does not declare a Materialized lifecycle.");
  });

  it.effect("creates nominal adapters and validates safe adapter failures", () =>
    Effect.gen(function* () {
      const adapter = makeMaterializedAdapter();
      expect(isSourceAdapterHandle(adapter)).toBe(true);
      expect(Object.isFrozen(adapter)).toBe(true);
      expect(Object.isFrozen(adapter.identity)).toBe(true);
      expect(Object.isFrozen(adapter.materialized)).toBe(true);

      const failure = yield* adapter.failure({
        _tag: "FixtureFailure",
        message: "offline",
      });
      expect(failure).toStrictEqual({
        _tag: "AdapterFailure",
        failure: {
          _tag: "FixtureFailure",
          message: "offline",
        },
      });
      const invalidFailureEffect: Effect.Effect<unknown, unknown> = Reflect.apply(
        adapter.failure,
        adapter,
        [
          {
            _tag: "FixtureFailure",
            message: 1,
          },
        ],
      );
      const invalidFailure = yield* Effect.exit(invalidFailureEffect);
      expect(Exit.isFailure(invalidFailure)).toBe(true);

      const executionFailure = sourceExecutionFailureSchema(Failure);
      expect(yield* Schema.decodeUnknownEffect(executionFailure)(failure)).toStrictEqual(failure);
      expect(
        yield* Schema.decodeUnknownEffect(executionFailure)({
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceMetrics",
            message: "invalid",
          },
        }),
      ).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceMetrics",
          message: "invalid",
        },
      });
    }),
  );

  it("rejects malformed adapter declarations at the runtime boundary", () => {
    const validLifecycle = {
      metrics: Metrics,
      rejectionLocation: Location,
      definitionOptions: SourceAdapter.definitionOptions<void>(),
    };
    const invalidInputs = [
      {
        identity: { name: "" },
        failure: Failure,
        materialized: validLifecycle,
        leased: undefined,
      },
      {
        identity: { name: "fixture", version: "" },
        failure: Failure,
        materialized: validLifecycle,
        leased: undefined,
      },
      {
        identity: { name: "fixture", version: undefined },
        failure: Failure,
        materialized: validLifecycle,
        leased: undefined,
      },
      {
        identity: { name: "fixture", extra: true },
        failure: Failure,
        materialized: validLifecycle,
        leased: undefined,
      },
      {
        identity: { name: "fixture" },
        failure: {},
        materialized: validLifecycle,
        leased: undefined,
      },
      {
        identity: { name: "fixture" },
        failure: Failure,
        materialized: undefined,
        leased: undefined,
      },
      {
        identity: { name: "fixture" },
        failure: Failure,
        materialized: {
          metrics: {},
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      },
      {
        identity: { name: "fixture" },
        failure: Failure,
        materialized: {
          metrics: Metrics,
          rejectionLocation: Location,
          definitionOptions: {},
        },
        leased: undefined,
      },
      {
        identity: { name: "fixture" },
        failure: Failure,
        materialized: {
          ...validLifecycle,
          extra: true,
        },
        leased: undefined,
      },
    ];
    for (const input of invalidInputs) {
      expect(() => Reflect.apply(SourceAdapter.make, undefined, [input])).toThrow(TypeError);
    }
  });

  it("snapshot-freezes Source Definitions and rejects unsafe option graphs", () => {
    const adapter = makeMaterializedAdapter();
    const options = {
      label: "orders",
      nested: {
        enabled: true,
      },
    };
    const definition = adapter.materializedSource(options);
    options.label = "changed";
    options.nested.enabled = false;

    expect(definition.options).toStrictEqual({
      label: "orders",
      nested: {
        enabled: true,
      },
    });
    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(definition.options)).toBe(true);
    expect(Object.isFrozen(definition.options.nested)).toBe(true);
    expect(isSourceDefinition(definition)).toBe(true);

    type CyclicOptions = { self?: CyclicOptions };
    const cyclic: CyclicOptions = {};
    cyclic.self = cyclic;
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [cyclic])).toThrow(
      "must not contain cycles",
    );

    const cyclicArray: Array<unknown> = [];
    cyclicArray.push(cyclicArray);
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [cyclicArray])).toThrow(
      "must not contain cycles",
    );

    const symbolOptions = {
      label: "orders",
      [Symbol("private")]: true,
    };
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [symbolOptions])).toThrow(
      "must use string data fields",
    );

    const accessorOptions = Object.defineProperty({}, "label", {
      enumerable: true,
      get: () => "orders",
    });
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [accessorOptions])).toThrow(
      "enumerable data properties",
    );

    class FrozenOption {
      readonly label = "orders";
    }
    const classOption = new FrozenOption();
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [classOption])).toThrow(
      "plain data objects or supported Effect executable values",
    );
    expect(Object.isFrozen(classOption)).toBe(false);

    const dateOption = {
      label: "orders",
      cursor: new Date(0),
    };
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [dateOption])).toThrow(
      "plain data objects or supported Effect executable values",
    );
    expect(Object.isFrozen(dateOption.cursor)).toBe(false);
  });

  it("preserves supported executable Source Definition option leaves", () => {
    const program = Effect.succeed("ready");
    const schedule = Schedule.recurs(1);
    const row = Schema.Struct({ id: Schema.String });
    const map = (value: string) => ({ id: value });
    const executableMap = SourceAdapter.executable(map);
    const codec = SourceAdapter.executable({
      format: "fixture",
      decode: executableMap,
    });
    const adapter = SourceAdapter.make({
      identity: { name: "executable-options" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<{
          readonly program: typeof program;
          readonly schedule: typeof schedule;
          readonly row: typeof row;
          readonly map: typeof map;
          readonly codec: typeof codec;
        }>(),
      },
      leased: undefined,
    });
    const definition = adapter.materializedSource({
      program,
      schedule,
      row,
      map: executableMap,
      codec,
    });

    expect(definition.options.program).toBe(program);
    expect(definition.options.schedule).toBe(schedule);
    expect(definition.options.row).toBe(row);
    expect(definition.options.map).toBe(executableMap);
    expect(definition.options.codec).toBe(codec);
    expect(SourceAdapter.executable(codec)).toBe(codec);
    expect(SourceAdapter.executable(executableMap)).toBe(executableMap);
    expect(() =>
      Reflect.apply(SourceAdapter.executable, undefined, [Object.freeze({ format: "frozen" })]),
    ).toThrow("must be extensible objects");
    expect(() => Reflect.apply(SourceAdapter.executable, undefined, [1])).toThrow(
      "must be extensible objects",
    );
    expect(() => Reflect.apply(SourceAdapter.executable, undefined, [null])).toThrow(
      "must be extensible objects",
    );
  });

  it("rejects every hostile Source Definition envelope branch without throwing", () => {
    const materializedAdapter = makeMaterializedAdapter();
    const materialized = materializedAdapter.materializedSource({ label: "orders" });
    const leasedAdapter = SourceAdapter.make({
      identity: { name: "leased-envelope" },
      failure: Failure,
      materialized: undefined,
      leased: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
      },
    });
    const leased = leasedAdapter.leasedSource(["region"], { label: "orders" }, Schedule.recurs(1));
    const metadataKey = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(materialized).find(
          (property) =>
            typeof property === "symbol" && typeof Reflect.get(materialized, property) === "object",
        ),
      ),
    );
    const metadata = Option.getOrThrow(
      Option.liftPredicate(
        Reflect.get(materialized, metadataKey),
        (value): value is object => typeof value === "object" && value !== null,
      ),
    );
    const metadataWith = (overrides: ReadonlyMap<PropertyKey, unknown>) =>
      nominalClone(metadata, { overrides });
    const definitionWith = (
      overrides: ReadonlyMap<PropertyKey, unknown>,
      metadataOverrides: ReadonlyMap<PropertyKey, unknown> = new Map(),
    ) =>
      nominalClone(materialized, {
        overrides: new Map([
          ...overrides,
          [
            metadataKey,
            metadataWith(
              new Map([
                ...metadataOverrides,
                ...(overrides.has("adapter")
                  ? [["adapter", overrides.get("adapter")] as const]
                  : []),
                ...(overrides.has("lifecycle")
                  ? [["lifecycle", overrides.get("lifecycle")] as const]
                  : []),
                ...(overrides.has("options")
                  ? [["options", overrides.get("options")] as const]
                  : []),
                ...(overrides.has("routeBy")
                  ? [["routeFields", overrides.get("routeBy")] as const]
                  : []),
              ]),
            ),
          ],
        ]),
      });
    const unfrozenAdapter = nominalClone(materializedAdapter, { freeze: false });
    const missingLifecycleAdapter = nominalClone(materializedAdapter, {
      overrides: new Map([["materialized", undefined]]),
    });
    const invalidValues = [
      null,
      {},
      nominalClone(materialized, { freeze: false }),
      nominalClone(materialized, {
        extras: new Map([["extra", true]]),
      }),
      nominalClone(materialized, {
        omit: new Set(["options"]),
        extras: new Map([["unexpected", materialized.options]]),
      }),
      nominalClone(materialized, {
        accessors: new Map([["options", () => materialized.options]]),
      }),
      nominalClone(materialized, {
        nonEnumerable: new Set(["options"]),
      }),
      definitionWith(new Map([["adapter", null]])),
      definitionWith(new Map([["adapter", {}]])),
      definitionWith(new Map([["adapter", unfrozenAdapter]])),
      definitionWith(new Map([["identity", { name: "other" }]])),
      definitionWith(new Map([["lifecycle", "invalid"]])),
      definitionWith(new Map([["routeBy", {}]])),
      definitionWith(new Map([["routeBy", []]])),
      definitionWith(new Map([["routeBy", Object.freeze([""])]])),
      definitionWith(new Map([["routeBy", Object.freeze(["region", 1])]])),
      definitionWith(new Map([["routeBy", Object.freeze(["region", "region"])]])),
      definitionWith(new Map([["routeBy", Object.freeze(["region"])]])),
      nominalClone(leased, {
        overrides: new Map([["routeBy", Object.freeze([])]]),
      }),
      definitionWith(new Map([["adapter", missingLifecycleAdapter]])),
      definitionWith(new Map([["retry", null]])),
      definitionWith(new Map([["retry", { _tag: "UseAdapterDefault" }]])),
      definitionWith(new Map([["retry", Object.freeze({ _tag: "Unknown" })]])),
      definitionWith(
        new Map([
          [
            "retry",
            Object.freeze({
              _tag: "UseAdapterDefault",
              extra: true,
            }),
          ],
        ]),
      ),
      definitionWith(
        new Map([
          [
            "retry",
            Object.freeze({
              _tag: "Override",
              policy: null,
            }),
          ],
        ]),
      ),
      nominalClone(materialized, {
        overrides: new Map([[metadataKey, null]]),
      }),
      nominalClone(materialized, {
        overrides: new Map([[metadataKey, nominalClone(metadata, { freeze: false })]]),
      }),
      nominalClone(materialized, {
        overrides: new Map([
          [
            metadataKey,
            nominalClone(metadata, {
              extras: new Map([["extra", true]]),
            }),
          ],
        ]),
      }),
      definitionWith(new Map(), new Map([["adapter", leasedAdapter]])),
      definitionWith(new Map(), new Map([["lifecycle", "leased"]])),
      definitionWith(new Map(), new Map([["options", {}]])),
      definitionWith(new Map(), new Map([["routeFields", ["region"]]])),
      new Proxy(materialized, {
        ownKeys: () => {
          throw new Error("hostile ownKeys");
        },
      }),
      new Proxy(materialized, {
        getOwnPropertyDescriptor: () => {
          throw new Error("hostile descriptor");
        },
      }),
    ];

    expect(Object.isFrozen(materialized.retry)).toBe(true);
    expect(Object.isFrozen(leased.retry)).toBe(true);
    expect(isSourceDefinition(leased)).toBe(true);
    expect(
      invalidValues.map((value) =>
        typeof value === "object" && value !== null
          ? isSourceDefinition(registerSourceDefinition(value))
          : isSourceDefinition(value),
      ),
    ).toStrictEqual(invalidValues.map(() => false));
  });

  it("constructs exact retry selections and validates leased route declarations", () => {
    const adapter = SourceAdapter.make({
      identity: { name: "leased-fixture" },
      failure: Failure,
      materialized: undefined,
      leased: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
      },
    });
    const defaultRetry = adapter.leasedSource(["region"], {
      label: "orders",
    });
    const overrideRetry = adapter.leasedSource(
      ["region", "desk"],
      { label: "orders" },
      Schedule.recurs(0),
    );
    expect(defaultRetry.retry).toStrictEqual({
      _tag: "UseAdapterDefault",
    });
    expect(overrideRetry.retry._tag).toBe("Override");
    expect(defaultRetry.routeBy).toStrictEqual(["region"]);
    expect(Object.isFrozen(defaultRetry.routeBy)).toBe(true);

    for (const routeBy of [[], [""], ["region", "region"]]) {
      expect(() =>
        Reflect.apply(adapter.leasedSource, adapter, [routeBy, { label: "orders" }]),
      ).toThrow("non-empty, unique");
    }
    expect(() => Reflect.apply(adapter.materializedSource, adapter, [{}])).toThrow(
      "does not declare a Materialized lifecycle",
    );
    expect(() =>
      Reflect.apply(makeMaterializedAdapter().leasedSource, adapter, [["region"], {}]),
    ).toThrow("does not declare a Leased lifecycle");
  });

  it.effect("constructs nominal mutations, events, toolkits, and attempts", () =>
    Effect.gen(function* () {
      const upsert = makeSourceUpsert({ id: "a" });
      const deletion = makeSourceDelete("a");
      const delivery = makeSourceDelivery(Chunk.make(upsert, deletion));
      let transitioned = 0;
      const mutableCancellationIds = ["expiry:a:1"];
      const lifetimeIdentity = Object.freeze({});
      const transition = makeSourceApplicationTransition(
        "orders",
        () => {
          transitioned += 1;
        },
        mutableCancellationIds,
        lifetimeIdentity,
      );
      mutableCancellationIds[0] = "hostile-replacement";
      mutableCancellationIds.push("hostile-addition");
      const transitionedDelivery = makeSourceTransitionDelivery(deletion, undefined, transition);
      const rejection = makeSourceItemRejection({
        failure: makeRuntimeSourceFailure({
          _tag: "InvalidTopicRow",
          topic: "orders",
          message: "invalid",
        }),
        location: { offset: 1n },
        rejectedAtNanos: 2n,
      });
      const unbuffered: SourceBufferMetrics = {
        _tag: "Unbuffered",
      };
      const lane = {
        id: "fixture",
        events: Stream.make(delivery, rejection),
        bufferMetrics: Effect.succeed(unbuffered),
      };
      const attempt = makeSourceAttempt([lane]);
      const toolkit = markSourceToolkit({
        topic: "orders",
        upsert: (row: { readonly row: unknown }) => Effect.succeed(makeSourceUpsert(row)),
        decodeUpsert: <Row>(row: Row) => Effect.succeed(makeSourceUpsert<ToolkitRow>({ row })),
        delete: (id: string) => Effect.succeed(makeSourceDelete(id)),
        delivery: toolkitDelivery,
        reject: (input) => Effect.succeed(makeSourceItemRejection(input)),
      });

      expect(isSourceMutation(upsert)).toBe(true);
      expect(isSourceMutation(deletion)).toBe(true);
      expect(isSourceDelivery(delivery)).toBe(true);
      expect(isSourceDelivery(transitionedDelivery)).toBe(true);
      expect(transitionedDelivery.transition).toBe(transition);
      const internalTransition = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(transition)),
      );
      internalTransition.apply();
      expect(transitioned).toBe(1);
      expect(internalTransition.cancelledMaintenanceWorkIds).toStrictEqual(["expiry:a:1"]);
      expect(Object.isFrozen(internalTransition.cancelledMaintenanceWorkIds)).toBe(true);
      expect(() =>
        makeSourceApplicationTransition(
          "orders",
          () => undefined,
          ["duplicate", "duplicate"],
          lifetimeIdentity,
        ),
      ).toThrow("unique non-empty strings");
      expect(() =>
        makeSourceApplicationTransition("orders", () => undefined, [""], lifetimeIdentity),
      ).toThrow("unique non-empty strings");
      expect(isSourceItemRejection(rejection)).toBe(true);
      expect(isSourceAttempt(attempt)).toBe(true);
      expect(isSourceToolkit(toolkit)).toBe(true);
      expect(Object.hasOwn(toolkit, "decodeUpsert")).toBe(true);
      expect((yield* decodeSourceToolkitUpsert(toolkit, { decoded: true })).row).toStrictEqual({
        row: { decoded: true },
      });
      expect(Object.isFrozen(upsert)).toBe(true);
      expect(Object.isFrozen(delivery)).toBe(true);
      expect(Object.isFrozen(rejection.diagnostic)).toBe(true);
      yield* delivery.settle(Exit.void);
      yield* rejection.settle(Exit.void);

      expect(() => Reflect.apply(makeSourceAttempt, undefined, [[]])).toThrow(
        "non-empty and unique",
      );
      expect(() => makeSourceAttempt([lane, lane])).toThrow("non-empty and unique");
      expect(() =>
        makeSourceAttempt([
          {
            ...lane,
            id: "",
          },
        ]),
      ).toThrow("non-empty and unique");
    }),
  );

  it("rejects structural copies of nominal values", () => {
    const adapter = makeMaterializedAdapter();
    const definition = adapter.materializedSource({ label: "orders" });
    const mutation = makeSourceDelete("a");
    const lifetimeIdentity = Object.freeze({});
    const transition = makeSourceApplicationTransition(
      "orders",
      () => undefined,
      [],
      lifetimeIdentity,
    );
    const maintenance = makeSourceMaintenanceOperation({
      topic: "orders",
      id: "a",
      workId: "a:1",
      lifetimeIdentity,
      isCurrent: () => true,
      onSuccess: () => undefined,
      onFailure: () => undefined,
      onStale: () => undefined,
    });
    const copiedAdapter = Object.defineProperties({}, Object.getOwnPropertyDescriptors(adapter));
    const copiedDefinition = Object.defineProperties(
      {},
      Object.getOwnPropertyDescriptors(definition),
    );
    const sourceDefinitionBrand = Option.getOrThrow(
      Option.fromUndefinedOr(
        Reflect.ownKeys(definition).find(
          (key) =>
            typeof key === "symbol" &&
            key.description === "@effect-view-server/source-adapter/SourceDefinition",
        ),
      ),
    );
    const selfBrandedDefinitionCopy: Record<PropertyKey, unknown> = {};
    for (const key of Reflect.ownKeys(definition)) {
      const descriptor = Option.getOrThrow(
        Option.fromUndefinedOr(Object.getOwnPropertyDescriptor(definition, key)),
      );
      Object.defineProperty(selfBrandedDefinitionCopy, key, {
        ...descriptor,
        value: key === sourceDefinitionBrand ? () => selfBrandedDefinitionCopy : descriptor.value,
      });
    }
    Object.freeze(selfBrandedDefinitionCopy);
    const copiedMutation = Object.defineProperties({}, Object.getOwnPropertyDescriptors(mutation));
    const copiedTransition = Object.defineProperties(
      {},
      Object.getOwnPropertyDescriptors(transition),
    );
    const copiedMaintenance = Object.defineProperties(
      {},
      Object.getOwnPropertyDescriptors(maintenance),
    );

    expect(isSourceAdapterHandle(copiedAdapter)).toBe(false);
    expect(isSourceDefinition(copiedDefinition)).toBe(false);
    expect(isSourceDefinition(selfBrandedDefinitionCopy)).toBe(false);
    expect(isSourceMutation(copiedMutation)).toBe(false);
    expect(isSourceApplicationTransition(transition)).toBe(true);
    expect(isSourceApplicationTransition(copiedTransition)).toBe(false);
    expect(isSourceApplicationTransition(null)).toBe(false);
    expect(isSourceApplicationTransition({})).toBe(false);
    expect(
      Reflect.apply(resolveSourceApplicationTransition, undefined, [copiedTransition]),
    ).toBeUndefined();
    expect(isSourceMaintenanceOperation(maintenance)).toBe(true);
    expect(isSourceMaintenanceOperation(copiedMaintenance)).toBe(false);
    expect(isSourceMaintenanceOperation(null)).toBe(false);
    expect(isSourceMaintenanceOperation({})).toBe(false);
    expect(
      Reflect.apply(resolveSourceMaintenanceOperation, undefined, [copiedMaintenance]),
    ).toBeUndefined();
    expect(isSourceAdapterHandle(null)).toBe(false);
    expect(isSourceDefinition({})).toBe(false);
    expect(isSourceDelivery({})).toBe(false);
    expect(isSourceItemRejection({})).toBe(false);
    expect(isSourceAttempt({})).toBe(false);
    expect(isSourceToolkit({})).toBe(false);
  });
});
