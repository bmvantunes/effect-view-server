import { describe, expect, it } from "@effect/vitest";
import { Chunk, Effect, Exit, Option, Schedule, Schema, Stream } from "effect";
import type { SourceBufferMetrics, SourceDefinitionOptionsFamily } from "./index";
import {
  decodeSourceToolkitUpsert,
  isSourceAdapterHandle,
  isSourceAttempt,
  isSourceDefinition,
  isSourceDelivery,
  isSourceItemRejection,
  isSourceMutation,
  isSourceToolkit,
  makeRuntimeSourceFailure,
  makeSourceAttempt,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceUpsert,
  markSourceToolkit,
} from "./internal";
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
      ...(input.nonEnumerable?.has(property) === true ? { enumerable: false } : {}),
      ...("value" in descriptor
        ? {
            value:
              typeof property === "symbol" && typeof descriptor.value === "function"
                ? () => clone
                : input.overrides?.has(property) === true
                  ? input.overrides.get(property)
                  : descriptor.value,
          }
        : {}),
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
    expect(definition.options.row).toBe(Row);
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

    const cyclic: { self?: unknown } = {};
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
    const frozenOption = new FrozenOption();
    Reflect.apply(adapter.materializedSource, adapter, [frozenOption]);
    expect(Object.isFrozen(frozenOption)).toBe(true);
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
    expect(invalidValues.map(isSourceDefinition)).toStrictEqual(invalidValues.map(() => false));
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
        decodeUpsert: (row: unknown) => Effect.succeed(makeSourceUpsert({ row })),
        delete: (id: string) => Effect.succeed(makeSourceDelete(id)),
        delivery: (mutations) => Effect.succeed(makeSourceDelivery(mutations)),
        reject: (input) => Effect.succeed(makeSourceItemRejection(input)),
      });

      expect(isSourceMutation(upsert)).toBe(true);
      expect(isSourceMutation(deletion)).toBe(true);
      expect(isSourceDelivery(delivery)).toBe(true);
      expect(isSourceItemRejection(rejection)).toBe(true);
      expect(isSourceAttempt(attempt)).toBe(true);
      expect(isSourceToolkit(toolkit)).toBe(true);
      expect(Object.hasOwn(toolkit, "decodeUpsert")).toBe(false);
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
    const copiedAdapter = Object.defineProperties({}, Object.getOwnPropertyDescriptors(adapter));
    const copiedDefinition = Object.defineProperties(
      {},
      Object.getOwnPropertyDescriptors(definition),
    );
    const copiedMutation = Object.defineProperties({}, Object.getOwnPropertyDescriptors(mutation));

    expect(isSourceAdapterHandle(copiedAdapter)).toBe(false);
    expect(isSourceDefinition(copiedDefinition)).toBe(false);
    expect(isSourceMutation(copiedMutation)).toBe(false);
    expect(isSourceAdapterHandle(null)).toBe(false);
    expect(isSourceDefinition({})).toBe(false);
    expect(isSourceDelivery({})).toBe(false);
    expect(isSourceItemRejection({})).toBe(false);
    expect(isSourceAttempt({})).toBe(false);
    expect(isSourceToolkit({})).toBe(false);
  });
});
