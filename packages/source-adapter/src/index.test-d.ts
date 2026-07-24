import { describe, expectTypeOf, it } from "@effect/vitest";
import {
  SourceAdapter,
  type SourceApplicationExit,
  type SourceAdapterFailure,
  type SourceAdapterHandle,
  type SourceAttempt,
  type SourceDefinitionOptionsFamily,
  type SourceDefinitionRow,
  type SourceDelivery,
  type SourceDeliveryLane,
  type SourceDefinitionLifecycle,
  type SourceDefinitionOptions,
  type SourceDefinitionRouteFields,
  type SourceExecutionFailure,
  type SourceLifecycleDeclaration,
  type SourceMutation,
  type SourceToolkit,
} from "./index";
import {
  SourceAdapterServer,
  SourceBuffer,
  type BackpressurableSourceBuffer,
  type NonPausableSourceBuffer,
  type SourceAdapterServerLifecycle,
} from "./server";
import { Chunk, Context, Effect, Layer, Schedule, Schema, Scope, Stream } from "effect";

const Failure = Schema.TaggedStruct("TypeFixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  connected: Schema.Boolean,
});
const Location = Schema.Struct({
  partition: Schema.Number,
  offset: Schema.BigInt,
});

type MappedDefinitionOptions<Row extends object> = {
  readonly initial: Row;
};

interface MappedDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: MappedDefinitionOptions<this["Row"]>;
}

interface ErasedAnyDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: any;
}

interface ErasedUnknownDefinitionOptionsFamily extends SourceDefinitionOptionsFamily {
  readonly type: unknown;
}

const mappedAdapter = SourceAdapter.make({
  identity: { name: "mapped-type-fixture" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptionsFamily<MappedDefinitionOptionsFamily>(),
  },
  leased: undefined,
});
const mappedDefinition = mappedAdapter.materializedSource<{
  readonly id: string;
  readonly value: number;
}>({
  initial: {
    id: "initial",
    value: 1,
  },
});

const adapter = SourceAdapter.make({
  identity: {
    name: "type-fixture",
    version: "1",
  },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
      readonly batchSize: number;
    }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
      readonly batchSize: number;
    }>(),
  },
});

const materialized = adapter.materializedSource({
  label: "orders",
  batchSize: 100,
});
const leased = adapter.leasedSource(["region", "desk"], {
  label: "orders",
  batchSize: 100,
});

class AdapterDependency extends Context.Service<
  AdapterDependency,
  { readonly connected: boolean }
>()("@effect-view-server/source-adapter/type-test/AdapterDependency") {}

const materializedLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.materialized>,
  "materialized",
  AdapterDependency
> = {
  acquire: (input) => {
    expectTypeOf(input.definition).toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(input.target).toEqualTypeOf<{
      readonly _tag: "Materialized";
    }>();
    return Effect.succeed(
      SourceAdapterServer.attempt([
        SourceAdapterServer.lane({
          id: "materialized",
          events: Stream.never,
        }),
      ]),
    );
  },
  metrics: (input) => {
    void input.topic;
    expectTypeOf(input.definition).toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(input.target).toEqualTypeOf<{
      readonly _tag: "Materialized";
    }>();
    return AdapterDependency.pipe(
      Effect.map((dependency) => ({
        connected: dependency.connected,
      })),
    );
  },
  retry: Schedule.recurs(0),
};

const leasedLifecycle: SourceAdapterServerLifecycle<
  typeof Failure.Type,
  NonNullable<typeof adapter.leased>,
  "leased",
  AdapterDependency
> = {
  acquire: (input) => {
    expectTypeOf(input.target._tag).toEqualTypeOf<"Leased">();
    void input.target.route;
    return Effect.succeed(
      SourceAdapterServer.attempt([
        SourceAdapterServer.lane({
          id: "leased",
          events: Stream.never,
        }),
      ]),
    );
  },
  metrics: (input) => {
    void input.topic;
    expectTypeOf(input.target._tag).toEqualTypeOf<"Leased">();
    void input.target.route;
    return AdapterDependency.pipe(
      Effect.map((dependency) => ({
        connected: dependency.connected,
      })),
    );
  },
  retry: Schedule.recurs(0),
};

const serverLayer = SourceAdapterServer.make(adapter, {
  materialized: materializedLifecycle,
  leased: leasedLifecycle,
});

const mappedServerLayer = SourceAdapterServer.make(mappedAdapter, {
  materialized: {
    acquire: (input) =>
      Effect.gen(function* () {
        const mutation = yield* input.toolkit.upsert(input.definition.initial);
        const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
        return SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "mapped",
            events: Stream.make(delivery).pipe(Stream.concat(Stream.never)),
          }),
        ]);
      }),
    metrics: () => Effect.succeed({ connected: true }),
    retry: Schedule.recurs(0),
  },
});

declare const topicToolkit: SourceToolkit<
  {
    readonly id: string;
    readonly value: number;
  },
  typeof Failure.Type,
  typeof Location.Type,
  never,
  "orders"
>;
declare const settlementToolkit: SourceToolkit<
  {
    readonly id: string;
    readonly value: number;
  },
  typeof Failure.Type,
  typeof Location.Type,
  AdapterDependency,
  "orders"
>;
declare const wrongSettlement: (
  exit: SourceApplicationExit,
) => Effect.Effect<void, string, AdapterDependency>;
declare const typedMutation: SourceMutation<{
  readonly id: string;
  readonly value: number;
}>;
declare const extraFieldMutation: SourceMutation<{
  readonly id: string;
  readonly value: number;
  readonly extra: boolean;
}>;
type ErasedAnyLifecycle = SourceLifecycleDeclaration<
  typeof Metrics.Type,
  typeof Location.Type,
  { readonly label: string },
  ErasedAnyDefinitionOptionsFamily
>;
type ErasedUnknownLifecycle = SourceLifecycleDeclaration<
  typeof Metrics.Type,
  typeof Location.Type,
  { readonly label: string },
  ErasedUnknownDefinitionOptionsFamily
>;
declare const erasedAnyAdapter: SourceAdapterHandle<
  "erased-any",
  undefined,
  typeof Failure.Type,
  ErasedAnyLifecycle,
  undefined
>;
declare const erasedUnknownAdapter: SourceAdapterHandle<
  "erased-unknown",
  undefined,
  typeof Failure.Type,
  ErasedUnknownLifecycle,
  undefined
>;

describe("Source Adapter public type contracts", () => {
  it("preserves exact declaration and definition inference without as const", () => {
    expectTypeOf(adapter.identity.name).toEqualTypeOf<"type-fixture">();
    expectTypeOf(adapter.identity.version).toEqualTypeOf<"1" | undefined>();
    expectTypeOf<SourceAdapterFailure<typeof adapter>>().toEqualTypeOf<typeof Failure.Type>();
    expectTypeOf<SourceDefinitionLifecycle<typeof materialized>>().toEqualTypeOf<"materialized">();
    expectTypeOf<SourceDefinitionLifecycle<typeof leased>>().toEqualTypeOf<"leased">();
    expectTypeOf<SourceDefinitionRouteFields<typeof leased>>().toEqualTypeOf<
      readonly ["region", "desk"]
    >();
    expectTypeOf<SourceDefinitionOptions<typeof leased>>().toEqualTypeOf<{
      readonly label: string;
      readonly batchSize: number;
    }>();
    expectTypeOf(serverLayer).toEqualTypeOf<
      Layer.Layer<
        Context.Service.Identifier<typeof adapter.runtimeService>,
        never,
        AdapterDependency
      >
    >();
    expectTypeOf(serverLayer).not.toBeAny();
    expectTypeOf<SourceDefinitionRow<typeof mappedDefinition>>().toEqualTypeOf<{
      readonly id: string;
      readonly value: number;
    }>();
    expectTypeOf(mappedServerLayer).not.toBeAny();
  });

  it("rejects invalid portable declarations and definitions", () => {
    // @ts-expect-error a Source Adapter must declare at least one lifecycle.
    SourceAdapter.make({
      identity: { name: "empty" },
      failure: Failure,
      materialized: undefined,
      leased: undefined,
    });

    // @ts-expect-error lifecycle declarations are exact.
    SourceAdapter.make({
      identity: { name: "extra-lifecycle-field" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
        unexpected: true,
      },
      leased: undefined,
    });

    // @ts-expect-error definition options must include every adapter field.
    adapter.materializedSource({
      label: "orders",
    });
    // @ts-expect-error definition options reject extra fields.
    adapter.materializedSource({
      label: "orders",
      batchSize: 100,
      unexpected: true,
    });
    // @ts-expect-error Leased Source routes must be non-empty tuples.
    adapter.leasedSource([], {
      label: "orders",
      batchSize: 100,
    });

    // @ts-expect-error adapter failures may not erase to any.
    SourceAdapter.make({
      identity: { name: "unsafe-failure" },
      failure: Schema.Any,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error adapter metrics may not erase to any.
    SourceAdapter.make({
      identity: { name: "unsafe-metrics" },
      failure: Failure,
      materialized: {
        metrics: Schema.Any,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error rejection locations may not erase to unknown.
    SourceAdapter.make({
      identity: { name: "unsafe-location" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Schema.Unknown,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });

    // @ts-expect-error Source Definition options may not erase to any.
    SourceAdapter.definitionOptions<any>();
    // @ts-expect-error Source Definition options may not erase to unknown.
    SourceAdapter.definitionOptions<unknown>();
    // @ts-expect-error Source Definition option families may not erase to any.
    SourceAdapter.definitionOptionsFamily<any, { readonly label: string }>();
    // @ts-expect-error evaluated Source Definition option families may not erase to any.
    SourceAdapter.definitionOptionsFamily<ErasedAnyDefinitionOptionsFamily>();
    // @ts-expect-error explicit token options cannot mask an any-valued family.
    SourceAdapter.definitionOptionsFamily<
      ErasedAnyDefinitionOptionsFamily,
      { readonly label: string }
    >();
    // @ts-expect-error evaluated Source Definition option families may not erase to unknown.
    SourceAdapter.definitionOptionsFamily<ErasedUnknownDefinitionOptionsFamily>();
    // @ts-expect-error explicit token options cannot mask an unknown-valued family.
    SourceAdapter.definitionOptionsFamily<
      ErasedUnknownDefinitionOptionsFamily,
      { readonly label: string }
    >();
    // @ts-expect-error Materialized Source options reject an any-valued evaluated family.
    erasedAnyAdapter.materializedSource<{ readonly id: string }>({ totallyWrong: true });
    // @ts-expect-error Materialized Source options reject an unknown-valued evaluated family.
    erasedUnknownAdapter.materializedSource<{ readonly id: string }>({ totallyWrong: true });
    const unsafeMaterializedRow = adapter.materializedSource<any>({
      label: "unsafe-row",
      batchSize: 100,
    });
    expectTypeOf<SourceDefinitionRow<typeof unsafeMaterializedRow>>().toBeNever();
    const unsafeLeasedRow = adapter.leasedSource<readonly ["region"], any>(["region"], {
      label: "unsafe-row",
      batchSize: 100,
    });
    expectTypeOf<SourceDefinitionRow<typeof unsafeLeasedRow>>().toBeNever();
  });

  it("keeps the Topic-Bound Toolkit exact", () => {
    expectTypeOf(topicToolkit.topic).toEqualTypeOf<"orders">();
    const upsert = topicToolkit.upsert({
      id: "a",
      value: 1,
    });
    expectTypeOf<Effect.Success<typeof upsert>["row"]>().toEqualTypeOf<{
      readonly id: string;
      readonly value: number;
    }>();
    const extraRow = {
      id: "variable-extra",
      value: 1,
      extra: true,
    };
    const operations = [
      // @ts-expect-error Topic-Bound upserts require every row field.
      topicToolkit.upsert({ id: "missing-value" }),
      // @ts-expect-error Topic-Bound upserts reject unknown fields.
      topicToolkit.upsert({ id: "extra", value: 1, extra: true }),
      // @ts-expect-error Topic-Bound upserts reject extra fields carried by variables.
      topicToolkit.upsert(extraRow),
      // @ts-expect-error Topic-Bound upserts preserve field types.
      topicToolkit.upsert({ id: "wrong", value: "1" }),
      // @ts-expect-error canonical deletes require string ids.
      topicToolkit.delete(1),
      topicToolkit.reject({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "TypeFixtureFailure",
            message: "rejected",
          },
        },
        location: {
          partition: 1,
          offset: 2n,
        },
        rejectedAtNanos: 3n,
      }),
      topicToolkit.reject({
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "TypeFixtureFailure",
            message: "rejected",
          },
        },
        // @ts-expect-error rejection locations are exact.
        location: { partition: 1 },
        rejectedAtNanos: 3n,
      }),
    ];
    expectTypeOf(operations).not.toBeAny();
  });

  it("preserves Delivery, lane, attempt, and settlement generics", () => {
    type Row = {
      readonly id: string;
      readonly value: number;
    };

    const settlement = (_exit: SourceApplicationExit) => AdapterDependency.pipe(Effect.asVoid);
    const delivery = settlementToolkit
      .upsert({ id: "typed", value: 1 })
      .pipe(
        Effect.flatMap((mutation) => settlementToolkit.delivery(Chunk.of(mutation), settlement)),
      );
    expectTypeOf<Effect.Success<typeof delivery>>().toEqualTypeOf<
      SourceDelivery<Row, typeof Failure.Type, AdapterDependency>
    >();
    expectTypeOf<Effect.Error<typeof delivery>>().toEqualTypeOf<
      SourceExecutionFailure<typeof Failure.Type>
    >();

    const rejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      settlement,
    });
    const events = AdapterDependency.pipe(
      Effect.as(
        Stream.fromEffect(delivery).pipe(
          Stream.concat(Stream.fromEffect(rejection)),
          Stream.concat(Stream.never),
        ),
      ),
      Stream.unwrap,
    );
    const lane = SourceAdapterServer.lane({
      id: "typed",
      events,
    });
    expectTypeOf(lane).toEqualTypeOf<
      SourceDeliveryLane<Row, typeof Failure.Type, typeof Location.Type, AdapterDependency>
    >();
    const attempt = SourceAdapterServer.attempt([lane]);
    expectTypeOf(attempt).toEqualTypeOf<
      SourceAttempt<Row, typeof Failure.Type, typeof Location.Type, AdapterDependency>
    >();

    // @ts-expect-error Source Attempts require at least one lane.
    SourceAdapterServer.attempt([]);
    // @ts-expect-error Deliveries require a non-empty mutation Chunk.
    const invalidEmptyDelivery = settlementToolkit.delivery(Chunk.empty(), settlement);
    expectTypeOf(invalidEmptyDelivery).not.toBeAny();
    // These deliberately invalid Effect error channels are the contract under test.
    // @effect-diagnostics missingEffectError:off
    const invalidSettlementDelivery = settlementToolkit.delivery(
      Chunk.of(typedMutation),
      // @ts-expect-error Delivery settlements preserve the Adapter Failure channel.
      wrongSettlement,
    );
    expectTypeOf(invalidSettlementDelivery).not.toBeAny();
    const invalidSettlementRejection = settlementToolkit.reject({
      failure: {
        _tag: "AdapterFailure",
        failure: {
          _tag: "TypeFixtureFailure",
          message: "rejected",
        },
      },
      location: {
        partition: 1,
        offset: 1n,
      },
      rejectedAtNanos: 1n,
      // @ts-expect-error Rejection settlements preserve the Adapter Failure channel.
      settlement: wrongSettlement,
    });
    expectTypeOf(invalidSettlementRejection).not.toBeAny();
    // @effect-diagnostics missingEffectError:on
    const invalidMutationDelivery = settlementToolkit.delivery(
      // @ts-expect-error Delivery mutations must use the Toolkit's exact Topic Row.
      Chunk.of(extraFieldMutation),
      settlement,
    );
    expectTypeOf(invalidMutationDelivery).not.toBeAny();
  });

  it("preserves both finite Source Buffer constructor contracts", () => {
    type Value = {
      readonly id: string;
    };
    const backpressurable = SourceBuffer.backpressurable<
      Value,
      typeof Failure.Type,
      AdapterDependency
    >({
      capacity: 2,
      register: (emit) =>
        AdapterDependency.pipe(
          Effect.map(() => {
            const accepted = emit({ id: "one" });
            expectTypeOf(accepted).toEqualTypeOf<Effect.Effect<void>>();
            return Effect.void;
          }),
        ),
    });
    expectTypeOf<Effect.Success<typeof backpressurable>>().toEqualTypeOf<
      BackpressurableSourceBuffer<Value>
    >();
    expectTypeOf<Effect.Error<typeof backpressurable>>().toEqualTypeOf<typeof Failure.Type>();
    expectTypeOf<Effect.Services<typeof backpressurable>>().toEqualTypeOf<
      AdapterDependency | Scope.Scope
    >();

    const nonPausable = SourceBuffer.nonPausable<Value>({
      capacity: 2,
      register: (emit) =>
        Effect.sync(() => {
          expectTypeOf(emit({ id: "one" })).toEqualTypeOf<void>();
          return Effect.void;
        }),
    });
    expectTypeOf<Effect.Success<typeof nonPausable>>().toEqualTypeOf<
      NonPausableSourceBuffer<Value>
    >();

    const invalidBackpressurable = SourceBuffer.backpressurable<Value>({
      capacity: 1,
      register: (emit) =>
        // @ts-expect-error backpressurable emitters preserve the Value type.
        emit({ id: 1 }).pipe(Effect.as(Effect.void)),
    });
    const invalidNonPausable = SourceBuffer.nonPausable<Value>({
      capacity: 1,
      register: (emit) =>
        Effect.sync(() => {
          // @ts-expect-error non-pausable emitters preserve the Value type.
          emit({ id: 1 });
          return Effect.void;
        }),
    });
    expectTypeOf(invalidBackpressurable).not.toBeAny();
    expectTypeOf(invalidNonPausable).not.toBeAny();
  });

  it("requires exactly the declared server implementations", () => {
    // @ts-expect-error every declared lifecycle requires one implementation.
    SourceAdapterServer.make(adapter, {
      materialized: materializedLifecycle,
    });
    SourceAdapterServer.make(adapter, {
      materialized: materializedLifecycle,
      leased: leasedLifecycle,
      // @ts-expect-error server implementation records are exact.
      unexpected: leasedLifecycle,
    });
    SourceAdapterServer.make(
      // @ts-expect-error structurally similar values are not nominal handles.
      {
        identity: adapter.identity,
      },
      {
        materialized: materializedLifecycle,
        leased: leasedLifecycle,
      },
    );
  });
});
