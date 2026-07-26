import { describe, expect, it } from "@effect/vitest";
import {
  Chunk,
  Context,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { SourceAdapter } from "./index";
import {
  decodeSourceToolkitUpsert,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceUpsert,
  markSourceToolkit,
} from "./internal";
import type { SourceToolkit } from "./index";
import { SourceAdapterServer, type SourceAdapterServerLifecycle } from "./server";

const Failure = Schema.TaggedStruct("ServerFixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  active: Schema.Boolean,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

const Adapter = SourceAdapter.make({
  identity: { name: "server-fixture" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
    }>(),
  },
  leased: undefined,
});

const toolkit: SourceToolkit<
  { readonly id: string },
  typeof Failure.Type,
  { readonly offset: bigint }
> = markSourceToolkit({
  topic: "orders",
  upsert: (row) => Effect.succeed(makeSourceUpsert<{ readonly id: string }>(row)),
  decodeUpsert: (row: unknown) => Effect.succeed(makeSourceUpsert({ id: String(row) })),
  delete: (id: string) => Effect.succeed(makeSourceDelete(id)),
  delivery: (mutations, settlement) => Effect.succeed(makeSourceDelivery(mutations, settlement)),
  reject: (input) => Effect.succeed(makeSourceItemRejection(input)),
});

const nominalClone = <Value extends object>(
  value: Value,
  overrides: Readonly<Record<string, unknown>>,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(value, property);
    if (descriptor === undefined) {
      continue;
    }
    const next =
      typeof property === "symbol" &&
      "value" in descriptor &&
      typeof descriptor.value === "function"
        ? {
            ...descriptor,
            value: () => clone,
          }
        : typeof property === "string" &&
            Object.hasOwn(overrides, property) &&
            "value" in descriptor
          ? {
              ...descriptor,
              value: overrides[property],
            }
          : descriptor;
    Object.defineProperty(clone, property, next);
  }
  return Object.freeze(clone);
};

const hostileNominalClone = <Value extends object>(
  value: Value,
  propertyToThrow: string,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(value, property);
    if (descriptor === undefined) {
      continue;
    }
    const next =
      typeof property === "symbol" &&
      "value" in descriptor &&
      typeof descriptor.value === "function"
        ? {
            ...descriptor,
            value: () => clone,
          }
        : property === propertyToThrow
          ? {
              configurable: false,
              enumerable: descriptor.enumerable === true,
              get() {
                throw new Error(`hostile ${propertyToThrow}`);
              },
            }
          : descriptor;
    Object.defineProperty(clone, property, next);
  }
  return Object.freeze(clone);
};

describe("Source Adapter server SDK", () => {
  it("collects only nominal definitions belonging to the requested Adapter", () => {
    const definition = Adapter.materializedSource({ label: "orders" });
    const otherAdapter = SourceAdapter.make({
      identity: { name: "other-server-fixture" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });
    const otherDefinition = otherAdapter.materializedSource(undefined);
    let sourceAccessorCalls = 0;
    const accessorTopic = {};
    Object.defineProperty(accessorTopic, "source", {
      enumerable: true,
      get: () => {
        sourceAccessorCalls += 1;
        return definition;
      },
    });
    let topicAccessorCalls = 0;
    const topics = {
      orders: { source: definition },
      other: { source: otherDefinition },
      absent: {},
      sourceFree: { schema: "source-free" },
      primitive: "source-free",
      accessor: accessorTopic,
      forged: { source: { adapter: Adapter } },
    };
    Object.defineProperty(topics, "topicAccessor", {
      enumerable: true,
      get: () => {
        topicAccessorCalls += 1;
        return { source: definition };
      },
    });
    Object.defineProperty(topics, "hidden", {
      enumerable: false,
      value: { source: definition },
    });
    const definitions = SourceAdapterServer.definitions(
      {
        topics,
      },
      Adapter,
    );

    expect(definitions).toStrictEqual([
      {
        topic: "orders",
        definition,
      },
    ]);
    expect(Object.isFrozen(definitions)).toBe(true);
    expect(sourceAccessorCalls).toBe(0);
    expect(topicAccessorCalls).toBe(0);
  });

  it.effect("keeps attempt resources in the caller attempt Scope", () =>
    Effect.gen(function* () {
      let finalized = 0;
      const adapterLayer = SourceAdapterServer.make(Adapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              yield* Scope.addFinalizer(
                yield* Effect.scope,
                Effect.sync(() => {
                  finalized += 1;
                }),
              );
              const mutation = yield* input.toolkit.delete("a");
              yield* decodeSourceToolkitUpsert(input.toolkit, { id: "decoded" });
              const event = yield* input.toolkit.delivery(Chunk.of(mutation));
              const failure = yield* Adapter.failure({
                _tag: "ServerFixtureFailure",
                message: "rejected",
              }).pipe(Effect.orDie);
              const rejection = yield* input.toolkit.reject({
                failure,
                location: { offset: 1n },
                rejectedAtNanos: 2n,
              });
              const events = Stream.make(event, rejection);
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "materialized",
                  events,
                }),
                SourceAdapterServer.lane({
                  id: "sibling",
                  events,
                }),
              ]);
            }),
          metrics: () => Effect.succeed({ active: true }),
          retry: Schedule.recurs(0),
        },
      });
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, Adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      const attempt = yield* materialized
        .acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));

      expect(finalized).toBe(0);
      const events = yield* attempt.lanes[0].events.pipe(Stream.take(2), Stream.runCollect);
      expect(events.map((event) => event._tag)).toStrictEqual([
        "SourceDelivery",
        "SourceItemRejection",
      ]);
      yield* Option.getOrThrow(Option.fromNullishOr(events[1])).settle(Exit.void);
      const retryTermination = yield* Effect.flip(
        materialized.retryDefault(
          Effect.fail({
            _tag: "UnexpectedCompletion",
          }),
          () => Effect.void,
        ),
      );
      expect(retryTermination).toStrictEqual({
        _tag: "UnexpectedCompletion",
      });
      yield* Scope.close(attemptScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
      expect(finalized).toBe(1);
    }),
  );

  it.effect("closes adapter service environments over attempts and settlements", () =>
    Effect.gen(function* () {
      class AdapterDependency extends Context.Service<
        AdapterDependency,
        { readonly value: string }
      >()("@effect-view-server/source-adapter/test/AdapterDependency") {}

      const adapterLayer = SourceAdapterServer.make(Adapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              const dependency = yield* AdapterDependency;
              const mutation = yield* input.toolkit.delete(dependency.value);
              const delivery = yield* input.toolkit.delivery(Chunk.of(mutation), () =>
                AdapterDependency.pipe(Effect.asVoid),
              );
              const failure = yield* Adapter.failure({
                _tag: "ServerFixtureFailure",
                message: "rejected",
              }).pipe(Effect.orDie);
              const rejection = yield* input.toolkit.reject({
                failure,
                location: { offset: 2n },
                rejectedAtNanos: 3n,
                settlement: () => AdapterDependency.pipe(Effect.asVoid),
              });
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "dependency",
                  events: Stream.fromEffect(
                    AdapterDependency.pipe(Effect.as([delivery, rejection])),
                  ).pipe(Stream.flatMap((events) => Stream.fromIterable(events))),
                }),
              ]);
            }),
          metrics: () =>
            AdapterDependency.pipe(
              Effect.map((dependency) => ({
                active: dependency.value.length > 0,
              })),
            ),
          retry: Schedule.recurs(0),
        },
      }).pipe(
        Layer.provide(
          Layer.succeed(AdapterDependency)({
            value: "dependency-id",
          }),
        ),
      );
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, Adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      const lifetimeScope = yield* Scope.make();
      const attempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const events = yield* attempt.lanes[0].events.pipe(Stream.take(2), Stream.runCollect);
      expect(events.map((event) => event._tag)).toStrictEqual([
        "SourceDelivery",
        "SourceItemRejection",
      ]);
      yield* Option.getOrThrow(Option.fromNullishOr(events[0])).settle(Exit.void);
      yield* Option.getOrThrow(Option.fromNullishOr(events[1])).settle(Exit.void);
      expect(
        yield* materialized.metrics({
          topic: "orders",
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({
        active: true,
      });
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("maps hostile adapter output to typed runtime failures", () =>
    Effect.gen(function* () {
      let output:
        | "forged-attempt"
        | "empty-attempt"
        | "hostile-attempt"
        | "forged-event"
        | "forged-rejection"
        | "invalid-inner-mutation"
        | "invalid-rejection-diagnostic"
        | "hostile-event" = "forged-attempt";
      const lifecycle: SourceAdapterServerLifecycle<
        typeof Failure.Type,
        NonNullable<typeof Adapter.materialized>,
        "materialized",
        never
      > = {
        acquire: (input) =>
          Effect.gen(function* () {
            const mutation = yield* input.toolkit.delete("hostile");
            const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
            const forgedDelivery =
              output === "hostile-event"
                ? new Proxy(delivery, {
                    get: (target, property, receiver) => {
                      if (property === "_tag") {
                        throw new Error("hostile event tag");
                      }
                      return Reflect.get(target, property, receiver);
                    },
                  })
                : new Proxy(delivery, {});
            const failure = yield* Adapter.failure({
              _tag: "ServerFixtureFailure",
              message: "type witness",
            }).pipe(Effect.orDie);
            const rejection = yield* input.toolkit.reject({
              failure,
              location: { offset: 1n },
              rejectedAtNanos: 1n,
            });
            const invalidInnerMutation = nominalClone(delivery, {
              mutations: Chunk.of(nominalClone(mutation, { id: 1 })),
            });
            const invalidRejectionDiagnostic = nominalClone(rejection, {
              diagnostic: {
                ...rejection.diagnostic,
                rejectedAtNanos: "invalid",
              },
            });
            const firstEvent =
              output === "forged-rejection"
                ? new Proxy(rejection, {})
                : output === "invalid-inner-mutation"
                  ? invalidInnerMutation
                  : output === "invalid-rejection-diagnostic"
                    ? invalidRejectionDiagnostic
                    : forgedDelivery;
            const lane = SourceAdapterServer.lane({
              id: "hostile",
              events: Stream.make(firstEvent, rejection),
            });
            const validAttempt = SourceAdapterServer.attempt([lane]);
            const hostileAttempt = hostileNominalClone(validAttempt, "lanes");
            return output === "forged-attempt"
              ? new Proxy(validAttempt, {})
              : output === "empty-attempt"
                ? nominalClone(validAttempt, { lanes: [] })
                : output === "hostile-attempt"
                  ? hostileAttempt
                  : validAttempt;
          }),
        metrics: () => Effect.succeed({ active: true }),
        retry: Schedule.recurs(0),
      };
      const adapterLayer = SourceAdapterServer.make(Adapter, {
        materialized: lifecycle,
      });
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, Adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      const lifetimeScope = yield* Scope.make();

      const forgedAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(forgedAttemptFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle returned a structurally forged Source Attempt.",
        },
      });

      output = "empty-attempt";
      const invalidNominalAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(invalidNominalAttemptFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle returned an invalid nominal Source Attempt.",
        },
      });

      output = "hostile-attempt";
      const hostileNominalAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(hostileNominalAttemptFailure).toStrictEqual(invalidNominalAttemptFailure);

      output = "forged-event";
      const closedAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const forgedEventFailure = yield* Effect.flip(
        closedAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(forgedEventFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle emitted a structurally forged Source Lane Event.",
        },
      });

      output = "forged-rejection";
      const forgedRejectionAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const forgedRejectionFailure = yield* Effect.flip(
        forgedRejectionAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(forgedRejectionFailure).toStrictEqual(forgedEventFailure);

      output = "invalid-inner-mutation";
      const invalidInnerMutationAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      expect(
        yield* Effect.flip(
          invalidInnerMutationAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
        ),
      ).toStrictEqual(forgedEventFailure);

      output = "invalid-rejection-diagnostic";
      const invalidRejectionDiagnosticAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      expect(
        yield* Effect.flip(
          invalidRejectionDiagnosticAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
        ),
      ).toStrictEqual(forgedEventFailure);

      output = "hostile-event";
      const hostileEventAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const hostileEventFailure = yield* Effect.flip(
        hostileEventAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(hostileEventFailure).toStrictEqual(forgedEventFailure);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it("rejects missing, extra, or structurally copied runtime linkage", () => {
    const dualAdapter = SourceAdapter.make({
      identity: { name: "dual-server-fixture" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
    });
    const lifecycle = {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "fixture",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ active: true }),
      retry: Schedule.recurs(0),
    };
    const copiedAdapter = Object.defineProperties({}, Object.getOwnPropertyDescriptors(Adapter));

    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        dualAdapter,
        { materialized: lifecycle },
      ]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [dualAdapter, { leased: lifecycle }]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        Adapter,
        { materialized: lifecycle, leased: lifecycle },
      ]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        copiedAdapter,
        { materialized: lifecycle },
      ]),
    ).toThrow("nominal Source Adapter handle");
  });

  it.effect("builds leased-only services and rejects undefined implementations", () =>
    Effect.gen(function* () {
      const leasedAdapter = SourceAdapter.make({
        identity: { name: "leased-only-server-fixture" },
        failure: Failure,
        materialized: undefined,
        leased: {
          metrics: Metrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
      });
      const leasedLayer = SourceAdapterServer.make(leasedAdapter, {
        leased: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "leased",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed({ active: true }),
          retry: Schedule.recurs(0),
        },
      });
      const leasedContext = yield* Effect.scoped(Layer.build(leasedLayer));
      const leasedService = Context.getUnsafe(leasedContext, leasedAdapter.runtimeService);
      expect(leasedService.materialized).toBeUndefined();
      expect(leasedService.leased).toBeDefined();

      const invalidMaterializedLayer: Layer.Layer<never> = Reflect.apply(
        SourceAdapterServer.make,
        undefined,
        [Adapter, { materialized: undefined }],
      );
      const invalidLeasedLayer: Layer.Layer<never> = Reflect.apply(
        SourceAdapterServer.make,
        undefined,
        [leasedAdapter, { leased: undefined }],
      );
      const invalidMaterialized = yield* Effect.exit(
        Effect.scoped(Layer.build(invalidMaterializedLayer)),
      );
      const invalidLeased = yield* Effect.exit(Effect.scoped(Layer.build(invalidLeasedLayer)));
      expect(Exit.isFailure(invalidMaterialized)).toBe(true);
      expect(Exit.isFailure(invalidLeased)).toBe(true);
    }),
  );

  it.effect("validates lane IDs and supplies unbuffered metrics by default", () =>
    Effect.gen(function* () {
      expect(() =>
        SourceAdapterServer.lane({
          id: "",
          events: Stream.never,
        }),
      ).toThrow("must be non-empty");

      const lane = SourceAdapterServer.lane({
        id: "fixture",
        events: Stream.never,
      });
      expect(yield* lane.bufferMetrics).toStrictEqual({
        _tag: "Unbuffered",
      });
      const fiber = yield* lane.events.pipe(Stream.runDrain, Effect.forkChild);
      yield* Fiber.interrupt(fiber);
    }),
  );
});
