import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceAdapter,
  type SourceDeliveryLane,
  type SourceExecutionFailure,
  type SourceRuntimeFailure,
  type SourceStatus,
} from "@effect-view-server/source-adapter";
import {
  decodeSourceToolkitUpsert,
  makeRuntimeSourceFailure,
  makeSourceApplicationTransition,
  makeSourceDelivery,
} from "@effect-view-server/source-adapter/internal";
import {
  SourceFixture,
  type ControllableSourceFixture,
  type SourceFixtureFailure,
} from "@effect-view-server/source-adapter-testing";
import {
  BigDecimal,
  Cause,
  Chunk,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  SchemaGetter,
  SchemaIssue,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";
import type { ViewServerRuntimeCoreInternalMutations } from "./source-mutation-pipeline";
import { makeRuntimeCoreSourceManager } from "./source-runtime";

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
  value: Schema.String,
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

const awaitExhausted = Effect.fn("RuntimeCore.sourceAdversarial.awaitExhausted")(function* <
  EventError,
  EventServices,
  CloseError,
  CloseServices,
>(diagnostics: {
  readonly events: Stream.Stream<
    {
      readonly status: SourceStatus<unknown, unknown>;
    },
    EventError,
    EventServices
  >;
  readonly close: () => Effect.Effect<void, CloseError, CloseServices>;
}) {
  const result = yield* diagnostics.events.pipe(
    Stream.filter((current) => current.status._tag === "Exhausted"),
    Stream.take(1),
    Stream.runHead,
  );
  yield* diagnostics.close();
  const status = Option.getOrThrow(result).status;
  if (status._tag !== "Exhausted") {
    return yield* Effect.die("Expected exhausted Source status.");
  }
  return status;
});

type Fixture = ControllableSourceFixture<typeof Row.Type>;
type FixtureRuntimeService = Context.Service.Shape<Fixture["adapter"]["runtimeService"]>;

const materializedLifecycle = (service: FixtureRuntimeService) =>
  Option.getOrThrow(Option.fromUndefinedOr(service.materialized));

const invokeHostile = <Operation extends (...arguments_: ReadonlyArray<never>) => unknown>(
  operation: Operation,
  receiver: unknown,
  arguments_: ReadonlyArray<unknown>,
): ReturnType<Operation> => Reflect.apply(operation, receiver, arguments_);

const invalidRuntimeFailure = () =>
  new Proxy(
    makeRuntimeSourceFailure({
      _tag: "InvalidSourceDelivery",
      message: "valid",
    }),
    {
      get: (target, property, receiver) =>
        property === "failure"
          ? {
              _tag: "NotASourceRuntimeFailure",
              message: "invalid",
            }
          : Reflect.get(target, property, receiver),
    },
  );

const withChangingLaneId = <Row extends object, AdapterFailure, RejectionLocation>(
  lane: SourceDeliveryLane<Row, AdapterFailure, RejectionLocation>,
): SourceDeliveryLane<Row, AdapterFailure, RejectionLocation> => {
  const mutable: SourceDeliveryLane<Row, AdapterFailure, RejectionLocation> = {
    id: lane.id,
    events: lane.events,
    bufferMetrics: lane.bufferMetrics,
  };
  let idReads = 0;
  return new Proxy(mutable, {
    get: (target, property, receiver) => {
      if (property !== "id") {
        return Reflect.get(target, property, receiver);
      }
      idReads += 1;
      return idReads <= 3 ? "registered" : "unregistered";
    },
  });
};

describe("Runtime Core adversarial Source runtime", () => {
  it.effect("keeps invalid initial Source metrics inside Leased supervision", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(["region"], {
              label: "invalid-initial-metrics",
            }),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const leased = Option.getOrThrow(Option.fromUndefinedOr(service.leased));
      const lifecycle = new Proxy(leased, {
        get: (target, property, receiver) =>
          property === "metrics"
            ? () => Effect.succeed({ observed: "invalid" })
            : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          leased: lifecycle,
        }),
      );

      const subscription = yield* runtime.liveClient.subscribe("rows", {
        routeBy: { region: "eu" },
        select: ["id", "region"],
      });
      expect((yield* runtime.client.health()).sources).toStrictEqual({ rows: [] });
      yield* subscription.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "maps encode, re-decode, and post-decode freeze failures into typed metrics errors",
    () =>
      Effect.gen(function* () {
        const MetricsValue = Schema.Struct({
          value: Schema.Unknown,
        });
        const MetricsFailure = Schema.TaggedStruct("MetricsStageFailure", {
          message: Schema.String,
        });
        const MetricsLocation = Schema.Struct({
          offset: Schema.BigInt,
        });
        const schemaFailure = (value: { readonly value: unknown }) =>
          new SchemaIssue.Forbidden(
            {
              message: "deliberate metrics-stage failure",
            },
            value,
          );
        let encodeCount = 0;
        const encodeFailure = MetricsValue.pipe(
          Schema.decodeTo(MetricsValue, {
            decode: SchemaGetter.transform((value) => value),
            encode: SchemaGetter.transformOrFail((value) => {
              encodeCount += 1;
              return encodeCount === 2 ? Effect.fail(schemaFailure(value)) : Effect.succeed(value);
            }),
          }),
        );
        let decodeCount = 0;
        const reDecodeFailure = MetricsValue.pipe(
          Schema.decodeTo(MetricsValue, {
            decode: SchemaGetter.transformOrFail((value) => {
              decodeCount += 1;
              return decodeCount === 4 ? Effect.fail(schemaFailure(value)) : Effect.succeed(value);
            }),
            encode: SchemaGetter.transform((value) => value),
          }),
        );
        let hostileDecodeCount = 0;
        const cyclic: Array<unknown> = [];
        cyclic.push(cyclic);
        const postDecodeFreezeFailure = MetricsValue.pipe(
          Schema.decodeTo(MetricsValue, {
            decode: SchemaGetter.transform((value) => {
              hostileDecodeCount += 1;
              return hostileDecodeCount === 4 ? { value: cyclic } : value;
            }),
            encode: SchemaGetter.transform((value) => value),
          }),
        );

        for (const [name, metrics] of [
          ["metrics-encode-failure", encodeFailure],
          ["metrics-redecode-failure", reDecodeFailure],
          ["metrics-freeze-failure", postDecodeFreezeFailure],
        ] as const) {
          const adapter = SourceAdapter.make({
            identity: { name },
            failure: MetricsFailure,
            materialized: {
              metrics,
              rejectionLocation: MetricsLocation,
              definitionOptions: SourceAdapter.definitionOptions<void>(),
            },
            leased: undefined,
          });
          const layer = SourceAdapterServer.make(adapter, {
            materialized: {
              acquire: () =>
                Effect.succeed(
                  SourceAdapterServer.attempt([
                    SourceAdapterServer.lane({
                      id: name,
                      events: Stream.never,
                    }),
                  ]),
                ),
              metrics: () => Effect.succeed({ value: "valid" }),
              retry: Schedule.recurs(0),
            },
          });
          const config = defineViewServerConfig({
            topics: {
              rows: {
                schema: Row,
                source: adapter.materializedSource(undefined),
              },
            },
          });
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
          const readyDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
            topic: "rows",
          });
          expect(
            Option.getOrThrow(
              yield* readyDiagnostics.events.pipe(
                Stream.filter((health) => health.status._tag === "Ready"),
                Stream.take(1),
                Stream.runHead,
              ),
            ).status._tag,
          ).toBe("Ready");
          yield* readyDiagnostics.close();
          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
          const exhausted = yield* awaitExhausted(diagnostics).pipe(Effect.forkChild);
          yield* TestClock.adjust("1 second");
          expect((yield* Fiber.join(exhausted)).exhaustion.lastTermination).toStrictEqual({
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceMetrics",
                message: `Source Adapter ${name} returned metrics that cannot be frozen.`,
              },
            },
          });
          yield* runtime.close;
        }
      }),
  );

  it.effect("rejects accessor, class, Date, and symbol-keyed adapter metric values", () =>
    Effect.gen(function* () {
      const OpaqueFailure = Schema.TaggedStruct("OpaqueMetricsFailure", {
        message: Schema.String,
      });
      const OpaqueLocation = Schema.Struct({
        offset: Schema.BigInt,
      });
      const adapter = SourceAdapter.make({
        identity: {
          name: "opaque-metrics",
        },
        failure: OpaqueFailure,
        materialized: {
          metrics: Schema.Struct({
            value: Schema.Unknown,
          }),
          rejectionLocation: OpaqueLocation,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      let currentMetrics: { readonly value: unknown } = {
        value: {},
      };
      const layer = SourceAdapterServer.make(adapter, {
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "opaque-metrics",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.sync(() => currentMetrics),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: adapter.materializedSource(undefined),
          },
        },
      });
      class MetricClass {
        readonly value = 1;
      }
      const accessorMetric = Object.defineProperty({}, "value", {
        enumerable: true,
        get: () => 1,
      });
      const symbolMetric = {
        [Symbol("metric")]: 1,
      };
      const decimalMetric = BigDecimal.make(123n, 2);

      for (const invalidMetrics of [
        accessorMetric,
        new MetricClass(),
        Reflect.construct(Date, [0]),
        symbolMetric,
      ]) {
        currentMetrics = {
          value: decimalMetric,
        };
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
        const readyDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
        const ready = Option.getOrThrow(
          yield* readyDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        expect(ready.metrics.adapter.value === decimalMetric).toBe(false);
        expect(Object.isFrozen(ready.metrics.adapter.value)).toBe(true);
        yield* readyDiagnostics.close();
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
        const exhausted = yield* awaitExhausted(diagnostics).pipe(Effect.forkChild);
        currentMetrics = {
          value: invalidMetrics,
        };
        yield* TestClock.adjust("1 second");
        expect((yield* Fiber.join(exhausted)).exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceMetrics",
              message: "Source Adapter opaque-metrics returned metrics that cannot be frozen.",
            },
          },
        });
        expect(Object.isFrozen(invalidMetrics)).toBe(false);
        yield* runtime.close;
      }
    }),
  );

  it.effect("snapshots metrics explicitly admitted by a Schema Class", () =>
    Effect.gen(function* () {
      class DeclaredMetrics extends Schema.Class<DeclaredMetrics>("DeclaredMetrics")({
        observed: Schema.BigInt,
      }) {}
      const Failure = Schema.TaggedStruct("DeclaredMetricsFailure", {
        message: Schema.String,
      });
      const Location = Schema.Struct({
        offset: Schema.BigInt,
      });
      const adapter = SourceAdapter.make({
        identity: {
          name: "declared-class-metrics",
        },
        failure: Failure,
        materialized: {
          metrics: DeclaredMetrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const supplied = new DeclaredMetrics({ observed: 7n });
      const layer = SourceAdapterServer.make(adapter, {
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "declared-class-metrics",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed(supplied),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: adapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );

      expect({
        copied: health.metrics.adapter === supplied,
        frozen: Object.isFrozen(health.metrics.adapter),
        instance: health.metrics.adapter instanceof DeclaredMetrics,
        observed: health.metrics.adapter.observed,
      }).toStrictEqual({
        copied: false,
        frozen: true,
        instance: true,
        observed: 7n,
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("supervises cyclic adapter metric arrays and objects as invalid metrics", () =>
    Effect.gen(function* () {
      const CyclicFailure = Schema.TaggedStruct("CyclicFailure", {
        message: Schema.String,
      });
      const CyclicLocation = Schema.Struct({
        offset: Schema.BigInt,
      });
      const adapter = SourceAdapter.make({
        identity: {
          name: "cyclic-metrics",
        },
        failure: CyclicFailure,
        materialized: {
          metrics: Schema.Struct({
            nested: Schema.Unknown,
          }),
          rejectionLocation: CyclicLocation,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      let currentMetrics: { readonly nested: unknown } = {
        nested: {},
      };
      const layer = SourceAdapterServer.make(adapter, {
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "cyclic-metrics",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.sync(() => currentMetrics),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: adapter.materializedSource(undefined),
          },
        },
      });
      const cyclicArray: Array<object> = [];
      cyclicArray.push(cyclicArray);
      const cyclicObject: { self?: object } = {};
      cyclicObject.self = cyclicObject;

      for (const cyclic of [cyclicArray, cyclicObject]) {
        currentMetrics = {
          nested: {},
        };
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
        const exhaustedFiber = yield* awaitExhausted(diagnostics).pipe(Effect.forkChild);
        currentMetrics = {
          nested: cyclic,
        };
        yield* TestClock.adjust("1 second");
        const exhausted = yield* Fiber.join(exhaustedFiber);
        expect(exhausted.exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceMetrics",
              message: "Source Adapter cyclic-metrics returned metrics that cannot be frozen.",
            },
          },
        });
        yield* runtime.close;
      }
    }),
  );

  it.effect("does not let a delayed metric sample overwrite sticky degraded health", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "serialized-health-publication",
            }),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const delayedSampleStarted = yield* Deferred.make<void>();
      const releaseDelayedSample = yield* Deferred.make<void>();
      let metricReads = 0;
      const metrics: typeof materialized.metrics = () =>
        Effect.suspend(() => {
          metricReads += 1;
          return metricReads === 1
            ? Effect.succeed({ observed: 1n })
            : Deferred.succeed(delayedSampleStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseDelayedSample)),
                Effect.as({ observed: 2n }),
              );
        });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: new Proxy(materialized, {
            get: (target, property, receiver) =>
              property === "metrics" ? metrics : Reflect.get(target, property, receiver),
          }),
        }),
      );
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(delayedSampleStarted);
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
      const rejectionSettled = yield* Deferred.make<void>();
      yield* fixture.controls.reject(
        { _tag: "Materialized" },
        SourceFixture.failure("degraded while metrics are delayed", "stream"),
        {
          lane: "fixture",
          offset: 1n,
        },
        () => Deferred.succeed(rejectionSettled, undefined).pipe(Effect.asVoid),
      );
      yield* Deferred.await(rejectionSettled);
      yield* Deferred.succeed(releaseDelayedSample, undefined);
      const published = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.metrics.adapter.observed === 2n),
          Stream.take(1),
          Stream.runHead,
        ),
      );

      expect(published.status._tag).toBe("Degraded");
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("does not carry an invalid metric sample across a recovered retry wait", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "metric-generation" },
              Schedule.spaced("3 seconds").pipe(Schedule.upTo({ times: 1 })),
            ),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      yield* fixture.controls.fail(
        { _tag: "Materialized" },
        SourceFixture.failure("retry", "stream"),
      );
      yield* fixture.controls.awaitCounts(
        { _tag: "Materialized" },
        { acquisitions: 1n, finalizations: 1n },
      );

      yield* fixture.controls.setRawMetricObserved("invalid");
      yield* TestClock.adjust("1 second");
      yield* fixture.controls.setMetrics({ observed: 1n });
      yield* TestClock.adjust("1 second");
      yield* TestClock.adjust("1 second");
      yield* fixture.controls.awaitCounts(
        { _tag: "Materialized" },
        { acquisitions: 2n, finalizations: 1n },
      );
      yield* Effect.yieldNow;

      expect(fixture.controls.counts({ _tag: "Materialized" })).toStrictEqual({
        acquisitions: 2n,
        finalizations: 1n,
      });
      yield* runtime.close;
    }),
  );

  it.effect("turns an infinite retry delay into an exact typed exhaustion", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "infinite-retry-delay" },
              Schedule.spaced(Duration.infinity),
            ),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive({ _tag: "Materialized" });
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });
      yield* fixture.controls.fail(
        { _tag: "Materialized" },
        SourceFixture.failure("retry", "stream"),
      );
      const exhausted = yield* awaitExhausted(diagnostics);

      expect(exhausted).toStrictEqual({
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: {
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceDefinition",
                message: "rows: Source Retry Schedule must produce a finite delay.",
              },
            },
          },
        },
        exhaustedAtNanos: 0n,
      });
      const health = yield* runtime.refreshHealth;
      expect(health.status).toBe("starting");
      expect(health.engine.topics.rows.status).toBe("starting");
      yield* runtime.close;
    }),
  );

  it.effect("rejects structurally forged attempts and lane events", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const source = fixture.materializedSource({ label: "forged-attempt" }, Schedule.recurs(0));
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source,
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const lifecycle = materializedLifecycle(service);

      const acquireForgedAttempt: typeof lifecycle.acquire = () =>
        Effect.succeed(
          new Proxy(
            SourceAdapterServer.attempt([
              SourceAdapterServer.lane({
                id: "forged-attempt",
                events: Stream.never,
              }),
            ]),
            {},
          ),
        );
      const forgedAttemptLifecycle = new Proxy(lifecycle, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquireForgedAttempt : Reflect.get(target, property, receiver),
      });
      const forgedAttemptService = {
        ...service,
        materialized: forgedAttemptLifecycle,
      };
      const forgedAttemptRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, forgedAttemptService),
      );
      expect(
        (yield* awaitExhausted(
          yield* forgedAttemptRuntime.liveClient.subscribeSourceHealth({ topic: "rows" }),
        )).exhaustion.lastTermination,
      ).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Lifecycle acquisition returned a structurally forged Source Attempt.",
          },
        },
      });
      yield* forgedAttemptRuntime.close;

      const acquireMalformedAttempt: typeof lifecycle.acquire = (input) =>
        lifecycle
          .acquire(input)
          .pipe(Effect.map((attempt) => nominalClone(attempt, { lanes: [] })));
      const malformedAttemptRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: new Proxy(lifecycle, {
            get: (target, property, receiver) =>
              property === "acquire"
                ? acquireMalformedAttempt
                : Reflect.get(target, property, receiver),
          }),
        }),
      );
      expect(
        (yield* awaitExhausted(
          yield* malformedAttemptRuntime.liveClient.subscribeSourceHealth({ topic: "rows" }),
        )).exhaustion.lastTermination,
      ).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message:
              "rows: Source Attempt requires non-empty unique lane IDs, Streams, and buffer metrics.",
          },
        },
      });
      yield* malformedAttemptRuntime.close;

      const acquireForgedEvent: typeof lifecycle.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "forged-event",
            region: "eu",
            value: "valid",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "forged-event",
              events: Stream.make(new Proxy(delivery, {})),
            }),
          ]);
        });
      const forgedEventLifecycle = new Proxy(lifecycle, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquireForgedEvent : Reflect.get(target, property, receiver),
      });
      const forgedEventRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: forgedEventLifecycle,
        }),
      );
      expect(
        (yield* awaitExhausted(
          yield* forgedEventRuntime.liveClient.subscribeSourceHealth({ topic: "rows" }),
        )).exhaustion.lastTermination,
      ).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Lane emitted a structurally forged event.",
          },
        },
      });
      yield* forgedEventRuntime.close;
    }),
  );

  it.effect("rejects forged mutations after passing complete application Exit to settlement", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "forged-mutation" }, Schedule.recurs(0)),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const settlementExit = yield* Deferred.make<"Success" | "Failure">();
      const acquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const valid = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "forged-mutation",
            region: "eu",
            value: "valid",
          });
          const forged = nominalClone(valid, {
            row: {
              id: "",
              region: "eu",
              value: "invalid",
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "forged-mutation",
              events: Stream.make(
                makeSourceDelivery(Chunk.of(forged), (exit) =>
                  Deferred.succeed(settlementExit, exit._tag).pipe(Effect.asVoid),
                ),
              ),
            }),
          ]);
        });
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );

      expect(yield* Deferred.await(settlementExit)).toBe("Failure");
      expect(
        (yield* awaitExhausted(yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" })))
          .exhaustion.lastTermination,
      ).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidCanonicalId",
            topic: "rows",
            message: "Source Topic rows requires a canonical string id.",
          },
        },
      });
      yield* runtime.close;
    }),
  );

  it.effect("revalidates forged rows, deletes, and lane registration at application time", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "application-forgeries" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const invalidRow: typeof materialized.acquire = (input) =>
        invokeHostile(input.toolkit.upsert, input.toolkit, [
          {
            id: "invalid-row",
          },
        ]).pipe(Effect.andThen(Effect.die(new Error("Invalid Source Upsert accepted."))));
      const invalidAppliedRow: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const valid = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "invalid-applied-row",
            region: "eu",
            value: "valid",
          });
          const forged = nominalClone(valid, {
            row: {
              id: "invalid-applied-row",
            },
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(forged));
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-applied-row",
              events: Stream.make(delivery),
            }),
          ]);
        });
      const invalidDelete: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const valid = yield* input.toolkit.delete("valid-delete");
          const forged = nominalClone(valid, {
            id: "",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(forged));
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-delete",
              events: Stream.make(delivery),
            }),
          ]);
        });
      const unregisteredLane: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "changing-lane",
            region: "eu",
            value: "valid",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
          const rejection = yield* input.toolkit.reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("unreachable", "stream"),
            },
            location: {
              lane: "registered",
              offset: 1n,
            },
            rejectedAtNanos: 1n,
          });
          const changingLane = withChangingLaneId(
            SourceAdapterServer.lane({
              id: "registered",
              events: Stream.make(delivery, rejection),
            }),
          );
          return SourceAdapterServer.attempt([changingLane]);
        });

      const exhaustAcquire = Effect.fn("RuntimeCore.sourceAdversarial.exhaustAcquire")(function* (
        acquire: typeof materialized.acquire,
      ) {
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideService(fixture.adapter.runtimeService, {
            ...service,
            materialized: new Proxy(materialized, {
              get: (target, property, receiver) =>
                property === "acquire" ? acquire : Reflect.get(target, property, receiver),
            }),
          }),
        );
        const exhausted = yield* awaitExhausted(
          yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" }),
        );
        yield* runtime.close;
        return exhausted.exhaustion.lastTermination;
      });
      expect(yield* exhaustAcquire(invalidRow)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidTopicRow",
            topic: "rows",
            message: "Source Upsert does not satisfy Topic rows Schema.",
          },
        },
      });
      expect(yield* exhaustAcquire(invalidAppliedRow)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidTopicRow",
            topic: "rows",
            message: "Source Upsert does not satisfy Topic rows Schema.",
          },
        },
      });
      expect(yield* exhaustAcquire(invalidDelete)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidCanonicalId",
            topic: "rows",
            message: "Source Topic rows requires a non-empty canonical string id.",
          },
        },
      });
      expect(yield* exhaustAcquire(unregisteredLane)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Lane unregistered was not registered by the active attempt.",
          },
        },
      });
    }),
  );

  it.effect("revalidates Leased route congruence after nominal mutation construction", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(
              ["region"],
              { label: "leased-route-forgery" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const leased = Option.getOrThrow(Option.fromUndefinedOr(service.leased));
      const settlementExit = yield* Deferred.make<string>();
      const acquire: typeof leased.acquire = (input) =>
        Effect.gen(function* () {
          const valid = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "wrong-route",
            region: "eu",
            value: "valid",
          });
          const forged = nominalClone(valid, {
            row: {
              id: "wrong-route",
              region: "us",
              value: "invalid",
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "wrong-route",
              events: Stream.make(
                makeSourceDelivery(Chunk.of(forged), (application) =>
                  Deferred.succeed(settlementExit, application._tag).pipe(Effect.asVoid),
                ),
              ),
            }),
          ]);
        });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          leased: new Proxy(leased, {
            get: (target, property, receiver) =>
              property === "acquire" ? acquire : Reflect.get(target, property, receiver),
          }),
        }),
      );
      const subscription = yield* runtime.liveClient.subscribe("rows", {
        routeBy: { region: "eu" },
        select: ["id"],
      });

      expect(yield* Deferred.await(settlementExit)).toBe("Failure");
      yield* subscription.close();
      yield* runtime.close;
    }),
  );

  it.effect("rejects forged rejection diagnostics at the lane boundary", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "forged-rejection" }, Schedule.recurs(0)),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const invalidLocationSettlement = yield* Deferred.make<Exit.Exit<void, unknown>>();

      const invalidLocationAcquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: {
              lane: "fixture",
              offset: 1n,
            },
            rejectedAtNanos: 1n,
            settlement: (applicationExit) =>
              Deferred.succeed(invalidLocationSettlement, applicationExit).pipe(Effect.asVoid),
          });
          const forged = nominalClone(rejection, {
            diagnostic: {
              ...rejection.diagnostic,
              location: {
                lane: "fixture",
                offset: "invalid",
              },
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-location",
              events: Stream.make(forged),
            }),
          ]);
        });
      const invalidTimestampAcquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: {
              lane: "fixture",
              offset: 1n,
            },
            rejectedAtNanos: 1n,
          });
          const forged = nominalClone(rejection, {
            diagnostic: {
              ...rejection.diagnostic,
              rejectedAtNanos: 1,
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-timestamp",
              events: Stream.make(forged),
            }),
          ]);
        });
      const invalidRuntimeFailureAcquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: makeRuntimeSourceFailure({
              _tag: "InvalidSourceDelivery",
              message: "valid runtime rejection",
            }),
            location: {
              lane: "fixture",
              offset: 1n,
            },
            rejectedAtNanos: 1n,
          });
          const forged = nominalClone(rejection, {
            diagnostic: {
              ...rejection.diagnostic,
              failure: invalidRuntimeFailure(),
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-runtime-failure",
              events: Stream.make(forged),
            }),
          ]);
        });
      const runtimeFailureSettlement = yield* Deferred.make<Exit.Exit<void, unknown>>();
      const validRuntimeFailureAcquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: makeRuntimeSourceFailure({
              _tag: "InvalidSourceDelivery",
              message: "valid runtime rejection",
            }),
            location: {
              lane: "fixture",
              offset: 2n,
            },
            rejectedAtNanos: 2n,
            settlement: (applicationExit) =>
              Deferred.succeed(runtimeFailureSettlement, applicationExit).pipe(Effect.asVoid),
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "valid-runtime-failure",
              events: Stream.make(rejection).pipe(Stream.concat(Stream.never)),
            }),
          ]);
        });

      const exhaustRejection = Effect.fn("RuntimeCore.sourceAdversarial.exhaustRejection")(
        function* (acquire: typeof materialized.acquire) {
          const lifecycle = new Proxy(materialized, {
            get: (target, property, receiver) =>
              property === "acquire" ? acquire : Reflect.get(target, property, receiver),
          });
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provideService(fixture.adapter.runtimeService, {
              ...service,
              materialized: lifecycle,
            }),
          );
          const exhausted = yield* awaitExhausted(
            yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" }),
          );
          yield* runtime.close;
          return exhausted.exhaustion.lastTermination;
        },
      );
      expect(yield* exhaustRejection(invalidLocationAcquire)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Rejection Location does not satisfy its declared Schema.",
          },
        },
      });
      expect((yield* Deferred.await(invalidLocationSettlement))._tag).toBe("Failure");
      expect(yield* exhaustRejection(invalidTimestampAcquire)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Lane emitted a structurally forged event.",
          },
        },
      });
      expect(yield* exhaustRejection(invalidRuntimeFailureAcquire)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Execution Failure did not satisfy the SDK Schema.",
          },
        },
      });

      const runtimeFailureLifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire"
            ? validRuntimeFailureAcquire
            : Reflect.get(target, property, receiver),
      });
      const runtimeFailureRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: runtimeFailureLifecycle,
        }),
      );
      const runtimeFailureHealth = yield* runtimeFailureRuntime.liveClient
        .subscribeSourceHealth({ topic: "rows" })
        .pipe(
          Effect.flatMap((diagnostics) =>
            diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Degraded"),
              Stream.take(1),
              Stream.runHead,
              Effect.ensuring(diagnostics.close().pipe(Effect.orDie)),
            ),
          ),
          Effect.map(Option.getOrThrow),
        );
      expect(runtimeFailureHealth.status).toStrictEqual({
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [
          {
            _tag: "SourceItemRejection",
            latestRejection: {
              failure: {
                _tag: "RuntimeFailure",
                failure: {
                  _tag: "InvalidSourceDelivery",
                  message: "valid runtime rejection",
                },
              },
              location: {
                lane: "fixture",
                offset: 2n,
              },
              rejectedAtNanos: 2n,
            },
          },
        ],
      });
      expect(yield* Deferred.await(runtimeFailureSettlement)).toStrictEqual(Exit.void);
      yield* runtimeFailureRuntime.close;
    }),
  );

  it.effect("keeps degradation sticky but hidden through retry, exhaustion, and stopping", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "sticky-degradation" },
              Schedule.spaced("2 seconds").pipe(Schedule.upTo({ times: 1 })),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const failFirstAttempt = yield* Deferred.make<void>();
      const secondAcquisitionStarted = yield* Deferred.make<void>();
      const releaseSecondAcquisition = yield* Deferred.make<void>();
      const failSecondAttempt = yield* Deferred.make<void>();
      const streamFailure = yield* fixture.adapter
        .failure(SourceFixture.failure("sticky degradation retry", "stream"))
        .pipe(Effect.orDie);
      let acquisitions = 0;
      const acquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          acquisitions += 1;
          if (acquisitions === 2) {
            yield* Deferred.succeed(secondAcquisitionStarted, undefined);
            yield* Deferred.await(releaseSecondAcquisition);
            return SourceAdapterServer.attempt([
              SourceAdapterServer.lane({
                id: "sticky-degradation",
                events: Stream.fromEffect(
                  Deferred.await(failSecondAttempt).pipe(
                    Effect.andThen(Effect.fail(streamFailure)),
                  ),
                ),
              }),
            ]);
          }
          const rejection = yield* input.toolkit.reject({
            failure: makeRuntimeSourceFailure({
              _tag: "InvalidSourceDelivery",
              message: "sticky degradation rejection",
            }),
            location: {
              lane: "sticky-degradation",
              offset: 1n,
            },
            rejectedAtNanos: 1n,
            settlement: () => Effect.void,
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "sticky-degradation",
              events: Stream.make(rejection).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.await(failFirstAttempt).pipe(
                      Effect.andThen(Effect.fail(streamFailure)),
                    ),
                  ),
                ),
              ),
            }),
          ]);
        });
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );
      const healthWith = (tag: SourceStatus<unknown, unknown>["_tag"]) =>
        Effect.acquireUseRelease(
          runtime.liveClient.subscribeSourceHealth({ topic: "rows" }),
          (diagnostics) =>
            diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === tag),
              Stream.take(1),
              Stream.runHead,
              Effect.map(
                Option.getOrThrowWith(
                  () => new Error(`Source diagnostics ended before ${tag} was observed.`),
                ),
              ),
            ),
          (diagnostics) => diagnostics.close().pipe(Effect.ignore),
        );
      const rejectionReason = {
        _tag: "SourceItemRejection",
        latestRejection: {
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceDelivery",
              message: "sticky degradation rejection",
            },
          },
          location: {
            lane: "sticky-degradation",
            offset: 1n,
          },
          rejectedAtNanos: 1n,
        },
      } as const;
      const termination = {
        _tag: "Failed",
        failure: {
          _tag: "AdapterFailure",
          failure: SourceFixture.failure("sticky degradation retry", "stream"),
        },
      } as const;

      expect((yield* healthWith("Degraded")).status).toStrictEqual({
        _tag: "Degraded",
        attempt: 1n,
        degradedAtNanos: 0n,
        reasons: [rejectionReason],
      });
      yield* Deferred.succeed(failFirstAttempt, undefined);
      expect((yield* healthWith("WaitingToRetry")).status).toStrictEqual({
        _tag: "WaitingToRetry",
        nextAttempt: 2n,
        termination,
        retryAtNanos: 2_000_000_000n,
      });

      const reacquiringFiber = yield* healthWith("Reacquiring").pipe(Effect.forkChild);
      yield* TestClock.adjust("2 seconds");
      yield* Deferred.await(secondAcquisitionStarted);
      expect((yield* Fiber.join(reacquiringFiber)).status).toStrictEqual({
        _tag: "Reacquiring",
        previousTermination: termination,
        attempt: 2n,
        startedAtNanos: 2_000_000_000n,
      });
      yield* Deferred.succeed(releaseSecondAcquisition, undefined);
      expect((yield* healthWith("Degraded")).status).toStrictEqual({
        _tag: "Degraded",
        attempt: 2n,
        degradedAtNanos: 0n,
        reasons: [rejectionReason],
      });

      yield* Deferred.succeed(failSecondAttempt, undefined);
      expect((yield* healthWith("Exhausted")).status).toStrictEqual({
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: termination,
        },
        exhaustedAtNanos: 2_000_000_000n,
      });
      const stoppingDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "rows",
      });
      const stoppingFiber = yield* stoppingDiagnostics.events.pipe(
        Stream.filter((health) => health.status._tag === "Stopping"),
        Stream.take(1),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* runtime.close;
      expect((yield* Fiber.join(stoppingFiber)).status).toStrictEqual({
        _tag: "Stopping",
        reason: "runtime-shutdown",
        stoppingAtNanos: 2_000_000_000n,
      });
      yield* stoppingDiagnostics.close().pipe(Effect.ignore);
      expect(acquisitions).toBe(2);
    }),
  );

  it.effect("requires stable lane ids across retries", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "unstable-lanes" }, Schedule.recurs(1)),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      let acquisitions = 0;
      const acquire: typeof materialized.acquire = () =>
        Effect.gen(function* () {
          acquisitions += 1;
          const failure = yield* fixture.adapter
            .failure(SourceFixture.failure("retry", "stream"))
            .pipe(Effect.orDie);
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: acquisitions === 1 ? "first" : "second",
              events: Stream.fail(failure),
            }),
          ]);
        });
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );

      expect(
        (yield* awaitExhausted(yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" })))
          .exhaustion.lastTermination,
      ).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Delivery Lane IDs must remain stable across retries.",
          },
        },
      });
      expect(acquisitions).toBe(2);
      yield* runtime.close;
    }),
  );

  it.effect("validates every SDK input and failure again at the runtime boundary", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "boundary-forgeries" }, Schedule.recurs(0)),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);

      const emptyDelivery: typeof materialized.acquire = (input) =>
        invokeHostile(input.toolkit.delivery, input.toolkit, [Chunk.empty()]).pipe(
          Effect.andThen(Effect.die(new Error("Invalid delivery accepted."))),
        );
      const invalidDeliverySettlement: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "delivery-settlement",
            region: "eu",
            value: "valid",
          });
          return yield* invokeHostile(input.toolkit.delivery, input.toolkit, [
            Chunk.of(mutation),
            "invalid",
          ]).pipe(Effect.andThen(Effect.die(new Error("Invalid delivery settlement accepted."))));
        });
      const invalidRejectionSettlement: typeof materialized.acquire = (input) =>
        invokeHostile(input.toolkit.reject, input.toolkit, [
          {
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: 1n,
            settlement: "invalid",
          },
        ]).pipe(Effect.andThen(Effect.die(new Error("Invalid rejection settlement accepted."))));
      const invalidRejectionTimestamp: typeof materialized.acquire = (input) =>
        invokeHostile(input.toolkit.reject, input.toolkit, [
          {
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: 1,
          },
        ]).pipe(Effect.andThen(Effect.die(new Error("Invalid rejection timestamp accepted."))));
      const negativeRejectionTimestamp: typeof materialized.acquire = (input) =>
        input.toolkit
          .reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: -1n,
          })
          .pipe(Effect.andThen(Effect.die(new Error("Negative rejection timestamp accepted."))));
      const invalidRuntimeRejection: typeof materialized.acquire = (input) =>
        input.toolkit
          .reject({
            failure: invalidRuntimeFailure(),
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: 1n,
          })
          .pipe(Effect.andThen(Effect.die(new Error("Invalid runtime failure accepted."))));
      const invalidAcquisitionFailure: typeof materialized.acquire = () =>
        Effect.fail(invalidRuntimeFailure());
      const invalidLaneFailure: typeof materialized.acquire = () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-lane-failure",
              events: Stream.fail(invalidRuntimeFailure()),
            }),
          ]),
        );
      const invalidTransitionDelivery: typeof materialized.acquire = (input) =>
        invokeHostile(input.toolkit.delivery, input.toolkit, [
          {},
          undefined,
          makeSourceApplicationTransition("rows", () => undefined, [], Object.freeze({})),
        ]).pipe(Effect.andThen(Effect.die(new Error("Invalid transition delivery accepted."))));
      const forgedTransitionBatch: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "forged-transition-batch",
            region: "eu",
            value: "valid",
          });
          const delivery = yield* input.toolkit.delivery(
            mutation,
            undefined,
            makeSourceApplicationTransition(
              input.toolkit.topic,
              () => undefined,
              [],
              Object.freeze({}),
            ),
          );
          const forged = nominalClone(delivery, {
            mutations: Chunk.make(mutation, mutation),
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "forged-transition-batch",
              events: Stream.make(forged),
            }),
          ]);
        });
      const invalidSettlementReturn: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "invalid-settlement-return",
            region: "eu",
            value: "invalid",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
          const forged = nominalClone(delivery, {
            settle: () => "not-an-effect",
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-settlement-return",
              events: Stream.make(forged),
            }),
          ]);
        });
      const throwingSettlement: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "throwing-settlement",
            region: "eu",
            value: "invalid",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
          const forged = nominalClone(delivery, {
            settle: () => {
              throw new Error("settlement threw");
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "throwing-settlement",
              events: Stream.make(forged),
            }),
          ]);
        });
      const hostileSettlementReturn: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "hostile-settlement-return",
            region: "eu",
            value: "invalid",
          });
          const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
          const hostile = new Proxy(
            {},
            {
              get: () => {
                throw new Error("settlement result inspection threw");
              },
              has: () => {
                throw new Error("settlement result inspection threw");
              },
            },
          );
          const forged = nominalClone(delivery, {
            settle: () => hostile,
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "hostile-settlement-return",
              events: Stream.make(forged),
            }),
          ]);
        });
      const negativeForgedRejection: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: 1n,
          });
          const forged = nominalClone(rejection, {
            diagnostic: {
              ...rejection.diagnostic,
              rejectedAtNanos: -1n,
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "negative-forged-rejection",
              events: Stream.make(forged),
            }),
          ]);
        });
      const failExecution = (
        failure: SourceExecutionFailure<SourceFixtureFailure>,
      ): Effect.Effect<never, SourceExecutionFailure<SourceFixtureFailure>> => Effect.fail(failure);
      const failLane = (
        failure: SourceExecutionFailure<SourceFixtureFailure>,
      ): Stream.Stream<never, SourceExecutionFailure<SourceFixtureFailure>> => Stream.fail(failure);
      const nullAcquisitionFailure: typeof materialized.acquire = () =>
        invokeHostile(failExecution, undefined, [null]);
      const nullLaneFailure: typeof materialized.acquire = () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "null-lane-failure",
              events: invokeHostile(failLane, undefined, [null]),
            }),
          ]),
        );
      const nullRejectionFailure: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const rejection = yield* input.toolkit.reject({
            failure: {
              _tag: "AdapterFailure",
              failure: SourceFixture.failure("rejected", "stream"),
            },
            location: { lane: "fixture", offset: 1n },
            rejectedAtNanos: 1n,
          });
          const forged = nominalClone(rejection, {
            diagnostic: {
              ...rejection.diagnostic,
              failure: null,
            },
          });
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "null-rejection-failure",
              events: Stream.make(forged),
            }),
          ]);
        });

      const expectInvalidDefinition = Effect.fn(
        "RuntimeCore.sourceAdversarial.expectInvalidDefinition",
      )(function* (
        acquire: typeof materialized.acquire,
        message: string,
        failure: SourceRuntimeFailure = {
          _tag: "InvalidSourceDefinition",
          message,
        },
      ) {
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provideService(fixture.adapter.runtimeService, {
            ...service,
            materialized: new Proxy(materialized, {
              get: (target, property, receiver) =>
                property === "acquire" ? acquire : Reflect.get(target, property, receiver),
            }),
          }),
        );
        const exhausted = yield* awaitExhausted(
          yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" }),
        );
        expect(exhausted.exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "RuntimeFailure",
            failure,
          },
        });
        yield* runtime.close;
      });
      yield* expectInvalidDefinition(
        emptyDelivery,
        "rows: Source Delivery requires one or more nominal Source Mutations.",
      );
      yield* expectInvalidDefinition(
        invalidDeliverySettlement,
        "rows: Source Delivery settlement must be an Effect function.",
      );
      yield* expectInvalidDefinition(
        invalidTransitionDelivery,
        "rows: Source Application Transition requires exactly one nominal Source Mutation.",
      );
      yield* expectInvalidDefinition(
        forgedTransitionBatch,
        "rows: Source Application Transition requires exactly one nominal Source Mutation.",
      );
      yield* expectInvalidDefinition(
        invalidRejectionSettlement,
        "rows: Source Rejection settlement must be an Effect function.",
      );
      yield* expectInvalidDefinition(
        invalidRejectionTimestamp,
        "rows: Source Rejection timestamp must be non-negative epoch nanoseconds.",
      );
      yield* expectInvalidDefinition(
        negativeRejectionTimestamp,
        "rows: Source Rejection timestamp must be non-negative epoch nanoseconds.",
      );
      yield* expectInvalidDefinition(
        invalidRuntimeRejection,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        invalidAcquisitionFailure,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        invalidLaneFailure,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        invalidSettlementReturn,
        "rows: Source settlement must return an Effect without throwing.",
      );
      yield* expectInvalidDefinition(
        throwingSettlement,
        "rows: Source settlement must return an Effect without throwing.",
        {
          _tag: "InvalidSourceSettlement",
          message: "Source Settlement callback threw before returning an Effect",
        },
      );
      yield* expectInvalidDefinition(
        hostileSettlementReturn,
        "rows: Source settlement must return an Effect without throwing.",
        {
          _tag: "InvalidSourceSettlement",
          message: "Source Settlement callback threw before returning an Effect",
        },
      );
      yield* expectInvalidDefinition(
        negativeForgedRejection,
        "rows: Source Rejection timestamp must be non-negative epoch nanoseconds.",
      );
      yield* expectInvalidDefinition(
        nullAcquisitionFailure,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        nullLaneFailure,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        nullRejectionFailure,
        "rows: Source Execution Failure did not satisfy the SDK Schema.",
      );
    }),
  );

  it.effect("exhausts when the one-second adapter metrics sample becomes invalid", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "invalid-metric-sample" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      let observedReads = 0;
      const changingMetrics = new Proxy(
        { observed: 0n },
        {
          get: (target, property, receiver) => {
            if (property !== "observed") {
              return Reflect.get(target, property, receiver);
            }
            observedReads += 1;
            return observedReads === 1 ? 0n : "invalid";
          },
        },
      );
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "metrics"
            ? () => Effect.succeed(changingMetrics)
            : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });

      yield* TestClock.adjust("1 second");
      expect((yield* awaitExhausted(diagnostics)).exhaustion.lastTermination).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceMetrics",
            message:
              "Source Adapter controllable-fixture returned metrics outside its declared Schema.",
          },
        },
      });
      yield* runtime.close;
    }),
  );

  it.effect("redacts a composite late adapter metrics defect as a safe runtime failure", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              { label: "defective-metric-sample" },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      let sampleCount = 0;
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "metrics"
            ? () => {
                sampleCount += 1;
                return sampleCount === 1
                  ? Effect.succeed({ observed: 0n })
                  : Effect.failCause(
                      Cause.combine(
                        Cause.interrupt(),
                        Cause.combine(
                          Cause.fail(
                            makeRuntimeSourceFailure({
                              _tag: "InvalidSourceMetrics",
                              message: "typed metrics failure must not hide a parallel defect",
                            }),
                          ),
                          Cause.die(new Error("hostile metrics defect")),
                        ),
                      ),
                    );
              }
            : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });

      yield* TestClock.adjust("1 second");
      expect((yield* awaitExhausted(diagnostics)).exhaustion.lastTermination).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceMetrics",
            message: "Source Adapter controllable-fixture failed while sampling metrics.",
          },
        },
      });
      yield* runtime.close;
    }),
  );

  it.effect("propagates adapter metrics interruption instead of continuing the cadence", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "interrupted-metric-sample" }),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const sampleStarted = yield* Deferred.make<void>();
      let sampleCount = 0;
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "metrics"
            ? () => {
                sampleCount += 1;
                return sampleCount === 1
                  ? Effect.succeed({ observed: 0n })
                  : Deferred.succeed(sampleStarted, undefined).pipe(
                      Effect.andThen(Effect.interrupt),
                    );
              }
            : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );

      yield* TestClock.adjust("1 second");
      yield* Deferred.await(sampleStarted);
      yield* TestClock.adjust("1 second");
      expect(sampleCount).toBe(2);
      const health = yield* runtime.client.health();
      expect({
        adapterMetrics: health.sources.rows?.metrics.adapter,
        runtimeStatus: health.status,
        sourceStatus: health.sources.rows?.status._tag,
      }).toStrictEqual({
        adapterMetrics: { observed: 0n },
        runtimeStatus: "ready",
        sourceStatus: "Ready",
      });
      yield* runtime.close;
    }),
  );

  it.effect("supervises invalid lane buffer metrics as an exact runtime failure", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource(
              {
                label: "invalid-lane-buffer-metrics",
              },
              Schedule.recurs(0),
            ),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const invalidBufferMetrics = new Proxy(
        {
          _tag: "Unbuffered" as const,
        },
        {
          get: (target, property, receiver) =>
            property === "_tag" ? "InvalidBufferMetrics" : Reflect.get(target, property, receiver),
        },
      );
      const acquire: typeof materialized.acquire = () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "invalid-buffer",
              events: Stream.never,
              bufferMetrics: Effect.succeed(invalidBufferMetrics),
            }),
          ]),
        );
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "rows" });

      expect((yield* awaitExhausted(diagnostics)).exhaustion.lastTermination).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceMetrics",
            message:
              "Source Adapter controllable-fixture lane invalid-buffer returned buffer metrics outside the Source Buffer Metrics Schema.",
          },
        },
      });
      yield* runtime.close;
    }),
  );

  it.effect("runs sibling lanes concurrently while preserving lane-local order", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "concurrent-lanes",
            }),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const siblingStarted = yield* Deferred.make<void>();
      const ordered = yield* Deferred.make<ReadonlyArray<"first" | "second">>();
      const acquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const firstMutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "first",
            region: "eu",
            value: "first",
          });
          const secondMutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "second",
            region: "eu",
            value: "second",
          });
          const observed: Array<"first" | "second"> = [];
          const first = yield* input.toolkit.delivery(Chunk.of(firstMutation), () =>
            Deferred.await(siblingStarted).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  observed.push("first");
                }),
              ),
            ),
          );
          const second = yield* input.toolkit.delivery(Chunk.of(secondMutation), () =>
            Effect.sync(() => {
              observed.push("second");
            }).pipe(
              Effect.andThen(
                Effect.suspend(() => Deferred.succeed(ordered, [...observed]).pipe(Effect.asVoid)),
              ),
            ),
          );
          const sibling = Stream.unwrap(
            Deferred.succeed(siblingStarted, undefined).pipe(Effect.as(Stream.never)),
          );
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "ordered",
              events: Stream.make(first, second).pipe(Stream.concat(Stream.never)),
            }),
            SourceAdapterServer.lane({
              id: "sibling",
              events: sibling,
            }),
          ]);
        });
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );

      expect(yield* Deferred.await(ordered)).toStrictEqual(["first", "second"]);
      yield* runtime.close;
    }),
  );

  it.effect("applies transition-free sibling lanes without the lifecycle gate", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "concurrent-applications",
            }),
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const materialized = materializedLifecycle(service);
      const firstApplicationBlocked = yield* Deferred.make<void>();
      const releaseFirstApplication = yield* Deferred.make<void>();
      const secondApplicationCompleted = yield* Deferred.make<void>();
      let applicationCount = 0;
      const afterMutationApplication = Effect.gen(function* () {
        applicationCount += 1;
        if (applicationCount === 1) {
          yield* Deferred.succeed(firstApplicationBlocked, undefined);
          yield* Deferred.await(releaseFirstApplication);
          return;
        }
        yield* Deferred.succeed(secondApplicationCompleted, undefined);
      });
      const acquire: typeof materialized.acquire = (input) =>
        Effect.gen(function* () {
          const firstMutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "first",
            region: "eu",
            value: "first",
          });
          const secondMutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
            id: "second",
            region: "eu",
            value: "second",
          });
          const first = yield* input.toolkit.delivery(Chunk.of(firstMutation), () => Effect.void);
          const second = yield* input.toolkit.delivery(Chunk.of(secondMutation), () => Effect.void);
          const secondLane = Stream.unwrap(
            Deferred.await(firstApplicationBlocked).pipe(
              Effect.as(Stream.make(second).pipe(Stream.concat(Stream.never))),
            ),
          );
          return SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "first",
              events: Stream.make(first).pipe(Stream.concat(Stream.never)),
            }),
            SourceAdapterServer.lane({
              id: "second",
              events: secondLane,
            }),
          ]);
        });
      const lifecycle = new Proxy(materialized, {
        get: (target, property, receiver) =>
          property === "acquire" ? acquire : Reflect.get(target, property, receiver),
      });
      const mutations: ViewServerRuntimeCoreInternalMutations<typeof config.topics> = {
        publish: () => Effect.void,
        publishMany: () => Effect.void,
        patch: () => Effect.void,
        delete: () => Effect.void,
        reset: () => Effect.void,
        deleteStorageKey: () => Effect.void,
        patchDecodedFields: () => Effect.void,
        publishManyDecodedRows: () => Effect.void,
        publishManyDecodedRowsWithStorageKeys: () => Effect.void,
        publishManyWithStorageKeys: () => Effect.void,
      };
      const manager = yield* makeRuntimeCoreSourceManager(config, mutations, Effect.void, {
        afterMutationApplication,
      }).pipe(
        Effect.provideService(fixture.adapter.runtimeService, {
          ...service,
          materialized: lifecycle,
        }),
      );

      yield* Deferred.await(secondApplicationCompleted);
      yield* Deferred.succeed(releaseFirstApplication, undefined);
      expect(applicationCount).toBe(2);
      yield* manager.close;
    }),
  );
});
