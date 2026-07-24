import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceAdapter,
  type SourceDeliveryLane,
  type SourceStatus,
} from "@effect-view-server/source-adapter";
import {
  decodeSourceToolkitUpsert,
  makeRuntimeSourceFailure,
  makeSourceDelivery,
} from "@effect-view-server/source-adapter/internal";
import {
  SourceFixture,
  type ControllableSourceFixture,
} from "@effect-view-server/source-adapter-testing";
import {
  Chunk,
  Context,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: Schema.String,
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
  it.effect("supervises cyclic adapter metric arrays and objects as invalid metrics", () =>
    Effect.gen(function* () {
      class CyclicMetrics {
        constructor(readonly nested: object) {}
      }
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
          metrics: Schema.instanceOf(CyclicMetrics),
          rejectionLocation: CyclicLocation,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      let currentMetrics = new CyclicMetrics({});
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
        currentMetrics = new CyclicMetrics({});
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
        const exhaustedFiber = yield* awaitExhausted(diagnostics).pipe(Effect.forkChild);
        currentMetrics = new CyclicMetrics(cyclic);
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
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
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
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
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
          yield* forgedAttemptRuntime.liveClient.subscribeSourceHealth("rows"),
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
        (yield* awaitExhausted(yield* forgedEventRuntime.liveClient.subscribeSourceHealth("rows")))
          .exhaustion.lastTermination,
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
        (yield* awaitExhausted(yield* runtime.liveClient.subscribeSourceHealth("rows"))).exhaustion
          .lastTermination,
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
          yield* runtime.liveClient.subscribeSourceHealth("rows"),
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
            yield* runtime.liveClient.subscribeSourceHealth("rows"),
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
      expect(yield* exhaustRejection(invalidTimestampAcquire)).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "RuntimeFailure",
          failure: {
            _tag: "InvalidSourceDefinition",
            message: "rows: Source Rejection timestamp must be epoch nanoseconds.",
          },
        },
      });
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
        (yield* awaitExhausted(yield* runtime.liveClient.subscribeSourceHealth("rows"))).exhaustion
          .lastTermination,
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

      const expectInvalidDefinition = Effect.fn(
        "RuntimeCore.sourceAdversarial.expectInvalidDefinition",
      )(function* (acquire: typeof materialized.acquire, message: string) {
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
          yield* runtime.liveClient.subscribeSourceHealth("rows"),
        );
        expect(exhausted.exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidSourceDefinition",
              message,
            },
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
        "rows: Source Delivery requires one or more nominal Source Mutations.",
      );
      yield* expectInvalidDefinition(
        invalidRejectionSettlement,
        "rows: Source Rejection settlement must be an Effect function.",
      );
      yield* expectInvalidDefinition(
        invalidRejectionTimestamp,
        "rows: Source Rejection timestamp must be epoch nanoseconds.",
      );
      yield* expectInvalidDefinition(
        invalidRuntimeRejection,
        "rows: Source Runtime Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        invalidAcquisitionFailure,
        "rows: Source Runtime Failure did not satisfy the SDK Schema.",
      );
      yield* expectInvalidDefinition(
        invalidLaneFailure,
        "rows: Source Runtime Failure did not satisfy the SDK Schema.",
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
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");

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
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");

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
});
