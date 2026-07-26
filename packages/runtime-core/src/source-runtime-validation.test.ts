import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceFixture,
  type SourceFixtureFailure,
  type SourceFixtureRejectionLocation,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import { Effect, Exit, Fiber, Option, Schedule, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  value: Schema.String,
});

const materializedTarget: SourceFixtureTarget = {
  _tag: "Materialized",
};
const leasedTarget = (region: string): SourceFixtureTarget => ({
  _tag: "Leased",
  route: { region },
});

describe("Runtime Core Source boundary validation", () => {
  it.effect("publishes sorted declared Lane IDs before and after attempt acquisition", () =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "declared-lanes" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const layer = SourceAdapterServer.make(Adapter, {
        materialized: {
          initialLaneIds: () => ["second", "first"],
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "second",
                  events: Stream.never,
                }),
                SourceAdapterServer.lane({
                  id: "first",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
      const health = Option.getOrThrow(
        yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );

      expect(health.metrics.runtime.lanes).toStrictEqual([
        { id: "first", buffer: { _tag: "Unbuffered" } },
        { id: "second", buffer: { _tag: "Unbuffered" } },
      ]);
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect.each([
    {
      label: "a throwing declaration",
      initialLaneIds: (): readonly [string] => {
        throw new Error("initial Lane declaration failed");
      },
      message: "Source Adapter initial Lane IDs must be returned without throwing.",
    },
    {
      label: "a hostile iterable declaration",
      initialLaneIds: (): readonly [string] =>
        new Proxy(["lane"], {
          get: (target, property, receiver) => {
            if (property === Symbol.iterator) {
              throw new Error("initial Lane iterator failed");
            }
            return Reflect.get(target, property, receiver);
          },
        }),
      message: "Source Adapter initial Lane IDs must be non-empty, unique strings.",
    },
    {
      label: "an empty declaration",
      initialLaneIds: (): readonly [string] =>
        new Proxy(["lane"], {
          get: (target, property, receiver) =>
            property === Symbol.iterator
              ? function* () {}
              : Reflect.get(target, property, receiver),
        }),
      message: "Source Adapter initial Lane IDs must be non-empty, unique strings.",
    },
    {
      label: "a non-string declaration",
      initialLaneIds: (): readonly [string] =>
        new Proxy(["lane"], {
          get: (target, property, receiver) =>
            property === Symbol.iterator
              ? function* () {
                  yield 1;
                }
              : Reflect.get(target, property, receiver),
        }),
      message: "Source Adapter initial Lane IDs must be non-empty, unique strings.",
    },
    {
      label: "an empty-string declaration",
      initialLaneIds: (): readonly [string] => [""],
      message: "Source Adapter initial Lane IDs must be non-empty, unique strings.",
    },
    {
      label: "a duplicate declaration",
      initialLaneIds: (): readonly [string, string] => ["lane", "lane"],
      message: "Source Adapter initial Lane IDs must be non-empty, unique strings.",
    },
  ])("exhausts $label before acquisition", ({ initialLaneIds, message }) =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "invalid-declared-lanes" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const layer = SourceAdapterServer.make(Adapter, {
        materialized: {
          initialLaneIds,
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "lane",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );

      expect(exhausted.status).toStrictEqual({
        _tag: "Exhausted",
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: {
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceDefinition",
                message: `rows: ${message}`,
              },
            },
          },
        },
        exhaustedAtNanos: 0n,
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect(
    "keeps an invalid Lane declaration terminal across successful metrics samples and retries",
    () =>
      Effect.gen(function* () {
        const Adapter = SourceAdapter.make({
          identity: { name: "persistent-invalid-declared-lanes" },
          failure: Schema.String,
          materialized: {
            metrics: Schema.Struct({ samples: Schema.BigInt }),
            rejectionLocation: Schema.Struct({}),
            definitionOptions: SourceAdapter.definitionOptions<void>(),
          },
          leased: undefined,
        });
        let acquisitions = 0;
        let samples = 0n;
        const layer = SourceAdapterServer.make(Adapter, {
          materialized: {
            initialLaneIds: (): readonly [string] => [""],
            acquire: () => {
              acquisitions += 1;
              return Effect.succeed(
                SourceAdapterServer.attempt([
                  SourceAdapterServer.lane({
                    id: "lane",
                    events: Stream.never,
                  }),
                ]),
              );
            },
            metrics: () => {
              samples += 1n;
              return Effect.succeed({ samples });
            },
            retry: Schedule.spaced("2 seconds").pipe(Schedule.upTo({ times: 1 })),
          },
        });
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: Adapter.materializedSource(undefined),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
        const waiting = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "WaitingToRetry"),
            Stream.take(1),
            Stream.runHead,
          ),
        );

        expect(waiting.status).toStrictEqual({
          _tag: "WaitingToRetry",
          nextAttempt: 2n,
          termination: {
            _tag: "Failed",
            failure: {
              _tag: "RuntimeFailure",
              failure: {
                _tag: "InvalidSourceDefinition",
                message: "rows: Source Adapter initial Lane IDs must be non-empty, unique strings.",
              },
            },
          },
          retryAtNanos: 2_000_000_000n,
        });
        yield* TestClock.adjust("1 second");
        expect(samples).toBe(2n);
        expect(acquisitions).toBe(0);
        yield* TestClock.adjust("1 second");
        const exhausted = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );

        expect(exhausted.status).toStrictEqual({
          _tag: "Exhausted",
          exhaustion: {
            _tag: "RetryExhausted",
            lastTermination: {
              _tag: "Failed",
              failure: {
                _tag: "RuntimeFailure",
                failure: {
                  _tag: "InvalidSourceDefinition",
                  message:
                    "rows: Source Adapter initial Lane IDs must be non-empty, unique strings.",
                },
              },
            },
          },
          exhaustedAtNanos: 2_000_000_000n,
        });
        expect(acquisitions).toBe(0);
        yield* diagnostics.close();
        yield* runtime.close;
      }),
  );

  it.live(
    "rejects invalid rows, ids, failure payloads, and rejection locations before application",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(Row);
        const config = defineViewServerConfig({
          topics: {
            rows: {
              schema: Row,
              source: fixture.materializedSource(
                { label: "boundary-validation" },
                Schedule.forever,
              ),
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(fixture.layer),
        );
        yield* fixture.controls.awaitActive(materializedTarget);

        const expectRetry = Effect.fn("RuntimeCore.sourceValidation.expectRetry")(function* (
          label: string,
          command: Effect.Effect<void, SourceFixtureFailure>,
        ) {
          const before = fixture.controls.counts(materializedTarget);
          yield* command;
          yield* fixture.controls
            .awaitCounts(materializedTarget, {
              acquisitions: before.acquisitions + 1n,
              finalizations: before.finalizations + 1n,
            })
            .pipe(
              Effect.timeout("1 second"),
              Effect.catch(() =>
                Effect.die(
                  new Error(
                    `${label} did not retry: ${JSON.stringify(
                      fixture.controls.counts(materializedTarget),
                      (_key, value) => (typeof value === "bigint" ? value.toString() : value),
                    )}`,
                  ),
                ),
              ),
            );
        });

        yield* expectRetry(
          "invalid row",
          fixture.controls.upsert(materializedTarget, {
            id: "missing-fields",
          }),
        );
        yield* expectRetry(
          "empty id",
          fixture.controls.upsert(materializedTarget, {
            id: "",
            region: "eu",
            value: "empty-id",
          }),
        );
        yield* expectRetry("empty delete", fixture.controls.delete(materializedTarget, ""));

        const invalidFailure = new Proxy(SourceFixture.failure("invalid failure", "stream"), {
          get: (target, property, receiver) =>
            property === "phase" ? "invalid" : Reflect.get(target, property, receiver),
        });
        yield* expectRetry(
          "invalid failure",
          fixture.controls.reject(materializedTarget, invalidFailure, {
            lane: "fixture",
            offset: 1n,
          }),
        );

        const invalidLocation = new Proxy<SourceFixtureRejectionLocation>(
          {
            lane: "fixture",
            offset: 1n,
          },
          {
            get: (target, property, receiver) =>
              property === "offset" ? "invalid" : Reflect.get(target, property, receiver),
          },
        );
        yield* expectRetry(
          "invalid location",
          fixture.controls.reject(
            materializedTarget,
            SourceFixture.failure("invalid location", "stream"),
            invalidLocation,
          ),
        );

        yield* runtime.close;
        const counts = fixture.controls.counts(materializedTarget);
        expect(counts.finalizations).toBe(counts.acquisitions);
      }),
  );

  it.effect("rejects rows incongruent with an acquired Leased route", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(
              ["region"],
              { label: "route-validation" },
              Schedule.forever,
            ),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      const subscription = yield* runtime.liveClient.subscribe("rows", {
        routeBy: { region: "eu" },
        select: ["id"],
      });
      yield* fixture.controls.awaitActive(leasedTarget("eu"));
      yield* fixture.controls.upsert(leasedTarget("eu"), {
        id: "wrong-route",
        region: "us",
        value: "invalid",
      });
      yield* fixture.controls.awaitCounts(leasedTarget("eu"), {
        acquisitions: 2n,
        finalizations: 1n,
      });

      yield* subscription.close();
      yield* runtime.close;
      expect(fixture.controls.counts(leasedTarget("eu")).finalizations).toBe(2n);
    }),
  );

  it.effect("exhausts a source whose initial adapter metrics violate its Schema", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const invalidMetrics = new Proxy(
        { observed: 0n },
        {
          get: (target, property, receiver) =>
            property === "observed" ? "invalid" : Reflect.get(target, property, receiver),
        },
      );
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({ label: "invalid-metrics" }, Schedule.recurs(0)),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
      yield* fixture.controls.setMetrics(invalidMetrics);
      const exhausted = yield* diagnostics.events.pipe(
        Stream.filter((result) => result.status._tag === "Exhausted"),
        Stream.take(1),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* TestClock.adjust("1 second");

      const exhaustedHealth = Option.getOrThrow(yield* Fiber.join(exhausted));
      if (exhaustedHealth.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected exhausted Source status.");
      }
      expect(exhaustedHealth.status.exhaustion.lastTermination).toStrictEqual({
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
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("rejects invalid diagnostic topic and lifecycle route arguments", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const materializedConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "diagnostic-validation",
            }),
          },
          manual: {
            schema: Row,
            key: "id",
          },
        },
      });
      const materializedRuntime = yield* makeViewServerRuntimeCore(materializedConfig, {}).pipe(
        Effect.provide(fixture.layer),
      );
      const materializedFailures = yield* Effect.all([
        // @ts-expect-error source-free Topics do not expose diagnostics.
        materializedRuntime.liveClient.subscribeSourceHealth("manual").pipe(Effect.exit),
        materializedRuntime.liveClient
          .subscribeSourceHealth(
            "rows",
            // @ts-expect-error Materialized diagnostics reject routeBy.
            { region: "eu" },
          )
          .pipe(Effect.exit),
      ]);
      expect(Option.getOrThrow(Exit.findErrorOption(materializedFailures[0]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "manual",
        message: "Topic manual has no canonical Source Definition.",
      });
      expect(Option.getOrThrow(Exit.findErrorOption(materializedFailures[1]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "rows",
        message: "Materialized Source Topic rows does not accept routeBy.",
      });
      yield* materializedRuntime.close;

      const leasedFixture = yield* SourceFixture.make(Row);
      const leasedConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: leasedFixture.leasedSource(["region"], {
              label: "leased-diagnostic-validation",
            }),
          },
        },
      });
      const leasedRuntime = yield* makeViewServerRuntimeCore(leasedConfig, {}).pipe(
        Effect.provide(leasedFixture.layer),
      );
      const leasedFailures = yield* Effect.all([
        // @ts-expect-error Leased diagnostics require a route.
        leasedRuntime.liveClient.subscribeSourceHealth("rows").pipe(Effect.exit),
        leasedRuntime.liveClient
          .subscribeSourceHealth(
            "rows",
            // @ts-expect-error Leased diagnostics routes reject extra fields.
            { region: "eu", extra: true },
          )
          .pipe(Effect.exit),
        leasedRuntime.liveClient
          .subscribeSourceHealth(
            "rows",
            // @ts-expect-error Leased diagnostics preserve route field types.
            { region: 1 },
          )
          .pipe(Effect.exit),
      ]);
      expect(Option.getOrThrow(Exit.findErrorOption(leasedFailures[0]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "rows",
        message: "Leased Source Topic rows requires exact routeBy.",
      });
      expect(Option.getOrThrow(Exit.findErrorOption(leasedFailures[1]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "rows",
        message: "Leased Source routeBy must contain all and only: region.",
      });
      expect(Option.getOrThrow(Exit.findErrorOption(leasedFailures[2]))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "rows",
        message: "Leased Source route field region does not satisfy the Topic Schema.",
      });
      yield* leasedRuntime.close;
    }),
  );

  it.effect("releases Leased Feeds when subscription acquisition fails", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.leasedSource(["region"], {
              label: "lease-handoff",
            }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );

      const invalidAcquisition: Effect.Effect<unknown, unknown> = Reflect.apply(
        runtime.liveClient.subscribe,
        runtime.liveClient,
        [
          "rows",
          {
            routeBy: { region: "eu" },
            select: ["missing"],
          },
        ],
      );
      expect(
        Option.getOrThrow(Exit.findErrorOption(yield* Effect.exit(invalidAcquisition))),
      ).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        topic: "rows",
        message: "Raw query select contains unknown field: missing.",
      });
      yield* Effect.yieldNow;
      const afterFailedAcquisition = fixture.controls.counts(leasedTarget("eu"));
      expect(afterFailedAcquisition.finalizations).toBe(afterFailedAcquisition.acquisitions);

      yield* runtime.close;
    }),
  );

  it.effect("rejects cached Source Health that no longer satisfies its exact contract", () =>
    Effect.gen(function* () {
      let metricsAccepted = true;
      const DynamicMetrics = Schema.Struct({
        observed: Schema.BigInt,
      }).check(Schema.makeFilter(() => metricsAccepted));
      const DynamicFailure = Schema.TaggedStruct("DynamicHealthFailure", {
        message: Schema.String,
      });
      const DynamicLocation = Schema.Struct({
        offset: Schema.BigInt,
      });
      const dynamicAdapter = SourceAdapter.make({
        identity: {
          name: "dynamic-health",
        },
        failure: DynamicFailure,
        materialized: {
          metrics: DynamicMetrics,
          rejectionLocation: DynamicLocation,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const dynamicLayer = SourceAdapterServer.make(dynamicAdapter, {
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "dynamic",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed({ observed: 1n }),
          retry: Schedule.recurs(0),
        },
      });
      const dynamicConfig = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: dynamicAdapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(dynamicConfig, {}).pipe(
        Effect.provide(dynamicLayer),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth("rows");
      metricsAccepted = false;
      const exit = yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead, Effect.exit);

      expect(Option.getOrThrow(Exit.findErrorOption(exit))).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "rows",
        message: "Cached Source Health for Topic rows violated its configured contract.",
      });
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );
});
