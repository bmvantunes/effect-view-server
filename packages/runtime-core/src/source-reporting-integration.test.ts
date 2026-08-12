import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter, type SourceDependencyTarget } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceFixture,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
});

const materializedTarget: SourceFixtureTarget = { _tag: "Materialized" };

const hostileTarget = (
  property: "dependency" | "target" | "endpoints",
  value: unknown,
): SourceDependencyTarget =>
  new Proxy(
    { target: "orders", endpoints: ["fixture://orders"] },
    {
      get: (target, key, receiver) =>
        key === property ? value : Reflect.get(target, key, receiver),
    },
  );

const malformedTargets = (value: unknown): ReadonlyArray<SourceDependencyTarget> =>
  Reflect.apply((targets: ReadonlyArray<SourceDependencyTarget>) => targets, undefined, [value]);

const throwingTarget = malformedTargets([
  new Proxy(
    {},
    {
      get: () => {
        throw new Error("hostile dependency target getter");
      },
    },
  ),
]);

describe("Runtime Core Source reporting integration", () => {
  it.effect("reports adapter inventory, leased inactivity, and dependency degradation", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: fixture.materializedSource({ label: "orders" }),
          },
          regionalOrders: {
            schema: Row,
            source: fixture.leasedSource(["region"], { label: "regional-orders" }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(fixture.layer),
      );
      yield* fixture.controls.awaitActive(materializedTarget);

      const healthy = yield* runtime.reporting.snapshot;
      expect(healthy).toStrictEqual({
        heartbeat: { status: "Ready", problems: [] },
        dependencies: [
          {
            dependency: "controllable-fixture",
            target: "orders",
            endpoints: ["fixture://orders"],
            status: "Ready",
            issues: [],
          },
          {
            dependency: "controllable-fixture",
            target: "regional-orders",
            endpoints: ["fixture://regional-orders"],
            status: "Inactive",
            issues: [],
          },
        ],
      });

      yield* fixture.controls.reject(
        materializedTarget,
        {
          _tag: "SourceFixtureFailure",
          message: "upstream record rejected",
          phase: "stream",
        },
        { lane: "fixture", offset: 1n },
      );
      const degraded = Option.getOrThrow(
        yield* runtime.reporting.changes.pipe(
          Stream.filter(({ heartbeat }) => heartbeat.status === "Degraded"),
          Stream.runHead,
        ),
      );
      expect(degraded).toStrictEqual({
        heartbeat: { status: "Degraded", problems: ["dependency"] },
        dependencies: [
          {
            dependency: "controllable-fixture",
            target: "orders",
            endpoints: ["fixture://orders"],
            status: "Degraded",
            issues: [],
          },
          {
            dependency: "controllable-fixture",
            target: "regional-orders",
            endpoints: ["fixture://regional-orders"],
            status: "Inactive",
            issues: [],
          },
        ],
      });
      yield* runtime.close;
    }),
  );

  const malformedDependencyEffect = (
    value: unknown,
  ): Effect.Effect<ReadonlyArray<SourceDependencyTarget>> =>
    Reflect.apply(
      (effect: Effect.Effect<ReadonlyArray<SourceDependencyTarget>>) => effect,
      undefined,
      [value],
    );

  it.effect.each([
    { label: "an undefined target list", targets: malformedTargets(undefined) },
    { label: "a null target list", targets: malformedTargets(null) },
    { label: "a primitive target list", targets: malformedTargets("orders") },
    { label: "a null target entry", targets: malformedTargets([null]) },
    { label: "a primitive target entry", targets: malformedTargets(["orders"]) },
    { label: "a throwing target getter", targets: throwingTarget },
    { label: "a non-string dependency", targets: [hostileTarget("dependency", 1)] },
    { label: "an empty dependency", targets: [hostileTarget("dependency", "")] },
    { label: "a non-string target", targets: [hostileTarget("target", 1)] },
    { label: "an empty target", targets: [{ target: "", endpoints: ["fixture://orders"] }] },
    { label: "non-array endpoints", targets: [hostileTarget("endpoints", null)] },
    {
      label: "a non-string endpoint",
      targets: [{ target: "orders", endpoints: [hostileTarget("target", 1).target] }],
    },
    { label: "an empty endpoint", targets: [{ target: "orders", endpoints: [""] }] },
    {
      label: "duplicate targets",
      targets: [
        { target: "orders", endpoints: ["fixture://one"] },
        { target: "orders", endpoints: ["fixture://two"] },
      ],
    },
  ])("rejects $label returned by adapter dependency reporting", ({ targets }) =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "hostile-reporting" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const layer = SourceAdapterServer.make(Adapter, {
        reporting: {
          dependencies: () => Effect.succeed(targets),
          classifyFailure: () => ({ problem: "self" }),
        },
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({ id: "hostile", events: Stream.never }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });

      const error = yield* Effect.flip(
        makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer)),
      );

      expect(error).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "Source Adapter hostile-reporting returned invalid dependency reporting targets.",
      });
    }),
  );

  it.effect("rejects a dependency reporter that throws before returning its Effect", () =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "throwing-reporting" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const layer = SourceAdapterServer.make(Adapter, {
        reporting: {
          dependencies: () => {
            throw new Error("dependency reporter defect");
          },
          classifyFailure: () => ({ problem: "self" }),
        },
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({ id: "throwing", events: Stream.never }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });

      expect(
        yield* Effect.flip(makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer))),
      ).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "Source Adapter throwing-reporting returned invalid dependency reporting targets.",
      });
    }),
  );

  it.effect.each([
    { label: "undefined", dependencyEffect: malformedDependencyEffect(undefined) },
    { label: "a non-Effect value", dependencyEffect: malformedDependencyEffect([]) },
    {
      label: "a value whose Effect identity check throws",
      dependencyEffect: malformedDependencyEffect(
        new Proxy(
          {},
          {
            has: () => {
              throw new Error("hostile Effect identity check");
            },
          },
        ),
      ),
    },
    {
      label: "a defecting Effect",
      dependencyEffect: Effect.die("dependency reporting effect defect"),
    },
  ])("rejects $label returned instead of a dependency Effect", ({ dependencyEffect }) =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "invalid-effect-reporting" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const baseLayer = SourceAdapterServer.make(Adapter, {
        reporting: {
          dependencies: () => Effect.succeed([]),
          classifyFailure: () => ({ problem: "self" }),
        },
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({ id: "invalid-effect", events: Stream.never }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const baseContext = yield* Effect.scoped(Layer.build(baseLayer));
      const baseService = Context.getUnsafe(baseContext, Adapter.runtimeService);
      const baseReporting = Option.getOrThrow(Option.fromNullishOr(baseService.reporting));
      const layer = Layer.succeed(Adapter.runtimeService)({
        ...baseService,
        reporting: {
          ...baseReporting,
          dependencies: () => dependencyEffect,
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });

      expect(
        yield* Effect.flip(makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer))),
      ).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message:
          "Source Adapter invalid-effect-reporting returned invalid dependency reporting targets.",
      });
    }),
  );

  it.effect("preserves interruption while dependency reporting is suspended", () =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "suspended-reporting" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const reportingStarted = yield* Deferred.make<void>();
      const layer = SourceAdapterServer.make(Adapter, {
        reporting: {
          dependencies: () =>
            Deferred.succeed(reportingStarted, undefined).pipe(Effect.andThen(Effect.never)),
          classifyFailure: () => ({ problem: "self" }),
        },
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({ id: "suspended", events: Stream.never }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });
      const construction = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(layer),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(reportingStarted);

      construction.interruptUnsafe();
      const exit = yield* Fiber.await(construction);

      expect(
        Exit.match(exit, {
          onFailure: Cause.hasInterruptsOnly,
          onSuccess: () => false,
        }),
      ).toBe(true);
    }),
  );

  it.effect("preserves interruption from a mixed dependency reporting cause", () =>
    Effect.gen(function* () {
      const Adapter = SourceAdapter.make({
        identity: { name: "mixed-cause-reporting" },
        failure: Schema.String,
        materialized: {
          metrics: Schema.Struct({}),
          rejectionLocation: Schema.Struct({}),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const layer = SourceAdapterServer.make(Adapter, {
        reporting: {
          dependencies: () =>
            Effect.failCause(
              Cause.fromReasons<never>([
                Cause.makeDieReason("dependency reporting defect"),
                Cause.makeInterruptReason(),
              ]),
            ),
          classifyFailure: () => ({ problem: "self" }),
        },
        materialized: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({ id: "mixed-cause", events: Stream.never }),
              ]),
            ),
          metrics: () => Effect.succeed({}),
          retry: Schedule.recurs(0),
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Row,
            source: Adapter.materializedSource(undefined),
          },
        },
      });

      const exit = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(layer),
        Effect.exit,
      );

      expect(
        Exit.match(exit, {
          onFailure: Cause.hasInterruptsOnly,
          onSuccess: () => false,
        }),
      ).toBe(true);
    }),
  );
});
