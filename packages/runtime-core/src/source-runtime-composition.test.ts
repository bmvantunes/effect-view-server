import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  decodeSourceToolkitUpsert,
  makeSourceDelivery,
} from "@effect-view-server/source-adapter/internal";
import { SourceFixture } from "@effect-view-server/source-adapter-testing";
import {
  Chunk,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: ViewServerId,
  value: Schema.String,
});

const OtherFailure = Schema.TaggedStruct("OtherSourceFailure", {
  message: Schema.String,
});
const otherAdapter = SourceAdapter.make({
  identity: { name: "other-source" },
  failure: OtherFailure,
  materialized: {
    metrics: Schema.Struct({ observed: Schema.BigInt }),
    rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});

describe("Runtime Core Source composition validation", () => {
  it("fails before startup when the nominal runtime service is missing", async () => {
    const fixture = await Effect.runPromise(SourceFixture.make(Row));
    const config = defineViewServerConfig({
      topics: {
        rows: {
          schema: Row,
          source: fixture.materializedSource({
            label: "missing-service",
          }),
        },
      },
    });
    const runtime = Reflect.apply(makeViewServerRuntimeCore, undefined, [config, {}]);
    const exit = await Effect.runPromiseExit(Effect.provide(runtime, Context.empty()));

    expect(Exit.isFailure(exit)).toBe(true);
  });

  it.effect("rejects mismatched nominal handles and missing lifecycle implementations", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const definition = fixture.materializedSource({
        label: "invalid-service",
      });
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: definition,
          },
        },
      });
      const context = yield* Layer.build(fixture.layer);
      const service = Context.get(context, fixture.adapter.runtimeService);
      const mismatchedService = new Proxy(service, {
        get: (target, property, receiver) =>
          property === "adapter" ? otherAdapter : Reflect.get(target, property, receiver),
      });
      const missingLifecycleService = new Proxy(service, {
        get: (target, property, receiver) =>
          property === "materialized" ? undefined : Reflect.get(target, property, receiver),
      });
      const mismatched = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, mismatchedService),
        Effect.exit,
      );
      const missingLifecycle = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provideService(fixture.adapter.runtimeService, missingLifecycleService),
        Effect.exit,
      );

      expect(Exit.isFailure(mismatched) && Exit.isFailure(missingLifecycle)).toBe(true);
    }),
  );

  it.effect("rejects a topic graph that changes after source binding capture", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const stable = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "changing-topic",
            }),
          },
        },
      });
      let reads = 0;
      const topics = new Proxy(stable.topics, {
        get: (target, property, receiver) => {
          if (property !== "rows") {
            return Reflect.get(target, property, receiver);
          }
          reads += 1;
          return reads === 1 ? Reflect.get(target, property, receiver) : undefined;
        },
      });
      const changing = {
        ...stable,
        topics,
      };
      const exit = yield* makeViewServerRuntimeCore(changing, {}).pipe(
        Effect.provide(fixture.layer),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("revalidates hostile Source Definition envelopes before starting attempts", () =>
    Effect.gen(function* () {
      const fixture = yield* SourceFixture.make(Row);
      const stable = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: fixture.materializedSource({
              label: "hostile-definition",
            }),
          },
        },
      });
      const hostileDefinition = new Proxy(stable.topics.rows.source, {
        get: (target, property, receiver) =>
          property === "retry"
            ? {
                _tag: "Override",
                policy: null,
              }
            : Reflect.get(target, property, receiver),
      });
      const hostile = {
        ...stable,
        topics: {
          rows: {
            ...stable.topics.rows,
            source: hostileDefinition,
          },
        },
      };
      const exit = yield* makeViewServerRuntimeCore(hostile, {}).pipe(
        Effect.provide(fixture.layer),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(fixture.controls.counts({ _tag: "Materialized" }).acquisitions).toBe(0n);
    }),
  );

  it.effect("keeps invalid initial Source metrics inside Materialized supervision", () =>
    Effect.gen(function* () {
      const invalidFixture = yield* SourceFixture.make(Row);
      const healthyAdapter = SourceAdapter.make({
        identity: { name: "healthy-sibling-source" },
        failure: Schema.Never,
        materialized: {
          metrics: Schema.Struct({ observed: Schema.BigInt }),
          rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
        leased: undefined,
      });
      const healthyApplied = yield* Deferred.make<void>();
      let healthyAcquisitions = 0n;
      let healthyFinalizations = 0n;
      const healthyLayer = SourceAdapterServer.make(healthyAdapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              healthyAcquisitions += 1n;
              const mutation = yield* decodeSourceToolkitUpsert(input.toolkit, {
                id: "healthy-row",
                value: "delivered",
              });
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "healthy",
                  events: Stream.make(
                    makeSourceDelivery(Chunk.of(mutation), () =>
                      Deferred.succeed(healthyApplied, undefined).pipe(Effect.asVoid),
                    ),
                  ).pipe(
                    Stream.concat(Stream.never),
                    Stream.ensuring(
                      Effect.sync(() => {
                        healthyFinalizations += 1n;
                      }),
                    ),
                  ),
                }),
              ]);
            }),
          metrics: () => Effect.succeed({ observed: 7n }),
          retry: Schedule.recurs(0),
        },
      });
      const invalidMetrics = new Proxy(
        { observed: 0n },
        {
          get: (target, property, receiver) =>
            property === "observed" ? "invalid" : Reflect.get(target, property, receiver),
        },
      );
      yield* invalidFixture.controls.setMetrics(invalidMetrics);
      const config = defineViewServerConfig({
        topics: {
          invalid: {
            schema: Row,
            source: invalidFixture.materializedSource({
              label: "invalid-initial-metrics",
            }),
          },
          healthy: {
            schema: Row,
            source: healthyAdapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(Layer.merge(invalidFixture.layer, healthyLayer)),
      );
      yield* Deferred.await(healthyApplied);
      const subscription = yield* runtime.liveClient.subscribe("healthy", {
        select: ["id", "value"],
      });
      const snapshot = Option.getOrThrow(
        yield* subscription.events.pipe(
          Stream.filter((event) => event.type === "snapshot"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      const health = yield* runtime.client.health();

      expect(snapshot.rows).toStrictEqual([{ id: "healthy-row", value: "delivered" }]);
      expect(health.sources.invalid).toBeUndefined();
      expect(health.sources.healthy?.metrics.adapter.observed).toBe(7n);
      expect(invalidFixture.controls.metricReads()).toBe(1n);
      expect(invalidFixture.controls.counts({ _tag: "Materialized" })).toStrictEqual({
        acquisitions: 0n,
        finalizations: 0n,
      });
      expect(healthyAcquisitions).toBe(1n);
      expect(healthyFinalizations).toBe(0n);
      yield* subscription.close();
      yield* runtime.close;
      expect(healthyFinalizations).toBe(1n);
    }),
  );
});
