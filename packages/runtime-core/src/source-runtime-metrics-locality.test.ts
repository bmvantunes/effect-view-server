import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import { Effect, Option, Schedule, Schema, Stream } from "effect";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: ViewServerId,
  region: Schema.String,
});

const Failure = Schema.TaggedStruct("MetricsLocalityFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  binding: Schema.String,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

const adapter = SourceAdapter.make({
  identity: { name: "metrics-locality" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
  leased: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly stream: string;
    }>(),
  },
});

const layer = SourceAdapterServer.make(adapter, {
  materialized: {
    acquire: () =>
      Effect.succeed(
        SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "materialized",
            events: Stream.never,
          }),
        ]),
      ),
    metrics: (input) =>
      Effect.succeed({
        binding: `materialized:${input.topic}:${input.definition.stream}`,
      }),
    retry: Schedule.recurs(0),
  },
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
    metrics: (input) =>
      Effect.succeed({
        binding: `leased:${input.topic}:${input.definition.stream}:${String(
          input.target.route["region"],
        )}`,
      }),
    retry: Schedule.recurs(0),
  },
});

const primitiveMetricsAdapter = SourceAdapter.make({
  identity: { name: "primitive-metrics" },
  failure: Failure,
  materialized: {
    metrics: Schema.BigInt,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<void>(),
  },
  leased: undefined,
});

const primitiveMetricsLayer = SourceAdapterServer.make(primitiveMetricsAdapter, {
  materialized: {
    acquire: () =>
      Effect.succeed(
        SourceAdapterServer.attempt([
          SourceAdapterServer.lane({
            id: "materialized",
            events: Stream.never,
          }),
        ]),
      ),
    metrics: () => Effect.succeed(7n),
    retry: Schedule.recurs(0),
  },
});

describe("Runtime Core Source metrics locality", () => {
  it.effect("retains exact primitive Adapter metrics in canonical health", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: {
          rows: {
            schema: Row,
            source: primitiveMetricsAdapter.materializedSource(undefined),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(primitiveMetricsLayer),
      );

      expect((yield* runtime.client.health()).sources.rows.metrics.adapter).toBe(7n);

      yield* runtime.close;
    }),
  );

  it.effect("samples metrics for each Topic binding and Leased route", () =>
    Effect.gen(function* () {
      const config = defineViewServerConfig({
        topics: {
          first: {
            schema: Row,
            source: adapter.materializedSource({ stream: "shared" }),
          },
          second: {
            schema: Row,
            source: adapter.materializedSource({ stream: "shared" }),
          },
          routed: {
            schema: Row,
            source: adapter.leasedSource(["region"], { stream: "shared" }),
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
      const firstDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "first" });
      const secondDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "second",
      });
      const firstHealth = Option.getOrThrow(
        yield* firstDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      const secondHealth = Option.getOrThrow(
        yield* secondDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );

      expect({
        first: firstHealth.metrics.adapter.binding,
        second: secondHealth.metrics.adapter.binding,
      }).toStrictEqual({
        first: "materialized:first:shared",
        second: "materialized:second:shared",
      });
      const materializedAggregate = yield* runtime.client.health();
      expect(materializedAggregate.sources).toStrictEqual({
        first: firstHealth,
        second: secondHealth,
        routed: [],
      });

      const usSubscription = yield* runtime.liveClient.subscribe("routed", {
        routeBy: { region: "us" },
        select: ["id", "region"],
      });
      const euSubscription = yield* runtime.liveClient.subscribe("routed", {
        routeBy: { region: "eu" },
        select: ["id", "region"],
      });
      const euDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "routed",
        routeBy: { region: "eu" },
      });
      const usDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "routed",
        routeBy: { region: "us" },
      });
      const euHealth = Option.getOrThrow(
        yield* euDiagnostics.events.pipe(
          Stream.filter((result) => result._tag === "Active"),
          Stream.map((result) => result.health),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      const usHealth = Option.getOrThrow(
        yield* usDiagnostics.events.pipe(
          Stream.filter((result) => result._tag === "Active"),
          Stream.map((result) => result.health),
          Stream.take(1),
          Stream.runHead,
        ),
      );

      expect({
        eu: euHealth.metrics.adapter.binding,
        us: usHealth.metrics.adapter.binding,
      }).toStrictEqual({
        eu: "leased:routed:shared:eu",
        us: "leased:routed:shared:us",
      });
      const leasedAggregate = yield* runtime.client.health();
      expect(leasedAggregate.sources).toStrictEqual({
        first: firstHealth,
        second: secondHealth,
        routed: [euHealth, usHealth],
      });

      yield* firstDiagnostics.close();
      yield* secondDiagnostics.close();
      yield* euDiagnostics.close();
      yield* usDiagnostics.close();
      yield* euSubscription.close();
      const afterEuRelease = yield* runtime.client.health();
      expect(afterEuRelease.sources.routed).toStrictEqual([usHealth]);
      yield* usSubscription.close();
      const afterAllLeasesRelease = yield* runtime.client.health();
      expect(afterAllLeasesRelease.sources.routed).toStrictEqual([]);
      yield* runtime.close;
    }),
  );
});
