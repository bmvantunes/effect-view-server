import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@effect-view-server/config";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import { Effect, Option, Schedule, Schema, Stream } from "effect";
import { makeViewServerRuntimeCore } from "./index";

const Row = Schema.Struct({
  id: Schema.String,
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

describe("Runtime Core Source metrics locality", () => {
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
      const firstDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("first");
      const secondDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("second");
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

      const euSubscription = yield* runtime.liveClient.subscribe("routed", {
        routeBy: { region: "eu" },
        select: ["id", "region"],
      });
      const usSubscription = yield* runtime.liveClient.subscribe("routed", {
        routeBy: { region: "us" },
        select: ["id", "region"],
      });
      const euDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("routed", {
        region: "eu",
      });
      const usDiagnostics = yield* runtime.liveClient.subscribeSourceHealth("routed", {
        region: "us",
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

      yield* firstDiagnostics.close();
      yield* secondDiagnostics.close();
      yield* euDiagnostics.close();
      yield* usDiagnostics.close();
      yield* euSubscription.close();
      yield* usSubscription.close();
      yield* runtime.close;
    }),
  );
});
