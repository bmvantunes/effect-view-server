// Import Vitest directly so the Effect test-runtime graph does not distort
// the Source Adapter hot-path measurements.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@effect-view-server/config";
import {
  SourceFixture,
  type ControllableSourceFixture,
  type SourceFixtureMaterializedDefinition,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import { Clock, Deferred, Effect, Schema } from "effect";
import { TestClock } from "effect/testing";
import { makeViewServerRuntimeCore } from "./index";
import { sourceRuntimeInternals } from "./source-runtime";

const benchmarkOptions = {
  iterations: 5,
  time: 0,
  warmupIterations: 1,
  warmupTime: 0,
};
const manySourceCount = 32;
const lookupSourceCount = 1_024;
const materializedTarget: SourceFixtureTarget = {
  _tag: "Materialized",
};
const Row = Schema.Struct({
  id: Schema.String,
  value: Schema.Number,
});

type BenchmarkTopic = {
  readonly schema: typeof Row;
  readonly source: SourceFixtureMaterializedDefinition;
};
type ClosableRuntime = {
  readonly close: Effect.Effect<void>;
};
type BenchmarkState = {
  readonly clock: TestClock.TestClock;
  readonly eventFixture: ControllableSourceFixture;
  readonly eventRuntime: ClosableRuntime;
  readonly manyFixture: ControllableSourceFixture;
  readonly manyRuntime: ClosableRuntime;
  readonly lookup: Effect.Effect<ReadonlyMap<string, unknown>, ViewServerRuntimeError>;
};

let state: BenchmarkState | undefined;
let nextId = 0;

const requireState = (): BenchmarkState => {
  if (state === undefined) {
    throw new Error("Source Adapter benchmark setup did not complete.");
  }
  return state;
};

const makeTopics = (
  fixture: ControllableSourceFixture,
  count: number,
  label: string,
): Readonly<Record<string, BenchmarkTopic>> => {
  const topics: Record<string, BenchmarkTopic> = {};
  for (let index = 0; index < count; index += 1) {
    topics[`${label}-${index}`] = {
      schema: Row,
      source: fixture.materializedSource({
        label: `${label}-${index}`,
      }),
    };
  }
  return topics;
};

beforeAll(async () => {
  const clock = await Effect.runPromise(TestClock.make().pipe(Effect.scoped));
  const eventFixture = await Effect.runPromise(SourceFixture.make(Row));
  const eventConfig = defineViewServerConfig({
    topics: {
      events: {
        schema: Row,
        source: eventFixture.materializedSource({
          label: "event-processing",
        }),
      },
    },
  });
  const eventRuntime = await Effect.runPromise(
    makeViewServerRuntimeCore(eventConfig, {}).pipe(
      Effect.provide(eventFixture.layer),
      Effect.provideService(Clock.Clock, clock),
    ),
  );
  await Effect.runPromise(eventFixture.controls.awaitActive(materializedTarget));

  const manyFixture = await Effect.runPromise(SourceFixture.make(Row));
  const manyConfig = defineViewServerConfig({
    topics: makeTopics(manyFixture, manySourceCount, "sample"),
  });
  const manyRuntime = await Effect.runPromise(
    makeViewServerRuntimeCore(manyConfig, {}).pipe(
      Effect.provide(manyFixture.layer),
      Effect.provideService(Clock.Clock, clock),
    ),
  );
  for (
    let attempt = 0;
    attempt < 100 && manyFixture.controls.metricReads() < BigInt(manySourceCount);
    attempt += 1
  ) {
    await Effect.runPromise(Effect.yieldNow);
  }
  if (manyFixture.controls.metricReads() < BigInt(manySourceCount)) {
    throw new Error("Many-source metrics samplers did not become ready.");
  }
  for (let turn = 0; turn < 10; turn += 1) {
    await Effect.runPromise(Effect.yieldNow);
  }

  const lookupTopics = makeTopics(manyFixture, lookupSourceCount, "lookup");
  const lookupConfig = defineViewServerConfig({
    topics: lookupTopics,
  });
  const lookup = Effect.gen(function* () {
    const context =
      yield* Effect.context<
        import("./source-runtime").ViewServerSourceRequirements<typeof lookupConfig.topics>
      >();
    return yield* sourceRuntimeInternals.resolveEntries(lookupConfig, context);
  }).pipe(Effect.provide(manyFixture.layer));

  state = {
    clock,
    eventFixture,
    eventRuntime,
    manyFixture,
    manyRuntime,
    lookup,
  };
});

afterAll(async () => {
  const current = state;
  if (current === undefined) {
    return;
  }
  await Effect.runPromise(
    Effect.all([current.eventRuntime.close, current.manyRuntime.close], {
      concurrency: 2,
      discard: true,
    }),
  );
});

describe("Source Adapter core", () => {
  bench(
    "Source Lane Event processing (16 ordered Upserts)",
    async () => {
      const current = requireState();
      const batch = nextId;
      nextId += 1;
      const settled = await Effect.runPromise(Deferred.make<void>());
      const first = {
        _tag: "Upsert" as const,
        row: {
          id: `event-${batch}-0`,
          value: 0,
        },
      };
      const rest = Array.from({ length: 15 }, (_, index) => ({
        _tag: "Upsert" as const,
        row: {
          id: `event-${batch}-${index + 1}`,
          value: index + 1,
        },
      }));
      await Effect.runPromise(
        Effect.gen(function* () {
          yield* current.eventFixture.controls.delivery(materializedTarget, [first, ...rest], () =>
            Deferred.succeed(settled, undefined).pipe(Effect.asVoid),
          );
          yield* Deferred.await(settled);
        }),
      );
    },
    benchmarkOptions,
  );

  bench(
    "Source Item Rejection recording and valid-item continuation",
    async () => {
      const current = requireState();
      const item = nextId;
      nextId += 1;
      const rejectionSettled = await Effect.runPromise(Deferred.make<void>());
      const deliverySettled = await Effect.runPromise(Deferred.make<void>());
      await Effect.runPromise(
        Effect.gen(function* () {
          yield* current.eventFixture.controls.reject(
            materializedTarget,
            SourceFixture.failure("benchmark rejection", "stream"),
            {
              lane: "fixture",
              offset: BigInt(item),
            },
            () => Deferred.succeed(rejectionSettled, undefined).pipe(Effect.asVoid),
          );
          yield* current.eventFixture.controls.upsert(
            materializedTarget,
            {
              id: `continued-${item}`,
              value: item,
            },
            () => Deferred.succeed(deliverySettled, undefined).pipe(Effect.asVoid),
          );
          yield* Deferred.await(rejectionSettled);
          yield* Deferred.await(deliverySettled);
        }),
      );
    },
    benchmarkOptions,
  );

  bench(
    "one-second adapter metrics sampling across 32 active sources",
    async () => {
      const current = requireState();
      const before = current.manyFixture.controls.metricReads();
      await Effect.runPromise(
        current.clock.adjust("1 second").pipe(Effect.andThen(Effect.yieldNow)),
      );
      const sampled = current.manyFixture.controls.metricReads() - before;
      if (sampled < BigInt(manySourceCount)) {
        throw new Error(`Expected ${manySourceCount} metric samples, received ${sampled}.`);
      }
    },
    benchmarkOptions,
  );

  bench(
    "O(1) nominal adapter runtime lookup across 1,024 Source Definitions",
    async () => {
      const entries = await Effect.runPromise(requireState().lookup);
      if (entries.size !== lookupSourceCount) {
        throw new Error(
          `Expected ${lookupSourceCount} resolved sources, received ${entries.size}.`,
        );
      }
    },
    benchmarkOptions,
  );
});
