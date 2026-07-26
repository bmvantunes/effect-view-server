// Import Vitest directly so the Effect test runtime does not distort the
// adapter hot-path measurements.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Clock, Effect, Fiber, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { grpc } from "./model";
import { grpcServerLayer, type GrpcRuntimeClient } from "./server";
import { awaitTestCondition } from "./test-support";

declare const process: {
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
};

type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

type RequestMessage = Message<"grpc.benchmark.Request"> & {
  readonly region: string;
};

type EventMessage = Message<"grpc.benchmark.Event"> & {
  readonly id: string;
  readonly region: string;
  readonly value: number;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/benchmark.proto",
          package: "grpc.benchmark",
          syntax: "proto3",
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
            {
              name: "Event",
              field: [
                {
                  name: "id",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "region",
                  number: 2,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "value",
                  number: 3,
                  type: FieldDescriptorProto_Type.DOUBLE,
                },
              ],
            },
          ],
          service: [
            {
              name: "Rows",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.benchmark.Request",
                  outputType: ".grpc.benchmark.Event",
                  serverStreaming: true,
                },
              ],
            },
          ],
        }),
      ),
      (byte) => String.fromCharCode(byte),
    ).join(""),
  ),
);

const RequestSchema = messageDesc<RequestMessage>(descriptorFile, 0);
const EventSchema = messageDesc<EventMessage>(descriptorFile, 1);
const RowsService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);

const Row = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  value: Schema.Number,
});

type Pending = {
  readonly resolve: (result: IteratorResult<unknown>) => void;
};

type Invocation = {
  closed: boolean;
  readonly pending: Array<Pending>;
  readonly values: Array<unknown>;
};

const makeControlledClient = () => {
  const invocations: Array<Invocation> = [];
  const client: GrpcRuntimeClient = {
    service: RowsService,
    invoke: () => {
      const invocation: Invocation = {
        closed: false,
        pending: [],
        values: [],
      };
      invocations.push(invocation);
      return {
        [Symbol.asyncIterator]: () => ({
          next: () => {
            const value = invocation.values.shift();
            if (value !== undefined) {
              return Promise.resolve({ done: false, value });
            }
            return new Promise<IteratorResult<unknown>>((resolve) => {
              invocation.pending.push({ resolve });
            });
          },
          return: () => {
            invocation.closed = true;
            const pending = invocation.pending.splice(0);
            for (const waiter of pending) {
              waiter.resolve({ done: true, value: undefined });
            }
            return Promise.resolve({ done: true, value: undefined });
          },
        }),
      };
    },
  };
  const emit = (invocation: Invocation, value: unknown): void => {
    const pending = invocation.pending.shift();
    if (pending === undefined) {
      invocation.values.push(value);
    } else {
      pending.resolve({ done: false, value });
    }
  };
  return {
    client,
    emit,
    invocations,
  };
};

const awaitCondition = (
  label: string,
  predicate: () => boolean,
  remaining = 100_000,
): Effect.Effect<void> => awaitTestCondition(() => label, predicate, remaining, Effect.yieldNow);

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/u.test(normalized)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a safe positive integer.`);
  }
  return parsed;
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(normalized)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a safe non-negative integer.`);
  }
  return parsed;
};

const batchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_BATCH_SIZE",
  32,
);
const routeCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_GRPC_SOURCE_ADAPTER_ROUTE_COUNT",
  32,
);
const benchmarkOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", 5),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_TIME_MS", 0),
  warmupIterations: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS", 0),
  warmupTime: nonNegativeIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS", 0),
};
if (
  benchmarkOptions.time > 0 ||
  benchmarkOptions.warmupIterations > 0 ||
  benchmarkOptions.warmupTime > 0
) {
  throw new Error(
    "gRPC Source Adapter benchmark requires fixed independent samples; time and warmup must stay disabled.",
  );
}

const makeBenchmarkState = Effect.gen(function* () {
  const clock = yield* TestClock.make();
  const controlled = makeControlledClient();
  const sources = grpc.topicSources({
    rows: RowsService,
  });
  const materializedSource = sources.materialized({
    client: "rows",
    method: "stream",
    request: () => ({ region: "all" }),
    map: ({ value }) => {
      if (value.id.startsWith("reject-")) {
        throw new Error("planned benchmark Mapping rejection");
      }
      return {
        id: value.id,
        region: value.region,
        value: value.value,
      };
    },
  });
  const materializedConfig = defineViewServerConfig({
    topics: {
      rows: {
        schema: Row,
        source: materializedSource,
      },
    },
  });
  const materializedRuntime = yield* makeViewServerRuntimeCore(materializedConfig, {}).pipe(
    Effect.provide(
      grpcServerLayer(materializedConfig, {
        rows: controlled.client,
      }),
    ),
    Effect.provideService(Clock.Clock, clock),
  );
  const materializedSubscription = yield* materializedRuntime.liveClient.subscribe("rows", {
    select: ["id"],
  });
  const materializedDiagnostics =
    yield* materializedRuntime.liveClient.subscribeSourceHealth("rows");
  const committedIds = new Set<string>();
  let observedRejectedItemCount = 0n;
  const materializedObserverFiber = yield* materializedSubscription.events.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => {
        if (event.type === "snapshot") {
          for (const row of event.rows) {
            committedIds.add(row.id);
          }
        } else if (event.type === "delta") {
          for (const operation of event.operations) {
            if (operation.type === "insert" || operation.type === "update") {
              committedIds.add(operation.row.id);
            }
          }
        }
      }),
    ),
    Effect.forkDetach({ startImmediately: true }),
  );
  const materializedDiagnosticsFiber = yield* materializedDiagnostics.events.pipe(
    Stream.runForEach((health) =>
      Effect.sync(() => {
        observedRejectedItemCount = health.metrics.runtime.rejectedItemCount;
      }),
    ),
    Effect.forkDetach({ startImmediately: true }),
  );
  yield* Effect.yieldNow;
  yield* awaitCondition(
    "one materialized gRPC invocation",
    () => controlled.invocations.length === 1,
  );

  const leasedSource = sources.leased({
    client: "rows",
    method: "stream",
    routeBy: ["region"],
    request: (route) => ({ region: route.region }),
    map: ({ value }) => ({
      id: value.id,
      region: value.region,
      value: value.value,
    }),
  });
  const leasedConfig = defineViewServerConfig({
    topics: {
      rows: {
        schema: Row,
        source: leasedSource,
      },
    },
  });
  const leasedRuntime = yield* makeViewServerRuntimeCore(leasedConfig, {}).pipe(
    Effect.provide(
      grpcServerLayer(leasedConfig, {
        rows: controlled.client,
      }),
    ),
    Effect.provideService(Clock.Clock, clock),
  );
  const routes = Array.from({ length: routeCount }, (_, index) => ({
    region: `region-${index}`,
  }));
  const leasedSubscriptions = yield* Effect.forEach(
    routes,
    (route) =>
      leasedRuntime.liveClient
        .subscribe("rows", {
          routeBy: route,
          select: ["id"],
        })
        .pipe(Effect.provideService(Clock.Clock, clock)),
    { concurrency: "unbounded" },
  );
  const leasedDiagnostics = yield* Effect.forEach(
    routes,
    (route) => leasedRuntime.liveClient.subscribeSourceHealth("rows", route),
    { concurrency: "unbounded" },
  );
  const leasedHealthSampleCounts = Array.from({ length: routeCount }, () => 0);
  const leasedDiagnosticsFibers = yield* Effect.forEach(
    leasedDiagnostics,
    (subscription, index) =>
      subscription.events.pipe(
        Stream.runForEach(() =>
          Effect.sync(() => {
            leasedHealthSampleCounts[index] = (leasedHealthSampleCounts[index] ?? 0) + 1;
          }),
        ),
        Effect.forkDetach({ startImmediately: true }),
      ),
    { concurrency: "unbounded" },
  );
  yield* awaitCondition(
    `${routeCount} leased gRPC invocations`,
    () => controlled.invocations.length === routeCount + 1,
  );
  yield* awaitCondition("initial leased health samples", () =>
    leasedHealthSampleCounts.every((count) => count >= 1),
  );
  return {
    clock,
    controlled,
    leasedDiagnostics,
    leasedDiagnosticsFibers,
    leasedHealthSampleCounts,
    leasedRuntime,
    leasedSubscriptions,
    materializedRuntime,
    materializedDiagnostics,
    materializedDiagnosticsFiber,
    materializedObserverFiber,
    materializedSubscription,
    committedIds,
    observedRejectedItemCount: () => observedRejectedItemCount,
    routes,
  };
});

type BenchmarkState = Effect.Success<typeof makeBenchmarkState>;

let state: BenchmarkState | undefined;
let nextId = 0;
let nextRejectedItemCount = 0n;
let successfulMutationCount = 0;
let memoryBefore: BenchmarkMemorySnapshot | undefined;

const mappedBenchmarkName = `maps ${batchSize} decoded response messages into ordered Upserts`;
const rejectionBenchmarkName = "records one Mapping rejection and continues with a valid response";
const leasedHealthBenchmarkName = `publishes one-second health samples for ${routeCount} active Leased Feeds`;
const benchmarkCases = [
  mappedBenchmarkName,
  rejectionBenchmarkName,
  leasedHealthBenchmarkName,
] as const;

const benchmarkOutputJsonPath = (): string => {
  const configured = process.env["VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON"];
  return configured === undefined || configured.trim() === ""
    ? join(".artifacts", `grpc-source-adapter-${batchSize}batch-${routeCount}routes.json`)
    : configured.trim();
};

const benchmarkSummaryPath = (path: string): string =>
  path.endsWith(".json")
    ? `${path.slice(0, -".json".length)}.summary.json`
    : `${path}.summary.json`;

const memorySnapshot = (): BenchmarkMemorySnapshot => {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
};

const memoryDelta = (
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemorySnapshot => ({
  arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
  externalBytes: after.externalBytes - before.externalBytes,
  heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
  heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
  rssBytes: after.rssBytes - before.rssBytes,
});

const writeJsonFile = (path: string, value: unknown): void => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
};

const requireState = (): BenchmarkState => {
  if (state === undefined) {
    throw new Error("gRPC Source Adapter benchmark setup did not complete.");
  }
  return state;
};

beforeAll(async () => {
  memoryBefore = memorySnapshot();
  state = await Effect.runPromise(makeBenchmarkState.pipe(Effect.scoped));
});

afterAll(async () => {
  const current = state;
  if (current === undefined) {
    return;
  }
  await Effect.runPromise(
    Effect.all(
      [
        current.materializedSubscription.close(),
        current.materializedDiagnostics.close(),
        Fiber.interrupt(current.materializedDiagnosticsFiber),
        Fiber.interrupt(current.materializedObserverFiber),
        ...current.leasedSubscriptions.map((subscription) => subscription.close()),
        ...current.leasedDiagnostics.map((subscription) => subscription.close()),
        ...current.leasedDiagnosticsFibers.map(Fiber.interrupt),
      ],
      { concurrency: "unbounded", discard: true },
    ),
  );
  const [materializedHealth, leasedHealth] = await Effect.runPromise(
    Effect.all([
      current.materializedRuntime.client.health(),
      current.leasedRuntime.client.health(),
    ]),
  );
  await Effect.runPromise(
    Effect.all([current.materializedRuntime.close, current.leasedRuntime.close], {
      concurrency: "unbounded",
      discard: true,
    }),
  );
  await Effect.runPromise(
    awaitCondition("all gRPC benchmark invocations to finalize", () =>
      current.controlled.invocations.every((invocation) => invocation.closed),
    ),
  );
  const healthSnapshots = [materializedHealth, leasedHealth];
  const cleanupLeakCount =
    current.controlled.invocations.filter((invocation) => !invocation.closed).length +
    healthSnapshots.reduce((total, health) => {
      const topic = health.engine.topics.rows;
      return total + topic.activeSubscriptions + topic.activeViews + topic.queuedEvents;
    }, 0);
  const backpressureCount = healthSnapshots.reduce(
    (total, health) => total + health.engine.topics.rows.backpressureEvents,
    0,
  );
  const queuedEventCount = healthSnapshots.reduce(
    (total, health) => total + health.engine.topics.rows.queuedEvents,
    0,
  );
  const before = Option.getOrThrow(Option.fromUndefinedOr(memoryBefore));
  const after = memorySnapshot();
  const outputJsonPath = benchmarkOutputJsonPath();
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    benchmarkCases,
    benchmarkName: "gRPC Source Adapter focused overhead benchmark",
    benchmarkScope: "runtime-grpc-source-adapter",
    cleanupLeakCount,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: after,
      before,
      totalDelta: memoryDelta(before, after),
    },
    mutationCount: successfulMutationCount,
    notes: [
      "Localhost CPU/GC stress benchmark over the in-process gRPC Source Adapter runtime seam; no network transport is involved.",
      "Warmups and timed repetitions stay disabled because every measured case mutates shared benchmark state.",
      "Cleanup, backpressure, queued-event, mutation, route, and subscriber evidence is recorded exactly.",
    ],
    queuedEventCount,
    rowCount: batchSize,
    subscriberCount: routeCount + 1,
    topics: ["rows"],
  });
});

describe("gRPC Source Adapter focused overhead", () => {
  bench(
    mappedBenchmarkName,
    async () => {
      const current = requireState();
      const invocation = Option.getOrThrow(
        Option.fromUndefinedOr(current.controlled.invocations[0]),
      );
      let lastId = "";
      for (let index = 0; index < batchSize; index += 1) {
        lastId = `mapped-${nextId}`;
        current.controlled.emit(invocation, {
          id: lastId,
          region: "eu",
          value: nextId,
        });
        nextId += 1;
      }
      await Effect.runPromise(
        awaitCondition("mapped gRPC response convergence", () => current.committedIds.has(lastId)),
      );
      successfulMutationCount += batchSize;
    },
    benchmarkOptions,
  );

  bench(
    rejectionBenchmarkName,
    async () => {
      const current = requireState();
      const invocation = Option.getOrThrow(
        Option.fromUndefinedOr(current.controlled.invocations[0]),
      );
      const expectedRejectedItemCount = nextRejectedItemCount + 1n;
      current.controlled.emit(invocation, {
        id: `reject-${nextId}`,
        region: "eu",
        value: nextId,
      });
      nextId += 1;
      const continuedId = `continued-${nextId}`;
      current.controlled.emit(invocation, {
        id: continuedId,
        region: "eu",
        value: nextId,
      });
      nextId += 1;
      await Effect.runPromise(
        Effect.all(
          [
            awaitCondition("post-rejection gRPC response convergence", () =>
              current.committedIds.has(continuedId),
            ),
            awaitCondition(
              "gRPC Mapping rejection observation",
              () => current.observedRejectedItemCount() >= expectedRejectedItemCount,
            ),
          ],
          { concurrency: "unbounded" },
        ),
      );
      nextRejectedItemCount = expectedRejectedItemCount;
      successfulMutationCount += 1;
    },
    benchmarkOptions,
  );

  bench(
    leasedHealthBenchmarkName,
    async () => {
      const current = requireState();
      const expectedSampleCounts = current.leasedHealthSampleCounts.map((count) => count + 1);
      await Effect.runPromise(
        current.clock
          .adjust("1 second")
          .pipe(
            Effect.andThen(
              awaitCondition("leased gRPC health sampling", () =>
                current.leasedHealthSampleCounts.every(
                  (count, index) =>
                    count >= (expectedSampleCounts[index] ?? Number.POSITIVE_INFINITY),
                ),
              ),
            ),
          ),
      );
    },
    benchmarkOptions,
  );
});
