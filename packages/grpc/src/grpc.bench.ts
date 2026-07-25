// Import Vitest directly so the Effect test runtime does not distort the
// adapter hot-path measurements.
import { afterAll, beforeAll, bench, describe } from "vitest";
import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import { Clock, Effect, Fiber, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { grpc } from "./model";
import { grpcServerLayer, type GrpcRuntimeClient } from "./server";

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
  readonly pending: Array<Pending>;
  readonly values: Array<unknown>;
};

const makeControlledClient = () => {
  const invocations: Array<Invocation> = [];
  const client: GrpcRuntimeClient = {
    service: RowsService,
    invoke: () => {
      const invocation: Invocation = {
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

const awaitCondition = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    predicate() ? Effect.void : Effect.yieldNow.pipe(Effect.andThen(awaitCondition(predicate))),
  );

const routeCount = 32;
const benchmarkOptions = {
  iterations: 5,
  time: 0,
  warmupIterations: 1,
  warmupTime: 0,
};

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
  yield* awaitCondition(() => controlled.invocations.length === 1);

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
      leasedRuntime.liveClient.subscribe("rows", {
        routeBy: route,
        select: ["id"],
      }),
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
  yield* awaitCondition(() => controlled.invocations.length === routeCount + 1);
  yield* awaitCondition(() => leasedHealthSampleCounts.every((count) => count >= 1));
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

const requireState = (): BenchmarkState => {
  if (state === undefined) {
    throw new Error("gRPC Source Adapter benchmark setup did not complete.");
  }
  return state;
};

beforeAll(async () => {
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
        current.materializedRuntime.close,
        current.leasedRuntime.close,
      ],
      { concurrency: "unbounded", discard: true },
    ),
  );
});

describe("gRPC Source Adapter focused overhead", () => {
  bench(
    "maps 32 decoded response messages into ordered Upserts",
    async () => {
      const current = requireState();
      const invocation = Option.getOrThrow(
        Option.fromUndefinedOr(current.controlled.invocations[0]),
      );
      let lastId = "";
      for (let index = 0; index < 32; index += 1) {
        lastId = `mapped-${nextId}`;
        current.controlled.emit(invocation, {
          id: lastId,
          region: "eu",
          value: nextId,
        });
        nextId += 1;
      }
      await Effect.runPromise(awaitCondition(() => current.committedIds.has(lastId)));
    },
    benchmarkOptions,
  );

  bench(
    "records one Mapping rejection and continues with a valid response",
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
            awaitCondition(() => current.committedIds.has(continuedId)),
            awaitCondition(() => current.observedRejectedItemCount() >= expectedRejectedItemCount),
          ],
          { concurrency: "unbounded" },
        ),
      );
      nextRejectedItemCount = expectedRejectedItemCount;
    },
    benchmarkOptions,
  );

  bench(
    "publishes one-second health samples for 32 active Leased Feeds",
    async () => {
      const current = requireState();
      const expectedSampleCounts = current.leasedHealthSampleCounts.map((count) => count + 1);
      await Effect.runPromise(
        current.clock
          .adjust("1 second")
          .pipe(
            Effect.andThen(
              awaitCondition(() =>
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
